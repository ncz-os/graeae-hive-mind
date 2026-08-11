"""Regression tests for deployability defects found in adversarial review."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest

import httpx
from starlette.requests import Request


_DB_FILE = tempfile.NamedTemporaryFile(prefix="hive-defects-", suffix=".db", delete=False)
_DB_FILE.close()
os.environ["AGENT_BUS_DB"] = _DB_FILE.name

import agent_bus
from hive.persistence.factory import get_hive_repository
from hive.persistence.sqlite import SqliteHiveRepository
from hive.auth import client_token


def _load_script(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class AgentBusDefectTests(unittest.IsolatedAsyncioTestCase):
    async def test_worker_session_cannot_cancel_or_reroute_queued_job(self):
        original_workers = agent_bus.HIVE_BUS_TOKENS
        original_admins = agent_bus.HIVE_BUS_ADMIN_TOKENS
        agent_bus.HIVE_BUS_TOKENS = frozenset({"worker-secret"})
        agent_bus.HIVE_BUS_ADMIN_TOKENS = frozenset({"admin-secret"})
        try:
            async with agent_bus.lifespan(agent_bus.app):
                transport = httpx.ASGITransport(app=agent_bus.app)
                async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                    registration = await client.post(
                        "/v1/agents/register",
                        headers={"Authorization": "Bearer worker-secret"},
                        json={"kind": "goose", "runtime": "goose", "host": "queue-auth"},
                    )
                    token = registration.json()["agent_token"]
                    job_id = "queue-auth-regression"
                    async with agent_bus.connect_db() as db:
                        await db.execute(
                            "INSERT OR REPLACE INTO jobs (id, submitter_urn, kind, priority, "
                            "status, started_at) VALUES (?, 'urn:test', 'test', 0, 'queued', ?)",
                            (job_id, agent_bus.time.time()),
                        )
                        await db.commit()
                    cancelled = await client.patch(
                        f"/v1/jobs/{job_id}",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"status": "cancelled"},
                    )
                    rerouted = await client.patch(
                        f"/v1/jobs/{job_id}",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"status": "queued", "eligible_hosts": ["attacker"]},
                    )
                self.assertEqual(cancelled.status_code, 403)
                self.assertEqual(rerouted.status_code, 403)
                async with agent_bus.connect_db() as db:
                    async with db.execute(
                        "SELECT status, eligible_hosts FROM jobs WHERE id=?", (job_id,)
                    ) as cur:
                        self.assertEqual(await cur.fetchone(), ("queued", None))
        finally:
            agent_bus.HIVE_BUS_TOKENS = original_workers
            agent_bus.HIVE_BUS_ADMIN_TOKENS = original_admins

    def test_derived_session_secret_is_stable_across_restart(self):
        workers = frozenset({"worker-b", "worker-a"})
        admins = frozenset({"admin"})
        first = agent_bus._session_signing_secret(None, workers, admins)
        second = agent_bus._session_signing_secret(None, workers, admins)
        self.assertEqual(first, second)
        self.assertEqual(
            agent_bus._session_signing_secret("explicit", workers, admins), "explicit"
        )

    async def test_worker_token_cannot_create_job_through_mcp(self):
        original_workers = agent_bus.HIVE_BUS_TOKENS
        original_admins = agent_bus.HIVE_BUS_ADMIN_TOKENS
        agent_bus.HIVE_BUS_TOKENS = frozenset({"worker-secret"})
        agent_bus.HIVE_BUS_ADMIN_TOKENS = frozenset({"admin-secret"})
        try:
            transport = httpx.ASGITransport(app=agent_bus.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.post(
                    "/mcp",
                    headers={"Authorization": "Bearer worker-secret"},
                    json={"method": "job.create", "params": {}},
                )
            self.assertEqual(response.status_code, 403)
        finally:
            agent_bus.HIVE_BUS_TOKENS = original_workers
            agent_bus.HIVE_BUS_ADMIN_TOKENS = original_admins

    async def test_single_worker_token_is_not_promoted_to_admin(self):
        original_admins = agent_bus.HIVE_BUS_ADMIN_TOKENS
        agent_bus.HIVE_BUS_ADMIN_TOKENS = frozenset()
        request = Request({
            "type": "http", "method": "POST", "path": "/mcp", "headers": [],
            "query_string": b"", "server": ("test", 80), "scheme": "http",
        })
        request.state.bus_tier = "worker"
        try:
            with self.assertRaises(agent_bus.HTTPException) as raised:
                await agent_bus.require_bus_admin(request)
            self.assertEqual(raised.exception.status_code, 503)
        finally:
            agent_bus.HIVE_BUS_ADMIN_TOKENS = original_admins

    def test_singular_and_plural_bus_token_contracts(self):
        old = {name: os.environ.get(name) for name in (
            "HIVE_BUS_TOKEN", "HIVE_BUS_TOKENS", "HIVE_BUS_ADMIN_TOKEN", "HIVE_BUS_ADMIN_TOKENS",
        )}
        try:
            os.environ["HIVE_BUS_TOKEN"] = "worker"
            os.environ.pop("HIVE_BUS_TOKENS", None)
            self.assertEqual(
                agent_bus._load_compatible_token_set("HIVE_BUS_TOKEN", "HIVE_BUS_TOKENS"),
                frozenset({"worker"}),
            )
            os.environ["HIVE_BUS_TOKENS"] = "old,worker"
            self.assertEqual(len(agent_bus._load_compatible_token_set(
                "HIVE_BUS_TOKEN", "HIVE_BUS_TOKENS"
            )), 2)
            self.assertEqual(client_token("HIVE_BUS_TOKEN", "HIVE_BUS_TOKENS"), "worker")
            os.environ.pop("HIVE_BUS_TOKEN")
            self.assertEqual(client_token("HIVE_BUS_TOKEN", "HIVE_BUS_TOKENS"), "old")
            os.environ["HIVE_BUS_TOKEN"] = "worker"
            os.environ["HIVE_BUS_TOKENS"] = "different"
            with self.assertRaisesRegex(RuntimeError, "conflicting"):
                agent_bus._load_compatible_token_set("HIVE_BUS_TOKEN", "HIVE_BUS_TOKENS")
        finally:
            for name, value in old.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value

    async def test_agent_session_token_is_bound_to_registered_urn(self):
        original_workers = agent_bus.HIVE_BUS_TOKENS
        original_admins = agent_bus.HIVE_BUS_ADMIN_TOKENS
        agent_bus.HIVE_BUS_TOKENS = frozenset({"worker-secret"})
        agent_bus.HIVE_BUS_ADMIN_TOKENS = frozenset({"admin-secret"})
        try:
            async with agent_bus.lifespan(agent_bus.app):
                transport = httpx.ASGITransport(app=agent_bus.app)
                async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                    registration = await client.post(
                        "/v1/agents/register",
                        headers={"Authorization": "Bearer worker-secret"},
                        json={"kind": "goose", "runtime": "goose", "host": "session-test"},
                    )
                    self.assertEqual(registration.status_code, 200)
                    body = registration.json()
                    shared = await client.post(
                        "/v1/agents/heartbeat",
                        headers={"Authorization": "Bearer worker-secret"},
                        json={"urn": body["urn"]},
                    )
                    self.assertEqual(shared.status_code, 403)
                    bound = await client.post(
                        "/v1/agents/heartbeat",
                        headers={"Authorization": f"Bearer {body['agent_token']}"},
                        json={"urn": body["urn"]},
                    )
                    self.assertEqual(bound.status_code, 200)
                    impersonation = await client.post(
                        "/v1/agents/heartbeat",
                        headers={"Authorization": f"Bearer {body['agent_token']}"},
                        json={"urn": "urn:agent:goose:victim:other"},
                    )
                    self.assertEqual(impersonation.status_code, 403)
        finally:
            agent_bus.HIVE_BUS_TOKENS = original_workers
            agent_bus.HIVE_BUS_ADMIN_TOKENS = original_admins

    async def test_health_reports_missing_auth_as_degraded(self):
        original_workers = agent_bus.HIVE_BUS_TOKENS
        original_admins = agent_bus.HIVE_BUS_ADMIN_TOKENS
        agent_bus.HIVE_BUS_TOKENS = frozenset()
        agent_bus.HIVE_BUS_ADMIN_TOKENS = frozenset()
        task = asyncio.create_task(asyncio.sleep(30))
        agent_bus.app.state.reaper_task = task
        agent_bus.app.state.reaper_last_success = agent_bus.time.time()
        agent_bus.app.state.reaper_consecutive_failures = 0
        try:
            response = await agent_bus.health()
            self.assertEqual(response.status_code, 503)
            self.assertEqual(json.loads(response.body)["checks"]["authentication"], "not_configured")
        finally:
            agent_bus.HIVE_BUS_TOKENS = original_workers
            agent_bus.HIVE_BUS_ADMIN_TOKENS = original_admins
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_worker_token_cannot_toggle_knemon_bypass(self):
        original_workers = agent_bus.HIVE_BUS_TOKENS
        original_admins = agent_bus.HIVE_BUS_ADMIN_TOKENS
        agent_bus.HIVE_BUS_TOKENS = frozenset({"worker-secret"})
        agent_bus.HIVE_BUS_ADMIN_TOKENS = frozenset({"admin-secret"})
        try:
            transport = httpx.ASGITransport(app=agent_bus.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.post(
                    "/v1/knemon/bypass",
                    headers={"Authorization": "Bearer worker-secret"},
                    json={"enabled": True},
                )
            self.assertEqual(response.status_code, 403)
        finally:
            agent_bus.HIVE_BUS_TOKENS = original_workers
            agent_bus.HIVE_BUS_ADMIN_TOKENS = original_admins

    def test_oracle_dedup_violation_uses_real_index_name(self):
        self.assertTrue(agent_bus._is_dedup_violation(
            RuntimeError("ORA-00001: unique constraint (MNEMOS.JOBS_ACTIVE_DEDUP_UQ) violated")
        ))
        self.assertFalse(agent_bus._is_dedup_violation(
            RuntimeError("ORA-00001: unique constraint (MNEMOS.OTHER_UQ) violated")
        ))

    async def test_fresh_sqlite_schema_accepts_deduplicated_job_submission(self):
        async with agent_bus.lifespan(agent_bus.app):
            now = agent_bus.time.time()
            submitter = "urn:agent:human:test:session"
            async with agent_bus.connect_db() as db:
                await db.execute(
                    "INSERT INTO agents (urn, kind, host, session_id, started_at, "
                    "last_heartbeat, status, runtime) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (submitter, "human", "test", "session", now, now, "online", "human"),
                )
                await db.commit()

            created = await agent_bus.create_job(
                agent_bus.JobCreate(
                    submitter_urn=submitter,
                    kind="test:defect-f2",
                    description="fresh schema dedup submission",
                )
            )
            async with agent_bus.connect_db() as db:
                async with db.execute(
                    "SELECT dedup_hash FROM jobs WHERE id=?", (created["id"],)
                ) as cur:
                    row = await cur.fetchone()
                async with db.execute("PRAGMA index_list(jobs)") as cur:
                    indexes = {index[1] for index in await cur.fetchall()}
            self.assertEqual(len(row[0]), 64)
            self.assertIn("jobs_active_dedup_uq", indexes)

    async def test_health_returns_503_when_database_probe_fails(self):
        original = agent_bus.connect_db

        @asynccontextmanager
        async def broken_connection():
            raise OSError("database unavailable")
            yield

        task = asyncio.create_task(asyncio.sleep(30))
        agent_bus.app.state.reaper_task = task
        agent_bus.app.state.reaper_last_success = agent_bus.time.time()
        agent_bus.app.state.reaper_consecutive_failures = 0
        agent_bus.connect_db = broken_connection
        try:
            response = await agent_bus.health()
            self.assertEqual(response.status_code, 503)
            payload = json.loads(response.body)
            self.assertEqual(payload["status"], "degraded")
            self.assertTrue(payload["checks"]["database"].startswith("error:"))
        finally:
            agent_bus.connect_db = original
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_system_claim_filter_only_claims_structured_execution_jobs(self):
        async with agent_bus.lifespan(agent_bus.app):
            now = agent_bus.time.time()
            submitter = "urn:agent:human:execution-test:session"
            worker = "urn:agent:system:execution-test:session"
            async with agent_bus.connect_db() as db:
                await db.execute(
                    "INSERT OR REPLACE INTO agents (urn, kind, host, session_id, started_at, "
                    "last_heartbeat, status, runtime, capabilities, provider, cost_tier) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (submitter, "human", "execution-test", "session", now, now,
                     "online", "human", None, "local", "A"),
                )
                await db.execute(
                    "INSERT OR REPLACE INTO agents (urn, kind, host, session_id, started_at, "
                    "last_heartbeat, status, runtime, capabilities, provider, cost_tier) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (worker, "system", "execution-test", "session", now, now,
                     "online", "system", json.dumps(["local-process"]), "local", "A"),
                )
                await db.commit()

            ordinary = await agent_bus.create_job(agent_bus.JobCreate(
                submitter_urn=submitter,
                kind="build",
                description="ordinary higher-priority build",
                priority=100,
                idempotency_key="f8-ordinary",
            ))
            async with agent_bus.connect_db() as db:
                await db.executemany(
                    "INSERT INTO jobs (id, submitter_urn, kind, description, priority, "
                    "status, started_at, max_cost_tier) VALUES (?, ?, 'build', ?, 100, 'queued', ?, 'B')",
                    [
                        (f"f8-ordinary-{index}", submitter, f"ordinary backlog {index}", now + index / 1000)
                        for index in range(105)
                    ],
                )
                await db.commit()
            executable = await agent_bus.create_job(agent_bus.JobCreate(
                submitter_urn=submitter,
                kind="build",
                description="structured lower-priority build",
                priority=1,
                execution=agent_bus.LocalProcessExecution(
                    argv=["make", "release"], cwd="project-a", timeout_seconds=60,
                ),
                idempotency_key="f8-executable",
            ))

            claimed = await agent_bus.dequeue_next_job(
                worker, execution_type="local-process", job_kinds="build,compile,render"
            )
            self.assertEqual(claimed["id"], executable["id"])
            self.assertEqual(claimed["execution"]["argv"], ["make", "release"])
            self.assertIn("hard:local-process", claimed["required_capabilities"])
            async with agent_bus.connect_db() as db:
                async with db.execute(
                    "SELECT status, execution_spec FROM jobs WHERE id=?", (ordinary["id"],)
                ) as cur:
                    ordinary_row = await cur.fetchone()
            self.assertEqual(ordinary_row[0], "queued")
            self.assertIsNone(ordinary_row[1])

    def test_execution_spec_participates_in_cache_identity(self):
        base = agent_bus.cache_key_for("build", "release", "A", ["local-process"])
        first = agent_bus.cache_key_for(
            "build", "release", "A", ["local-process"],
            {"type": "local-process", "argv": ["make", "one"], "cwd": ".", "env": {}, "timeout_seconds": 60},
        )
        second = agent_bus.cache_key_for(
            "build", "release", "A", ["local-process"],
            {"type": "local-process", "argv": ["make", "two"], "cwd": ".", "env": {}, "timeout_seconds": 60},
        )
        self.assertNotEqual(base, first)
        self.assertNotEqual(first, second)

    async def test_receipt_reconciliation_uses_terminal_bookkeeping(self):
        async with agent_bus.lifespan(agent_bus.app):
            now = agent_bus.time.time()
            urn = "urn:agent:system:receipt-test:session"
            job_id = "receipt-reconciliation-regression"
            receipt = {
                "exit_code": 0,
                "_execution_receipt": {
                    "version": 1, "execution_id": job_id,
                    "completed": True, "terminal_status": "done",
                },
            }
            async with agent_bus.connect_db() as db:
                await db.execute(
                    "INSERT OR REPLACE INTO agents (urn, kind, host, session_id, started_at, "
                    "last_heartbeat, status, runtime) VALUES (?, 'system', 'receipt-test', "
                    "'session', ?, ?, 'online', 'system')",
                    (urn, now - 1000, now - 1000),
                )
                await db.execute(
                    "INSERT OR REPLACE INTO jobs (id, submitter_urn, kind, description, priority, "
                    "status, claimed_by, claimed_at, started_at, result, max_cost_tier, "
                    "claimed_model, claimed_provider, tokens_in, tokens_out) "
                    "VALUES (?, ?, 'build', 'receipt job', 0, "
                    "'running', ?, ?, ?, ?, 'A', 'local', 'local', 11, 7)",
                    (job_id, "urn:agent:human:test:receipt", urn, now - 1000,
                     now - 1000, json.dumps(receipt)),
                )
                await db.commit()

            original_sleep = agent_bus.asyncio.sleep
            sleep_calls = 0

            async def one_iteration(_seconds):
                nonlocal sleep_calls
                sleep_calls += 1
                if sleep_calls > 1:
                    raise asyncio.CancelledError

            agent_bus.asyncio.sleep = one_iteration
            try:
                with self.assertRaises(asyncio.CancelledError):
                    await agent_bus.reaper_task(agent_bus.app)
            finally:
                agent_bus.asyncio.sleep = original_sleep

            async with agent_bus.connect_db() as db:
                async with db.execute("SELECT status FROM jobs WHERE id=?", (job_id,)) as cur:
                    self.assertEqual((await cur.fetchone())[0], "done")
                async with db.execute(
                    "SELECT success_count, total_tokens_in, total_tokens_out "
                    "FROM worker_kind_stats WHERE urn=? AND kind='build'", (urn,)
                ) as cur:
                    self.assertEqual(await cur.fetchone(), (1, 11, 7))
                async with db.execute(
                    "SELECT new_status FROM job_audit_log WHERE job_id=? ORDER BY id DESC LIMIT 1",
                    (job_id,),
                ) as cur:
                    self.assertEqual((await cur.fetchone())[0], "done")
                async with db.execute(
                    "SELECT COUNT(*) FROM events WHERE kind='job.done' AND payload LIKE ?",
                    (f'%{job_id}%',),
                ) as cur:
                    self.assertGreaterEqual((await cur.fetchone())[0], 1)
                async with db.execute(
                    "SELECT COUNT(*) FROM hive_cache WHERE source_job_id=?", (job_id,)
                ) as cur:
                    self.assertEqual((await cur.fetchone())[0], 1)


class WorkerAndRestoreDefectTests(unittest.TestCase):
    def test_system_watcher_terminal_patch_retries_and_persists_receipt(self):
        watcher = _load_script("system_watcher_finalize_test", "system_watcher.py")
        calls = []

        def flaky_patch(job_id, status, result=None):
            calls.append((status, result))
            return status == "running"

        watcher.patch_job = flaky_patch
        watcher._sleep_while_running = lambda _seconds: None
        self.assertFalse(watcher.finalize_job("job-receipt", "done", {"exit_code": 0}, attempts=2))
        receipts = [result["_execution_receipt"] for status, result in calls if status == "running"]
        self.assertTrue(receipts)
        self.assertEqual(receipts[0]["terminal_status"], "done")
        self.assertTrue(agent_bus._completed_execution_receipt(json.dumps(calls[-1][1])))

    def test_default_executor_does_not_allow_container_runtimes(self):
        watcher = _load_script("system_watcher_default_allowlist_test", "system_watcher.py")
        self.assertNotIn("docker", watcher.ALLOWED_EXECUTABLES)
        self.assertNotIn("podman", watcher.ALLOWED_EXECUTABLES)
    def test_system_watcher_load_gate_fails_closed(self):
        watcher = _load_script("system_watcher_load_test", "system_watcher.py")
        watcher.LOAD_THRESHOLD = 2.0
        watcher.ALLOW_CLAIM_WITHOUT_LOAD = False
        self.assertFalse(watcher.load_allows_claim({})[0])
        self.assertFalse(watcher.load_allows_claim({"load_1min": 2.1})[0])
        self.assertTrue(watcher.load_allows_claim({"load_1min": 2.0})[0])

    def test_system_watcher_executes_and_renews_lease(self):
        watcher = _load_script("system_watcher_execution_test", "system_watcher.py")
        executable = Path(os.sys.executable).name
        watcher.JOB_WORK_ROOT = str(Path.cwd())
        watcher.ALLOWED_EXECUTABLES = frozenset({executable})
        watcher.ALLOWED_JOB_ENV = frozenset()
        watcher.JOB_LEASE_RENEW_INTERVAL = 0.01
        watcher.HEARTBEAT_INTERVAL = 60
        watcher._running = True
        patches = []

        def fake_patch(job_id, status, result=None):
            patches.append((job_id, status, result))
            return True

        watcher.patch_job = fake_patch
        status, result = watcher.execute_job({
            "id": "job-f8",
            "execution": {
                "type": "local-process",
                "argv": [executable, "-c", "import time; print('ok'); time.sleep(0.35)"],
                "cwd": ".",
                "env": {},
                "timeout_seconds": 5,
            },
        })
        self.assertEqual(status, "done")
        self.assertEqual(result["exit_code"], 0)
        self.assertIn("ok", result["stdout"])
        self.assertGreaterEqual(sum(status == "running" for _, status, _ in patches), 2)

    def test_system_watcher_rejects_path_escape_and_unlisted_executable(self):
        watcher = _load_script("system_watcher_validation_test", "system_watcher.py")
        watcher.JOB_WORK_ROOT = str(Path.cwd())
        watcher.ALLOWED_EXECUTABLES = frozenset({"make"})
        with self.assertRaisesRegex(ValueError, "not allowed"):
            watcher._validated_process({
                "execution": {"type": "local-process", "argv": ["sh", "-c", "true"], "cwd": "."}
            })
        executable = Path(os.sys.executable).name
        watcher.ALLOWED_EXECUTABLES = frozenset({executable})
        watcher.PASSTHROUGH_ENV = frozenset({"PATH"})
        os.environ["HIVE_BUS_TOKEN"] = "must-not-reach-child"
        _, _, child_env, _ = watcher._validated_process({
            "execution": {"type": "local-process", "argv": [executable], "cwd": "."}
        })
        self.assertNotIn("HIVE_BUS_TOKEN", child_env)
        os.environ.pop("HIVE_BUS_TOKEN", None)
        with self.assertRaisesRegex(ValueError, "outside JOB_WORK_ROOT"):
            watcher._validated_process({
                "execution": {"type": "local-process", "argv": [executable], "cwd": "../"}
            })

    def test_zeroclaw_worker_advertises_hard_codex_capability(self):
        worker = _load_script("zeroclaw_worker_for_test", "zeroclaw_wss_worker_v2.py")
        captured = {}

        def fake_http(method, path, body=None, timeout=0):
            captured.update(body or {})
            return 200, {"urn": "urn:agent:zeroclaw:test:session"}

        worker.http = fake_http
        self.assertIsNotNone(worker.register())
        self.assertIn("codex", captured["capabilities"])

    def test_auto_restore_uses_admin_requeue_results(self):
        restore = _load_script("hive_auto_restore_for_test", "hive-auto-restore.py")
        captured = {}

        def fake_http(method, path, body=None, timeout=0, *, admin=False):
            captured.update(method=method, path=path, body=body, admin=admin)
            return 200, {"ok": ["job-1"], "not_found": [], "skipped_done": ["job-2"]}

        restore._http = fake_http
        restored, error = restore.restore_jobs(["job-1", "job-2"])
        self.assertEqual(restored, {"job-1": "ok", "job-2": "skipped_done"})
        self.assertEqual(error, "")
        self.assertEqual(captured["method"], "POST")
        self.assertEqual(captured["path"], "/v1/admin/jobs/requeue")
        self.assertTrue(captured["admin"])

    def test_auto_restore_fetch_failure_is_not_an_empty_success(self):
        restore = _load_script("hive_auto_restore_fetch_failure_test", "hive-auto-restore.py")
        restore._http = lambda *args, **kwargs: (401, {"detail": "invalid token"})
        with self.assertRaisesRegex(RuntimeError, "http=401"):
            restore.fetch_failed_jobs()
        restore._http = lambda *args, **kwargs: (200, {})
        with self.assertRaisesRegex(RuntimeError, "missing field 'jobs'"):
            restore.fetch_failed_jobs()

    def test_auto_restore_requeue_failure_returns_nonzero(self):
        restore = _load_script("hive_auto_restore_requeue_failure_test", "hive-auto-restore.py")
        restore.load_state = lambda: {}
        restore.save_state = lambda state: None
        restore.fetch_failed_jobs = lambda: [{
            "id": "job-restore", "kind": "build", "ended_at": 0,
        }]
        restore.restore_jobs = lambda job_ids: ({}, "http=401")
        self.assertEqual(restore.main(), 1)

    def test_zeroclaw_orchestrator_retag_is_validated_and_checked(self):
        worker = _load_script("zeroclaw_worker_retag_test", "zeroclaw_wss_worker_v2.py")
        worker.route_chain = lambda *args: ["unused"]
        worker._resolve_workspace = lambda kind, desc: (None, "no_workspace_for_kind")
        worker.patch_job = lambda *args, **kwargs: True
        calls = []

        def fake_http(method, path, body=None, timeout=0):
            calls.append((method, path, body))
            return 200, {"ok": True}

        worker.http = fake_http
        worker.process_job("urn:agent:zeroclaw:test:s", {
            "id": "job-retag", "kind": "unknown-kind", "description": "x",
            "eligible_kinds": ["zeroclaw", "human"],
        })
        self.assertEqual(calls[-1][2], {"status": "queued", "eligible_kinds": ["human"]})

    def test_zeroclaw_requires_referenceable_mnemos_provenance(self):
        worker = _load_script("zeroclaw_worker_mnemos_test", "zeroclaw_wss_worker_v2.py")
        worker.MNEMOS_TOKEN = ""
        with self.assertRaisesRegex(SystemExit, "MNEMOS_TOKEN is required"):
            worker.main()
        worker.MNEMOS_TOKEN = "configured"
        worker._mnemos_http = lambda *args, **kwargs: (201, {})
        result = worker._record_job_to_mnemos(
            {"id": "job-provenance", "kind": "analysis"},
            {"full_response": "answer"},
            "done",
        )
        self.assertIsNone(result)


class McpBridgeDefectTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bridge = _load_script("mcp_bridge_for_test", "mcp_bridge.py")

    async def asyncTearDown(self):
        await self.bridge.client.aclose()

    async def test_canonical_mcp_v2_handlers_are_registered(self):
        self.assertIn("tools/list", self.bridge.server._request_handlers)
        self.assertIn("tools/call", self.bridge.server._request_handlers)
        result = await self.bridge._on_list_tools(None, None)
        self.assertIn("mnemos.memory_create", {tool.name for tool in result.tools})

    async def test_mnemos_tools_use_versioned_routes_and_preserve_tags(self):
        calls = []

        async def fake_mnemos(method, path, **kwargs):
            calls.append((method, path, kwargs))
            return {"ok": True}

        self.bridge._mnemos = fake_mnemos
        await self.bridge.handle_call_tool("mnemos.memory_search", {"query": "q"})
        await self.bridge.handle_call_tool(
            "mnemos.memory_create", {"content": "c", "category": "facts", "tags": ["release"]}
        )
        await self.bridge.handle_call_tool("mnemos.memory_get", {"id": "mem_1"})
        self.assertEqual([call[1] for call in calls], [
            "/v1/memories/search", "/v1/memories", "/v1/memories/mem_1",
        ])
        self.assertEqual(calls[1][2]["json"]["metadata"]["tags"], ["release"])
        self.assertNotIn("tags", calls[1][2]["json"])

    async def test_job_create_uses_bus_admin_credential(self):
        calls = []

        async def fake_hive(method, path, **kwargs):
            calls.append((method, path, kwargs))
            return {"id": "job-1"}

        self.bridge._hive = fake_hive
        result = await self.bridge.handle_call_tool(
            "hive.job_create", {"submitter_urn": "urn:test", "kind": "test"}
        )
        self.assertFalse(result.is_error)
        self.assertTrue(calls[0][2]["admin"])

    async def test_backend_and_unknown_tool_fail_at_protocol_level(self):
        async def unavailable(*_args, **_kwargs):
            raise RuntimeError("backend unavailable")

        self.bridge._mnemos = unavailable
        backend = await self.bridge.handle_call_tool("mnemos.memory_search", {"query": "q"})
        unknown = await self.bridge.handle_call_tool("not.a.tool", {})
        self.assertTrue(backend.is_error)
        self.assertTrue(unknown.is_error)

    async def test_bridge_requires_inbound_auth_and_separates_write_tools(self):
        transport = httpx.ASGITransport(app=self.bridge.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            unavailable = await client.get("/sse")
        self.assertEqual(unavailable.status_code, 503)

        self.bridge.MCP_BRIDGE_TOKENS = frozenset({"read-secret"})
        self.bridge.MCP_BRIDGE_WRITE_TOKENS = frozenset({"write-secret"})
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            rejected = await client.get("/missing", headers={"Authorization": "Bearer bad"})
            accepted = await client.get(
                "/missing", headers={"Authorization": "Bearer read-secret"}
            )
        self.assertEqual(rejected.status_code, 401)
        self.assertEqual(accepted.status_code, 404)

        token = self.bridge._MCP_TIER.set("read")
        try:
            params = type("Params", (), {"name": "mnemos.memory_create", "arguments": {}})()
            result = await self.bridge._on_call_tool(None, params)
            self.assertTrue(result.is_error)
        finally:
            self.bridge._MCP_TIER.reset(token)

    def test_bridge_defaults_to_loopback(self):
        source = Path("mcp_bridge.py").read_text()
        self.assertIn('os.environ.get("MCP_BRIDGE_HOST", "127.0.0.1")', source)


class DashboardDefectTests(unittest.TestCase):
    def test_dashboard_authenticates_fetch_and_sse_without_query_tokens(self):
        source = Path("dashboard.html").read_text()
        self.assertIn("opts.headers.Authorization='Bearer '+_busToken", source)
        self.assertIn("headers.Authorization='Bearer '+_busToken", source)
        self.assertIn("sessionStorage", source)
        self.assertNotIn("new EventSource", source)
        self.assertNotIn("/v1/events?token=", source)


class MigrationDefectTests(unittest.TestCase):
    def test_factory_constructs_sqlite_repository(self):
        repository = get_hive_repository("sqlite", db_path=_DB_FILE.name)
        self.assertIsInstance(repository, SqliteHiveRepository)

    def test_oracle_factory_import_does_not_require_aiosqlite(self):
        script = """
import builtins
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == 'aiosqlite':
        raise ModuleNotFoundError("blocked", name='aiosqlite')
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded
from hive.persistence.factory import get_hive_repository
repo = get_hive_repository('oracle', db_path='unused', dsn='oracle://u:p@host/service')
assert repo.connect_dsn == 'host:1521/service'
"""
        completed = subprocess.run(
            [os.sys.executable, "-c", script], cwd=Path.cwd(),
            text=True, capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_oracle_agent_bus_import_does_not_require_aiosqlite(self):
        script = """
import builtins, os, sys, types
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == 'aiosqlite':
        raise ModuleNotFoundError("blocked", name='aiosqlite')
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded
driver = types.ModuleType('oracledb')
class Probe:
    def close(self): pass
driver.connect = lambda **kwargs: Probe()
sys.modules['oracledb'] = driver
os.environ['HIVE_BACKEND'] = 'oracle'
os.environ['HIVE_BUS_TOKEN'] = 'worker'
import agent_bus
assert agent_bus.HIVE_BACKEND == 'oracle'
"""
        completed = subprocess.run(
            [os.sys.executable, "-c", script], cwd=Path.cwd(),
            text=True, capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_db2_backend_fails_explicitly_without_an_adapter(self):
        with self.assertRaisesRegex(RuntimeError, "no DB2 persistence adapter"):
            get_hive_repository("db2", db_path=_DB_FILE.name)

    def test_sqlite_populated_rebuild_preserves_data_and_base_indexes(self):
        with tempfile.NamedTemporaryFile(prefix="hive-legacy-", suffix=".db") as db_file:
            raw = sqlite3.connect(db_file.name)
            raw.execute(
                "CREATE TABLE jobs (id TEXT PRIMARY KEY, submitter_urn TEXT NOT NULL, "
                "parent_job_id TEXT, kind TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 0, "
                "status TEXT NOT NULL CHECK(status IN ('queued','claimed','running','done','failed','cancelled')), "
                "claimed_by TEXT, started_at REAL NOT NULL)"
            )
            raw.execute(
                "CREATE INDEX idx_jobs_queue ON jobs(status, priority DESC, started_at ASC)"
            )
            raw.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, priority, status, started_at) "
                "VALUES ('legacy-job', 'urn:test', 'test', 7, 'queued', 1.0)"
            )
            raw.commit()
            raw.close()

            old_path, old_repo = agent_bus.DB_PATH, agent_bus._HIVE_REPO
            agent_bus.DB_PATH, agent_bus._HIVE_REPO = db_file.name, None
            try:
                asyncio.run(self._run_lifespan_once())
                raw = sqlite3.connect(db_file.name)
                self.assertEqual(raw.execute(
                    "SELECT id, priority FROM jobs"
                ).fetchall(), [("legacy-job", 7)])
                indexes = {row[1] for row in raw.execute("PRAGMA index_list(jobs)")}
                self.assertTrue({
                    "idx_jobs_status", "idx_jobs_submitter", "idx_jobs_claimed_by",
                    "idx_jobs_parent", "idx_jobs_queue",
                }.issubset(indexes))
                sql = raw.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs'"
                ).fetchone()[0]
                self.assertIn("dead-letter", sql)
                raw.close()
            finally:
                agent_bus.DB_PATH, agent_bus._HIVE_REPO = old_path, old_repo

    async def _run_lifespan_once(self):
        async with agent_bus.lifespan(agent_bus.app):
            pass

    def test_sqlite_interrupted_rebuild_recovers_stranded_table(self):
        with tempfile.NamedTemporaryFile(prefix="hive-interrupted-", suffix=".db") as db_file:
            raw = sqlite3.connect(db_file.name)
            raw.execute(
                "CREATE TABLE jobs__pre_deadletter_migration ("
                "id TEXT PRIMARY KEY, submitter_urn TEXT NOT NULL, parent_job_id TEXT, kind TEXT NOT NULL, "
                "priority INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL "
                "CHECK(status IN ('queued','claimed','running','done','failed','cancelled')), "
                "claimed_by TEXT, started_at REAL NOT NULL)"
            )
            raw.execute(
                "INSERT INTO jobs__pre_deadletter_migration "
                "(id, submitter_urn, kind, priority, status, started_at) VALUES "
                "('stranded-job', 'urn:test', 'test', 1, 'queued', 1.0)"
            )
            raw.commit()
            raw.close()

            old_path, old_repo = agent_bus.DB_PATH, agent_bus._HIVE_REPO
            agent_bus.DB_PATH, agent_bus._HIVE_REPO = db_file.name, None
            try:
                asyncio.run(self._run_lifespan_once())
                raw = sqlite3.connect(db_file.name)
                self.assertEqual(
                    raw.execute("SELECT id FROM jobs").fetchall(), [("stranded-job",)]
                )
                self.assertIsNone(raw.execute(
                    "SELECT name FROM sqlite_master WHERE name='jobs__pre_deadletter_migration'"
                ).fetchone())
                raw.close()
            finally:
                agent_bus.DB_PATH, agent_bus._HIVE_REPO = old_path, old_repo

    def test_zeroclaw_has_no_source_baked_mnemos_token(self):
        source = Path("zeroclaw_wss_worker_v2.py").read_text()
        self.assertIn('MNEMOS_TOKEN = os.environ.get("MNEMOS_TOKEN", "")', source)
        self.assertNotRegex(source, r'["\'][0-9a-fA-F]{64}["\']')

    def test_oracle_dedup_migration_targets_hive_jobs(self):
        sql = Path("migrations/2026-06-03-jobs-dedup.sql").read_text()
        oracle_section = sql.split("SQLite (dev / fallback)", 1)[0]
        self.assertIn("ALTER TABLE HIVE_JOBS", oracle_section)
        self.assertIn("ON HIVE_JOBS", oracle_section)
        self.assertNotIn("ALTER TABLE jobs", oracle_section)

    def test_oracle_required_migration_is_fatal(self):
        source = Path("agent_bus.py").read_text()
        handler = source.split("oracle required schema migration failed", 1)[1].split(
            "# Idempotent index", 1
        )[0]
        self.assertIn("raise", handler)
        self.assertNotIn("non-fatal", handler)

    def test_oracle_startup_converges_dedup_column_and_index(self):
        source = Path("agent_bus.py").read_text()
        oracle_block = source.split('if HIVE_BACKEND == "oracle" and', 1)[1].split(
            "# Idempotent index", 1
        )[0]
        self.assertIn("column_name = 'DEDUP_HASH'", oracle_block)
        self.assertIn("ALTER TABLE HIVE_JOBS ADD (dedup_hash VARCHAR2(64))", oracle_block)
        self.assertIn("index_name = 'JOBS_ACTIVE_DEDUP_UQ'", oracle_block)
        self.assertIn("CREATE UNIQUE INDEX jobs_active_dedup_uq ON HIVE_JOBS", oracle_block)

    def test_oracle_startup_closes_async_cursor_synchronously_in_finally(self):
        source = Path("agent_bus.py").read_text()
        oracle_block = source.split('if HIVE_BACKEND == "oracle" and', 1)[1].split(
            "# Idempotent index", 1
        )[0]
        self.assertIn("finally:\n                    rcur.close()", oracle_block)
        self.assertNotIn("await rcur.close()", oracle_block)

    def test_system_execution_migration_targets_canonical_oracle_table(self):
        sql = Path("migrations/2026-08-10-system-job-execution.sql").read_text()
        self.assertIn("ALTER TABLE HIVE_JOBS", sql)
        self.assertIn("column_name = 'EXECUTION_SPEC'", sql)
        self.assertNotIn("ALTER TABLE jobs ADD", sql.split("SQLite", 1)[0])


if __name__ == "__main__":
    unittest.main()
