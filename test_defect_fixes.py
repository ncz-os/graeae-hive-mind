"""Regression tests for deployability defects found in adversarial review."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest


_DB_FILE = tempfile.NamedTemporaryFile(prefix="hive-defects-", suffix=".db", delete=False)
_DB_FILE.close()
os.environ["AGENT_BUS_DB"] = _DB_FILE.name

import agent_bus
from hive.persistence.factory import get_hive_repository
from hive.persistence.sqlite import SqliteHiveRepository


def _load_script(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class AgentBusDefectTests(unittest.IsolatedAsyncioTestCase):
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


class WorkerAndRestoreDefectTests(unittest.TestCase):
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


class MigrationDefectTests(unittest.TestCase):
    def test_factory_constructs_sqlite_repository(self):
        repository = get_hive_repository("sqlite", db_path=_DB_FILE.name)
        self.assertIsInstance(repository, SqliteHiveRepository)

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

    def test_system_execution_migration_targets_canonical_oracle_table(self):
        sql = Path("migrations/2026-08-10-system-job-execution.sql").read_text()
        self.assertIn("ALTER TABLE HIVE_JOBS", sql)
        self.assertIn("column_name = 'EXECUTION_SPEC'", sql)
        self.assertNotIn("ALTER TABLE jobs ADD", sql.split("SQLite", 1)[0])


if __name__ == "__main__":
    unittest.main()
