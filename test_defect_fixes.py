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


class WorkerAndRestoreDefectTests(unittest.TestCase):
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
        handler = source.split("oracle dead-letter migration failed", 1)[1].split(
            "# Idempotent index", 1
        )[0]
        self.assertIn("raise", handler)
        self.assertNotIn("non-fatal", handler)


if __name__ == "__main__":
    unittest.main()
