import importlib.util
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import MagicMock, patch

# Load module from file with hyphen
spec = importlib.util.spec_from_file_location(
    "hive_auto_restore", Path(__file__).resolve().parents[1] / "hive-auto-restore.py"
)
hive_auto_restore = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hive_auto_restore)


class TestAutoRestorePolicy(unittest.TestCase):
    def setUp(self):
        self.mock_restore = MagicMock(return_value=(True, ""))
        self.mock_fetch = MagicMock()
        self.mock_load = MagicMock(return_value={})
        self.mock_save = MagicMock()
        self.mock_log = MagicMock()

        self.patchers = [
            patch.object(hive_auto_restore, "restore_job", self.mock_restore),
            patch.object(hive_auto_restore, "fetch_failed_jobs", self.mock_fetch),
            patch.object(hive_auto_restore, "load_state", self.mock_load),
            patch.object(hive_auto_restore, "save_state", self.mock_save),
            patch.object(hive_auto_restore, "log", self.mock_log),
        ]
        for p in self.patchers:
            p.start()

    def tearDown(self):
        for p in self.patchers:
            p.stop()

    def _run_main_with_jobs(self, jobs):
        self.mock_fetch.return_value = jobs
        hive_auto_restore.main()

    def _make_job(self, **kwargs):
        base = {
            "id": "job-1",
            "kind": "build:zeroclaw",
            "ended_at": 1000000,
            "status": "failed",
            "max_retries": 5,
            "retry_count": 0,
            "result": {"retryable": True},
        }
        base.update(kwargs)
        return base

    def test_zero_max_retries_blocked(self):
        job = self._make_job(max_retries=0)
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()

    def test_missing_max_retries_blocked(self):
        job = self._make_job()
        del job["max_retries"]
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()

    def test_malformed_max_retries_blocked(self):
        job = self._make_job(max_retries="5")
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()

    def test_negative_max_retries_blocked(self):
        job = self._make_job(max_retries=-1)
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()

    def test_bool_max_retries_blocked(self):
        job = self._make_job(max_retries=True)
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()

    def test_malformed_retry_count_blocked(self):
        job = self._make_job(retry_count="0")
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()

    def test_exhausted_server_count_blocked(self):
        job = self._make_job(max_retries=5, retry_count=5)
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()

    def test_exhausted_local_count_blocked(self):
        job = self._make_job()
        self.mock_load.return_value = {"job-1": {"count": 5, "next_at": 0}}
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()

    def test_held_result_blocked_despite_retryable(self):
        job = self._make_job(result={"retryable": True, "held": True})
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()

    def test_held_string_blocked(self):
        job = self._make_job(result={"retryable": True, "held": "manual"})
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()

    def test_superseded_by_string_blocked(self):
        job = self._make_job(result={"retryable": True, "superseded_by": "job-99"})
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()

    def test_manual_review_reason_blocked(self):
        job = self._make_job(result={"retryable": True, "reason": "manual-review"})
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()

    def test_needs_review_status_blocked(self):
        job = self._make_job(result={"retryable": True, "status": "needs_review"})
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()

    def test_eligible_nonbuild_allowed(self):
        job = self._make_job(kind="research", result={"error": "timeout"})
        self._run_main_with_jobs([job])
        self.mock_restore.assert_called_once_with("job-1")

    def test_eligible_build_allowed(self):
        job = self._make_job()
        self._run_main_with_jobs([job])
        self.mock_restore.assert_called_once_with("job-1")

    def test_build_retryable_string_blocked(self):
        job = self._make_job(result={"retryable": "True"})
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()

    def test_explicit_retryable_false_blocked(self):
        job = self._make_job(kind="research", result={"retryable": False})
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()

    def test_non_failed_status_blocked(self):
        job = self._make_job(status="queued")
        self._run_main_with_jobs([job])
        self.mock_restore.assert_not_called()


class TestAutoRestoreApi(unittest.TestCase):
    def test_api_returns_exact_stored_values(self):
        import sqlite3

        from fastapi.testclient import TestClient

        import agent_bus as bus

        with (
            tempfile.TemporaryDirectory() as root,
            patch.object(bus, "DB_PATH", str(Path(root, "hive.db"))),
            patch.object(bus, "HIVE_BACKEND", "sqlite"),
            patch.object(bus, "_HIVE_REPO", None),
            TestClient(bus.app) as client,
        ):
            with closing(sqlite3.connect(bus.DB_PATH)) as db, db:
                db.execute(
                    "INSERT INTO jobs (id,submitter_urn,kind,repo,status,claimed_by,started_at,max_retries,retry_count) VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        "job-api",
                        "orchestrator",
                        "build:zeroclaw",
                        "example",
                        "failed",
                        "worker",
                        time.time(),
                        0,
                        2,
                    ),
                )

            # GET list
            resp_list = client.get("/v1/jobs?status=failed")
            self.assertEqual(resp_list.status_code, 200)
            jobs = resp_list.json().get("jobs", [])
            job = next((j for j in jobs if j["id"] == "job-api"), None)
            self.assertIsNotNone(job)
            self.assertEqual(job["max_retries"], 0)
            self.assertEqual(job["retry_count"], 2)

            # GET single
            resp_single = client.get("/v1/jobs/job-api")
            self.assertEqual(resp_single.status_code, 200)
            job_single = resp_single.json()
            self.assertEqual(job_single["max_retries"], 0)
            self.assertEqual(job_single["retry_count"], 2)


if __name__ == "__main__":
    unittest.main()
