import importlib.util
import unittest
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
        self.mock_restore = MagicMock(return_value=({"job-1": "ok"}, ""))
        self.mock_fetch = MagicMock()
        self.mock_load = MagicMock(return_value={})
        self.mock_save = MagicMock()
        self.mock_log = MagicMock()

        self.patchers = [
            patch.object(hive_auto_restore, "restore_jobs", self.mock_restore),
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
        self.mock_restore.assert_called_once_with(["job-1"])

    def test_eligible_build_allowed(self):
        job = self._make_job()
        self._run_main_with_jobs([job])
        self.mock_restore.assert_called_once_with(["job-1"])

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
