"""A failed or unrecorded job must leave its checkout available for rescue."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ["WORKER_KIND"] = "zeroclaw"

import hive_worker as worker

JOB_ID = "00000000-0000-0000-0000-000000000001"
JOB = {"id": JOB_ID, "repo": "graeae-hive", "description": "workspace retention test"}


class _Driver:
    def __init__(self, code):
        self.returncode = code

    def communicate(self, timeout):
        return "driver output", None


class WorkspaceRetentionTests(unittest.TestCase):
    def test_existing_workspace_is_moved_aside_intact_and_job_proceeds(self):
        """2026-10-03: failing on an existing workspace killed every retry on
        that host (23 jobs). The old checkout is now moved to .rescued/ intact
        and the job clones fresh."""
        with tempfile.TemporaryDirectory() as root:
            checkout = Path(root, JOB_ID)
            checkout.mkdir()
            (checkout / "unfinished.patch").write_text("rescue me")
            cloned = []

            def fake_run(cmd, **kwargs):
                if cmd[:2] == ["git", "clone"]:
                    cloned.append(cmd)
                    raise subprocess.CalledProcessError(128, cmd)  # stop after the clone
                return subprocess.CompletedProcess(cmd, 128, "", "not a git repo")

            with (
                patch.object(worker, "WORKSPACE_ROOT", root),
                patch.object(worker, "patch_job", return_value=True),
                patch.object(worker.subprocess, "run", side_effect=fake_run),
            ):
                worker._run_job_inner(JOB)
            rescued = list(Path(root, ".rescued").glob(f"{JOB_ID}-*"))
            self.assertEqual(len(rescued), 1, "old checkout must be moved, not erased")
            self.assertEqual((rescued[0] / "unfinished.patch").read_text(), "rescue me")
            self.assertTrue(cloned, "job must proceed to a fresh clone")

    def test_rescue_move_failure_keeps_old_fail_and_preserve(self):
        with tempfile.TemporaryDirectory() as root:
            checkout = Path(root, JOB_ID)
            checkout.mkdir()
            with (
                patch.object(worker, "WORKSPACE_ROOT", root),
                patch.object(worker, "_rescue_workspace", return_value=None),
                patch.object(worker, "patch_job", return_value=True) as recorded,
                patch.object(worker.subprocess, "run") as clone,
            ):
                worker._run_job_inner(JOB)
            clone.assert_not_called()
            self.assertEqual(recorded.call_args.args[1], "failed")
            self.assertEqual(recorded.call_args.args[2]["workspace_preserved"], str(checkout))

    def _run_driver(self, exit_code, recorded):
        with tempfile.TemporaryDirectory() as root:
            checkout = Path(root, JOB_ID)

            def clone(cmd, **kwargs):
                Path(cmd[-1]).mkdir()

            with (
                patch.object(worker, "WORKSPACE_ROOT", root),
                patch.object(worker, "KEEP_WORKSPACE", False),
                patch.object(worker.subprocess, "check_output", return_value="a" * 40),
                patch.object(worker, "completion_evidence", return_value={}),
                patch.object(worker.subprocess, "run", side_effect=clone),
                patch.object(
                    worker.subprocess, "Popen", return_value=_Driver(exit_code)
                ),
                patch.object(
                    worker, "patch_job", return_value=recorded
                ) as patch_status,
            ):
                worker._run_job_inner(JOB)
            return checkout.exists(), patch_status.call_args.args[2]

    def test_failed_driver_preserves_workspace(self):
        exists, result = self._run_driver(1, True)
        self.assertTrue(exists)
        self.assertIn("workspace_preserved", result)

    def test_partial_clone_failure_preserves_workspace(self):
        with tempfile.TemporaryDirectory() as root:
            checkout = Path(root, JOB_ID)

            def partial_clone(cmd, **kwargs):
                checkout.mkdir()
                (checkout / "partial").write_text("diagnostic")
                raise subprocess.CalledProcessError(128, cmd)

            with (
                patch.object(worker, "WORKSPACE_ROOT", root),
                patch.object(worker.subprocess, "run", side_effect=partial_clone),
                patch.object(worker, "patch_job", return_value=True) as recorded,
            ):
                worker._run_job_inner(JOB)
            self.assertEqual((checkout / "partial").read_text(), "diagnostic")
            self.assertEqual(
                recorded.call_args.args[2]["workspace_preserved"], str(checkout)
            )

    def test_success_without_delivery_proof_always_preserves_workspace(self):
        exists, _ = self._run_driver(0, True)
        self.assertTrue(exists)
        exists, _ = self._run_driver(0, False)
        self.assertTrue(exists)


if __name__ == "__main__":
    unittest.main()
