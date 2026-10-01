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
    def test_missing_driver_never_advertises_worker_capability(self):
        with (
            patch.object(worker, "_find_driver", return_value="/does/not/exist"),
            patch.object(worker, "_http") as http,
        ):
            with self.assertRaises(SystemExit):
                worker.register()
            http.assert_not_called()

    def test_existing_workspace_is_never_erased_on_reclaim(self):
        with tempfile.TemporaryDirectory() as root:
            checkout = Path(root, JOB_ID)
            checkout.mkdir()
            (checkout / "unfinished.patch").write_text("rescue me")
            with (
                patch.object(worker, "WORKSPACE_ROOT", root),
                patch.object(worker, "patch_job", return_value=True) as recorded,
                patch.object(worker.subprocess, "run") as clone,
            ):
                worker._run_job_inner(JOB)
            self.assertEqual((checkout / "unfinished.patch").read_text(), "rescue me")
            clone.assert_not_called()
            self.assertEqual(recorded.call_args.args[1], "failed")
            self.assertEqual(
                recorded.call_args.args[2]["workspace_preserved"], str(checkout)
            )

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
                ) as driver,
                patch.object(
                    worker, "patch_job", return_value=recorded
                ) as patch_status,
            ):
                worker._run_job_inner(JOB)
                command = driver.call_args.args[0]
                if worker.WORKER_KIND == "zeroclaw":
                    self.assertIn("--require-approval", command)
                    self.assertEqual(command[command.index("--approve") + 1], "all")
                else:
                    self.assertEqual(
                        command[command.index("--sandbox") + 1], worker.CODEX_SANDBOX
                    )
                    self.assertIn('approval_policy="never"', command)
            return checkout.exists(), patch_status.call_args.args[2]

    def test_codex_uses_explicit_fleet_sandbox_policy(self):
        with patch.object(worker, "WORKER_KIND", "codex"):
            exists, result = self._run_driver(0, True)
        self.assertTrue(exists)
        self.assertIn("workspace_preserved", result)

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
