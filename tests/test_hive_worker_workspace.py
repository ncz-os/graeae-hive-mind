"""A failed or unrecorded job must leave its checkout available for rescue."""

import os
import shlex
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ["WORKER_KIND"] = "zeroclaw"

import hive_worker as worker
from queue_logic import match

JOB_ID = "00000000-0000-0000-0000-000000000001"
JOB = {"id": JOB_ID, "repo": "graeae-hive", "description": "workspace retention test"}


class _Driver:
    def __init__(self, code):
        self.returncode = code

    def communicate(self, timeout):
        return "driver output", None


class WorkspaceRetentionTests(unittest.TestCase):
    def test_registration_advertises_verified_delivery_version(self):
        with (
            patch.object(worker, "_find_driver", return_value="/usr/bin/zoder"),
            patch.object(worker.os.path, "isfile", return_value=True),
            patch.object(worker.os, "access", return_value=True),
            patch.object(
                worker, "_driver_supports_verified_delivery", return_value=True
            ),
            patch.object(
                worker, "_http", return_value=(200, {"urn": "urn:test"})
            ) as http,
        ):
            worker.register()
        body = http.call_args.args[2]
        self.assertIn("verified-git-delivery", body["capabilities"])
        self.assertEqual(body["version"], "hive_worker.py/2")
        self.assertEqual(body["instance"], "hive-worker-zeroclaw-1")
        required = ["hard:verified-git-delivery"]
        self.assertFalse(match(required, ["python", "git", "zeroclaw"]).eligible)
        self.assertTrue(match(required, body["capabilities"]).eligible)

    def test_old_driver_refuses_registration_without_verified_contract(self):
        with (
            patch.object(worker, "_find_driver", return_value="/usr/bin/zoder"),
            patch.object(worker.os.path, "isfile", return_value=True),
            patch.object(worker.os, "access", return_value=True),
            patch.object(
                worker, "_driver_supports_verified_delivery", return_value=False
            ),
            patch.object(worker, "_http") as http,
        ):
            with self.assertRaises(SystemExit):
                worker.register()
            http.assert_not_called()

    def test_zoder_help_probe_requires_delivery_flags(self):
        result = subprocess.CompletedProcess(
            ["zoder", "loop", "--help"],
            0,
            "loop --require-approval --approve POLICY --check COMMAND",
            "",
        )
        with patch.object(worker.subprocess, "run", return_value=result) as run:
            self.assertTrue(worker._driver_supports_verified_delivery("/usr/bin/zoder"))
        run.assert_called_once()

    def test_zoder_help_probe_rejects_old_flags(self):
        result = subprocess.CompletedProcess(["zoder", "loop", "--help"], 0, "loop", "")
        with patch.object(worker.subprocess, "run", return_value=result):
            self.assertFalse(
                worker._driver_supports_verified_delivery("/usr/bin/zoder")
            )

    def test_strict_route_capability_requires_exact_driver_flag(self):
        with (
            patch.object(worker, "_find_driver", return_value="/usr/bin/zoder"),
            patch.object(worker.os.path, "isfile", return_value=True),
            patch.object(worker.os, "access", return_value=True),
            patch.object(
                worker, "_driver_supports_verified_delivery", return_value=True
            ),
            patch.object(worker, "_driver_supports_strict_routes", return_value=True),
            patch.object(
                worker, "_http", return_value=(200, {"urn": "urn:test"})
            ) as http,
        ):
            worker.register()
        self.assertIn("strict-rescue-routes", http.call_args.args[2]["capabilities"])
        self.assertTrue(
            match(
                ["hard:strict-rescue-routes"],
                http.call_args.args[2]["capabilities"],
            ).eligible
        )

    def test_strict_rescue_pins_allowed_routes_and_delivery_gate(self):
        strict_job = dict(JOB, required_capabilities=["hard:strict-rescue-routes"])
        with tempfile.TemporaryDirectory(prefix="hive strict route ") as root:

            def clone(cmd, **kwargs):
                if cmd[1] == "clone":
                    Path(cmd[-1]).mkdir()

            with (
                patch.object(worker, "WORKSPACE_ROOT", root),
                patch.object(
                    worker, "_driver_supports_strict_routes", return_value=True
                ),
                patch.object(worker.subprocess, "check_output", return_value="a" * 40),
                patch.object(worker.subprocess, "run", side_effect=clone),
                patch.object(
                    worker.subprocess, "Popen", return_value=_Driver(1)
                ) as driver,
                patch.object(worker, "patch_job", return_value=True),
            ):
                worker._run_job_inner(strict_job)
            command = driver.call_args.args[0]
            self.assertIn("--require-approval", command)
            self.assertIn("--check", command)
            self.assertEqual(command[command.index("-m") + 1], "nemotron35")
            self.assertEqual(command[command.index("--reviewer") + 1], "qwen38")
            self.assertEqual(
                command[command.index("--allowed-routes") + 1],
                worker.RESCUE_ALLOWED_ROUTES,
            )
            self.assertNotIn("minimax", " ".join(command).lower())

    def test_missing_driver_never_advertises_worker_capability(self):
        with (
            patch.object(worker, "_find_driver", return_value="/does/not/exist"),
            patch.object(worker, "_http") as http,
        ):
            with self.assertRaises(SystemExit):
                worker.register()
            http.assert_not_called()

    def test_existing_workspace_is_never_erased_on_reclaim(self):
        with tempfile.TemporaryDirectory(prefix="hive workspace '") as root:
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
        with tempfile.TemporaryDirectory(prefix="hive workspace '") as root:
            checkout = Path(root, JOB_ID)

            def clone(cmd, **kwargs):
                if cmd[1] == "clone":
                    Path(cmd[-1]).mkdir()

            with (
                patch.object(worker, "WORKSPACE_ROOT", root),
                patch.object(worker, "KEEP_WORKSPACE", False),
                patch.dict(os.environ, {"GIT_FLEET_SSH_PASSWORD": "test-not-a-secret"}),
                patch.object(worker.subprocess, "check_output", return_value="a" * 40),
                patch.object(worker, "completion_evidence", return_value={}),
                patch.object(worker.subprocess, "run", side_effect=clone) as git_runs,
                patch.object(
                    worker.subprocess, "Popen", return_value=_Driver(exit_code)
                ) as driver,
                patch.object(
                    worker, "patch_job", return_value=recorded
                ) as patch_status,
            ):
                worker._run_job_inner(JOB)
                self.assertEqual(
                    [call.args[0][4:6] for call in git_runs.call_args_list[1:]],
                    [["--local", "user.name"], ["--local", "user.email"]],
                )
                command = driver.call_args.args[0]
                driver_env = driver.call_args.kwargs["env"]
                self.assertEqual(driver_env["SSHPASS"], "test-not-a-secret")
                self.assertIn("sshpass -e", driver_env["GIT_SSH_COMMAND"])
                self.assertNotIn("test-not-a-secret", driver_env["GIT_SSH_COMMAND"])
                if worker.WORKER_KIND == "zeroclaw":
                    self.assertIn("--require-approval", command)
                    self.assertEqual(command[command.index("--approve") + 1], "all")
                    self.assertEqual(
                        shlex.split(command[command.index("--check") + 1]),
                        [
                            sys.executable,
                            os.path.abspath(worker.__file__),
                            "--verify-delivery",
                            str(checkout),
                            "a" * 40,
                        ],
                    )
                    self.assertIn("checkout a dedicated branch", command[3])
                    self.assertIn("explicit job-specific restriction", command[3])
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
