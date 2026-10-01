"""Real Git fixtures reproduce dirty, unpushed and falsely successful jobs."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

os.environ["WORKER_KIND"] = "zeroclaw"
import hive_worker as worker


class CompletionEvidenceTests(unittest.TestCase):
    def test_actual_commits_and_remote_delivery(self):
        with tempfile.TemporaryDirectory() as root:
            remote, repo = Path(root, "remote.git"), Path(root, "repo")

            def run(*args):
                return subprocess.check_output(
                    args, text=True, stderr=subprocess.DEVNULL
                ).strip()

            run("git", "init", "--bare", str(remote))
            run("git", "clone", str(remote), str(repo))

            def git(*args):
                return run("git", "-C", str(repo), *args)

            git("config", "user.name", "Test")
            git("config", "user.email", "test@example.com")
            Path(repo, "source.py").write_text("value = 1\n")
            git("add", "source.py")
            git("commit", "-m", "base")
            git("push", "-u", "origin", "HEAD")
            base = git("rev-parse", "HEAD")

            def evidence():
                return worker.completion_evidence(str(repo), base, os.environ.copy())

            def cli(expected):
                env = os.environ.copy()
                env.pop("WORKER_KIND", None)
                result = subprocess.run(
                    [
                        sys.executable,
                        worker.__file__,
                        "--verify-delivery",
                        str(repo),
                        base,
                    ],
                    env=env,
                    check=False,
                    text=True,
                    capture_output=True,
                    timeout=10,
                )
                self.assertEqual(result.returncode, expected, result.stderr)
                report = json.loads(result.stdout)
                self.assertEqual(report["delivery_ready"], expected == 0)
                self.assertLess(len(result.stdout), 32000)
                self.assertNotIn(str(remote), result.stdout)

            self.assertFalse(worker.completion_ready(evidence()))
            cli(1)
            Path(repo, "source.py").write_text("value = 2\n")
            self.assertFalse(worker.completion_ready(evidence()))
            git("commit", "-am", "fix")
            self.assertFalse(worker.completion_ready(evidence()))
            cli(1)
            # The actual canary created/pushed a branch without checking it out.
            git("branch", "delivery")
            git("push", "origin", "delivery")
            cli(1)
            git("checkout", "delivery")
            cli(1)  # no configured upstream, despite remote branch existing
            git("push", "-u", "origin", "HEAD")
            cli(0)
            proof = evidence()
            self.assertTrue(worker.completion_ready(proof))
            self.assertEqual(proof["files_changed"], ["source.py"])
            Path(repo, "stranded.txt").write_text("uncommitted work")
            self.assertFalse(worker.completion_ready(evidence()))
            cli(1)

    def test_clone_identity_defaults_and_explicit_override(self):
        with tempfile.TemporaryDirectory() as root:
            subprocess.run(["git", "init", "-q", root], check=True)
            env = os.environ.copy()
            env.pop("HIVE_GIT_AUTHOR_NAME", None)
            env.pop("HIVE_GIT_AUTHOR_EMAIL", None)
            worker.configure_git_identity(root, env)

            def configured(key):
                return subprocess.check_output(
                    ["git", "-C", root, "config", "--local", key], text=True
                ).strip()

            self.assertEqual(configured("user.name"), "Jason Perlow")
            self.assertEqual(configured("user.email"), "jperlow@gmail.com")
            env.update(
                HIVE_GIT_AUTHOR_NAME="Configured Author",
                HIVE_GIT_AUTHOR_EMAIL="author@example.org",
            )
            worker.configure_git_identity(root, env)
            self.assertEqual(configured("user.name"), "Configured Author")
            self.assertEqual(configured("user.email"), "author@example.org")


if __name__ == "__main__":
    unittest.main()
