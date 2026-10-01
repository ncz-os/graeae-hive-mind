"""Real Git fixtures reproduce dirty, unpushed and falsely successful jobs."""

import os
import subprocess
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

            self.assertFalse(worker.completion_ready(evidence()))
            Path(repo, "source.py").write_text("value = 2\n")
            self.assertFalse(worker.completion_ready(evidence()))
            git("commit", "-am", "fix")
            self.assertFalse(worker.completion_ready(evidence()))
            git("push")
            proof = evidence()
            self.assertTrue(worker.completion_ready(proof))
            self.assertEqual(proof["files_changed"], ["source.py"])
            Path(repo, "stranded.txt").write_text("uncommitted work")
            self.assertFalse(worker.completion_ready(evidence()))


if __name__ == "__main__":
    unittest.main()
