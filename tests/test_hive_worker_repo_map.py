"""Job `repo` names resolve only against the known-repo map."""

import os
import unittest

os.environ["WORKER_KIND"] = "zeroclaw"

import hive_worker as worker


class RepoMapTests(unittest.TestCase):
    def test_gitlab_repo_name_resolves_to_the_hive_clone(self):
        self.assertEqual(
            worker.resolve_repo_url({"repo": "graeae-hive-mind"}),
            worker.REPO_MAP["graeae-hive"],
        )

    def test_unknown_or_url_repos_are_never_resolved(self):
        self.assertIsNone(worker.resolve_repo_url({"repo": "no-such-repo"}))
        self.assertIsNone(worker.resolve_repo_url({"repo": "https://evil.example/x.git"}))
        self.assertIsNone(worker.resolve_repo_url({}))


if __name__ == "__main__":
    unittest.main()
