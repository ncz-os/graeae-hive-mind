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

    def test_allowlisted_url_forms_resolve_to_the_map_url(self):
        canon = worker.REPO_MAP["riskyeats"]
        for form in (
            "https://gitlab.com/argonautsystems/riskyeats.git",
            "https://gitlab.com/argonautsystems/riskyeats",
            "git@gitlab.com:argonautsystems/riskyeats.git",
            "ssh://root@192.168.207.101/mnt/datapool/git/riskyeats.git",
            "jasonperlow@192.168.207.101:/mnt/datapool/git/riskyeats.git",
        ):
            self.assertEqual(worker.resolve_repo_url({"repo": form}), canon, form)
        self.assertEqual(
            worker.resolve_repo_url({"repo": "https://gitlab.com/ncz-os/graeae-hive-mind.git"}),
            worker.REPO_MAP["graeae-hive"],
        )

    def test_unknown_or_url_repos_are_never_resolved(self):
        self.assertIsNone(worker.resolve_repo_url({"repo": "no-such-repo"}))
        self.assertIsNone(worker.resolve_repo_url({"repo": "https://evil.example/x.git"}))
        self.assertIsNone(worker.resolve_repo_url({}))
        # allowlisted host but unknown repo name, and look-alike hosts
        self.assertIsNone(worker.resolve_repo_url({"repo": "https://gitlab.com/ncz-os/not-in-map.git"}))
        self.assertIsNone(worker.resolve_repo_url({"repo": "https://gitlab.com.evil.example/ncz-os/riskyeats.git"}))
        self.assertIsNone(worker.resolve_repo_url({"repo": "https://gitlab.com/someone-else/riskyeats.git"}))
        self.assertIsNone(worker.resolve_repo_url({"repo": "ssh://root@10.0.0.5/mnt/datapool/git/riskyeats.git"}))


if __name__ == "__main__":
    unittest.main()
