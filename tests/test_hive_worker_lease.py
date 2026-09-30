"""Claim renewal behavior during a long blocking driver run."""

import os
import subprocess
import sys
import unittest
from unittest.mock import patch

os.environ["WORKER_KIND"] = "zeroclaw"

import hive_worker as worker


class ClaimRenewalTests(unittest.TestCase):
    def setUp(self):
        self.old_state = (
            worker._running,
            worker._urn,
            worker._current_job_id,
            worker._claim_renewal_blocked_id,
        )
        worker._running = True
        worker._urn = "urn:agent:zeroclaw:test:123"
        worker._current_job_id = "00000000-0000-0000-0000-000000000001"
        worker._claim_renewal_blocked_id = None

    def tearDown(self):
        (
            worker._running,
            worker._urn,
            worker._current_job_id,
            worker._claim_renewal_blocked_id,
        ) = self.old_state

    def test_claim_survives_more_than_thirty_minutes(self):
        clock = 0
        lease_expires_at = 1800
        renewals = 0

        def sleep(seconds):
            nonlocal clock
            clock += seconds
            if clock >= 1860:
                worker._running = False

        def http(method, path, body, timeout):
            nonlocal lease_expires_at, renewals
            if method == "POST":
                self.assertEqual(path, "/v1/agents/heartbeat")
                return 200, None
            self.assertEqual(method, "PATCH")
            self.assertEqual(path, f"/v1/jobs/{worker._current_job_id}")
            self.assertEqual(body, {"status": "claimed", "claimed_by": worker._urn})
            if clock >= lease_expires_at:
                return 409, None  # the bus reaped the claim
            renewals += 1
            lease_expires_at = clock + 1800
            return 200, None

        with (
            patch.object(worker.time, "sleep", sleep),
            patch.object(worker, "_http", http),
        ):
            worker.heartbeat_loop()

        self.assertGreaterEqual(clock, 1860)
        self.assertGreater(lease_expires_at, clock)
        self.assertGreater(renewals, 90)

    def test_rejected_claim_is_not_renewed_again(self):
        clock = 0
        patch_count = 0

        def sleep(seconds):
            nonlocal clock
            clock += seconds
            if clock >= 80:
                worker._running = False

        def http(method, path, body, timeout):
            nonlocal patch_count
            if method == "POST":
                return 200, None
            patch_count += 1
            return 409, {"detail": "job cancelled"}

        with (
            patch.object(worker.time, "sleep", sleep),
            patch.object(worker, "_http", http),
        ):
            worker.heartbeat_loop()

        self.assertEqual(patch_count, 1)
        self.assertEqual(worker._claim_renewal_blocked_id, worker._current_job_id)

    def test_gitops_route_overrides_stale_environment_file(self):
        env = os.environ.copy()
        env.update({
            "WORKER_KIND": "zeroclaw",
            "ZODER_AGENT": "minimax",
            "ZODER_REVIEWER": "MiniMax-M3",
            "HIVE_ZODER_AGENT": "coder",
            "HIVE_ZODER_REVIEWER": "qwen38",
        })
        proc = subprocess.run(
            [sys.executable, "-c", "import hive_worker; print(hive_worker.ZODER_AGENT, hive_worker.ZODER_REVIEWER)"],
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.stdout.strip(), "coder qwen38")


if __name__ == "__main__":
    unittest.main()
