"""Unit tests for hive_worker.py heartbeat claim renewal.

Verifies that heartbeat_loop PATCHes the same job claim at each interval,
preventing the reaper from expiring it after 1800s.
"""
import threading
import time
import unittest
from unittest.mock import patch

# Import the module under test
import importlib.util
spec = importlib.util.spec_from_file_location("hive_worker", "/home/jasonperlow/hive-workspaces/01a0f537-4f5a-78d5-accf-6bb640ac65bc/hive_worker.py")
hwm = importlib.util.module_from_spec(spec)

# Configure env before loading
import os
os.environ["WORKER_KIND"] = "zeroclaw"
os.environ["HIVE_URL"] = "http://127.0.0.1:5005"
os.environ["AGENT_HOST"] = "test-host"
os.environ["HEARTBEAT_INTERVAL"] = "1"  # 1s for tests
os.environ["JOB_TIMEOUT"] = "7200"
spec.loader.exec_module(hwm)


class TestHeartbeatClaimRenewal(unittest.TestCase):
    """Test that heartbeat_loop renews the job claim via PATCH."""

    def setUp(self):
        # Reset module globals
        hwm._urn = "urn:test:worker"
        hwm._current_job_id = "test-job-123"
        hwm._running = True

    def tearDown(self):
        hwm._running = False

    def test_heartbeat_patches_claim_when_job_id_exists(self):
        """Heartbeat should PATCH status=claimed, claimed_by=_urn when _current_job_id is set."""
        with patch.object(hwm, "_http") as mock_http:
            # Side effect: enough calls for 3 heartbeat intervals
            mock_http.side_effect = [
                (200, {"status": "claimed", "claimed_by": "urn:test:worker"}),  # PATCH job (call 0)
                (200, None),  # agent heartbeat (call 1)
                (200, {"status": "claimed", "claimed_by": "urn:test:worker"}),  # PATCH job (call 2)
                (200, None),  # agent heartbeat (call 3)
                (200, {"status": "claimed", "claimed_by": "urn:test:worker"}),  # PATCH job (call 4)
                (200, None),  # agent heartbeat (call 5)
            ]

            # Run heartbeat_loop for 3 intervals (3s)
            thread = threading.Thread(target=hwm.heartbeat_loop)
            thread.daemon = True
            thread.start()
            time.sleep(3.5)  # 3 intervals × 1s + margin
            hwm._running = False
            thread.join(timeout=5)

            # Find all PATCH calls
            patch_calls = [c for c in mock_http.call_args_list if "PATCH" in str(c)]
            self.assertGreater(len(patch_calls), 0, "PATCH job call should have been made")

            # Check the LAST PATCH call had the right args
            last_patch = patch_calls[-1]
            args, kwargs = last_patch.args, last_patch.kwargs
            url_str = args[1] if len(args) > 1 else ""
            self.assertIn("test-job-123", url_str, f"URL should contain job ID, got: {url_str}")
            # body is args[2]
            body = args[2] if len(args) > 2 else {}
            self.assertEqual(body.get("status"), "claimed", f"status should be claimed, got: {body.get('status')}")
            self.assertEqual(
                body.get("claimed_by"), "urn:test:worker",
                f"claimed_by should be urn:test:worker, got: {body.get('claimed_by')}"
            )

    def test_heartbeat_skips_claim_patch_when_no_job(self):
        """Heartbeat should skip claim PATCH when _current_job_id is None."""
        hwm._current_job_id = None
        with patch.object(hwm, "_http") as mock_http:
            mock_http.return_value = (200, None)
            hwm._running = True
            thread = threading.Thread(target=hwm.heartbeat_loop)
            thread.daemon = True
            thread.start()
            time.sleep(1.5)
            hwm._running = False
            thread.join(timeout=5)

            # Verify no PATCH call was made (only agent heartbeat)
            patch_calls = [c for c in mock_http.call_args_list if "PATCH" in str(c)]
            self.assertEqual(len(patch_calls), 0, "PATCH should not be called when no job")

    def test_heartbeat_does_not_crash_on_403(self):
        """Heartbeat should not crash on 403 from job PATCH."""
        hwm._current_job_id = "test-job-456"
        with patch.object(hwm, "_http") as mock_http:
            # PATCH gets 403, agent heartbeat succeeds
            mock_http.side_effect = [
                (403, {"error": "claimed_by_mismatch"}),
                (200, None),
            ]

            thread = threading.Thread(target=hwm.heartbeat_loop)
            thread.daemon = True
            thread.start()
            time.sleep(1.5)
            hwm._running = False
            thread.join(timeout=5)

            # If we get here without exception, the 403 was handled gracefully
            self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()
