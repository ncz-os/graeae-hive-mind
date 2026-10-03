"""A superseded failure can be retired, but no terminal job can be reopened.

2026-10-03: the orchestrator could not retire failed jobs whose work had been
superseded ("invalid status transition 'failed' -> 'cancelled'"), so they sat
in the failure counts forever. failed -> cancelled now mirrors the existing
done -> cancelled exception; every other exit from a terminal status stays a
409.
"""

import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import agent_bus as bus


class JobTransitionTests(unittest.TestCase):
    def _client(self, root):
        return (
            patch.object(bus, "DB_PATH", str(Path(root, "hive.db"))),
            patch.object(bus, "HIVE_BACKEND", "sqlite"),
            patch.object(bus, "_HIVE_REPO", None),
        )

    def _insert(self, jid, status):
        with closing(sqlite3.connect(bus.DB_PATH)) as db, db:
            db.execute(
                "INSERT INTO jobs (id,submitter_urn,kind,repo,status,claimed_by,started_at,max_retries,retry_count) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    jid,
                    "orchestrator",
                    "build:zeroclaw",
                    "example",
                    status,
                    "worker",
                    time.time(),
                    0,
                    0,
                ),
            )

    def _status(self, jid):
        with closing(sqlite3.connect(bus.DB_PATH)) as db, db:
            return db.execute("SELECT status FROM jobs WHERE id=?", (jid,)).fetchone()[
                0
            ]

    def test_failed_job_can_be_retired_but_not_reopened(self):
        with tempfile.TemporaryDirectory() as root:
            p1, p2, p3 = self._client(root)
            with p1, p2, p3, TestClient(bus.app) as client:
                self._insert("superseded", "failed")
                self._insert("reopen", "failed")
                self._insert("gone", "cancelled")

                ok = client.patch(
                    "/v1/jobs/superseded",
                    json={
                        "status": "cancelled",
                        "claimed_by": "worker",
                        "result": {"reason": "superseded"},
                    },
                )
                self.assertEqual(ok.status_code, 200, ok.text)
                self.assertEqual(self._status("superseded"), "cancelled")

                for jid, status in (("reopen", "queued"), ("gone", "failed")):
                    bad = client.patch(
                        f"/v1/jobs/{jid}",
                        json={"status": status, "claimed_by": "worker"},
                    )
                    self.assertEqual(bad.status_code, 409, bad.text)
                self.assertEqual(self._status("reopen"), "failed")
                self.assertEqual(self._status("gone"), "cancelled")


if __name__ == "__main__":
    unittest.main()
