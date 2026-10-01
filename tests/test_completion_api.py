"""The real update route must derive delivery requirements from the job row."""

import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import agent_bus as bus


class CompletionApiTests(unittest.TestCase):
    def test_missing_or_forged_kind_cannot_bypass_contract(self):
        with (
            tempfile.TemporaryDirectory() as root,
            patch.object(bus, "DB_PATH", str(Path(root, "hive.db"))),
            patch.object(bus, "HIVE_BACKEND", "sqlite"),
            patch.object(bus, "_HIVE_REPO", None),
            TestClient(bus.app) as client,
        ):
            import sqlite3

            with closing(sqlite3.connect(bus.DB_PATH)) as db, db:
                for jid in ("missing", "forged", "valid"):
                    db.execute(
                        "INSERT INTO jobs (id,submitter_urn,kind,repo,status,claimed_by,started_at,max_retries,retry_count) VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            jid,
                            "orchestrator",
                            "build:zeroclaw",
                            "example",
                            "claimed",
                            "worker",
                            time.time(),
                            0,
                            0,
                        ),
                    )
            for jid, result in [
                ("missing", {"exit_code": 0}),
                ("forged", {"exit_code": 0, "kind": "research", "commits": []}),
            ]:
                response = client.patch(
                    f"/v1/jobs/{jid}",
                    json={
                        "status": "done",
                        "claimed_by": "worker",
                        "result": result,
                    },
                )
                self.assertEqual(response.status_code, 409, response.text)
                with closing(sqlite3.connect(bus.DB_PATH)) as db, db:
                    self.assertEqual(
                        db.execute(
                            "SELECT status FROM jobs WHERE id=?", (jid,)
                        ).fetchone()[0],
                        "needs-review",
                    )
            sha = "a" * 40
            proof = {
                "head_sha": sha,
                "commits": [sha],
                "files_changed": ["a.py"],
                "worktree_clean": True,
                "remote_verified": True,
                "remote_sha": sha,
            }
            response = client.patch(
                "/v1/jobs/valid",
                json={"status": "done", "claimed_by": "worker", "result": proof},
            )
            self.assertEqual(response.status_code, 200, response.text)


if __name__ == "__main__":
    unittest.main()
