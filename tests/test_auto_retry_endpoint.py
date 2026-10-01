import json
import sqlite3
import time

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client_db(tmp_path, monkeypatch):
    import agent_bus as bus

    db_path = str(tmp_path / "hive.db")
    monkeypatch.setattr(bus, "DB_PATH", db_path)
    monkeypatch.setattr(bus, "HIVE_BACKEND", "sqlite")
    monkeypatch.setattr(bus, "_HIVE_REPO", None)
    monkeypatch.setattr(bus, "HIVE_BUS_TOKENS", frozenset({"worker-fixture"}))
    monkeypatch.setattr(bus, "HIVE_BUS_ADMIN_TOKENS", frozenset({"admin-fixture"}))
    with TestClient(
        bus.app, headers={"Authorization": "Bearer admin-fixture"}
    ) as client:
        yield client, db_path


def insert(db_path, **changes):
    values = {
        "id": "retry-fixture",
        "submitter_urn": "fixture",
        "kind": "build:zeroclaw",
        "status": "failed",
        "started_at": time.time(),
        "max_retries": 3,
        "retry_count": 0,
        "result": json.dumps({"retryable": True}),
    }
    values.update(changes)
    with sqlite3.connect(db_path) as db:
        db.execute(
            f"INSERT INTO jobs ({','.join(values)}) VALUES ({','.join('?' for _ in values)})",
            tuple(values.values()),
        )


def row(db_path):
    with sqlite3.connect(db_path) as db:
        return db.execute(
            "SELECT status,retry_count,claimed_by,result,max_retries FROM jobs WHERE id='retry-fixture'"
        ).fetchone()


@pytest.mark.parametrize(
    "changes",
    [
        {"max_retries": 0},
        {"retry_count": 3},
        {"retry_count": -1},
        {"max_retries": "malformed"},
        {"result": json.dumps({"retryable": True, "held": "operator hold"})},
        {"result": json.dumps({"retryable": True, "superseded_by": "other"})},
        {"result": json.dumps({"retryable": True, "reason": "manual review"})},
        {"result": json.dumps({"retryable": "true"})},
        {"status": "running"},
        {"status": "cancelled"},
        {"kind": "research", "result": "malformed JSON"},
        {"kind": "research", "result": json.dumps({"retryable": False})},
    ],
)
def test_rejects_current_policy_without_mutation(client_db, changes):
    client, db_path = client_db
    insert(db_path, **changes)
    before = row(db_path)
    assert client.post("/v1/jobs/retry-fixture/auto-retry").status_code == 409
    assert row(db_path) == before


@pytest.mark.parametrize("kind", ["build:zeroclaw", "research"])
def test_retry_consumes_budget_once_and_keeps_evidence(client_db, kind):
    client, db_path = client_db
    insert(db_path, kind=kind, claimed_by="old-worker")
    before = row(db_path)
    response = client.post("/v1/jobs/retry-fixture/auto-retry")
    assert response.status_code == 200
    assert response.json() == {"ok": True, "id": "retry-fixture", "retry_count": 1}
    assert row(db_path) == ("queued", 1, None, before[3], 3)
    assert client.post("/v1/jobs/retry-fixture/auto-retry").status_code == 409
    assert row(db_path)[1] == 1


def test_missing_and_general_patch_guard(client_db):
    client, db_path = client_db
    assert client.post("/v1/jobs/missing/auto-retry").status_code == 404
    insert(db_path)
    assert (
        client.patch("/v1/jobs/retry-fixture", json={"status": "queued"}).status_code
        == 409
    )


def test_retry_endpoint_requires_admin(client_db):
    client, db_path = client_db
    insert(db_path)
    for headers, status in [
        ({"Authorization": "Bearer worker-fixture"}, 403),
        ({"Authorization": ""}, 401),
    ]:
        assert (
            client.post(
                "/v1/jobs/retry-fixture/auto-retry", headers=headers
            ).status_code
            == status
        )
    assert row(db_path)[0:2] == ("failed", 0)


def test_list_keeps_projection_and_exposes_retry_fields(client_db):
    client, db_path = client_db
    insert(db_path, max_retries=0, retry_count=2)
    for include in ("true", "false"):
        response = client.get("/v1/jobs", params={"include_result": include})
        assert response.status_code == 200
        job = response.json()["jobs"][0]
        assert (job["max_retries"], job["retry_count"]) == (0, 2)
        assert ("result" in job) == (include == "true")
