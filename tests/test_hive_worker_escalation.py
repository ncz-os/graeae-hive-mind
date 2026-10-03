"""Retried zeroclaw jobs climb the escalation ladder (operator 2026-10-03)."""

import os
import time

import pytest

os.environ["WORKER_KIND"] = "zeroclaw"

import hive_worker as worker

# 2026-10-03 is a Saturday (always off-peak); 2026-10-05 Monday 07:00Z is peak.
SAT = time.strptime("2026-10-03 12:00", "%Y-%m-%d %H:%M")
MON_PEAK = time.strptime("2026-10-05 07:00", "%Y-%m-%d %H:%M")
MON_OFF = time.strptime("2026-10-05 12:00", "%Y-%m-%d %H:%M")


@pytest.fixture(autouse=True)
def ladder(monkeypatch):
    monkeypatch.setattr(worker, "ZODER_LADDER", ["coder", "flash", "minimax"])
    monkeypatch.setattr(worker, "ZODER_AGENT", "coder")


def job_with_flash_hash(even: bool, **extra):
    """Find a job id whose hash picks the deepseek (even) or minimax side."""
    import hashlib

    for i in range(100):
        jid = f"job-{i}"
        if (hashlib.sha256(jid.encode()).digest()[0] % 2 == 0) == even:
            return {"id": jid, **extra}
    raise AssertionError("no id found")


def test_first_attempt_uses_host_coder():
    assert worker.escalation_agent({"id": "a", "retry_count": 0}, SAT) == "coder"


def test_retry_moves_to_flash_then_minimax():
    job = job_with_flash_hash(False)
    assert worker.escalation_agent({**job, "retry_count": 1}, SAT) == "minimax_flash"
    assert worker.escalation_agent({**job, "retry_count": 2}, SAT) == "minimax"
    assert worker.escalation_agent({**job, "retry_count": 9}, SAT) == "minimax"


def test_flash_rung_balances_to_deepseek_only_off_peak():
    job = job_with_flash_hash(True, retry_count=1)
    assert worker.escalation_agent(job, SAT) == "deepseek"
    assert worker.escalation_agent(job, MON_OFF) == "deepseek"
    assert worker.escalation_agent(job, MON_PEAK) == "minimax_flash"


def test_deepseek_peak_windows_use_utc_weekday():
    assert worker.deepseek_peak(MON_PEAK)
    assert not worker.deepseek_peak(MON_OFF)
    assert not worker.deepseek_peak(SAT)
    assert worker.deepseek_peak(time.strptime("2026-10-06 01:30", "%Y-%m-%d %H:%M"))
    assert not worker.deepseek_peak(time.strptime("2026-10-06 04:00", "%Y-%m-%d %H:%M"))


def test_route_capability_starts_higher_on_the_ladder():
    job = {"id": "a", "retry_count": 0, "required_capabilities": ["route:minimax"]}
    assert worker.escalation_agent(job, SAT) == "minimax"


def test_unknown_route_capability_runs_first_then_climbs_past_coder():
    job = job_with_flash_hash(
        False, retry_count=0, required_capabilities=["route:special"]
    )
    assert worker.escalation_agent(job, SAT) == "special"
    assert worker.escalation_agent({**job, "retry_count": 1}, SAT) == "minimax_flash"
    assert worker.escalation_agent({**job, "retry_count": 2}, SAT) == "minimax"


def test_garbage_retry_count_is_attempt_zero():
    assert worker.escalation_agent({"id": "a", "retry_count": "x"}, SAT) == "coder"


def test_empty_ladder_falls_back_to_host_pin(monkeypatch):
    monkeypatch.setattr(worker, "ZODER_LADDER", [])
    assert worker.escalation_agent({"id": "a", "retry_count": 3}, SAT) == "coder"
