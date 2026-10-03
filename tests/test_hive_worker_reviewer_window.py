"""Hive reviews leave CERBERUS gemma alone during RiskyEats publish bursts."""

import os
from datetime import datetime

import pytest

os.environ["WORKER_KIND"] = "zeroclaw"

import hive_worker as worker


@pytest.fixture(autouse=True)
def pinned(monkeypatch):
    monkeypatch.setattr(worker, "ZODER_REVIEWER", "gemma4-31b")
    monkeypatch.setattr(
        worker, "ZODER_REVIEWER_ALT", "nvidia/nemotron-3-ultra-550b-a55b"
    )
    monkeypatch.setattr(worker, "REVIEWER_QUIET_WINDOWS", "07:30-09:00,18:00-19:30")


def at(h, m):
    return datetime(2026, 10, 3, h, m)


def test_outside_windows_keeps_gemma():
    for h, m in ((7, 29), (9, 0), (12, 0), (17, 59), (19, 30), (23, 0)):
        assert worker.review_model(at(h, m)) == "gemma4-31b", (h, m)


def test_inside_windows_uses_alternate():
    for h, m in ((7, 30), (8, 45), (18, 0), (19, 29)):
        assert worker.review_model(at(h, m)) == "nvidia/nemotron-3-ultra-550b-a55b", (
            h,
            m,
        )


def test_no_pinned_reviewer_stays_unpinned(monkeypatch):
    monkeypatch.setattr(worker, "ZODER_REVIEWER", "")
    assert worker.review_model(at(8, 0)) == ""


def test_malformed_window_spec_is_ignored(monkeypatch):
    monkeypatch.setattr(worker, "REVIEWER_QUIET_WINDOWS", "junk,18:00-19:30")
    assert worker.review_model(at(8, 0)) == "gemma4-31b"
    assert worker.review_model(at(18, 30)) == "nvidia/nemotron-3-ultra-550b-a55b"
