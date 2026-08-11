#!/usr/bin/env python3
"""
Hive auto-restore daemon. Polls the hive bus HTTP API for newly-failed jobs
and restores them to 'queued' with retry backoff. Caps per-job retries to prevent
infinite loops on permanently-broken jobs.

Run via systemd timer every 5 min on PYTHIA as graeae-hive user.

State: tracks per-job retry count in /var/lib/hive-auto-restore/retry_counts.json
Skip after MAX_RETRIES per job. After skip, leave in 'failed' permanently.
"""

from __future__ import annotations
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

HIVE_URL = os.environ.get("HIVE_URL", "http://192.168.207.67:5005")
# C1: bus transport auth bearer token (env-sourced, never baked in).
HIVE_BUS_TOKEN = os.environ.get("HIVE_BUS_TOKEN", "")
HIVE_BUS_ADMIN_TOKEN = os.environ.get("HIVE_BUS_ADMIN_TOKEN", HIVE_BUS_TOKEN)
STATE_DIR = Path("/var/lib/hive-auto-restore")
STATE_FILE = STATE_DIR / "retry_counts.json"
LOG_FILE = "/var/log/hive-auto-restore.log"

MAX_RETRIES = 5          # never restore more than 5 times per job
BACKOFF_BASE = 300       # 5 min after 1st fail, doubles each
MIN_FAIL_AGE_SEC = 300    # only restore failures older than 60s (give workers time to truly finish)

def log(msg):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass

def load_state():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            return {}
    return {}

def save_state(state):
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(STATE_FILE)

def _http(method: str, path: str, body: dict | None = None, timeout: float = 15.0,
          *, admin: bool = False) -> tuple[int, dict | None]:
    url = f"{HIVE_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {"content-type": "application/json"}
    token = HIVE_BUS_ADMIN_TOKEN if admin else HIVE_BUS_TOKEN
    if token:
        headers["authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except Exception as e:
        log(f"http {method} {path}: {e}")
        return 0, None

def fetch_failed_jobs(limit: int = 1000) -> list[dict]:
    query = urllib.parse.urlencode({"status": "failed", "limit": str(limit)})
    code, resp = _http("GET", f"/v1/jobs?{query}", timeout=15.0)
    if code != 200 or not isinstance(resp, dict):
        raise RuntimeError(f"fetch_failed_jobs http={code} resp={str(resp)[:200]}")
    if "jobs" not in resp:
        raise RuntimeError("fetch_failed_jobs response is missing field 'jobs'")
    jobs = resp.get("jobs") or []
    if not isinstance(jobs, list):
        raise RuntimeError("fetch_failed_jobs response field 'jobs' is not a list")
    jobs.sort(key=lambda j: j.get("ended_at") or 0, reverse=True)
    return jobs

def restore_jobs(job_ids: list[str]) -> tuple[dict[str, str], str]:
    code, resp = _http(
        "POST",
        "/v1/admin/jobs/requeue",
        {"job_ids": job_ids, "reason": "auto-restore"},
        timeout=15.0,
        admin=True,
    )
    if code == 200 and isinstance(resp, dict):
        results = {
            jid: outcome
            for outcome in ("ok", "not_found", "skipped_done")
            for jid in (resp.get(outcome) or [])
        }
        return results, ""
    return {}, f"http={code} resp={str(resp)[:200]}"

def main():
    state = load_state()
    now = time.time()
    restored = 0
    skipped_capped = 0
    skipped_backoff = 0
    skipped_too_fresh = 0

    failed = fetch_failed_jobs()
    log(f"poll: {len(failed)} failed jobs to consider")

    candidates: list[tuple[dict, dict]] = []
    for job in failed:
        jid = job.get("id") or ""
        kind = job.get("kind") or ""
        ended_at = job.get("ended_at")
        if not jid:
            continue
        age = now - (ended_at or 0)
        if age < MIN_FAIL_AGE_SEC:
            skipped_too_fresh += 1
            continue
        entry = state.get(jid, {"count": 0, "next_at": 0})
        if entry["count"] >= MAX_RETRIES:
            skipped_capped += 1
            continue
        if entry.get("next_at", 0) > now:
            skipped_backoff += 1
            continue

        candidates.append((job, entry))

    restore_results, restore_error = restore_jobs([job["id"] for job, _entry in candidates]) if candidates else ({}, "")
    restore_failed = bool(restore_error)
    for job, entry in candidates:
        jid = job["id"]
        kind = job.get("kind") or ""
        if restore_results.get(jid) == "ok":
            restored += 1
            entry["count"] = entry.get("count", 0) + 1
            entry["next_at"] = now + BACKOFF_BASE * (2 ** (entry["count"] - 1))
            entry["last_kind"] = kind
            state[jid] = entry
            log(f"restored {jid[:13]} kind={kind[:40]} attempt={entry['count']}/{MAX_RETRIES}")
        elif restore_results.get(jid) in {"not_found", "skipped_done"}:
            log(f"skipped restoring {jid[:13]}: {restore_results[jid]}")
        else:
            restore_failed = True
            outcome = restore_results.get(jid, "missing_result")
            log(f"err restoring {jid[:13]}: {restore_error or outcome}")

    save_state(state)
    log(f"summary: restored={restored} skipped_capped={skipped_capped} skipped_backoff={skipped_backoff} skipped_too_fresh={skipped_too_fresh}")
    return 1 if restore_failed else 0

if __name__ == "__main__":
    sys.exit(main())
