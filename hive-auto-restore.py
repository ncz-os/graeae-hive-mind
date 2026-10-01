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
STATE_DIR = Path("/var/lib/hive-auto-restore")
STATE_FILE = STATE_DIR / "retry_counts.json"
LOG_FILE = "/var/log/hive-auto-restore.log"

MAX_RETRIES = 5  # never restore more than 5 times per job
BACKOFF_BASE = 300  # 5 min after 1st fail, doubles each
MIN_FAIL_AGE_SEC = 300  # only restore failures older than five minutes (give workers time to truly finish)


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


def _http(
    method: str, path: str, body: dict | None = None, timeout: float = 15.0
) -> tuple[int, dict | None]:
    url = f"{HIVE_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"content-type": "application/json"},
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
    if code != 200 or not resp:
        log(f"fetch_failed_jobs http={code} resp={str(resp)[:200]}")
        return []
    jobs = resp.get("jobs", []) or []
    jobs.sort(key=lambda j: j.get("ended_at") or 0, reverse=True)
    return jobs


def restore_job(jid: str) -> tuple[bool, str]:
    body = {
        "status": "queued",
        "claimed_by": None,
        "claimed_at": None,
        "claimed_runtime": None,
        "claimed_model": None,
        "claimed_provider": None,
        "claimed_cost_tier": None,
        "claim_lease_expires_at": None,
        "ended_at": None,
        "retry_backoff_until": None,
    }
    code, resp = _http(
        "PATCH", f"/v1/jobs/{urllib.parse.quote(jid, safe='')}", body, timeout=15.0
    )
    if code in (200, 204):
        return True, ""
    return False, f"http={code} resp={str(resp)[:200]}"


def _is_positive_int(val) -> bool:
    return isinstance(val, int) and not isinstance(val, bool) and val > 0


def _is_non_negative_int(val) -> bool:
    return isinstance(val, int) and not isinstance(val, bool) and val >= 0


def _normalize_status_str(val) -> str:
    if not isinstance(val, str):
        return ""
    return val.strip().lower().replace("_", "-")


def _is_blocked_result(result) -> bool:
    if not isinstance(result, dict):
        return True

    # Check explicit markers: non-empty values are holds
    for key in (
        "held",
        "hold",
        "manual_review",
        "requires_manual_review",
        "needs_review",
        "superseded",
        "superseded_by",
    ):
        val = result.get(key)
        if val is not None and val is not False and val != "":
            return True

    # Check reason/status strings
    for key in ("reason", "status", "error", "message"):
        val = result.get(key)
        if isinstance(val, str):
            norm = _normalize_status_str(val)
            if norm in ("hold", "held", "superseded", "manual-review", "needs-review"):
                return True

    return False


def auto_retry_allowed(job: dict) -> bool:
    """
    Returns True if the job is eligible for auto-restore.
    Fails closed on missing/malformed budget fields.
    """
    # 0. Status check
    if job.get("status") != "failed":
        return False

    # 1. Budget checks (fail closed)
    max_retries = job.get("max_retries")
    retry_count = job.get("retry_count")

    if not _is_positive_int(max_retries):
        return False
    if not _is_non_negative_int(retry_count):
        return False

    # 2. Exhaustion checks
    # Server-side exhaustion
    if retry_count >= max_retries:
        return False

    # 3. Result checks
    result = job.get("result")
    if _is_blocked_result(result):
        return False

    # 4. Kind-specific checks
    kind = job.get("kind") or ""
    if kind.startswith("build"):
        # Build kinds require result.retryable is literal True
        if not isinstance(result, dict):
            return False
        if result.get("retryable") is not True:
            return False
    else:
        # Non-build failed jobs may preserve retry eligibility only when budget and hold checks pass.
        # Explicit retryable=False blocks any kind.
        if isinstance(result, dict) and result.get("retryable") is False:
            return False

    return True


def main():
    state = load_state()
    now = time.time()
    restored = 0
    skipped_capped = 0
    skipped_backoff = 0
    skipped_too_fresh = 0
    skipped_policy = 0

    failed = fetch_failed_jobs()
    log(f"poll: {len(failed)} failed jobs to consider")

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

        # Policy check
        if not auto_retry_allowed(job):
            skipped_policy += 1
            continue

        entry = state.get(jid, {"count": 0, "next_at": 0})
        local_count = entry.get("count", 0)
        if not _is_non_negative_int(local_count):
            local_count = 0

        # Cap by both global MAX_RETRIES and job's max_retries
        effective_cap = min(MAX_RETRIES, job.get("max_retries", MAX_RETRIES))
        if local_count >= effective_cap:
            skipped_capped += 1
            continue
        if entry.get("next_at", 0) > now:
            skipped_backoff += 1
            continue

        ok, err = restore_job(jid)
        if ok:
            restored += 1
            entry["count"] = entry.get("count", 0) + 1
            entry["next_at"] = now + BACKOFF_BASE * (2 ** (entry["count"] - 1))
            entry["last_kind"] = kind
            state[jid] = entry
            log(
                f"restored {jid[:13]} kind={kind[:40]} attempt={entry['count']}/{effective_cap}"
            )
        else:
            log(f"err restoring {jid[:13]}: {err}")

    save_state(state)
    log(
        f"summary: restored={restored} skipped_capped={skipped_capped} skipped_backoff={skipped_backoff} skipped_too_fresh={skipped_too_fresh} skipped_policy={skipped_policy}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
