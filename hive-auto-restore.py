#!/usr/bin/env python3
"""
Hive auto-restore daemon. Polls /srv/agent-bus/agents.db for newly-failed jobs
and restores them to 'queued' with retry backoff. Caps per-job retries to prevent
infinite loops on permanently-broken jobs.

Run via systemd timer every 5 min on PYTHIA as graeae-hive user.

State: tracks per-job retry count in /var/lib/hive-auto-restore/retry_counts.json
Skip after MAX_RETRIES per job. After skip, leave in 'failed' permanently.
"""

from __future__ import annotations
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

DB_PATH = "/srv/agent-bus/agents.db"
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

def main():
    state = load_state()
    now = time.time()
    restored = 0
    skipped_capped = 0
    skipped_backoff = 0
    skipped_too_fresh = 0

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")

    cur = conn.execute(
        "SELECT id, kind, ended_at, started_at, priority FROM jobs WHERE status='failed' ORDER BY ended_at DESC LIMIT 1000"
    )
    failed = cur.fetchall()
    log(f"poll: {len(failed)} failed jobs to consider")

    for jid, kind, ended_at, started_at, priority in failed:
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

        # Restore
        try:
            conn.execute(
                "UPDATE jobs SET status='queued', claimed_by=NULL, claimed_at=NULL, "
                "claimed_runtime=NULL, claimed_model=NULL, claimed_provider=NULL, "
                "claimed_cost_tier=NULL, claim_lease_expires_at=NULL, ended_at=NULL, "
                "retry_backoff_until=NULL WHERE id=? AND status='failed'",
                (jid,),
            )
            conn.commit()
            restored += 1
            entry["count"] = entry.get("count", 0) + 1
            entry["next_at"] = now + BACKOFF_BASE * (2 ** (entry["count"] - 1))
            entry["last_kind"] = kind
            state[jid] = entry
            log(f"restored {jid[:13]} kind={kind[:40]} attempt={entry['count']}/{MAX_RETRIES}")
        except sqlite3.Error as e:
            log(f"err restoring {jid[:13]}: {e}")

    conn.close()
    save_state(state)
    log(f"summary: restored={restored} skipped_capped={skipped_capped} skipped_backoff={skipped_backoff} skipped_too_fresh={skipped_too_fresh}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
