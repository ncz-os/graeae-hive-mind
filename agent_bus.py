"""
GRAEAE Hive Mind — fleet-wide MCP-compatible agent coordination + triage queue.

Brand: GRAEAE Hive Mind (extension of GRAEAE consensus engine — sister to mnemos memory)
Identity: urn:agent:<kind>:<host>:<session_uuid>   (kinds: claude, opencode, codex, zeroclaw, openclaw, hermes, human, ic-engine, mnemos, ...)
Backend: SQLite WAL (Phase 1) — PG migration documented for Phase 2 when >50 agents OR LISTEN/NOTIFY needed
Pub/sub: SSE (Phase 1) — NATS migration Phase 2 when >20 concurrent OR >100 msg/s
Triage queue: /v1/jobs/next dequeues highest-priority eligible work; no central scheduler — agents self-claim.
"""
from __future__ import annotations
import asyncio
import json
import os
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from contextlib import suppress
from typing import Optional, Any

import aiosqlite
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

DB_PATH = os.environ.get("AGENT_BUS_DB", "/srv/agent-bus/agents.db")
HEARTBEAT_REAP_INTERVAL = 30.0
HEARTBEAT_STALE_AFTER = 90.0
HEARTBEAT_OFFLINE_AFTER = 300.0
EVENTS_RETAIN_HOURS = 168  # 7 days
SSE_PING_INTERVAL = 15.0
EVENT_QUEUE: dict[str, asyncio.Queue] = {}  # subscriber_id -> queue
SQLITE_BUSY_TIMEOUT_MS = int(os.environ.get("AGENT_BUS_SQLITE_BUSY_TIMEOUT_MS", "5000"))
MAX_EVENT_SUBSCRIBERS = int(os.environ.get("AGENT_BUS_MAX_EVENT_SUBSCRIBERS", "100"))


# ---------- helpers ----------

@asynccontextmanager
async def connect_db():
    db = await aiosqlite.connect(
        DB_PATH,
        timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
        check_same_thread=False,
    )
    try:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        yield db
    finally:
        await db.close()


def uuidv7() -> str:
    """Time-ordered UUID for monotonic index inserts."""
    ts_ms = int(time.time() * 1000)
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    val = (
        (ts_ms & ((1 << 48) - 1)) << 80
        | (0x7 << 76)
        | (rand_a << 64)
        | (0x2 << 62)
        | rand_b
    )
    return str(uuid.UUID(int=val))


def make_urn(kind: str, host: str, session_id: str) -> str:
    return f"urn:agent:{kind}:{host}:{session_id}"


async def emit_event(db, kind: str, payload: dict) -> None:
    ts = time.time()
    payload_json = json.dumps(payload, separators=(",", ":"))
    await db.execute(
        "INSERT INTO events (ts, kind, payload, agent_urn) VALUES (?, ?, ?, ?)",
        (ts, kind, payload_json, payload.get("urn") or payload.get("agent_urn")),
    )
    await db.commit()
    # broadcast to live SSE subscribers
    for q in list(EVENT_QUEUE.values()):
        try:
            q.put_nowait({"kind": kind, "ts": ts, "payload": payload})
        except asyncio.QueueFull:
            pass


# ---------- schema ----------

SCHEMA = """
-- Fresh-DB shape; matches what the code actually reads & writes
-- (graeae:schema-drift fix 2026-05-23). For live DBs the lifespan
-- migrator below issues idempotent ALTER/CREATE to converge.
CREATE TABLE IF NOT EXISTS agents (
  urn TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  host TEXT NOT NULL,
  session_id TEXT NOT NULL,
  pid INTEGER,
  capabilities TEXT,
  version TEXT,
  started_at REAL NOT NULL,
  last_heartbeat REAL NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('online','idle','stale','offline','error')),
  metadata TEXT,
  -- v0.2 cost/runtime/autonomy columns (graeae:schema-drift)
  runtime TEXT,
  model TEXT,
  provider TEXT,
  autonomy_level TEXT,
  cost_tier TEXT,
  current_load TEXT,
  auth_method TEXT,
  plan_cap_usd REAL,
  plan_period_used_usd REAL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_agents_status      ON agents(status);
CREATE INDEX IF NOT EXISTS idx_agents_kind        ON agents(kind);

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  submitter_urn TEXT NOT NULL,                  -- who created the work (user/human OR delegating agent)
  parent_job_id TEXT,
  kind TEXT NOT NULL,                           -- code-edit/research/review/build/test/etc
  description TEXT,
  priority INTEGER NOT NULL DEFAULT 0,          -- higher = more urgent
  deadline REAL,
  required_capabilities TEXT,                   -- json array; worker must have ALL
  eligible_kinds TEXT,                          -- json array; agent kinds eligible (null = any)
  eligible_hosts TEXT,                          -- json array; agent hosts eligible (null = any); e.g. ["cixmini"]
  project TEXT,                                 -- #10 FIX: separate project tag from capabilities (riskyeats/investorclaw/etc)
  status TEXT NOT NULL CHECK(status IN ('queued','offered','claimed','running','done','failed','cancelled')),
  claimed_by TEXT,                              -- worker urn (set on claim/dequeue)
  claimed_at REAL,
  started_at REAL NOT NULL,                     -- when job ENTERED queue
  ended_at REAL,
  result TEXT,
  -- v0.2 cost/autonomy/retry columns (graeae:schema-drift)
  required_autonomy TEXT,
  max_cost_tier TEXT,
  preferred_providers TEXT,                     -- json array
  preferred_models TEXT,                        -- json array
  claimed_runtime TEXT,
  claimed_model TEXT,
  claimed_provider TEXT,
  claimed_cost_tier TEXT,
  tokens_in INTEGER,
  tokens_out INTEGER,
  estimated_cost_usd REAL,
  mnemos_refs TEXT,                             -- json array of mem_* ids
  result_mnemos_id TEXT,
  required_resources TEXT,                      -- json
  claimed_host_caps TEXT,                       -- json
  tags TEXT,                                    -- json array
  depends_on TEXT,                              -- json array of job ids
  retry_count INTEGER NOT NULL DEFAULT 0,
  max_retries INTEGER NOT NULL DEFAULT 2,
  retry_backoff_until REAL,
  last_update_at REAL,
  claim_lease_expires_at REAL
);
CREATE INDEX IF NOT EXISTS idx_jobs_status            ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_submitter         ON jobs(submitter_urn);
CREATE INDEX IF NOT EXISTS idx_jobs_claimed_by        ON jobs(claimed_by);
CREATE INDEX IF NOT EXISTS idx_jobs_parent            ON jobs(parent_job_id);
CREATE INDEX IF NOT EXISTS idx_jobs_queue             ON jobs(status, priority DESC, started_at ASC);

CREATE TABLE IF NOT EXISTS job_audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL,
  ts REAL NOT NULL,
  actor_urn TEXT,
  old_status TEXT,
  new_status TEXT,
  old_claimed_by TEXT,
  new_claimed_by TEXT,
  patch TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_job_audit_job_ts ON job_audit_log(job_id, ts);

CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  from_urn TEXT NOT NULL,
  to_urn TEXT,
  in_reply_to TEXT,
  topic TEXT NOT NULL,
  payload TEXT NOT NULL,
  ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_to    ON messages(to_urn);
CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic);
CREATE INDEX IF NOT EXISTS idx_messages_ts    ON messages(ts);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts REAL NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  agent_urn TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_ts    ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_kind  ON events(kind);
CREATE INDEX IF NOT EXISTS idx_events_agent ON events(agent_urn);

-- v0.2 tables (graeae:schema-drift fix 2026-05-23)
CREATE TABLE IF NOT EXISTS hive_cache (
  cache_key TEXT PRIMARY KEY,
  result_json TEXT NOT NULL,
  source_job_id TEXT,
  result_mnemos_id TEXT,
  hit_count INTEGER NOT NULL DEFAULT 0,
  cost_saved_usd REAL NOT NULL DEFAULT 0,
  model TEXT,
  provider TEXT,
  cached_at REAL NOT NULL,
  last_hit_at REAL
);
CREATE INDEX IF NOT EXISTS idx_hive_cache_cached_at ON hive_cache(cached_at);

CREATE TABLE IF NOT EXISTS worker_kind_stats (
  urn TEXT NOT NULL,
  kind TEXT NOT NULL,
  success_count INTEGER NOT NULL DEFAULT 0,
  fail_count INTEGER NOT NULL DEFAULT 0,
  cancelled_count INTEGER NOT NULL DEFAULT 0,
  total_tokens_in INTEGER NOT NULL DEFAULT 0,
  total_tokens_out INTEGER NOT NULL DEFAULT 0,
  total_cost_usd REAL NOT NULL DEFAULT 0,
  total_duration_sec REAL NOT NULL DEFAULT 0,
  last_run REAL,
  PRIMARY KEY (urn, kind)
);
CREATE INDEX IF NOT EXISTS idx_wkstats_kind ON worker_kind_stats(kind);

CREATE TABLE IF NOT EXISTS scheduled_jobs (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_by_urn TEXT NOT NULL,
  interval_seconds INTEGER NOT NULL,
  job_template TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  last_fired_at REAL,
  next_fire_at REAL NOT NULL,
  fire_count INTEGER NOT NULL DEFAULT 0,
  created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sched_next ON scheduled_jobs(next_fire_at, enabled);
"""


# ---------- models ----------

# Runtime → eligible kinds map (Kimi-K2.6 advisory 2026-05-23).
RUNTIME_KIND_MAP: dict[str, set[str]] = {
    "claude-code":   {"claude", "claude-code"},
    "claude-cli":    {"claude", "claude-code"},
    "opencode":      {"opencode"},                 # opencode CAN run any model — kind=opencode
    "opencode-cli":  {"opencode"},
    "codex":         {"codex"},
    "codex-cli":     {"codex"},
    "hermes":        {"hermes"},
    "zeroclaw":      {"zeroclaw"},  # zeroclaw is its own kind; does not alias goose/codex/opencode/claude
    "openclaw":      {"openclaw"},
    "ic-engine":     {"ic-engine"},
    "mnemos":        {"mnemos"},
    "human":         {"human"},
    "claude":        {"claude"},
    "system":        {"system"},                    # fleet hosts (ARGOS/TYPHON/HYDRA/MEDUSA/CERBERUS/PROTEUS/cixmini)
    "doctor":        {"doctor"},  # PYTHIA zeroclaw doctor — triage authority + DB access
    "unknown":       {"unknown"},
}
AUTONOMY_LEVELS = {"autonomous", "confirm-risky", "interactive", "unknown"}
AUTH_METHODS = {"subscription", "api", "free", "unknown"}
# Plan caps (USD/month) per CLAUDE.md:
DEFAULT_PLAN_CAPS = {
    "subscription": 200.0,   # Anthropic Max plan ($200 until 2026-05-31, $100 from 2026-06-01 — operator updates)
    "api":          1000.0,  # pay-per-token has no hard cap; treat as high ceiling for safety
    "free":         0.0,     # no cap (no cost)
    "unknown":      50.0,    # conservative
}
THROTTLE_HEADROOM = 0.85  # at >=85% of plan cap, prefer non-subscription workers for tier-B/C jobs
VALID_AGENT_STATUSES = {"online", "idle", "stale", "offline", "error"}
ACTIVE_AGENT_STATUSES = {"online", "idle"}

# ROLE SPLIT (user directive 2026-05-23): opencode + codex + hermes +
# claw-family + ic-engine + unknown = WORKERS (claim-only). Cannot submit jobs.
# Orchestrators: claude-code, human, mnemos. They submit work; workers execute it.
WORKER_ONLY_RUNTIMES: set[str] = {
    "opencode", "opencode-cli",
    "opencode", "opencode-cli",
    "codex", "codex-cli",
    "hermes",
    "zeroclaw", "openclaw",
    "ic-engine",
    "system",   # fleet hosts (system-watcher daemons) — sensors + optional build/ci workers, never submitters
}
ORCHESTRATOR_RUNTIMES: set[str] = {
    "claude-code", "claude-cli", "claude",
    "human",
    "mnemos",
    "doctor",   # PYTHIA triage authority — submits codex sub-jobs + auto-triage
    "unknown",
}

# HOST AFFINITY — automatic eligible_hosts injection based on job kind prefix.
# When a job kind matches a prefix, the server adds eligible_hosts if not already set.
# This ensures e.g. cixmini-os: jobs only run on cixmini hardware without callers needing to specify.
KIND_HOST_AFFINITY: dict[str, list[str]] = {
    # Hardware-bound: needs physical CIX Sky1 NPU — zeroclaw cannot substitute with SSH
    "cixmini-os:":    ["cixmini"],
    "ncz-os:":        ["cixmini"],
    # Pi-specific tasks (explicit opt-in; general jobs use any worker)
    "pi:":            ["bigpi", "clawpi", "zeropi"],
    "arm64-test:":    ["bigpi", "clawpi", "zeropi", "cixmini"],
    # Note: investorclaw/investorclaude are NOT here — any zeroclaw can execute
    # those by SSHing to the appropriate host. Host affinity would over-restrict.
}

# COST-TIER MAP (per ~/.claude/rules/llm-usage-policy-2026-05-22.md):
#   A = FREE   — local + NGC NIM (try first, token-miser)
#   B = CHEAP  — Groq Dev tier, xAI, DeepSeek direct, Together cheap, Gemini-Flash, OpenAI-mini
#   C = RESERVE — Anthropic Opus/Sonnet, OpenAI GPT-5.5/Pro, Gemini Pro, Together DeepSeek-Pro
#                 (Together DeepSeek-V4-Pro = anti-pattern — use DeepSeek direct instead)
PROVIDER_COST_TIER: dict[str, str] = {
    # Tier A = PREMIUM (highest quality, expensive) - operator authorizes top budget
    "anthropic":       "A",
    "claude":          "A",
    "openai":          "A",
    "openai-pro":      "A",
    "openai-gpt55":    "A",
    "gemini":          "A",
    "gemini-pro":      "A",
    "together-pro":    "A",

    # Tier B = MID (cheap paid commercial)
    "groq":            "B",
    "xai":             "B",
    "deepseek":        "B",
    "deepseek-direct": "B",
    "together":        "B",
    "gemini-flash":    "B",
    "openai-mini":     "B",
    "perplexity":      "B",

    # Tier C = ROUTINE (local / free NGC / Nvidia inference)
    "ngc":             "C",
    "nvidia":          "C",
    "nvidia-ngc":      "C",
    "local-llamacpp":  "C",
    "local-vllm":      "C",
    "ollama":          "C",
    "ollama-cerberus": "C",
    "local":           "C",
    "pantheon":        "C",  # fleet PANTHEON router routes to free/cheap providers

    "unknown":         "C",  # treat as routine when classification missing
}
COST_TIERS = ["A", "B", "C"]
VALID_JOB_STATUSES = {"queued", "offered", "claimed", "running", "done", "failed", "cancelled"}
TERMINAL_JOB_STATUSES = {"done", "failed", "cancelled"}
STATUS_TRANSITIONS: dict[str, set[str]] = {
    "queued": {"queued", "offered", "claimed", "cancelled"},
    "offered": {"queued", "claimed", "cancelled"},
    "claimed": {"queued", "claimed", "running", "done", "failed", "cancelled"},
    "running": {"queued", "running", "done", "failed", "cancelled"},
    "done": {"done"},
    "failed": {"failed"},
    "cancelled": {"cancelled"},
}
CLAIM_LEASE_SECONDS = float(os.environ.get("CLAIM_LEASE_SECONDS", "1800"))
# Hosts the bus refuses to offer jobs to — typically because their workers
# have a broken config (e.g. enc2 secret-key mismatch) and would fail any
# claim immediately. User directive 2026-05-26: pegasus (.79) is flooding
# the failed queue with 241/243 recent fails — denylist it until config-fix.
# Format: lowercase short hostname. Updated via env or admin endpoint.
HOST_DENYLIST: set[str] = {
    h.strip().lower() for h in os.environ.get("HIVE_HOST_DENYLIST", "pegasus").split(",")
    if h.strip()
}


def cost_tier_for(provider: str) -> str:
    return PROVIDER_COST_TIER.get((provider or "unknown").lower(), "C")


def json_list(raw: Optional[str]) -> list[Any]:
    if not raw:
        return []
    val = json.loads(raw)
    return val if isinstance(val, list) else []


def clamp_limit(value: int, *, default: int = 100, max_limit: int = 1000) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        return default
    if limit < 1:
        return default
    return min(limit, max_limit)


def agent_kind_aliases(kind: str, runtime: Optional[str]) -> set[str]:
    aliases = {kind}
    if runtime:
        aliases.add(runtime)
        aliases.update(RUNTIME_KIND_MAP.get(runtime, set()))
    for rt, kinds in RUNTIME_KIND_MAP.items():
        if kind in kinds:
            aliases.add(rt)
            aliases.update(kinds)
    aliases.discard("")
    return aliases


def field_was_set(model: BaseModel, name: str) -> bool:
    fields_set = getattr(model, "model_fields_set", None)
    if fields_set is None:
        fields_set = getattr(model, "__fields_set__", set())
    return name in fields_set


async def require_registered_agent(urn: str, *, active_only: bool = False) -> tuple[str, str]:
    if not urn or not urn.strip():
        raise HTTPException(422, "agent urn is required")
    async with connect_db() as db:
        async with db.execute(
            "SELECT runtime, status FROM agents WHERE urn=?",
            (urn,),
        ) as cur:
            row = await cur.fetchone()
    if not row:
        raise HTTPException(403, f"agent is not registered: {urn}")
    runtime = (row[0] or "unknown").lower()
    status = (row[1] or "unknown").lower()
    if active_only and status not in ACTIVE_AGENT_STATUSES:
        raise HTTPException(403, f"agent {urn} is not active; current status={status!r}")
    return runtime, status


async def require_orchestrator_submitter(submitter_urn: str) -> str:
    runtime, _status = await require_registered_agent(submitter_urn, active_only=True)
    if runtime in WORKER_ONLY_RUNTIMES or runtime not in ORCHESTRATOR_RUNTIMES:
        raise HTTPException(
            status_code=403,
            detail=(
                f"role-violation: runtime={runtime!r} is not an orchestrator. "
                f"Workers CLAIM jobs via POST /v1/jobs/next; only registered active "
                f"orchestrators may SUBMIT jobs. Orchestrators: {sorted(ORCHESTRATOR_RUNTIMES)}."
            ),
        )
    return runtime


def normalize_result_payload(status: str, result: Optional[dict]) -> Optional[dict]:
    if result is None or status != "done":
        return result
    normalized = dict(result)
    commits = normalized.get("commits")
    files_changed = normalized.get("files_changed")
    normalized["commits"] = commits if isinstance(commits, list) else []
    normalized["files_changed"] = files_changed if isinstance(files_changed, list) else []
    return normalized


# Per-million-token rates (USD). Workers SHOULD report tokens_in/tokens_out in PATCH result;
# hive computes estimated_cost_usd. Wildcard model "*" applies to any model on that provider.
# Source: ~/.claude/rules/llm-usage-policy-2026-05-22.md
LLM_RATES: dict[tuple[str, str], tuple[float, float]] = {
    ("anthropic", "claude-opus-4-7"):       (5.00, 25.00),
    ("anthropic", "claude-opus-4-6"):       (5.00, 25.00),
    ("anthropic", "claude-sonnet-4-6"):     (3.00, 15.00),
    ("anthropic", "claude-sonnet-4-7"):     (3.00, 15.00),
    ("anthropic", "claude-haiku-4-5"):      (1.00,  5.00),
    ("anthropic", "*"):                     (3.00, 15.00),
    ("openai",    "gpt-5.5"):               (5.00, 30.00),
    ("openai",    "gpt-5.5-pro"):           (30.00, 180.00),
    ("openai",    "gpt-5.4-nano"):          (0.20,  1.25),
    ("openai",    "o4-mini"):               (0.55,  2.20),
    ("openai",    "o3"):                    (2.00,  8.00),
    ("openai",    "*"):                     (5.00, 30.00),
    ("xai",       "grok-4.3"):              (1.25,  2.50),
    ("xai",       "grok-4.1-fast"):         (0.20,  0.50),
    ("xai",       "grok-4.20"):             (2.00,  6.00),
    ("xai",       "*"):                     (1.25,  2.50),
    ("groq",      "llama-3.3-70b-versatile"): (0.59,  0.79),
    ("groq",      "llama-3.1-8b-instant"):  (0.05,  0.08),
    ("groq",      "llama-4-scout-17b"):     (0.11,  0.34),
    ("groq",      "qwen3-32b"):             (0.29,  0.59),
    ("groq",      "gpt-oss-120b"):          (0.15,  0.60),
    ("groq",      "gpt-oss-20b"):           (0.075, 0.30),
    ("groq",      "*"):                     (0.29,  0.59),
    ("deepseek-direct", "deepseek-v4-pro"): (0.435, 0.87),
    ("deepseek-direct", "deepseek-v4-flash"): (0.14, 0.28),
    ("deepseek-direct", "*"):               (0.435, 0.87),
    ("deepseek",        "*"):               (0.435, 0.87),
    ("together",  "minimax-m2.7"):          (0.40,  1.20),
    ("together",  "deepseek-v4-pro"):       (2.10,  4.40),
    ("together",  "kimi-k2.6"):             (1.20,  4.40),
    ("together",  "glm-3.5-90"):            (0.10,  0.15),
    ("together",  "qwen2.5-coder-32b"):     (0.05,  0.12),
    ("together",  "*"):                     (0.40,  1.20),
    ("gemini",    "gemini-2.5-flash-lite"): (0.10,  0.40),
    ("gemini",    "gemini-2.5-flash"):      (0.30,  2.50),
    ("gemini",    "gemini-3.1-pro"):        (2.00, 12.00),
    ("gemini",    "*"):                     (1.25, 10.00),
    ("perplexity","sonar"):                 (1.00,  1.00),
    ("perplexity","sonar-pro"):             (3.00, 15.00),
    ("perplexity","*"):                     (1.00,  1.00),
}


def rate_for(provider: str, model: str) -> tuple[float, float]:
    p = (provider or "unknown").lower()
    m = (model or "").lower()
    # exact match first
    if (p, m) in LLM_RATES:
        return LLM_RATES[(p, m)]
    # wildcard model on provider
    if (p, "*") in LLM_RATES:
        return LLM_RATES[(p, "*")]
    # tier A providers = free
    if cost_tier_for(p) == "A":
        return (0.0, 0.0)
    return (1.0, 5.0)  # conservative fallback


def estimate_cost(provider: str, model: str, tokens_in: int, tokens_out: int) -> float:
    in_rate, out_rate = rate_for(provider, model)
    return round((tokens_in / 1_000_000) * in_rate + (tokens_out / 1_000_000) * out_rate, 6)


def hallucination_check(result: dict) -> Optional[str]:
    """Return reason string if result looks like an LLM token-loop / hallucination,
    else None. Saves marking-done garbage that wastes a cache entry + claims completion.

    Heuristics:
    - >40% of stdout is a single repeated word/phrase
    - stdout >1500 chars with <50 unique words (Kimi-K2.6 'extension extension extension' loop)
    - stdout contains 'Rate limit exceeded' / 'Authentication error' / 'context-length exceeded'
    """
    if not isinstance(result, dict):
        return None
    stdout = result.get("stdout") or result.get("output") or ""
    if not isinstance(stdout, str) or len(stdout) < 200:
        return None
    if "rate limit exceeded" in stdout.lower():
        return "rate_limit_in_output"
    if "authentication error" in stdout.lower() or "authentication failed" in stdout.lower():
        return "auth_error_in_output"
    if "context length" in stdout.lower() and "exceed" in stdout.lower():
        return "context_overflow_in_output"
    # token-loop detection
    words = stdout.split()
    if len(words) > 200:
        from collections import Counter
        top_word, top_count = Counter(words).most_common(1)[0]
        if top_count / len(words) > 0.4:
            return f"token_loop:{top_word!r}_repeated_{top_count}_of_{len(words)}"
    return None


# RESULT CACHE — Nemotron killer feature 2026-05-23.
# Memoize (kind, description, max_cost_tier, sorted-required-capabilities) → result.
# When identical job submitted, return cached result instantly. Cuts LLM spend
# 30-70% on repetitive work + avoids NGC 429 storms from duplicate dispatches.
import hashlib as _hashlib
CACHE_TTL_SECONDS = 24 * 3600  # 24h default; idempotent work like compiles/lints often valid much longer


def cache_key_for(kind: str, description: Optional[str], max_cost_tier: str,
                  required_capabilities: Optional[list[str]]) -> str:
    norm_desc = (description or "").strip()
    norm_caps = ",".join(sorted(required_capabilities or []))
    payload = f"{kind}\n{norm_desc}\n{max_cost_tier}\n{norm_caps}"
    return _hashlib.sha256(payload.encode()).hexdigest()


async def cache_lookup(db, cache_key: str) -> Optional[dict]:
    cutoff = time.time() - CACHE_TTL_SECONDS
    async with db.execute(
        "SELECT result_json, source_job_id, result_mnemos_id, hit_count, cost_saved_usd, model, provider, cached_at "
        "FROM hive_cache WHERE cache_key=? AND cached_at >= ?",
        (cache_key, cutoff),
    ) as cur:
        r = await cur.fetchone()
    if not r:
        return None
    return {
        "result": json.loads(r[0]) if r[0] else None,
        "source_job_id": r[1], "result_mnemos_id": r[2],
        "hit_count": r[3], "cost_saved_usd": r[4],
        "model": r[5], "provider": r[6], "cached_at": r[7],
    }


async def cache_store(db, cache_key: str, source_job_id: str, result: dict,
                      result_mnemos_id: Optional[str], model: str, provider: str,
                      cost_for_save: float):
    now = time.time()
    await db.execute(
        "INSERT INTO hive_cache (cache_key, result_json, source_job_id, result_mnemos_id, "
        "hit_count, cost_saved_usd, model, provider, cached_at, last_hit_at) "
        "VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, NULL) "
        "ON CONFLICT(cache_key) DO UPDATE SET "
        "result_json=excluded.result_json, source_job_id=excluded.source_job_id, "
        "result_mnemos_id=excluded.result_mnemos_id, cached_at=excluded.cached_at, "
        "model=excluded.model, provider=excluded.provider",
        (cache_key, json.dumps(result, default=str)[:32000], source_job_id,
         result_mnemos_id, model, provider, now),
    )


async def cache_record_hit(db, cache_key: str, cost_saved: float):
    await db.execute(
        "UPDATE hive_cache SET hit_count = hit_count + 1, "
        "cost_saved_usd = COALESCE(cost_saved_usd,0) + ?, last_hit_at = ? "
        "WHERE cache_key=?",
        (cost_saved, time.time(), cache_key),
    )


class AgentRegister(BaseModel):
    # OPEN REGISTRATION + RUNTIME→KIND ENFORCEMENT (per Kimi advisory 2026-05-23).
    # Any agent registers; identity is recorded for transparency. Kind must align
    # with runtime (prevents opencode-misregistering-as-claude). Soft fields
    # below default to "unknown" if not declared.
    #
    #   runtime          = TOOL/ORCHESTRATOR (claude-code, opencode, codex, hermes, zeroclaw, openclaw, ic-engine, mnemos, human)
    #   model            = LLM (claude-opus-4-7, grok-4.3, kimi-k2.6, deepseek-v4-pro, gpt-5.5, gemini-3.1-pro, ...)
    #   provider         = INFERENCE HOST (anthropic, xai, ngc, openai, deepseek, together, groq, gemini, local-llamacpp, ...)
    #   kind             = URN routing segment — MUST be in RUNTIME_KIND_MAP[runtime] if runtime known
    #   autonomy_level   = autonomous / confirm-risky / interactive / unknown
    runtime: str = Field("unknown", pattern=r"^[a-z][a-z0-9-]{0,31}$")
    model: str = "unknown"
    provider: str = "unknown"
    host: str
    kind: Optional[str] = Field(None, pattern=r"^[a-z][a-z0-9-]{0,31}$",
                                description="URN routing segment — defaults to runtime")
    autonomy_level: str = Field("unknown",
                                description="autonomous / confirm-risky / interactive / unknown")
    auth_method: str = Field("unknown",
                             description="subscription (Max plan), api (pay-per-token), free, unknown")
    plan_cap_usd: Optional[float] = None  # monthly cap; defaults from DEFAULT_PLAN_CAPS by auth_method
    pid: Optional[int] = None
    capabilities: Optional[list[str]] = None
    version: Optional[str] = None
    metadata: Optional[dict] = None


class AgentHeartbeat(BaseModel):
    urn: str
    status: str = "online"
    metadata: Optional[dict] = None


class JobCreate(BaseModel):
    submitter_urn: str                                # who is asking (user OR delegating agent)
    parent_job_id: Optional[str] = None
    kind: str                                          # work type (code-edit/research/review/build/etc)
    description: Optional[str] = None
    priority: int = 0                                  # higher = more urgent (default 0)
    deadline: Optional[float] = None                   # unix ts, optional SLA
    required_capabilities: Optional[list[str]] = None  # worker must have ALL of these
    eligible_kinds: Optional[list[str]] = None         # restrict to agent kinds; null = any
    eligible_hosts: Optional[list[str]] = None         # restrict to agent hosts (e.g. ["cixmini"]); null = any
    # #10 FIX (review 2026-05-23): project tag — separate from worker capabilities.
    # 'riskyeats'/'investorclaw' are PROJECTS, not capabilities. Workers don't gain/lose
    # the ability to do work because of a project label; the label is for filter+routing.
    project: Optional[str] = None
    max_retries: int = 2                               # auto-resubmit after worker reports failed (up to N times)
    # COST DISCIPLINE (per CLAUDE.md llm-usage-policy):
    max_cost_tier: str = "B"                           # adaptive tier cap: A=local/NGC first (free), B=Groq/xAI overflow (paid), C=reserve. Default B=Tier A preferred, Tier B fallback when Tier A overloaded.
    preferred_providers: Optional[list[str]] = None    # ranked preference (first match wins among tier-eligible)
    preferred_models: Optional[list[str]] = None       # ranked model preference
    # MNEMOS provenance:
    mnemos_refs: Optional[list[str]] = None            # mem_XXX ids — context/handoffs/related work the worker should consult
    # DAG support:
    depends_on: Optional[list[str]] = None             # job ids that must be status='done' before this job is dequeueable


class JobUpdate(BaseModel):
    status: str
    result: Optional[dict] = None
    claimed_by: Optional[str] = None
    tokens_in: Optional[int] = None    # workers SHOULD report token usage on done/failed for cost audit
    tokens_out: Optional[int] = None
    result_mnemos_id: Optional[str] = None   # mem_XXX id where worker stored the outcome — closes provenance loop
    # Routing re-assignment (queued/offered jobs only — use status=queued to re-route)
    eligible_kinds: Optional[list[str]] = None
    eligible_hosts: Optional[list[str]] = None


class ScheduleCreate(BaseModel):
    name: str
    interval_seconds: int = Field(..., ge=60, le=86400 * 30,
                                  description="60s minimum (avoid hot-loop); 30d maximum")
    job_template: dict   # full JobCreate body that will be submitted each tick
    enabled: bool = True


class MessagePublish(BaseModel):
    from_urn: str
    to_urn: Optional[str] = None
    in_reply_to: Optional[str] = None
    topic: str
    payload: dict


# ---------- lifecycle ----------

CLAIM_STALE_AFTER = 1800.0   # claim → still 'claimed'/'running' without update >30min ⇒ orphan
# REVIEW #2 fix: kind-specific overrides. Orchestration jobs that fan out
# + collect can legitimately run 60+ minutes without a PATCH while
# sub-jobs execute. Reaper also measures against last_update_at (any PATCH
# touches it) instead of just claimed_at — so a steady stream of progress
# patches keeps the orphan reaper at bay.
CLAIM_STALE_AFTER_BY_KIND = {
    "orchestration": 3600.0,
    "investigation": 3600.0,
    "benchmark": 3600.0,
    "migration": 7200.0,  # multi-host db / fleet migrations need wide window
}
async def reaper_task(app: FastAPI):
    while True:
        await asyncio.sleep(HEARTBEAT_REAP_INTERVAL)
        try:
            async with connect_db() as db:
                # 1. Heartbeat reaper: online/idle -> stale after 90s, offline after 5m.
                now_ts = time.time()
                stale_cutoff = now_ts - HEARTBEAT_STALE_AFTER
                offline_cutoff = now_ts - HEARTBEAT_OFFLINE_AFTER
                async with db.execute(
                    "SELECT urn FROM agents "
                    "WHERE status IN ('online','idle','error') "
                    "AND last_heartbeat < ? AND last_heartbeat >= ?",
                    (stale_cutoff, offline_cutoff),
                ) as cur:
                    stale = [row[0] async for row in cur]
                if stale:
                    await db.executemany(
                        "UPDATE agents SET status='stale' WHERE urn=? AND status != 'offline'",
                        [(u,) for u in stale],
                    )
                    await db.commit()
                    for urn in stale:
                        await emit_event(db, "agent.stale", {"urn": urn, "reason": "heartbeat_stale"})

                async with db.execute(
                    "SELECT urn FROM agents WHERE status != 'offline' AND last_heartbeat < ?",
                    (offline_cutoff,),
                ) as cur:
                    dead = [row[0] async for row in cur]
                if dead:
                    for urn in dead:
                        async with db.execute(
                            "SELECT id FROM jobs WHERE claimed_by=? AND status IN ('offered','claimed','running')",
                            (urn,),
                        ) as cur:
                            assigned_jobs = [row[0] async for row in cur]
                        await db.execute(
                            "UPDATE jobs SET status='queued', claimed_by=NULL, claimed_at=NULL, "
                            "claimed_runtime=NULL, claimed_model=NULL, claimed_provider=NULL, "
                            "claimed_cost_tier=NULL, claim_lease_expires_at=NULL "
                            "WHERE claimed_by=? AND status IN ('offered','claimed','running')",
                            (urn,),
                        )
                        await db.execute(
                            "UPDATE agents SET status='offline' WHERE urn=?",
                            (urn,),
                        )
                        for job_id in assigned_jobs:
                            await emit_event(db, "job.unclaimed", {
                                "id": job_id, "prior_claimer": urn,
                                "reason": "agent_offline_heartbeat_timeout",
                            })
                    await db.commit()
                    for urn in dead:
                        await emit_event(db, "agent.offline", {"urn": urn, "reason": "heartbeat_timeout"})
                # 2. ORPHAN CLAIM RECOVERY — jobs claimed by dead/stale workers go back to queue.
                # REVIEW #2 fix: measure staleness against COALESCE(last_update_at, claimed_at)
                # so steady PATCH progress keeps reaper at bay. Kind-specific cutoffs let
                # orchestration / investigation / migration jobs run longer than default 30min.
                now_ts = time.time()
                default_cutoff = now_ts - CLAIM_STALE_AFTER
                async with db.execute(
                    "SELECT j.id, j.kind, j.claimed_by, j.claimed_at, j.last_update_at, "
                    "j.claim_lease_expires_at "
                    "FROM jobs j "
                    "LEFT JOIN agents a ON a.urn = j.claimed_by "
                    "WHERE j.status IN ('claimed','running') "
                    "AND ( a.status = 'offline' OR a.urn IS NULL OR "
                    "      j.claim_lease_expires_at <= ? OR "
                    "      COALESCE(j.last_update_at, j.claimed_at) < ? )",
                    (now_ts, default_cutoff,)
                ) as cur:
                    candidates = [(r[0], r[1], r[2], r[3], r[4], r[5]) async for r in cur]
                # Apply kind-specific override: keep job if its kind has a longer TTL
                # AND its latest progress timestamp is within that window.
                orphans = []
                for jid, jkind, claimer, c_at, u_at, lease_exp in candidates:
                    if lease_exp is not None and lease_exp <= now_ts:
                        orphans.append((jid, claimer))
                        continue
                    kind_ttl = CLAIM_STALE_AFTER_BY_KIND.get(jkind, CLAIM_STALE_AFTER)
                    latest = u_at if u_at is not None else (c_at or 0)
                    if (now_ts - latest) >= kind_ttl:
                        orphans.append((jid, claimer))
                if orphans:
                    for job_id, claimer in orphans:
                        await db.execute(
                            "UPDATE jobs SET status='queued', claimed_by=NULL, claimed_at=NULL, "
                            "claimed_runtime=NULL, claimed_model=NULL, claimed_provider=NULL, "
                            "claimed_cost_tier=NULL, claim_lease_expires_at=NULL "
                            "WHERE id=? AND status IN ('claimed','running')",
                            (job_id,))
                        await emit_event(db, "job.unclaimed", {
                            "id": job_id, "prior_claimer": claimer,
                            "reason": "worker_offline_or_stale_claim",
                        })
                    await db.commit()
                # 3. Scheduler — fire due interval-based jobs
                now_ts = time.time()
                async with db.execute(
                    "SELECT id, name, created_by_urn, interval_seconds, job_template, fire_count "
                    "FROM scheduled_jobs WHERE enabled=1 AND next_fire_at <= ?",
                    (now_ts,)
                ) as cur:
                    due = [tuple(r) async for r in cur]
                for sched_id, sname, sub_urn, interval, tpl_json, fcount in due:
                    try:
                        tpl = json.loads(tpl_json)
                        tpl["submitter_urn"] = sub_urn
                        # synth a JobCreate + go through the cache+role machinery
                        from fastapi import HTTPException as _HE
                        await create_job(JobCreate(**tpl))  # type: ignore
                        await db.execute(
                            "UPDATE scheduled_jobs SET last_fired_at=?, next_fire_at=?, "
                            "fire_count=fire_count+1 WHERE id=?",
                            (now_ts, now_ts + interval, sched_id),
                        )
                        await emit_event(db, "schedule.fired", {
                            "schedule_id": sched_id, "name": sname,
                            "fire_count": fcount + 1, "next_at": now_ts + interval,
                        })
                    except Exception as se:
                        print(f"scheduler error {sched_id}: {se}", flush=True)
                if due:
                    await db.commit()
                # 4. Auto-cancel stale jobs (queued > 7 days untouched)
                stale_job_cutoff = time.time() - 7 * 24 * 3600
                async with db.execute(
                    "SELECT id FROM jobs WHERE status='queued' AND started_at < ?",
                    (stale_job_cutoff,)
                ) as cur:
                    stale_ids = [r[0] async for r in cur]
                if stale_ids:
                    await db.executemany(
                        "UPDATE jobs SET status='cancelled', ended_at=? WHERE id=?",
                        [(time.time(), j) for j in stale_ids],
                    )
                    for j in stale_ids:
                        await emit_event(db, "job.cancelled", {"id": j, "reason": "stale_>7d"})
                    await db.commit()
                # 5. Purge old events
                retain_cutoff = time.time() - EVENTS_RETAIN_HOURS * 3600
                await db.execute("DELETE FROM events WHERE ts < ?", (retain_cutoff,))
                await db.commit()
        except Exception as e:
            print(f"reaper error: {e}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with connect_db() as db:
        await db.executescript("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
        await db.executescript(SCHEMA)
        # graeae:schema-drift (2026-05-23): additive migrations for live DBs.
        # New-column indexes stay out of SCHEMA because CREATE INDEX fails on
        # old live tables until the corresponding ALTER TABLE has run.
        async def _ensure_column(conn, table: str, col: str, ddl_fragment: str) -> None:
            cursor = await conn.execute(f"PRAGMA table_info({table})")
            cols = {row[1] for row in await cursor.fetchall()}
            if col not in cols:
                await conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl_fragment}")

        async def _ensure_agents_status_allows_stale(conn) -> None:
            async with conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='agents'"
            ) as cur:
                row = await cur.fetchone()
            table_sql = row[0] if row else ""
            if "stale" in table_sql:
                return
            cols = (
                "urn", "kind", "host", "session_id", "pid", "capabilities",
                "version", "started_at", "last_heartbeat", "status", "metadata",
                "runtime", "model", "provider", "autonomy_level", "cost_tier",
                "current_load", "auth_method", "plan_cap_usd", "plan_period_used_usd",
            )
            col_list = ", ".join(cols)
            await conn.execute("ALTER TABLE agents RENAME TO agents__pre_stale_migration")
            await conn.execute(
                "CREATE TABLE agents ("
                "urn TEXT PRIMARY KEY, "
                "kind TEXT NOT NULL, "
                "host TEXT NOT NULL, "
                "session_id TEXT NOT NULL, "
                "pid INTEGER, "
                "capabilities TEXT, "
                "version TEXT, "
                "started_at REAL NOT NULL, "
                "last_heartbeat REAL NOT NULL, "
                "status TEXT NOT NULL CHECK(status IN ('online','idle','stale','offline','error')), "
                "metadata TEXT, "
                "runtime TEXT, "
                "model TEXT, "
                "provider TEXT, "
                "autonomy_level TEXT, "
                "cost_tier TEXT, "
                "current_load TEXT, "
                "auth_method TEXT, "
                "plan_cap_usd REAL, "
                "plan_period_used_usd REAL DEFAULT 0)"
            )
            await conn.execute(
                f"INSERT INTO agents ({col_list}) SELECT {col_list} FROM agents__pre_stale_migration"
            )
            await conn.execute("DROP TABLE agents__pre_stale_migration")

        live_columns = (
            # agents — v0.2 cost/runtime/autonomy columns
            ("agents", "runtime", "runtime TEXT"),
            ("agents", "model", "model TEXT"),
            ("agents", "provider", "provider TEXT"),
            ("agents", "autonomy_level", "autonomy_level TEXT"),
            ("agents", "cost_tier", "cost_tier TEXT"),
            ("agents", "current_load", "current_load TEXT"),
            ("agents", "auth_method", "auth_method TEXT"),
            ("agents", "plan_cap_usd", "plan_cap_usd REAL"),
            ("agents", "plan_period_used_usd", "plan_period_used_usd REAL DEFAULT 0"),
            # jobs — project tag + v0.2 cost/autonomy/retry columns
            ("jobs", "project", "project TEXT"),
            ("jobs", "eligible_hosts", "eligible_hosts TEXT"),
            ("jobs", "required_autonomy", "required_autonomy TEXT"),
            ("jobs", "max_cost_tier", "max_cost_tier TEXT"),
            ("jobs", "preferred_providers", "preferred_providers TEXT"),
            ("jobs", "preferred_models", "preferred_models TEXT"),
            ("jobs", "claimed_runtime", "claimed_runtime TEXT"),
            ("jobs", "claimed_model", "claimed_model TEXT"),
            ("jobs", "claimed_provider", "claimed_provider TEXT"),
            ("jobs", "claimed_cost_tier", "claimed_cost_tier TEXT"),
            ("jobs", "tokens_in", "tokens_in INTEGER"),
            ("jobs", "tokens_out", "tokens_out INTEGER"),
            ("jobs", "estimated_cost_usd", "estimated_cost_usd REAL"),
            ("jobs", "mnemos_refs", "mnemos_refs TEXT"),
            ("jobs", "result_mnemos_id", "result_mnemos_id TEXT"),
            ("jobs", "required_resources", "required_resources TEXT"),
            ("jobs", "claimed_host_caps", "claimed_host_caps TEXT"),
            ("jobs", "tags", "tags TEXT"),
            ("jobs", "depends_on", "depends_on TEXT"),
            ("jobs", "retry_count", "retry_count INTEGER NOT NULL DEFAULT 0"),
            ("jobs", "max_retries", "max_retries INTEGER NOT NULL DEFAULT 2"),
            ("jobs", "retry_backoff_until", "retry_backoff_until REAL"),
            ("jobs", "last_update_at", "last_update_at REAL"),
            ("jobs", "claim_lease_expires_at", "claim_lease_expires_at REAL"),
        )
        for table, col, ddl_fragment in live_columns:
            await _ensure_column(db, table, col, ddl_fragment)
        await _ensure_agents_status_allows_stale(db)
        # Idempotent index re-creates (in case live DB pre-dates an index).
        live_indexes = (
            "CREATE INDEX IF NOT EXISTS idx_agents_status      ON agents(status)",
            "CREATE INDEX IF NOT EXISTS idx_agents_kind        ON agents(kind)",
            "CREATE INDEX IF NOT EXISTS idx_agents_runtime     ON agents(runtime)",
            "CREATE INDEX IF NOT EXISTS idx_agents_model       ON agents(model)",
            "CREATE INDEX IF NOT EXISTS idx_agents_provider    ON agents(provider)",
            "CREATE INDEX IF NOT EXISTS idx_agents_cost_tier   ON agents(cost_tier)",
            "CREATE INDEX IF NOT EXISTS idx_agents_auth_method ON agents(auth_method)",
            "CREATE INDEX IF NOT EXISTS idx_jobs_project           ON jobs(project)",
            "CREATE INDEX IF NOT EXISTS idx_jobs_autonomy          ON jobs(required_autonomy)",
            "CREATE INDEX IF NOT EXISTS idx_jobs_max_cost_tier     ON jobs(max_cost_tier)",
            "CREATE INDEX IF NOT EXISTS idx_jobs_claimed_provider  ON jobs(claimed_provider)",
            "CREATE INDEX IF NOT EXISTS idx_jobs_claimed_tier      ON jobs(claimed_cost_tier)",
            "CREATE INDEX IF NOT EXISTS idx_jobs_backoff           ON jobs(retry_backoff_until)",
            "CREATE INDEX IF NOT EXISTS idx_jobs_claim_lease       ON jobs(claim_lease_expires_at)",
            "CREATE TABLE IF NOT EXISTS job_audit_log ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL, ts REAL NOT NULL, "
            "actor_urn TEXT, old_status TEXT, new_status TEXT, old_claimed_by TEXT, "
            "new_claimed_by TEXT, patch TEXT NOT NULL)",
            "CREATE INDEX IF NOT EXISTS idx_job_audit_job_ts ON job_audit_log(job_id, ts)",
        )
        for stmt in live_indexes:
            await db.execute(stmt)
        await db.commit()
    task = asyncio.create_task(reaper_task(app))
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="GRAEAE Hive Mind", version="0.1.0", lifespan=lifespan)


# ---------- endpoints ----------

@app.get("/health")
async def health():
    return {"status": "healthy", "ts": time.time(), "service": "graeae-hive-mind", "version": "0.1.0"}


@app.get("/")
async def dashboard():
    """Minimal HTML+JS dashboard. Auto-refreshes /v1/agents + /v1/jobs + /v1/stats/* + SSE."""
    from fastapi.responses import HTMLResponse
    p = "/srv/agent-bus/dashboard.html"
    try:
        with open(p) as f:
            return HTMLResponse(f.read(), headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"})
    except FileNotFoundError:
        return HTMLResponse(f"<h1>dashboard.html missing</h1><p>Expected at {p}</p>", status_code=404)


@app.post("/v1/agents/register")
async def register(req: AgentRegister):
    # #5 FIX (review 2026-05-23): reject minimally-incomplete registrations with 422 so
    # callers see the error instead of getting a null URN that bricks their session.
    if not req.host or not req.host.strip():
        raise HTTPException(422, "host is required (e.g., 'studio' or hostname -s)")
    runtime = (req.runtime or "unknown").lower()
    kind = (req.kind or runtime).lower()
    if runtime == "unknown" and not req.kind:
        raise HTTPException(
            422, "must provide runtime (claude-code/opencode/codex/...) OR explicit kind. "
                 "Defaulting to 'unknown' was masking session-bricking misregistrations."
        )
    # If kind was provided but runtime omitted/unknown, default runtime to the
    # kind when the kind matches a known runtime identifier. Previously a
    # kind-only registration left runtime='unknown', which cascaded into
    # provider='unknown' + cost_tier='C', so default-A jobs never dequeued
    # AND the WORKER_ONLY_RUNTIMES role check stopped applying (unknown is
    # in ORCHESTRATOR_RUNTIMES). README, test/smoke.sh
    # all register kind-only. Fix: lift kind to runtime when safe.
    if runtime == "unknown" and req.kind:
        if kind in RUNTIME_KIND_MAP:
            runtime = kind
        else:
            # kind is not a recognised runtime alias — keep runtime='unknown'
            # but make the registration fail loud so callers fix it.
            raise HTTPException(
                422,
                f"kind={kind!r} provided without runtime; cannot infer "
                f"runtime. Provide an explicit runtime from "
                f"{sorted(RUNTIME_KIND_MAP.keys())}.",
            )
    allowed = RUNTIME_KIND_MAP.get(runtime, {runtime, "unknown"})
    if runtime != "unknown" and kind not in allowed and kind != runtime:
        raise HTTPException(
            status_code=422,
            detail=(
                f"identity-mismatch: runtime={runtime!r} cannot register as kind={kind!r}. "
                f"Allowed kinds for this runtime: {sorted(allowed)}. "
                f"Set kind to one of those (or omit it) — fixes opencode-misregistering-as-claude per advisory."
            ),
        )
    autonomy = (req.autonomy_level or "unknown").lower()
    if autonomy not in AUTONOMY_LEVELS:
        raise HTTPException(422, f"autonomy_level must be one of {sorted(AUTONOMY_LEVELS)}, got {autonomy!r}")
    auth_method = (req.auth_method or "unknown").lower()
    if auth_method not in AUTH_METHODS:
        raise HTTPException(422, f"auth_method must be one of {sorted(AUTH_METHODS)}, got {auth_method!r}")
    plan_cap_usd = req.plan_cap_usd if req.plan_cap_usd is not None else DEFAULT_PLAN_CAPS.get(auth_method, 50.0)
    provider = (req.provider or "unknown").lower()
    model = (req.model or "unknown").lower()
    tier = cost_tier_for(provider)
    # User directive 2026-05-26: allow opencode + codex to claim work at ALL
    # tiers (A/B/C) regardless of provider. They are flexible multi-provider
    # agentic CLIs and the tier-ceiling is a routing aid, not a real cost gate
    # for them.
    if kind in {"opencode", "codex", "doctor"}:
        tier = "A"
    session_id = str(uuid.uuid4())
    urn = make_urn(kind, req.host, session_id)
    now = time.time()
    async with connect_db() as db:
        await db.execute(
            "INSERT INTO agents (urn, kind, runtime, model, provider, cost_tier, autonomy_level, "
            "auth_method, plan_cap_usd, plan_period_used_usd, "
            "host, session_id, pid, capabilities, version, started_at, last_heartbeat, status, metadata) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, 'online', ?)",
            (
                urn, kind, runtime, model, provider, tier, autonomy,
                auth_method, plan_cap_usd,
                req.host, session_id, req.pid,
                json.dumps(req.capabilities) if req.capabilities else None,
                req.version, now, now,
                json.dumps(req.metadata) if req.metadata else None,
            ),
        )
        await db.commit()
        await emit_event(db, "agent.online", {
            "urn": urn, "kind": kind, "runtime": runtime,
            "model": model, "provider": provider, "cost_tier": tier,
            "host": req.host, "autonomy_level": autonomy,
        })
    return {
        "urn": urn, "session_id": session_id, "registered_at": now,
        "kind": kind, "runtime": runtime, "model": model,
        "provider": provider, "cost_tier": tier, "autonomy_level": autonomy,
        "auth_method": auth_method, "plan_cap_usd": plan_cap_usd,
    }


@app.post("/v1/agents/heartbeat")
async def heartbeat(req: AgentHeartbeat):
    now = time.time()
    status = (req.status or "online").lower()
    if status not in VALID_AGENT_STATUSES:
        raise HTTPException(422, f"agent status must be one of {sorted(VALID_AGENT_STATUSES)}, got {status!r}")
    async with connect_db() as db:
        if req.metadata is not None:
            async with db.execute(
                "SELECT metadata FROM agents WHERE urn=?",
                (req.urn,),
            ) as meta_cur:
                meta_row = await meta_cur.fetchone()
            if not meta_row:
                raise HTTPException(404, f"agent not found: {req.urn}")
            try:
                existing_meta = json.loads(meta_row[0]) if meta_row[0] else {}
            except Exception:
                existing_meta = {}
            if not isinstance(existing_meta, dict):
                existing_meta = {}
            existing_meta.update(req.metadata)
            cur = await db.execute(
                "UPDATE agents SET last_heartbeat=?, status=?, metadata=? WHERE urn=?",
                (now, status, json.dumps(existing_meta, default=str), req.urn),
            )
        else:
            cur = await db.execute(
                "UPDATE agents SET last_heartbeat=?, status=? WHERE urn=?",
                (now, status, req.urn),
            )
        await db.commit()
        if cur.rowcount == 0:
            raise HTTPException(404, f"agent not found: {req.urn}")
    return {"ack": True, "ts": now}


@app.get("/v1/agents")
async def list_agents(
    status: Optional[str] = None,
    kind: Optional[str] = None,
    host: Optional[str] = None,
    runtime: Optional[str] = None,
    pid: Optional[int] = None,
    cost_tier: Optional[str] = None,
    include_offline: bool = False,
):
    sql = ("SELECT urn, kind, host, status, last_heartbeat, capabilities, version, metadata, "
           "pid, runtime, model, provider, cost_tier, autonomy_level "
           "FROM agents WHERE 1=1")
    args: list = []
    if status:
        if status not in VALID_AGENT_STATUSES:
            raise HTTPException(422, f"status must be one of {sorted(VALID_AGENT_STATUSES)}, got {status!r}")
        sql += " AND status=?";    args.append(status)
    elif not include_offline:
        sql += " AND status='online'"
    if kind:      sql += " AND kind=?";      args.append(kind)
    if host:      sql += " AND host=?";      args.append(host)
    if runtime:   sql += " AND runtime=?";   args.append(runtime)
    if pid is not None: sql += " AND pid=?"; args.append(pid)
    if cost_tier: sql += " AND cost_tier=?"; args.append(cost_tier)
    sql += " ORDER BY last_heartbeat DESC"
    rows = []
    async with connect_db() as db:
        async with db.execute(sql, args) as cur:
            async for r in cur:
                meta = json.loads(r[7]) if r[7] else {}
                # build display name: kind@host[pid] cwd:cwd_basename — distinguishes multiple sessions on same host
                cwd = (meta or {}).get("cwd") if isinstance(meta, dict) else None
                cwd_short = cwd.split("/")[-1] if cwd else None
                display = f"{r[1]}@{r[2]}"
                if r[8] is not None:
                    display += f"[pid={r[8]}]"
                if cwd_short:
                    display += f" cwd={cwd_short}"
                rows.append({
                    "urn": r[0], "kind": r[1], "host": r[2], "status": r[3],
                    "last_heartbeat": r[4],
                    "capabilities": json.loads(r[5]) if r[5] else None,
                    "version": r[6],
                    "metadata": meta,
                    "pid": r[8],
                    "runtime": r[9],
                    "model": r[10],
                    "provider": r[11],
                    "cost_tier": r[12],
                    "autonomy_level": r[13],
                    "display": display,
                })
    return {"count": len(rows), "agents": rows}


@app.get("/v1/agents/stats")
async def agent_stats():
    counts = {status: 0 for status in VALID_AGENT_STATUSES}
    async with connect_db() as db:
        async with db.execute(
            "SELECT status, COUNT(*) FROM agents GROUP BY status"
        ) as cur:
            async for status, count in cur:
                counts[status] = count
    total = sum(counts.values())
    return {
        "online": counts.get("online", 0),
        "stale": counts.get("stale", 0),
        "offline": counts.get("offline", 0),
        "total": total,
    }


@app.get("/v1/hosts")
async def list_hosts(include_stale: bool = False):
    """Aggregate view of all system-watcher agents: specs + current load.

    Returns one entry per host (latest system agent wins if multiple).
    Agents that last heartbeated > 60s ago are flagged as stale unless
    include_stale=true, in which case all are returned.
    Used by workers to make resource-aware routing decisions.
    """
    cutoff = time.time() - 60  # 60s stale threshold
    sql = ("SELECT urn, host, status, last_heartbeat, capabilities, metadata "
           "FROM agents WHERE kind='system'")
    if not include_stale:
        sql += " AND last_heartbeat >= ?"
        args: list = [cutoff]
    else:
        args = []
    sql += " ORDER BY last_heartbeat DESC"
    seen_hosts: dict[str, dict] = {}
    async with connect_db() as db:
        async with db.execute(sql, args) as cur:
            async for r in cur:
                h = r[1]
                if h in seen_hosts:
                    continue  # already have a newer entry for this host
                meta = json.loads(r[5]) if r[5] else {}
                specs = meta.get("specs", {})
                load  = meta.get("load", {})
                age_s = time.time() - (r[3] or 0)
                seen_hosts[h] = {
                    "host":           h,
                    "urn":            r[0],
                    "status":         r[2],
                    "last_heartbeat": r[3],
                    "age_s":          round(age_s, 1),
                    "stale":          age_s > 60,
                    "capabilities":   json.loads(r[4]) if r[4] else [],
                    # static specs (from registration)
                    "cpu_model":      specs.get("cpu_model"),
                    "cpu_threads":    specs.get("cpu_threads") or specs.get("cpu_count"),
                    "ram_gb":         specs.get("ram_gb"),
                    "os":             specs.get("os"),
                    "arch":           specs.get("arch"),
                    "gpus":           specs.get("gpus", []),
                    "has_docker":     specs.get("has_docker"),
                    "has_podman":     specs.get("has_podman"),
                    "has_npu":        specs.get("has_npu", False),
                    # dynamic load (updated each heartbeat)
                    "load_1min":      load.get("load_1min"),
                    "load_5min":      load.get("load_5min"),
                    "ram_used_pct":   load.get("ram_used_pct"),
                    "srv_free_gb":    load.get("srv_free_gb"),
                    "uptime_sec":     load.get("uptime_sec"),
                    # Multi-volume + cpu utilization (added 2026-05-26)
                    "volumes":        load.get("volumes") or [],
                    "cpu_used_pct":   load.get("cpu_used_pct"),
                }
    hosts = sorted(seen_hosts.values(), key=lambda x: x["host"])
    return {"count": len(hosts), "hosts": hosts}


@app.get("/v1/agents/{urn_path:path}/throttle")
async def agent_throttle(urn_path: str):
    """Inspect an agent's plan-cap throttle state."""
    async with connect_db() as db:
        async with db.execute(
            "SELECT urn, kind, runtime, auth_method, plan_cap_usd, plan_period_used_usd "
            "FROM agents WHERE urn=?", (urn_path,)) as cur:
            row = await cur.fetchone()
    if not row:
        raise HTTPException(404, f"agent not found: {urn_path}")
    used = row[5] or 0
    cap = row[4] or 0
    pct = (100 * used / cap) if cap else 0
    return {
        "urn": row[0], "kind": row[1], "runtime": row[2],
        "auth_method": row[3], "plan_cap_usd": cap,
        "plan_period_used_usd": round(used, 4),
        "plan_period_pct": round(pct, 1),
        "throttled": row[3] == "subscription" and pct >= 85.0,
        "headroom_pct": THROTTLE_HEADROOM * 100,
    }


@app.post("/v1/agents/{urn_path:path}/plan-reset")
async def reset_plan_period(urn_path: str):
    """Operator zeros the MTD usage — call monthly on billing rollover."""
    async with connect_db() as db:
        cur = await db.execute(
            "UPDATE agents SET plan_period_used_usd = 0 WHERE urn=?", (urn_path,))
        await db.commit()
    if cur.rowcount == 0:
        raise HTTPException(404, f"agent not found: {urn_path}")
    return {"ok": True, "urn": urn_path, "reset_at": time.time()}


@app.get("/v1/agents/whoami")
async def whoami(host: str, pid: int):
    """Help a session find ITS OWN urn by (host, pid) — most recent online registration wins.
    Useful when a session forgets its urn after restart and needs to re-discover its identity."""
    async with connect_db() as db:
        async with db.execute(
            "SELECT urn, kind, runtime, model, provider, cost_tier, autonomy_level, started_at "
            "FROM agents WHERE host=? AND pid=? AND status='online' "
            "ORDER BY started_at DESC LIMIT 1",
            (host, pid),
        ) as cur:
            r = await cur.fetchone()
    if not r:
        raise HTTPException(404, f"no online agent matches host={host} pid={pid}")
    return {
        "urn": r[0], "kind": r[1], "runtime": r[2], "model": r[3],
        "provider": r[4], "cost_tier": r[5], "autonomy_level": r[6],
        "started_at": r[7],
    }


@app.post("/v1/jobs")
async def create_job(req: JobCreate):
    """Submit work to the triage queue. No agent assignment — workers self-claim via /v1/jobs/next.

    ROLE ENFORCEMENT (user directive 2026-05-23):
    Worker runtimes (opencode/codex/hermes/claw-family/ic-engine) are CLAIMERS, not submitters.
    Posting jobs requires submitter to be registered with an orchestrator runtime (claude-code/human/mnemos).
    Workers attempting to POST jobs get 403 — they should call /v1/jobs/next instead.
    """
    job_id = uuidv7()
    now = time.time()
    max_cost_tier = (req.max_cost_tier or "B").upper()
    if max_cost_tier not in COST_TIERS:
        raise HTTPException(422, f"max_cost_tier must be one of {COST_TIERS}, got {max_cost_tier!r}")
    if req.mnemos_refs:
        bad = [r for r in req.mnemos_refs if not (isinstance(r, str) and r.startswith("mem_"))]
        if bad:
            raise HTTPException(422, f"mnemos_refs must be mem_XXX ids — bad entries: {bad}")
    # DAG: validate depends_on targets exist + no self-cycle
    if req.depends_on:
        if job_id in req.depends_on:
            raise HTTPException(422, "depends_on cannot include self")
        async with connect_db() as _vd:
            placeholders = ",".join("?" * len(req.depends_on))
            async with _vd.execute(
                f"SELECT id FROM jobs WHERE id IN ({placeholders})",
                tuple(req.depends_on),
            ) as cur:
                found = {row[0] async for row in cur}
            missing = [d for d in req.depends_on if d not in found]
            if missing:
                raise HTTPException(422, f"depends_on references unknown job ids: {missing}")
    # AUTO HOST AFFINITY: inject eligible_hosts based on kind prefix if not already set.
    if req.eligible_hosts is None:
        for prefix, hosts in KIND_HOST_AFFINITY.items():
            if req.kind.startswith(prefix):
                req = req.model_copy(update={"eligible_hosts": hosts})
                break
    # ROLE ENFORCEMENT: check submitter runtime BEFORE the cache lookup so
    # worker-only runtimes get a 403 consistently for both cache hits and
    # cache misses. Unregistered submitter_urn values are rejected too; the
    # endpoint contract says jobs are submitted by registered orchestrators.
    await require_orchestrator_submitter(req.submitter_urn)

    # RESULT-CACHE CHECK: identical (kind, description, max_cost_tier, required_caps) within TTL → return cached result, mark new job done immediately
    ck = cache_key_for(req.kind, req.description, max_cost_tier, req.required_capabilities)
    async with connect_db() as db:
        cached = await cache_lookup(db, ck)
        if cached:
            # short-circuit: store job as done with cached result + cost=0
            await db.execute(
                "INSERT INTO jobs (id, submitter_urn, parent_job_id, kind, description, priority, deadline, "
                "required_capabilities, eligible_kinds, eligible_hosts, project, max_cost_tier, preferred_providers, preferred_models, "
                "mnemos_refs, status, started_at, ended_at, result, claimed_provider, claimed_model, "
                "claimed_cost_tier, estimated_cost_usd, result_mnemos_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'done', ?, ?, ?, ?, ?, 'A', 0, ?)",
                (
                    job_id, req.submitter_urn, req.parent_job_id, req.kind, req.description,
                    req.priority, req.deadline,
                    json.dumps(req.required_capabilities) if req.required_capabilities else None,
                    json.dumps(req.eligible_kinds) if req.eligible_kinds else None,
                    json.dumps(req.eligible_hosts) if req.eligible_hosts else None,
                    req.project,
                    max_cost_tier,
                    json.dumps(req.preferred_providers) if req.preferred_providers else None,
                    json.dumps(req.preferred_models) if req.preferred_models else None,
                    json.dumps(req.mnemos_refs) if req.mnemos_refs else None,
                    now, now,
                    json.dumps({**(cached["result"] or {}), "cache_hit": True,
                                "source_job_id": cached["source_job_id"]}),
                    cached["provider"], cached["model"],
                    cached.get("result_mnemos_id"),
                ),
            )
            # record cache hit with estimated cost-saving (use prior job's cost or fallback)
            saved = 0.01  # conservative fallback if no token data
            await cache_record_hit(db, ck, saved)
            await db.commit()
            await emit_event(db, "job.cached", {
                "id": job_id, "source_job_id": cached["source_job_id"],
                "kind": req.kind, "cost_saved_usd": saved,
            })
            return {
                "id": job_id, "created_at": now,
                "status": "done", "cache_hit": True,
                "source_job_id": cached["source_job_id"],
                "result": cached["result"],
                "result_mnemos_id": cached.get("result_mnemos_id"),
            }
        # role check moved to the top of create_job() so cache hits and
        # cache misses share the same enforcement (was bypassable for
        # worker-runtimes when cache had a relevant entry; see comment
        # near the role-enforcement block above).
        await db.execute(
            "INSERT INTO jobs (id, submitter_urn, parent_job_id, kind, description, priority, deadline, "
            "required_capabilities, eligible_kinds, eligible_hosts, project, max_cost_tier, preferred_providers, preferred_models, "
            "mnemos_refs, depends_on, max_retries, status, started_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)",
            (
                job_id, req.submitter_urn, req.parent_job_id, req.kind, req.description,
                req.priority, req.deadline,
                json.dumps(req.required_capabilities) if req.required_capabilities else None,
                json.dumps(req.eligible_kinds) if req.eligible_kinds else None,
                json.dumps(req.eligible_hosts) if req.eligible_hosts else None,
                req.project,
                max_cost_tier,
                json.dumps(req.preferred_providers) if req.preferred_providers else None,
                json.dumps(req.preferred_models) if req.preferred_models else None,
                json.dumps(req.mnemos_refs) if req.mnemos_refs else None,
                json.dumps(req.depends_on) if req.depends_on else None,
                int(req.max_retries),
                now,
            ),
        )
        await db.commit()
        await emit_event(db, "job.queued", {
            "id": job_id, "submitter": req.submitter_urn, "kind": req.kind,
            "description": req.description, "priority": req.priority,
            "required_capabilities": req.required_capabilities,
            "eligible_kinds": req.eligible_kinds,
            "max_cost_tier": max_cost_tier,
            "mnemos_refs": req.mnemos_refs,
        })
    return {"id": job_id, "created_at": now, "mnemos_refs": req.mnemos_refs}


@app.post("/v1/jobs/next")
async def dequeue_next_job(agent_urn: str):
    """Atomic dequeue: highest-priority queued job that this agent is eligible for.

    Self-assignment for swarm: agent calls this in its main loop. Server:
      1. Looks up agent's kind + capabilities.
      2. Finds top-priority queued job where (eligible_kinds covers agent kind) AND (required_capabilities subset of agent capabilities).
      3. Atomically claims it (status='claimed', claimed_by=agent_urn, claimed_at=now).
      4. Returns job to caller; or 204 No Content if nothing matches.

    Race-safe via SQLite immediate-mode UPDATE...WHERE rowid=(SELECT...LIMIT 1) under a transaction.
    """
    now = time.time()
    async with connect_db() as db:
        async with db.execute(
            "SELECT kind, capabilities, runtime, model, provider, cost_tier, "
            "auth_method, plan_cap_usd, plan_period_used_usd, status "
            "FROM agents WHERE urn=?",
            (agent_urn,),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            raise HTTPException(404, f"claim failed: agent not registered: {agent_urn}")
        agent_kind, caps_json, a_runtime, a_model, a_provider, a_tier, a_auth, a_cap, a_used, agent_status = row
        if agent_status not in ACTIVE_AGENT_STATUSES:
            raise HTTPException(409, f"claim failed: agent status is {agent_status!r}, not online/idle: {agent_urn}")
        # Host denylist — refuse to offer jobs to known-broken hosts.
        # Extracts host short-name from urn:agent:<kind>:<host>:<sid>.
        try:
            urn_parts = agent_urn.split(":")
            urn_host = (urn_parts[3] if len(urn_parts) >= 4 else "").lower()
        except Exception:
            urn_host = ""
        if urn_host in HOST_DENYLIST:
            # Don't return 404 (worker would retry frantically) — return 204
            # so the worker treats it as 'no work, idle wait' and stops flooding.
            from fastapi.responses import Response as _DRsp
            return _DRsp(status_code=204, headers={
                "X-Hive-Claim-Result": "host_denylisted",
                "X-Hive-Claim-Detail": f"host {urn_host} is in HIVE_HOST_DENYLIST",
            })
        agent_caps = set(json.loads(caps_json)) if caps_json else set()
        eligible_aliases = agent_kind_aliases(agent_kind, a_runtime)
        a_tier = a_tier or "C"
        a_auth = (a_auth or "unknown").lower()
        # Throttle: subscription agents over 85% MTD usage get refused tier-B/C jobs;
        # they can still claim tier-A (free) work. Forces API/free fallback as plan cap nears.
        sub_throttled = (a_auth == "subscription" and a_cap and a_used and a_used >= THROTTLE_HEADROOM * a_cap)

        await db.execute("BEGIN IMMEDIATE")
        committed = False
        try:
            async with db.execute(
                "SELECT id, kind, description, priority, deadline, required_capabilities, eligible_kinds, "
                "eligible_hosts, submitter_urn, parent_job_id, started_at, max_cost_tier, preferred_providers, preferred_models, "
                "mnemos_refs, depends_on "
                "FROM jobs WHERE status='queued' "
                "AND (retry_backoff_until IS NULL OR retry_backoff_until <= ?) "
                "ORDER BY priority DESC, started_at ASC",
                (time.time(),)
            ) as cur:
                # parse agent host from URN: urn:agent:<kind>:<host>:<session_id>
                urn_parts = agent_urn.split(":")
                agent_host = urn_parts[3] if len(urn_parts) > 3 else None
                async for r in cur:
                    (job_id, j_kind, j_desc, j_prio, j_dead, j_caps_json, j_kinds_json,
                     j_hosts_json, j_sub, j_par, j_started, j_max_tier, j_pref_providers, j_pref_models,
                     j_mnemos_refs, j_deps_json) = r
                    # DAG gate: all depends_on must be status='done'
                    if j_deps_json:
                        deps = json_list(j_deps_json)
                        if deps:
                            ph = ",".join("?" * len(deps))
                            async with db.execute(
                                f"SELECT COUNT(*) FROM jobs WHERE id IN ({ph}) AND status='done'",
                                tuple(deps),
                            ) as dc:
                                done_count = (await dc.fetchone())[0]
                            if done_count < len(deps):
                                continue  # parent not yet done; skip
                    # filter: eligible_kinds
                    if j_kinds_json:
                        kinds = set(json_list(j_kinds_json))
                        if kinds and "*" not in kinds and not kinds.intersection(eligible_aliases):
                            continue
                    # filter: eligible_hosts (host affinity)
                    if j_hosts_json:
                        hosts = set(json_list(j_hosts_json))
                        if hosts and "*" not in hosts and agent_host not in hosts:
                            continue
                    # filter: required_capabilities
                    if j_caps_json and "*" not in agent_caps:
                        need = set(json_list(j_caps_json))
                        if not need.issubset(agent_caps):
                            continue
                    # filter: cost-tier cap (token-miser default: A=free only)
                    job_max_tier = (j_max_tier or "B").upper()
                    if job_max_tier not in COST_TIERS:
                        continue
                    if COST_TIERS.index(a_tier) > COST_TIERS.index(job_max_tier):
                        continue
                    # throttle: subscription claude past 85% MTD limited to tier-A only
                    if sub_throttled and job_max_tier != "A":
                        continue
                    # filter: preferred_providers (if set, agent must match one)
                    if j_pref_providers:
                        provs = json.loads(j_pref_providers)
                        if provs and a_provider not in provs:
                            continue
                    if j_pref_models:
                        models = json.loads(j_pref_models)
                        if models and a_model not in models:
                            continue
                    # match — claim + record dispatch resources
                    await db.execute(
                        "UPDATE jobs SET status='claimed', claimed_by=?, claimed_at=?, "
                        "claimed_runtime=?, claimed_model=?, claimed_provider=?, claimed_cost_tier=?, "
                        "claim_lease_expires_at=? "
                        "WHERE id=? AND status='queued'",
                        (agent_urn, now, a_runtime, a_model, a_provider, a_tier,
                         now + CLAIM_LEASE_SECONDS, job_id),
                    )
                    await db.execute("COMMIT")
                    committed = True
                    await emit_event(db, "job.claimed", {
                        "id": job_id, "claimed_by": agent_urn, "kind": j_kind,
                        "runtime": a_runtime, "model": a_model,
                        "provider": a_provider, "cost_tier": a_tier,
                    })
                    return {
                        "id": job_id, "kind": j_kind, "description": j_desc,
                        "priority": j_prio, "deadline": j_dead,
                        "submitter_urn": j_sub, "parent_job_id": j_par,
                        "claimed_at": now, "queued_at": j_started,
                        "mnemos_refs": json.loads(j_mnemos_refs) if j_mnemos_refs else [],
                        "claimed_resources": {
                            "runtime": a_runtime, "model": a_model,
                            "provider": a_provider, "cost_tier": a_tier,
                        },
                    }
            await db.execute("COMMIT")
            committed = True
        except aiosqlite.Error as e:
            if not committed:
                await db.execute("ROLLBACK")
            raise HTTPException(500, f"claim failed: database error while claiming next job: {e}") from e
        except Exception:
            if not committed:
                await db.execute("ROLLBACK")
            raise
    # HTTP 204 = No Content. By spec the response body must be EMPTY.
    # JSONResponse(content=None) writes 'null' (4 bytes) which violates the
    # contract and triggers h11 LocalProtocolError. Use Response (no body).
    from fastapi.responses import Response as _Response
    return _Response(
        status_code=204,
        headers={
            "X-Hive-Claim-Result": "no_jobs_available",
            "X-Hive-Claim-Detail": "no queued jobs matched agent eligibility, dependencies, backoff, or cost tier",
        },
    )


@app.patch("/v1/jobs/{job_id}")
async def update_job(job_id: str, req: JobUpdate):
    now = time.time()
    if req.status not in VALID_JOB_STATUSES:
        raise HTTPException(
            422,
            f"unsupported job status {req.status!r}; valid statuses: {sorted(VALID_JOB_STATUSES)}",
        )
    original_claimed_by_was_set = field_was_set(req, "claimed_by")
    original_result_mnemos_was_set = field_was_set(req, "result_mnemos_id")
    # #3 FIX: Hallucination guard now surfaces failure_reason + exit_code=-2 (per review 2026-05-23)
    halluc_reason = None
    if req.status == "done" and req.result:
        halluc_reason = hallucination_check(req.result)
        if halluc_reason:
            # patch result: exit_code=-2 + top-level failure_reason for easy filtering
            patched = dict(req.result or {})
            patched["exit_code"] = -2
            patched["failure_reason"] = f"hallucination_guard:{halluc_reason}"
            patched["_hallucination_guard"] = halluc_reason
            req = JobUpdate(
                status="failed",
                result=patched,
                claimed_by=req.claimed_by, tokens_in=req.tokens_in,
                tokens_out=req.tokens_out, result_mnemos_id=req.result_mnemos_id,
            )
    req.result = normalize_result_payload(req.status, req.result)

    claimed_by_was_set = original_claimed_by_was_set
    result_mnemos_was_set = original_result_mnemos_was_set
    patch_payload = (
        req.model_dump(exclude_unset=True) if hasattr(req, "model_dump")
        else req.dict(exclude_unset=True)
    )
    cost_estimate = None

    async with connect_db() as db:
        await db.execute("BEGIN IMMEDIATE")
        try:
            async with db.execute(
                "SELECT status, claimed_by, claimed_provider, claimed_model, retry_count, "
                "max_retries FROM jobs WHERE id=?",
                (job_id,),
            ) as cur:
                current = await cur.fetchone()
            if not current:
                await db.execute("ROLLBACK")
                raise HTTPException(404, f"job not found: {job_id}")
            old_status, old_claimed_by, prov, mod, retry_count, max_retries = current

            if req.status not in STATUS_TRANSITIONS.get(old_status, set()):
                await db.execute("ROLLBACK")
                raise HTTPException(
                    409,
                    f"invalid status transition {old_status!r} -> {req.status!r}",
                )

            if old_status in TERMINAL_JOB_STATUSES and req.status != old_status:
                await db.execute("ROLLBACK")
                raise HTTPException(409, f"terminal job status {old_status!r} cannot be reopened by PATCH")

            if old_status in {"queued", "offered"} and req.status in {"claimed", "running"}:
                await db.execute("ROLLBACK")
                raise HTTPException(
                    409,
                    "claim bypass rejected: use POST /v1/jobs/next or POST /v1/jobs/{job_id}/claim "
                    "so agent registration, capabilities, dependencies, and cost tier are checked",
                )

            release_to_queue = req.status == "queued" and old_status in {"claimed", "running", "offered"}
            if req.status == "queued" and old_status in {"claimed", "running", "offered"}:
                if not old_claimed_by:
                    await db.execute("ROLLBACK")
                    raise HTTPException(409, "cannot release a job with no current claimant")
                if not claimed_by_was_set or req.claimed_by != old_claimed_by:
                    await db.execute("ROLLBACK")
                    raise HTTPException(403, "release to queued requires claimed_by to match the current claimant")

            if claimed_by_was_set and req.claimed_by is None:
                await db.execute("ROLLBACK")
                raise HTTPException(422, "claimed_by=null is not accepted; provide the current claimant urn")

            if claimed_by_was_set and req.claimed_by is not None:
                if old_claimed_by and req.claimed_by != old_claimed_by:
                    await db.execute("ROLLBACK")
                    raise HTTPException(
                        403,
                        f"claimed_by overwrite rejected: current={old_claimed_by!r} requested={req.claimed_by!r}",
                    )

            if old_claimed_by and req.status in {"claimed", "running", "done", "failed", "cancelled"}:
                if not claimed_by_was_set or req.claimed_by != old_claimed_by:
                    await db.execute("ROLLBACK")
                    raise HTTPException(403, "job update requires claimed_by to match the current claimant")

            effective_claimed_by = None if release_to_queue else (req.claimed_by or old_claimed_by)
            if req.status in {"claimed", "running"} and not effective_claimed_by:
                await db.execute("ROLLBACK")
                raise HTTPException(422, f"status={req.status!r} requires an existing or explicit claimed_by")

            fields = ["status=?", "last_update_at=?"]
            args: list = [req.status, now]
            if req.status in TERMINAL_JOB_STATUSES:
                fields.append("ended_at=?")
                args.append(now)
            if req.result is not None:
                fields.append("result=?")
                args.append(json.dumps(req.result))
            if release_to_queue:
                fields.extend([
                    "claimed_by=NULL", "claimed_at=NULL", "claimed_runtime=NULL",
                    "claimed_model=NULL", "claimed_provider=NULL", "claimed_cost_tier=NULL",
                ])
            if req.status in {"claimed", "running"}:
                fields.append("claim_lease_expires_at=?")
                args.append(now + CLAIM_LEASE_SECONDS)
            if req.status in TERMINAL_JOB_STATUSES or req.status == "queued":
                fields.append("claim_lease_expires_at=NULL")
            if result_mnemos_was_set:
                fields.append("result_mnemos_id=?")
                args.append(req.result_mnemos_id)

            if req.tokens_in is not None or req.tokens_out is not None:
                t_in = int(req.tokens_in or 0)
                t_out = int(req.tokens_out or 0)
                cost_estimate = estimate_cost(prov or "unknown", mod or "unknown", t_in, t_out)
                fields.extend(["tokens_in=?", "tokens_out=?", "estimated_cost_usd=?"])
                args.extend([t_in, t_out, cost_estimate])

            # Routing re-assignment: only allowed for non-terminal, non-claimed jobs
            if field_was_set(req, "eligible_kinds") and req.eligible_kinds is not None:
                if old_status not in {"queued", "offered"}:
                    await db.execute("ROLLBACK")
                    raise HTTPException(409, f"eligible_kinds can only be updated on queued/offered jobs, not {old_status!r}")
                fields.append("eligible_kinds=?")
                args.append(json.dumps(req.eligible_kinds))
            if field_was_set(req, "eligible_hosts") and req.eligible_hosts is not None:
                if old_status not in {"queued", "offered"}:
                    await db.execute("ROLLBACK")
                    raise HTTPException(409, f"eligible_hosts can only be updated on queued/offered jobs, not {old_status!r}")
                fields.append("eligible_hosts=?")
                args.append(json.dumps(req.eligible_hosts))

            args.append(job_id)
            sql = f"UPDATE jobs SET {', '.join(fields)} WHERE id=?"
            cur = await db.execute(sql, args)
            if cur.rowcount == 0:
                await db.execute("ROLLBACK")
                raise HTTPException(404, f"job not found: {job_id}")

            # Roll MTD spend onto claimer (subscription throttle requires this)
            if cost_estimate and cost_estimate > 0 and effective_claimed_by:
                await db.execute(
                    "UPDATE agents SET plan_period_used_usd = COALESCE(plan_period_used_usd,0) + ? WHERE urn=?",
                    (cost_estimate, effective_claimed_by),
                )

            # On failed: auto-retry if under max_retries. Exponential backoff: 30s x 2^retry_count
            retried = False
            if req.status == "failed" and retry_count < max_retries:
                backoff = 30.0 * (2 ** retry_count)
                next_at = time.time() + backoff
                await db.execute(
                    "UPDATE jobs SET status='queued', retry_count=retry_count+1, "
                    "retry_backoff_until=?, claimed_by=NULL, claimed_at=NULL, "
                    "claimed_runtime=NULL, claimed_model=NULL, claimed_provider=NULL, claimed_cost_tier=NULL, "
                    "ended_at=NULL, result=NULL, tokens_in=NULL, tokens_out=NULL, estimated_cost_usd=NULL, "
                    "claim_lease_expires_at=NULL "
                    "WHERE id=?",
                    (next_at, job_id),
                )
                retried = True

            # On done/failed/cancelled: roll per-worker per-kind stats (capability scoring)
            if req.status in TERMINAL_JOB_STATUSES:
                async with db.execute(
                    "SELECT kind, description, max_cost_tier, required_capabilities, "
                    "claimed_model, claimed_provider, claimed_by, result, started_at "
                    "FROM jobs WHERE id=?", (job_id,)
                ) as cur2:
                    jrow = await cur2.fetchone()
                if jrow:
                    kind_j, desc_j, mtier, reqcaps_json, mdl_j, prov_j, claimed_by_j, result_j, started_j = jrow
                    if claimed_by_j and kind_j:
                        duration = (time.time() - (started_j or time.time())) if started_j else 0
                        col = {"done": "success_count", "failed": "fail_count", "cancelled": "cancelled_count"}[req.status]
                        await db.execute(
                            f"INSERT INTO worker_kind_stats (urn, kind, {col}, total_tokens_in, total_tokens_out, "
                            f"total_cost_usd, total_duration_sec, last_run) "
                            f"VALUES (?, ?, 1, ?, ?, ?, ?, ?) "
                            f"ON CONFLICT(urn, kind) DO UPDATE SET "
                            f"{col} = {col} + 1, "
                            f"total_tokens_in = total_tokens_in + ?, "
                            f"total_tokens_out = total_tokens_out + ?, "
                            f"total_cost_usd = total_cost_usd + ?, "
                            f"total_duration_sec = total_duration_sec + ?, "
                            f"last_run = ?",
                            (
                                claimed_by_j, kind_j,
                                int(req.tokens_in or 0), int(req.tokens_out or 0),
                                cost_estimate or 0, duration, time.time(),
                                int(req.tokens_in or 0), int(req.tokens_out or 0),
                                cost_estimate or 0, duration, time.time(),
                            ),
                        )
                    if req.status == "done":
                        ck = cache_key_for(kind_j, desc_j, (mtier or "A").upper(),
                                           json_list(reqcaps_json))
                        rdict = json.loads(result_j) if result_j else (req.result or {})
                        ec = (rdict or {}).get("exit_code")
                        if ec == 0 or ec is None:
                            await cache_store(db, ck, job_id, rdict, req.result_mnemos_id,
                                              mdl_j or "unknown", prov_j or "unknown", cost_estimate or 0)

            await db.execute(
                "INSERT INTO job_audit_log (job_id, ts, actor_urn, old_status, new_status, "
                "old_claimed_by, new_claimed_by, patch) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    job_id, now, req.claimed_by or old_claimed_by, old_status,
                    "queued" if retried else req.status,
                    old_claimed_by,
                    None if release_to_queue else effective_claimed_by,
                    json.dumps(patch_payload, default=str),
                ),
            )
            await db.execute("COMMIT")
        except HTTPException:
            raise
        except Exception:
            await db.execute("ROLLBACK")
            raise

    async with connect_db() as event_db:
        if req.status == "failed" and retry_count < max_retries:
            await emit_event(event_db, "job.retry", {
                "id": job_id, "retry_count": retry_count + 1,
                "max_retries": max_retries,
            })
        await emit_event(event_db, f"job.{req.status}", {
            "id": job_id, "claimed_by": req.claimed_by or old_claimed_by,
            "tokens_in": req.tokens_in, "tokens_out": req.tokens_out,
            "estimated_cost_usd": cost_estimate,
        })
    return {"ok": True, "ts": now, "estimated_cost_usd": cost_estimate}




class RequeueRequest(BaseModel):
    job_ids: list[str]
    reason: str = "manual-requeue"

@app.post("/v1/admin/jobs/requeue")
async def admin_requeue_jobs(req: RequeueRequest):
    """Force-requeue terminal (failed/cancelled) or any non-done jobs back to queued."""
    now = time.time()
    results = {"ok": [], "not_found": [], "skipped_done": []}
    async with connect_db() as db:
        for job_id in req.job_ids:
            async with db.execute("SELECT status FROM jobs WHERE id=?", (job_id,)) as cur:
                row = await cur.fetchone()
            if not row:
                results["not_found"].append(job_id)
                continue
            if row[0] == "done":
                results["skipped_done"].append(job_id)
                continue
            await db.execute(
                """UPDATE jobs SET status='queued', claimed_by=NULL, claimed_at=NULL,
                   retry_count=0, retry_backoff_until=NULL
                   WHERE id=?""",
                (job_id,),
            )
            await db.commit()
            results["ok"].append(job_id)
    return results

@app.post("/v1/schedules")
async def create_schedule(req: ScheduleCreate):
    """Create a recurring scheduled job. Re-fires every `interval_seconds`.
    job_template is the JobCreate body that will be submitted each tick.
    Submitter_urn auto-injected from caller — they take responsibility for the cron loop.
    """
    sid = uuidv7()
    now = time.time()
    # Validate the template parses as a JobCreate
    try:
        tpl_copy = dict(req.job_template)
        if not tpl_copy.get("submitter_urn"):
            raise ValueError("job_template.submitter_urn is required and must be a registered orchestrator")
        JobCreate(**tpl_copy)
    except Exception as e:
        raise HTTPException(422, f"job_template invalid: {e}")
    await require_orchestrator_submitter(tpl_copy["submitter_urn"])
    async with connect_db() as db:
        await db.execute(
            "INSERT INTO scheduled_jobs (id, name, created_by_urn, interval_seconds, "
            "job_template, enabled, last_fired_at, next_fire_at, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)",
            (sid, req.name, tpl_copy.get("submitter_urn"), int(req.interval_seconds),
             json.dumps(req.job_template), 1 if req.enabled else 0,
             now + req.interval_seconds, now),
        )
        await db.commit()
    return {"id": sid, "name": req.name, "next_fire_at": now + req.interval_seconds}


@app.get("/v1/schedules")
async def list_schedules():
    rows = []
    async with connect_db() as db:
        async with db.execute(
            "SELECT id, name, created_by_urn, interval_seconds, enabled, "
            "last_fired_at, next_fire_at, fire_count, created_at FROM scheduled_jobs"
        ) as cur:
            async for r in cur:
                rows.append({
                    "id": r[0], "name": r[1], "created_by": r[2],
                    "interval_seconds": r[3], "enabled": bool(r[4]),
                    "last_fired_at": r[5], "next_fire_at": r[6],
                    "fire_count": r[7], "created_at": r[8],
                })
    return {"count": len(rows), "schedules": rows}


@app.patch("/v1/schedules/{sid}")
async def patch_schedule(sid: str, enabled: Optional[bool] = None,
                         interval_seconds: Optional[int] = None):
    sets, args = [], []
    if enabled is not None:
        sets.append("enabled=?")
        args.append(1 if enabled else 0)
    if interval_seconds is not None:
        if interval_seconds < 60 or interval_seconds > 86400 * 30:
            raise HTTPException(422, "interval_seconds must be 60..2592000")
        sets.append("interval_seconds=?")
        args.append(int(interval_seconds))
        sets.append("next_fire_at=?")
        args.append(time.time() + int(interval_seconds))
    if not sets:
        raise HTTPException(422, "no fields to update")
    args.append(sid)
    async with connect_db() as db:
        cur = await db.execute(
            f"UPDATE scheduled_jobs SET {', '.join(sets)} WHERE id=?", args)
        await db.commit()
    if cur.rowcount == 0:
        raise HTTPException(404, "schedule not found")
    return {"ok": True}


@app.delete("/v1/schedules/{sid}")
async def delete_schedule(sid: str):
    async with connect_db() as db:
        cur = await db.execute("DELETE FROM scheduled_jobs WHERE id=?", (sid,))
        await db.commit()
    if cur.rowcount == 0:
        raise HTTPException(404, "schedule not found")
    return {"ok": True}


@app.get("/v1/stats/workers")
async def worker_stats(kind: Optional[str] = None, top_n: int = 30,
                       include_system: bool = False):
    """Per-worker per-kind capability scores. Submitters use this to pick best worker for a kind.

    #8 FIX (review 2026-05-23): exclude `system` kind agents by default (they're host monitors,
    not compute workers — including them was misleading). Pass include_system=true to override.
    """
    top_n = clamp_limit(top_n, default=30, max_limit=500)
    sql = ("SELECT urn, kind, success_count, fail_count, cancelled_count, "
           "total_tokens_in, total_tokens_out, ROUND(total_cost_usd,4), "
           "ROUND(total_duration_sec/NULLIF(success_count+fail_count,0),1) AS avg_dur, "
           "datetime(last_run,'unixepoch') AS last_run, "
           "ROUND(100.0 * success_count / NULLIF(success_count+fail_count+cancelled_count,0), 1) AS success_pct "
           "FROM worker_kind_stats WHERE 1=1")
    args: list = []
    if kind:
        sql += " AND kind=?"
        args.append(kind)
    if not include_system:
        sql += " AND urn NOT LIKE 'urn:agent:system:%'"
    sql += " ORDER BY success_count DESC, success_pct DESC LIMIT ?"
    args.append(top_n)
    rows = []
    async with connect_db() as db:
        async with db.execute(sql, args) as cur:
            async for r in cur:
                rows.append({
                    "urn": r[0], "kind": r[1],
                    "success_count": r[2], "fail_count": r[3], "cancelled_count": r[4],
                    "total_tokens_in": r[5], "total_tokens_out": r[6],
                    "total_cost_usd": r[7], "avg_duration_sec": r[8],
                    "last_run": r[9], "success_pct": r[10],
                })
    return {"count": len(rows), "workers": rows}


@app.get("/v1/stats/cache")
async def cache_stats(top_n: int = 20):
    """Result-cache hit-rate + cost savings."""
    top_n = clamp_limit(top_n, default=20, max_limit=500)
    async with connect_db() as db:
        async with db.execute(
            "SELECT COUNT(*), SUM(hit_count), ROUND(SUM(cost_saved_usd),4) FROM hive_cache"
        ) as cur:
            r = await cur.fetchone()
        totals = {"cached_jobs": r[0] or 0, "total_hits": r[1] or 0,
                  "total_cost_saved_usd": r[2] or 0}
        # job-side stats
        async with db.execute(
            "SELECT COUNT(*) FROM jobs WHERE result LIKE '%\"cache_hit\": true%'"
        ) as cur:
            r2 = await cur.fetchone()
        totals["jobs_short_circuited"] = r2[0] or 0
        # top cached entries
        async with db.execute(
            "SELECT cache_key, hit_count, cost_saved_usd, model, provider, "
            "datetime(cached_at,'unixepoch') AS cached_at, source_job_id "
            "FROM hive_cache ORDER BY hit_count DESC, cost_saved_usd DESC LIMIT ?",
            (top_n,)
        ) as cur:
            entries = []
            async for row in cur:
                entries.append({
                    "cache_key": row[0][:16], "hit_count": row[1],
                    "cost_saved_usd": row[2], "model": row[3],
                    "provider": row[4], "cached_at": row[5],
                    "source_job_id": row[6],
                })
    return {"totals": totals, "top_entries": entries}


@app.get("/v1/stats/costs")
async def cost_stats(since_hours: int = 168, group_by: str = "provider"):
    """Aggregated cost stats. group_by: provider | model | runtime | cost_tier | claimed_by | day.
    since_hours: window in hours (default 168 = 7 days).
    """
    if group_by not in {"provider", "model", "runtime", "cost_tier", "claimed_by", "day", "kind"}:
        raise HTTPException(422, "group_by must be one of: provider, model, runtime, cost_tier, claimed_by, day, kind")
    cutoff = time.time() - since_hours * 3600
    if group_by == "day":
        sel = "DATE(ended_at, 'unixepoch')"
    elif group_by == "provider":
        sel = "COALESCE(claimed_provider,'unknown')"
    elif group_by == "model":
        sel = "COALESCE(claimed_model,'unknown')"
    elif group_by == "runtime":
        sel = "COALESCE(claimed_runtime,'unknown')"
    elif group_by == "cost_tier":
        sel = "COALESCE(claimed_cost_tier,'unknown')"
    elif group_by == "claimed_by":
        sel = "COALESCE(claimed_by,'unknown')"
    else:  # kind
        sel = "kind"
    sql = (
        f"SELECT {sel} AS bucket, "
        "COUNT(*) AS job_count, "
        "SUM(COALESCE(tokens_in,0)) AS tot_in, "
        "SUM(COALESCE(tokens_out,0)) AS tot_out, "
        "ROUND(SUM(COALESCE(estimated_cost_usd,0)),4) AS tot_cost_usd, "
        "ROUND(AVG(CASE WHEN ended_at IS NOT NULL THEN ended_at-started_at END),2) AS avg_dur_s "
        "FROM jobs WHERE ended_at IS NOT NULL AND ended_at >= ? "
        f"GROUP BY bucket ORDER BY tot_cost_usd DESC, job_count DESC"
    )
    rows = []
    async with connect_db() as db:
        async with db.execute(sql, (cutoff,)) as cur:
            async for r in cur:
                rows.append({
                    "bucket": r[0], "job_count": r[1],
                    "tokens_in": r[2], "tokens_out": r[3],
                    "estimated_cost_usd": r[4], "avg_duration_sec": r[5],
                })
    # totals
    totals = {
        "job_count": sum(r["job_count"] for r in rows),
        "tokens_in": sum(r["tokens_in"] or 0 for r in rows),
        "tokens_out": sum(r["tokens_out"] or 0 for r in rows),
        "estimated_cost_usd": round(sum(r["estimated_cost_usd"] or 0 for r in rows), 4),
    }
    return {"since_hours": since_hours, "group_by": group_by, "totals": totals, "buckets": rows}


@app.get("/v1/jobs/metrics")
async def job_metrics():
    counts = {"pending": 0, "running": 0, "completed": 0, "failed": 0, "total": 0}
    async with connect_db() as db:
        async with db.execute(
            "SELECT status, COUNT(*) FROM jobs GROUP BY status"
        ) as cur:
            async for status, count in cur:
                counts["total"] += count
                if status in {"queued", "offered"}:
                    counts["pending"] += count
                elif status in {"claimed", "running"}:
                    counts["running"] += count
                elif status == "done":
                    counts["completed"] += count
                elif status == "failed":
                    counts["failed"] += count
    return counts


@app.post("/v1/jobs/{job_id}/claim")
async def claim_job(job_id: str, by: str):
    """Targeted single-job claim with the same eligibility checks as
    /v1/jobs/next (dequeue_next_job).

    Previously this endpoint just ran UPDATE WHERE status IN
    ('queued','offered') with no validation — any caller could claim
    any job by passing an arbitrary `by` URN, bypassing agent
    registration, capability matching, eligible_kinds, cost-tier ceiling,
    throttling, preferred provider/model, and DAG dependency gating, and
    skipping the claimed_runtime/claimed_model/claimed_provider/
    claimed_cost_tier population that cost accounting + worker_kind_stats
    depend on. This rewrite reuses the dequeue eligibility chain for
    one specific job_id so manual claims are subject to the same
    contract as auto-claims.
    """
    now = time.time()
    async with connect_db() as db:
        # 1. agent must exist + be online/idle
        async with db.execute(
            "SELECT kind, capabilities, runtime, model, provider, cost_tier, "
            "auth_method, plan_cap_usd, plan_period_used_usd, status "
            "FROM agents WHERE urn=?",
            (by,),
        ) as cur:
            agent_row = await cur.fetchone()
        if not agent_row:
            raise HTTPException(404, f"claim failed: agent not registered: {by}")
        (agent_kind, caps_json, a_runtime, a_model, a_provider, a_tier,
         a_auth, a_cap, a_used, agent_status) = agent_row
        if agent_status not in ACTIVE_AGENT_STATUSES:
            raise HTTPException(409, f"claim failed: agent status is {agent_status!r}, not online/idle: {by}")
        agent_caps = set(json.loads(caps_json)) if caps_json else set()
        eligible_aliases = agent_kind_aliases(agent_kind, a_runtime)
        a_tier = a_tier or "C"
        a_auth = (a_auth or "unknown").lower()
        sub_throttled = (
            a_auth == "subscription" and a_cap and a_used
            and a_used >= THROTTLE_HEADROOM * a_cap
        )

        # 2. job must exist + be claimable + agent must satisfy its filters
        async with db.execute(
            "SELECT status, kind, required_capabilities, eligible_kinds, "
            "max_cost_tier, preferred_providers, preferred_models, "
            "depends_on, retry_backoff_until "
            "FROM jobs WHERE id=?",
            (job_id,),
        ) as cur:
            job_row = await cur.fetchone()
        if not job_row:
            raise HTTPException(404, f"job not found: {job_id}")
        (j_status, j_kind, j_caps_json, j_kinds_json, j_max_tier,
         j_pref_providers, j_pref_models, j_deps_json,
         j_retry_backoff_until) = job_row

        if j_status not in ("queued", "offered"):
            raise HTTPException(
                409, f"job already in status={j_status!r}; cannot claim"
            )

        # retry-backoff gate (same as dequeue)
        if j_retry_backoff_until is not None and j_retry_backoff_until > now:
            raise HTTPException(
                409,
                f"job is in retry backoff until {j_retry_backoff_until} (now={now})",
            )

        # DAG gate: all depends_on must be status='done'
        if j_deps_json:
            deps = json_list(j_deps_json)
            if deps:
                ph = ",".join("?" * len(deps))
                async with db.execute(
                    f"SELECT COUNT(*) FROM jobs WHERE id IN ({ph}) AND status='done'",
                    tuple(deps),
                ) as dc:
                    done_count = (await dc.fetchone())[0]
                if done_count < len(deps):
                    raise HTTPException(
                        409,
                        f"job has unsatisfied DAG dependencies "
                        f"({done_count}/{len(deps)} done)",
                    )

        # eligible_kinds
        if j_kinds_json:
            kinds = set(json_list(j_kinds_json))
            if kinds and "*" not in kinds and not kinds.intersection(eligible_aliases):
                raise HTTPException(
                    403,
                    f"agent kind aliases={sorted(eligible_aliases)!r} not in eligible_kinds={sorted(kinds)}",
                )

        # required_capabilities (wildcard "*" claim-any escape)
        if j_caps_json and "*" not in agent_caps:
            need = set(json_list(j_caps_json))
            if not need.issubset(agent_caps):
                missing = sorted(need - agent_caps)
                raise HTTPException(
                    403,
                    f"agent missing required_capabilities: {missing}",
                )

        # cost-tier ceiling
        job_max_tier = (j_max_tier or "B").upper()
        if job_max_tier not in COST_TIERS:
            raise HTTPException(422, f"job max_cost_tier is invalid: {job_max_tier!r}")
        if COST_TIERS.index(a_tier) > COST_TIERS.index(job_max_tier):
            raise HTTPException(
                403,
                f"agent cost_tier={a_tier!r} exceeds job max_cost_tier={job_max_tier!r}",
            )

        # subscription throttling
        if sub_throttled and job_max_tier != "A":
            raise HTTPException(
                429,
                f"subscription agent throttled (>= {THROTTLE_HEADROOM*100:.0f}% MTD); "
                f"cannot claim tier-{job_max_tier} jobs",
            )

        # preferred_providers / preferred_models
        if j_pref_providers:
            provs = json.loads(j_pref_providers)
            if provs and a_provider not in provs:
                raise HTTPException(
                    403,
                    f"agent provider={a_provider!r} not in preferred_providers={provs}",
                )
        if j_pref_models:
            models = json.loads(j_pref_models)
            if models and a_model not in models:
                raise HTTPException(
                    403,
                    f"agent model={a_model!r} not in preferred_models={models}",
                )

        # 3. atomic claim with race guard + populate claimed_resources
        cur = await db.execute(
            "UPDATE jobs SET status='claimed', claimed_by=?, claimed_at=?, "
            "claimed_runtime=?, claimed_model=?, claimed_provider=?, claimed_cost_tier=?, "
            "claim_lease_expires_at=? "
            "WHERE id=? AND status IN ('queued','offered')",
            (by, now, a_runtime, a_model, a_provider, a_tier,
             now + CLAIM_LEASE_SECONDS, job_id),
        )
        await db.commit()
        if cur.rowcount == 0:
            raise HTTPException(409, "job already claimed or not available (race)")
        await emit_event(db, "job.claimed", {
            "id": job_id, "claimed_by": by,
            "claimed_runtime": a_runtime, "claimed_model": a_model,
            "claimed_provider": a_provider, "claimed_cost_tier": a_tier,
            "manual_claim": True,
        })
    return {
        "claimed": True, "ts": now,
        "claimed_runtime": a_runtime, "claimed_model": a_model,
        "claimed_provider": a_provider, "claimed_cost_tier": a_tier,
    }


@app.get("/v1/jobs")
async def list_jobs(
    status: Optional[str] = None,
    agent_urn: Optional[str] = None,
    since: Optional[float] = None,
    limit: int = 100,
):
    limit = clamp_limit(limit, default=100, max_limit=1000)
    sql = ("SELECT id, submitter_urn, parent_job_id, kind, description, priority, status, "
           "claimed_by, started_at, ended_at, result, estimated_cost_usd, eligible_kinds, eligible_hosts FROM jobs WHERE 1=1")
    args: list = []
    cnt_sql = "SELECT COUNT(*) FROM jobs WHERE 1=1"
    cnt_args: list = []
    if status:
        sql += " AND status=?"
        cnt_sql += " AND status=?"
        args.append(status)
        cnt_args.append(status)
    if agent_urn:
        sql += " AND (submitter_urn=? OR claimed_by=?)"
        cnt_sql += " AND (submitter_urn=? OR claimed_by=?)"
        args.extend([agent_urn, agent_urn])
        cnt_args.extend([agent_urn, agent_urn])
    if since:
        sql += " AND started_at >= ?"
        cnt_sql += " AND started_at >= ?"
        args.append(since)
        cnt_args.append(since)
    sql += " ORDER BY priority DESC, started_at DESC LIMIT ?"
    args.append(limit)
    rows = []
    total = 0
    async with connect_db() as db:
        async with db.execute(cnt_sql, cnt_args) as cur:
            row = await cur.fetchone()
            total = row[0] if row else 0
        async with db.execute(sql, args) as cur:
            async for r in cur:
                rows.append({
                    "id": r[0], "submitter_urn": r[1], "parent_job_id": r[2],
                    "kind": r[3], "description": r[4], "priority": r[5],
                    "status": r[6], "claimed_by": r[7],
                    "started_at": r[8], "ended_at": r[9],
                    "result": json.loads(r[10]) if r[10] else None,
                    "estimated_cost_usd": r[11],
                    "eligible_kinds": json.loads(r[12]) if r[12] else None,
                    "eligible_hosts": json.loads(r[13]) if r[13] else None,
                })
    return {"count": len(rows), "total": total, "jobs": rows}


@app.post("/v1/messages")
async def publish_message(req: MessagePublish):
    msg_id = uuidv7()
    now = time.time()
    await require_registered_agent(req.from_urn, active_only=True)
    async with connect_db() as db:
        await db.execute(
            "INSERT INTO messages (id, from_urn, to_urn, in_reply_to, topic, payload, ts) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (msg_id, req.from_urn, req.to_urn, req.in_reply_to, req.topic, json.dumps(req.payload), now),
        )
        await db.commit()
        await emit_event(db, "message", {
            "id": msg_id, "from": req.from_urn, "to": req.to_urn,
            "topic": req.topic, "payload": req.payload,
        })
    return {"id": msg_id, "ts": now}


@app.get("/v1/messages")
async def list_messages(
    to_urn: Optional[str] = None,
    topic: Optional[str] = None,
    since: Optional[float] = None,
    limit: int = 100,
):
    limit = clamp_limit(limit, default=100, max_limit=1000)
    sql = "SELECT id, from_urn, to_urn, in_reply_to, topic, payload, ts FROM messages WHERE 1=1"
    args: list = []
    if to_urn:
        sql += " AND (to_urn=? OR to_urn IS NULL)"
        args.append(to_urn)
    if topic:
        sql += " AND topic LIKE ?"
        args.append(topic.replace("*", "%"))
    if since:
        sql += " AND ts >= ?"
        args.append(since)
    sql += " ORDER BY ts DESC LIMIT ?"
    args.append(limit)
    rows = []
    async with connect_db() as db:
        async with db.execute(sql, args) as cur:
            async for r in cur:
                rows.append({
                    "id": r[0], "from": r[1], "to": r[2], "in_reply_to": r[3],
                    "topic": r[4], "payload": json.loads(r[5]), "ts": r[6],
                })
    return {"count": len(rows), "messages": rows}


@app.get("/v1/events")
async def stream_events(request: Request, since_id: Optional[int] = None):
    """SSE stream of events. Optional since_id for catch-up."""
    if len(EVENT_QUEUE) >= MAX_EVENT_SUBSCRIBERS:
        raise HTTPException(503, f"too many event subscribers; max={MAX_EVENT_SUBSCRIBERS}")
    sub_id = str(uuid.uuid4())
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    EVENT_QUEUE[sub_id] = queue

    async def gen():
        try:
            # catch-up phase
            if since_id is not None:
                async with connect_db() as db:
                    async with db.execute(
                        "SELECT id, ts, kind, payload FROM events WHERE id > ? ORDER BY id ASC",
                        (since_id,),
                    ) as cur:
                        async for r in cur:
                            yield {
                                "id": str(r[0]),
                                "data": json.dumps({"ts": r[1], "kind": r[2], "payload": json.loads(r[3])}),
                            }
            # live stream — use un-typed events (event: message) so browser onmessage fires
            last_ping = time.time()
            while True:
                if await request.is_disconnected():
                    break
                try:
                    evt = await asyncio.wait_for(queue.get(), timeout=SSE_PING_INTERVAL)
                    yield {
                        "data": json.dumps(evt),
                    }
                except asyncio.TimeoutError:
                    if time.time() - last_ping >= SSE_PING_INTERVAL:
                        yield {"event": "ping", "data": str(int(time.time()))}
                        last_ping = time.time()
        finally:
            EVENT_QUEUE.pop(sub_id, None)

    return EventSourceResponse(gen())


@app.get("/v1/events/log")
async def events_log(since_id: Optional[int] = None, limit: int = 100):
    """JSON polling alternative to SSE."""
    limit = clamp_limit(limit, default=100, max_limit=1000)
    sql = "SELECT id, ts, kind, payload, agent_urn FROM events"
    args: list = []
    if since_id is not None:
        sql += " WHERE id > ?"
        args.append(since_id)
    sql += " ORDER BY id ASC LIMIT ?"
    args.append(limit)
    rows = []
    async with connect_db() as db:
        async with db.execute(sql, args) as cur:
            async for r in cur:
                rows.append({
                    "id": r[0], "ts": r[1], "kind": r[2],
                    "payload": json.loads(r[3]), "agent_urn": r[4],
                })
    return {"count": len(rows), "events": rows, "last_id": rows[-1]["id"] if rows else since_id}


# ---------- minimal MCP shim ----------
# Maps a few common MCP-style JSON-RPC calls to the REST endpoints.
# Phase 2: replace with full mcp-server-sdk.

@app.post("/mcp")
async def mcp_rpc(body: dict):
    method = body.get("method", "")
    params = body.get("params", {})
    handlers = {
        "agent.register": register,
        "agent.heartbeat": heartbeat,
        "agent.list": list_agents,
        "job.create": create_job,
        "job.list": list_jobs,
        "message.publish": publish_message,
        "message.list": list_messages,
    }
    if method not in handlers:
        return JSONResponse({"error": f"unknown method: {method}"}, status_code=404)
    # crude param coercion - production MCP would use proper validation
    try:
        if method == "agent.register":
            return await register(AgentRegister(**params))
        if method == "agent.heartbeat":
            return await heartbeat(AgentHeartbeat(**params))
        if method == "agent.list":
            return await list_agents(**params)
        if method == "job.create":
            return await create_job(JobCreate(**params))
        if method == "job.list":
            return await list_jobs(**params)
        if method == "message.publish":
            return await publish_message(MessagePublish(**params))
        if method == "message.list":
            return await list_messages(**params)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
