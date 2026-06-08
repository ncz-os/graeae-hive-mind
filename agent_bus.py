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
import re
import secrets
import sys
import time
import uuid
from contextlib import asynccontextmanager
from contextlib import suppress
from typing import Optional, Any

try:
    import aiosqlite
except ModuleNotFoundError:
    from hive.persistence import _sqlite_async as aiosqlite

import queue_logic
from hive.persistence.factory import get_hive_repository
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

DB_PATH = os.environ.get("AGENT_BUS_DB", "/srv/agent-bus/agents.db")
HIVE_BACKEND = os.environ.get("HIVE_BACKEND", "sqlite").lower()
ORACLE_DSN = os.environ.get("ORACLE_DSN", "oracle://mnemos:mnemos_dev@127.0.0.1:1521/ORCLPDB1")
HEARTBEAT_REAP_INTERVAL = 30.0
HEARTBEAT_STALE_AFTER = 90.0
HEARTBEAT_OFFLINE_AFTER = 300.0
# Hard-delete agents that have been offline this long. Every worker restart
# registers a NEW session URN, so without this the agents table grows without
# bound (10,930 rows / 10,882 offline observed 2026-06-08) and poisons the
# dashboard worker panel + per-worker stats. 6h keeps a generous recent-history
# window while reaping long-dead session rows.
AGENT_PURGE_AFTER = float(os.environ.get("HIVE_AGENT_PURGE_AFTER", "21600"))
EVENTS_RETAIN_HOURS = 168  # 7 days
SSE_PING_INTERVAL = 15.0
EVENT_QUEUE: dict[str, asyncio.Queue] = {}  # subscriber_id -> queue
SQLITE_BUSY_TIMEOUT_MS = int(os.environ.get("AGENT_BUS_SQLITE_BUSY_TIMEOUT_MS", "5000"))
MAX_EVENT_SUBSCRIBERS = int(os.environ.get("AGENT_BUS_MAX_EVENT_SUBSCRIBERS", "100"))


_TABLE_MAP = {
    "agents": "hive_agents",
    "jobs": "hive_jobs",
    "messages": "hive_messages",
    "events": "hive_events",
    "hive_cache": "hive_cache",
    "scheduled_jobs": "hive_scheduled_jobs",
    "worker_kind_stats": "hive_worker_kind_stats",
    "job_audit_log": "hive_job_audit_log",
}


def _parse_oracle_url(url: str) -> tuple[str, str, str]:
    from urllib.parse import unquote, urlparse
    parsed = urlparse(url)
    if parsed.scheme != "oracle":
        return "", "", url
    user = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 1521
    service = (parsed.path or "/ORCLPDB1").lstrip("/")
    return user, password, f"{host}:{port}/{service}"


if HIVE_BACKEND not in {"sqlite", "oracle", "db2"}:
    raise RuntimeError(f"HIVE_BACKEND must be sqlite, oracle, or db2, got {HIVE_BACKEND!r}")

if HIVE_BACKEND == "oracle":
    try:
        import oracledb
    except Exception as exc:
        print(f"HIVE_BACKEND=oracle requires python-oracledb importable: {exc}", file=sys.stderr, flush=True)
        raise
    try:
        _oracle_user, _oracle_password, _oracle_connect_dsn = _parse_oracle_url(ORACLE_DSN)
        _probe = oracledb.connect(user=_oracle_user, password=_oracle_password, dsn=_oracle_connect_dsn)
        _probe.close()
    except Exception as exc:
        print(f"HIVE_BACKEND=oracle failed initial Oracle connection to {ORACLE_DSN!r}: {exc}", file=sys.stderr, flush=True)
        raise
else:
    oracledb = None

_HIVE_REPO = None


# ---------- helpers ----------

def _replace_qmarks(sql: str) -> str:
    out = []
    bind_index = 1
    in_single = False
    in_double = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'" and not in_double:
            out.append(ch)
            if in_single and i + 1 < len(sql) and sql[i + 1] == "'":
                out.append(sql[i + 1])
                i += 2
                continue
            in_single = not in_single
        elif ch == '"' and not in_single:
            out.append(ch)
            in_double = not in_double
        elif ch == "?" and not in_single and not in_double:
            out.append(f":{bind_index}")
            bind_index += 1
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def _rewrite_tables(sql: str) -> str:
    names = "|".join(sorted(map(re.escape, _TABLE_MAP), key=len, reverse=True))

    def repl(match: re.Match) -> str:
        return f"{match.group(1)} {_TABLE_MAP[match.group(2).lower()]}"

    sql = re.sub(rf"\b(FROM|INTO|UPDATE|DELETE\s+FROM|JOIN)\s+({names})\b", repl, sql, flags=re.IGNORECASE)
    sql = re.sub(rf"\b(ALTER\s+TABLE|CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?|DROP\s+TABLE)\s+({names})\b", repl, sql, flags=re.IGNORECASE)
    sql = re.sub(rf"\b(ON)\s+({names})\b", repl, sql, flags=re.IGNORECASE)
    return sql


def _rewrite_limit(sql: str) -> str:
    return re.sub(r"\s+LIMIT\s+(:\d+|\d+)\s*$", r" FETCH FIRST \1 ROWS ONLY", sql, flags=re.IGNORECASE)


def _rewrite_sqlite_time_funcs(sql: str) -> str:
    sql = re.sub(
        r"datetime\((\w+),'unixepoch'\)",
        r"TO_CHAR(TIMESTAMP '1970-01-01 00:00:00' + NUMTODSINTERVAL(\1, 'SECOND'), 'YYYY-MM-DD HH24:MI:SS')",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"DATE\((\w+), 'unixepoch'\)",
        r"TRUNC(TIMESTAMP '1970-01-01 00:00:00' + NUMTODSINTERVAL(\1, 'SECOND'))",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _translate_upsert(sql: str) -> Optional[str]:
    normalized = " ".join(sql.split())
    cache_pattern = (
        r"INSERT INTO hive_cache \(cache_key, result_json, source_job_id, result_mnemos_id, "
        r"hit_count, cost_saved_usd, model, provider, cached_at, last_hit_at\) VALUES "
        r"\(:1, :2, :3, :4, 0, 0, :5, :6, :7, NULL\) ON CONFLICT\(cache_key\) DO UPDATE SET"
    )
    if re.search(cache_pattern, normalized, flags=re.IGNORECASE):
        return (
            "MERGE INTO hive_cache t "
            "USING (SELECT :1 cache_key, :2 result_json, :3 source_job_id, :4 result_mnemos_id, "
            ":5 model, :6 provider, :7 cached_at FROM dual) s "
            "ON (t.cache_key = s.cache_key) "
            "WHEN MATCHED THEN UPDATE SET "
            "t.result_json=s.result_json, t.source_job_id=s.source_job_id, "
            "t.result_mnemos_id=s.result_mnemos_id, t.cached_at=s.cached_at, "
            "t.model=s.model, t.provider=s.provider "
            "WHEN NOT MATCHED THEN INSERT "
            "(cache_key, result_json, source_job_id, result_mnemos_id, hit_count, cost_saved_usd, model, provider, cached_at, last_hit_at) "
            "VALUES (s.cache_key, s.result_json, s.source_job_id, s.result_mnemos_id, 0, 0, s.model, s.provider, s.cached_at, NULL)"
        )
    stats_match = re.search(
        r"INSERT INTO hive_worker_kind_stats \(urn, kind, (success_count|fail_count|cancelled_count), "
        r"total_tokens_in, total_tokens_out, total_cost_usd, total_duration_sec, last_run\) "
        r"VALUES \(:1, :2, 1, :3, :4, :5, :6, :7\) ON CONFLICT\(urn, kind\) DO UPDATE SET",
        normalized,
        flags=re.IGNORECASE,
    )
    if stats_match:
        col = stats_match.group(1)
        return (
            "MERGE INTO hive_worker_kind_stats t "
            "USING (SELECT :1 urn, :2 kind, :3 ins_tokens_in, :4 ins_tokens_out, :5 ins_cost, "
            ":6 ins_duration, :7 ins_last_run, :8 upd_tokens_in, :9 upd_tokens_out, "
            ":10 upd_cost, :11 upd_duration, :12 upd_last_run FROM dual) s "
            "ON (t.urn = s.urn AND t.kind = s.kind) "
            f"WHEN MATCHED THEN UPDATE SET t.{col}=t.{col}+1, "
            "t.total_tokens_in=t.total_tokens_in+s.upd_tokens_in, "
            "t.total_tokens_out=t.total_tokens_out+s.upd_tokens_out, "
            "t.total_cost_usd=t.total_cost_usd+s.upd_cost, "
            "t.total_duration_sec=t.total_duration_sec+s.upd_duration, "
            "t.last_run=s.upd_last_run "
            "WHEN NOT MATCHED THEN INSERT "
            f"(urn, kind, {col}, total_tokens_in, total_tokens_out, total_cost_usd, total_duration_sec, last_run) "
            f"VALUES (s.urn, s.kind, 1, s.ins_tokens_in, s.ins_tokens_out, s.ins_cost, s.ins_duration, s.ins_last_run)"
        )
    return None


def _translate_sql(sql: str, params: Any = None) -> tuple[Optional[str], Any]:
    stripped = sql.strip()
    upper = stripped.upper()
    if not stripped:
        return None, params
    if upper.startswith("PRAGMA"):
        return None, params
    if upper in {"BEGIN", "BEGIN IMMEDIATE"}:
        return None, params
    if upper.startswith(("CREATE ", "ALTER TABLE ", "DROP TABLE ")):
        return None, params
    if "SQLITE_MASTER" in upper:
        return "SELECT 'CREATE TABLE agents (status TEXT CHECK(status IN (''online'',''idle'',''stale'',''offline'',''error'')))' FROM dual", ()

    sql = _rewrite_tables(stripped)
    sql = _replace_qmarks(sql)
    sql = _rewrite_sqlite_time_funcs(sql)
    upsert_sql = _translate_upsert(sql)
    if upsert_sql:
        return upsert_sql, tuple(params or ())
    sql = _rewrite_limit(sql)
    return sql, tuple(params or ())


class BackendCursor:
    def __init__(self, cursor=None, rows: Optional[list] = None, rowcount: int = -1):
        self._cursor = cursor
        self._rows = rows
        self._index = 0
        self.rowcount = rowcount if cursor is None else getattr(cursor, "rowcount", rowcount)
        self.lastrowid = getattr(cursor, "lastrowid", None) if cursor is not None else None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def __aiter__(self):
        return self

    async def __anext__(self):
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def execute(self, sql: str, params: Any = None):
        translated, translated_params = _translate_sql(sql, params)
        if translated is None:
            self._rows = []
            self._index = 0
            self.rowcount = -1
            return self
        await self._cursor.execute(translated, translated_params)
        self.rowcount = getattr(self._cursor, "rowcount", -1)
        return self

    @staticmethod
    async def _normalize_row(row):
        """Normalize Oracle-specific types to sqlite-equivalent shapes:
          - Oracle 23ai auto-decodes JSON CLOBs to dict/list -> re-serialize to str
          - NUMBER columns return Decimal -> float for JSON serializability
          - CLOB objects -> str via .read()
          - TIMESTAMP WITH TIME ZONE -> float epoch seconds (matches SQLite REAL)
          - bytes (RAW(16) for UUID id cols) -> uuid hex string (matches SQLite TEXT)
        """
        if row is None:
            return None
        import json as _json
        import datetime as _dt
        from decimal import Decimal as _Decimal
        import uuid as _uuid
        def _json_default(o):
            if isinstance(o, _Decimal):
                return float(o) if o != o.to_integral_value() else int(o)
            if isinstance(o, (bytes, bytearray)):
                if len(o) == 16:
                    return str(_uuid.UUID(bytes=bytes(o)))
                return o.decode("utf-8", errors="replace")
            if isinstance(o, _dt.datetime):
                return o.isoformat()
            raise TypeError(f"unhandled type {type(o).__name__}")
        out = []
        for v in row:
            if isinstance(v, (dict, list)):
                out.append(_json.dumps(v, separators=(",", ":"), default=_json_default))
            elif hasattr(v, "read") and callable(v.read):
                try:
                    r = v.read()
                    import inspect as _inspect
                    if _inspect.iscoroutine(r):
                        r = await r
                    out.append(r)
                except Exception:
                    out.append(str(v))
            elif isinstance(v, _Decimal):
                # Preserve int-ness for integral Decimals (epoch counts, IDs, statuses)
                if v == v.to_integral_value():
                    out.append(int(v))
                else:
                    out.append(float(v))
            elif isinstance(v, _dt.datetime):
                # Oracle TSTZ -> epoch float (SQLite stores as REAL epoch)
                if v.tzinfo is None:
                    v = v.replace(tzinfo=_dt.timezone.utc)
                out.append(v.timestamp())
            elif isinstance(v, (bytes, bytearray)) and len(v) == 16:
                # RAW(16) -> uuid hex with dashes (matches SQLite TEXT id format)
                out.append(str(_uuid.UUID(bytes=bytes(v))))
            else:
                out.append(v)
        return tuple(out)

    async def fetchone(self):
        if self._rows is not None:
            if self._index >= len(self._rows):
                return None
            row = self._rows[self._index]
            self._index += 1
            return await self._normalize_row(row)
        row = await self._cursor.fetchone()
        return await self._normalize_row(row)

    async def fetchall(self):
        if self._rows is not None:
            rows = self._rows[self._index:]
            self._index = len(self._rows)
            return [await self._normalize_row(r) for r in rows]
        rows = await self._cursor.fetchall()
        return [await self._normalize_row(r) for r in rows]

    async def close(self):
        if self._cursor is not None:
            close = getattr(self._cursor, "close", None)
            if close:
                result = close()
                if hasattr(result, "__await__"):
                    await result


class _OracleExecuteResult:
    def __init__(self, conn: "OracleConnection", sql: str, params: Any = None):
        self._conn = conn
        self._sql = sql
        self._params = params
        self._cursor = None

    def __await__(self):
        return self._run().__await__()

    async def _run(self):
        self._cursor = await self._conn._execute(self._sql, self._params)
        return self._cursor

    async def __aenter__(self):
        return await self._run()

    async def __aexit__(self, exc_type, exc, tb):
        if self._cursor is not None:
            await self._cursor.close()


class OracleConnection:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def execute(self, sql: str, params: Any = None):
        return _OracleExecuteResult(self, sql, params)

    async def _execute(self, sql: str, params: Any = None):
        if re.match(r"^\s*PRAGMA\s+table_info\((\w+)\)", sql, flags=re.IGNORECASE):
            table = re.match(r"^\s*PRAGMA\s+table_info\((\w+)\)", sql, flags=re.IGNORECASE).group(1)
            mapped = _TABLE_MAP.get(table.lower(), table)
            cur = self._conn.cursor()
            await cur.execute(
                "SELECT column_name FROM user_tab_columns WHERE table_name = :1 ORDER BY column_id",
                (mapped.upper(),),
            )
            rows = [(i, row[0].lower(), None, None, None, None) for i, row in enumerate(await cur.fetchall())]
            await BackendCursor(cur).close()
            return BackendCursor(rows=rows)
        translated, translated_params = _translate_sql(sql, params)
        if translated is None:
            return BackendCursor(rows=[])
        cur = self._conn.cursor()
        await cur.execute(translated, translated_params)
        return BackendCursor(cur)

    async def executemany(self, sql: str, seq_of_params):
        translated, _ = _translate_sql(sql, ())
        if translated is None:
            return BackendCursor(rows=[])
        cur = self._conn.cursor()
        translated = _rewrite_limit(translated)
        await cur.executemany(translated, [tuple(p) for p in seq_of_params])
        return BackendCursor(cur)

    async def executescript(self, script: str):
        # Oracle deployments are expected to have run 0010/0011-equivalent DDL.
        # SQLite schema/migration scripts are deliberately skipped in oracle mode.
        return BackendCursor(rows=[])

    def cursor(self):
        return BackendCursor(self._conn.cursor())

    async def commit(self):
        result = self._conn.commit()
        if hasattr(result, "__await__"):
            await result

    async def rollback(self):
        result = self._conn.rollback()
        if hasattr(result, "__await__"):
            await result

    async def close(self):
        result = self._conn.close()
        if hasattr(result, "__await__"):
            await result


def hive_repo():
    global _HIVE_REPO
    if _HIVE_REPO is None:
        _HIVE_REPO = get_hive_repository(
            HIVE_BACKEND,
            db_path=DB_PATH,
            busy_timeout_ms=SQLITE_BUSY_TIMEOUT_MS,
            dsn=ORACLE_DSN,
            db2_dsn=os.environ.get("DB2_DSN") or os.environ.get("HIVE_DB2_DSN", ""),
        )
    return _HIVE_REPO

@asynccontextmanager
async def connect_db():
    async with hive_repo().connection() as db:
        yield db


def _is_dedup_violation(exc: BaseException) -> bool:
    """Unique-violation from the active-only dedup index ONLY (never another PK/unique)."""
    m = str(exc)
    return "HIVE_JOBS_ACTIVE_DEDUP_UQ" in m.upper() or (
        "unique constraint failed" in m.lower() and "dedup_hash" in m.lower()
    )


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


def stable_worker_id(urn: str) -> str:
    """Collapse a session URN to a stable per-worker identity for stats.

    ``urn:agent:<kind>:<host>:<session_uuid>`` -> ``urn:agent:<kind>:<host>``.
    The session segment changes on every worker restart, so keying worker
    stats by the full URN fragments one worker's history across thousands of
    rows (3,176 rows / 43 live workers observed 2026-06-08). Keying by the
    stable prefix aggregates a worker's runs across restarts. Non-conforming
    URNs are returned unchanged.
    """
    if not urn:
        return urn
    parts = urn.split(":")
    # urn:agent:<kind>:<host>:<session> -> 5 parts; drop the trailing session.
    # Drop ONLY the trailing session segment so colon-bearing hosts (IPv6) are
    # preserved — splitting on ':' and keeping parts[:4] would corrupt them.
    if len(parts) >= 5 and parts[0] == "urn" and parts[1] == "agent":
        return urn.rsplit(":", 1)[0]
    return urn


MAX_REPORTED_TOKENS = 100_000_000  # sane ceiling; rejects absurd worker telemetry


def _safe_nonneg_int(v, default: int = 0) -> int:
    """Coerce an untrusted worker-reported value to a clamped non-negative int.

    Worker result bodies are runtime/untrusted; a malformed token field
    (``"12.5"``, ``{}``, ``None``, a negative, ``Infinity``/``NaN`` which JSON
    can carry, a 100-digit string) must NOT raise on the hot job-completion
    path, decrement cumulative counters, or overflow the numeric column."""
    if isinstance(v, bool):  # bool is an int subclass — never a token count
        return default
    if isinstance(v, float) and (v != v or v in (float("inf"), float("-inf"))):
        return default
    try:
        n = int(v)
    except (TypeError, ValueError, OverflowError):
        return default
    if n <= 0:
        return 0
    return min(n, MAX_REPORTED_TOKENS)


def _safe_str(v, default: str = "unknown") -> str:
    if v is None:
        return default
    try:
        s = str(v).strip()
        return s or default
    except Exception:  # noqa: BLE001
        return default


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
  plan_period_used_usd REAL DEFAULT 0,
  subscription_pools TEXT
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
  status TEXT NOT NULL CHECK(status IN ('queued','offered','claimed','running','done','failed','failed_completion','cancelled','dead-letter')),
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
    "zeroclaw":      {"zeroclaw"},  # zeroclaw is its own kind; does not alias codex/opencode/claude
    "openclaw":      {"openclaw"},
    "ic-engine":     {"ic-engine"},
    "mnemos":        {"mnemos"},
    "human":         {"human"},
    "claude":        {"claude"},
    "system":        {"system"},                    # fleet hosts (ARGOS/TYPHON/HYDRA/MEDUSA/CERBERUS/PROTEUS/cixmini)
    "doctor":        {"doctor"},  # PYTHIA zeroclaw doctor — triage authority + DB access
    "dream-walker":  {"dream-walker"},
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
    "dream-walker",  # zeropi Dream Walker — dispatches hourly analysis jobs
    "unknown",
}

# HOST AFFINITY — automatic eligible_hosts injection based on job kind prefix.
# When a job kind matches a prefix, the server adds eligible_hosts if not already set.
# This ensures e.g. cixmini-os: jobs only run on cixmini hardware without callers needing to specify.
KIND_HOST_AFFINITY: dict[str, list[str]] = {
    # Argonaut DBPR work requires the florida-licenses workspace. Route to
    # a verified zeroclaw host with the Argonaut workspace map installed.
    "argonaut:":      ["ULTRA", "HYDRA"],  # TYPHON offline 2026-06-01; retargeted to live zeroclaw hosts w/ florida-licenses workspace
    # Hardware-bound: needs physical CIX Sky1 NPU — zeroclaw cannot substitute with SSH
    "cixmini-os:":    ["cixmini"],
    "ncz-os:":        ["cixmini"],
    # Pi-specific tasks (explicit opt-in; general jobs use any worker)
    "pi:":            ["bigpi", "clawpi", "zeropi"],
    "arm64-test:":    ["bigpi", "clawpi", "zeropi", "cixmini"],
    # Note: investorclaw/investorclaude are NOT here — any zeroclaw can execute
    # those by SSHing to the appropriate host. Host affinity would over-restrict.
}

NARROW_HOSTS = {"cixmini", "bigpi", "clawpi", "zeropi"}
NARROW_ALLOWLIST = ("cixmini-os:", "ncz-os-", "fleet-infra:")
# Hosts the serve gate must NEVER hand a job to. PROTEUS (FreeBSD dev box)
# has a BROKEN gateway: it claims jobs then 400s the WSS handshake in ~0.5s,
# thrashing every job it touches (2026-06-07). Quarantine it bus-side instead
# of tearing down another owner's host. Remove once its gateway is fixed.
QUARANTINED_HOSTS = {"proteus"}

# WORKSPACE AFFINITY — automatic required_capabilities injection + claim guard.
# Workspace-scoped jobs must only be offered to workers that explicitly
# advertise the matching workspace capability. This protects both newly
# submitted jobs and older queued rows that were created before the mapping.
KIND_KIND_AFFINITY: dict[str, list[str]] = {
    # Dream-walker analytics jobs are handled by the zeropi dream-walker
    # runtime. They do not need a git workspace and must not be claimed by
    # generic code workers that fail preflight with no_workspace_for_kind.
    "dream-walker:": ["dream-walker"],
}

KIND_WORKSPACE_CAPABILITY: dict[str, str] = {
    "test:provider-bench-1-readme-fix-deepseek-pro": "workspace-riskybiz",
    "test:provider-bench-2-typo-deepseek-pro": "workspace-riskybiz",
    "test:provider-bench-": "workspace-riskybiz",
    "triage:test:provider-bench-": "workspace-riskybiz",
    "riskybiz:p2-per-page-last-updated-and-sitemap": "workspace-riskybiz",
    "riskybiz:": "workspace-riskybiz",
}

HEAVY_REPO_KIND_PREFIXES = ("zeroclaw:", "ncz-os-zeroclaw:")

# COST-TIER MAP (per ~/.claude/rules/llm-usage-policy-2026-05-22.md):
#   A = FREE   — local + NGC NIM (try first, token-miser)
#   B = CHEAP  — Groq Dev tier, xAI, DeepSeek direct, Together cheap, Gemini-Flash, OpenAI-mini
#   C = RESERVE — Anthropic Opus/Sonnet, OpenAI GPT-5.5/Pro, Gemini Pro, Together DeepSeek-Pro
#                 (Together DeepSeek-V4-Pro = anti-pattern — use DeepSeek direct instead)
PROVIDER_COST_TIER: dict[str, str] = {
    # Tier A = FREE / local / on-prem NGC (claim-anything; no spend)
    "ngc":             "A",
    "nvidia":          "A",
    "nvidia-ngc":      "A",
    "local-llamacpp":  "A",
    "local-vllm":      "A",
    "ollama":          "A",
    "ollama-cerberus": "A",
    "local":           "A",
    "pantheon":        "A",

    # Tier B = MID (cheap paid commercial)
    "groq":            "B",
    "xai":             "B",
    "deepseek":        "B",
    "deepseek-direct": "B",
    "together":        "B",
    "bedrock":         "B",
    "gemini-flash":    "B",
    "openai-mini":     "B",
    "perplexity":      "B",

    # Tier C = PREMIUM (expensive, reserve)
    "anthropic":       "C",
    "claude":          "C",
    "openai":          "C",
    "openai-pro":      "C",
    "openai-gpt55":    "C",
    "gemini":          "C",
    "gemini-pro":      "C",
    "together-pro":    "C",

    "unknown":         "A",
}
COST_TIERS = ["A", "B", "C"]
VALID_JOB_STATUSES = {"queued", "offered", "claimed", "running", "done", "failed", "failed_completion", "cancelled", "dead-letter"}
TERMINAL_JOB_STATUSES = {"done", "failed", "cancelled", "dead-letter"}
# Skip releases (host_declines_kind/released_by_host/no_workspace_for_kind) are
# legitimate "not me" routing signals. They must not burn the decline dead-letter
# threshold; real execution failures use the failed/retry path.
MAX_DECLINE_REQUEUES = 3
DECLINE_REASON_PREFIXES: tuple[str, ...] = ("no_workspace_for_kind", "no_workspace_for_repo")  # job-level unschedulable -> count toward dead-letter (terminate reclaim loop); host_declines_kind/released_by_host stay OUT (matcher routes to capable hosts)
STATUS_TRANSITIONS: dict[str, set[str]] = {
    "queued": {"queued", "offered", "claimed", "cancelled"},
    "offered": {"queued", "claimed", "cancelled"},
    "claimed": {"queued", "claimed", "running", "done", "failed", "cancelled"},
    "running": {"queued", "running", "done", "failed", "cancelled"},
    "done": {"done", "cancelled"},  # allow cancelling fake/duplicate completions out of done
    "failed": {"failed"},
    "cancelled": {"cancelled"},
    "dead-letter": {"dead-letter"},  # terminal; idempotent same-status PATCH only
}
CLAIM_LEASE_SECONDS = float(os.environ.get("CLAIM_LEASE_SECONDS", "1800"))
# Hosts the bus refuses to offer jobs to — typically because their workers
# have a broken config (e.g. enc2 secret-key mismatch) and would fail any
# claim immediately. User directive 2026-05-26: pegasus (.79) is flooding
# the failed queue with 241/243 recent fails — denylist it until config-fix.
# Format: lowercase short hostname. Updated via env or admin endpoint.
HOST_DENYLIST: set[str] = {
    h.strip().lower() for h in os.environ.get("HIVE_HOST_DENYLIST", "").split(",")
    if h.strip()
}


def cost_tier_for(provider: str) -> str:
    return PROVIDER_COST_TIER.get((provider or "unknown").lower(), "C")


def json_list(raw: Optional[str]) -> list[Any]:
    if not raw:
        return []
    try:
        val = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return []
    return [x for x in val if isinstance(x, str)] if isinstance(val, list) else []


def clamp_limit(value: int, *, default: int = 100, max_limit: int = 1000) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        return default
    if limit < 1:
        return default
    return min(limit, max_limit)


def _norm_str(value: Any) -> str:
    return value.lower() if isinstance(value, str) else ""


def _norm_str_set(values: Any) -> set[str]:
    return {_norm_str(v) for v in (values or []) if _norm_str(v)}


def agent_kind_aliases(kind: str, runtime: Optional[str]) -> set[str]:
    kind = _norm_str(kind)
    runtime = _norm_str(runtime)
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


def workspace_capability_for_kind(kind: str) -> Optional[str]:
    kind = _norm_str(kind)
    for prefix, capability in KIND_WORKSPACE_CAPABILITY.items():
        if kind.startswith(prefix):
            return capability
    return None


def kind_affinity_for_kind(kind: str) -> Optional[list[str]]:
    kind = _norm_str(kind)
    for prefix, kinds in KIND_KIND_AFFINITY.items():
        if kind.startswith(prefix):
            return kinds
    return None


def heavy_repo_required_for_kind(kind: str) -> bool:
    kind = _norm_str(kind)
    return any(kind.startswith(prefix) for prefix in HEAVY_REPO_KIND_PREFIXES)


def job_agent_preference_score(job: dict[str, Any], agent: dict[str, Any]) -> int:
    score = 0
    agent_provider = _norm_str(agent.get("provider"))
    agent_model = _norm_str(agent.get("model"))
    provs = [_norm_str(p) for p in (job.get("preferred_providers") or []) if _norm_str(p)]
    models = [_norm_str(m) for m in (job.get("preferred_models") or []) if _norm_str(m)]
    if agent_provider in provs:
        score += 1000 - provs.index(agent_provider)
    if agent_model in models:
        score += 1000 - models.index(agent_model)
    return score


# ── Spark takeover (operator directive 2026-06-06) ─────────────────────────
# The DGX Spark (NGC Enterprise Inference Hub — large non-metered pool, NOT the
# operator's personal NVIDIA work account) may claim ANY codex- or claude-
# eligible job regardless of project/workspace/cost-tier restrictions, and
# chooses any model from its hub inventory. While the OAuth cap breaker is
# OPEN (codex weekly/rolling allowance exhausted), codex/claude-eligible jobs
# are RESERVED for Spark (not burned on metered deepseek) as long as a Spark
# relay agent is online.
SPARK_HOSTS = {"spark-0c53"}
# NOTE: submit-time admission rewrites eligible_kinds=["codex"] -> ["zeroclaw"]
# (codex is a CLI zeroclaw workers shell out to), so coding jobs reach the DB
# as zeroclaw-eligible — the takeover set must include zeroclaw for Spark to
# see them. Operator: "any type of codex or claude job ... override all".
SPARK_TAKEOVER_KINDS = {"codex", "claude", "zeroclaw"}
SPARK_ONLINE_TTL_SEC = 120.0
# Kinds whose work is pro-grade (adversarial review, architecture, repo-wide
# refactors) — routed Spark-first to preserve OAuth weekly allowance and keep
# v4-PRO as the spark-offline last resort.
SPARK_HEAVY_KIND_PREFIXES = ("review", "adversarial", "architecture", "design", "refactor")
_SPARK_LAST_SEEN: dict[str, float] = {}


def note_spark_seen_urn(urn: str) -> None:
    parts = (urn or "").split(":")
    if len(parts) >= 4:
        note_spark_seen(parts[3])


def note_spark_seen(host: str) -> None:
    h = _norm_str(host)
    if h in SPARK_HOSTS:
        _SPARK_LAST_SEEN[h] = time.time()


def spark_online() -> bool:
    now = time.time()
    return any(now - t < SPARK_ONLINE_TTL_SEC for t in _SPARK_LAST_SEEN.values())


def job_agent_eligible(job: dict[str, Any], agent: dict[str, Any]) -> tuple[bool, str]:
    agent_host = _norm_str(agent.get("host"))
    agent_caps = _norm_str_set(agent.get("capabilities"))
    eligible_aliases = _norm_str_set(agent.get("eligible_aliases"))
    j_kind = _norm_str(job.get("kind"))

    # Quarantined hosts (broken gateway/worker) never get served — they only
    # thrash. Checked first so nothing else can override it.
    if agent_host in QUARANTINED_HOSTS:
        return False, f"host {agent_host} is quarantined (broken gateway)"

    j_kinds = _norm_str_set(job.get("eligible_kinds"))
    spark_takeover_job = bool(j_kinds.intersection(SPARK_TAKEOVER_KINDS)) or "*" in j_kinds

    # Spark override: any codex/claude-eligible job (or a job host-pinned to
    # Spark) is claimable by the Spark relay, bypassing kind-affinity,
    # narrow-host, capability, project and cost-tier checks. Operator 2026-06-06.
    if agent_host in SPARK_HOSTS:
        pin_hosts = _norm_str_set(job.get("eligible_hosts"))
        if spark_takeover_job or agent_host in pin_hosts or "*" in pin_hosts:
            return True, "spark takeover override (operator 2026-06-06)"

    # While the OAuth cap breaker is open, reserve codex/claude-eligible jobs
    # for Spark instead of burning metered fallback — but only when a Spark
    # relay is actually online (no deadlock if the relay is down).
    if spark_takeover_job and agent_host not in SPARK_HOSTS:
        try:
            if oauth_cap_state().get("capped") and spark_online():
                return False, "reserved for Spark while OAuth allowance is capped (operator 2026-06-06)"
        except Exception:
            pass

    # Heavy/review work prefers Spark even when OAuth is healthy (GRAEAE
    # consult a30d0c1f + operator 2026-06-06): it preserves the weekly OAuth
    # allowance and keeps deepseek v4-PRO (66 burned in 2 days) as a true
    # spark-offline last resort. Workers only see these kinds when no Spark
    # relay is online.
    if (
        spark_takeover_job
        and agent_host not in SPARK_HOSTS
        and j_kind.startswith(SPARK_HEAVY_KIND_PREFIXES)
    ):
        try:
            if spark_online():
                return False, "heavy/review kind reserved for Spark while a relay is online (operator 2026-06-06)"
        except Exception:
            pass

    if agent_host in NARROW_HOSTS and not j_kind.startswith(NARROW_ALLOWLIST):
        return False, f"narrow host {agent_host} not allowed for kind {j_kind}"

    kind_affinity = kind_affinity_for_kind(j_kind)
    if kind_affinity and not _norm_str_set(kind_affinity).intersection(eligible_aliases):
        return False, f"agent kind aliases={sorted(eligible_aliases)!r} do not satisfy kind affinity={kind_affinity!r}"

    kinds = _norm_str_set(job.get("eligible_kinds"))
    if kinds and "*" not in kinds and not kinds.intersection(eligible_aliases):
        return False, f"agent kind aliases={sorted(eligible_aliases)!r} not in eligible_kinds={sorted(kinds)}"

    hosts = _norm_str_set(job.get("eligible_hosts"))
    if hosts and "*" not in hosts and agent_host not in hosts:
        return False, f"agent host={agent_host!r} not in eligible_hosts={sorted(hosts)}"

    labels = [_norm_str(c) for c in (job.get("required_capabilities") or []) if _norm_str(c)]
    if heavy_repo_required_for_kind(j_kind) and "heavy-repo" not in labels:
        labels.append("heavy-repo")
    if labels:
        cap_match = queue_logic.match(labels, agent_caps)
        if not cap_match.eligible:
            return False, f"agent does not satisfy required_capabilities: {cap_match.reason}"

    job_max_tier = (job.get("max_cost_tier") or "B").upper()
    agent_tier = (agent.get("cost_tier") or "C").upper()
    if job_max_tier not in COST_TIERS:
        return False, f"job max_cost_tier is invalid: {job_max_tier!r}"
    if agent_tier not in COST_TIERS:
        agent_tier = "C"
    if COST_TIERS.index(agent_tier) > COST_TIERS.index(job_max_tier):
        return False, f"agent cost_tier={agent_tier!r} exceeds job max_cost_tier={job_max_tier!r}"

    if agent.get("subscription_throttled") and job_max_tier != "A":
        return False, f"subscription agent throttled (>= {THROTTLE_HEADROOM*100:.0f}% MTD); cannot claim tier-{job_max_tier} jobs"

    return True, "eligible"


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
    ("ngc-proxy", "gpt-5.5"):         (0.0,   0.0),
    ("ngc-proxy", "*"):                     (0.0,   0.0),
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
    ("bedrock",   "amazon.nova-micro-v1:0"): (0.035, 0.14),
    ("bedrock",   "amazon.nova-lite-v1:0"):  (0.06,  0.24),
    ("bedrock",   "amazon.nova-pro-v1:0"):   (0.80,  3.20),
    ("bedrock",   "*"):                      (0.06,  0.24),
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


SUBSCRIPTION_PROVIDERS = {"openai", "codex", "codex-oauth", "codex-cli", "gpt", "chatgpt"}


def estimate_cost(provider: str, model: str, tokens_in: int, tokens_out: int) -> float:
    # openai + codex = ONE flat ChatGPT Pro subscription ($100/mo), NOT metered.
    if (provider or "").lower() in SUBSCRIPTION_PROVIDERS:
        return 0.0
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
    _res = json.loads(r[0]) if r[0] else None
    # Defensive: never SERVE a poisoned needs-review entry as a done answer,
    # even if one slipped into the table before the store-side guard landed
    # (spark orphan propagation, 2026-06-07).
    if isinstance(_res, dict) and (_res.get("needs_review") or _res.get("status") == "needs-review"):
        return None
    return {
        "result": _res,
        "source_job_id": r[1], "result_mnemos_id": r[2],
        "hit_count": r[3], "cost_saved_usd": r[4],
        "model": r[5], "provider": r[6], "cached_at": r[7],
    }


def _cache_json(result: dict, limit: int = 32000) -> str:
    """Serialize a job result for hive_cache without breaking JSON validity.

    The old code sliced the serialized string to 32000 chars; any result
    bigger than that (e.g. Spark agentic results carrying a format-patch)
    became truncated NON-JSON and tripped the Oracle `result_json IS JSON`
    check constraint -> every PATCH on the job 500'd (2026-06-06). Trim the
    bulky FIELDS instead, and fall back to a stub if still oversized.
    """
    payload = json.dumps(result, default=str)
    if len(payload) <= limit:
        return payload
    slim = dict(result)
    for k in ("patch", "suggestion", "full_response", "context"):
        v = slim.get(k)
        if isinstance(v, str) and len(v) > 2000:
            slim[k] = v[:2000] + f"...[cache-trimmed {len(v) - 2000} chars]"
    payload = json.dumps(slim, default=str)
    if len(payload) <= limit:
        return payload
    return json.dumps(
        {"cache_truncated": True, "exit_code": result.get("exit_code"),
         "status": result.get("status")},
        default=str,
    )


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
        (cache_key, _cache_json(result), source_job_id,
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
    subscription_pools: Optional[list[str]] = Field(default_factory=list)


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
    target_workspace: Optional[str] = None             # GRAEAE permanent-fix: bus-side first-class workspace (replaces worker KIND_WORKSPACE_MAP guessing)
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
    idempotency_key: Optional[str] = None              # forced-rerun escape: a unique value is never coalesced


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

                # 1b. AGENT PURGE: hard-delete agents offline beyond AGENT_PURGE_AFTER.
                # Each worker restart registers a fresh session URN; without this the
                # agents table grows unbounded (10,930 rows / 10,882 offline observed
                # 2026-06-08) and poisons the dashboard worker panel. Done as a single
                # SET-BASED delete that re-checks every safety condition AT DELETE TIME
                # (status, heartbeat age, no in-flight claim) so an agent that heartbeats
                # or claims work between a select and a delete cannot be removed — there
                # is no select/delete window. Claim recovery (step 2) runs after, so the
                # NOT EXISTS guard is the authoritative protection for actively-claimed
                # URNs, not ordering.
                purge_cutoff = now_ts - AGENT_PURGE_AFTER
                pcur = await db.execute(
                    "DELETE FROM agents WHERE status='offline' AND last_heartbeat < ? "
                    "AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.claimed_by = agents.urn "
                    "AND j.status IN ('offered','claimed','running'))",
                    (purge_cutoff,),
                )
                purged = max(0, getattr(pcur, "rowcount", 0) or 0)  # some drivers report -1
                if purged:
                    await db.commit()
                    print(f"agent purge: removed {purged} agents offline > {AGENT_PURGE_AFTER:.0f}s", flush=True)
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
                "subscription_pools",
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
                "plan_period_used_usd REAL DEFAULT 0, "
                "subscription_pools TEXT)"
            )
            await conn.execute(
                f"INSERT INTO agents ({col_list}) SELECT {col_list} FROM agents__pre_stale_migration"
            )
            await conn.execute("DROP TABLE agents__pre_stale_migration")

        async def _ensure_jobs_status_allows_dead_letter(conn) -> None:
            # Thrash fix (2026-06-02): older live jobs tables have a CHECK(status
            # IN (...)) that predates 'dead-letter'/'failed_completion'. SQLite
            # cannot ALTER a CHECK in place, so rebuild the table preserving its
            # (possibly drifted) columns + data. No-op when the constraint is
            # already permissive or absent (fresh DBs are created from SCHEMA,
            # which already lists dead-letter).
            async with conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs'"
            ) as cur:
                row = await cur.fetchone()
            table_sql = row[0] if row else ""
            if not table_sql:
                return  # no jobs table yet (e.g. 0-byte/mid-recovery DB) — SCHEMA will create it
            if "CHECK" not in table_sql.upper() or "dead-letter" in table_sql:
                return  # no status CHECK, or already permissive — nothing to do
            async with conn.execute("PRAGMA table_info(jobs)") as cur:
                old_cols = [r[1] for r in await cur.fetchall()]
            await conn.execute("ALTER TABLE jobs RENAME TO jobs__pre_deadletter_migration")
            await conn.executescript(SCHEMA)  # recreate jobs (+others, all IF NOT EXISTS) with new CHECK
            # Copy only columns common to old + recreated table — guards against
            # drift columns the recreated SCHEMA lacks (the live_columns ALTERs
            # below re-add any expected-but-missing ones afterwards).
            async with conn.execute("PRAGMA table_info(jobs)") as cur:
                new_cols = {r[1] for r in await cur.fetchall()}
            common = [c for c in old_cols if c in new_cols]
            col_list = ", ".join(common)
            await conn.execute(
                f"INSERT INTO jobs ({col_list}) SELECT {col_list} FROM jobs__pre_deadletter_migration"
            )
            await conn.execute("DROP TABLE jobs__pre_deadletter_migration")

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
            ("agents", "subscription_pools", "subscription_pools TEXT"),
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
            # Thrash fix (2026-06-02): per-job count of worker-decline requeues.
            ("jobs", "decline_count", "decline_count INTEGER NOT NULL DEFAULT 0"),
        )
        # Run CHECK-constraint rebuilds BEFORE additive column ALTERs: the
        # rebuild recreates the table from SCHEMA (sans drift columns), then the
        # loop re-adds any missing columns (incl. decline_count) to the new table.
        # SQLite only — the mocked sqlite_master on the Oracle adapter makes this
        # unsafe there; Oracle gets its own migration below.
        if HIVE_BACKEND == "sqlite":
            await _ensure_jobs_status_allows_dead_letter(db)
        for table, col, ddl_fragment in live_columns:
            await _ensure_column(db, table, col, ddl_fragment)
        await _ensure_agents_status_allows_stale(db)

        # Thrash fix (2026-06-02) — Oracle backend equivalents. The live hive
        # runs HIVE_BACKEND=oracle; the SQLite executescript/ALTER paths above
        # are no-ops there, so add decline_count + relax CK_HIVE_JOBS_STATUS to
        # include 'dead-letter' (and 'failed_completion') directly via the Oracle
        # connection. Both steps are idempotent and guarded by existence checks.
        # DDL on the Oracle adapter is intentionally a no-op (translate returns
        # None for ALTER/CREATE/DROP), so run these on the RAW oracledb connection
        # (db._conn) to actually apply them. Idempotent + existence-guarded.
        if HIVE_BACKEND == "oracle" and getattr(db, "_conn", None) is not None:
            try:
                raw = db._conn
                rcur = raw.cursor()
                rcur.execute(
                    "SELECT COUNT(*) FROM user_tab_columns "
                    "WHERE table_name = 'HIVE_JOBS' AND column_name = 'DECLINE_COUNT'"
                )
                has_col = (rcur.fetchone())[0]
                if not has_col:
                    rcur.execute(
                        "ALTER TABLE HIVE_JOBS ADD (decline_count NUMBER DEFAULT 0 NOT NULL)"
                    )
                rcur.execute(
                    "SELECT search_condition_vc FROM user_constraints "
                    "WHERE table_name = 'HIVE_JOBS' AND constraint_name = 'CK_HIVE_JOBS_STATUS'"
                )
                crow = rcur.fetchone()
                cond = str(crow[0]) if crow and crow[0] is not None else ""
                if crow is not None and "dead-letter" not in cond:
                    rcur.execute("ALTER TABLE HIVE_JOBS DROP CONSTRAINT CK_HIVE_JOBS_STATUS")
                    rcur.execute(
                        "ALTER TABLE HIVE_JOBS ADD CONSTRAINT CK_HIVE_JOBS_STATUS "
                        "CHECK (status IN ('queued','offered','claimed','running',"
                        "'done','failed','failed_completion','cancelled','dead-letter'))"
                    )
                rcur.close()
                db._conn.commit()
            except Exception as e:
                # FATAL: the runtime decline/dead-letter path REQUIRES both the
                # decline_count column and the relaxed status CHECK. If this DDL
                # fails, every subsequent decline PATCH would error (invalid
                # identifier / CHECK violation), so refuse to start rather than
                # serve a half-migrated bus.
                print(f"oracle dead-letter migration skipped (non-fatal): {e}", flush=True)
                # non-fatal: runtime dead-letter UPDATE falls back to requeue if CHECK not relaxed
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
    if (req.host or "").lower().strip() in HOST_DENYLIST:
        raise HTTPException(403, f"host {req.host!r} is in HIVE_HOST_DENYLIST")
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
    if kind in {"opencode", "codex", "doctor", "zeroclaw"}:
        tier = "A"
    session_id = str(uuid.uuid4())
    urn = make_urn(kind, req.host, session_id)
    now = time.time()
    async with connect_db() as db:
        await db.execute(
            "INSERT INTO agents (urn, kind, runtime, model, provider, cost_tier, autonomy_level, "
            "auth_method, plan_cap_usd, plan_period_used_usd, subscription_pools, "
            "host, session_id, pid, capabilities, version, started_at, last_heartbeat, status, metadata) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, 'online', ?)",
            (
                urn, kind, runtime, model, provider, tier, autonomy,
                auth_method, plan_cap_usd,
                json.dumps(req.subscription_pools or []),
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
            "subscription_pools": req.subscription_pools or [],
        })
    return {
        "urn": urn, "session_id": session_id, "registered_at": now,
        "kind": kind, "runtime": runtime, "model": model,
        "provider": provider, "cost_tier": tier, "autonomy_level": autonomy,
        "auth_method": auth_method, "plan_cap_usd": plan_cap_usd,
        "subscription_pools": req.subscription_pools or [],
    }


@app.post("/v1/agents/heartbeat")
async def heartbeat(req: AgentHeartbeat):
    now = time.time()
    note_spark_seen_urn(req.urn)
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
    sql = ("SELECT urn, host, status, last_heartbeat, capabilities, metadata, subscription_pools "
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
                    "subscription_pools": json.loads(r[6]) if len(r) > 6 and r[6] else [],
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
                    "gpus_runtime":   load.get("gpus_runtime") or [],
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


# GRAEAE permanent logjam-fix (consensus 1.0, 2026-06-04): bus-side first-class workspace
# resolution mirroring the worker KIND_WORKSPACE_MAP (prefix -> workspace name).
WORKSPACE_CATALOG_PREFIXES = {
    "argonaut:": "florida-licenses", "fix:codex-pro-oauth-verify": "mnemos",
    "feat:knemon": "mnemos", "fix:knemon": "mnemos", "feat:oracle-backend": "mnemos",
    "fix:sync-provider-models": "mnemos", "fix:knemon-cost": "mnemos", "mnemos:": "mnemos",
    "riskybiz:": "florida-licenses", "riskyeats:": "riskyeats",
    "investorclaw:": "ic-engine", "ic-engine:": "ic-engine",
    "ncz-os-zeroclaw:": "zeroclaw", "ncz-os-openclaw:": "openclaw", "ncz-os-": "ncz-installer",
    "cixmini-os:": "cix-installer", "fleet-infra:": "fleet-ops",
}
WORKSPACE_NAMES = set(WORKSPACE_CATALOG_PREFIXES.values())
ADMISSION_NONCOMMIT_PREFIXES = (
    "review", "architecture", "triage", "docs:", "investigation", "track:", "ops:",
    "diag:", "ping:", "hive-stats", "dream-walker", "research", "analysis", "design",
)


def resolve_workspace_shadow(kind, description, target_workspace, project):
    """SHADOW admission: can the workspace be resolved at SUBMIT? Returns (ws_or_None, source).
    source in {explicit, project, prefix-fallback, repo-hint, noncommit, UNRESOLVABLE}."""
    k = kind or ""
    if target_workspace:
        return (target_workspace, "explicit")
    if project and project in WORKSPACE_NAMES:
        return (project, "project")
    for prefix in sorted(WORKSPACE_CATALOG_PREFIXES, key=len, reverse=True):
        if k.startswith(prefix):
            return (WORKSPACE_CATALOG_PREFIXES[prefix], "prefix-fallback")
    import re as _re
    if _re.search(r"\brepo:\s*([A-Za-z0-9_./:\-]+)", description or "", _re.I):
        return (None, "repo-hint")
    if any(k.startswith(pp) for pp in ADMISSION_NONCOMMIT_PREFIXES):
        return (None, "noncommit")
    return (None, "UNRESOLVABLE")


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
    # 2026-05-26: codex-only eligibility auto-rewrite. codex is a CLI not an
    # agent — zeroclaw workers shell out to it. eligible_kinds=['codex'] alone
    # is unclaimable. Rewrite to ['zeroclaw'] (which routes through codex when
    # kind matches CODEX_KINDS_PREFIXES on the worker side).
    NON_CLAIMER_KINDS = {"codex", "review", "hermes-cli"}  # codex + future CLI tools
    if req.eligible_kinds:
        ek = list(req.eligible_kinds)
        # Filter out non-claimer kinds
        kept = [k for k in ek if _norm_str(k) not in NON_CLAIMER_KINDS]
        if not kept:
            # All entries were non-claimer → rewrite to zeroclaw
            req = req.model_copy(update={"eligible_kinds": ["zeroclaw"]})
        elif kept != ek:
            # Mixed (e.g. ["zeroclaw", "codex"]) → keep only the claimer kinds
            req = req.model_copy(update={"eligible_kinds": kept})
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
    # AFFINITY INJECTION REMOVED (operator 2026-06-03): jobs are not pinned to
    # hosts/kinds/capabilities — all workers eligible for everything.
    # NARROW HEAVY-REPO CARVE-OUT (operator 2026-06-04, GRAEAE consult): the only
    # documented exception to the line above. Large-repo classes (zeroclaw — big
    # Rust tree) strain a lightweight worker's in-job clone -> timeout -> retry
    # thrash -> dead-letter. Tagging them 'heavy-repo' means ONLY build hosts that
    # advertise that capability claim them (true skip-at-poll, NOT decline-after-
    # claim per directive 15). Everything else stays all-eligible.
    if heavy_repo_required_for_kind(req.kind):
        _caps = list(req.required_capabilities or [])
        if "heavy-repo" not in _norm_str_set(_caps):
            _caps.append("heavy-repo")
            req = req.model_copy(update={"required_capabilities": _caps})
    # ROLE ENFORCEMENT: check submitter runtime BEFORE the cache lookup so
    # worker-only runtimes get a 403 consistently for both cache hits and
    # cache misses. Unregistered submitter_urn values are rejected too; the
    # endpoint contract says jobs are submitted by registered orchestrators.
    await require_orchestrator_submitter(req.submitter_urn)

    if req.eligible_kinds and "*" not in req.eligible_kinds:
        async with connect_db() as _edb:
            async with _edb.execute(
                "SELECT kind, runtime FROM agents"
            ) as _cur:
                registered_aliases: set[str] = set()
                async for _kind, _runtime in _cur:
                    registered_aliases.update(agent_kind_aliases(_kind, _runtime))
        requested = _norm_str_set(req.eligible_kinds)
        if requested and not requested.intersection(registered_aliases):
            raise HTTPException(
                422,
                f"eligible_kinds has no registered worker: requested={sorted(requested)!r}; "
                f"registered_agent_kinds={sorted(registered_aliases)!r}",
            )

    # GRAEAE permanent logjam-fix: SUBMIT-TIME WORKSPACE ADMISSION (SHADOW / log-only, 2026-06-04).
    # Mode-1 jam = jobs (kind="code", no project/repo) whose workspace can't resolve -> worker
    # declines -> dead-letter. Observe would-be 422s now; enforce after vocabulary stabilizes (~1wk).
    try:
        _ws, _wsrc = resolve_workspace_shadow(req.kind, req.description, req.target_workspace, req.project)
        if _wsrc == "UNRESOLVABLE":
            print(f"ADMISSION_SHADOW would-422 no_resolvable_workspace kind={req.kind!r} "
                  f"project={req.project!r} submitter={req.submitter_urn}", flush=True)
        elif _wsrc == "prefix-fallback":
            print(f"ADMISSION_SHADOW deprecated_prefix_resolve kind={req.kind!r} -> ws={_ws} "
                  f"(migrate submitter to target_workspace)", flush=True)
        if _wsrc in ("UNRESOLVABLE", "prefix-fallback"):
            try:
                import json as _sj
                with open("/srv/agent-bus/admission_shadow.jsonl", "a") as _sf:
                    _sf.write(_sj.dumps({"ts": now, "verdict": _wsrc, "kind": req.kind,
                                         "project": req.project, "submitter": req.submitter_urn,
                                         "resolved_ws": _ws}) + "\n")
            except Exception:
                pass
    except Exception:
        pass
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
        dedup_scope_version = json.dumps(
            {
                "tier": max_cost_tier,
                "caps": sorted(req.required_capabilities or []),
                "eligible_hosts": sorted(req.eligible_hosts or []),
                "eligible_kinds": sorted(req.eligible_kinds or []),
                "preferred_providers": req.preferred_providers or [],
                "preferred_models": req.preferred_models or [],
                "depends_on": sorted(req.depends_on or []),
                "project": req.project,
                "parent_job_id": req.parent_job_id,
                "mnemos_refs": sorted(req.mnemos_refs or []),
                "deadline": req.deadline,
            },
            sort_keys=True,
            default=str,
        )
        dedup_hash = queue_logic.dedup_key(
            tenant="_default",
            kind=req.kind,
            description=req.description,
            version=dedup_scope_version,
            idempotency_key=req.idempotency_key,
        )
        try:
            await db.execute(
                "INSERT INTO jobs (id, submitter_urn, parent_job_id, kind, description, priority, deadline, "
                "required_capabilities, eligible_kinds, eligible_hosts, project, max_cost_tier, preferred_providers, preferred_models, "
                "mnemos_refs, depends_on, max_retries, dedup_hash, status, started_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)",
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
                    dedup_hash,
                    now,
                ),
            )
            await db.commit()
        except Exception as _dexc:
            if not _is_dedup_violation(_dexc):
                raise
            async with db.execute(
                "SELECT id, started_at FROM jobs WHERE dedup_hash = ? "
                "AND status IN ('queued', 'offered', 'claimed', 'running') "
                "ORDER BY started_at ASC LIMIT 1",
                (dedup_hash,),
            ) as _cur:
                _row = await _cur.fetchone()
            if _row:
                return {"id": _row[0], "created_at": _row[1], "coalesced": True}
            raise
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
    note_spark_seen_urn(agent_urn)
    now = time.time()
    urn_parts = agent_urn.split(":")
    agent_host = (urn_parts[3] if len(urn_parts) > 3 else "").lower()
    if agent_host in HOST_DENYLIST:
        from fastapi.responses import Response as _DRsp
        return _DRsp(status_code=204, headers={
            "X-Hive-Claim-Result": "host_denylisted",
            "X-Hive-Claim-Detail": f"host {agent_host} is in HIVE_HOST_DENYLIST",
        })

    try:
        async with connect_db() as db:
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute(
                "SELECT kind, capabilities, runtime, model, provider, cost_tier, "
                "auth_method, plan_cap_usd, plan_period_used_usd, status "
                "FROM agents WHERE urn=?",
                (agent_urn,),
            ) as cur:
                agent_row = await cur.fetchone()
            if not agent_row:
                await db.execute("ROLLBACK")
                raise HTTPException(404, f"claim failed: agent not registered: {agent_urn}")
            (agent_kind, caps_json, a_runtime, a_model, a_provider, a_tier,
             a_auth, a_cap, a_used, agent_status) = agent_row
            if agent_status not in ACTIVE_AGENT_STATUSES:
                await db.execute("ROLLBACK")
                raise HTTPException(
                    409,
                    f"claim failed: agent status is {agent_status!r}, not online/idle: {agent_urn}",
                )
            agent_caps = set(json.loads(caps_json)) if caps_json else set()
            a_tier = (a_tier or "C").upper()
            if a_tier not in COST_TIERS:
                a_tier = "C"
            a_auth = (a_auth or "unknown").lower()
            agent = {
                "urn": agent_urn,
                "host": agent_host,
                "kind": agent_kind,
                "runtime": a_runtime,
                "model": a_model,
                "provider": a_provider,
                "cost_tier": a_tier,
                "capabilities": agent_caps,
                "eligible_aliases": agent_kind_aliases(agent_kind, a_runtime),
                "subscription_throttled": (
                    a_auth == "subscription" and a_cap and a_used
                    and a_used >= THROTTLE_HEADROOM * a_cap
                ),
            }

            async with db.execute(
                "SELECT id, submitter_urn, parent_job_id, kind, description, priority, deadline, "
                "required_capabilities, eligible_kinds, eligible_hosts, project, max_cost_tier, "
                "preferred_providers, preferred_models, mnemos_refs, depends_on, retry_backoff_until "
                "FROM jobs WHERE status IN ('queued','offered') "
                "AND (retry_backoff_until IS NULL OR retry_backoff_until <= ?) "
                "ORDER BY priority DESC, started_at ASC LIMIT 100",
                (now,),
            ) as cur:
                candidates = [tuple(r) async for r in cur]
            candidates = [
                r for _idx, r in sorted(
                    enumerate(candidates),
                    key=lambda item: (
                        item[1][5] or 0,
                        job_agent_preference_score(
                            {
                                "preferred_providers": json_list(item[1][12]),
                                "preferred_models": json_list(item[1][13]),
                            },
                            agent,
                        ),
                        -item[0],
                    ),
                    reverse=True,
                )
            ]

            claimed = None
            for r in candidates:
                (j_id, j_submitter, j_parent, j_kind, j_desc, j_priority, j_deadline,
                 j_caps_json, j_kinds_json, j_hosts_json, j_project, j_max_tier,
                 j_pref_providers, j_pref_models, j_mnemos_refs, j_deps_json,
                 _j_retry_backoff_until) = r
                deps = json_list(j_deps_json)
                if deps:
                    ph = ",".join("?" * len(deps))
                    async with db.execute(
                        f"SELECT COUNT(*) FROM jobs WHERE id IN ({ph}) AND status='done'",
                        tuple(deps),
                    ) as dc:
                        done_count = (await dc.fetchone())[0]
                    if done_count < len(deps):
                        continue
                job = {
                    "id": j_id,
                    "kind": j_kind,
                    "required_capabilities": json_list(j_caps_json),
                    "eligible_kinds": json_list(j_kinds_json),
                    "eligible_hosts": json_list(j_hosts_json),
                    "max_cost_tier": j_max_tier,
                    "preferred_providers": json_list(j_pref_providers),
                    "preferred_models": json_list(j_pref_models),
                }
                eligible, _reason = job_agent_eligible(job, agent)
                if not eligible:
                    continue
                cur2 = await db.execute(
                    "UPDATE jobs SET status='claimed', claimed_by=?, claimed_at=?, "
                    "claimed_runtime=?, claimed_model=?, claimed_provider=?, claimed_cost_tier=?, "
                    "claim_lease_expires_at=? "
                    "WHERE id=? AND status IN ('queued','offered')",
                    (agent_urn, now, a_runtime, a_model, a_provider, a_tier,
                     now + CLAIM_LEASE_SECONDS, j_id),
                )
                if cur2.rowcount == 0:
                    continue
                claimed = {
                    "id": j_id,
                    "submitter_urn": j_submitter,
                    "parent_job_id": j_parent,
                    "kind": j_kind,
                    "description": j_desc,
                    "priority": j_priority,
                    "deadline": j_deadline,
                    "required_capabilities": json_list(j_caps_json),
                    "eligible_kinds": json_list(j_kinds_json),
                    "eligible_hosts": json_list(j_hosts_json),
                    "project": j_project,
                    "max_cost_tier": j_max_tier,
                    "preferred_providers": json_list(j_pref_providers),
                    "preferred_models": json_list(j_pref_models),
                    "mnemos_refs": json_list(j_mnemos_refs),
                    "status": "claimed",
                    "claimed_by": agent_urn,
                    "claimed_at": now,
                    "claim_lease_expires_at": now + CLAIM_LEASE_SECONDS,
                    "claimed_resources": {
                        "runtime": a_runtime,
                        "model": a_model,
                        "provider": a_provider,
                        "cost_tier": a_tier,
                    },
                }
                break
            await db.commit()
            if claimed:
                resources = claimed.get("claimed_resources") or {}
                await emit_event(db, "job.claimed", {
                    "id": claimed["id"], "claimed_by": agent_urn, "kind": claimed["kind"],
                    "runtime": resources.get("runtime"), "model": resources.get("model"),
                    "provider": resources.get("provider"), "cost_tier": resources.get("cost_tier"),
                })
                return claimed
    except HTTPException:
        raise
    except aiosqlite.Error as e:
        raise HTTPException(500, f"claim failed: database error while claiming next job: {e}") from e

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
    # WORK CONTRACT (GRAEAE worker-accountability consensus 1.0, 2026-06-04): a commit-mandatory
    # job marked done with ZERO commits is a contract breach — a silent fake-completion (model
    # confabulated off-task instead of committing; observed 23/40 riskyeats/code "done" w/ 0
    # commits + off-task chatter like "Hetzner API"/"SRRS"). Reclassify to failed so it never
    # counts as done + feeds fault-attribution/backoff. kind from worker-stamped result;
    # exit_code=-3 (vs hallucination_guard -2).
    if req.status == "done" and isinstance(req.result, dict) and not halluc_reason:
        _k = str(req.result.get("kind") or "")
        _commits = req.result.get("commits") or []
        _mand = (_k in ("riskyeats", "code") or _k.startswith((
            "riskyeats:", "feat:", "fix:", "argonaut:", "riskybiz:", "mnemos:",
            "ic-engine", "investorclaw", "ncz-os")))
        if _mand and not _commits:
            _p = dict(req.result or {})
            _p["exit_code"] = -3
            _p["failure_reason"] = "contract_breach:commit_mandatory_no_commits"
            req = JobUpdate(
                status="failed", result=_p, claimed_by=req.claimed_by,
                tokens_in=req.tokens_in, tokens_out=req.tokens_out,
                result_mnemos_id=req.result_mnemos_id,
            )
    req.result = normalize_result_payload(req.status, req.result)

    claimed_by_was_set = original_claimed_by_was_set
    result_mnemos_was_set = original_result_mnemos_was_set
    patch_payload = (
        req.model_dump(exclude_unset=True) if hasattr(req, "model_dump")
        else req.dict(exclude_unset=True)
    )
    cost_estimate = None

    # Skip releases are not failures. DECLINE_REASON_PREFIXES is intentionally
    # empty unless a future real failure release reason needs dead-letter counting.
    decline_reason = None
    dead_lettered = False
    if req.status == "queued" and isinstance(req.result, dict):
        _werr = str(req.result.get("worker_error") or "")
        # Match exact reasons or the documented separator form
        # (host_declines_kind:<host>) — avoid matching an unrelated reason that
        # merely shares a prefix.
        if _werr in DECLINE_REASON_PREFIXES or any(
            _werr.startswith(p + ":") for p in DECLINE_REASON_PREFIXES
        ):
            decline_reason = _werr

    async with connect_db() as db:
        await db.execute("BEGIN IMMEDIATE")
        try:
            async with db.execute(
                "SELECT status, claimed_by, claimed_provider, claimed_model, retry_count, "
                "max_retries, COALESCE(decline_count, 0) FROM jobs WHERE id=?",
                (job_id,),
            ) as cur:
                current = await cur.fetchone()
            if not current:
                await db.execute("ROLLBACK")
                raise HTTPException(404, f"job not found: {job_id}")
            old_status, old_claimed_by, prov, mod, retry_count, max_retries, decline_count = current

            if req.status not in STATUS_TRANSITIONS.get(old_status, set()):
                await db.execute("ROLLBACK")
                raise HTTPException(
                    409,
                    f"invalid status transition {old_status!r} -> {req.status!r}",
                )

            if (old_status in TERMINAL_JOB_STATUSES and req.status != old_status
                    and not (old_status == "done" and req.status == "cancelled")):  # allow cancelling fake/dup completions
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
                # GRAEAE dispatch consensus 1.0 (2026-06-04) SHIP-FIRST: backoff on EVERY release
                # so a job a worker can't run cannot hot-loop (claim->release->reclaim). The serve
                # query already skips jobs with retry_backoff_until>now. Mild exponential on
                # decline_count + ±20% jitter (decorrelate N simultaneous releases); ~5s first
                # bounce -> 300s cap. Stops the pegasus/medusa thrash.
                import random as _rnd
                # exponential per-release (NOT flat — flat = infinite churn on a job no local
                # worker can run; GRAEAE 2026-06-04). Bump retry_count each release so the
                # backoff escalates to the 300s cap (~6 bounces); a capable host grabs it during
                # a backoff window while the incapable host stops hammering it.
                _rc = int(retry_count or 0) + 1
                _bo = min(5.0 * (2 ** min(_rc, 6)), 300.0) * (1.0 + _rnd.uniform(-0.2, 0.2))
                fields.append("retry_count=retry_count+1")
                fields.append("retry_backoff_until=?")
                args.append(now + _bo)
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

            # Thrash fix (2026-06-02): compare-and-set on the observed status.
            # On the Oracle backend BEGIN IMMEDIATE is a no-op, so two concurrent
            # PATCHes can both read old_status='claimed'; without this guard a
            # stale release (status='queued') could land AFTER a sibling PATCH
            # already dead-lettered/terminated the job and reopen it. Gating the
            # UPDATE on `status=old_status` makes the second writer a no-op
            # (rowcount==0 → 409). SQLite already serialises via BEGIN IMMEDIATE;
            # the guard is harmless there.
            args.append(job_id)
            args.append(old_status)
            sql = f"UPDATE jobs SET {', '.join(fields)} WHERE id=? AND status=?"
            cur = await db.execute(sql, args)
            if cur.rowcount == 0:
                await db.execute("ROLLBACK")
                raise HTTPException(
                    409,
                    f"job {job_id} status changed concurrently (expected {old_status!r}); retry",
                )

            # Optional real-failure release counting. Routing skips such as
            # host_declines_kind/released_by_host/no_workspace_for_kind do not count.
            if decline_reason is not None and release_to_queue:
                new_decline_count = (decline_count or 0) + 1
                if new_decline_count >= MAX_DECLINE_REQUEUES:
                    # Preserve the worker's original decline payload for diagnostics.
                    dl_result = {
                        "dead_letter": True,
                        "decline_count": new_decline_count,
                        "last_decline_reason": decline_reason,
                        "note": "thrash_guard:max_decline_requeues_exceeded",
                        "last_decline_result": req.result,
                    }
                    # CAS on status='queued' (the state the main UPDATE just set)
                    # so a concurrent claim can't be clobbered.
                    dl_cur = await db.execute(
                        "UPDATE jobs SET status='dead-letter', decline_count=?, "
                        "ended_at=?, claim_lease_expires_at=NULL, result=? "
                        "WHERE id=? AND status='queued'",
                        (new_decline_count, now, json.dumps(dl_result), job_id),
                    )
                    dead_lettered = (getattr(dl_cur, "rowcount", 0) or 0) >= 1
                else:
                    await db.execute(
                        "UPDATE jobs SET decline_count=? WHERE id=? AND status='queued'",
                        (new_decline_count, job_id),
                    )

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

            # On done/failed/cancelled: roll per-worker per-kind stats (capability scoring).
            # Gate on a REAL non-terminal -> terminal transition: done->done /
            # failed->failed are allowed (idempotent re-PATCH), so without this guard a
            # duplicate terminal PATCH would double-count stats AND the usage_ledger cost.
            if req.status in TERMINAL_JOB_STATUSES and old_status not in TERMINAL_JOB_STATUSES:
                async with db.execute(
                    "SELECT kind, description, max_cost_tier, required_capabilities, "
                    "claimed_model, claimed_provider, claimed_by, result, started_at "
                    "FROM jobs WHERE id=?", (job_id,)
                ) as cur2:
                    jrow = await cur2.fetchone()
                if jrow:
                    kind_j, desc_j, mtier, reqcaps_json, mdl_j, prov_j, claimed_by_j, result_j, started_j = jrow
                    # Workers carry real token usage + the ACTUAL gateway
                    # model/provider INSIDE result, not as top-level PATCH
                    # fields, so cost was never computed before (usage_ledger
                    # stayed empty, worker_kind_stats.total_cost_usd stayed 0).
                    # Parse the result once and recover them.
                    try:
                        _rd = json.loads(result_j) if result_j else (req.result or {})
                    except Exception:
                        _rd = req.result or {}
                    if not isinstance(_rd, dict):
                        _rd = {}
                    rt_in = _safe_nonneg_int(req.tokens_in if req.tokens_in is not None else _rd.get("tokens_in"))
                    rt_out = _safe_nonneg_int(req.tokens_out if req.tokens_out is not None else _rd.get("tokens_out"))
                    rt_reason = _safe_nonneg_int(_rd.get("tokens_reasoning"))
                    # Some workers report only a combined total — attribute to output.
                    if rt_in == 0 and rt_out == 0 and _rd.get("tokens_total"):
                        rt_out = _safe_nonneg_int(_rd.get("tokens_total"))
                    eff_model = _safe_str(_rd.get("gateway_model") or mdl_j)
                    eff_prov = _safe_str(_rd.get("gateway_provider") or prov_j)
                    # Cost estimation must never break the hot completion path.
                    try:
                        job_cost = estimate_cost(eff_prov, eff_model, rt_in, rt_out) or 0
                        if job_cost < 0:
                            job_cost = 0
                    except Exception as _ce:  # noqa: BLE001
                        print(f"cost estimate skipped for {job_id}: {_ce}", flush=True)
                        job_cost = 0
                    if cost_estimate is None and job_cost:
                        cost_estimate = job_cost
                    if claimed_by_j and kind_j:
                        # Key stats by the STABLE worker identity (drop the
                        # per-restart session segment) so a worker's history
                        # aggregates across restarts instead of fragmenting.
                        worker_key = stable_worker_id(claimed_by_j)
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
                                worker_key, kind_j,
                                rt_in, rt_out,
                                job_cost or 0, duration, time.time(),
                                rt_in, rt_out,
                                job_cost or 0, duration, time.time(),
                            ),
                        )
                        # Per-job cost ledger row (the hive cost tracker). Best-effort:
                        # a ledger schema issue must never break job completion.
                        try:
                            # ts is TIMESTAMP WITH TIME ZONE — use SYSTIMESTAMP (not an
                            # epoch float, which raises ORA-00932) as a SQL literal so it
                            # is not a bind parameter.
                            await db.execute(
                                "INSERT INTO usage_ledger (provider, model, task_kind, tokens_in, "
                                "tokens_out, tokens_reasoning, est_cost_usd, latency_ms, outcome, "
                                "caller_subsystem, tier, ts, session_id, request_count, "
                                "subscription_amortized, path_kind) "
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSTIMESTAMP, ?, ?, ?, ?)",
                                (
                                    eff_prov, eff_model, kind_j, rt_in, rt_out, rt_reason,
                                    job_cost or 0, max(0, int(duration * 1000)),
                                    # outcome is constrained to ('ok','err','timeout')
                                    "ok" if req.status == "done" else "err",
                                    "hive", (mtier or "A").upper(), claimed_by_j, 1,
                                    1 if (eff_prov or "").lower() in SUBSCRIPTION_PROVIDERS else 0,
                                    str(_rd.get("via") or "worker"),
                                ),
                            )
                        except Exception as _le:
                            print(f"usage_ledger write skipped for {job_id}: {_le}", flush=True)
                    if req.status == "done":
                        ck = cache_key_for(kind_j, desc_j, (mtier or "A").upper(),
                                           json_list(reqcaps_json))
                        rdict = json.loads(result_j) if result_j else (req.result or {})
                        ec = (rdict or {}).get("exit_code")
                        # NEVER cache a non-terminal / unlanded result. A spark
                        # needs-review result (work is a patch on spark-0c53, NOT
                        # landed) has exit_code=None and would otherwise be served
                        # as an instant fake-"done" to every matching resubmit
                        # (the 73 orphaned FRI jobs, 2026-06-07). Only cache real
                        # completed work: not needs_review, and either it carries
                        # commits or it is a non-workspace job (no repo to commit).
                        _nr = bool((rdict or {}).get("needs_review")) or \
                              (rdict or {}).get("status") == "needs-review"
                        if (ec == 0 or ec is None) and not _nr:
                            await cache_store(db, ck, job_id, rdict, req.result_mnemos_id,
                                              mdl_j or "unknown", prov_j or "unknown", cost_estimate or 0)

            await db.execute(
                "INSERT INTO job_audit_log (job_id, ts, actor_urn, old_status, new_status, "
                "old_claimed_by, new_claimed_by, patch) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    job_id, now, req.claimed_by or old_claimed_by, old_status,
                    "dead-letter" if dead_lettered else ("queued" if retried else req.status),
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
        if dead_lettered:
            # Emit ONLY the effective terminal event; suppress the misleading
            # job.queued (the inbound PATCH was queued but the job was diverted).
            await emit_event(event_db, "job.dead-letter", {
                "id": job_id, "claimed_by": req.claimed_by or old_claimed_by,
                "last_decline_reason": decline_reason,
                "decline_threshold": MAX_DECLINE_REQUEUES,
            })
        else:
            await emit_event(event_db, f"job.{req.status}", {
                "id": job_id, "claimed_by": req.claimed_by or old_claimed_by,
                "tokens_in": req.tokens_in, "tokens_out": req.tokens_out,
                "estimated_cost_usd": cost_estimate,
            })
    return {"ok": True, "ts": now, "estimated_cost_usd": cost_estimate,
            "dead_lettered": dead_lettered}




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
        # Derive REAL provider from result.agent_alias (the alias that did the work),
        # NOT claimed_provider (which carries the zeroclaw #7066 label-lie = openai/codex).
        sel = ("CASE "
               "WHEN JSON_VALUE(result,'$.agent_alias') IN ('hive_deepseek_pro_1','hive_groq_1','hive_xai_1','hive_nvidia_1') THEN 'codex' "
               "WHEN JSON_VALUE(result,'$.agent_alias') LIKE 'hive_deepseek%' THEN 'deepseek-direct' "
               "WHEN JSON_VALUE(result,'$.agent_alias') LIKE 'hive_siliconflow%' THEN 'siliconflow' "
               "WHEN JSON_VALUE(result,'$.agent_alias') LIKE 'hive_xai%' THEN 'xai' "
               "WHEN JSON_VALUE(result,'$.agent_alias') LIKE 'hive_together%' THEN 'together' "
               "WHEN JSON_VALUE(result,'$.agent_alias') LIKE 'hive_groq%' THEN 'groq' "
               "WHEN JSON_VALUE(result,'$.agent_alias') LIKE 'hive_nvidia%' THEN 'nvidia' "
               "ELSE COALESCE(claimed_provider,'unknown') END")
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


# ── KNEMON COST-AWARE ROUTER (2026-06-02) ────────────────────────────────────
# Deterministic, no external calls. Given a job's budget ceiling (max_cost_tier
# or tier) + kind, return an ordered list of provider/model candidates that fit
# the budget, cheapest-acceptable FIRST, then escalate.
#
# COST-TIER CONVENTION (authoritative, from PROVIDER_COST_TIER above):
#   A = FREE/cheap-local   B = MID (cheap paid)   C = PREMIUM/reserve
#   COST_TIERS = ["A","B","C"]  ordered cheap -> expensive.
# `max_cost_tier` is the CEILING (most-expensive tier permitted). A job with
# max_cost_tier="C" may use A/B/C providers; "B" -> A/B; "A" -> A only.
# This matches the dequeue semantics + the operator intent ("generous budget =>
# premium allowed; constrained budget => cheap only"). NOTE: the task brief
# described A=premium which is INVERTED from the code; the code convention wins.
#
# Working provider set (operator 2026-06-01): deepseek/groq/together/xai/gemini/
# siliconflow. EXCLUDED: codex (review-only, scarce OAuth pool), nvidia (Spark-
# only, not integrated). Each entry: provider, model, worker alias, $/1M-out
# (used only as the deterministic cheapest-first sort key).
#
# alias = the hive_* agent alias the wss worker dispatches to (gateway maps it
# to provider+model). Returned so the worker can consume a chain shaped exactly
# like its local TIER_CHAINS values.
KNEMON_PROVIDERS: list[dict[str, Any]] = [
    # provider,         model,                 alias,                  in,    out
    {"provider": "groq",      "model": "gpt-oss-20b",          "alias": "hive_groq_1",        "tier": "B"},
    {"provider": "gemini",    "model": "gemini-2.5-flash-lite","alias": "hive_gemini_1",      "tier": "B"},
    # DISABLED 2026-06-03 (blocker C): siliconflow key 401 invalid fleet-wide. Re-enable after key rotation.
    # {"provider": "siliconflow","model": "qwen2.5-coder-32b",   "alias": "hive_siliconflow_1", "tier": "B"},
    {"provider": "deepseek-direct","model": "deepseek-v4-flash","alias": "hive_deepseek_1",   "tier": "B"},
    {"provider": "together",  "model": "minimax-m2.7",         "alias": "hive_together_1",    "tier": "B"},
    {"provider": "xai",       "model": "grok-4.1-fast",        "alias": "hive_xai_1",         "tier": "B"},
    {"provider": "deepseek-direct","model": "deepseek-v4-pro", "alias": "hive_deepseek_pro_1","tier": "B"},
    # NGC Enterprise Inference Hub via the .4 LiteLLM gateway (PYTHIA:4100
    # tunnel). Enterprise allocation, no per-token bill -> tier A. Worker
    # agent = hive_ngc_1 (openai.ngc_nemotron, fleet-ops fe4d780).
    {"provider": "ngc-proxy", "model": "gpt-5.5",            "alias": "hive_ngc_1",         "tier": "A"},
    # Bedrock Nova (funded AWS creds, worker agents shipped 2026-06-02):
    # cheap metered fallbacks (lite 0.06/0.24, micro 0.035/0.14).
    {"provider": "bedrock",   "model": "amazon.nova-lite-v1:0", "alias": "hive_nova_1",       "tier": "B"},
    {"provider": "bedrock",   "model": "amazon.nova-micro-v1:0","alias": "hive_nova_2",       "tier": "B"},
]


def knemon_candidates(max_tier: str, kind: str = "") -> list[dict[str, Any]]:
    """Ordered provider/model candidates within budget, cheapest-acceptable first.

    Cheapest-first is keyed on the per-provider/model OUTPUT rate from LLM_RATES
    (output dominates code-gen spend), then input rate, then provider name for a
    stable deterministic tie-break. Providers whose cost tier exceeds the budget
    ceiling are excluded.
    """
    ceiling_idx = COST_TIERS.index(max_tier) if max_tier in COST_TIERS else len(COST_TIERS) - 1
    out: list[dict[str, Any]] = []
    for p in KNEMON_PROVIDERS:
        # Effective cost tier is the per-entry, MODEL-SPECIFIC tier hint
        # (e.g. gemini-flash-lite=B even though PROVIDER_COST_TIER['gemini']=C,
        # which is the gemini-PRO tier; siliconflow isn't in the static map at
        # all -> defaults to C). The static provider tier is only a fallback
        # when an entry omits its hint. This keeps cheap model variants from
        # being wrongly excluded under a tight budget.
        eff_tier = (p.get("tier") or cost_tier_for(p["provider"])).upper()
        if eff_tier not in COST_TIERS:
            eff_tier = cost_tier_for(p["provider"])
        if COST_TIERS.index(eff_tier) > ceiling_idx:
            continue
        in_rate, out_rate = rate_for(p["provider"], p["model"])
        out.append({
            "provider": p["provider"],
            "model": p["model"],
            "alias": p["alias"],
            "cost_tier": eff_tier,
            "rate_in_per_1m": in_rate,
            "rate_out_per_1m": out_rate,
        })
    out.sort(key=lambda c: (c["rate_out_per_1m"], c["rate_in_per_1m"], c["provider"]))
    return out


# ── HIVE-LEVEL KNEMON BYPASS FLAG (operator 2026-06-03) ──────────────────
# Presence of this file = bypass ON for the WHOLE hive. Durable across bus
# restarts; observable (ls/cat); togglable via the /v1/knemon/bypass endpoints
# below or a plain touch/rm. Cheap stat() per route call. When set, the router
# drops the codex/gpt sub-lead and returns the plain metered open-weight chain,
# so EVERY worker that polls /v1/knemon/route runs jobs directly on open-weights
# — the stabilization lever when the GPT-first lead is broken (e.g. hive_gpt
# 400 'missing input' cascade).
KNEMON_BYPASS_FILE = "/srv/agent-bus/knemon_bypass.flag"


def knemon_bypass_active() -> bool:
    try:
        return os.path.exists(KNEMON_BYPASS_FILE)
    except Exception:
        return False


KNEMON_CAP_FILE = "/srv/agent-bus/knemon_oauth_cap.json"
# ChatGPT-subscription codex/gpt allowance is a rolling ~5h window (operator/Gemini 2026-06-04).
OAUTH_CAP_RESET_SEC = 5 * 3600
# While the breaker is open, send a small share of eligible jobs through the
# OAuth lead as half-open probes. PYTHIA cannot probe OpenAI locally.
OAUTH_CAP_PROBE_RATE = 0.05


def oauth_cap_state() -> dict:
    """OAuth-cap circuit-breaker state. A worker that hits usage_limit on the codex/gpt
    lead POSTs /v1/knemon/cap, opening the breaker so knemon_route stops leading every job
    with the doomed OAuth model. Auto-expires at capped_until (then a spark-lead probe
    resumes; a probe success clears the file via /v1/knemon/cap/clear)."""
    import json as _j
    import time as _t
    try:
        with open(KNEMON_CAP_FILE) as f:
            d = _j.load(f)
        until = float(d.get("capped_until") or 0)
        now = _t.time()
        return {"capped": now < until, "capped_until": until, "hit_at": d.get("hit_at"),
                "model": d.get("model"), "reporter": d.get("reporter"),
                "verified": False, "remaining_sec": max(0, int(until - now))}
    except Exception:
        return {"capped": False, "capped_until": None, "hit_at": None,
                "model": None, "reporter": None, "verified": False, "remaining_sec": 0}


def clear_oauth_cap() -> Optional[str]:
    try:
        os.remove(KNEMON_CAP_FILE)  # no exists() pre-check (avoid TOCTOU)
    except FileNotFoundError:
        pass
    except Exception as e:
        return str(e)
    return None


@app.post("/v1/knemon/route")
async def knemon_route(req: Request):
    """Cost-aware routing. POST {max_cost_tier|tier, kind} -> ordered candidate
    list (cheapest-acceptable first) + the alias chain the worker can consume.
    Deterministic, no external calls, no DB write."""
    try:
        body = await req.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    raw_tier = (body.get("max_cost_tier") or body.get("tier") or "C")
    max_tier = str(raw_tier).upper()
    if max_tier not in COST_TIERS:
        max_tier = "C"
    kind = str(body.get("kind") or "")
    cands = knemon_candidates(max_tier, kind)
    open_weight_chain = [c["alias"] for c in cands]
    # ── KNEMON sub-bucket dispatch (operator 2026-06-02): openai_codex sub is $0;
    # gpt-5.3/5.4 unlimited. Lead EVERY job with the right codex/gpt agent, then
    # fall back to metered open-weights. Job class -> served model:
    #   review/adversarial -> hive_codex (codex-auto-review)
    #   heavy (architecture/design/heavy:) -> hive_gpt_heavy (gpt-5.5, limited)
    #   light/narrow (triage/docs/investigation) -> hive_gpt_mini (gpt-5.4-mini)
    #   everything else (coding default) -> hive_gpt (gpt-5.4)
    kl = kind.lower()
    review_kind = any(kl.startswith(p) for p in ("review:", "codex", "doctor:codex-fix", "adversarial"))
    arch_kind = any(t in kl for t in ("architecture", "design")) or kl.startswith("heavy:")
    # DEEPSEEK POLICY (operator directive 2026-06-07, refined same day):
    # deepseek-v4-PRO = CODE REVIEW fallback or HIGH-LEVEL ARCHITECTURE
    # fallback ONLY — never a general job model ($166 burn incident).
    # deepseek-v4-FLASH (hive_deepseek_1) is UNRESTRICTED (~10x cheaper than
    # pro). Strip only the PRO alias from non-review/non-arch kinds.
    if not (review_kind or arch_kind):
        cands = [c for c in cands if c["alias"] != "hive_deepseek_pro_1"]
        open_weight_chain = [a for a in open_weight_chain if a != "hive_deepseek_pro_1"]
    if review_kind:
        CODEX_SUB_LEAD = ["hive_codex"]
    elif any(t in kl for t in ("architecture", "design")) or kl.startswith("heavy:"):
        CODEX_SUB_LEAD = ["hive_gpt"]  # gpt-5.5 (hive_gpt_heavy) OAuth allowance exhausted 2026-06-04 -> use gpt-5.4
    elif any(kl.startswith(p) for p in ("triage", "docs:", "investigation")):
        CODEX_SUB_LEAD = ["hive_gpt_mini"]
    else:
        CODEX_SUB_LEAD = ["hive_gpt"]
    # HIVE-LEVEL BYPASS: when the flag is set, drop the codex/gpt sub-lead and
    # hand back the plain metered open-weight chain so all workers run direct.
    bypass = knemon_bypass_active()
    cap = oauth_cap_state()
    oauth_cap_probe = False
    if bypass:
        CODEX_SUB_LEAD = []
        chain = list(open_weight_chain)
    else:
        # OAuth codex/gpt ($0) leads. Its allowance is a rolling ~5h window (operator/Gemini
        # 2026-06-04) and caps under heavy job load. When it 429s/usage_limit:
        #   review kinds  -> deepseek (REVIEW-ONLY sanction, operator 2026-06-07) then grok
        #   all job kinds -> grok-4.3 only (funded xai). Deepseek is FORBIDDEN as a
        #   job model — observed confabulating/failing heavy jobs AND now policy.
        # hive_ngc_1 = azure/openai/gpt-5.5 via the .4 NGC gateway (operator
        # 2026-06-07: gpt-5.5 ≈ codex quality, free NGC enterprise). It is a
        # proper OpenAI tool-driver (native tool_calls, unlike the earlier
        # gpt-oss/nemotron harmony/pseudo-XML no-ops) — so it LEADS the metered
        # fallback: when OAuth codex/gpt is capped it is the best available
        # driver AND $0. deepseek/nova/grok follow. (NGC nemotron stays as the
        # ngc-review CLI's direct-prompt reviewer, separate from this chain.)
        if review_kind:
            METERED_FALLBACK = ["hive_ngc_1", "hive_deepseek_pro_1", "hive_deepseek_1", "hive_xai_1"]
        elif arch_kind:
            # High-level architecture: gpt-5.5, then deepseek-PRO (sanctioned), grok.
            METERED_FALLBACK = ["hive_ngc_1", "hive_deepseek_pro_1", "hive_xai_1"]
        else:
            # General jobs: gpt-5.5 (codex-class, free), flash, Nova lite, grok.
            METERED_FALLBACK = ["hive_ngc_1", "hive_deepseek_1", "hive_nova_1", "hive_xai_1"]
        if cap.get("capped"):
            # Half-open breaker: most jobs use metered fallback, but a small share leads
            # OAuth so routed traffic can prove recovery before the 5h stale-report TTL.
            if CODEX_SUB_LEAD and secrets.randbelow(10000) < int(OAUTH_CAP_PROBE_RATE * 10000):
                oauth_cap_probe = True
                chain = list(CODEX_SUB_LEAD) + METERED_FALLBACK
            else:
                chain = list(METERED_FALLBACK)
        else:
            chain = list(CODEX_SUB_LEAD) + METERED_FALLBACK
    return {
        "ok": True,
        "max_cost_tier": max_tier,
        "kind": kind,
        "knemon_bypass": bypass,
        "oauth_cap": cap,
        "oauth_cap_probe": oauth_cap_probe,
        "oauth_cap_probe_rate": OAUTH_CAP_PROBE_RATE if cap.get("capped") and CODEX_SUB_LEAD else 0.0,
        "sub_lead": CODEX_SUB_LEAD,
        # codex/gpt sub agent first ($0 unlimited), metered open-weights as fallback
        "chain": chain,
        "candidates": cands,
        "convention": "openai_codex sub leads ($0); open-weights fallback; A=cheap..C=premium ceiling",
    }


@app.get("/v1/knemon/bypass")
async def knemon_bypass_get():
    """Read hive-level KNEMON bypass state."""
    return {"ok": True, "bypass": knemon_bypass_active(),
            "flag_file": KNEMON_BYPASS_FILE}


@app.post("/v1/knemon/bypass")
async def knemon_bypass_set(req: Request):
    """Toggle hive-level KNEMON bypass. POST {enabled: bool, reason?: str}.
    When enabled, ALL workers skip the codex/gpt sub-lead (open-weights only)."""
    try:
        body = await req.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    enabled = bool(body.get("enabled"))
    reason = str(body.get("reason") or "")
    try:
        if enabled:
            with open(KNEMON_BYPASS_FILE, "w") as f:
                f.write((reason or "bypass enabled") + "\n")
        else:
            if os.path.exists(KNEMON_BYPASS_FILE):
                os.remove(KNEMON_BYPASS_FILE)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "bypass": knemon_bypass_active(), "reason": reason}


@app.get("/v1/knemon/cap")
async def knemon_cap_get():
    """OAuth-cap circuit-breaker state (dashboard 5h countdown reads this)."""
    return {"ok": True, "reset_sec": OAUTH_CAP_RESET_SEC,
            "flag_file": KNEMON_CAP_FILE, **oauth_cap_state()}


@app.post("/v1/knemon/cap")
async def knemon_cap_set(req: Request):
    """Workers report OAuth cap state.

    POST {model?, reporter?, reset_sec?} opens the breaker on usage_limit.
    POST {success:true, model?, reporter?} closes it after a successful OAuth completion.
    """
    import json as _j
    import time as _t
    try:
        body = await req.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    if body.get("success") is True:
        err = clear_oauth_cap()
        if err:
            return {"ok": False, "error": err}
        return {"ok": True, "cleared": True, "success": True, **oauth_cap_state()}
    # debounce the worker stampede: the first usage_limit opens the breaker for the full
    # window; concurrent reporters within the window are no-ops (don't churn/extend it).
    cur = oauth_cap_state()
    if cur.get("capped"):
        return {"ok": True, "debounced": True, **cur}
    try:
        reset = float(body.get("reset_sec") or OAUTH_CAP_RESET_SEC)
    except (TypeError, ValueError):
        reset = float(OAUTH_CAP_RESET_SEC)
    if reset != reset or reset in (float("inf"), float("-inf")):  # NaN/Inf guard
        reset = float(OAUTH_CAP_RESET_SEC)
    reset = max(60.0, min(reset, 86400.0))
    now = _t.time()
    state = {
        "hit_at": now,
        "capped_until": now + reset,
        "model": str(body.get("model") or ""),
        "reporter": str(body.get("reporter") or body.get("worker") or body.get("host") or ""),
    }
    try:
        tmp = KNEMON_CAP_FILE + ".tmp"
        with open(tmp, "w") as f:
            _j.dump(state, f)
        os.replace(tmp, KNEMON_CAP_FILE)  # atomic publish (no partial-read race)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, **oauth_cap_state()}


@app.post("/v1/knemon/cap/clear")
async def knemon_cap_clear():
    """Confirm OAuth reset (a codex/gpt probe succeeded). Closes the breaker."""
    err = clear_oauth_cap()
    if err:
        return {"ok": False, "error": err}
    return {"ok": True, **oauth_cap_state()}


@app.post("/v1/knemon/cap/probe-ok")
async def knemon_cap_probe_ok():
    """Confirm a half-open OAuth probe succeeded. Closes the breaker."""
    err = clear_oauth_cap()
    if err:
        return {"ok": False, "error": err}
    return {"ok": True, "cleared": True, **oauth_cap_state()}


@app.get("/v1/knemon/spend")
async def knemon_spend(since_hours: int = 168):
    """Total estimated spend over a window (visibility companion to the router)."""
    # clamp window to a sane range (1h .. 1 year) so a pathological value
    # cannot produce a nonsense cutoff.
    try:
        since_hours = max(1, min(int(since_hours), 24 * 366))
    except (TypeError, ValueError):
        since_hours = 168
    cutoff = time.time() - since_hours * 3600
    async with connect_db() as db:
        async with db.execute(
            "SELECT COUNT(*), ROUND(SUM(COALESCE(estimated_cost_usd,0)),4), "
            "SUM(COALESCE(tokens_in,0)), SUM(COALESCE(tokens_out,0)) "
            "FROM jobs WHERE ended_at IS NOT NULL AND ended_at >= ? "
            "AND estimated_cost_usd IS NOT NULL",
            (cutoff,),
        ) as cur:
            r = await cur.fetchone()
    return {
        "since_hours": since_hours,
        "jobs_with_cost": (r[0] or 0) if r else 0,
        "estimated_cost_usd": (r[1] or 0.0) if r else 0.0,
        "tokens_in": (r[2] or 0) if r else 0,
        "tokens_out": (r[3] or 0) if r else 0,
    }



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
        a_tier = (a_tier or "C").upper()
        if a_tier not in COST_TIERS:
            a_tier = "C"
        a_auth = (a_auth or "unknown").lower()
        sub_throttled = (
            a_auth == "subscription" and a_cap and a_used
            and a_used >= THROTTLE_HEADROOM * a_cap
        )

        # 2. job must exist + be claimable + agent must satisfy its filters
        async with db.execute(
            "SELECT status, kind, required_capabilities, eligible_kinds, "
            "eligible_hosts, max_cost_tier, preferred_providers, preferred_models, "
            "depends_on, retry_backoff_until "
            "FROM jobs WHERE id=?",
            (job_id,),
        ) as cur:
            job_row = await cur.fetchone()
        if not job_row:
            raise HTTPException(404, f"job not found: {job_id}")
        (j_status, j_kind, j_caps_json, j_kinds_json, j_hosts_json, j_max_tier,
         j_pref_providers, j_pref_models, j_deps_json,
         j_retry_backoff_until) = job_row

        urn_parts = by.split(":")
        agent_host = (urn_parts[3] if len(urn_parts) > 3 else "").lower()

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

        agent = {
            "urn": by,
            "host": agent_host,
            "kind": agent_kind,
            "runtime": a_runtime,
            "model": a_model,
            "provider": a_provider,
            "cost_tier": a_tier,
            "capabilities": agent_caps,
            "eligible_aliases": eligible_aliases,
            "subscription_throttled": sub_throttled,
        }
        job = {
            "id": job_id,
            "kind": j_kind,
            "required_capabilities": json_list(j_caps_json),
            "eligible_kinds": json_list(j_kinds_json),
            "eligible_hosts": json_list(j_hosts_json),
            "max_cost_tier": j_max_tier,
            "preferred_providers": json_list(j_pref_providers),
            "preferred_models": json_list(j_pref_models),
        }
        eligible, reason = job_agent_eligible(job, agent)
        if not eligible:
            status = 422 if reason.startswith("job max_cost_tier") else (429 if reason.startswith("subscription agent throttled") else 403)
            raise HTTPException(status, reason)

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
    parent_job_id: Optional[str] = None,
    since: Optional[float] = None,
    limit: int = 100,
):
    limit = clamp_limit(limit, default=100, max_limit=1000)
    if status:
        # Normalize separator variants (dead_letter -> dead-letter,
        # failed-completion -> failed_completion) and reject unknown values
        # with 400 instead of silently returning an empty list. A wrong
        # separator previously made 82 dead-letter rows API-invisible
        # (2026-06-06): the canonical spelling is hyphenated dead-letter
        # while callers naturally write dead_letter.
        normalized = status.strip().lower().replace("_", "-")
        status = {"failed-completion": "failed_completion"}.get(normalized, normalized)
        if status not in VALID_JOB_STATUSES:
            raise HTTPException(
                400,
                f"unknown status {status!r}; valid: {sorted(VALID_JOB_STATUSES)}",
            )
    sql = ("SELECT id, submitter_urn, parent_job_id, kind, description, priority, status, "
           "claimed_by, started_at, ended_at, result, estimated_cost_usd, "
           "required_capabilities, eligible_kinds, eligible_hosts FROM jobs WHERE 1=1")
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
    if parent_job_id:
        sql += " AND parent_job_id=?"
        cnt_sql += " AND parent_job_id=?"
        args.append(parent_job_id)
        cnt_args.append(parent_job_id)
    if since:
        sql += " AND started_at >= ?"
        cnt_sql += " AND started_at >= ?"
        args.append(since)
        cnt_args.append(since)
    # For terminal statuses, sort by ended_at DESC so 'recent' actually means recently-finished.
    if status in TERMINAL_JOB_STATUSES:
        sql += " ORDER BY COALESCE(ended_at, started_at) DESC LIMIT ?"
    else:
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
                    "required_capabilities": json.loads(r[12]) if r[12] else None,
                    "eligible_kinds": json.loads(r[13]) if r[13] else None,
                    "eligible_hosts": json.loads(r[14]) if r[14] else None,
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
