"""IBM Db2 Hive Mind repository."""

from __future__ import annotations

import asyncio
import datetime as _dt
import json as _json
import os
import re
from contextlib import asynccontextmanager
from decimal import Decimal as _Decimal
from typing import Any, AsyncIterator
from urllib.parse import unquote, urlparse

from .sqlite import SqliteHiveRepository


DB2_DDL = """
CREATE TABLE agents (
  urn VARCHAR(512) PRIMARY KEY,
  kind VARCHAR(64) NOT NULL,
  host VARCHAR(255) NOT NULL,
  session_id VARCHAR(128) NOT NULL,
  pid INTEGER,
  capabilities CLOB,
  version VARCHAR(128),
  started_at DOUBLE NOT NULL,
  last_heartbeat DOUBLE NOT NULL,
  status VARCHAR(32) NOT NULL,
  metadata CLOB,
  runtime VARCHAR(64),
  model VARCHAR(255),
  provider VARCHAR(255),
  autonomy_level VARCHAR(64),
  cost_tier VARCHAR(8),
  current_load CLOB,
  auth_method VARCHAR(64),
  plan_cap_usd DOUBLE,
  plan_period_used_usd DOUBLE DEFAULT 0,
  subscription_pools CLOB
);
CREATE INDEX idx_agents_status ON agents(status);
CREATE INDEX idx_agents_kind ON agents(kind);

CREATE TABLE jobs (
  id VARCHAR(64) PRIMARY KEY,
  submitter_urn VARCHAR(512) NOT NULL,
  parent_job_id VARCHAR(64),
  kind VARCHAR(255) NOT NULL,
  description CLOB,
  priority INTEGER NOT NULL DEFAULT 0,
  deadline DOUBLE,
  required_capabilities CLOB,
  eligible_kinds CLOB,
  eligible_hosts CLOB,
  project VARCHAR(255),
  status VARCHAR(32) NOT NULL,
  claimed_by VARCHAR(512),
  claimed_at DOUBLE,
  started_at DOUBLE NOT NULL,
  ended_at DOUBLE,
  result CLOB,
  required_autonomy VARCHAR(64),
  max_cost_tier VARCHAR(8),
  preferred_providers CLOB,
  preferred_models CLOB,
  claimed_runtime VARCHAR(64),
  claimed_model VARCHAR(255),
  claimed_provider VARCHAR(255),
  claimed_cost_tier VARCHAR(8),
  tokens_in INTEGER,
  tokens_out INTEGER,
  estimated_cost_usd DOUBLE,
  mnemos_refs CLOB,
  result_mnemos_id VARCHAR(128),
  required_resources CLOB,
  claimed_host_caps CLOB,
  tags CLOB,
  depends_on CLOB,
  retry_count INTEGER NOT NULL DEFAULT 0,
  max_retries INTEGER NOT NULL DEFAULT 2,
  retry_backoff_until DOUBLE,
  last_update_at DOUBLE,
  claim_lease_expires_at DOUBLE,
  decline_count INTEGER NOT NULL DEFAULT 0,
  dedup_hash VARCHAR(64)
);
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_submitter ON jobs(submitter_urn);
CREATE INDEX idx_jobs_claimed_by ON jobs(claimed_by);
CREATE INDEX idx_jobs_parent ON jobs(parent_job_id);
CREATE INDEX idx_jobs_queue ON jobs(status, priority DESC, started_at ASC);

CREATE TABLE events (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ts DOUBLE NOT NULL,
  kind VARCHAR(255) NOT NULL,
  payload CLOB NOT NULL,
  agent_urn VARCHAR(512)
);
CREATE INDEX idx_events_ts ON events(ts);
CREATE INDEX idx_events_kind ON events(kind);
CREATE INDEX idx_events_agent ON events(agent_urn);

CREATE TABLE messages (
  id VARCHAR(64) PRIMARY KEY,
  from_urn VARCHAR(512) NOT NULL,
  to_urn VARCHAR(512),
  in_reply_to VARCHAR(64),
  topic VARCHAR(255) NOT NULL,
  payload CLOB NOT NULL,
  ts DOUBLE NOT NULL
);
CREATE INDEX idx_messages_to ON messages(to_urn);
CREATE INDEX idx_messages_topic ON messages(topic);
CREATE INDEX idx_messages_ts ON messages(ts);

CREATE TABLE job_audit_log (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  job_id VARCHAR(64) NOT NULL,
  ts DOUBLE NOT NULL,
  actor_urn VARCHAR(512),
  old_status VARCHAR(32),
  new_status VARCHAR(32),
  old_claimed_by VARCHAR(512),
  new_claimed_by VARCHAR(512),
  patch CLOB NOT NULL
);
CREATE INDEX idx_job_audit_job_ts ON job_audit_log(job_id, ts);

CREATE TABLE hive_cache (
  cache_key VARCHAR(128) PRIMARY KEY,
  result_json CLOB NOT NULL,
  source_job_id VARCHAR(64),
  result_mnemos_id VARCHAR(128),
  hit_count INTEGER NOT NULL DEFAULT 0,
  cost_saved_usd DOUBLE NOT NULL DEFAULT 0,
  model VARCHAR(255),
  provider VARCHAR(255),
  cached_at DOUBLE NOT NULL,
  last_hit_at DOUBLE
);

CREATE TABLE worker_kind_stats (
  urn VARCHAR(512) NOT NULL,
  kind VARCHAR(255) NOT NULL,
  success_count INTEGER NOT NULL DEFAULT 0,
  fail_count INTEGER NOT NULL DEFAULT 0,
  cancelled_count INTEGER NOT NULL DEFAULT 0,
  total_tokens_in INTEGER NOT NULL DEFAULT 0,
  total_tokens_out INTEGER NOT NULL DEFAULT 0,
  total_cost_usd DOUBLE NOT NULL DEFAULT 0,
  total_duration_sec DOUBLE NOT NULL DEFAULT 0,
  last_run DOUBLE,
  PRIMARY KEY (urn, kind)
);

CREATE TABLE scheduled_jobs (
  id VARCHAR(64) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  created_by_urn VARCHAR(512) NOT NULL,
  interval_seconds INTEGER NOT NULL,
  job_template CLOB NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  last_fired_at DOUBLE,
  next_fire_at DOUBLE NOT NULL,
  fire_count INTEGER NOT NULL DEFAULT 0,
  created_at DOUBLE NOT NULL
);
"""


def _parse_db2_dsn(dsn: str) -> dict[str, str]:
    if "://" not in dsn:
        return {"DSN": dsn}
    parsed = urlparse(dsn)
    return {
        "DATABASE": (parsed.path or "/").lstrip("/") or "HIVE",
        "HOSTNAME": parsed.hostname or "localhost",
        "PORT": str(parsed.port or 50000),
        "PROTOCOL": "TCPIP",
        "UID": unquote(parsed.username or "db2inst1"),
        "PWD": unquote(parsed.password or ""),
    }


def _dsn_string(parts: dict[str, str]) -> str:
    for key, value in parts.items():
        if key != "PORT" and (";" in value or "=" in value):
            raise ValueError(f"DB2 DSN attribute {key} contains a forbidden character")
    return ";".join(f"{key}={value}" for key, value in parts.items()) + ";"


def _split_script(script: str) -> list[str]:
    return [stmt.strip() for stmt in script.split(";") if stmt.strip()]


def _is_already_exists(exc: BaseException) -> bool:
    text = str(exc).upper()
    return "SQLSTATE=42710" in text or "SQLSTATE 42710" in text or "ALREADY EXISTS" in text


def _adapt_sql(sql: str, params: Any = None) -> tuple[str | None, tuple[Any, ...]]:
    stripped = sql.strip()
    upper = stripped.upper()
    values = tuple(params or ())
    if not stripped:
        return None, values
    if upper in {"BEGIN", "BEGIN IMMEDIATE"}:
        return None, values
    if upper.startswith("PRAGMA TABLE_INFO"):
        table = re.search(r"\((\w+)\)", stripped, flags=re.IGNORECASE)
        name = table.group(1).upper() if table else ""
        return (
            "SELECT colno, LOWER(colname), typename, NULL, NULL, NULL "
            "FROM syscat.columns WHERE tabschema = CURRENT SCHEMA AND tabname = ? "
            "ORDER BY colno",
            (name,),
        )

    cache_upsert = re.search(r"INSERT INTO hive_cache .* ON CONFLICT\(cache_key\) DO UPDATE SET", " ".join(stripped.split()), re.IGNORECASE)
    if cache_upsert:
        return (
            "MERGE INTO hive_cache t USING (VALUES (?, ?, ?, ?, ?, ?, ?)) "
            "s(cache_key, result_json, source_job_id, result_mnemos_id, model, provider, cached_at) "
            "ON t.cache_key = s.cache_key "
            "WHEN MATCHED THEN UPDATE SET result_json=s.result_json, source_job_id=s.source_job_id, "
            "result_mnemos_id=s.result_mnemos_id, cached_at=s.cached_at, model=s.model, provider=s.provider "
            "WHEN NOT MATCHED THEN INSERT "
            "(cache_key, result_json, source_job_id, result_mnemos_id, hit_count, cost_saved_usd, model, provider, cached_at, last_hit_at) "
            "VALUES (s.cache_key, s.result_json, s.source_job_id, s.result_mnemos_id, 0, 0, s.model, s.provider, s.cached_at, NULL)",
            values,
        )

    stats_match = re.search(
        r"INSERT INTO worker_kind_stats \(urn, kind, (success_count|fail_count|cancelled_count), .* ON CONFLICT\(urn, kind\) DO UPDATE SET",
        " ".join(stripped.split()),
        re.IGNORECASE,
    )
    if stats_match:
        col = stats_match.group(1)
        return (
            "MERGE INTO worker_kind_stats t USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) "
            "s(urn, kind, ins_tokens_in, ins_tokens_out, ins_cost, ins_duration, ins_last_run, "
            "upd_tokens_in, upd_tokens_out, upd_cost, upd_duration, upd_last_run) "
            "ON t.urn = s.urn AND t.kind = s.kind "
            f"WHEN MATCHED THEN UPDATE SET {col}={col}+1, "
            "total_tokens_in=total_tokens_in+s.upd_tokens_in, total_tokens_out=total_tokens_out+s.upd_tokens_out, "
            "total_cost_usd=total_cost_usd+s.upd_cost, total_duration_sec=total_duration_sec+s.upd_duration, last_run=s.upd_last_run "
            f"WHEN NOT MATCHED THEN INSERT (urn, kind, {col}, total_tokens_in, total_tokens_out, total_cost_usd, total_duration_sec, last_run) "
            "VALUES (s.urn, s.kind, 1, s.ins_tokens_in, s.ins_tokens_out, s.ins_cost, s.ins_duration, s.ins_last_run)",
            values,
        )

    limit_match = re.search(r"\s+LIMIT\s+\?\s*$", stripped, flags=re.IGNORECASE)
    if limit_match:
        if not values:
            raise ValueError("Db2 LIMIT ? translation requires a bound limit value")
        limit = int(values[-1])
        stripped = re.sub(r"\s+LIMIT\s+\?\s*$", f" FETCH FIRST {limit} ROWS ONLY", stripped, flags=re.IGNORECASE)
        values = values[:-1]
    stripped = re.sub(r"\s+LIMIT\s+(\d+)\s*$", r" FETCH FIRST \1 ROWS ONLY", stripped, flags=re.IGNORECASE)
    return stripped, values


class _Db2Cursor:
    def __init__(self, cursor: Any | None = None, rows: list[tuple[Any, ...]] | None = None, rowcount: int = -1):
        self._cursor = cursor
        self._rows = rows
        self._index = 0
        self.rowcount = rowcount if cursor is None else getattr(cursor, "rowcount", rowcount)
        self.lastrowid = None

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

    @staticmethod
    def _json_default(value: Any) -> Any:
        if isinstance(value, _Decimal):
            return int(value) if value == value.to_integral_value() else float(value)
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", errors="replace")
        if isinstance(value, _dt.datetime):
            return value.isoformat()
        raise TypeError(f"unhandled type {type(value).__name__}")

    @classmethod
    async def _normalize_row(cls, row: Any) -> Any:
        if row is None:
            return None
        out = []
        for value in row:
            if isinstance(value, (dict, list)):
                out.append(_json.dumps(value, separators=(",", ":"), default=cls._json_default))
            elif hasattr(value, "read") and callable(value.read):
                result = await asyncio.to_thread(value.read)
                out.append(result)
            elif isinstance(value, _Decimal):
                out.append(int(value) if value == value.to_integral_value() else float(value))
            elif isinstance(value, _dt.datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=_dt.timezone.utc)
                out.append(value.timestamp())
            elif isinstance(value, (bytes, bytearray)):
                out.append(value.decode("utf-8", errors="replace"))
            else:
                out.append(value)
        return tuple(out)

    async def fetchone(self):
        if self._rows is not None:
            if self._index >= len(self._rows):
                return None
            row = self._rows[self._index]
            self._index += 1
            return await self._normalize_row(row)
        return await self._normalize_row(await asyncio.to_thread(self._cursor.fetchone))

    async def fetchall(self):
        if self._rows is not None:
            rows = self._rows[self._index:]
            self._index = len(self._rows)
            return [await self._normalize_row(row) for row in rows]
        return [await self._normalize_row(row) for row in await asyncio.to_thread(self._cursor.fetchall)]

    async def close(self):
        if self._cursor is not None:
            await asyncio.to_thread(self._cursor.close)


class _Db2ExecuteResult:
    def __init__(self, conn: "_Db2Connection", sql: str, params: Any = None):
        self._conn = conn
        self._sql = sql
        self._params = params
        self._cursor: _Db2Cursor | None = None

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


class _Db2Connection:
    def __init__(self, raw: Any):
        self._raw = raw

    def execute(self, sql: str, params: Any = None):
        return _Db2ExecuteResult(self, sql, params)

    async def _execute(self, sql: str, params: Any = None) -> _Db2Cursor:
        adapted, adapted_params = _adapt_sql(sql, params)
        if adapted is None:
            return _Db2Cursor(rows=[])

        def _go():
            cur = self._raw.cursor()
            if len(adapted_params) > 0:
                cur.execute(adapted, adapted_params)
            else:
                cur.execute(adapted)
            return cur

        return _Db2Cursor(await asyncio.to_thread(_go))

    async def executemany(self, sql: str, seq_of_params: list[tuple[Any, ...]]) -> _Db2Cursor:
        if not seq_of_params:
            return _Db2Cursor(rows=[])
        adapted, _ = _adapt_sql(sql, seq_of_params[0])
        if adapted is None:
            return _Db2Cursor(rows=[])

        def _go():
            cur = self._raw.cursor()
            cur.executemany(adapted, seq_of_params)
            return cur

        return _Db2Cursor(await asyncio.to_thread(_go))

    async def executescript(self, script: str) -> _Db2Cursor:
        for stmt in _split_script(script):
            try:
                await self._execute(stmt)
            except Exception as exc:
                if not _is_already_exists(exc):
                    raise
        return _Db2Cursor(rows=[])

    async def commit(self) -> None:
        await asyncio.to_thread(self._raw.commit)

    async def rollback(self) -> None:
        await asyncio.to_thread(self._raw.rollback)

    async def close(self) -> None:
        await asyncio.to_thread(self._raw.close)


class _Db2Pool:
    def __init__(self, dsn: str, *, min_size: int = 0, max_size: int = 8, acquire_timeout: float = 30.0):
        self._parts = _parse_db2_dsn(dsn)
        self._min_size = max(0, min_size)
        self._max_size = max(1, max_size)
        self._acquire_timeout = acquire_timeout
        self._idle: list[_Db2Connection] = []
        self._in_use: set[_Db2Connection] = set()
        self._reserved = 0
        self._closed = False
        self._lock = asyncio.Lock()
        self._not_full = asyncio.Condition(self._lock)

    async def _open(self) -> _Db2Connection:
        try:
            import ibm_db_dbi  # type: ignore
        except Exception as exc:
            raise RuntimeError(f"HIVE_BACKEND=db2 requires ibm_db/ibm_db_dbi importable before connecting: {exc}") from exc
        raw = await asyncio.to_thread(ibm_db_dbi.connect, _dsn_string(self._parts), "", "")
        return _Db2Connection(raw)

    async def warmup(self) -> None:
        opened = []
        for _ in range(self._min_size):
            try:
                opened.append(await self._open())
            except Exception:
                break
        async with self._lock:
            self._idle.extend(opened)

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[_Db2Connection]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._acquire_timeout
        conn: _Db2Connection | None = None
        need_open = False
        async with self._lock:
            while True:
                if self._closed:
                    raise RuntimeError("DB2 pool is closed")
                if self._idle:
                    conn = self._idle.pop()
                    self._in_use.add(conn)
                    break
                if len(self._in_use) + self._reserved < self._max_size:
                    self._reserved += 1
                    need_open = True
                    break
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError(f"DB2 pool acquire timeout after {self._acquire_timeout:.1f}s")
                await asyncio.wait_for(self._not_full.wait(), remaining)
        if need_open:
            try:
                conn = await self._open()
            finally:
                async with self._lock:
                    self._reserved -= 1
                    self._not_full.notify()
            async with self._lock:
                self._in_use.add(conn)
        try:
            yield conn
        finally:
            async with self._lock:
                self._in_use.discard(conn)
                if self._closed:
                    await conn.close()
                else:
                    self._idle.append(conn)
                self._not_full.notify()

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            self._not_full.notify_all()
            while self._idle:
                await self._idle.pop().close()


class Db2HiveRepository(SqliteHiveRepository):
    def __init__(self, dsn: str, *, min_size: int | None = None, max_size: int | None = None, acquire_timeout: float | None = None) -> None:
        super().__init__(":memory:")
        self.dsn = dsn
        self._pool: _Db2Pool | None = None
        self._min_size = int(min_size if min_size is not None else os.environ.get("HIVE_DB2_POOL_MIN", "0"))
        self._max_size = int(max_size if max_size is not None else os.environ.get("HIVE_DB2_POOL_MAX", "8"))
        self._acquire_timeout = float(acquire_timeout if acquire_timeout is not None else os.environ.get("HIVE_DB2_ACQUIRE_TIMEOUT", "30"))

    async def init(self, schema: str | None = None) -> None:
        if not self.dsn:
            raise RuntimeError("HIVE_BACKEND=db2 requires DB2_DSN or HIVE_DB2_DSN")
        async with self.connection() as db:
            await db.executescript(DB2_DDL)
            await db.commit()

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def _ensure_pool(self) -> _Db2Pool:
        if self._pool is None:
            pool = _Db2Pool(self.dsn, min_size=self._min_size, max_size=self._max_size, acquire_timeout=self._acquire_timeout)
            await pool.warmup()
            self._pool = pool
        return self._pool

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        if not self.dsn:
            raise RuntimeError("HIVE_BACKEND=db2 requires DB2_DSN or HIVE_DB2_DSN")
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            yield conn
