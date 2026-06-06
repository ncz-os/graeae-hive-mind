"""Oracle Hive Mind repository.

The live Oracle SQL translation shim remains behavior-compatible with the
pre-refactor bus. This module owns Oracle backend selection; the FastAPI layer
uses the repository factory instead of branching on HIVE_BACKEND.
"""

from __future__ import annotations

import datetime as _dt
import json as _json
import re
import uuid as _uuid
from contextlib import asynccontextmanager
from decimal import Decimal as _Decimal
from typing import Any, AsyncIterator
from urllib.parse import unquote, urlparse

from .sqlite import SqliteHiveRepository


def _parse_oracle_url(url: str) -> tuple[str, str, str]:
    parsed = urlparse(url)
    if parsed.scheme != "oracle":
        return "", "", url
    user = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 1521
    service = (parsed.path or "/ORCLPDB1").lstrip("/")
    return user, password, f"{host}:{port}/{service}"


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


def _replace_qmarks(sql: str) -> str:
    out: list[str] = []
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


def _translate_upsert(sql: str) -> str | None:
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
            "WHEN MATCHED THEN UPDATE SET t.result_json=s.result_json, t.source_job_id=s.source_job_id, "
            "t.result_mnemos_id=s.result_mnemos_id, t.cached_at=s.cached_at, t.model=s.model, t.provider=s.provider "
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
            "t.total_tokens_in=t.total_tokens_in+s.upd_tokens_in, t.total_tokens_out=t.total_tokens_out+s.upd_tokens_out, "
            "t.total_cost_usd=t.total_cost_usd+s.upd_cost, t.total_duration_sec=t.total_duration_sec+s.upd_duration, "
            "t.last_run=s.upd_last_run "
            "WHEN NOT MATCHED THEN INSERT "
            f"(urn, kind, {col}, total_tokens_in, total_tokens_out, total_cost_usd, total_duration_sec, last_run) "
            f"VALUES (s.urn, s.kind, 1, s.ins_tokens_in, s.ins_tokens_out, s.ins_cost, s.ins_duration, s.ins_last_run)"
        )
    return None


def _translate_sql(sql: str, params: Any = None) -> tuple[str | None, Any]:
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
    def __init__(self, cursor=None, rows: list | None = None, rowcount: int = -1):
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

    @staticmethod
    async def _normalize_row(row):
        if row is None:
            return None

        def default(value):
            if isinstance(value, _Decimal):
                return float(value) if value != value.to_integral_value() else int(value)
            if isinstance(value, (bytes, bytearray)):
                if len(value) == 16:
                    return str(_uuid.UUID(bytes=bytes(value)))
                return value.decode("utf-8", errors="replace")
            if isinstance(value, _dt.datetime):
                return value.isoformat()
            raise TypeError(f"unhandled type {type(value).__name__}")

        out = []
        for value in row:
            if isinstance(value, (dict, list)):
                out.append(_json.dumps(value, separators=(",", ":"), default=default))
            elif hasattr(value, "read") and callable(value.read):
                result = value.read()
                if hasattr(result, "__await__"):
                    result = await result
                out.append(result)
            elif isinstance(value, _Decimal):
                out.append(int(value) if value == value.to_integral_value() else float(value))
            elif isinstance(value, _dt.datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=_dt.timezone.utc)
                out.append(value.timestamp())
            elif isinstance(value, (bytes, bytearray)) and len(value) == 16:
                out.append(str(_uuid.UUID(bytes=bytes(value))))
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
        return await self._normalize_row(await self._cursor.fetchone())

    async def fetchall(self):
        if self._rows is not None:
            rows = self._rows[self._index:]
            self._index = len(self._rows)
            return [await self._normalize_row(row) for row in rows]
        return [await self._normalize_row(row) for row in await self._cursor.fetchall()]

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

    def execute(self, sql: str, params: Any = None):
        return _OracleExecuteResult(self, sql, params)

    async def _execute(self, sql: str, params: Any = None):
        match = re.match(r"^\s*PRAGMA\s+table_info\((\w+)\)", sql, flags=re.IGNORECASE)
        if match:
            mapped = _TABLE_MAP.get(match.group(1).lower(), match.group(1))
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
        await cur.executemany(_rewrite_limit(translated), [tuple(p) for p in seq_of_params])
        return BackendCursor(cur)

    async def executescript(self, script: str):
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


class OracleHiveRepository(SqliteHiveRepository):
    """Oracle backend using the repository-owned SQL compatibility adapter."""

    connection_class: Any = OracleConnection

    def __init__(self, dsn: str) -> None:
        super().__init__(":memory:")
        self.dsn = dsn

    async def init(self, schema: str | None = None) -> None:
        # Oracle deployments are migrated separately; the connection adapter
        # treats SQLite DDL scripts as no-ops just like the pre-refactor bus.
        return None

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        try:
            import oracledb
        except Exception as exc:
            raise RuntimeError(f"HIVE_BACKEND=oracle requires python-oracledb importable: {exc}") from exc
        user, password, dsn = _parse_oracle_url(self.dsn)
        raw = await oracledb.connect_async(user=user, password=password, dsn=dsn)
        db = self.connection_class(raw)
        try:
            yield db
        finally:
            await db.close()
