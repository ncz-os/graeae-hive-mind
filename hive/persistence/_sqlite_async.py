"""Tiny async sqlite compatibility layer used only when aiosqlite is absent."""

from __future__ import annotations

import sqlite3
from typing import Any, Iterable

Error = sqlite3.Error
IntegrityError = sqlite3.IntegrityError
OperationalError = sqlite3.OperationalError


class Cursor:
    def __init__(self, cursor: sqlite3.Cursor):
        self._cursor = cursor
        self.rowcount = cursor.rowcount
        self.lastrowid = cursor.lastrowid

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

    async def fetchone(self):
        return self._cursor.fetchone()

    async def fetchall(self):
        return self._cursor.fetchall()

    async def close(self):
        self._cursor.close()


class _ExecuteResult:
    def __init__(self, conn: "Connection", sql: str, params: Any = None):
        self._conn = conn
        self._sql = sql
        self._params = params
        self._cursor: Cursor | None = None

    def __await__(self):
        return self._run().__await__()

    async def _run(self) -> Cursor:
        self._cursor = await self._conn._execute(self._sql, self._params)
        return self._cursor

    async def __aenter__(self):
        return await self._run()

    async def __aexit__(self, exc_type, exc, tb):
        if self._cursor is not None:
            await self._cursor.close()


class Connection:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def execute(self, sql: str, params: Any = None):
        return _ExecuteResult(self, sql, params)

    async def _execute(self, sql: str, params: Any = None) -> Cursor:
        cur = self._conn.cursor()
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        return Cursor(cur)

    async def executemany(self, sql: str, seq_of_params: Iterable[Any]) -> Cursor:
        cur = self._conn.cursor()
        cur.executemany(sql, seq_of_params)
        return Cursor(cur)

    async def executescript(self, script: str) -> Cursor:
        cur = self._conn.cursor()
        cur.executescript(script)
        return Cursor(cur)

    async def commit(self):
        self._conn.commit()

    async def rollback(self):
        self._conn.rollback()

    async def close(self):
        self._conn.close()


async def connect(database: str, **kwargs: Any) -> Connection:
    timeout = kwargs.pop("timeout", 5.0)
    check_same_thread = kwargs.pop("check_same_thread", False)
    conn = sqlite3.connect(database, timeout=timeout, check_same_thread=check_same_thread)
    return Connection(conn)
