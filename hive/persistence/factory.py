"""Persistence backend construction for the Hive Mind service."""

from __future__ import annotations

from contextlib import asynccontextmanager
import sys
from typing import Any, AsyncIterator
from urllib.parse import unquote, urlparse

class _OracleCompatibilityRepository:
    """Expose the connection seam still used by ``agent_bus`` for Oracle."""

    def __init__(self, dsn: str) -> None:
        parsed = urlparse(dsn)
        if parsed.scheme != "oracle":
            self.user = ""
            self.password = ""
            self.connect_dsn = dsn
        else:
            self.user = unquote(parsed.username or "")
            self.password = unquote(parsed.password or "")
            host = parsed.hostname or "127.0.0.1"
            port = parsed.port or 1521
            service = (parsed.path or "/ORCLPDB1").lstrip("/")
            self.connect_dsn = f"{host}:{port}/{service}"

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        import oracledb

        raw = await oracledb.connect_async(
            user=self.user,
            password=self.password,
            dsn=self.connect_dsn,
        )
        # Imported lazily to avoid a module cycle while agent_bus imports this
        # factory. The compatibility wrapper remains in agent_bus during the
        # repository migration.
        bus_module = sys.modules.get("agent_bus") or sys.modules.get("__main__")
        wrapper = getattr(bus_module, "OracleConnection", None)
        if wrapper is None:
            await raw.close()
            raise RuntimeError("agent_bus Oracle compatibility wrapper is unavailable")
        connection = wrapper(raw)
        try:
            yield connection
        finally:
            await connection.close()

    async def init(self, schema: str | None = None) -> None:
        return None

    async def close(self) -> None:
        return None


def get_hive_repository(
    backend: str,
    *,
    db_path: str,
    busy_timeout_ms: int = 5000,
    dsn: str = "",
    db2_dsn: str = "",
) -> Any:
    """Return the configured persistence adapter.

    DB2 is named in legacy configuration validation, but no DB2 connection
    adapter exists in this repository. Fail explicitly if it is selected.
    """
    normalized = backend.strip().lower()
    if normalized == "sqlite":
        # Keep the optional aiosqlite dependency out of Oracle-only processes.
        from .sqlite import SqliteHiveRepository

        return SqliteHiveRepository(db_path, busy_timeout_ms=busy_timeout_ms)
    if normalized == "oracle":
        return _OracleCompatibilityRepository(dsn)
    if normalized == "db2":
        raise RuntimeError(
            "HIVE_BACKEND=db2 is not available: no DB2 persistence adapter is installed"
        )
    raise ValueError(f"unsupported Hive persistence backend: {backend!r}")


__all__ = ["get_hive_repository"]
