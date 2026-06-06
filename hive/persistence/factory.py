"""Hive repository factory."""

from __future__ import annotations

import os

from .base import HiveMindRepository
from .sqlite import SqliteHiveRepository


def get_hive_repository(backend: str | None = None, **settings) -> HiveMindRepository:
    selected = (backend or os.environ.get("HIVE_BACKEND", "sqlite")).lower()
    if selected == "sqlite":
        return SqliteHiveRepository(
            settings.get("db_path") or os.environ.get("AGENT_BUS_DB", "/srv/agent-bus/agents.db"),
            busy_timeout_ms=int(settings.get("busy_timeout_ms") or os.environ.get("AGENT_BUS_SQLITE_BUSY_TIMEOUT_MS", "5000")),
        )
    if selected == "oracle":
        from .oracle import OracleHiveRepository

        return OracleHiveRepository(settings.get("dsn") or os.environ.get("ORACLE_DSN", "oracle://mnemos:mnemos_dev@127.0.0.1:1521/ORCLPDB1"))
    if selected == "db2":
        from .db2 import Db2HiveRepository

        return Db2HiveRepository(settings.get("db2_dsn") or os.environ.get("DB2_DSN") or os.environ.get("HIVE_DB2_DSN", ""))
    raise RuntimeError(f"HIVE_BACKEND must be sqlite, oracle, or db2, got {selected!r}")
