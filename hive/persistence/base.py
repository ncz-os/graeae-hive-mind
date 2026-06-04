"""Backend-neutral Hive Mind persistence interfaces."""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol, runtime_checkable


Row = tuple[Any, ...]


@runtime_checkable
class Transaction(Protocol):
    """Backend-neutral transaction/connection handle."""

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...


class HiveMindRepository(ABC):
    """Storage facade used by the Hive Mind bus.

    Concrete backends own their connection/dialect details. The generic
    ``connection`` handle is intentionally present during Phase 1 so the live
    FastAPI service can be moved behind this facade without changing endpoint
    behavior in one risky rewrite.
    """

    @abstractmethod
    async def init(self, schema: str | None = None) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    def connection(self) -> AbstractAsyncContextManager[Transaction]: ...

    @abstractmethod
    async def emit_event(self, tx: Transaction, *, ts: float, kind: str, payload_json: str, agent_urn: str | None) -> None: ...

    @abstractmethod
    async def register_agent(self, **fields: Any) -> None: ...

    @abstractmethod
    async def heartbeat_agent(self, *, urn: str, ts: float, status: str, metadata_json: str | None = None) -> int: ...

    @abstractmethod
    async def require_agent(self, *, urn: str) -> Row | None: ...

    @abstractmethod
    async def list_agents(self, **filters: Any) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def submit_job(self, **fields: Any) -> None: ...

    @abstractmethod
    async def claim_next_job(
        self,
        *,
        agent_urn: str,
        now: float,
        active_statuses: set[str],
        claim_lease_seconds: float,
        host_denylist: set[str],
        match_capabilities: Any,
        kind_aliases: Any,
        json_list: Any,
        cost_tiers: list[str],
        throttle_headroom: float,
    ) -> dict[str, Any] | None: ...

    @abstractmethod
    async def patch_job_status(self, **fields: Any) -> None: ...

    @abstractmethod
    async def get_job(self, *, job_id: str) -> Row | None: ...

    @abstractmethod
    async def list_jobs(self, **filters: Any) -> tuple[int, list[dict[str, Any]]]: ...

    @abstractmethod
    async def find_active_dedup_job(self, *, dedup_hash: str) -> Row | None: ...

    @abstractmethod
    async def post_message(self, **fields: Any) -> None: ...

    @abstractmethod
    async def list_messages(self, **filters: Any) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def tail_events(self, *, since_id: int | None = None, limit: int = 100) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def cache_lookup(self, *, cache_key: str, cutoff: float) -> dict[str, Any] | None: ...

    @abstractmethod
    async def cache_store(self, **fields: Any) -> None: ...

    @abstractmethod
    async def cache_record_hit(self, *, cache_key: str, cost_saved_usd: float, ts: float) -> None: ...

    @abstractmethod
    async def upsert_worker_kind_stats(self, **fields: Any) -> None: ...

    @abstractmethod
    async def create_schedule(self, **fields: Any) -> None: ...

    @abstractmethod
    async def list_schedules(self) -> list[dict[str, Any]]: ...
