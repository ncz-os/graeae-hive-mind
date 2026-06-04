"""SQLite Hive Mind repository."""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

try:
    import aiosqlite
except ModuleNotFoundError:
    from . import _sqlite_async as aiosqlite

from .base import HiveMindRepository, Transaction


class SqliteHiveRepository(HiveMindRepository):
    def __init__(self, db_path: str, *, busy_timeout_ms: int = 5000) -> None:
        self.db_path = db_path
        self.busy_timeout_ms = busy_timeout_ms

    async def init(self, schema: str | None = None) -> None:
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        async with self.connection() as db:
            if schema:
                await db.executescript("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
                await db.executescript(schema)
            await self._ensure_column(db, "jobs", "dedup_hash", "dedup_hash TEXT")
            await self._ensure_column(db, "jobs", "decline_count", "decline_count INTEGER NOT NULL DEFAULT 0")
            await db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS jobs_active_dedup_uq ON jobs (dedup_hash) "
                "WHERE dedup_hash IS NOT NULL AND status IN ('queued', 'offered', 'claimed', 'running')"
            )
            await db.commit()

    async def close(self) -> None:
        return None

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Transaction]:
        db = await aiosqlite.connect(
            self.db_path,
            timeout=self.busy_timeout_ms / 1000,
            check_same_thread=False,
        )
        try:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
            await db.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            yield db
        finally:
            await db.close()

    async def _ensure_column(self, db: Any, table: str, col: str, ddl_fragment: str) -> None:
        try:
            cursor = await db.execute(f"PRAGMA table_info({table})")
            cols = {row[1] for row in await cursor.fetchall()}
        except Exception:
            return
        if col not in cols:
            await db.execute(f"ALTER TABLE {table} ADD COLUMN {ddl_fragment}")

    async def emit_event(self, tx: Transaction, *, ts: float, kind: str, payload_json: str, agent_urn: str | None) -> None:
        await tx.execute(
            "INSERT INTO events (ts, kind, payload, agent_urn) VALUES (?, ?, ?, ?)",
            (ts, kind, payload_json, agent_urn),
        )

    async def register_agent(self, **fields: Any) -> None:
        async with self.connection() as db:
            await db.execute(
                "INSERT INTO agents (urn, kind, runtime, model, provider, cost_tier, autonomy_level, "
                "auth_method, plan_cap_usd, plan_period_used_usd, subscription_pools, host, session_id, "
                "pid, capabilities, version, started_at, last_heartbeat, status, metadata) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, 'online', ?)",
                (
                    fields["urn"], fields["kind"], fields["runtime"], fields["model"],
                    fields["provider"], fields["cost_tier"], fields["autonomy_level"],
                    fields["auth_method"], fields["plan_cap_usd"], fields.get("subscription_pools_json"),
                    fields["host"], fields["session_id"], fields.get("pid"),
                    fields.get("capabilities_json"), fields.get("version"), fields["started_at"],
                    fields["last_heartbeat"], fields.get("metadata_json"),
                ),
            )
            await db.commit()

    async def heartbeat_agent(self, *, urn: str, ts: float, status: str, metadata_json: str | None = None) -> int:
        async with self.connection() as db:
            if metadata_json is not None:
                cur = await db.execute(
                    "UPDATE agents SET last_heartbeat=?, status=?, metadata=? WHERE urn=?",
                    (ts, status, metadata_json, urn),
                )
            else:
                cur = await db.execute(
                    "UPDATE agents SET last_heartbeat=?, status=? WHERE urn=?",
                    (ts, status, urn),
                )
            await db.commit()
            return cur.rowcount

    async def require_agent(self, *, urn: str) -> tuple[Any, ...] | None:
        async with self.connection() as db:
            async with db.execute("SELECT runtime, status FROM agents WHERE urn=?", (urn,)) as cur:
                return await cur.fetchone()

    async def list_agents(self, **filters: Any) -> list[dict[str, Any]]:
        sql = ("SELECT urn, kind, host, status, last_heartbeat, capabilities, version, metadata, "
               "pid, runtime, model, provider, cost_tier, autonomy_level FROM agents WHERE 1=1")
        args: list[Any] = []
        if filters.get("status"):
            sql += " AND status=?"; args.append(filters["status"])
        elif not filters.get("include_offline", False):
            sql += " AND status='online'"
        for key in ("kind", "host", "runtime", "pid", "cost_tier"):
            if filters.get(key) is not None:
                sql += f" AND {key}=?"; args.append(filters[key])
        sql += " ORDER BY last_heartbeat DESC"
        rows: list[dict[str, Any]] = []
        async with self.connection() as db:
            async with db.execute(sql, args) as cur:
                async for r in cur:
                    rows.append({"urn": r[0], "kind": r[1], "host": r[2], "status": r[3], "last_heartbeat": r[4],
                                 "capabilities": json.loads(r[5]) if r[5] else None, "version": r[6],
                                 "metadata": json.loads(r[7]) if r[7] else {}, "pid": r[8], "runtime": r[9],
                                 "model": r[10], "provider": r[11], "cost_tier": r[12], "autonomy_level": r[13]})
        return rows

    async def submit_job(self, **fields: Any) -> None:
        async with self.connection() as db:
            try:
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, parent_job_id, kind, description, priority, deadline, "
                    "required_capabilities, eligible_kinds, eligible_hosts, project, max_cost_tier, preferred_providers, "
                    "preferred_models, mnemos_refs, depends_on, max_retries, dedup_hash, status, started_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)",
                    (
                        fields["id"], fields["submitter_urn"], fields.get("parent_job_id"), fields["kind"],
                        fields.get("description"), fields.get("priority", 0), fields.get("deadline"),
                        fields.get("required_capabilities_json"), fields.get("eligible_kinds_json"),
                        fields.get("eligible_hosts_json"), fields.get("project"), fields.get("max_cost_tier"),
                        fields.get("preferred_providers_json"), fields.get("preferred_models_json"),
                        fields.get("mnemos_refs_json"), fields.get("depends_on_json"),
                        int(fields.get("max_retries", 2)), fields.get("dedup_hash"), fields["started_at"],
                    ),
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise

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
    ) -> dict[str, Any] | None:
        async with self.connection() as db:
            async with db.execute(
                "SELECT kind, capabilities, runtime, model, provider, cost_tier, auth_method, "
                "plan_cap_usd, plan_period_used_usd, status FROM agents WHERE urn=?",
                (agent_urn,),
            ) as cur:
                row = await cur.fetchone()
            if not row:
                return {"error": "not_registered"}
            agent_kind, caps_json, a_runtime, a_model, a_provider, a_tier, a_auth, a_cap, a_used, agent_status = row
            if agent_status not in active_statuses:
                return {"error": "inactive", "status": agent_status}
            urn_parts = agent_urn.split(":")
            urn_host = (urn_parts[3] if len(urn_parts) >= 4 else "").lower()
            if urn_host in host_denylist:
                return {"error": "host_denylisted", "host": urn_host}
            agent_caps = set(json.loads(caps_json)) if caps_json else set()
            eligible_aliases = kind_aliases(agent_kind, a_runtime)
            a_tier = (a_tier or "C").upper()
            if a_tier not in cost_tiers:
                a_tier = "C"
            a_auth = (a_auth or "unknown").lower()
            sub_throttled = (
                a_auth == "subscription" and a_cap and a_used
                and a_used >= throttle_headroom * a_cap
            )

            await db.execute("BEGIN IMMEDIATE")
            committed = False
            try:
                async with db.execute(
                    "SELECT id, kind, description, priority, deadline, required_capabilities, eligible_kinds, "
                    "eligible_hosts, submitter_urn, parent_job_id, started_at, max_cost_tier, preferred_providers, "
                    "preferred_models, mnemos_refs, depends_on FROM jobs WHERE status='queued' "
                    "AND (retry_backoff_until IS NULL OR retry_backoff_until <= ?) "
                    "ORDER BY priority DESC, started_at ASC",
                    (now,),
                ) as cur:
                    async for r in cur:
                        (job_id, j_kind, j_desc, j_prio, j_dead, j_caps_json, _j_kinds_json,
                         _j_hosts_json, j_sub, j_par, j_started, j_max_tier, j_pref_providers,
                         j_pref_models, j_mnemos_refs, j_deps_json) = r
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
                                    continue
                        if _j_kinds_json:
                            kinds = set(json_list(_j_kinds_json))
                            if kinds and "*" not in kinds and not kinds.intersection(eligible_aliases):
                                continue
                        if _j_hosts_json:
                            hosts = set(json_list(_j_hosts_json))
                            host_lc = {h.lower() for h in hosts}
                            agent_host = (urn_parts[3] if len(urn_parts) > 3 else "").lower()
                            if hosts and "*" not in hosts and agent_host not in host_lc:
                                continue
                        if j_caps_json:
                            labels = json_list(j_caps_json)
                            if labels and not match_capabilities(labels, agent_caps).eligible:
                                continue
                        job_max_tier = (j_max_tier or "B").upper()
                        if job_max_tier not in cost_tiers:
                            continue
                        if a_tier not in cost_tiers:
                            continue
                        if cost_tiers.index(a_tier) > cost_tiers.index(job_max_tier):
                            continue
                        if sub_throttled and job_max_tier != "A":
                            continue
                        if j_pref_providers:
                            provs = json.loads(j_pref_providers)
                            if provs and a_provider not in provs:
                                continue
                        if j_pref_models:
                            models = json.loads(j_pref_models)
                            if models and a_model not in models:
                                continue
                        claim_cur = await db.execute(
                            "UPDATE jobs SET status='claimed', claimed_by=?, claimed_at=?, "
                            "claimed_runtime=?, claimed_model=?, claimed_provider=?, claimed_cost_tier=?, "
                            "claim_lease_expires_at=? WHERE id=? AND status='queued'",
                            (agent_urn, now, a_runtime, a_model, a_provider, a_tier,
                             now + claim_lease_seconds, job_id),
                        )
                        if (getattr(claim_cur, "rowcount", 0) or 0) < 1:
                            continue
                        await db.execute("COMMIT")
                        committed = True
                        return {
                            "id": job_id, "kind": j_kind, "description": j_desc, "priority": j_prio,
                            "deadline": j_dead, "max_cost_tier": j_max_tier, "submitter_urn": j_sub,
                            "parent_job_id": j_par, "claimed_at": now, "queued_at": j_started,
                            "mnemos_refs": json.loads(j_mnemos_refs) if j_mnemos_refs else [],
                            "claimed_resources": {"runtime": a_runtime, "model": a_model,
                                                  "provider": a_provider, "cost_tier": a_tier},
                        }
                await db.execute("COMMIT")
                committed = True
                return None
            finally:
                if not committed:
                    await db.execute("ROLLBACK")

    async def patch_job_status(self, **fields: Any) -> None:
        raise NotImplementedError("PATCH /v1/jobs still uses the compatibility transaction handle in Phase 1")

    async def get_job(self, *, job_id: str) -> tuple[Any, ...] | None:
        async with self.connection() as db:
            async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
                return await cur.fetchone()

    async def list_jobs(self, **filters: Any) -> tuple[int, list[dict[str, Any]]]:
        raise NotImplementedError("list_jobs endpoint still uses the compatibility transaction handle in Phase 1")

    async def find_active_dedup_job(self, *, dedup_hash: str) -> tuple[Any, ...] | None:
        async with self.connection() as db:
            async with db.execute(
                "SELECT id, started_at FROM jobs WHERE dedup_hash=? "
                "AND status IN ('queued', 'offered', 'claimed', 'running') "
                "ORDER BY started_at ASC LIMIT 1",
                (dedup_hash,),
            ) as cur:
                return await cur.fetchone()

    async def post_message(self, **fields: Any) -> None:
        async with self.connection() as db:
            await db.execute(
                "INSERT INTO messages (id, from_urn, to_urn, in_reply_to, topic, payload, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (fields["id"], fields["from_urn"], fields.get("to_urn"), fields.get("in_reply_to"),
                 fields["topic"], fields["payload_json"], fields["ts"]),
            )
            await db.commit()

    async def list_messages(self, **filters: Any) -> list[dict[str, Any]]:
        raise NotImplementedError("list_messages endpoint still uses the compatibility transaction handle in Phase 1")

    async def tail_events(self, *, since_id: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
        sql = "SELECT id, ts, kind, payload, agent_urn FROM events"
        args: list[Any] = []
        if since_id is not None:
            sql += " WHERE id > ?"; args.append(since_id)
        sql += " ORDER BY id ASC LIMIT ?"; args.append(limit)
        rows: list[dict[str, Any]] = []
        async with self.connection() as db:
            async with db.execute(sql, args) as cur:
                async for r in cur:
                    rows.append({"id": r[0], "ts": r[1], "kind": r[2], "payload": json.loads(r[3]), "agent_urn": r[4]})
        return rows

    async def cache_lookup(self, *, cache_key: str, cutoff: float) -> dict[str, Any] | None:
        async with self.connection() as db:
            async with db.execute(
                "SELECT result_json, source_job_id, result_mnemos_id, hit_count, cost_saved_usd, model, provider, cached_at "
                "FROM hive_cache WHERE cache_key=? AND cached_at >= ?",
                (cache_key, cutoff),
            ) as cur:
                r = await cur.fetchone()
        if not r:
            return None
        return {"result": json.loads(r[0]) if r[0] else None, "source_job_id": r[1],
                "result_mnemos_id": r[2], "hit_count": r[3], "cost_saved_usd": r[4],
                "model": r[5], "provider": r[6], "cached_at": r[7]}

    async def cache_store(self, **fields: Any) -> None:
        async with self.connection() as db:
            await db.execute(
                "INSERT INTO hive_cache (cache_key, result_json, source_job_id, result_mnemos_id, hit_count, "
                "cost_saved_usd, model, provider, cached_at, last_hit_at) VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, NULL) "
                "ON CONFLICT(cache_key) DO UPDATE SET result_json=excluded.result_json, "
                "source_job_id=excluded.source_job_id, result_mnemos_id=excluded.result_mnemos_id, "
                "cached_at=excluded.cached_at, model=excluded.model, provider=excluded.provider",
                (fields["cache_key"], fields["result_json"], fields.get("source_job_id"),
                 fields.get("result_mnemos_id"), fields.get("model"), fields.get("provider"), fields["cached_at"]),
            )
            await db.commit()

    async def cache_record_hit(self, *, cache_key: str, cost_saved_usd: float, ts: float) -> None:
        async with self.connection() as db:
            await db.execute(
                "UPDATE hive_cache SET hit_count=hit_count+1, cost_saved_usd=COALESCE(cost_saved_usd,0)+?, "
                "last_hit_at=? WHERE cache_key=?",
                (cost_saved_usd, ts, cache_key),
            )
            await db.commit()

    async def upsert_worker_kind_stats(self, **fields: Any) -> None:
        col = fields["column"]
        if col not in {"success_count", "fail_count", "cancelled_count"}:
            raise ValueError(f"invalid worker stats column: {col!r}")
        async with self.connection() as db:
            await db.execute(
                f"INSERT INTO worker_kind_stats (urn, kind, {col}, total_tokens_in, total_tokens_out, "
                f"total_cost_usd, total_duration_sec, last_run) VALUES (?, ?, 1, ?, ?, ?, ?, ?) "
                f"ON CONFLICT(urn, kind) DO UPDATE SET {col}={col}+1, "
                f"total_tokens_in=total_tokens_in+?, total_tokens_out=total_tokens_out+?, "
                f"total_cost_usd=total_cost_usd+?, total_duration_sec=total_duration_sec+?, last_run=?",
                (fields["urn"], fields["kind"], fields.get("tokens_in", 0), fields.get("tokens_out", 0),
                 fields.get("cost_usd", 0), fields.get("duration_sec", 0), fields["last_run"],
                 fields.get("tokens_in", 0), fields.get("tokens_out", 0), fields.get("cost_usd", 0),
                 fields.get("duration_sec", 0), fields["last_run"]),
            )
            await db.commit()

    async def create_schedule(self, **fields: Any) -> None:
        async with self.connection() as db:
            await db.execute(
                "INSERT INTO scheduled_jobs (id, name, created_by_urn, interval_seconds, job_template, enabled, "
                "last_fired_at, next_fire_at, created_at) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)",
                (fields["id"], fields["name"], fields["created_by_urn"], fields["interval_seconds"],
                 fields["job_template_json"], fields["enabled"], fields["next_fire_at"], fields["created_at"]),
            )
            await db.commit()

    async def list_schedules(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        async with self.connection() as db:
            async with db.execute(
                "SELECT id, name, created_by_urn, interval_seconds, enabled, last_fired_at, next_fire_at, fire_count, created_at "
                "FROM scheduled_jobs ORDER BY next_fire_at ASC"
            ) as cur:
                async for r in cur:
                    rows.append({"id": r[0], "name": r[1], "created_by_urn": r[2], "interval_seconds": r[3],
                                 "enabled": bool(r[4]), "last_fired_at": r[5], "next_fire_at": r[6],
                                 "fire_count": r[7], "created_at": r[8]})
        return rows
