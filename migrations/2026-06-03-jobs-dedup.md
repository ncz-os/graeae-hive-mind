# Migration: in-flight job dedup

**Status:** ready for fleet-ops. NOT applied. Designed from a Codex adversarial
review of the live `create_job` path + the existing `hive_cache` layer.

## Why
`hive_cache` (`cache_key_for`/`cache_lookup`) only short-circuits *after* a job
reaches `done` (cache is written at the done-PATCH, ~`agent_bus.py:2467`). A
barrage of N identical submits that arrives *before* the first completes all
miss the cache and enqueue N rows — the observed 129-duplicate-SHA incident.
This adds submit-time **in-flight coalescing** for ACTIVE jobs; `hive_cache`
still handles completed-result reuse. The two are complementary.

## Design (Codex-reviewed)
- DB-enforced **active-only uniqueness** (see `.sql`), not a SELECT-then-INSERT
  coalesce — Oracle's `BEGIN` is a no-op (`agent_bus.py:204`), so SELECT-coalesce
  races (two submits both miss, both insert). The function-based unique index
  closes the race in the DB.
- Terminal rows keep `dedup_hash` but are NOT indexed → reruns never blocked.
- Escape hatch for a deliberate forced rerun: pass a unique `idempotency_key`.

## Apply order
1. Run `2026-06-03-jobs-dedup.sql` against the live DB.
2. Apply the `agent_bus.py` patch below.
3. `sudo systemctl restart graeae-hive`.
4. Verify: submit two identical jobs back-to-back → second returns the first's id
   (coalesced); `curl :5005/v1/jobs` shows one active row, not two.

## agent_bus.py patch

**(a) JobCreate — add the rerun escape (`class JobCreate`, ~L1122):**
```python
    idempotency_key: Optional[str] = None   # explicit forced-rerun escape; unique => never coalesced
```

**(b) import the dedup helper (top of file):**
```python
import queue_logic
```

**(c) create_job — compute the hash + coalesce on collision.**
Right before the `queued` INSERT (~L2011), compute the hash. Fold cost-tier +
required-capabilities into the scope (Codex finding #4) so jobs that differ by
budget/caps are NOT merged:
```python
    dedup_scope_version = f"{max_cost_tier}|{','.join(sorted(req.required_capabilities or []))}"
    dedup_hash = queue_logic.dedup_key(
        tenant="_default",                      # single-tenant hive today; thread real tenant when multi-tenant lands
        kind=req.kind,
        description=req.description,
        version=dedup_scope_version,
        idempotency_key=req.idempotency_key,
    )
```
Add `dedup_hash` to the INSERT column list + value bind. Then wrap the INSERT so
a unique-violation returns the existing ACTIVE job instead of erroring:
```python
    try:
        await db.execute(<existing INSERT ... + dedup_hash>, (... , dedup_hash))
        await db.commit()
    except Exception as e:                      # sqlite IntegrityError / oracledb DPY/ORA-00001
        if not _is_unique_violation(e):
            raise
        async with db.execute(
            "SELECT id FROM jobs WHERE dedup_hash=? "
            "AND status IN ('queued','offered','claimed','running') "
            "ORDER BY started_at ASC LIMIT 1",
            (dedup_hash,),
        ) as cur:
            row = await cur.fetchone()
        if row:
            return {"ok": True, "job_id": row[0], "coalesced": True}
        raise                                   # collision but no active row (rare) -> surface
```
`_is_unique_violation(e)`: match sqlite `IntegrityError` ("UNIQUE constraint
failed") and oracledb `ORA-00001`. (LIMIT 1 → Oracle uses `FETCH FIRST 1 ROWS
ONLY`; mirror the `?`-rewrite the bus already does for param style.)

## Notes
- `queue_logic.dedup_key` + `match` already committed (bus `457cd7c`), 17 tests.
- Do NOT add a plain `UNIQUE(dedup_hash)` — it would block reruns on terminal
  rows forever (Codex finding #5). Active-only function index only.
- Tenant scope is `_default` until multi-tenancy lands; the duplicate source can
  fan across orchestrators, so do not key tenant on submitter_urn alone.
