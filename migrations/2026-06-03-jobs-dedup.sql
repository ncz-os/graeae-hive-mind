-- Migration: in-flight job dedup (active-only uniqueness on dedup_hash)
-- 2026-06-03. Designed from Codex adversarial review. APPLY ORDER:
--   1. apply this DDL to the live DB
--   2. deploy the create_job dedup patch (see 2026-06-03-jobs-dedup.md)
--   3. restart graeae-hive
-- The DDL is additive + reversible. The function-based unique index enforces
-- "at most one ACTIVE job per dedup_hash" — terminal rows (done/failed/
-- cancelled/dead-letter) and NULL hashes are NOT indexed, so deliberate reruns
-- are never blocked. This is what closes the concurrent-submit race that a
-- SELECT-then-INSERT coalesce cannot.

-- ── Oracle (ORCLPDB1) ────────────────────────────────────────────────────
ALTER TABLE jobs ADD (dedup_hash VARCHAR2(64));

CREATE UNIQUE INDEX jobs_active_dedup_uq ON jobs (
  CASE WHEN status IN ('queued', 'offered', 'claimed', 'running')
       THEN dedup_hash END
);

-- Rollback:
--   DROP INDEX jobs_active_dedup_uq;
--   ALTER TABLE jobs DROP COLUMN dedup_hash;

-- ── SQLite (dev / fallback) equivalent ───────────────────────────────────
-- ALTER TABLE jobs ADD COLUMN dedup_hash TEXT;
-- CREATE UNIQUE INDEX jobs_active_dedup_uq ON jobs (dedup_hash)
--   WHERE dedup_hash IS NOT NULL
--     AND status IN ('queued', 'offered', 'claimed', 'running');
