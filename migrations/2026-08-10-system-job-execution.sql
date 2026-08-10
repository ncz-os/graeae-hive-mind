-- F8: structured local-process execution contract for system-watcher workers.
-- Apply this migration before deploying the matching agent_bus.py build.
-- It is idempotent for Oracle and SQLite and does not rewrite existing jobs.

-- Oracle (production)
DECLARE
  column_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO column_count
    FROM user_tab_columns
   WHERE table_name = 'HIVE_JOBS'
     AND column_name = 'EXECUTION_SPEC';
  IF column_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE HIVE_JOBS ADD (execution_spec CLOB)';
  END IF;
END;
/

-- SQLite (development / fallback)
-- SQLite does not support ADD COLUMN IF NOT EXISTS. The application lifespan
-- migration and SqliteHiveRepository.init() inspect PRAGMA table_info(jobs)
-- and run the following only when the column is absent:
-- ALTER TABLE jobs ADD COLUMN execution_spec TEXT;
