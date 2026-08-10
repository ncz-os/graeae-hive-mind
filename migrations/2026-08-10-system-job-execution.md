# System Job Execution Migration

Apply `2026-08-10-system-job-execution.sql` to Oracle before deploying the
matching service and system-watcher build. The service requires
`HIVE_JOBS.EXECUTION_SPEC`; it will refuse Oracle startup with an actionable
error when the column is absent. SQLite is migrated idempotently at startup.

The new `JobCreate.execution` field is optional, so existing callers and jobs
remain valid. Active system watchers only claim jobs with a structured
`execution.type` of `local-process` and an allowed job kind. The bus injects
`hard:local-process` into these jobs so generic wildcard workers cannot claim
them. A process request has this shape:

```json
{
  "execution": {
    "type": "local-process",
    "argv": ["make", "release"],
    "cwd": "project-a",
    "env": {"BUILD_TYPE": "release"},
    "timeout_seconds": 3600
  }
}
```

`argv` is executed directly without a shell. `cwd` must resolve beneath
`JOB_WORK_ROOT`. The executable basename and job-provided environment names
must be present in `SYSTEM_WATCHER_ALLOWED_EXECUTABLES` and
`SYSTEM_WATCHER_ALLOWED_JOB_ENV`, respectively. Child processes receive only
the parent environment names listed in `SYSTEM_WATCHER_PASSTHROUGH_ENV`; Hive
authentication tokens are not passed through by default.
