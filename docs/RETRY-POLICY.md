# Automatic retry policy

Automatic retries use `POST /v1/jobs/{id}/auto-retry` with an admin bus token.
The endpoint reads the stored job under a database row lock, checks the shared
policy, increments `retry_count`, and clears the previous claim. It preserves
`max_retries` and the previous result. Ordinary PATCH cannot reopen terminal jobs.

Only failed jobs with a valid positive integer budget and remaining retries qualify.
Missing or malformed policy, explicit holds, superseded work, manual review and
`retryable: false` block retry. Build jobs require literal `retryable: true`.
The restore process also applies its existing age, local retry cap and backoff.
It fetches results because the hold and build policies depend on that evidence;
other list clients can retain `include_result=false`.

Deploy `retry_policy.py` alongside both `agent_bus.py` and the standalone
`hive-auto-restore.py`. Preserve the existing `hive.auth` import path and supply
`HIVE_BUS_TOKEN` for reads and `HIVE_BUS_ADMIN_TOKEN` for retries. Transport failures
remain failures; a stored-policy rejection is a skipped job, not a successful retry.

Do not run the older `doctor_requeue_failed.py` cron. It uses the administrative
force-requeue endpoint, bypasses automatic retry policy and resets the retry count.
On PYTHIA that separate cron was disabled after disposable fixtures demonstrated
that it revived held and zero-budget work even while the restore timer was stopped.
Keep it disabled; the guarded restore process is the supported automatic path.

The legacy deployment branch and canonical public master have unrelated histories.
This port preserves canonical authentication and API projection behavior. It does
not authorize replacing the live deployment with the entire canonical source tree:
fleet authentication and the other legacy worker changes require separate reconciliation.

Verification includes SQLite API tests for policy rejection, authentication,
projection, and single budget consumption. The same locked endpoint implementation
was exercised against live Oracle using disposable records, including concurrent
requests returning one success and one conflict. DB2 locking is implemented but has
not been exercised against a live DB2 server in this change.
