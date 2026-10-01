# Completion integrity repair

## Verified causes

- Worker invocations omitted `--approve all`; zoder defaults to read-only tool
  approval. Autonomous implementation requires explicit write/shell approval.
- Bare `codex exec` inherited read-only sandbox configuration on HYDRA. Its
  actual canary reported Git, docs, network and SSH access denied, then exited 0.
  Trusted fleet coding workers now select `CODEX_SANDBOX` explicitly (default
  `danger-full-access`) and noninteractive approval policy; restricted hosts can
  select another Codex sandbox. Delivery proof still determines completion.

- Worker success was the process return code; it did not inspect Git delivery.
- The bus checked a worker-supplied result kind absent from real worker results,
  and omitted `build:zeroclaw` from the delivery contract.
- An HTTP 200 response let older workers delete successful workspaces, including
  uncommitted changes. Failed workspaces were already preserved.
- Cached repository results could bypass new execution and current evidence.
- zoder could switch from task edits to a historical branch diff after commits,
  and permissive comment/neutral reviews did not establish explicit approval.
- Retried descriptions inherited obsolete commit/push prohibitions. Only
  explicit job-specific restrictions should override normal branch delivery.

## Repair contract

Workers collect full commit IDs, changed paths, clean status, configured upstream
and the actual remote branch SHA. An unpushed or empty delivery is `needs-review`,
not `done`. Checkouts remain available for recovery after every outcome. The bus
uses its stored job kind and rejects missing proof with HTTP 409, preventing old
workers from treating rejection as successful recording and deleting work.

Repository task results are not reused from the result cache. New zoder workers
require explicit review approval. Job IDs enter the subprocess environment and,
when MNEMOS is configured, prompts describe the ledger lookup/write commands.
Never include credentials in ledger content.

A legitimate no-change audit or already-delivered retry needs an evidence review;
creating an empty or unrelated commit is not a valid workaround. Compare remote
content before resubmission: different commit IDs can have identical trees.

## Validation and rollout

Regression tests exercise real local Git repositories and remote pushes, worker
workspace retention, and the actual HTTP update route with SQLite. Nine unit tests
and 18 queue-logic checks passed before rollout. zoder has separate cumulative
review, strict verdict, ignored-path and MNEMOS transport tests.

Deploy the bus guard first to protect checkouts from existing workers. Install the
matching zoder binary before workers that use `--require-approval`. Restart workers
only after preserving active work. Source/test success does not establish fleet
rollout or completion of the jobs' hardware and browser acceptance requirements.
