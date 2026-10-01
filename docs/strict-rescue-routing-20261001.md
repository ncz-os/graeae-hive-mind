# Strict rescue routing, 2026-10-01

Rescue jobs opt in with both `hard:strict-rescue-routes` and
`hard:verified-git-delivery`. A zeroclaw worker advertises the strict capability
only when its installed zoder CLI exposes `--allowed-routes`. On a strict claim,
the worker pins the author to `tydeus-coder=nemotron35`, the first reviewer to
`tydeus-reviewer=qwen38`, and permits NVIDIA EIH Ultra 550B as the other exact
route. The zoder CLI filters resolution and checks the actual provider and
model immediately before dispatch. A disallowed pin fails closed.

The worker runs `zoder loop` with `--check` for verified Git delivery and
`--require-approval`. Its registration uses a stable per-host slot instance.
The separate Hive job-description reviewer must honor the same capability:
FleetOps `fix/strict-hive-review-routes-20261001` at `58ca829` routes strict
jobs through qwen38 and then NVIDIA EIH. Existing MiniMax review records remain
in the audit history but do not satisfy a strict job's review requirement.

The cancelled legacy job `01a0f537-4ee2-7534-b0dc-f3a982c14a77` has a clean,
preserved workspace at `3549bab5e87f57b06d96cd056ec67b6a0a6b7a6b` on
`fedora` (`192.168.207.88`). Replacement
`01a0f840-07ca-7b54-82a5-b6cd82f5e969` was claimed with both hard
capabilities. Its live command line contained the exact route allowlist,
`--check`, and `--require-approval`; the author connected to TYDEUS `:8002`.
Completion and remote Git delivery remain separate gates.

Fourteen other jobs remain queued with `eligible_hosts=["rescue-guard-hold"]`.
Their records lack `hard:strict-rescue-routes`, and one is RiskyEats. Do not
remove that hold on the existing records: a host-only change would allow a
MiniMax route. Submit corrected replacements with the hard capability only
after the selected workers have a reviewed CLI, worker script, reachable local
and EIH routes, and an eligible role/load. Keep the RiskyEats job held until
its separate production scope is reviewed. Use `max_retries=0` while the
Hive inline failed-PATCH retry path is not proven fail-closed.
