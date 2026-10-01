# Strict rescue routing, 2026-10-01

Rescue jobs opt in with `hard:strict-rescue-routes` and
`hard:verified-git-delivery`. The worker advertises the strict capability only
when zoder exposes all three role flags and the installed FleetOps readiness
validator succeeds. Each strict dispatch validates readiness again.

The author is pinned with `-m qwen38` to the exact pair
`tydeus-coder=qwen38` (TYDEUS `:8006`). This explicit model pin is a singleton
author chain. The primary reviewer is the validated CERBERUS Gemma 4 31B wire
model; the only secondary candidates are the manifest's verified EIH pool.
`--allowed-routes` contains that union; `--reviewer-allowed-routes` excludes the
author; `--reviewer-pool-routes` contains only EIH pairs. Zoder ranks eligible
pool members using its health/latency and cooldown state. No unknown or stale
pool candidate is admitted. There is no global `--no-fallback`, which would
also disable the authorized review fallback. `--check` for verified Git
delivery and `--require-approval` remain mandatory.

## Readiness contract and installation boundary

- `HIVE_STRICT_ROLE_CONFIG` defaults to `~/.zoder/role-readiness.json`.
- `HIVE_STRICT_ROLE_VALIDATOR` defaults to `~/.zoder/role_readiness.py`.
- `HIVE_STRICT_ROLE_VALIDATOR_SHA256` must pin the reviewed helper's exact bytes.

FleetOps branch `fix/final-role-readiness-20261001`, commit `321a436aa105b8762e04179630c07abb4344739e`,
provides the standalone validator. Its reviewed SHA256 is
`b1b0adcf6e82848fa74ec9ce8ef0c29fc45f9b4bb6b9faceb0a4dede17f5ff1f`.
The worker executes the verified bytes through Python stdin, avoiding a
replacement race between hashing and execution. The validator checks fresh
owner-approved evidence, boot persistence, sole Qwen service on TYDEUS,
actual Gemma 31B weights/parameter provenance and calibration, the verified EIH
pool (at least 31B parameters per verified member), and the PYTHIA embedding gateway with both HYDRA and ACHILLES backends.
The worker independently constrains the returned route identities and syntax.
Missing, stale, malformed, failed or hash-mismatched evidence blocks strict
registration/dispatch; ordinary worker registration remains available.

The known `gemma4-consult` 12B service is not the required 31B reviewer.
Source delivery does not activate this policy. Deployment requires the final
CLI, validator, manifest, provider configuration and host-owner readiness
gates. No worker services are restarted as part of this source change.

## Historical rescue evidence (previous policy)

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
