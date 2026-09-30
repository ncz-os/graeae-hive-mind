# Codex orchestrator role

Hive keeps ordinary `codex` and `codex-cli` runtimes claim-only. A session
that coordinates workers registers with both `runtime` and `kind` set to
`codex-orchestrator`. The distinct runtime can submit jobs through the existing
orchestrator gate without giving submission rights to Codex worker processes.

The role was added for the 2026-09-30 NCZ-OS handoff from Claude to Codex.
The live bus runs `/srv/agent-bus/agent_bus.py` on PYTHIA; its deployment source
is the ARGONAS `graeae-hive` branch
`wip/studio/2026-06-15-register-dedup`. The corresponding change is also
committed to GitLab `ncz-os/graeae-hive-mind` on
`fix/codex-orchestrator-role-2026-09-30`. Keep the role change on both sources
when reconciling the live branch with GitLab master.
