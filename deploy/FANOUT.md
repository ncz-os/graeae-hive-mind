# Zeroclaw Hive fanout

`ZEROCLAW_FANOUT` in `hosts/<host>/hive-worker.conf` is the total number of
zeroclaw worker processes on that host. The existing
`hive-worker-zeroclaw.service` is slot 1; `hive-worker-zeroclaw@2.service`
through `@N.service` are additive slots. Each process registers a distinct
Hive agent URN and uses the job UUID as its workspace directory. Root hosts
use the corresponding `hive-worker-zeroclaw-root@.service` source template;
the deployed unit name is the same `hive-worker-zeroclaw@N.service`.

Run `hive-worker-deploy.sh --fanout-only` when adding slots while a singleton
worker is busy. This installs the configured worker script and units but does
not restart the singleton. The regular reconcile maintains the slots afterward.

Four processes provide claim capacity, not permission to edit overlapping
source. Submit concurrent code jobs on isolated branches with separate file
scopes; sequence jobs that may edit the same files or branch. Keep ARGOS's
production load and RiskyEats pipeline limits in force when assigning jobs.
MINOS already runs five additional legacy zeroclaw agents, so its thin worker
remains at one slot; this preserves its existing six-agent capacity.

Verify capacity with `/v1/agents?status=online`: count distinct online
`zeroclaw` URNs for each host. A claimed job occupies one process; a host can
claim another job while it has an idle zeroclaw instance.

Failed jobs keep their per-job checkout for rescue, even when
`KEEP_WORKSPACE=0`. A repeat claim with the same job ID fails with
`workspace_exists_needs_rescue` rather than replacing the checkout. Inspect
and commit or export any edits before manually removing that workspace.
Successful jobs are cleaned only after Hive records the terminal result.
