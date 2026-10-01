# Zeroclaw Hive fanout

`ZEROCLAW_FANOUT` in `hosts/<host>/hive-worker.conf` is the total number of
zeroclaw worker processes on that host. The existing
`hive-worker-zeroclaw.service` is slot 1; `hive-worker-zeroclaw@2.service`
through `@N.service` are additive slots. Each process registers a distinct
Hive agent URN and uses the job UUID as its workspace directory. Root hosts
use the corresponding `hive-worker-zeroclaw-root@.service` source template;
the deployed unit name is the same `hive-worker-zeroclaw@N.service`.

The periodic deploy installs the configured worker script and units without
restarting an active worker. A worker checks the installed source before each
claim and exits for systemd to relaunch it after its current job completes.
It defers this exit while a model engine or other process remains in its
service cgroup. Unit-only changes update the source timestamp to trigger the
same drain. `--fanout-only` continues to defer singleton enable/start while
adding slots. Workers started before the `HIVE_WORKER_AUTO_RESTART=1` unit
setting need one explicit idle restart during the first migration; the timer
does not interrupt them.

New source-delivery jobs should require
`hard:verified-git-delivery`. Worker version `hive_worker.py/2` advertises that
capability; earlier processes cannot claim such jobs while hosts migrate.

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
