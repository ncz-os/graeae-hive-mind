#!/usr/bin/env python3
"""
GRAEAE Hive Mind -- thin claim/execute worker.

Replaces the legacy zc-gateway/zc-worker@ (zeroclaw-stack container) +
zc_oneshot.py/zc_native_build.py build-harness path. Per GRAEAE architecture
consult 2026-09-14 (7-muse consensus, winner MiniMax-M3 0.95): the hive's
job-claim path should be a THIN wrapper that shells out to `zoder`/`codex`
and lets THEM own model selection via their own per-host config -- not a
second independent routing layer (KNEMON, or an in-process build harness).

This script claims exactly ONE kind of job (--kind zeroclaw|codex), resolves
the job's repo to a fresh per-job clone (never a shared mutable workspace --
that was zc_oneshot's bug, fixed here the same way zc_native_build.py fixed
it), runs the driving tool inside it, and reports the result. No model
selection happens here: zeroclaw jobs get `zoder loop`, which reads that
host's own ~/.zoder/config.toml escalation ladder; codex jobs always get
`codex exec -m $CODEX_MODEL` -- a single fixed rung set per host via env,
NOT negotiated per job. required_capabilities is not consulted for model
choice; if per-job model escalation is wanted later, it needs to be added
here explicitly, not assumed.

Env:
  HIVE_URL            default http://192.168.207.67:5005
  AGENT_HOST          default hostname -s
  WORKER_KIND         "zeroclaw" or "codex" (required)
  POLL_INTERVAL       seconds between /v1/jobs/next polls (default 5)
  HEARTBEAT_INTERVAL  seconds (default 20)
  JOB_TIMEOUT         seconds, hard kill of the driving subprocess (default 7200)
  WORKSPACE_ROOT       default ~/hive-workspaces
  KEEP_WORKSPACE      if "1", don't rm -rf the per-job clone after (debug)
  CODEX_MODEL         default gpt-5.3-codex-spark (escalation ladder default rung)
"""
from __future__ import annotations
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

def _find_driver(name: str) -> str:
    """Resolve a driver binary's absolute path at import time. A bare PATH
    lookup is not enough: systemd services and non-interactive SSH sessions
    don't source the interactive shell's PATH, so `zoder`/`codex` installed
    under ~/.local/bin (the common fleet install location) silently
    FileNotFoundError otherwise -- caught deploying to PROTEUS, 2026-09-14,
    where `which zoder` in a plain ssh command found nothing despite the
    binary being present and working interactively."""
    found = shutil.which(name)
    if found:
        return found
    for candidate in (
        os.path.expanduser(f"~/.local/bin/{name}"),
        f"/usr/local/bin/{name}",
        f"/opt/homebrew/bin/{name}",
    ):
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return name  # let it fail loudly with FileNotFoundError at exec time


HIVE_URL = os.environ.get("HIVE_URL", "http://192.168.207.67:5005")
AGENT_HOST = os.environ.get("AGENT_HOST", socket.gethostname().split(".")[0])
WORKER_KIND = os.environ.get("WORKER_KIND", "").strip().lower()
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "5"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "20"))
JOB_TIMEOUT = int(os.environ.get("JOB_TIMEOUT", "7200"))
WORKSPACE_ROOT = os.path.expanduser(os.environ.get("WORKSPACE_ROOT", "~/hive-workspaces"))
KEEP_WORKSPACE = os.environ.get("KEEP_WORKSPACE", "0") == "1"
CODEX_MODEL = os.environ.get("CODEX_MODEL", "gpt-5.3-codex-spark")

if WORKER_KIND not in ("zeroclaw", "codex"):
    print("[hive-worker] WORKER_KIND must be 'zeroclaw' or 'codex'", file=sys.stderr)
    sys.exit(1)

# ARGONAS canonical bare repos. Extend as needed -- a job whose `repo` field
# isn't a URL and isn't in this map gets DECLINED, never guessed at.
REPO_MAP = {
    "riskyeats":            "ssh://root@192.168.207.101/mnt/datapool/git/riskyeats.git",
    "zeroclaw":             "ssh://root@192.168.207.101/mnt/datapool/git/zeroclaw.git",
    "mnemos":               "ssh://root@192.168.207.101/mnt/datapool/git/mnemos.git",
    "cix-installer":        "ssh://root@192.168.207.101/mnt/datapool/git/cix-installer.git",
    "meta-cix":             "ssh://root@192.168.207.101/mnt/datapool/git/meta-cix.git",
    "ic-engine":            "ssh://root@192.168.207.101/mnt/datapool/git/ic-engine.git",
    "florida-licenses":     "ssh://root@192.168.207.101/mnt/datapool/git/florida-licenses.git",
    "graeae-hive":          "ssh://root@192.168.207.101/mnt/datapool/git/graeae-hive.git",
    "zoder":                "https://gitlab.com/ncz-os/zoder.git",
    "artistpack":           "https://gitlab.com/ncz-os/artistpack.git",
}

_urn: str = ""
_running = True
_current_job_id: str | None = None


def _signal(signum, frame):
    global _running
    print(f"[hive-worker:{WORKER_KIND}] signal {signum} -- shutting down", flush=True)
    _running = False


signal.signal(signal.SIGTERM, _signal)
signal.signal(signal.SIGINT, _signal)


def _http(method: str, path: str, body: dict | None = None, timeout: float = 15) -> tuple[int, dict | None]:
    url = f"{HIVE_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except Exception as e:
        print(f"[hive-worker:{WORKER_KIND}] http error {method} {path}: {e}", flush=True)
        return 0, None


def register() -> None:
    global _urn
    caps = ["python", "git", "bash", "linux-compile", WORKER_KIND]
    body = {
        "runtime": WORKER_KIND,
        "kind": WORKER_KIND,
        "host": AGENT_HOST,
        "pid": os.getpid(),
        "model": "zoder" if WORKER_KIND == "zeroclaw" else CODEX_MODEL,
        "provider": "local",
        "autonomy_level": "autonomous",
        "capabilities": caps,
        "version": "hive_worker.py/1",
        "metadata": {"driver": WORKER_KIND, "workspace_root": WORKSPACE_ROOT},
    }
    code, resp = _http("POST", "/v1/agents/register", body)
    if code == 200 and resp:
        _urn = resp["urn"]
        print(f"[hive-worker:{WORKER_KIND}] registered urn={_urn}", flush=True)
    else:
        print(f"[hive-worker:{WORKER_KIND}] register failed code={code} resp={resp}", flush=True)
        sys.exit(1)


def heartbeat_loop() -> None:
    while _running:
        time.sleep(HEARTBEAT_INTERVAL)
        if not _urn:
            continue
        _http("POST", "/v1/agents/heartbeat", {
            "urn": _urn,
            "metadata": {"current_job": _current_job_id},
        }, timeout=10)


# Jobs are submitted only by registered orchestrator agents (bus-enforced,
# require_orchestrator_submitter), so this isn't an adversarial-multi-tenant
# boundary -- but a job's `repo` field still shouldn't be able to point git
# (which inherits this host's SSH agent / credential helpers) at an arbitrary
# host. Only resolve against the known-repo map; never trust a bare URL from
# the job (codex review finding, 2026-09-14).
def resolve_repo_url(job: dict) -> str | None:
    repo = (job.get("repo") or "").strip()
    return REPO_MAP.get(repo) if repo else None


# Bus job ids are UUIDv7 (see agent_bus.py uuidv7()). Enforced here because
# job_id is used as a path component below -- an unvalidated id could
# traverse out of WORKSPACE_ROOT via "../" or an absolute path (codex review
# finding, 2026-09-14).
_JOB_ID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def patch_job(job_id: str, status: str, result: dict) -> None:
    # PATCH /v1/jobs/{id} requires claimed_by to match the job's current
    # claimant (agent_bus.py: "job update requires claimed_by to match the
    # current claimant") -- omitting it 403s every single call (found live
    # on PROTEUS, 2026-09-14: every job ended up stranded in 'claimed'
    # because this worker never sent it).
    code, _ = _http("PATCH", f"/v1/jobs/{job_id}",
                     {"status": status, "result": result, "claimed_by": _urn}, timeout=15)
    if code != 200:
        print(f"[hive-worker:{WORKER_KIND}] WARNING: patch({job_id}, {status}) got code={code} -- "
              f"job may be stranded in claimed state", flush=True)


def release_job(job_id: str, reason: str) -> None:
    # NOT /v1/jobs/{id}/release -- that endpoint is a completely different
    # feature (zc-gate's human-gated "Release & Push" PR workflow, expects a
    # preview bundle already on the job). The real way to decline a claimed
    # job back to the queue is a PATCH to status=queued (same claimed_by
    # requirement as patch_job above), matching the worker_error/note shape
    # observed from the bus's own existing workers (thrash-guard dead-letters
    # a job after 3 such declines).
    patch_job(job_id, "queued", {"worker_error": reason, "via": "hive_worker"})


def run_job(job: dict) -> None:
    """Guarantees a terminal disposition (patch or release) for every claimed
    job, even on an exception type we didn't anticipate (codex review
    finding, 2026-09-14: PermissionError/OSError etc previously escaped this
    function entirely, killing the daemon with the job left claimed
    forever)."""
    global _current_job_id
    job_id = job.get("id", "")
    _current_job_id = job_id
    try:
        _run_job_inner(job)
    except Exception as e:
        print(f"[hive-worker:{WORKER_KIND}] job {job_id}: unhandled exception: {e!r}", flush=True)
        if job_id:
            patch_job(job_id, "failed", {"via": "hive_worker", "error": f"unhandled: {e!r}"})
    finally:
        _current_job_id = None


def _run_job_inner(job: dict) -> None:
    job_id = job["id"]
    kind = job.get("kind", "")
    description = job.get("description", "")
    head_branch = job.get("head_branch")

    if not _JOB_ID_RE.match(job_id):
        print(f"[hive-worker:{WORKER_KIND}] job id {job_id!r} doesn't look like a UUID, refusing", flush=True)
        release_job(job_id, "malformed_job_id")
        return

    repo_url = resolve_repo_url(job)
    if not repo_url:
        print(f"[hive-worker:{WORKER_KIND}] job {job_id} kind={kind!r}: no resolvable repo, declining", flush=True)
        release_job(job_id, "no_repo_mapping")
        return

    workdir = os.path.join(WORKSPACE_ROOT, job_id)
    os.makedirs(WORKSPACE_ROOT, exist_ok=True)
    if os.path.exists(workdir):
        shutil.rmtree(workdir, ignore_errors=True)

    clone_cmd = ["git", "clone", "--quiet", repo_url, workdir]
    if head_branch:
        if head_branch.startswith("-"):
            print(f"[hive-worker:{WORKER_KIND}] job {job_id}: refusing suspicious head_branch {head_branch!r}", flush=True)
            release_job(job_id, "invalid_head_branch")
            return
        clone_cmd[3:3] = ["--branch", head_branch]
    # root@ARGONAS git ops can hit the documented pubkey-exhaustion class of
    # failure (kernel-build-checklist.md #0f) on a host whose SSH agent
    # hasn't got root's key trusted -- found live on PROTEUS, 2026-09-14
    # ("returned non-zero exit status 128" on a plain ssh:// clone). Fall
    # back to password auth via GIT_FLEET_SSH_PASSWORD if the host provides
    # it (systemd EnvironmentFile, never hardcoded/committed here).
    clone_env = os.environ.copy()
    fleet_pw = os.environ.get("GIT_FLEET_SSH_PASSWORD")
    if fleet_pw and repo_url.startswith("ssh://root@"):
        clone_env["GIT_SSH_COMMAND"] = (
            f"sshpass -p {fleet_pw!r} ssh -o PubkeyAuthentication=no -o StrictHostKeyChecking=no"
        )
    try:
        subprocess.run(clone_cmd, check=True, timeout=300, env=clone_env,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except Exception as e:
        print(f"[hive-worker:{WORKER_KIND}] job {job_id}: clone failed: {e}", flush=True)
        patch_job(job_id, "failed", {"via": "hive_worker", "error": f"clone_failed: {e}"})
        shutil.rmtree(workdir, ignore_errors=True)
        return

    if WORKER_KIND == "zeroclaw":
        cmd = [_find_driver("zoder"), "loop", description, "--agent-timeout", str(JOB_TIMEOUT)]
    else:
        cmd = [_find_driver("codex"), "exec", "--skip-git-repo-check", "-m", CODEX_MODEL, description]

    print(f"[hive-worker:{WORKER_KIND}] job {job_id}: running {' '.join(cmd[:3])}...", flush=True)
    t0 = time.time()
    try:
        # start_new_session=True puts the child in its own process group so a
        # timeout kill takes any grandchildren zoder/codex spawn (build/test
        # subprocesses) with it -- subprocess.run's own TimeoutExpired only
        # kills the direct child, leaving those orphaned otherwise.
        # communicate() buffers the whole run in memory before truncating to
        # the last 4000 chars -- unbounded for a pathological job (codex
        # review, Low). Accepted for v1: jobs come only from registered
        # orchestrators on the LAN, not an adversarial source; revisit with a
        # streaming/size-capped reader if a real job ever produces enough
        # output to matter.
        proc = subprocess.Popen(cmd, cwd=workdir, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, text=True, start_new_session=True)
        try:
            stdout, _ = proc.communicate(timeout=JOB_TIMEOUT)
            exit_code = proc.returncode
            tail = stdout[-4000:] if stdout else ""
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            stdout, _ = proc.communicate()
            exit_code = -1
            tail = (stdout or "")[-4000:]
            print(f"[hive-worker:{WORKER_KIND}] job {job_id}: TIMEOUT after {JOB_TIMEOUT}s, process group killed", flush=True)
    except FileNotFoundError as e:
        patch_job(job_id, "failed", {"via": "hive_worker", "error": f"driver_not_found: {e}"})
        if not KEEP_WORKSPACE:
            shutil.rmtree(workdir, ignore_errors=True)
        return

    elapsed = round(time.time() - t0, 1)
    result = {"via": "hive_worker", "driver": WORKER_KIND, "exit_code": exit_code,
              "elapsed_s": elapsed, "tail": tail}
    status = "done" if exit_code == 0 else "failed"
    print(f"[hive-worker:{WORKER_KIND}] job {job_id}: {status} exit={exit_code} elapsed={elapsed}s", flush=True)
    patch_job(job_id, status, result)

    if not KEEP_WORKSPACE:
        shutil.rmtree(workdir, ignore_errors=True)


def main() -> None:
    register()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    print(f"[hive-worker:{WORKER_KIND}] polling {HIVE_URL} as {_urn}", flush=True)
    while _running:
        code, job = _http("POST", f"/v1/jobs/next?agent_urn={_urn}", timeout=10)
        if code == 200 and job:
            run_job(job)
            continue
        if code == 409:
            # agent status not online/idle -- re-register and back off
            print(f"[hive-worker:{WORKER_KIND}] claim 409, re-registering", flush=True)
            register()
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
