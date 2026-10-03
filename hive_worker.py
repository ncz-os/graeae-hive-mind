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
it), runs the driving tool inside it, and reports the result. Normal zeroclaw
jobs use the host's ~/.zoder/config.toml escalation ladder. Strict rescue
jobs pin an exact local/EIH route allowlist; codex jobs always get
`codex exec -m $CODEX_MODEL` -- a single fixed rung set per host via env,
NOT negotiated per job.

Env:
  HIVE_URL            default http://192.168.207.67:5005
  AGENT_HOST          default hostname -s
  HIVE_WORKER_SLOT    stable per-host worker slot (default 1)
  WORKER_KIND         "zeroclaw" or "codex" (required)
  POLL_INTERVAL       seconds between /v1/jobs/next polls (default 5)
  HEARTBEAT_INTERVAL  seconds (default 20)
  JOB_TIMEOUT         seconds, hard kill of the driving subprocess (default 7200)
  WORKSPACE_ROOT       default ~/hive-workspaces
  KEEP_WORKSPACE      if "1", don't rm -rf the per-job clone after (debug)
  CODEX_MODEL         default gpt-5.6-luna (see note below)
  CODEX_SANDBOX       default danger-full-access for trusted fleet coding jobs
  HIVE_GIT_AUTHOR_NAME   repo-local identity, default Jason Perlow
  HIVE_GIT_AUTHOR_EMAIL  repo-local identity, default jperlow@gmail.com

CODEX_MODEL note (2026-09-15): the escalation ladder's documented default
rung is gpt-5.3-codex-spark, but this account's live codex model catalog
(queried via chatgpt-api.com/backend-api/codex/models earlier this session)
does NOT serve a spark variant -- confirmed on codex-cli 0.144.1 AND after
upgrading to 0.153.3, same 400 both times: "The 'gpt-5.3-codex-spark' model
is not supported when using Codex with a ChatGPT account." Every codex-kind
hive job was failing on this, fleet-wide, regardless of host or codex
version. gpt-5.6-luna is the cheapest VALIDATED model actually served to
this account (~/.claude/rules/00-AGENT-DIRECTIVES.md escalation ladder) --
used as the default here until spark (or an equivalent low-cost rung)
actually appears in the live catalog.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path


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
        os.path.expanduser(f"~/.npm-global/bin/{name}"),
    ):
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return name  # let it fail loudly with FileNotFoundError at exec time


def _driver_supports_verified_delivery(driver: str) -> bool:
    """Require the CLI contract used by the delivery/completion gate.

    A binary that can start but lacks these flags would either fail every
    queued job or bypass the check, so it must not register as a verified
    delivery worker during a rolling fleet update.
    """
    if WORKER_KIND == "zeroclaw":
        command = [driver, "loop", "--help"]
        required = ("--require-approval", "--approve", "--check")
    else:
        command = [driver, "exec", "--help"]
        required = ("--sandbox",)
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    output = result.stdout or ""
    return result.returncode == 0 and all(flag in output for flag in required)


def load_strict_routes():
    """Consume the pinned FleetOps validator; never guess an unready role."""
    def require(cond, msg="strict role readiness unavailable"):
        if not cond:
            raise ValueError(msg)

    try:
        manifest = Path(
            os.environ.get("HIVE_STRICT_ROLE_CONFIG", "~/.zoder/role-readiness.json")
        ).expanduser()
        vpath = Path(
            os.environ.get("HIVE_STRICT_ROLE_VALIDATOR", "~/.zoder/role_readiness.py")
        ).expanduser()
        require(vpath.is_file(), "validator missing")
        with vpath.open("rb") as stream:
            raw = stream.read(65537)
        require(len(raw) <= 65536, "validator size")
        sha = os.environ.get("HIVE_STRICT_ROLE_VALIDATOR_SHA256", "")
        require(
            re.fullmatch(r"[0-9a-f]{64}", sha)
            and hashlib.sha256(raw).hexdigest() == sha,
            "sha mismatch",
        )
        proc = subprocess.run(
            [sys.executable, "-", "--manifest", str(manifest), "--json"],
            input=raw,
            capture_output=True,
            timeout=10,
            check=False,
        )
        require(proc.returncode == 0, "nonzero exit")
        out = proc.stdout
        require(len(out) <= 16384, "stdout too large")
        data = json.loads(out)
        require(isinstance(data, dict), "bad json")
        require(
            set(data.keys())
            == {"schema_version", "coder", "reviewer", "secondary", "eih_reviewers"},
            "keys mismatch",
        )
        require(
            type(data["schema_version"]) is int and data["schema_version"] == 1,
            "schema version",
        )
        coder = data["coder"]
        require(
            coder
            == {
                "provider_id": "tydeus-coder",
                "model_id": "qwen38",
                "base_url": "http://192.168.207.73:8006/v1",
            },
            "coder mismatch",
        )
        reviewer = data["reviewer"]
        require(isinstance(reviewer, dict), "reviewer dict")
        require(reviewer.get("provider_id") == "cerberus-reviewer", "reviewer provider")
        model = reviewer.get("model_id", "")
        require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", model), "reviewer model")
        secondary = data["secondary"]
        eih = data["eih_reviewers"]
        require(isinstance(eih, list) and 1 <= len(eih) <= 16, "eih count")
        seen = set()
        for r in eih:
            require(isinstance(r, dict), "eih dict")
            require(r.get("provider_id") == "nvidia-eih", "eih provider")
            require(
                r.get("base_url") == "https://integrate.api.nvidia.com/v1", "eih base"
            )
            m = r.get("model_id", "")
            require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", m), "eih model")
            key = (r["provider_id"], m)
            require(key not in seen, "eih dup")
            seen.add(key)
        require(secondary in eih, "secondary not in pool")
        author = "qwen38"
        rev_model = model
        author_pair = f"tydeus-coder={author}"
        gemma_pair = f"cerberus-reviewer={rev_model}"
        eih_pairs = [f"nvidia-eih={r['model_id']}" for r in eih]
        allowed = ",".join([author_pair, gemma_pair] + eih_pairs)
        rev_allowed = ",".join([gemma_pair] + eih_pairs)
        rev_pool = ",".join(eih_pairs)
        return {
            "author": author,
            "reviewer": rev_model,
            "allowed_routes": allowed,
            "reviewer_allowed_routes": rev_allowed,
            "reviewer_pool_routes": rev_pool,
        }
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError):
        raise ValueError("strict role readiness unavailable") from None


def _driver_supports_strict_routes(driver: str) -> bool:
    try:
        result = subprocess.run(
            [driver, "loop", "--help"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    out = result.stdout or ""
    flags = set(re.findall(r"--[a-z][a-z0-9-]*", out))
    return result.returncode == 0 and all(
        f in flags
        for f in [
            "--allowed-routes",
            "--reviewer-allowed-routes",
            "--reviewer-pool-routes",
        ]
    )


HIVE_URL = os.environ.get("HIVE_URL", "http://192.168.207.67:5005")
AGENT_HOST = os.environ.get("AGENT_HOST", socket.gethostname().split(".")[0])
WORKER_KIND = os.environ.get("WORKER_KIND", "").strip().lower()
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "5"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "20"))
JOB_TIMEOUT = int(os.environ.get("JOB_TIMEOUT", "7200"))
WORKSPACE_ROOT = os.path.expanduser(
    os.environ.get("WORKSPACE_ROOT", "~/hive-workspaces")
)
KEEP_WORKSPACE = os.environ.get("KEEP_WORKSPACE", "0") == "1"
CODEX_MODEL = os.environ.get("CODEX_MODEL", "gpt-5.6-luna")
CODEX_SANDBOX = os.environ.get("CODEX_SANDBOX", "danger-full-access")
AUTO_RESTART = os.environ.get("HIVE_WORKER_AUTO_RESTART", "0") == "1"
SOURCE_MTIME_NS = os.stat(__file__).st_mtime_ns
# Optional per-host override for zeroclaw jobs. Unset by default -- bare
# `zoder loop` (no --agent) is meant to defer entirely to that host's own
# ~/.zoder/config.toml escalation ladder, per this worker's original design.
# In practice, several hosts' zoder installs have real bugs/config drift in
# their AUTOMATIC (no-pin) routing path -- confirmed 2026-09-15 on both
# TYDEUS and CERBERUS, where bare invocation kept resolving to a stale/
# removed model (a provider-identity bug fixed in zoder itself, plus a
# separate automatic-routing-ignores-primary_model issue not yet fixed
# upstream) while the SAME model, invoked explicitly via --agent, dispatched
# correctly. Setting ZODER_AGENT sidesteps the broken automatic path
# entirely by pinning a known-good agent alias, the same way CODEX_MODEL
# already sidesteps codex's model-catalog drift above.
# HIVE_* overrides are set by host GitOps. EnvironmentFile values are applied
# after systemd Environment= lines, so a stale /etc/hive-worker-env ZODER_AGENT
# would otherwise override the host route drop-in.
ZODER_AGENT = os.environ.get("HIVE_ZODER_AGENT") or os.environ.get("ZODER_AGENT", "")
# `zoder loop`'s adversarial-review phase picks its own reviewer model
# independently of --agent -- confirmed 2026-09-16 on ARGOS: --agent minimax
# fixed authoring, but the review phase still auto-routed to "MiniMax-M2.7",
# a real but locally-unconfigured catalog entry (sambanova-hosted, no
# provider for it in this host's config.toml) pulled from zoder's global
# model_corpus.json, which is unfiltered by what a given host can actually
# reach. `--reviewer <MODEL>` pins it the same way ZODER_AGENT pins the
# author.
ZODER_REVIEWER = os.environ.get("HIVE_ZODER_REVIEWER") or os.environ.get(
    "ZODER_REVIEWER", ""
)

# Escalation ladder (operator 2026-10-03): a retried job moves up a rung instead
# of re-running the same model. Before this, every slot was pinned to `coder`
# (qwen38), so a job the local coder could not do failed on every attempt with
# "loop ended unresolved" and zero edits. Rung = the job's retry_count.
#   coder   -> local qwen38 on TYDEUS (or the host's HIVE_ZODER_AGENT)
#   flash   -> MiniMax-M3.1-Flash, balanced with deepseek-flash off-peak
#   minimax -> MiniMax-M3
# A job may start on a given rung with the capability `route:<alias>`.
ZODER_LADDER = [
    rung.strip()
    for rung in os.environ.get("HIVE_ZODER_LADDER", "coder,flash,minimax").split(",")
    if rung.strip()
]


def deepseek_peak(now_utc=None) -> bool:
    """DeepSeek bills 2x 01:00-04:00 and 06:00-10:00 UTC, Mon-Fri (UTC weekday)."""
    now_utc = now_utc or time.gmtime()
    if now_utc.tm_wday >= 5:
        return False
    return 1 <= now_utc.tm_hour < 4 or 6 <= now_utc.tm_hour < 10


def escalation_agent(job: dict, now_utc=None) -> str:
    """Return the zoder engine agent for this attempt of `job`, or "" to defer
    to the host's own routing (no ladder, no host pin).

    Contract: `route:<alias>` names the STARTING rung. Each retry climbs one
    rung from there and the top rung repeats once reached. An alias that is
    not on the ladder becomes a rung of its own ahead of the paid rungs (the
    local coder is skipped, since an explicit route outranks it)."""
    caps = job.get("required_capabilities") or []
    pinned = [
        c.split(":", 1)[1]
        for c in caps
        if isinstance(c, str) and c.startswith("route:")
    ]
    ladder = list(ZODER_LADDER)
    start = 0
    if pinned and pinned[0]:
        if pinned[0] in ladder:
            start = ladder.index(pinned[0])
        else:
            # An explicit route outranks the local coder, so a retry climbs to
            # the next paid rung rather than dropping back to qwen38.
            ladder = [pinned[0]] + [r for r in ladder if r != "coder"]
    if not ladder:
        return ZODER_AGENT
    try:
        retries = max(0, int(job.get("retry_count") or 0))
    except (TypeError, ValueError):
        retries = 0
    rung = ladder[min(start + retries, len(ladder) - 1)]
    if rung == "coder" and ZODER_AGENT:
        return ZODER_AGENT
    if rung == "flash":
        digest = hashlib.sha256(str(job.get("id", "")).encode()).digest()
        if digest[0] % 2 == 0 and not deepseek_peak(now_utc):
            return "deepseek"
        return "minimax_flash"
    return rung

# ARGONAS canonical bare repos. Extend as needed -- a job whose `repo` field
# isn't a URL and isn't in this map gets DECLINED, never guessed at.
REPO_MAP = {
    "riskyeats": "ssh://root@192.168.207.101/mnt/datapool/git/riskyeats.git",
    "zeroclaw": "ssh://root@192.168.207.101/mnt/datapool/git/zeroclaw.git",
    "mnemos": "ssh://root@192.168.207.101/mnt/datapool/git/mnemos.git",
    "cix-installer": "ssh://root@192.168.207.101/mnt/datapool/git/cix-installer.git",
    "meta-cix": "ssh://root@192.168.207.101/mnt/datapool/git/meta-cix.git",
    "ic-engine": "ssh://root@192.168.207.101/mnt/datapool/git/ic-engine.git",
    "florida-licenses": "ssh://root@192.168.207.101/mnt/datapool/git/florida-licenses.git",
    "graeae-hive": "ssh://root@192.168.207.101/mnt/datapool/git/graeae-hive.git",
    # GitLab's ncz-os/graeae-hive-mind is this same codebase; ARGONAS keeps it as
    # graeae-hive.git. Without this alias a job naming the GitLab repo was
    # declined with no_repo_mapping.
    "graeae-hive-mind": "ssh://root@192.168.207.101/mnt/datapool/git/graeae-hive.git",
    "zoder": "https://gitlab.com/ncz-os/zoder.git",
    "artistpack": "https://gitlab.com/ncz-os/artistpack.git",
    "ncz-screensavers": "ssh://root@192.168.207.101/mnt/datapool/git/ncz-screensavers.git",
    "wayshade": "ssh://root@192.168.207.101/mnt/datapool/git/wayshade.git",
    "singularity-plugins": "ssh://root@192.168.207.101/mnt/datapool/git/singularity-plugins.git",
    "amd64-kernel-t2": "ssh://root@192.168.207.101/mnt/datapool/git/amd64-kernel-t2.git",
    "fleet-ops": "ssh://root@192.168.207.101/mnt/datapool/git/fleet-ops.git",
}

_urn: str = ""
_running = True
_current_job_id: str | None = None
_claim_renewal_blocked_id: str | None = None


def _cgroup_has_other_processes() -> bool:
    """Keep a shared engine child alive even after this worker becomes idle."""
    try:
        with open("/proc/self/cgroup", encoding="ascii") as handle:
            group = next(
                line.split(":", 2)[2].strip()
                for line in handle
                if line.startswith("0::")
            )
        root = os.path.join("/sys/fs/cgroup", group.lstrip("/"))
        if not os.path.isdir(root):
            return True
        seen = False

        def _walk_error(error: OSError) -> None:
            raise error

        for directory, _, filenames in os.walk(root, onerror=_walk_error):
            if "cgroup.procs" not in filenames:
                continue
            seen = True
            with open(
                os.path.join(directory, "cgroup.procs"), encoding="ascii"
            ) as handle:
                if any(int(pid) != os.getpid() for pid in handle.read().split()):
                    return True
        return not seen
    except (OSError, StopIteration, ValueError):
        # Unknown cgroup layout: defer the restart rather than risk its children.
        return True


def restart_after_source_update() -> None:
    """Exit only between claims; systemd then loads the installed revision."""
    if not AUTO_RESTART:
        return
    try:
        changed = os.stat(__file__).st_mtime_ns != SOURCE_MTIME_NS
    except OSError:
        return
    if changed and not _cgroup_has_other_processes():
        print(
            f"[hive-worker:{WORKER_KIND}] source updated; restarting while idle",
            flush=True,
        )
        raise SystemExit(75)


def _signal(signum, frame):
    global _running
    print(f"[hive-worker:{WORKER_KIND}] signal {signum} -- shutting down", flush=True)
    _running = False


signal.signal(signal.SIGTERM, _signal)
signal.signal(signal.SIGINT, _signal)


def _http(
    method: str, path: str, body: dict | None = None, timeout: float = 15
) -> tuple[int, dict | None]:
    url = f"{HIVE_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method, headers={"content-type": "application/json"}
    )
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
        print(
            f"[hive-worker:{WORKER_KIND}] http error {method} {path}: {e}", flush=True
        )
        return 0, None


def register() -> None:
    global _urn
    driver = _find_driver("zoder" if WORKER_KIND == "zeroclaw" else "codex")
    if not os.path.isfile(driver) or not os.access(driver, os.X_OK):
        print(
            f"[hive-worker:{WORKER_KIND}] driver unavailable; refusing registration",
            flush=True,
        )
        raise SystemExit(2)
    if not _driver_supports_verified_delivery(driver):
        print(
            f"[hive-worker:{WORKER_KIND}] driver lacks verified-delivery CLI contract; refusing registration",
            flush=True,
        )
        raise SystemExit(2)
    caps = [
        "python",
        "git",
        "bash",
        "linux-compile",
        WORKER_KIND,
        "verified-git-delivery",
    ]
    if WORKER_KIND == "zeroclaw" and _driver_supports_strict_routes(driver):
        try:
            load_strict_routes()
            caps.append("strict-rescue-routes")
        except ValueError:
            pass
    body = {
        "runtime": WORKER_KIND,
        "kind": WORKER_KIND,
        "host": AGENT_HOST,
        "instance": f"hive-worker-{WORKER_KIND}-{os.environ.get('HIVE_WORKER_SLOT', '1')}",
        "pid": os.getpid(),
        "model": "zoder" if WORKER_KIND == "zeroclaw" else CODEX_MODEL,
        "provider": "local",
        "autonomy_level": "autonomous",
        "capabilities": caps,
        "version": "hive_worker.py/2",
        # The stable slot lets the bus replace stale registrations on restart;
        # job UUIDs still isolate workspaces.
        "metadata": {
            "driver": WORKER_KIND,
            "workspace_root": WORKSPACE_ROOT,
            "slot": os.environ.get("HIVE_WORKER_SLOT", "1"),
        },
    }
    code, resp = _http("POST", "/v1/agents/register", body)
    if code == 200 and resp:
        _urn = resp["urn"]
        print(f"[hive-worker:{WORKER_KIND}] registered urn={_urn}", flush=True)
    else:
        print(
            f"[hive-worker:{WORKER_KIND}] register failed code={code} resp={resp}",
            flush=True,
        )
        sys.exit(1)


def heartbeat_loop() -> None:
    global _claim_renewal_blocked_id
    while _running:
        time.sleep(HEARTBEAT_INTERVAL)
        if not _urn:
            continue
        _http(
            "POST",
            "/v1/agents/heartbeat",
            {
                "urn": _urn,
                "metadata": {"current_job": _current_job_id},
            },
            timeout=10,
        )
        job_id = _current_job_id
        if not job_id or job_id == _claim_renewal_blocked_id:
            continue
        # An agent heartbeat does not refresh the job's 30-minute lease.
        # A same-status PATCH does, without replacing an existing result.
        code, _ = _http(
            "PATCH",
            f"/v1/jobs/{job_id}",
            {
                "status": "claimed",
                "claimed_by": _urn,
            },
            timeout=15,
        )
        if code in (403, 409):
            # The claim was reassigned or the job became terminal/cancelled.
            # Never try to claim it again through this heartbeat.
            _claim_renewal_blocked_id = job_id
            print(
                f"[hive-worker:{WORKER_KIND}] WARNING: claim renewal rejected "
                f"for {job_id}: code={code}",
                flush=True,
            )
        elif code != 200:
            print(
                f"[hive-worker:{WORKER_KIND}] WARNING: claim renewal failed "
                f"for {job_id}: code={code}",
                flush=True,
            )


# Jobs are submitted only by registered orchestrator agents (bus-enforced,
# require_orchestrator_submitter), so this isn't an adversarial-multi-tenant
# boundary -- but a job's `repo` field still shouldn't be able to point git
# (which inherits this host's SSH agent / credential helpers) at an arbitrary
# host. Only resolve against the known-repo map; never trust a bare URL from
# the job (codex review finding, 2026-09-14).
# URL forms are accepted ONLY when they normalize to a name already in
# REPO_MAP; the clone then uses the REPO_MAP URL, never the job's string. That
# keeps the 2026-09-14 guarantee (no arbitrary hosts) while letting jobs that
# name the repo by its GitLab or ARGONAS URL resolve (4 jobs sat queued as
# no_repo_mapping on 2026-10-03 for exactly this).
_REPO_URL_RES = (
    re.compile(r"^https://gitlab\.com/(?:ncz-os|argonautsystems)/([A-Za-z0-9._-]+?)(?:\.git)?/?$"),
    re.compile(r"^git@gitlab\.com:(?:ncz-os|argonautsystems)/([A-Za-z0-9._-]+?)(?:\.git)?$"),
    re.compile(r"^(?:ssh://)?(?:root|jasonperlow)@192\.168\.207\.101[:/]+mnt/datapool/git/([A-Za-z0-9._-]+?)\.git/?$"),
)


def resolve_repo_url(job: dict) -> str | None:
    repo = (job.get("repo") or "").strip()
    if not repo:
        return None
    if repo in REPO_MAP:
        return REPO_MAP[repo]
    for rx in _REPO_URL_RES:
        m = rx.match(repo)
        if m and m.group(1) in REPO_MAP:
            return REPO_MAP[m.group(1)]
    return None


# Bus job ids are UUIDv7 (see agent_bus.py uuidv7()). Enforced here because
# job_id is used as a path component below -- an unvalidated id could
# traverse out of WORKSPACE_ROOT via "../" or an absolute path (codex review
# finding, 2026-09-14).
_JOB_ID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


def patch_job(job_id: str, status: str, result: dict) -> bool:
    # PATCH /v1/jobs/{id} requires claimed_by to match the job's current
    # claimant (agent_bus.py: "job update requires claimed_by to match the
    # current claimant") -- omitting it 403s every single call (found live
    # on PROTEUS, 2026-09-14: every job ended up stranded in 'claimed'
    # because this worker never sent it).
    code, _ = _http(
        "PATCH",
        f"/v1/jobs/{job_id}",
        {"status": status, "result": result, "claimed_by": _urn},
        timeout=15,
    )
    if code != 200:
        print(
            f"[hive-worker:{WORKER_KIND}] WARNING: patch({job_id}, {status}) got code={code} -- "
            f"job may be stranded in claimed state",
            flush=True,
        )
    return code == 200


def release_job(job_id: str, reason: str) -> None:
    # NOT /v1/jobs/{id}/release -- that endpoint is a completely different
    # feature (zc-gate's human-gated "Release & Push" PR workflow, expects a
    # preview bundle already on the job). The real way to decline a claimed
    # job back to the queue is a PATCH to status=queued (same claimed_by
    # requirement as patch_job above), matching the worker_error/note shape
    # observed from the bus's own existing workers (thrash-guard dead-letters
    # a job after 3 such declines).
    patch_job(job_id, "queued", {"worker_error": reason, "via": "hive_worker"})


def completion_evidence(workdir: str, baseline: str, env: dict) -> dict:
    """Inspect Git, never infer delivered work from model prose or exit status."""

    def git(*args):
        return subprocess.check_output(
            ["git", "-C", workdir, *args],
            env=env,
            text=True,
            stderr=subprocess.PIPE,
            timeout=60,
        ).strip()

    evidence = {
        "base_sha": baseline,
        "commits": [],
        "files_changed": [],
        "worktree_clean": False,
        "remote_verified": False,
    }
    try:
        head = git("rev-parse", "HEAD")
        evidence["head_sha"] = head
        evidence["worktree_clean"] = not git(
            "status", "--porcelain", "--untracked-files=all"
        )
        # Divergent/replaced histories cannot be presented as this job's work.
        git("merge-base", "--is-ancestor", baseline, head)
        evidence["commits"] = git("rev-list", f"{baseline}..{head}").splitlines()
        evidence["files_changed"] = git(
            "diff", "--name-only", baseline, head
        ).splitlines()
        upstream = git("rev-parse", "--symbolic-full-name", "@{upstream}")
        branch = git("symbolic-ref", "--short", "HEAD")
        remote = git("config", f"branch.{branch}.remote")
        merge = git("config", f"branch.{branch}.merge")
        if remote == "." or not upstream.startswith("refs/remotes/"):
            raise ValueError("upstream must be a remote branch")
        actual = git("ls-remote", "--exit-code", remote, merge).split()
        evidence["remote_ref"] = merge
        evidence["remote_sha"] = actual[0] if actual else None
        evidence["remote_verified"] = evidence["remote_sha"] == head
    except (subprocess.SubprocessError, OSError, ValueError) as exc:
        evidence["verification_error"] = type(exc).__name__
    return evidence


def completion_ready(evidence: dict) -> bool:
    return bool(
        evidence.get("commits")
        and evidence.get("files_changed")
        and evidence.get("worktree_clean") is True
        and evidence.get("remote_verified") is True
    )


def verify_delivery(workdir: str, baseline: str) -> int:
    """Return bounded, actionable evidence to zoder's check/fix loop."""
    evidence = completion_evidence(workdir, baseline, os.environ.copy())
    ready = completion_ready(evidence)
    # Lists and paths can grow without bound; neither Git stderr nor remote URLs
    # belong in model-visible output (they can contain credentials).
    report = {
        "delivery_ready": ready,
        "commit_count": len(evidence["commits"]),
        "changed_file_count": len(evidence["files_changed"]),
        "files_changed": [p[:240] for p in evidence["files_changed"][:20]],
        "worktree_clean": evidence["worktree_clean"],
        "remote_verified": evidence["remote_verified"],
        "verification_error": evidence.get("verification_error"),
        "required": (
            "Delivery verified."
            if ready
            else "Checkout a dedicated branch; commit legitimate task changes with "
            "the configured Git identity; push with upstream tracking; leave a clean "
            "worktree and remote HEAD matching local HEAD. Respect explicit job "
            "restrictions; report blockers instead of inventing edits."
        ),
    }
    print(json.dumps(report, ensure_ascii=True))
    return 0 if ready else 1


def configure_git_identity(workdir: str, env: dict) -> None:
    """Supply the authorized fleet identity even on hosts without global config."""
    for key, value in (
        ("user.name", env.get("HIVE_GIT_AUTHOR_NAME") or "Jason Perlow"),
        ("user.email", env.get("HIVE_GIT_AUTHOR_EMAIL") or "jperlow@gmail.com"),
    ):
        subprocess.run(
            ["git", "-C", workdir, "config", "--local", key, value],
            check=True,
            timeout=30,
            env=env,
            capture_output=True,
        )


def run_job(job: dict) -> None:
    """Guarantees a terminal disposition (patch or release) for every claimed
    job, even on an exception type we didn't anticipate (codex review
    finding, 2026-09-14: PermissionError/OSError etc previously escaped this
    function entirely, killing the daemon with the job left claimed
    forever)."""
    global _current_job_id, _claim_renewal_blocked_id
    job_id = job.get("id", "")
    _claim_renewal_blocked_id = None
    _current_job_id = job_id
    try:
        _run_job_inner(job)
    except Exception as e:
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id}: unhandled exception: {e!r}",
            flush=True,
        )
        if job_id:
            patch_job(
                job_id, "failed", {"via": "hive_worker", "error": f"unhandled: {e!r}"}
            )
    finally:
        _current_job_id = None


def _rescue_workspace(job_id: str, workdir: str, repo_url: str) -> dict | None:
    """Move an existing job checkout aside without losing anything.

    Returns a description of what was preserved, or None if the move itself
    failed (the caller then keeps the old fail-and-preserve behaviour).
    Unpushed commits on the checked-out branch are also pushed to
    rescue/hive-<job_id>-<ts> on the job's REPO_MAP URL (never the checkout's
    own `origin`, which an earlier job could have rewritten), best effort, so a
    re-imaged host (HYDRA, 2026-10-03) cannot take them with it. Uncommitted
    edits stay in the moved directory; nothing is staged, stashed or deleted.
    """
    ts = time.strftime("%Y%m%dT%H%M%S")
    rescue_root = os.path.join(WORKSPACE_ROOT, ".rescued")
    dest = os.path.join(rescue_root, f"{job_id}-{ts}")
    info: dict = {"moved_to": dest}
    try:
        unpushed = subprocess.run(
            ["git", "-C", workdir, "log", "--oneline", "HEAD", "--not", "--remotes"],
            capture_output=True, text=True, timeout=60,
        )
        if unpushed.returncode == 0 and unpushed.stdout.strip():
            branch = f"rescue/hive-{job_id}-{ts}"
            pushed = subprocess.run(
                ["git", "-C", workdir, "push", "--quiet", repo_url, f"HEAD:refs/heads/{branch}"],
                capture_output=True, text=True, timeout=300,
            )
            info["unpushed_commits"] = len(unpushed.stdout.strip().splitlines())
            info["rescue_branch"] = branch if pushed.returncode == 0 else None
        dirty = subprocess.run(
            ["git", "-C", workdir, "status", "--porcelain"],
            capture_output=True, text=True, timeout=60,
        )
        info["uncommitted_entries"] = len(dirty.stdout.splitlines()) if dirty.returncode == 0 else None
    except (OSError, subprocess.SubprocessError) as exc:
        info["inspect_error"] = str(exc)[:200]
    try:
        os.makedirs(rescue_root, exist_ok=True)
        os.rename(workdir, dest)
    except OSError as exc:
        print(f"[hive-worker:{WORKER_KIND}] job {job_id}: rescue move failed: {exc}", flush=True)
        return None
    print(f"[hive-worker:{WORKER_KIND}] job {job_id}: rescued prior workspace {info}", flush=True)
    return info


def _run_job_inner(job: dict) -> None:
    job_id = job["id"]
    kind = job.get("kind", "")
    description = job.get("description", "")
    head_branch = job.get("head_branch")

    if not _JOB_ID_RE.match(job_id):
        print(
            f"[hive-worker:{WORKER_KIND}] job id {job_id!r} doesn't look like a UUID, refusing",
            flush=True,
        )
        release_job(job_id, "malformed_job_id")
        return

    repo_url = resolve_repo_url(job)
    if not repo_url:
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id} kind={kind!r}: no resolvable repo, declining",
            flush=True,
        )
        release_job(job_id, "no_repo_mapping")
        return

    workdir = os.path.join(WORKSPACE_ROOT, job_id)
    os.makedirs(WORKSPACE_ROOT, exist_ok=True)
    if os.path.exists(workdir):
        # A restored claim may carry unfinished edits from an earlier run.
        # Never erase them -- but failing the job outright (the old behaviour)
        # made every retry/reclaim on the same host permanently dead: 23 jobs
        # were stuck as workspace_exists_needs_rescue on 2026-10-03. Move the
        # old checkout aside intact, push any unpushed commits to a rescue
        # branch so they survive host loss, then clone fresh.
        rescued = _rescue_workspace(job_id, workdir, repo_url)
        if rescued is None:
            patch_job(
                job_id,
                "failed",
                {
                    "via": "hive_worker",
                    "error": "workspace_exists_needs_rescue",
                    "workspace_preserved": workdir,
                },
            )
            return

    clone_cmd = ["git", "clone", "--quiet", repo_url, workdir]
    if head_branch:
        if head_branch.startswith("-"):
            print(
                f"[hive-worker:{WORKER_KIND}] job {job_id}: refusing suspicious head_branch {head_branch!r}",
                flush=True,
            )
            release_job(job_id, "invalid_head_branch")
            return
        clone_cmd[3:3] = ["--branch", head_branch]
    # root@ARGONAS git ops can hit the documented pubkey-exhaustion class of
    # failure (kernel-build-checklist.md #0f) on a host whose SSH agent
    # hasn't got root's key trusted -- found live on PROTEUS, 2026-09-14
    # ("returned non-zero exit status 128" on a plain ssh:// clone). Fall
    # back to password auth via GIT_FLEET_SSH_PASSWORD if the host provides
    # it (systemd EnvironmentFile, never hardcoded/committed here).
    #
    # StrictHostKeyChecking=no alone does NOT save a host whose known_hosts
    # already carries a CONFLICTING entry for ARGONAS (e.g. a stale ECDSA
    # key from before ARGONAS was reprovisioned to ED25519) -- ssh refuses
    # with "REMOTE HOST IDENTIFICATION HAS CHANGED" regardless of that flag,
    # since it exists specifically to catch a changed key. Confirmed live
    # 2026-09-16 on hydra+cerberus: identical clone_failed exit 128 on every
    # job, root cause was this mismatch, not a MaxStartups throttle as first
    # suspected. UserKnownHostsFile=/dev/null sidesteps host-key state
    # entirely for this break-glass password-auth path, the same way
    # PubkeyAuthentication=no already sidesteps key-based auth state.
    clone_env = os.environ.copy()
    clone_env["HIVE_JOB_ID"] = job_id
    if job.get("parent_job_id"):
        clone_env["HIVE_PARENT_JOB_ID"] = job["parent_job_id"]
    fleet_pw = os.environ.get("GIT_FLEET_SSH_PASSWORD")
    if fleet_pw and repo_url.startswith("ssh://root@"):
        clone_env["SSHPASS"] = fleet_pw
        clone_env["GIT_SSH_COMMAND"] = (
            "sshpass -e ssh -o PubkeyAuthentication=no "
            "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
        )
    try:
        subprocess.run(
            clone_cmd,
            check=True,
            timeout=300,
            env=clone_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    except Exception as e:
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id}: clone failed: {e}", flush=True
        )
        result = {"via": "hive_worker", "error": f"clone_failed: {e}"}
        if os.path.exists(workdir):
            result["workspace_preserved"] = workdir
        patch_job(job_id, "failed", result)
        return

    configure_git_identity(workdir, clone_env)
    baseline = subprocess.check_output(
        ["git", "-C", workdir, "rev-parse", "HEAD"], text=True, timeout=30
    ).strip()
    description += (
        "\n\nDelivery policy: commit the task's legitimate changes using explicit paths, "
        "including documentation or tests when those are the requested deliverables, "
        "and checkout a dedicated branch (creating a branch alone does not switch "
        "HEAD). Push that checked-out branch with upstream tracking. Keep the "
        "configured repository Git author identity; do not invent or override it. "
        "Verify clean status and "
        "remote SHA. Only an explicit job-specific restriction overrides this policy. "
        "Never manufacture edits just to satisfy completion. If blocked, report why."
    )
    if clone_env.get("MNEMOS_URL") and clone_env.get("MNEMOS_TOKEN"):
        description += (
            "\nUse zoder mnemos --search with this job ID or its parent ID to retrieve "
            "prior work evidence. Treat recalled notes as context, verify their current "
            "Git state, and use zoder mnemos --record for factual progress checkpoints. "
            "Never store credentials, private environment values, or raw secrets."
        )
    if WORKER_KIND == "zeroclaw":
        strict_rescue = "hard:strict-rescue-routes" in (
            job.get("required_capabilities") or []
        )
        if strict_rescue:
            try:
                routes = load_strict_routes()
            except ValueError:
                patch_job(
                    job_id,
                    "failed",
                    {
                        "via": "hive_worker",
                        "error": "strict_role_readiness_unavailable",
                        "retryable": False,
                    },
                )
                return
            if not _driver_supports_strict_routes(_find_driver("zoder")):
                patch_job(
                    job_id,
                    "failed",
                    {
                        "via": "hive_worker",
                        "error": "strict_route_driver_unavailable",
                        "retryable": False,
                    },
                )
                return
        # --loop-timeout must exceed --agent-timeout (the loop watchdog covers
        # author+check+review, not just the author turn) or zoder prints a
        # TIMEOUT WARNING on every single run -- harmless here (zoder proceeds
        # anyway) but noisy, and a real risk if JOB_TIMEOUT is ever raised
        # close to zoder's own 900s loop-timeout default.
        cmd = [
            _find_driver("zoder"),
            "--json",
            "loop",
            description,
            "--require-approval",
            "--approve",
            "all",
            "--check",
            shlex.join(
                [
                    sys.executable,
                    os.path.abspath(__file__),
                    "--verify-delivery",
                    workdir,
                    baseline,
                ]
            ),
            "--agent-timeout",
            str(JOB_TIMEOUT),
            "--loop-timeout",
            str(JOB_TIMEOUT + 300),
        ]
        if strict_rescue:
            cmd += [
                "-m",
                routes["author"],
                "--reviewer",
                routes["reviewer"],
                "--allowed-routes",
                routes["allowed_routes"],
                "--reviewer-allowed-routes",
                routes["reviewer_allowed_routes"],
                "--reviewer-pool-routes",
                routes["reviewer_pool_routes"],
            ]
        else:
            agent = escalation_agent(job)
            if agent:
                cmd += ["--agent", agent, "--allow-paid"]
                print(
                    f"[hive-worker:{WORKER_KIND}] job {job_id}: agent={agent} "
                    f"(retry_count={job.get('retry_count') or 0})",
                    flush=True,
                )
            if ZODER_REVIEWER:
                cmd += ["--reviewer", ZODER_REVIEWER]
    else:
        cmd = [
            _find_driver("codex"),
            "exec",
            "--sandbox",
            CODEX_SANDBOX,
            "-c",
            'approval_policy="never"',
            "--skip-git-repo-check",
            "-m",
            CODEX_MODEL,
            description,
        ]

    print(
        f"[hive-worker:{WORKER_KIND}] job {job_id}: running {' '.join(cmd[:3])}...",
        flush=True,
    )
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
        # env=clone_env: the driver (zoder/codex) must be able to `git push` to the
        # same root@ARGONAS origin it was cloned from; the password-auth
        # GIT_SSH_COMMAND was previously set only for the clone, so pushes from
        # inside the job failed and the workspace (formerly deleted on exit) lost the work.
        # No new exposure: GIT_FLEET_SSH_PASSWORD is already in this environment.
        proc = subprocess.Popen(
            cmd,
            cwd=workdir,
            stdout=subprocess.PIPE,
            env=clone_env,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
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
            print(
                f"[hive-worker:{WORKER_KIND}] job {job_id}: TIMEOUT after {JOB_TIMEOUT}s, process group killed",
                flush=True,
            )
    except FileNotFoundError as e:
        patch_job(
            job_id,
            "failed",
            {
                "via": "hive_worker",
                "error": f"driver_not_found: {e}",
                "workspace_preserved": workdir,
            },
        )
        return

    elapsed = round(time.time() - t0, 1)
    evidence = completion_evidence(workdir, baseline, clone_env)
    result = {
        "via": "hive_worker",
        "driver": WORKER_KIND,
        "kind": kind,
        "exit_code": exit_code,
        "elapsed_s": elapsed,
        "tail": tail,
        "workspace_preserved": workdir,
        **evidence,
    }
    status = "failed" if exit_code != 0 else "done"
    if exit_code == 0 and not completion_ready(evidence):
        status = "needs-review"
        result["failure_reason"] = "completion_evidence_missing"
    print(
        f"[hive-worker:{WORKER_KIND}] job {job_id}: {status} exit={exit_code} elapsed={elapsed}s",
        flush=True,
    )
    patch_job(job_id, status, result)
    # Retain even successful workspaces. A 200 from the bus acknowledges a
    # request, not independent validation of the code or a safe cleanup lease.


def main() -> None:
    if WORKER_KIND not in ("zeroclaw", "codex"):
        print(
            "[hive-worker] WORKER_KIND must be 'zeroclaw' or 'codex'", file=sys.stderr
        )
        raise SystemExit(1)
    register()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    print(f"[hive-worker:{WORKER_KIND}] polling {HIVE_URL} as {_urn}", flush=True)
    while _running:
        restart_after_source_update()
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
    if sys.argv[1:2] == ["--verify-delivery"]:
        if len(sys.argv) != 4:
            print(json.dumps({"error": "usage: --verify-delivery WORKDIR BASELINE"}))
            raise SystemExit(2)
        raise SystemExit(verify_delivery(sys.argv[2], sys.argv[3]))
    else:
        main()
