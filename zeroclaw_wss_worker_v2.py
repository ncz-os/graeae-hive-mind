#!/usr/bin/env python3
"""ZeroClaw WSS Worker v2 — workspace-aware + commit-verifying.

POLICY (user 2026-05-26): WSS via gateway open-weight models is DEFAULT.
Codex is a scarce resource reserved for:
  - kind=codex          (explicit)
  - kind=review:*       (adversarial review)
  - kind=doctor:codex-fix:* (doctor escalations)

Both paths run inside a real per-kind git workspace (auto-cloned on first
use), and any reported commit SHA is verified via `git cat-file -e`.
"""
import json
import logging
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import wss_driver  # noqa: E402

HIVE_URL = os.environ.get("HIVE_URL", "http://192.168.207.67:5005")
AGENT_HOST = os.environ.get("AGENT_HOST") or socket.gethostname().split(".")[0]
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "30"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "15"))
GATEWAY_HOST = os.environ.get("GATEWAY_HOST", "127.0.0.1")
GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "42617"))
JOB_TIMEOUT = float(os.environ.get("JOB_TIMEOUT", "900"))
INSTANCE = os.environ.get("ZEROCLAW_INSTANCE_ID") or os.environ.get("INSTANCE", "1")

WORKSPACE_ROOT = Path(os.path.expanduser("~/codex-workspace"))
WORKSPACE_ROOT.mkdir(parents=True, exist_ok=True)

# Kinds that MUST use codex (scarce resource — opt-in only).
CODEX_KINDS_PREFIXES = ("codex", "review:", "doctor:codex-fix")

# kind-prefix → (subdir under ~/codex-workspace, git_url-or-None)
KIND_WORKSPACE_MAP = {
    "argonaut:dbpr-site-scaffold-single-multivertical": ("florida-licenses", None),
    "argonaut:dbpr-platform-bootstrap-with-workspace": ("florida-licenses", None),
    "argonaut:dbpr-platform-bootstrap": ("florida-licenses", None),
    "argonaut:dbpr-discovery-sweep-35-categories": ("florida-licenses", None),
    "argonaut:":        ("florida-licenses", None),
    "fix:codex-pro-oauth-verify": ("mnemos",         "https://gitlab.com/mnemos-os/mnemos.git"),
    "mnemos:":          ("mnemos",         "https://gitlab.com/mnemos-os/mnemos.git"),
    "riskybiz:p1-sunbiz-entity-resolver": ("florida-licenses", None),
    "riskybiz:":        ("florida-licenses", None),
    "riskyeats:":       ("riskyeats",      "https://gitlab.com/perlowja/riskyeats.git"),
    "ncz-os-zeroclaw:": ("zeroclaw",       "https://gitlab.com/nclawzero/zeroclaw.git"),
    "ncz-os-openclaw:": ("openclaw",       "https://gitlab.com/nclawzero/openclaw.git"),
    "ncz-os-":          ("ncz-installer",  "https://gitlab.com/nclawzero/ncz-installer.git"),
    "cixmini-os:":      ("cix-installer",  "https://gitlab.com/nclawzero/cix-installer.git"),
    "fleet-infra:":     ("fleet-ops",      None),
}

# Hosts with limited compute — only accept light kinds.
NARROW_HOSTS = {"cixmini", "bigpi", "clawpi", "zeropi"}
NARROW_ALLOWLIST = ("cixmini-os:", "ncz-os-", "fleet-infra:")
_HOST_LOWER = (AGENT_HOST or "").lower()

# Open-weight tier chains via WSS gateway
TIER_CHAINS = {
    "A": ["hive_deepseek_pro_1", "hive_nvidia_1", "hive_deepseek_1"],
    "B": ["hive_deepseek_pro_1", "hive_nvidia_1", "hive_deepseek_1", "hive_groq_8b"],
    "C": ["hive_deepseek_1", "hive_groq_8b", "hive_nvidia_2"],
}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [zc-wss-worker@%(process)d] %(message)s")
log = logging.getLogger("wss-worker")


def http(method, path, body=None, timeout=20):
    url = HIVE_URL + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                  headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            txt = r.read()
            return r.status, (json.loads(txt) if txt else {})
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read())
        except Exception:
            body = {"error": str(e)}
        return e.code, body
    except Exception as e:
        return 0, {"error": str(e)}


def register():
    body = {
        "host": AGENT_HOST, "kind": "zeroclaw", "runtime": "zeroclaw",
        "autonomy_level": "interactive", "auth_method": "api",
        "capabilities": ["code-edit","build","test","debug","refactor",
                         "python","bash","docker","linux",
                         "wss-driven","workspace-aware","open-weights-first"],
        "model": "wss-via-gateway+codex-fallback",
        "provider": "openai",
    }
    code, resp = http("POST", "/v1/agents/register", body, timeout=15)
    if code in (200, 201):
        urn = resp.get("urn")
        log.info("registered urn=%s host=%s narrow=%s",
                 urn, AGENT_HOST, _HOST_LOWER in NARROW_HOSTS)
        return urn
    log.error("register failed code=%s resp=%s", code, resp)
    return None


def heartbeat(urn):
    code, _ = http("POST", "/v1/agents/heartbeat", {"urn": urn}, timeout=10)
    return code in (200, 201, 204)


def claim_next(urn):
    code, resp = http("POST", f"/v1/jobs/next?agent_urn={urn}", timeout=15)
    if code in (200, 201) and isinstance(resp, dict) and resp.get("id"):
        return resp
    return None


def patch_job(jid, urn, status, result=None):
    body = {"status": status, "claimed_by": urn}
    if result is not None:
        body["result"] = result
    code, resp = http("PATCH", f"/v1/jobs/{jid}", body, timeout=15)
    if code not in (200, 201, 204):
        log.warning("patch %s -> %s: %s", jid, code, str(resp)[:200])
    return code in (200, 201, 204)


def pick_agent_alias(tier, kind):
    chain = TIER_CHAINS.get((tier or "C").upper(), TIER_CHAINS["C"])
    return chain[0]


def _git(workspace, *args, timeout=30):
    try:
        return subprocess.run(["git", "-C", str(workspace), *args],
                              capture_output=True, text=True, timeout=timeout)
    except Exception:
        class _R: returncode = 1; stdout = ""; stderr = "git-exec-failed"
        return _R()


def _verify_commit(workspace, sha):
    if not (isinstance(sha, str) and re.fullmatch(r"[a-f0-9]{7,40}", sha)):
        return False
    r = _git(workspace, "cat-file", "-e", f"{sha}^{{commit}}", timeout=5)
    return r.returncode == 0


def _ensure_workspace(subdir, git_url, kind):
    path = WORKSPACE_ROOT / subdir
    git_dir = path / ".git"
    if git_dir.is_dir():
        _git(path, "fetch", "--quiet", "--all", timeout=60)
        return path, None
    if git_url is None:
        path.mkdir(parents=True, exist_ok=True)
        r = subprocess.run(
            ["git", "init", "--quiet", str(path)],
            capture_output=True, text=True, timeout=30,
        )
        if r.returncode != 0:
            return None, f"git_init_failed:{r.stderr[:200]}"
        _git(path, "config", "user.name",  "Jason Perlow", timeout=5)
        _git(path, "config", "user.email", "jperlow@gmail.com", timeout=5)
        log.info("created local git workspace: kind=%s path=%s", kind, path)
        return path, None
    log.info("first-use clone: kind=%s url=%s → %s", kind, git_url, path)
    r = subprocess.run(
        ["git", "clone", "--quiet", "--depth", "50", git_url, str(path)],
        capture_output=True, text=True, timeout=180,
    )
    if r.returncode != 0:
        return None, f"clone_failed:{r.stderr[:200]}"
    _git(path, "config", "user.name",  "Jason Perlow", timeout=5)
    _git(path, "config", "user.email", "jperlow@gmail.com", timeout=5)
    return path, None


def _resolve_workspace(kind, description):
    if _HOST_LOWER in NARROW_HOSTS and not any(
        kind.startswith(p) for p in NARROW_ALLOWLIST
    ):
        return None, f"host_declines_kind:{_HOST_LOWER}"

    if kind.startswith("doctor:codex-fix"):
        m = re.search(r"\brepo:\s*([a-z0-9_\-]+)", (description or ""), re.I)
        if not m:
            return None, "doctor_fix_missing_repo_hint"
        repo = m.group(1).lower()
        for prefix, (subdir, git_url) in KIND_WORKSPACE_MAP.items():
            if subdir.lower() == repo:
                return _ensure_workspace(subdir, git_url, kind)
        return None, f"no_workspace_for_repo:{repo}"

    best = None
    for prefix, val in KIND_WORKSPACE_MAP.items():
        if kind.startswith(prefix) and (best is None or len(prefix) > len(best[0])):
            best = (prefix, val)
    if best is None:
        return None, "no_workspace_for_kind"
    prefix, (subdir, git_url) = best
    return _ensure_workspace(subdir, git_url, kind)


def _augment_task_for_wss(task, workspace, kind):
    """Prepend a workspace+host preamble so the agent knows where to work."""
    fleet_ips = (
        "PYTHIA=192.168.207.67 TYPHON=192.168.207.61 "
        "CERBERUS=192.168.207.96 ARGONAS=192.168.207.101"
    )
    preamble = (
        f"[WORKER CONTEXT]\n"
        f"You are operating on host {AGENT_HOST} as user jasonperlow.\n"
        f"Working git repository: {workspace}\n"
        f"  (it is a clone of the canonical remote; use `cd {workspace}` "
        f"before any git operation)\n"
        f"Job kind: {kind}\n"
        f"Fleet hosts available via passwordless SSH: {fleet_ips}\n"
        f"If you need resources that don't exist here (Oracle, GPU, etc.), "
        f"`ssh jasonperlow@<host>` to a capable host and run remotely.\n"
        f"When making changes, commit them in {workspace} as "
        f"`Jason Perlow <jperlow@gmail.com>`. Never use @nvidia.com.\n"
        f"[END WORKER CONTEXT]\n\n"
        f"[TASK]\n{task}\n[END TASK]"
    )
    return preamble


def codex_cli_run(task, kind, workspace, timeout_sec=900.0):
    """Scarce — only for codex/review:/doctor:codex-fix kinds."""
    started = time.time()
    try:
        pre_head = _git(workspace, "rev-parse", "HEAD", timeout=10).stdout.strip()
        proc = subprocess.run(
            ["codex", "exec", "--skip-git-repo-check",
             "--dangerously-bypass-approvals-and-sandbox",
             "-m", "gpt-5.5", task[:4000]],
            cwd=str(workspace), capture_output=True, text=True,
            timeout=timeout_sec, stdin=subprocess.DEVNULL,
        )
        post_head = _git(workspace, "rev-parse", "HEAD", timeout=10).stdout.strip()
        commits, files = [], []
        if post_head and post_head != pre_head:
            raw = _git(workspace, "log", "--format=%H",
                       f"{pre_head}..{post_head}", timeout=15).stdout.strip().split("\n")
            commits = [c for c in raw if c and _verify_commit(workspace, c)]
            if commits:
                fc = _git(workspace, "diff", "--name-only",
                          f"{pre_head}..{post_head}", timeout=15).stdout
                files = [f for f in fc.strip().split("\n") if f][:50]
        return {
            "exit_code": proc.returncode,
            "commits": commits, "files_changed": files,
            "duration_sec": round(time.time()-started, 2),
            "via": "codex_cli", "workspace": str(workspace),
            "pre_head": pre_head[:12], "post_head": post_head[:12],
            "stdout_preview": (proc.stdout or "")[:1500],
            "stderr_preview": (proc.stderr or "")[:500],
            "error": None if proc.returncode == 0 else f"codex exit {proc.returncode}",
        }
    except subprocess.TimeoutExpired:
        return {"exit_code": 124, "error": f"codex timeout {timeout_sec}s",
                "via": "codex_cli", "commits": [], "files_changed": [],
                "workspace": str(workspace)}
    except FileNotFoundError:
        return {"exit_code": 127, "error": "codex CLI not found",
                "via": "codex_cli", "commits": [], "files_changed": []}
    except Exception as e:
        return {"exit_code": 1, "error": f"{type(e).__name__}: {e}",
                "via": "codex_cli", "commits": [], "files_changed": []}


def wss_run(task, kind, workspace, alias, job_id, timeout_sec):
    """DEFAULT path — open-weight via local zeroclaw gateway."""
    started = time.time()
    pre_head = _git(workspace, "rev-parse", "HEAD", timeout=10).stdout.strip()
    augmented = _augment_task_for_wss(task, workspace, kind)
    try:
        raw_result = wss_driver.drive_agent_via_wss(
            host=GATEWAY_HOST, agent_alias=alias, task=augmented,
            job_id=job_id, timeout_sec=timeout_sec,
        )
    except Exception as e:
        raw_result = {"exit_code": 1, "via": "wss_driver",
                      "error": f"driver_crash: {type(e).__name__}: {e}"}
    # Recompute commits authoritatively from the workspace (ignore regex harvest)
    post_head = _git(workspace, "rev-parse", "HEAD", timeout=10).stdout.strip()
    commits, files = [], []
    if post_head and post_head != pre_head:
        raw_log = _git(workspace, "log", "--format=%H",
                       f"{pre_head}..{post_head}", timeout=15).stdout
        for c in raw_log.strip().split("\n"):
            if c and _verify_commit(workspace, c):
                commits.append(c)
        if commits:
            fc = _git(workspace, "diff", "--name-only",
                      f"{pre_head}..{post_head}", timeout=15).stdout
            files = [f for f in fc.strip().split("\n") if f][:50]
    raw_result["commits"] = commits          # authoritative override
    raw_result["files_changed"] = files
    raw_result["workspace"] = str(workspace)
    raw_result["pre_head"] = pre_head[:12]
    raw_result["post_head"] = post_head[:12]
    raw_result["duration_sec"] = round(time.time()-started, 2)
    return raw_result


def process_job(urn, job):
    jid = job["id"]
    kind = job.get("kind", "") or ""
    desc = job.get("description", "") or ""
    tier = job.get("max_cost_tier", "C")
    alias = pick_agent_alias(tier, kind)
    log.info("claimed %s kind=%s tier=%s", jid[:12], kind[:60], tier)
    patch_job(jid, urn, "running")
    started = time.time()
    try:
        workspace, werr = _resolve_workspace(kind, desc)
        if werr:
            result = {"exit_code": 2, "via": "preflight",
                      "commits": [], "files_changed": [],
                      "worker_error": werr, "error": werr}
        else:
            # ALL kinds (incl doctor:/codex/review:) run through the same WSS
            # gateway agent path — no special codex-CLI snowflake. Uniform
            # agent type + uniform commit-verify/push gating for every kind.
            log.info("  → WSS open-weight (default) alias=%s workspace=%s",
                     alias, workspace)
            result = wss_run(desc, kind, workspace, alias, jid, JOB_TIMEOUT)
    except Exception as e:
        log.exception("dispatch failed")
        result = {"exit_code": 1, "via": "worker",
                  "error": f"dispatch_crash: {type(e).__name__}: {e}",
                  "commits": [], "files_changed": []}
    result["duration_sec"] = round(time.time() - started, 2)
    result["kind"] = kind
    # --- anti-fake-completion gate: verify real commit + push to remote ---
    commits = result.get("commits") or []
    workspace = result.get("workspace")
    has_remote = False
    pushed = False
    if workspace:
        rr = _git(Path(workspace), "remote", "get-url", "origin", timeout=10)
        has_remote = (rr.returncode == 0 and bool(rr.stdout.strip()))
    if commits and has_remote:
        pr = _git(Path(workspace), "push", "origin", "HEAD", timeout=180)
        pushed = (pr.returncode == 0)
        result["pushed"] = pushed
        if not pushed:
            result["worker_error"] = "push_failed:" + pr.stderr.strip()[:200]
    requires_commit = has_remote and not kind.startswith("review:")  # review jobs emit a report, not a commit
    if requires_commit:
        # For a repo job the VERIFIED, PUSHED commit is the authoritative
        # success signal — honor it even when the agent returns non-zero
        # because it hit a soft cap (e.g. "max tool iterations"). The work
        # landed in the remote, so the job is done. No pushed commit =>
        # failed (prevents the fake "completed but no commit" class).
        if commits and pushed:
            status = "done"
        else:
            status = "failed"
            if not commits:
                result.setdefault("error", "fake_prevented:exit0_but_zero_verified_commits")
            elif not pushed:
                result.setdefault("error", "commit_made_but_push_failed")
    elif result.get("exit_code") == 0 and not result.get("worker_error"):
        status = "done"
    else:
        status = "failed"
    patch_job(jid, urn, status, result)
    commits = result.get("commits") or []
    log.info("→ %s %s commits=%d dur=%.1fs %s",
             status, jid[:12], len(commits), result["duration_sec"],
             result.get("error") or result.get("worker_error") or "")


def main():
    log.info("starting wss-worker-v2 on %s, gateway=%s:%s, poll=%ss "
             "(open-weights default, codex scarce)",
             AGENT_HOST, GATEWAY_HOST, GATEWAY_PORT, POLL_INTERVAL)
    urn = register()
    if not urn:
        sys.exit(1)

    # Background heartbeat: keep the agent visibly ONLINE in the dashboard
    # even while the main loop is blocked for ~20s on a real agent job
    # (inline heartbeat used to lapse during work -> worker flickered offline).
    def _heartbeat_loop():
        while True:
            try:
                heartbeat(urn)
            except Exception:
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    threading.Thread(target=_heartbeat_loop, daemon=True).start()

    while True:
        try:
            job = claim_next(urn)
        except Exception as e:
            log.warning("claim error: %s", e)
            time.sleep(POLL_INTERVAL)
            continue
        if not job:
            time.sleep(POLL_INTERVAL)
            continue
        try:
            process_job(urn, job)
        except Exception:
            log.exception("process_job error")


if __name__ == "__main__":
    main()
