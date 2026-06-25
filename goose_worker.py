#!/usr/bin/env python3
"""
GRAEAE Hive Mind — goose worker daemon.

Self-driving worker: registers as agent, polls /v1/jobs/next, invokes `goose run`,
reports result. One in-flight job per process. Run multiple per host for parallelism.

Env:
  HIVE_URL              http://192.168.207.8:5005 (default)
  GOOSE_BIN             /usr/local/bin/goose (default)
  AGENT_HOST            $(hostname) (default)
  AGENT_CAPABILITIES    comma-sep, default "code-edit,build,test,debug,refactor"
  ELIGIBLE_KINDS_ONLY   if set, only claim jobs explicitly listing 'goose' in eligible_kinds
  POLL_INTERVAL         30 seconds idle wait
  HEARTBEAT_INTERVAL    15 seconds
  GOOSE_TIMEOUT         600 seconds per job
  GOOSE_EXTRA_ARGS      extra goose run args, e.g. "--agent build"
  AGENT_PROVIDER        provider name for cost-tier classification (default "unknown" → tier C)
  AGENT_MODEL           model name reported to hive for dispatch filtering
"""
from __future__ import annotations
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
import urllib.error
import random
import signal
from typing import Optional

HIVE_URL = os.environ.get("HIVE_URL", "http://192.168.207.8:5005")
HIVE_BUS_TOKEN = os.environ.get("HIVE_BUS_TOKEN", "").strip()
GOOSE_BIN = os.environ.get("GOOSE_BIN", "/usr/local/bin/goose")
AGENT_HOST = os.environ.get("AGENT_HOST", socket.gethostname())
AGENT_CAPABILITIES = [c.strip() for c in os.environ.get(
    "AGENT_CAPABILITIES",
    "code-edit,build,test,debug,refactor,python,bash,docker,linux"
).split(",") if c.strip()]
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "30"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "15"))
GOOSE_TIMEOUT = int(os.environ.get("GOOSE_TIMEOUT", "600"))
# #2 FIX (review 2026-05-23): orchestration meta-jobs fan out child jobs and routinely
# need >600s; give them their own ceiling. Override via env.
ORCHESTRATION_TIMEOUT = int(os.environ.get("ORCHESTRATION_TIMEOUT", "3600"))
GOOSE_EXTRA_ARGS = os.environ.get("GOOSE_EXTRA_ARGS", "").split()
WORKDIR = os.environ.get("HIVE_WORKDIR", os.getcwd())
AGENT_PROVIDER = os.environ.get("AGENT_PROVIDER", "unknown")
AGENT_MODEL = os.environ.get("AGENT_MODEL", "unknown")


def timeout_for_kind(kind: str) -> int:
    """Per-kind timeout override. Orchestration jobs need longer runway than typical worker jobs."""
    if (kind or "").lower() in ("orchestration", "orchestrate", "fan-out", "meta"):
        return ORCHESTRATION_TIMEOUT
    return GOOSE_TIMEOUT

_urn: str = ""
_last_heartbeat = 0.0
_heartbeat_count = 0
_local_inference_cache: list[dict] = []
_running = True


def _signal_handler(signum, frame):
    global _running
    print(f"[worker] signal {signum} — shutting down", flush=True)
    _running = False


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


def _http(method: str, path: str, body: dict | None = None, timeout: float = 10.0) -> tuple[int, dict | None]:
    url = f"{HIVE_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {"content-type": "application/json"}
    if HIVE_BUS_TOKEN:
        headers["authorization"] = f"Bearer {HIVE_BUS_TOKEN}"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            code = r.status
            if code == 204 or not raw:
                return code, None
            return code, json.loads(raw)
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except Exception as e:
        print(f"[worker] http error {method} {path}: {e}", flush=True)
        return 0, None


def register() -> str:
    global _urn, _last_heartbeat
    body = {
        "kind": "goose",
        "host": AGENT_HOST,
        "pid": os.getpid(),
        "capabilities": AGENT_CAPABILITIES,
        "provider": AGENT_PROVIDER,
        "model": AGENT_MODEL,
        "version": _goose_version(),
        "metadata": {
            "daemon": "goose_worker.py",
            "extra_args": GOOSE_EXTRA_ARGS,
            "started_at": time.time(),
        },
    }
    code, resp = _http("POST", "/v1/agents/register", body)
    if code == 200 and resp:
        _urn = resp["urn"]
        _last_heartbeat = time.time()
        print(f"[worker] registered urn={_urn}", flush=True)
        return _urn
    print(f"[worker] register failed code={code} resp={resp}", flush=True)
    sys.exit(1)


def _goose_version() -> str:
    try:
        out = subprocess.run([GOOSE_BIN, "--version"], capture_output=True, text=True, timeout=5)
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def _json_get(url: str, timeout: float = 2.0) -> dict | list | None:
    try:
        req = urllib.request.Request(url, method="GET", headers={"accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
        return json.loads(raw) if raw else None
    except Exception:
        return None


def _extract_model_names(payload: dict | list | None, *, response_type: str) -> list[str]:
    if not payload:
        return []
    if response_type == "ollama":
        items = payload.get("models", []) if isinstance(payload, dict) else []
    else:
        if isinstance(payload, dict):
            items = payload.get("data", payload.get("models", []))
        else:
            items = payload
    names: list[str] = []
    if not isinstance(items, list):
        return names
    for item in items:
        name = ""
        if isinstance(item, str):
            name = item
        elif isinstance(item, dict):
            name = str(item.get("id") or item.get("name") or item.get("model") or "")
        if name and name not in names:
            names.append(name)
    return names


def probe_local_inference() -> list[dict]:
    found: list[dict] = []
    probes = (
        ("http://localhost:8080/v1/models", "llama-server"),
        ("http://localhost:8082/v1/models", "llama-server"),
        ("http://localhost:11434/api/tags", "ollama"),
    )
    for url, kind in probes:
        payload = _json_get(url, timeout=2.0)
        if payload is None:
            continue
        found.append({
            "url": url,
            "models": _extract_model_names(payload, response_type=kind),
            "type": kind,
        })
    return found


def heartbeat():
    global _last_heartbeat, _heartbeat_count, _local_inference_cache
    if time.time() - _last_heartbeat < HEARTBEAT_INTERVAL:
        return
    _heartbeat_count += 1
    if not _local_inference_cache or _heartbeat_count % 5 == 0:
        _local_inference_cache = probe_local_inference()
    body = {"urn": _urn}
    if _local_inference_cache:
        body["metadata"] = {"local_inference": _local_inference_cache}
    _http("POST", "/v1/agents/heartbeat", body)
    _last_heartbeat = time.time()


def claim_next_job() -> dict | None:
    code, resp = _http("POST", f"/v1/jobs/next?agent_urn={_urn}")
    if code == 200 and resp:
        return resp
    if code != 204 and code != 0:
        print(f"[worker] dequeue unexpected code={code} resp={resp}", flush=True)
    return None


def update_job(job_id: str, status: str, result: dict):
    # REVIEW #9 fix: tokens_in/tokens_out MUST be top-level on JobUpdate
    # body or agent_bus.py never moves them into jobs.tokens_in/tokens_out
    # columns. Keep them in result too for caller introspection, but lift
    # to top-level for the PATCH so /v1/stats/workers counts populate.
    body = {"status": status, "result": result, "claimed_by": _urn}
    if isinstance(result, dict):
        t_in = result.get("tokens_in")
        t_out = result.get("tokens_out")
        if t_in is not None:
            body["tokens_in"] = int(t_in)
        if t_out is not None:
            body["tokens_out"] = int(t_out)
    _http("PATCH", f"/v1/jobs/{job_id}", body)


ERR_PATTERNS = [
    ("rate_limit",      "Rate limit exceeded"),
    ("rate_limit_429",  '"status":429'),
    ("auth_error",      "Authentication error"),
    ("auth_failed",     "Authentication failed"),
    ("context_overflow","context length"),
    ("model_not_found", "model not found"),
    ("upstream_500",    "InternalServerError"),
]


def detect_goose_error(stdout: str) -> Optional[str]:
    """Inspect goose stdout. Return error tag if recoverable failure pattern found, else None."""
    if not stdout:
        return None
    s = stdout.lower()
    for tag, needle in ERR_PATTERNS:
        if needle.lower() in s:
            return tag
    return None


import re as _re
_TOKEN_PAT = _re.compile(r"\[tokens?:\s*(\d+)(?:\s*[/+]\s*(\d+))?", _re.I)
_USAGE_PAT = _re.compile(r"(?:prompt[_ ]?tokens|input[_ ]?tokens)[:\s=]+(\d+).*?(?:completion[_ ]?tokens|output[_ ]?tokens)[:\s=]+(\d+)", _re.I | _re.S)


def parse_tokens(stdout: str, stderr: str = "") -> tuple[int, int]:
    """#9 FIX (review 2026-05-23): extract input/output token counts from goose output
    so hive cost discipline can audit actual usage. Falls back to (0, 0) if no markers.

    Goose emits a `[tokens: N]` line in some modes; some providers (OpenAI/Anthropic) leak
    `prompt_tokens=N completion_tokens=N` shaped lines on debug; rough char/4 estimate
    used only when nothing parseable found.
    """
    text = (stdout or "") + "\n" + (stderr or "")
    # Pattern 1: goose's own [tokens: N] (often only output count)
    m = _TOKEN_PAT.search(text)
    if m:
        a = int(m.group(1))
        b = int(m.group(2)) if m.group(2) else 0
        if b:
            return (a, b)
        return (0, a)
    # Pattern 2: structured prompt_tokens / completion_tokens
    m = _USAGE_PAT.search(text)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    return (0, 0)


def _git_lines(args: list[str], cwd: str = WORKDIR) -> list[str]:
    try:
        proc = subprocess.run(
            ["git", *args], cwd=cwd, capture_output=True, text=True, timeout=10
        )
        if proc.returncode != 0:
            return []
        return [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    except Exception:
        return []


def git_snapshot(cwd: str = WORKDIR) -> tuple[str | None, set[str]]:
    head = _git_lines(["rev-parse", "HEAD"], cwd)
    files = set(_git_lines(["status", "--porcelain"], cwd))
    return (head[0] if head else None, files)


def git_result_delta(before_head: str | None, before_status: set[str], cwd: str = WORKDIR) -> tuple[list[str], list[str]]:
    after_head, after_status = git_snapshot(cwd)
    commits: list[str] = []
    if before_head and after_head and before_head != after_head:
        commits = _git_lines(["log", "--format=%H", f"{before_head}..{after_head}"], cwd)
    files = sorted({
        line[3:] if len(line) > 3 else line
        for line in (after_status | before_status)
        if line
    })
    if not files and commits:
        files = sorted(set(_git_lines(["diff", "--name-only", f"{before_head}..{after_head}"], cwd)))
    return commits, files


# Interval at which workers send a job-level proof-of-work heartbeat via PATCH status=running.
# Must be well under CLAIM_LEASE_SECONDS (1800s) so long orchestration jobs don't get reaped.
JOB_HEARTBEAT_INTERVAL = int(os.environ.get("JOB_HEARTBEAT_INTERVAL", "300"))  # 5 min default


def run_goose(description: str, kind: str = "", job_heartbeat_fn=None) -> dict:
    cmd = [GOOSE_BIN, "run", "--text", description, "--no-session"] + GOOSE_EXTRA_ARGS
    print(f"[worker] $ {' '.join(cmd[:6])} … [desc len={len(description)}]", flush=True)
    start = time.time()
    timeout = timeout_for_kind(kind)
    before_head, before_status = git_snapshot()
    try:
        proc = subprocess.Popen(cmd, cwd=WORKDIR, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True)
        last_job_hb = time.time()
        while True:
            try:
                proc.wait(timeout=30)
                break  # process finished
            except subprocess.TimeoutExpired:
                elapsed = time.time() - start
                if elapsed >= timeout:
                    proc.kill()
                    proc.wait()
                    return {
                        "exit_code": -1,
                        "error": f"timeout {timeout}s exceeded",
                        "duration_sec": round(elapsed, 1),
                        "commits": [],
                        "files_changed": [],
                        "workdir": WORKDIR,
                    }
                # Proof-of-work heartbeat: renew job lease on the Hive so reaper doesn't
                # reclaim long-running orchestration/migration jobs while worker is alive.
                if job_heartbeat_fn and (time.time() - last_job_hb) >= JOB_HEARTBEAT_INTERVAL:
                    try:
                        job_heartbeat_fn(round(elapsed, 1))
                    except Exception:
                        pass
                    last_job_hb = time.time()

        stdout, stderr = proc.stdout.read(), proc.stderr.read()
        err_tag = detect_goose_error(stdout)
        t_in, t_out = parse_tokens(stdout, stderr)
        commits, files_changed = git_result_delta(before_head, before_status)
        # Fallback estimate when goose doesn't surface counts: char/4 (rough GPT-tokenizer heuristic)
        if t_in == 0 and t_out == 0:
            t_in = max(1, len(description) // 4)
            t_out = max(1, len(stdout) // 4)
        result = {
            "exit_code": proc.returncode,
            "stdout": stdout[-12000:],
            "stderr": stderr[-4000:],
            "duration_sec": round(time.time() - start, 1),
            "goose_cmd": " ".join(cmd[:6]),
            "tokens_in": t_in,
            "tokens_out": t_out,
            "commits": commits,
            "files_changed": files_changed,
            "workdir": WORKDIR,
        }
        if err_tag:
            result["exit_code"] = 1
            result["worker_error"] = err_tag
        return result
    except Exception as e:
        return {
            "exit_code": -1,
            "error": f"{type(e).__name__}: {e}",
            "commits": [],
            "files_changed": [],
            "workdir": WORKDIR,
        }


def main():
    register()
    backoff = 1.0
    while _running:
        heartbeat()
        job = claim_next_job()
        if not job:
            # exp backoff up to POLL_INTERVAL, plus jitter
            time.sleep(min(backoff, POLL_INTERVAL) + random.uniform(0, 2))
            backoff = min(backoff * 1.5, POLL_INTERVAL)
            continue
        backoff = 1.0  # reset on success
        print(f"[worker] claimed job {job['id'][:8]} kind={job['kind']} priority={job.get('priority')}", flush=True)
        update_job(job["id"], "running", {"started_by": _urn, "started_at": time.time()})
        job_id = job["id"]
        def _job_hb(elapsed):
            update_job(job_id, "running", {"heartbeat_at": time.time(), "elapsed_sec": elapsed})
        result = run_goose(job.get("description") or job.get("kind", ""), job.get("kind", ""), job_heartbeat_fn=_job_hb)
        status = "done" if result.get("exit_code") == 0 else "failed"
        result["finished_at"] = time.time()
        update_job(job["id"], status, result)
        print(f"[worker] {status} job {job['id'][:8]} exit={result.get('exit_code')} dur={result.get('duration_sec')}s", flush=True)
    print("[worker] clean shutdown", flush=True)


if __name__ == "__main__":
    main()
