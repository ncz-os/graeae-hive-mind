#!/usr/bin/env python3
"""
GRAEAE Hive Mind — PYTHIA zeroclaw doctor worker.

Registers as kind=doctor + runtime=doctor (RUNTIME_KIND_MAP entry added 2026-05-26).
Claims triage:* jobs from the hive queue and processes them by invoking
the local Codex CLI tool through ChatGPT subscription auth.

Doctor authority (GRAEAE-validated consultation 2026-05-26):
  • Read hive bus HTTP API to gather failed-job context.
  • Run codex exec with structured prompt requesting JSON action.
  • Execute allowlisted playbook actions: restart_service | resubmit_jobs |
    cancel_jobs | dispatch_codex_fix | no_action | escalate.
  • PATCH triage job to done with summary; auto-clean failed-queue entries.

Constraints:
  • One in-flight triage job per process.
  • No raw shell from LLM output — LLM emits a JSON action, doctor maps to
    a hardcoded playbook function.
  • Allowlisted services + hosts for restart actions.
  • Anthropic FORBIDDEN as agentic provider (CLAUDE.md directive #5).
  • Together AI FORBIDDEN for doctor (too expensive per hive rules).

Env:
  HIVE_URL                   http://192.168.207.67:5005
  ZEROCLAW_BIN               /usr/local/bin/zeroclaw
  DOCTOR_AGENT_ALIAS         hive_doctor (must exist in ~/.zeroclaw/config.toml)
  AGENT_HOST                 PYTHIA
  POLL_INTERVAL              30 seconds idle wait
  HEARTBEAT_INTERVAL         15 seconds
  ZEROCLAW_TIMEOUT           900 seconds per LLM call
  MAX_SSH_PER_HOUR           3
  MIN_HOST_COOLDOWN          1800 seconds
  DRY_RUN                    "1" to disable real actions (default 0)
  DOCTOR_TRIAGE_MODEL        diagnose/classify phase model (default deepseek)
  DOCTOR_REVIEW_MODEL        adversarial-review phase model (default codex)
  DOCTOR_FIX_MODEL           in-doctor fix phase model (default codex)
  DOCTOR_OPENWEIGHT_BASE_URL OpenAI-compatible base for non-codex phases
                             (default https://api.deepseek.com/v1)
  DOCTOR_OPENWEIGHT_MODEL    model id for the open-weight path (default deepseek-chat)
  DOCTOR_OPENWEIGHT_API_KEY  key for open-weight path (falls back to config.toml deepseek)
"""
from __future__ import annotations

import json
import logging
import os
import re
import signal
import shlex
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections import deque
from urllib.parse import quote

HIVE_URL = os.environ.get("HIVE_URL", "http://192.168.207.67:5005")
ZEROCLAW_BIN = os.environ.get("ZEROCLAW_BIN", "/usr/local/bin/zeroclaw")
DOCTOR_AGENT_ALIAS = os.environ.get("DOCTOR_AGENT_ALIAS", "hive_doctor")
AGENT_HOST = os.environ.get("AGENT_HOST", socket.gethostname().split(".")[0])
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "30"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "15"))
ZEROCLAW_TIMEOUT = int(os.environ.get("ZEROCLAW_TIMEOUT", "900"))
MAX_SSH_PER_HOUR = int(os.environ.get("MAX_SSH_PER_HOUR", "3"))
MIN_HOST_COOLDOWN = int(os.environ.get("MIN_HOST_COOLDOWN", "1800"))
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"
SUBMITTER_RUNTIME = "doctor"
# Loop-detection: per-job-id (re)fail counter. If the same job_id keeps
# coming back to the doctor (because we released it and some broken worker
# re-failed it), we escalate instead of releasing it again.
_seen_job_attempts: dict[str, list[float]] = {}
LOOP_WINDOW_SEC = float(os.environ.get("LOOP_WINDOW_SEC", "1800"))   # 30 min
LOOP_ESCALATE_THRESHOLD = int(os.environ.get("LOOP_ESCALATE_THRESHOLD", "3"))

# ── Per-phase model selection (2026-06-02) ──
# The doctor runs in phases; each phase picks its model independently.
#   triage  = diagnose/classify a failure cluster -> cheap open-weight (deepseek)
#   review  = adversarial review/verify of a fix  -> codex (its strength)
#   fix     = implement a code fix                -> codex by default, but the
#             implementation is normally DISPATCHED to open-weight workers via
#             action_dispatch_codex_fix(eligible_kinds=["zeroclaw"]); this knob
#             only governs any in-doctor model call labelled "fix".
# Value is a model alias: "codex" -> local codex CLI; anything else -> the
# open-weight OpenAI-compatible path (DOCTOR_OPENWEIGHT_*).
DOCTOR_TRIAGE_MODEL = os.environ.get("DOCTOR_TRIAGE_MODEL", "gateway").strip().lower()
DOCTOR_REVIEW_MODEL = os.environ.get("DOCTOR_REVIEW_MODEL", "codex").strip().lower()
DOCTOR_FIX_MODEL = os.environ.get("DOCTOR_FIX_MODEL", "codex").strip().lower()
_PHASE_MODELS = {
    "triage": DOCTOR_TRIAGE_MODEL,
    "review": DOCTOR_REVIEW_MODEL,
    "fix": DOCTOR_FIX_MODEL,
}
# Open-weight OpenAI-compatible endpoint used for any non-codex phase model.
# Defaults to DeepSeek direct (cheap, already keyed in ~/.zeroclaw/config.toml).
DOCTOR_OPENWEIGHT_BASE_URL = os.environ.get(
    "DOCTOR_OPENWEIGHT_BASE_URL", "https://api.deepseek.com/v1").rstrip("/")
DOCTOR_OPENWEIGHT_MODEL = os.environ.get("DOCTOR_OPENWEIGHT_MODEL", "deepseek-chat")
DOCTOR_OPENWEIGHT_API_KEY = os.environ.get("DOCTOR_OPENWEIGHT_API_KEY", "")
DOCTOR_OPENWEIGHT_TIMEOUT = int(os.environ.get("DOCTOR_OPENWEIGHT_TIMEOUT", "120"))

# Self-triage scanner config
SCAN_INTERVAL = float(os.environ.get("SCAN_INTERVAL", "300"))   # seconds between failed-cluster scans
CLUSTER_THRESHOLD = int(os.environ.get('CLUSTER_THRESHOLD', '1'))  # min failures of same base_kind to trigger a triage
CLUSTER_WINDOW_SEC = float(os.environ.get("CLUSTER_WINDOW_SEC", "21600"))  # only look at failures in last 6h
# To avoid re-triaging the same cluster repeatedly, we remember kinds we've already
# auto-triaged within this rolling window:
_recent_auto_triage: dict[str, float] = {}
AUTO_TRIAGE_COOLDOWN = float(os.environ.get("AUTO_TRIAGE_COOLDOWN", "3600"))  # 1h per base_kind

# ── Token-mismatch auto-fix (2026-05-27) ──
# When a worker fails with "no token for host" because its gateway-tokens.json
# 127.0.0.1 entry drifted away from the gateway's paired_tokens, the doctor
# auto-dispatches a codex fix without burning an LLM call.
# Rate-limit: max 3 fixes per host per hour to avoid loops.
_token_fix_attempts: dict[str, list[float]] = {}
MAX_TOKEN_FIX_PER_HOST = int(os.environ.get("MAX_TOKEN_FIX_PER_HOST", "3"))
TOKEN_FIX_WINDOW_SEC = float(os.environ.get("TOKEN_FIX_WINDOW_SEC", "3600"))  # 1 hour
FLEET_LAN_TOKEN = os.environ.get("FLEET_LAN_TOKEN", "fleet-lan-token-2026-05-26")
# Known failure signatures → automatic dispatch (no LLM needed)
KNOWN_FAILURE_SIGNATURES: list[tuple[re.Pattern, str]] = [
    (re.compile(r'no token for host', re.IGNORECASE), 'token_mismatch'),
    (re.compile(r'pair gateway first via POST /pair', re.IGNORECASE), 'token_mismatch'),
    (re.compile(r'gateway.*unauthorized.*token', re.IGNORECASE), 'token_mismatch'),
]
# Full fleet including Mac hosts (ULTRA, STUDIO) and sshpass hosts (MEDUSA, HYDRA)
ALL_FLEET_HOSTS: dict[str, dict] = {
    "ultra":    {"ip": "192.168.207.60", "os": "macos",   "auth": "passwordless", "user": "jasonperlow"},
    "studio":   {"ip": "192.168.207.10", "os": "macos",   "auth": "passwordless", "user": "jasonperlow"},
    "medusa":   {"ip": "192.168.207.64", "os": "linux",   "auth": "sshpass",      "user": "jasonperlow"},
    "hydra":    {"ip": "192.168.207.78", "os": "linux",   "auth": "sshpass",      "user": "jasonperlow"},
    "cerberus": {"ip": "192.168.207.96", "os": "linux",   "auth": "passwordless", "user": "jasonperlow"},
    "proteus":  {"ip": "192.168.207.25", "os": "linux",   "auth": "passwordless", "user": "jasonperlow"},
    "bigpi":    {"ip": "192.168.207.65", "os": "linux",   "auth": "passwordless", "user": "jasonperlow"},
    "clawpi":   {"ip": "192.168.207.54", "os": "linux",   "auth": "passwordless", "user": "jasonperlow"},
    "zeropi":   {"ip": "192.168.207.56", "os": "linux",   "auth": "passwordless", "user": "jasonperlow"},
    "typhon":   {"ip": "192.168.207.61", "os": "linux",   "auth": "passwordless", "user": "jasonperlow"},
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [doctor] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("doctor")

_running = True
_urn: str = ""
_session_id = str(uuid.uuid4())

# Rate-limit state for SSH restart actions
_ssh_actions_log: deque = deque()
_host_last_action: dict[str, float] = {}

# Allowlisted services for restart_service action
ALLOWED_SERVICE_RE = re.compile(
    r"^(zeroclaw-worker(@[a-z0-9-]+)?|zeroclaw-fanout|zeroclaw-doctor|"
    r"hive-triage|graeae-system-watcher|goose-worker(@[a-z0-9-]+)?)\.service$"
)

# Fleet hosts eligible for SSH restart actions (NOT PYTHIA itself, NOT ARGOS)
FLEET_HOSTS = {
    "cerberus": "192.168.207.96",
    "medusa":   "192.168.207.64",
    "proteus":  "192.168.207.25",
    "hydra":    "192.168.207.78",
    "bigpi":    "192.168.207.65",
    "clawpi":   "192.168.207.54",
    "zeropi":   "192.168.207.56",
    "typhon":   "192.168.207.61",
}
# Hosts the doctor must NEVER restart — production-critical only.
# (User directive 2026-05-26: cixmini is one of our most powerful systems —
# DO NOT blacklist it. Fix config-side bugs instead.)
RESTART_BLOCKED_HOSTS = {"argos", "pythia"}
SSH_TIMEOUT = 15


def _signal(signum, frame):
    global _running
    log.info("signal %s — shutting down after current job", signum)
    _running = False


signal.signal(signal.SIGTERM, _signal)
signal.signal(signal.SIGINT, _signal)


# ───────────────────────── HTTP helpers ─────────────────────────
def _http(method: str, path: str, body: dict | None = None, timeout: float = 15.0) -> tuple[int, dict | None]:
    url = f"{HIVE_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"content-type": "application/json"})
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
        log.warning("http %s %s: %s", method, path, e)
        return 0, None


# ───────────────────────── Registration ─────────────────────────
def register() -> str:
    global _urn
    body = {
        "host": AGENT_HOST,
        "kind": "doctor",
        "runtime": "doctor",
        # Per-phase models (2026-06-02): triage model is the doctor's primary
        # diagnose model; review/fix models surfaced in metadata.phase_models.
        "model": DOCTOR_TRIAGE_MODEL,
        "provider": ("codex" if DOCTOR_TRIAGE_MODEL in ("gateway", "codex", "openai", "gpt") else "openai-compatible"),
        "autonomy_level": "autonomous",
        "auth_method": "subscription",
        "capabilities": [
            "triage", "diagnose", "fix-agents", "fix-code",
            "dispatch-codex", "resubmit-jobs", "restart-services",
            "bus-http-read", "bus-http-write",
        ],
        "version": "zeroclaw-doctor/1.0",
        "metadata": {
            "agent_alias": DOCTOR_AGENT_ALIAS,
            "session_id": _session_id,
            "dry_run": DRY_RUN,
            "daemon": "zeroclaw_doctor.py",
            "phase_models": dict(_PHASE_MODELS),
        },
    }
    for attempt in range(1, 11):
        code, resp = _http("POST", "/v1/agents/register", body)
        if code in (200, 201) and resp and resp.get("urn"):
            _urn = resp["urn"]
            log.info("registered urn=%s alias=%s", _urn, DOCTOR_AGENT_ALIAS)
            return _urn
        log.warning("register attempt %d failed code=%s resp=%s", attempt, code, str(resp)[:200])
        time.sleep(min(30, 5 * attempt))
    log.error("could not register doctor after 10 attempts — exiting")
    sys.exit(1)


def heartbeat():
    code, _ = _http("POST", "/v1/agents/heartbeat", {"urn": _urn, "status": "online"})
    if code == 404:
        log.warning("heartbeat 404 — re-registering")
        register()


# ───────────────────────── Job claim/patch ─────────────────────────
def claim_next_job() -> dict | None:
    code, resp = _http("POST", f"/v1/jobs/next?agent_urn={_urn}")
    if code == 204:
        return None
    if code in (200, 201) and resp:
        return resp if isinstance(resp, dict) else None
    if code not in (0, 204, 404):
        log.warning("claim_next %s: %s", code, str(resp)[:200])
    return None


def patch_job(job_id: str, status: str, result: dict | None = None):
    # Bus requires `claimed_by` to match the current claimant URN — using
    # any other field name (e.g. `agent_urn`) yields 403 in the patch_job
    # ownership check at agent_bus.py:1690.
    body: dict = {"status": status, "claimed_by": _urn}
    if result is not None:
        body["result"] = result
    _http("PATCH", f"/v1/jobs/{job_id}", body)


# ───────────────────────── Failure context (HTTP API) ─────────────────────────
# Use the bus HTTP API for job context so the doctor stays behind the same
# queue contract as the rest of the fleet.
def fetch_jobs_for_kind(base_kind: str, status: str = "failed", limit: int = 10) -> list[dict]:
    """Recent jobs of status whose kind starts with base_kind, newest first."""
    code, resp = _http("GET", f"/v1/jobs?status={status}&limit=200", timeout=15.0)
    if code != 200 or not resp:
        log.warning("fetch_%s_jobs http=%s", status, code)
        return []
    jobs = resp.get("jobs", []) or []
    # match kind prefix OR exact base_kind after stripping [tag] suffix
    strip_re = re.compile(r"\s*\[.*?\]\s*$")
    matched = [
        j for j in jobs
        if (j.get("kind") or "").startswith(base_kind)
        or strip_re.sub("", j.get("kind", "")).strip() == base_kind
    ]
    matched.sort(key=lambda j: j.get("ended_at") or 0, reverse=True)
    return matched[:limit]


def fetch_failed_jobs_for_kind(base_kind: str, limit: int = 10) -> list[dict]:
    """Recent failed jobs whose kind starts with base_kind, newest first."""
    return fetch_jobs_for_kind(base_kind, "failed", limit)


def fetch_agent_registry_summary() -> dict:
    """Snapshot of agents grouped by kind / host / status via /v1/agents."""
    out = {"by_kind": {}, "by_host": {}, "by_status": {}}
    code, resp = _http("GET", "/v1/agents", timeout=10.0)
    if code != 200 or not resp:
        log.warning("fetch_agents http=%s", code)
        return out
    for a in resp.get("agents", []) or []:
        for col, key in (("kind", "by_kind"), ("host", "by_host"), ("status", "by_status")):
            v = a.get(col) or "(null)"
            out[key][v] = out[key].get(v, 0) + 1
    return out


# ───────────────────────── Doctor LLM prompt ─────────────────────────
DOCTOR_SYSTEM = """\
You are the PYTHIA Hive Mind doctor. You diagnose failed jobs and decide
how to release them back to the queue or take corrective action.

CRITICAL RULES:
  1. DO NOT call any tools. Tools are disabled. Calling them aborts you.
  2. DO NOT output any prose. No greetings, no explanation, no markdown.
  3. Your ENTIRE response must be ONE valid JSON object, nothing else.
  4. Begin your response with `{` and end with `}`. No code fences.

Output STRICT JSON only (no prose, no markdown fences), one object:
  "action_type": "release_to_queue" | "restart_service" | "resubmit_jobs" |
                 "cancel_jobs" | "dispatch_codex_fix" | "no_action" |
                 "escalate"
  "confidence":  0.0..1.0
  "reason":      short string explaining the diagnosis
  "release_eligible_kinds": list of kinds that should be retried, e.g.
                 ["zeroclaw","codex","opencode","goose"], for release_to_queue.
                 Use [] (empty) to allow any worker.  Use ["doctor"] to
                 keep the job in the doctor pool (only if you want re-diagnosis).
  "target_host": fleet hostname or null (cixmini|cerberus|medusa|proteus|
                 hydra|bigpi|clawpi|zeropi|typhon)
  "service_name":"zeroclaw-worker@1.service" pattern or null
  "resubmit_base_kind": kind prefix to resubmit (for resubmit_jobs/cancel_jobs)
  "codex_task":  short imperative task for codex sub-job, or null
  "max_resubmits": integer cap, default 20

When to use each action_type:
  - "release_to_queue": DEFAULT for a failed-job claim. You've inspected the
    failure and want to put it back into the queue so other workers (or
    specific kinds) can retry. Provide release_eligible_kinds.  This is
    the right answer for most claimed fail jobs.
  - "restart_service": failure pattern is clearly a stuck worker (timeout,
    no heartbeat). Provide target_host AND service_name.
  - "resubmit_jobs": broad pattern across MANY failures of same base_kind
    look transient. Operates on the failed queue at large.
  - "cancel_jobs": failures are structural (payload malformed, kind dead).
  - "dispatch_codex_fix": code bug in our workers / scripts. Provide codex_task.
  - "no_action": failure is informational only.
  - "escalate": confidence < 0.5 or beyond doctor authority.
"""


def build_doctor_prompt(job: dict, base_kind: str, failed: list[dict],
                        registry: dict, failure_count_hint: int,
                        source_status: str = "failed") -> str:
    status_label = "DEAD-LETTER" if source_status == "dead-letter" else "FAILED"
    msg = [
        "TRIAGE REQUEST",
        f"triage_job_id: {job.get('id', '')}",
        f"triage_kind: {job.get('kind', '')}",
        f"base_kind: {base_kind}",
        f"source_status: {source_status}",
        f"failure_count_hint: {failure_count_hint}",
        "",
        "TRIAGE DESCRIPTION:",
        (job.get("description") or "")[:6000],
        "",
        f"RECENT {status_label} JOBS OF base_kind={base_kind!r} ({len(failed)} shown):",
    ]
    for fj in failed[:8]:
        result_preview = ""
        if fj.get("result"):
            try:
                rd = json.loads(fj["result"]) if isinstance(fj["result"], str) else fj["result"]
                txt = (rd.get("stdout") or rd.get("error") or "")[:400]
                exit_code = rd.get("exit_code")
                result_preview = f"exit={exit_code} | {txt}"
            except Exception:
                result_preview = str(fj["result"])[:400]
        msg.append(
            f"  - id={fj.get('id', '')[:12]} kind={fj.get('kind', '')} "
            f"claimed_by={fj.get('claimed_by', '')} "
            f"provider={fj.get('claimed_provider', '')} "
            f"model={fj.get('claimed_model', '')}"
        )
        if result_preview:
            msg.append(f"    result: {result_preview}")
    msg.extend([
        "",
        "AGENT REGISTRY SUMMARY:",
        f"  by_kind: {json.dumps(registry.get('by_kind', {}))}",
        f"  by_host: {json.dumps(registry.get('by_host', {}))}",
        f"  by_status: {json.dumps(registry.get('by_status', {}))}",
        "",
        "DEAD-LETTER POLICY:",
        "  If source_status=dead-letter, do not reopen the terminal job.",
        "  Choose resubmit_jobs only when the observed cause looks transient or now-fixed,",
        "  such as thrash_guard:max_decline_requeues_exceeded, no_gateway_token,",
        "  ws_exception, network, timeout, gateway transport, connection reset/refused.",
        "  Choose no_action for test/junk work or clear permanent failures.",
        "  Choose dispatch_codex_fix or escalate when the dead-letter points to a real code/config bug.",
        "",
        "Respond with the JSON action object only.",
    ])
    return "\n".join(msg)


DOCTOR_GATEWAY_HOST = os.environ.get("GATEWAY_HOST", "127.0.0.1").strip()
DOCTOR_GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "42617"))
DOCTOR_GATEWAY_TIMEOUT = int(os.environ.get("DOCTOR_GATEWAY_TIMEOUT", "180"))


def _invoke_gateway(prompt: str) -> tuple[bool, str]:
    """Reason via the LOCAL zeroclaw gateway codex chain — the SAME path the
    workers use. The gateway KNEMON-routes POST /webhook to codex/gpt over
    ChatGPT-OAuth ($0, no API key, not the codex CLI subprocess). This is the
    doctor's DEFAULT reasoning path per operator doctrine 2026-06-03
    (codex/OpenAI-OAuth first; open-weight only on a genuine gateway outage)."""
    full = DOCTOR_SYSTEM + "\n\n" + prompt
    url = f"http://{DOCTOR_GATEWAY_HOST}:{DOCTOR_GATEWAY_PORT}/webhook"
    data = json.dumps({"message": full}).encode()
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"content-type": "application/json"})
    log.info("invoking gateway codex path %s prompt_chars=%d", url, len(full))
    try:
        with urllib.request.urlopen(req, timeout=DOCTOR_GATEWAY_TIMEOUT) as r:
            resp = json.loads(r.read())
        text = (resp.get("response") or resp.get("message") or "").strip()
        if text:
            return True, text
        return False, f"gateway empty response: {str(resp)[:300]}"
    except urllib.error.HTTPError as e:
        return False, f"gateway HTTP {e.code}: {e.read()[:300]!r}"
    except Exception as e:
        return False, f"gateway call failed: {e}"


def _gateway_failure_allows_fallback(reason: str) -> bool:
    """Only genuine gateway transport outages may fall back to open-weight."""
    text = (reason or "").lower()
    if re.search(r"gateway http (502|503|504)\b", text):
        return True
    if not text.startswith("gateway call failed:"):
        return False
    outage_markers = (
        "connection refused",
        "connection reset",
        "connection aborted",
        "timed out",
        "timeout",
        "network is unreachable",
        "no route to host",
        "temporarily unavailable",
    )
    return any(marker in text for marker in outage_markers)


CODEX_BIN = os.environ.get("CODEX_BIN", "/usr/local/bin/codex")
CODEX_TIMEOUT = int(os.environ.get("CODEX_TIMEOUT", "120"))


def _invoke_codex_cli(prompt: str) -> tuple[bool, str]:
    """Run the codex CLI tool (ChatGPT subscription auth, no API key).
    User directive 2026-05-26: 'don't use 5.5 api, use the codex tool which
    is authed'. Codex auth lives in ~/.codex/auth.json — already on PYTHIA."""
    full = DOCTOR_SYSTEM + "\n\n" + prompt
    output_path = f"/tmp/zeroclaw-doctor-codex-{uuid.uuid4().hex}.txt"
    cmd = [
        CODEX_BIN,
        "exec",
        "--skip-git-repo-check",
        "--output-last-message",
        output_path,
        "-",
    ]
    log.info("invoking codex-cli prompt_chars=%d", len(full))
    try:
        r = subprocess.run(cmd, input=full, capture_output=True, text=True,
                           timeout=CODEX_TIMEOUT)
        if r.returncode != 0:
            log.warning("codex-cli rc=%d stderr=%s", r.returncode, r.stderr[-300:])
            return False, r.stderr[-1000:] or r.stdout[-1000:]
        try:
            with open(output_path) as f:
                text = f.read().strip()
            if text:
                return True, text
        except OSError as e:
            log.warning("codex output file unreadable: %s", e)
        out = r.stdout.strip()
        m = re.search(r"\ncodex\n(.*?)(?:\ntokens used|\Z)", out, re.DOTALL)
        if m:
            return True, m.group(1).strip()
        return True, out
    except subprocess.TimeoutExpired:
        return False, f"codex-cli timeout after {CODEX_TIMEOUT}s"
    except FileNotFoundError:
        return False, f"codex binary not found at {CODEX_BIN}"
    finally:
        try:
            os.unlink(output_path)
        except OSError:
            pass


def _openweight_api_key() -> str:
    """Resolve the open-weight API key: env override first, else the deepseek
    key already provisioned in ~/.zeroclaw/config.toml (no extra secret store)."""
    if DOCTOR_OPENWEIGHT_API_KEY:
        return DOCTOR_OPENWEIGHT_API_KEY
    try:
        cfg = os.path.expanduser("~/.zeroclaw/config.toml")
        with open(cfg) as f:
            content = f.read()
        # First api_key under any [providers.models.deepseek.*] block.
        m = re.search(
            r"\[providers\.models\.deepseek\.[^\]]+\][^\[]*?api_key\s*=\s*\"([^\"]+)\"",
            content, re.DOTALL)
        if m:
            return m.group(1)
    except OSError:
        pass
    return ""


def _invoke_openweight(prompt: str, model_alias: str) -> tuple[bool, str]:
    """Run a non-codex (open-weight) phase via an OpenAI-compatible chat
    endpoint. Used for cheap phases like triage classification. Anthropic +
    Together remain FORBIDDEN per CLAUDE.md — default endpoint is DeepSeek
    direct. Returns (ok, text) with the same contract as the codex path."""
    api_key = _openweight_api_key()
    if not api_key:
        return False, (f"open-weight model {model_alias!r} requested but no API key "
                       "(set DOCTOR_OPENWEIGHT_API_KEY or deepseek key in config.toml)")
    body = {
        "model": DOCTOR_OPENWEIGHT_MODEL,
        "messages": [
            {"role": "system", "content": DOCTOR_SYSTEM},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.0,
        "stream": False,
    }
    url = f"{DOCTOR_OPENWEIGHT_BASE_URL}/chat/completions"
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"content-type": "application/json",
                 "authorization": f"Bearer {api_key}"})
    log.info("invoking open-weight model=%s alias=%s prompt_chars=%d",
             DOCTOR_OPENWEIGHT_MODEL, model_alias, len(prompt))
    try:
        with urllib.request.urlopen(req, timeout=DOCTOR_OPENWEIGHT_TIMEOUT) as r:
            resp = json.loads(r.read())
        text = (resp.get("choices") or [{}])[0].get("message", {}).get("content", "")
        if text and text.strip():
            return True, text.strip()
        return False, f"open-weight empty response: {str(resp)[:300]}"
    except urllib.error.HTTPError as e:
        return False, f"open-weight HTTP {e.code}: {e.read()[:300]!r}"
    except Exception as e:
        return False, f"open-weight call failed: {e}"


def invoke_doctor_agent(prompt: str, phase: str = "triage") -> tuple[bool, str]:
    """Run the doctor LLM for a given PHASE using its configured model.

    Phase model is selected via DOCTOR_<PHASE>_MODEL env (see _PHASE_MODELS):
      - "codex"        -> local Codex CLI (adversarial review's strength)
      - anything else  -> open-weight OpenAI-compatible path (e.g. deepseek)

    Codex remains the REVIEW/verify model; triage defaults to cheap open-weight.
    Anthropic/Together stay forbidden. Returns (ok, text); on failure the doctor
    job fails visibly rather than silently switching providers."""
    model_alias = _PHASE_MODELS.get(phase, DOCTOR_TRIAGE_MODEL)
    # Doctrine (operator 2026-06-03): the doctor reasons via the SAME local
    # gateway codex path the workers use — ChatGPT-OAuth ($0), no API key, NOT
    # the codex CLI subprocess, NEVER open-weight by default. The gateway
    # KNEMON-routes /webhook to codex/gpt. Open-weight (deepseek) is FALLBACK
    # only, for a genuine gateway outage.
    if model_alias in ("gateway", "codex", "openai", "gpt") or model_alias.startswith("gpt-"):
        ok, text = _invoke_gateway(prompt)
        if ok and text.strip():
            return True, text
        if not _gateway_failure_allows_fallback(text):
            return False, f"gateway codex failed: {text}"
        log.warning("gateway codex transport outage (%s) — open-weight fallback", str(text)[:200])
        ok2, text2 = _invoke_openweight(prompt, DOCTOR_OPENWEIGHT_MODEL)
        if ok2 and text2.strip():
            return True, text2
        return False, f"gateway codex failed: {text}; open-weight fallback failed: {text2}"
    ok, text = _invoke_openweight(prompt, model_alias)
    if ok and text.strip():
        return True, text
    return False, f"open-weight ({model_alias}) failed: {text}"


_CODE_FENCE_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)```", re.IGNORECASE)
_JSON_RE = re.compile(r"\{[\s\S]*\}", re.MULTILINE)


def parse_doctor_response(text: str) -> dict | None:
    """Extract the JSON action object from the LLM output. Tolerates markdown
    code fences and prose wrappers."""
    if not text:
        return None
    # First try: whole text is JSON (response_format=json_object case)
    try:
        obj = json.loads(text.strip())
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    # Second: strip markdown fences
    for cand in _CODE_FENCE_RE.findall(text):
        try:
            obj = json.loads(cand.strip())
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue
    # Third: largest braced span, prefer those with action_type
    candidates = _JSON_RE.findall(text)
    best = None
    for cand in candidates:
        try:
            obj = json.loads(cand)
            if not isinstance(obj, dict):
                continue
            if "action_type" in obj:
                return obj
            best = best or obj
        except Exception:
            continue
    return best


# ───────────────────────── Playbook actions ─────────────────────────
def _ssh_rate_ok(host: str) -> bool:
    now = time.time()
    while _ssh_actions_log and now - _ssh_actions_log[0] > 3600:
        _ssh_actions_log.popleft()
    if len(_ssh_actions_log) >= MAX_SSH_PER_HOUR:
        log.warning("SSH rate cap %d/h reached — skipping", MAX_SSH_PER_HOUR)
        return False
    last = _host_last_action.get(host, 0)
    if now - last < MIN_HOST_COOLDOWN:
        log.warning("host %s cooldown %ds remaining — skipping",
                    host, int(MIN_HOST_COOLDOWN - (now - last)))
        return False
    return True


def _record_ssh_action(host: str):
    now = time.time()
    _ssh_actions_log.append(now)
    _host_last_action[host] = now


def action_restart_service(target_host: str | None, service: str | None) -> str:
    if not target_host or not service:
        return "restart_service missing target_host or service"
    if target_host.lower() in RESTART_BLOCKED_HOSTS:
        return f"{target_host} is in restart blocklist (known-broken or production) — REFUSED"
    ip = FLEET_HOSTS.get(target_host.lower())
    if not ip:
        return f"unknown host {target_host!r}"
    if not ALLOWED_SERVICE_RE.match(service):
        return f"service {service!r} not in allowlist"
    if not _ssh_rate_ok(target_host):
        return f"rate-limited on {target_host}"
    if DRY_RUN:
        _record_ssh_action(target_host)
        return f"DRY_RUN: would restart {service} on {target_host} ({ip})"
    cixmini_password = os.environ.get("CIXMINI_SSH_PASSWORD")
    sudo_password = os.environ.get("DOCTOR_SUDO_PASSWORD")
    if target_host.lower() == "cixmini":
        if not cixmini_password:
            return "restart_service refused: CIXMINI_SSH_PASSWORD is not set"
        cmd = [
            "sshpass", "-p", cixmini_password,
            "ssh", "-o", "PubkeyAuthentication=no",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10",
            f"mini@{ip}",
            f"printf '%s\\n' {shlex.quote(cixmini_password)} | sudo -S systemctl restart {service}",
        ]
    else:
        restart_cmd = f"sudo -n systemctl restart {service}"
        if sudo_password:
            restart_cmd = (
                f"{restart_cmd} 2>/dev/null || "
                f"printf '%s\\n' {shlex.quote(sudo_password)} | sudo -S systemctl restart {service}"
            )
        cmd = [
            "ssh", "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10",
            f"jasonperlow@{ip}",
            restart_cmd,
        ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=SSH_TIMEOUT)
        if r.returncode == 0:
            _record_ssh_action(target_host)
            return f"restarted {service} on {target_host}"
        return f"restart FAILED on {target_host}: {r.stderr[-200:]}"
    except subprocess.TimeoutExpired:
        return f"SSH timeout to {target_host}"


DEAD_LETTER_RESUBMIT_CAP = int(os.environ.get("DEAD_LETTER_RESUBMIT_CAP", "2"))


def _doctor_submitted_jobs(limit: int = 1000) -> list[dict]:
    if not _urn:
        return []
    code, resp = _http("GET", f"/v1/jobs?agent_urn={_urn}&limit={limit}", timeout=15.0)
    if code != 200 or not resp:
        log.warning("fetch doctor-submitted jobs http=%s", code)
        return []
    return resp.get("jobs", []) or []


def _dead_letter_resubmit_count(source_job_id: str) -> int:
    if not source_job_id:
        return 0
    source_q = quote(source_job_id, safe="")
    code, resp = _http("GET", f"/v1/jobs?parent_job_id={source_q}&limit=1", timeout=15.0)
    if code != 200 or not resp:
        log.warning("fetch dead-letter resubmit count source=%s http=%s", source_job_id[:12], code)
        return DEAD_LETTER_RESUBMIT_CAP
    return int(resp.get("total") or resp.get("count") or 0)


def _dead_letter_sources_all_at_cap(sources: list[dict]) -> bool:
    if not sources:
        return False
    return all(_dead_letter_resubmit_count(j.get("id") or "") >= DEAD_LETTER_RESUBMIT_CAP
               for j in sources)


def _clone_dead_letter_job(source: dict) -> tuple[bool, str]:
    source_id = source.get("id") or ""
    if not source_id:
        return False, "dead-letter source missing id"
    prior = _dead_letter_resubmit_count(source_id)
    if prior >= DEAD_LETTER_RESUBMIT_CAP:
        return False, (f"skip {source_id[:12]}: already resubmitted "
                       f"{prior} times (cap={DEAD_LETTER_RESUBMIT_CAP})")
    body = {
        "kind": source.get("kind") or "",
        "description": source.get("description") or "",
        "submitter_urn": _urn,
        "parent_job_id": source_id,
        "priority": int(source.get("priority") or 0),
        "idempotency_key": (
            f"doctor-deadletter-resubmit:{source_id}:{prior + 1}:{int(time.time())}"
        ),
    }
    for field in ("required_capabilities", "eligible_kinds", "eligible_hosts"):
        val = source.get(field)
        if val:
            body[field] = val
    if not body["kind"]:
        return False, f"skip {source_id[:12]}: missing kind"
    if DRY_RUN:
        return True, f"DRY_RUN: would submit fresh retry for {source_id[:12]}"
    code, resp = _http("POST", "/v1/jobs", body, timeout=15.0)
    if code in (200, 201) and resp:
        return True, f"submitted fresh retry id={resp.get('id', '?')[:12]} source={source_id[:12]}"
    return False, f"submit fresh retry FAILED source={source_id[:12]} code={code} resp={str(resp)[:200]}"


def action_resubmit_dead_letter_jobs(base_kind: str | None, max_resubmits: int = 20) -> str:
    if not base_kind:
        return "resubmit_dead_letter_jobs missing base_kind"
    jobs = fetch_jobs_for_kind(base_kind, "dead-letter", limit=200)
    target_jobs = jobs[:max(1, int(max_resubmits))]
    if not target_jobs:
        return f"no dead-letter jobs of base_kind={base_kind}"
    submitted = 0
    skipped = 0
    notes = []
    for source in target_jobs:
        ok, msg = _clone_dead_letter_job(source)
        submitted += 1 if ok else 0
        skipped += 0 if ok else 1
        notes.append(msg)
    broadcast_doctor_message("doctor.resubmit.dead_letter", {
        "host": AGENT_HOST,
        "base_kind": base_kind,
        "submitted": submitted,
        "skipped": skipped,
    })
    return (f"dead-letter fresh retries submitted={submitted} skipped={skipped}; "
            + " | ".join(notes[:5]))


def action_resubmit_jobs(base_kind: str | None, max_resubmits: int = 20,
                         source_status: str = "failed") -> str:
    if source_status == "dead-letter":
        return action_resubmit_dead_letter_jobs(base_kind, max_resubmits)
    if not base_kind:
        return "resubmit_jobs missing base_kind"
    code, resp = _http("GET", "/v1/jobs?status=failed&limit=200")
    if code != 200 or not resp:
        return f"could not fetch failed jobs (code={code})"
    failed = resp.get("jobs", [])
    strip_re = re.compile(r"\s*\[.*?\]\s*$")
    target_ids = [
        j["id"] for j in failed
        if strip_re.sub("", j.get("kind", "")).strip() == base_kind
    ][:max(1, int(max_resubmits))]
    if not target_ids:
        return f"no failed jobs of base_kind={base_kind}"
    if DRY_RUN:
        return f"DRY_RUN: would requeue {len(target_ids)} jobs of {base_kind}"
    code2, resp2 = _http("POST", "/v1/admin/jobs/requeue", {
        "job_ids": target_ids,
        "reason": f"doctor-resubmit:{base_kind}",
    })
    return f"requeued {len(target_ids)} jobs of {base_kind} (api={code2})"


def action_cancel_jobs(base_kind: str | None, max_cancels: int = 20) -> str:
    if not base_kind:
        return "cancel_jobs missing base_kind"
    code, resp = _http("GET", "/v1/jobs?status=failed&limit=200")
    if code != 200 or not resp:
        return f"could not fetch failed jobs (code={code})"
    failed = resp.get("jobs", [])
    strip_re = re.compile(r"\s*\[.*?\]\s*$")
    target_ids = [
        j["id"] for j in failed
        if strip_re.sub("", j.get("kind", "")).strip() == base_kind
    ][:max(1, int(max_cancels))]
    if not target_ids:
        return f"no failed jobs of base_kind={base_kind}"
    if DRY_RUN:
        return f"DRY_RUN: would cancel {len(target_ids)} jobs of {base_kind}"
    # Note: terminal-status jobs (failed/cancelled/done) cannot be re-PATCHed
    # via this endpoint per bus contract. To truly cancel failed jobs use the
    # admin requeue endpoint to bring them back to queued first, then handle.
    # For "clear from queue" intent we just record the cancel intent in a
    # broadcast message.
    broadcast_doctor_message("doctor.cancel_intent", {
        "host": AGENT_HOST,
        "base_kind": base_kind,
        "job_ids": target_ids,
        "reason": "doctor-determined-structural-failure",
    })
    return f"recorded cancel intent for {len(target_ids)} jobs of {base_kind}"


DEFAULT_RELEASE_KINDS = ["zeroclaw", "codex", "opencode", "goose"]


def action_release_to_queue(job_id: str, release_eligible_kinds: list | None) -> str:
    """Return the claimed job back to the regular queue so other workers
    (NOT doctor) can retry it. Implements the user's directive:
    'once doctor grabs a fail, it needs to move it into its queue until it
    releases it back into the regular queue'.

    CRITICAL: release MUST exclude 'doctor' from eligible_kinds, otherwise
    the doctor pool will re-claim the job and trigger a loop (caught by
    loop detection but burns LLM calls and human attention)."""
    if release_eligible_kinds and isinstance(release_eligible_kinds, list):
        new_kinds = [k for k in release_eligible_kinds if k and k != "doctor"]
    else:
        new_kinds = []
    # If LLM didn't give us anything usable, broaden to all workers.
    if not new_kinds:
        new_kinds = list(DEFAULT_RELEASE_KINDS)
    if DRY_RUN:
        return f"DRY_RUN: would release {job_id[:12]} with eligible_kinds={new_kinds}"
    # Bus contract: eligible_kinds can only be updated on queued/offered jobs.
    # Our job is currently 'running' (we patched it on claim), so we must
    # release in TWO steps: (1) status running→queued (clears claimed_by),
    # (2) PATCH eligible_kinds on the now-queued job.
    code1, resp1 = _http("PATCH", f"/v1/jobs/{job_id}",
                         {"status": "queued", "claimed_by": _urn})
    if code1 not in (200, 201, 204):
        return f"release step 1 FAILED code={code1} resp={str(resp1)[:200]}"
    code2, resp2 = _http("PATCH", f"/v1/jobs/{job_id}",
                         {"status": "queued", "eligible_kinds": new_kinds})
    if code2 in (200, 201, 204):
        return f"released {job_id[:12]} → queued, eligible_kinds={new_kinds}"
    # Step 1 already succeeded so the job IS released; the kind retag just failed.
    return (f"released {job_id[:12]} → queued (eligible_kinds retag failed "
            f"code={code2} resp={str(resp2)[:200]})")


def action_dispatch_codex_fix(codex_task: str | None, parent_job_id: str) -> str:
    """Submit a codex sub-job for code-level fix. Requires registered orchestrator submitter."""
    if not codex_task:
        return "dispatch_codex_fix missing codex_task"
    if len(codex_task) > 4000:
        codex_task = codex_task[:4000]
    body = {
        "kind": f"doctor:codex-fix:{int(time.time())}",
        "description": codex_task,
        "submitter_urn": _urn,
        "parent_job_id": parent_job_id,
        # codex-first (operator 2026-06-03): the fix runs on a codex-capable
        # zeroclaw worker (ChatGPT-OAuth, NO API key). hard:codex gates it to
        # codex workers only AND stops the doctor (no codex cap) from re-claiming
        # its own dispatch — the self-loop that thrashed deepseek.
        "eligible_kinds": ["zeroclaw"],
        "max_cost_tier": "C",
        "priority": 80,
        "required_capabilities": ["hard:codex"],
    }
    if DRY_RUN:
        return f"DRY_RUN: would submit codex sub-job ({len(codex_task)} chars)"
    code, resp = _http("POST", "/v1/jobs", body)
    if code in (200, 201) and resp:
        return f"codex sub-job submitted id={resp.get('id', '?')[:12]}"
    return f"codex submit FAILED code={code} resp={str(resp)[:200]}"


# ── Token-mismatch auto-fix (2026-05-27) ──
def _extract_host_from_job(job: dict, failed_jobs: list[dict]) -> str:
    """Extract offending hostname from URN patterns in claimed_by or kind fields.
    URN format: urn:agent:<kind>:<host>:<session_id>"""
    # Check the triage job itself
    for field in ("claimed_by", "submitter_urn"):
        val = job.get(field) or ""
        m = re.search(r'urn:agent:\w+:(\w+)', val)
        if m:
            return m.group(1).lower()
    # Check recent failed jobs
    for fj in failed_jobs[:10]:
        for field in ("claimed_by", "submitter_urn"):
            val = fj.get(field) or ""
            m = re.search(r'urn:agent:\w+:(\w+)', val)
            if m:
                return m.group(1).lower()
    # Try extracting from kind (e.g. "zeroclaw[ULTRA]")
    kind = job.get("kind") or ""
    m = re.search(r'\[(\w+)\]', kind)
    if m:
        return m.group(1).lower()
    return ""


def _check_token_fix_rate_limit(host: str) -> bool:
    """True if we're under the rate limit for this host. False = blocked."""
    now = time.time()
    attempts = _token_fix_attempts.setdefault(host.lower(), [])
    attempts[:] = [t for t in attempts if (now - t) <= TOKEN_FIX_WINDOW_SEC]
    return len(attempts) < MAX_TOKEN_FIX_PER_HOST


def _record_token_fix_attempt(host: str):
    now = time.time()
    _token_fix_attempts.setdefault(host.lower(), []).append(now)


def _build_token_fix_codex_task(offending_host: str) -> str:
    """Build an imperative codex task to fix gateway token mismatch on a host.
    Includes OS detection, auth method selection, and health-check verification."""
    host_info = ALL_FLEET_HOSTS.get(offending_host.lower(), {})
    ip = host_info.get("ip", offending_host)
    os_type = host_info.get("os", "linux")
    auth = host_info.get("auth", "passwordless")
    user = host_info.get("user", "jasonperlow")
    host_upper = offending_host.upper()

    # Build SSH command prefix based on auth method
    if auth == "sshpass":
        password_var = f"{offending_host.upper()}_SSH_PASSWORD"
        ssh_prefix = (
            f'PASSWORD="${{{password_var}:-$DOCTOR_SSH_PASSWORD}}"; '
            f'if [ -n "$PASSWORD" ]; then '
            f'sshpass -p "$PASSWORD" ssh -o PubkeyAuthentication=no '
            f'-o StrictHostKeyChecking=no -o ConnectTimeout=10 {user}@{ip}; '
            f'else ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 {user}@{ip}; fi'
        )
    else:
        ssh_prefix = f'ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 {user}@{ip}'

    # Build restart commands based on OS
    if os_type == "macos":
        restart_gateway = (
            f'launchctl bootout gui/$(id -u {user}) '
            f'~/Library/LaunchAgents/com.zeroclaw.gateway.plist 2>/dev/null || true; '
            f'sleep 1; '
            f'launchctl bootstrap gui/$(id -u {user}) '
            f'~/Library/LaunchAgents/com.zeroclaw.gateway.plist'
        )
        restart_worker = (
            f'pkill -f zeroclaw-wss-worker 2>/dev/null || true; '
            f'sleep 1; '
            f'nohup ~/zeroclaw-wss-worker &>/dev/null &'
        )
        restart_cmd = f'{restart_gateway}; {restart_worker}'
    else:
        restart_cmd = 'sudo systemctl restart zeroclaw-gateway; for i in 1 2 3 4; do sudo systemctl restart zeroclaw-wss-worker@$i; done'

    return f"""FLEET INFRA FIX: Gateway token mismatch on {offending_host} ({ip}, {os_type} via {auth})

DIAGNOSIS: Worker on {offending_host} is failing with 'no token for host' / 'pair gateway first via POST /pair'.
The gateway-tokens.json 127.0.0.1 entry has drifted from the gateway's paired_tokens in config.toml.

FLEET LAN TOKEN: {FLEET_LAN_TOKEN}

FIX STEPS — execute in order via SSH to {offending_host} ({ip}):

STEP 1 — SSH to host:
  {ssh_prefix}

STEP 2 — Fix gateway-tokens.json (MUST have both 127.0.0.1 AND hostname entries):
  cat > /tmp/gateway-tokens-fix.json << 'TOKENS_EOF'
{{
  "127.0.0.1": "{FLEET_LAN_TOKEN}",
  "{host_upper}": "{FLEET_LAN_TOKEN}"
}}
TOKENS_EOF
  # Merge with existing entries (preserve other host tokens already in file)
  python3 -c "
import json, sys
with open('/tmp/gateway-tokens-fix.json') as f:
    new = json.load(f)
try:
    with open(os.path.expanduser('~/.zeroclaw/gateway-tokens.json')) as f:
        old = json.load(f)
except Exception:
    old = {{}}
old.update(new)
with open(os.path.expanduser('~/.zeroclaw/gateway-tokens.json'), 'w') as f:
    json.dump(old, f, indent=2)
print('gateway-tokens.json updated')
"
  # Verify: cat ~/.zeroclaw/gateway-tokens.json

STEP 3 — Fix config.toml [gateway] paired_tokens:
  python3 -c "
import sys, os
path = os.path.expanduser('~/.zeroclaw/config.toml')
with open(path) as f:
    content = f.read()
# Ensure [gateway] section exists with paired_tokens
if '[gateway]' not in content:
    content += '\\n[gateway]\\npaired_tokens = [\\'{FLEET_LAN_TOKEN}\\']\\n'
elif 'paired_tokens' not in content.split('[gateway]')[1].split('[')[0] if '[gateway]' in content else '':
    content = content.replace('[gateway]', '[gateway]\\npaired_tokens = [\\'{FLEET_LAN_TOKEN}\\']')
else:
    import re
    content = re.sub(
        r'paired_tokens\\s*=\\s*\\[.*?\\]',
        'paired_tokens = [\\'{FLEET_LAN_TOKEN}\\']',
        content
    )
with open(path, 'w') as f:
    f.write(content)
print('config.toml updated')
"

STEP 4 — Restart services:
  {restart_cmd}

STEP 5 — Verify health (CRITICAL — must return paired:true):
  for attempt in 1 2 3 4 5; do
    result=$(curl -s -m 5 http://127.0.0.1:42617/health 2>/dev/null)
    if echo "$result" | grep -q '"paired":true'; then
      echo "HEALTH CHECK PASSED: $result"
      break
    fi
    echo "attempt $attempt: paired not yet true, waiting..."
    sleep 2
  done
  if ! echo "$result" | grep -q '"paired":true'; then
    echo "CRITICAL: health check FAILED after fix — escalate to human"
    exit 1
  fi

CONSTRAINTS (READ CAREFULLY):
- DO NOT touch ~/.codex/auth.json or ~/.api_keys_master.json under ANY circumstance.
- Use sshpass with PubkeyAuthentication=no for MEDUSA and HYDRA.
- Mac hosts (ULTRA, STUDIO) use launchctl + nohup, NOT systemctl.
- Linux hosts use systemctl.
- If ANY step fails, report the exact error and escalate."""


def _match_known_signature(job: dict, base_kind: str,
                           failed: list[dict]) -> dict | None:
    """Check if the triage job or its associated failures match a known,
    auto-fixable signature. Returns an action dict (bypassing the LLM)
    or None if no signature matches.

    Current signatures:
      - token_mismatch: 'no token for host' / 'pair gateway first via POST /pair'
    """
    # Build searchable text from job description + failed-job results
    search_text = (job.get("description") or "")[:10000]
    for fj in failed[:8]:
        result = fj.get("result") or ""
        if isinstance(result, dict):
            try:
                result = json.dumps(result)
            except Exception:
                result = str(result)
        search_text += " " + str(result)[:3000]

    # Transient gateway/session error (ws-400 / ConnectionClosed) — e.g. a worker
    # restart severed the WSS mid-job. Release to queue WITHOUT burning codex-cli.
    _transient = ("ws_exception" in search_text or "ConnectionClosed" in search_text
                  or "reason_phrase='Bad Request'" in search_text)
    _structural = any(x in search_text for x in ("Traceback", "SyntaxError",
                      "no_workspace", "clone_failed", "no_zeroclaw_handler"))
    if _transient and not _structural:
        return {"action_type": "release_to_queue", "confidence": 0.9,
                "reason": "transient ws/gateway error (ws-400/ConnectionClosed) — retry; no LLM"}

    for pattern, sig_name in KNOWN_FAILURE_SIGNATURES:
        if not pattern.search(search_text):
            continue

        offending_host = _extract_host_from_job(job, failed)
        if not offending_host:
            log.info("signature matched '%s' but could not extract host — "
                     "falling through to LLM diagnosis", sig_name)
            return None

        # Check rate limit
        if not _check_token_fix_rate_limit(offending_host):
            count = len(_token_fix_attempts.get(offending_host.lower(), []))
            log.warning("token-fix rate cap reached for %s (%d in %ds) — escalating",
                        offending_host, count, int(TOKEN_FIX_WINDOW_SEC))
            return {
                "action_type": "escalate",
                "confidence": 1.0,
                "reason": (
                    f"token-fix rate-limit: {count} fixes dispatched for "
                    f"{offending_host} in last {int(TOKEN_FIX_WINDOW_SEC/3600)}h"
                ),
            }

        # Check host is known
        if offending_host.lower() not in ALL_FLEET_HOSTS:
            log.warning("token-fix: host %s not in ALL_FLEET_HOSTS — "
                        "falling through to LLM", offending_host)
            return None

        _record_token_fix_attempt(offending_host)
        log.info("AUTO-DETECT signature=%s host=%s — bypassing LLM, "
                 "dispatching codex fix directly", sig_name, offending_host)

        return {
            "action_type": "dispatch_codex_fix",
            "confidence": 0.95,
            "reason": (
                f"auto-detected signature '{sig_name}': "
                f"'no token for host' gateway-auth failure on {offending_host}"
            ),
            "codex_task": _build_token_fix_codex_task(offending_host),
            "target_host": offending_host,
            "signature": sig_name,
            "auto_detected": True,
        }

    return None


# ───────────────────────── Failed-cluster scanner (self-triage) ─────────────────────────
def _base_kind(kind: str) -> str:
    """Strip [tag] suffix to canonicalize the kind."""
    return re.sub(r"\s*\[.*?\]\s*$", "", kind or "").strip()


def scan_for_failure_clusters() -> list[dict]:
    """Inspect failed and dead-letter jobs; return clusters of N+ jobs of same
    base_kind within CLUSTER_WINDOW_SEC, not auto-triaged within cooldown.
    Returns list of {base_kind, source_status, count, sample_job_id}."""
    cutoff = time.time() - CLUSTER_WINDOW_SEC
    counts: dict[tuple[str, str], list[dict]] = {}
    for status in ("failed", "dead-letter"):
        code, resp = _http("GET", f"/v1/jobs?status={status}&limit=500", timeout=20.0)
        if code != 200 or not resp:
            continue
        for j in resp.get("jobs", []) or []:
            if (j.get("ended_at") or 0) < cutoff:
                continue
            bk = _base_kind(j.get("kind", ""))
            if not bk or bk.startswith("triage:") or bk.startswith("doctor:"):
                continue
            counts.setdefault((status, bk), []).append(j)
    clusters = []
    now = time.time()
    for (status, bk), jobs in counts.items():
        if len(jobs) < CLUSTER_THRESHOLD:
            continue
        triage_key = f"{status}:{bk}"
        last = _recent_auto_triage.get(triage_key, 0)
        if now - last < AUTO_TRIAGE_COOLDOWN:
            continue
        # Also skip if an active triage:<bk> already exists in queued/running
        if _active_triage_exists(bk, status):
            continue
        clusters.append({
            "base_kind": bk,
            "source_status": status,
            "count": len(jobs),
            "sample_job_id": jobs[0].get("id", ""),
        })
    return clusters


def _active_triage_exists(base_kind: str, source_status: str = "failed") -> bool:
    """True if a triage:<status>:<base_kind> job is currently queued or running."""
    for status in ("queued", "running"):
        code, resp = _http("GET", f"/v1/jobs?status={status}&limit=200", timeout=10.0)
        if code != 200 or not resp:
            continue
        target = f"triage:{source_status}:{base_kind}"
        legacy_target = f"triage:{base_kind}"
        for j in resp.get("jobs", []) or []:
            kind = j.get("kind") or ""
            if kind.startswith(target) or (
                source_status == "failed" and kind.startswith(legacy_target)
            ):
                # Also check it includes "doctor" in eligible_kinds OR is unrestricted
                ek = j.get("eligible_kinds") or []
                if not ek or "doctor" in ek:
                    return True
    return False


def submit_auto_triage(base_kind: str, count: int, sample_job_id: str,
                       source_status: str = "failed") -> str | None:
    """Submit a triage:<status>:<base_kind> job targeting eligible_kinds=['doctor']."""
    noun = "dead-letter" if source_status == "dead-letter" else "fail"
    action_hint = (
        "For dead-letter clusters: reason via gateway-codex; if cause is transient "
        "or now-fixed, choose resubmit_jobs so the doctor submits fresh queued "
        "clones capped at two per source job. If test/junk or permanent, choose "
        "no_action/escalate and leave the terminal jobs in place."
        if source_status == "dead-letter"
        else "Doctor: analyze failure pattern, diagnose root cause, dispatch fix "
             "(codex sub-job, service restart, or resubmit) and clear the failed cluster."
    )
    body = {
        "kind": f"triage:{source_status}:{base_kind}",
        "description": (
            f"HIVE AUTO-TRIAGE — {count} {noun} jobs of base_kind={base_kind} in last "
            f"{int(CLUSTER_WINDOW_SEC/3600)}h.\n"
            f"source_status: {source_status}\n"
            f"Sample {noun} job id: {sample_job_id}\n\n"
            f"{action_hint}"
        ),
        "submitter_urn": _urn,
        "priority": 90,
        "eligible_kinds": ["doctor"],
        "max_cost_tier": "B",
        "required_capabilities": ["triage"],
    }
    if DRY_RUN:
        log.info("DRY_RUN: would auto-submit triage:%s:%s (count=%d)",
                 source_status, base_kind, count)
        return None
    code, resp = _http("POST", "/v1/jobs", body, timeout=15.0)
    if code in (200, 201) and resp:
        jid = resp.get("id", "")
        _recent_auto_triage[f"{source_status}:{base_kind}"] = time.time()
        log.info("auto-triage submitted id=%s status=%s base_kind=%s count=%d",
                 jid[:12], source_status, base_kind, count)
        return jid
    log.warning("auto-triage submit failed code=%s status=%s base_kind=%s resp=%s",
                code, source_status, base_kind, str(resp)[:200])
    return None


def broadcast_doctor_message(topic: str, payload: dict):
    """Leave a message on the hive bus so other Claudes/agents can see what
    the doctor is doing. Non-blocking; failures logged but ignored.

    topic examples:
      'doctor.scan'    — completed a cluster scan
      'doctor.triage'  — taking action on a triage job
      'doctor.fix'     — dispatched a codex fix
      'doctor.resubmit' — resubmitted jobs
    """
    body = {
        "from_urn": _urn,
        "to_urn": None,  # broadcast
        "topic": topic,
        "payload": payload,
    }
    code, _ = _http("POST", "/v1/messages", body, timeout=5.0)
    if code not in (200, 201, 204):
        log.debug("broadcast %s failed code=%s", topic, code)


# ───────────────────────── Triage loop ─────────────────────────
def process_triage_job(job: dict) -> dict:
    """Process either an explicit triage:<base_kind> job OR any other job whose
    eligible_kinds includes 'doctor'.  The user's directive (2026-05-26):
    'ALL FAILS ARE DOCTOR ELIGIBLE' — so the doctor must be able to diagnose
    any failure, not only triage:* wrappers."""
    job_id = job.get("id", "")
    kind = job.get("kind", "")
    desc = job.get("description", "") or ""

    is_explicit_triage = kind.startswith("triage:")
    source_status = "failed"
    if is_explicit_triage:
        triage_target = kind[len("triage:"):]
        if triage_target.startswith("dead-letter:"):
            source_status = "dead-letter"
            base_kind = triage_target[len("dead-letter:"):]
        elif triage_target.startswith("failed:"):
            base_kind = triage_target[len("failed:"):]
        else:
            base_kind = triage_target
            m_status = re.search(r"source_status:\s*(dead-letter|failed)", desc)
            if m_status:
                source_status = m_status.group(1)
    else:
        base_kind = _base_kind(kind)

    m = re.search(r"(\d+)\s+(?:fail|dead-letter)", desc, re.IGNORECASE)
    failure_count = int(m.group(1)) if m else 0

    # Loop detection — if we've already cycled this job through the doctor
    # multiple times in the last 30min, escalate without burning another LLM
    # call.  This prevents broken workers (e.g. cixmini with empty config)
    # from creating an infinite release→fail→release cycle.
    now = time.time()
    history = _seen_job_attempts.setdefault(job_id, [])
    history[:] = [t for t in history if (now - t) <= LOOP_WINDOW_SEC]
    history.append(now)
    if len(history) >= LOOP_ESCALATE_THRESHOLD:
        log.warning("LOOP-DETECT job=%s seen %d times in %ds — escalating",
                    job_id[:12], len(history), int(LOOP_WINDOW_SEC))
        return {
            "exit_code": 0,
            "stdout": (f"action=escalate; conf=1.00; reason=loop-detect "
                       f"({len(history)} doctor-cycles in {int(LOOP_WINDOW_SEC)}s); "
                       f"likely a broken worker is reclaiming. Human review required."),
            "action": {"action_type": "escalate", "confidence": 1.0,
                       "reason": f"loop-detect {len(history)} attempts"},
            "base_kind": base_kind,
            "source_status": source_status,
            "loop_detected": True,
        }

    log.info("processing %s job=%s status=%s base_kind=%s failures=%d attempt=%d",
             "TRIAGE" if is_explicit_triage else "DIRECT",
             job_id[:12], source_status, base_kind, failure_count, len(history))

    failed = fetch_jobs_for_kind(base_kind, source_status, limit=10)
    if source_status == "dead-letter" and _dead_letter_sources_all_at_cap(failed):
        reason = (f"all {len(failed)} dead-letter source(s) already at "
                  f"resubmit cap={DEAD_LETTER_RESUBMIT_CAP}")
        log.info("dead-letter triage skipped base_kind=%s reason=%s", base_kind, reason)
        return {
            "exit_code": 0,
            "stdout": f"action=no_action; conf=1.00; reason={reason}",
            "action": {"action_type": "no_action", "confidence": 1.0, "reason": reason},
            "base_kind": base_kind,
            "source_status": source_status,
            "failed_count_seen": len(failed),
            "terminal_cluster_at_cap": True,
        }

    registry = fetch_agent_registry_summary()

    # ── Check known failure signatures BEFORE LLM call ──
    # If we match a known auto-fixable pattern (e.g. token mismatch),
    # dispatch the codex fix directly — no LLM tokens burned.
    known_action = None
    if source_status == "failed":
        known_action = _match_known_signature(job, base_kind, failed)
    if known_action:
        a = known_action.get("action_type", "no_action")
        reason = known_action.get("reason", "")
        log.info("doctor decision (SIGNATURE-MATCH): action=%s reason=%s", a, reason)
        parts = [f"action={a}", f"conf={known_action.get('confidence', 1.0):.2f}",
                 f"reason={reason}"]
        if a == "dispatch_codex_fix":
            parts.append(action_dispatch_codex_fix(
                known_action.get("codex_task"), job_id))
            broadcast_doctor_message("doctor.fix.token_mismatch", {
                "host": AGENT_HOST,
                "job_id": job_id,
                "target_host": known_action.get("target_host"),
                "signature": known_action.get("signature"),
                "base_kind": base_kind,
            })
            # Release the triage job back to queue so the offending host
            # (now fixed) or any other worker can reclaim it
            parts.append(action_release_to_queue(
                job_id, known_action.get("release_eligible_kinds")))
        elif a == "release_to_queue":
            parts.append(action_release_to_queue(
                job_id, known_action.get("release_eligible_kinds")))
        elif a == "escalate":
            parts.append("escalated to human")
            broadcast_doctor_message("doctor.escalate.token_mismatch", {
                "host": AGENT_HOST,
                "job_id": job_id,
                "reason": reason,
                "base_kind": base_kind,
            })
        else:
            parts.append(f"unknown auto-action {a!r}")
        return {
            "exit_code": 0,
            "stdout": "; ".join(parts),
            "action": known_action,
            "base_kind": base_kind,
            "source_status": source_status,
            "failed_count_seen": len(failed),
            "auto_detected": True,
            "released_to_queue": (a in ("dispatch_codex_fix", "release_to_queue")),
        }

    prompt = build_doctor_prompt(job, base_kind, failed, registry, failure_count,
                                 source_status)

    # Diagnose = triage phase (cheap open-weight by default). The doctor emits a
    # JSON action; any code FIX is dispatched to open-weight workers, and
    # adversarial REVIEW of that fix runs on codex (DOCTOR_REVIEW_MODEL).
    ok, text = invoke_doctor_agent(prompt, phase="triage")
    if not ok:
        return {
            "exit_code": 2,
            "stdout": "doctor agent failed",
            "stderr": text[:2000],
            "action_type": "escalate",
            "reason": "doctor LLM call failed",
        }

    action = parse_doctor_response(text)
    if not action:
        return {
            "exit_code": 3,
            "stdout": "could not parse doctor response",
            "raw": text[-2000:],
            "action_type": "escalate",
        }

    a = action.get("action_type", "no_action")
    conf = float(action.get("confidence", 0.0) or 0.0)
    reason = action.get("reason", "")
    log.info("doctor decision: action=%s conf=%.2f reason=%s", a, conf, reason)

    parts = [f"action={a}", f"conf={conf:.2f}", f"reason={reason}"]
    released = False  # whether we put the job back to queued (changes return shape)

    if a == "release_to_queue":
        if source_status == "dead-letter":
            parts.append("release_to_queue refused for dead-letter triage; terminal jobs left in place")
        else:
            rk = action.get("release_eligible_kinds")
            if isinstance(rk, list):
                rk_clean = [k for k in rk if isinstance(k, str) and k]
            else:
                rk_clean = None
            parts.append(action_release_to_queue(job_id, rk_clean))
            released = True
    elif a == "restart_service" and conf >= 0.7:
        parts.append(action_restart_service(
            action.get("target_host"), action.get("service_name")))
    elif a == "resubmit_jobs":
        parts.append(action_resubmit_jobs(
            action.get("resubmit_base_kind") or base_kind,
            int(action.get("max_resubmits", 20) or 20),
            source_status))
    elif a == "cancel_jobs":
        parts.append(action_cancel_jobs(
            action.get("resubmit_base_kind") or base_kind,
            int(action.get("max_resubmits", 20) or 20)))
    elif a == "dispatch_codex_fix":
        parts.append(action_dispatch_codex_fix(
            action.get("codex_task"), job_id))
    elif a == "no_action":
        parts.append("no action — triage cleared")
        if source_status == "dead-letter":
            broadcast_doctor_message("doctor.dead_letter.handled", {
                "host": AGENT_HOST,
                "job_id": job_id,
                "base_kind": base_kind,
                "reason": reason,
                "terminal_jobs_left_in_place": True,
            })
    elif a == "escalate":
        parts.append("escalated to human")
    else:
        parts.append(f"unknown action_type={a!r} — treated as escalate")

    return {
        "exit_code": 0,
        "stdout": "; ".join(parts),
        "action": action,
        "base_kind": base_kind,
        "source_status": source_status,
        "failed_count_seen": len(failed),
        "released_to_queue": released,
    }


# ───────────────────────── Main loop ─────────────────────────
def main():
    log.info("starting on %s url=%s alias=%s dry_run=%s",
             AGENT_HOST, HIVE_URL, DOCTOR_AGENT_ALIAS, DRY_RUN)
    register()
    last_hb = time.time()
    last_scan = 0.0

    while _running:
        try:
            now = time.time()
            if now - last_hb >= HEARTBEAT_INTERVAL:
                heartbeat()
                last_hb = now

            # Periodic failed-cluster scan — emit triage:* jobs targeting doctor.
            if now - last_scan >= SCAN_INTERVAL:
                last_scan = now
                try:
                    clusters = scan_for_failure_clusters()
                    if clusters:
                        log.info("scan: %d terminal clusters above threshold",
                                 len(clusters))
                        for c in clusters[:5]:  # cap to avoid burst-submit
                            submit_auto_triage(c["base_kind"], c["count"],
                                               c["sample_job_id"],
                                               c.get("source_status", "failed"))
                        broadcast_doctor_message("doctor.scan", {
                            "host": AGENT_HOST,
                            "clusters_found": len(clusters),
                            "auto_triaged": min(5, len(clusters)),
                            "statuses": sorted({c.get("source_status", "failed")
                                                for c in clusters}),
                        })
                except Exception as e:
                    log.warning("scan error: %s", e)

            job = claim_next_job()
            if not job:
                time.sleep(POLL_INTERVAL)
                continue

            job_id = job.get("id", "?")
            kind = job.get("kind", "?")
            log.info("claimed job %s kind=%s", job_id[:12], kind)
            patch_job(job_id, "running")
            broadcast_doctor_message("doctor.triage", {
                "host": AGENT_HOST,
                "job_id": job_id,
                "kind": kind,
                "stage": "claimed",
            })

            t0 = time.time()
            try:
                result = process_triage_job(job)
                dur = int(time.time() - t0)
                result.setdefault("duration_sec", dur)
                if result.get("released_to_queue"):
                    # Doctor already patched job back to queued via the action.
                    # Don't overwrite with done/failed; just log + broadcast.
                    status = "released"
                else:
                    status = "done" if result.get("exit_code", 1) == 0 else "failed"
                    patch_job(job_id, status, result)
                log.info("job %s → %s in %ds: %s",
                         job_id[:12], status, dur, result.get("stdout", "")[:200])
                broadcast_doctor_message("doctor.triage", {
                    "host": AGENT_HOST,
                    "job_id": job_id,
                    "kind": kind,
                    "stage": "complete",
                    "status": status,
                    "action_type": (result.get("action") or {}).get("action_type"),
                    "summary": result.get("stdout", "")[:240],
                    "duration_sec": dur,
                })
            except Exception as e:
                log.exception("process_triage_job crashed")
                patch_job(job_id, "failed", {
                    "exit_code": 99,
                    "error": f"{type(e).__name__}: {e}",
                })

        except KeyboardInterrupt:
            break
        except Exception as e:
            log.exception("loop error: %s", e)
            time.sleep(POLL_INTERVAL)

    log.info("clean shutdown")


if __name__ == "__main__":
    main()
