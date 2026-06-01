"""WSS driver module — to be wired into /home/jasonperlow/zeroclaw_doctor.py.

Connects to a host's zeroclaw gateway WebSocket endpoint, sends a task,
auto-approves shell/edit/git tool requests, harvests commits from
captured tool_calls, returns a structured result the bus can store.

Per singlerider's direction: the gateway WSS is the maintained automation
surface. The CLI `zeroclaw agent` path is one-shot and does not produce
real commits.

This module is import-safe (no side effects) so the doctor's existing
unit-test pattern still works.
"""
import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path
from urllib.parse import urlencode

import websockets

log = logging.getLogger("wss_driver")

# Per-host gateway tokens. Loaded lazily from /home/jasonperlow/.zeroclaw/gateway-tokens.json
# Format: {"PYTHIA": "zc_xxx", "TYPHON": "zc_yyy", ...}
TOKENS_FILE = Path(os.path.expanduser("~/.zeroclaw/gateway-tokens.json"))
HOST_PORT = 42617
SESSION_TIMEOUT_SEC = 600
AUTO_APPROVE_TOOLS = {"shell", "edit", "read", "list_files", "git", "write", "apply_patch"}
MAX_WS_FRAME_BYTES = 983_040  # leave websocket/proxy headroom below 1 MiB
MAX_CAPTURE_BYTES = 64_000
GIT_COMMIT_RE = re.compile(
    r"^[a-f0-9]{7,40}\b|"
    r"\[\S+\s+(?P<sha>[a-f0-9]{7,40})\]|"
    r"\bcommit\s+(?P<sha2>[a-f0-9]{40})\b",
    re.MULTILINE,
)

# ── Token Pricing Registry ────────────────────────────────────────────────
PRICING_REGISTRY_PATH = os.environ.get(
    "PRICING_REGISTRY_PATH",
    "/mnt/argonas/datapool/projects/fleet-registry/llm_provider_registry.json",
)
_pricing_cache: dict | None = None
_pricing_cache_ts: float = 0.0
PRICING_CACHE_TTL_SEC = 1800  # 30 min

def _load_pricing_registry() -> dict:
    global _pricing_cache, _pricing_cache_ts
    now = time.time()
    if _pricing_cache is not None and (now - _pricing_cache_ts) < PRICING_CACHE_TTL_SEC:
        return _pricing_cache
    try:
        raw = Path(PRICING_REGISTRY_PATH).read_text(encoding="utf-8")
        _pricing_cache = json.loads(raw)
        _pricing_cache_ts = now
        return _pricing_cache
    except Exception as e:
        log.warning("pricing_registry_load_failed: %s (using cached=%s)", e, _pricing_cache is not None)
        return _pricing_cache or {}

GATEWAY_PROVIDER_TO_REGISTRY = {
    "deepseek": "deepseek_direct",
    "openai": "openai", "xai": "xai", "groq": "groq",
    "anthropic": "anthropic", "google": "gemini", "gemini": "gemini",
    "together": "together", "siliconflow": "siliconflow",
    "bedrock": "bedrock", "ollama": "ollama",
    "nvidia": "ngc_integrate", "ngc": "ngc_integrate",
}

def compute_token_cost(gateway_provider, model, tokens_in, tokens_out, tokens_reasoning=0):
    """Returns (cost_usd_est, canonical_provider, canonical_model). cost_usd_est=None on miss."""
    registry = _load_pricing_registry()
    providers = registry.get("providers", {})
    canonical_provider = GATEWAY_PROVIDER_TO_REGISTRY.get(
        (gateway_provider or "").lower(), gateway_provider or "unknown")
    provider_entry = providers.get(canonical_provider, {})
    models = provider_entry.get("models", {}) if isinstance(provider_entry, dict) else {}
    model_entry = models.get(model)
    if model_entry is None:
        for k, v in models.items():
            if k.lower() == model.lower():
                model_entry = v; model = k; break
    if model_entry is None:
        log.warning("pricing_lookup_miss: provider=%s model=%s canonical=%s", gateway_provider, model, canonical_provider)
        return None, canonical_provider, model
    in_price = model_entry.get("cost_in_per_m")
    out_price = model_entry.get("cost_out_per_m")
    reasoning_price = model_entry.get("cost_reasoning_per_m", out_price)
    if in_price is None and out_price is None:
        return 0.0, canonical_provider, model
    if in_price == 0 and out_price == 0:
        return 0.0, canonical_provider, model
    in_price = in_price or 0.0; out_price = out_price or 0.0
    reasoning_price = reasoning_price or out_price
    cost = (tokens_in * in_price + tokens_out * out_price + tokens_reasoning * reasoning_price) / 1_000_000.0
    return round(cost, 6), canonical_provider, model


def _utf8_len(text: str) -> int:
    return len(text.encode("utf-8"))


def _truncate_utf8(text: str, max_bytes: int) -> tuple[str, bool]:
    data = text.encode("utf-8")
    if len(data) <= max_bytes:
        return text, False
    marker = "\n[truncated: websocket frame guard]\n"
    budget = max(0, max_bytes - len(marker.encode("utf-8")))
    truncated = data[:budget].decode("utf-8", errors="ignore") + marker
    return truncated, True


def _json_bytes(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


async def _send_json_bounded(ws, payload: dict, stats: dict) -> None:
    raw = _json_bytes(payload)
    stats["max_outbound_frame_bytes"] = max(stats.get("max_outbound_frame_bytes", 0), len(raw))
    if len(raw) <= MAX_WS_FRAME_BYTES:
        await ws.send(raw.decode("utf-8"))
        return

    bounded = dict(payload)
    content = bounded.get("content")
    if isinstance(content, str):
        overhead_payload = dict(bounded)
        overhead_payload["content"] = ""
        overhead = len(_json_bytes(overhead_payload))
        bounded_content, truncated = _truncate_utf8(content, max(0, MAX_WS_FRAME_BYTES - overhead - 64))
        bounded["content"] = bounded_content
        raw = _json_bytes(bounded)
        if truncated:
            stats["outbound_truncations"] = stats.get("outbound_truncations", 0) + 1

    if len(raw) > MAX_WS_FRAME_BYTES:
        bounded = {
            "type": payload.get("type", "message"),
            "content": "[truncated: websocket frame guard could not fit original payload]",
        }
        raw = _json_bytes(bounded)
        stats["outbound_truncations"] = stats.get("outbound_truncations", 0) + 1

    stats["max_outbound_frame_bytes"] = max(stats.get("max_outbound_frame_bytes", 0), len(raw))
    await ws.send(raw.decode("utf-8"))


def _capture_preview(text, max_bytes: int = MAX_CAPTURE_BYTES) -> str:
    if not isinstance(text, str):
        text = "" if text is None else str(text)
    return _truncate_utf8(text, max_bytes)[0]


def load_tokens():
    if TOKENS_FILE.exists():
        try:
            return json.loads(TOKENS_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_tokens(d):
    TOKENS_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKENS_FILE.write_text(json.dumps(d, indent=2))


def _harvest_commits_from_tool_call(call_args, tool_name):
    """Look at a tool_call's args + output for git commit SHAs."""
    out = []
    if not isinstance(call_args, dict):
        return out
    cmd = call_args.get("command", "") or call_args.get("cmd", "")
    if not isinstance(cmd, str):
        return out
    if tool_name in {"shell", "bash"} and "git commit" in cmd:
        # We mark this as a candidate; the SHA appears in the tool_result.
        out.append({"marker": "git_commit_invoked", "cmd_preview": cmd[:200]})
    return out


def _harvest_commits_from_tool_result(text):
    """Extract candidate commit SHAs from a tool_result body."""
    out = []
    if not isinstance(text, str):
        return out
    for m in GIT_COMMIT_RE.finditer(text):
        for grp in (m.group("sha"), m.group("sha2"), m.group(0)):
            if grp and re.fullmatch(r"[a-f0-9]{7,40}", grp):
                out.append(grp[:40])
                break
    return list(dict.fromkeys(out))  # dedupe, preserve order


def _token_keys_for_host(host: str) -> list[str]:
    """Return token-file keys to try for a gateway host.

    Workers usually connect to their local gateway as 127.0.0.1, while
    fleet pairing often stores the same token under the short host name
    (for example TYPHON or ULTRA). Treat loopback spellings as aliases.
    iter 49 patch: merged ULTRA's 127.0.0.1 fallback into TYPHON-instrumented driver.
    """
    import socket as _sock
    keys = []
    def add(value):
        if value and value not in keys:
            keys.append(value)
    add(host)
    if host in {"127.0.0.1", "localhost", "::1"}:
        add("127.0.0.1")
        add("localhost")
        add(os.environ.get("AGENT_HOST", ""))
        try:
            short = _sock.gethostname().split(".")[0]
        except Exception:
            short = ""
        add(short)
        add(short.lower())
        add(short.upper())
    return keys


async def _drive_one(host: str, agent_alias: str, task: str, job_id: str,
                    timeout_sec: float = SESSION_TIMEOUT_SEC,
                    max_tool_iterations: int | None = None) -> dict:
    """Open a WSS session, drive one task to completion, return result dict."""
    tokens = load_tokens()
    token = None
    for _key in _token_keys_for_host(host):
        token = tokens.get(_key)
        if token: break
    if not token:
        return {
            "exit_code": 2,
            "worker_error": "no_gateway_token",
            "error": f"no token for host {host}; pair gateway first via POST /pair with X-Pairing-Code",
        }
    params = {
        "agent": agent_alias,
        "session_id": f"doctor-{job_id[:12]}-{int(time.time())}",
        "name": "doctor-driven",
        "token": token,
    }
    if max_tool_iterations is not None:
        params["max_tool_iterations"] = str(max_tool_iterations)
    uri = f"ws://{host}:{HOST_PORT}/ws/chat?{urlencode(params)}"
    started = time.time()
    tool_calls = []
    tool_results = []
    candidate_commits = []
    files_touched = set()
    final_response = ""
    token_data = {}
    approvals_handled = 0
    error = None
    frame_stats = {"outbound_truncations": 0, "max_outbound_frame_bytes": 0, "max_inbound_frame_bytes": 0}

    try:
        async with websockets.connect(uri, open_timeout=15, close_timeout=10, max_size=None) as ws:
            # First frame: session_start
            first = await asyncio.wait_for(ws.recv(), timeout=15.0)
            frame_stats["max_inbound_frame_bytes"] = max(frame_stats["max_inbound_frame_bytes"], _utf8_len(first))
            session_start = json.loads(first)
            # Send the actual task
            await _send_json_bounded(ws, {"type": "message", "content": task}, frame_stats)
            deadline = time.time() + timeout_sec
            while time.time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue
                frame_stats["max_inbound_frame_bytes"] = max(frame_stats["max_inbound_frame_bytes"], _utf8_len(raw))
                try:
                    f = json.loads(raw)
                except Exception:
                    continue
                t = f.get("type", "")
                if t == "approval_request":
                    tool = f.get("tool", "")
                    request_id = f.get("request_id")
                    decision = "approve" if tool in AUTO_APPROVE_TOOLS else "deny"
                    await _send_json_bounded(ws, {
                        "type": "approval_response",
                        "request_id": request_id,
                        "decision": decision,
                    }, frame_stats)
                    approvals_handled += 1
                elif t == "tool_call":
                    name = f.get("name", "")
                    args = f.get("args", {}) or f.get("arguments", {})
                    tool_calls.append({"name": name, "args_preview": _capture_preview(str(args), 2_000)})
                    for m in _harvest_commits_from_tool_call(args, name):
                        candidate_commits.append(m)
                    # Record file paths touched by edit/write tools
                    if isinstance(args, dict):
                        for k in ("path", "file_path", "filename"):
                            p = args.get(k)
                            if isinstance(p, str):
                                files_touched.add(p)
                elif t == "tool_result":
                    name = f.get("name", "")
                    out = f.get("output", "") or f.get("result", "")
                    tool_results.append({"name": name, "out_preview": _capture_preview(out, 4_000)})
                    shas = _harvest_commits_from_tool_result(out)
                    candidate_commits.extend(shas)
                elif t == "done":
                    final_response = _capture_preview(f.get("full_response", "") or "", MAX_CAPTURE_BYTES)
                    # ── Token tracking (Phase 1 instrument) ──
                    token_data = {
                        "tokens_in": f.get("input_tokens", 0) or 0,
                        "tokens_out": f.get("output_tokens", 0) or 0,
                        "tokens_reasoning": f.get("reasoning_tokens", 0) or 0,
                        "tokens_total": f.get("tokens_used", 0) or 0,
                        "gateway_model": f.get("model", ""),
                        "gateway_provider": f.get("provider", ""),
                    }
                    break
                elif t in ("error", "fatal"):
                    error = f.get("error") or f.get("message") or str(f)[:200]
                    break
                # chunk/chunk_reset/system frames: ignored
    except asyncio.TimeoutError:
        error = "session_timeout"
    except websockets.exceptions.WebSocketException as e:
        error = f"ws_exception: {e!r}"[:200]
    except Exception as e:
        error = f"unexpected: {type(e).__name__}: {e}"[:200]

    duration = time.time() - started
    # Dedupe commits, keep only SHAs (skip marker dicts)
    real_commits = []
    for c in candidate_commits:
        if isinstance(c, str) and re.fullmatch(r"[a-f0-9]{7,40}", c):
            if c not in real_commits:
                real_commits.append(c)

    return {
        "exit_code": 1 if error else 0,
        "agent_alias": agent_alias,
        "host": host,
        "duration_sec": round(duration, 2),
        "tool_call_count": len(tool_calls),
        "tool_results_count": len(tool_results),
        "approvals_handled": approvals_handled,
        "outbound_truncations": frame_stats["outbound_truncations"],
        "max_outbound_frame_bytes": frame_stats["max_outbound_frame_bytes"],
        "max_inbound_frame_bytes": frame_stats["max_inbound_frame_bytes"],
        "commits": real_commits,
        "files_changed": sorted(files_touched),
        "full_response": _capture_preview(final_response, MAX_CAPTURE_BYTES),
        "error": error,
        "worker_error": "no_code_output" if not error and not real_commits else None,
        "via": "wss_driver",
        "tokens_in": token_data.get("tokens_in", 0),
        "tokens_out": token_data.get("tokens_out", 0),
        "tokens_reasoning": token_data.get("tokens_reasoning", 0),
        "tokens_total": token_data.get("tokens_total", 0),
        "gateway_model": token_data.get("gateway_model", ""),
        "gateway_provider": token_data.get("gateway_provider", ""),
        "max_tool_iterations": max_tool_iterations,
    }


def drive_agent_via_wss(host: str, agent_alias: str, task: str, job_id: str,
                        timeout_sec: float = SESSION_TIMEOUT_SEC,
                        max_tool_iterations: int | None = None) -> dict:
    """Synchronous wrapper for the doctor's existing sync codepath."""
    return asyncio.run(_drive_one(host, agent_alias, task, job_id, timeout_sec, max_tool_iterations))


if __name__ == "__main__":
    # Quick CLI smoke: python3 wss_driver.py PYTHIA hive_deepseek_1 "echo: smoke"
    import sys
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    alias = sys.argv[2] if len(sys.argv) > 2 else "hive_deepseek_1"
    task = sys.argv[3] if len(sys.argv) > 3 else "Reply with exactly: WSS DRIVER OK"
    r = drive_agent_via_wss(host, alias, task, job_id=f"smoke-{int(time.time())}", timeout_sec=120)
    print(json.dumps(r, indent=2))
