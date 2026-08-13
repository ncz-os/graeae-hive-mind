#!/usr/bin/env python3
"""
GRAEAE Hive Mind — fleet system watcher daemon.

Registers the host as kind=system with static hardware specs + dynamic load.
Heartbeats current cpu/ram/load/disk metrics. Optionally claims jobs that
require physical resources (build, compile, render) when load permits.

Prevents ARGOS-style oversubscription (CLAUDE.md gotcha: load=92 incident
2026-05-20 from multi-job spawning). Dispatcher filters by required_resources
AND current load before assigning to a host.

Env:
  HIVE_URL                  http://192.168.207.67:5005
  AGENT_HOST                default hostname -s
  LOAD_THRESHOLD            max 1-min load_avg before refusing new jobs (default 0.75 × cpu_count)
  HEARTBEAT_INTERVAL        seconds (default 15)
  CLAIM_JOBS                if "1", actively claim eligible jobs and dispatch to local executor (default "0" = passive monitor only)
  JOB_WORK_ROOT             root containing allowed relative execution.cwd paths (default /srv)
  SYSTEM_WATCHER_JOB_KINDS  comma-separated kinds this worker may claim (default build,compile,render)
  SYSTEM_WATCHER_ALLOWED_EXECUTABLES comma-separated executable basename allowlist
  SYSTEM_WATCHER_ALLOWED_JOB_ENV comma-separated job-provided environment variable allowlist
  SYSTEM_WATCHER_PASSTHROUGH_ENV parent environment names exposed to child processes
  JOB_LEASE_RENEW_INTERVAL  seconds between running patches (default 300)
  JOB_TIMEOUT_MAX           hard per-process timeout ceiling in seconds (default 21600)
  ALLOW_CLAIM_WITHOUT_LOAD  development-only opt-out for unavailable load metrics (default 0)
"""
from __future__ import annotations
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
import urllib.error
import signal
import tempfile
import urllib.parse

from hive.auth import client_token

HIVE_URL = os.environ.get("HIVE_URL", "http://192.168.207.67:5005")
# C1: bus transport auth bearer token (env-sourced, never baked in).
HIVE_BUS_TOKEN = client_token("HIVE_BUS_TOKEN", "HIVE_BUS_TOKENS")
AGENT_HOST = os.environ.get("AGENT_HOST", socket.gethostname().split(".")[0])
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "15"))
CLAIM_JOBS = os.environ.get("CLAIM_JOBS", "0") == "1"
LOAD_THRESHOLD = float(os.environ.get(
    "LOAD_THRESHOLD", str(0.75 * max(os.cpu_count() or 1, 1))
))
ALLOW_CLAIM_WITHOUT_LOAD = os.environ.get("ALLOW_CLAIM_WITHOUT_LOAD", "0") == "1"
JOB_POLL_INTERVAL = float(os.environ.get("JOB_POLL_INTERVAL", "5"))
TELEMETRY_CACHE_SECONDS = max(
    0.0, float(os.environ.get("SYSTEM_WATCHER_TELEMETRY_CACHE_SECONDS", "0"))
)
JOB_LEASE_RENEW_INTERVAL = float(os.environ.get("JOB_LEASE_RENEW_INTERVAL", "300"))
JOB_TIMEOUT_MAX = int(os.environ.get("JOB_TIMEOUT_MAX", "21600"))
JOB_WORK_ROOT = os.path.realpath(os.environ.get("JOB_WORK_ROOT", "/srv"))
JOB_KINDS = tuple(
    item.strip() for item in os.environ.get(
        "SYSTEM_WATCHER_JOB_KINDS", "build,compile,render"
    ).split(",") if item.strip()
)
ALLOWED_EXECUTABLES = frozenset(
    item.strip() for item in os.environ.get(
        "SYSTEM_WATCHER_ALLOWED_EXECUTABLES",
        "make,ninja,cmake,cargo,go,npm,pnpm,yarn,blender",
    ).split(",") if item.strip()
)
ALLOWED_JOB_ENV = frozenset(
    item.strip() for item in os.environ.get(
        "SYSTEM_WATCHER_ALLOWED_JOB_ENV", "CI,BUILD_TYPE,CMAKE_BUILD_TYPE"
    ).split(",") if item.strip()
)
PASSTHROUGH_ENV = frozenset(
    item.strip() for item in os.environ.get(
        "SYSTEM_WATCHER_PASSTHROUGH_ENV", "PATH,HOME,LANG,LC_ALL,TMPDIR,TEMP,TMP"
    ).split(",") if item.strip()
)

_urn: str = ""
_running = True


def _signal(signum, frame):
    global _running
    print(f"[sysw] signal {signum} — shutting down", flush=True)
    _running = False


signal.signal(signal.SIGTERM, _signal)
signal.signal(signal.SIGINT, _signal)


def _http(method: str, path: str, body: dict | None = None) -> tuple[int, dict | None]:
    url = f"{HIVE_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {"content-type": "application/json"}
    if HIVE_BUS_TOKEN:
        headers["authorization"] = f"Bearer {HIVE_BUS_TOKEN}"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except Exception as e:
        print(f"[sysw] http error {method} {path}: {e}", flush=True)
        return 0, None


_IS_MACOS = platform.system() == "Darwin"


def static_specs() -> dict:
    """One-shot specs gathered at startup."""
    out = {
        "hostname": AGENT_HOST,
        "fqdn": socket.getfqdn(),
        "os": platform.system(),
        "kernel": platform.release(),
        "arch": platform.machine(),
        "python": sys.version.split()[0],
    }
    # CPU
    out["cpu_count"] = os.cpu_count()
    if _IS_MACOS:
        try:
            r = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                               capture_output=True, text=True, timeout=5)
            out["cpu_model"] = r.stdout.strip()
            r2 = subprocess.run(["sysctl", "-n", "hw.logicalcpu"],
                                capture_output=True, text=True, timeout=5)
            out["cpu_threads"] = int(r2.stdout.strip())
        except Exception:
            pass
    else:
        try:
            with open("/proc/cpuinfo") as f:
                content = f.read()
            m = re.search(r"model name\s*:\s*(.+)", content)
            if m:
                out["cpu_model"] = m.group(1).strip()
            out["cpu_threads"] = content.count("processor\t:")
        except Exception:
            pass
    # RAM
    if _IS_MACOS:
        try:
            r = subprocess.run(["sysctl", "-n", "hw.memsize"],
                               capture_output=True, text=True, timeout=5)
            ram_bytes = int(r.stdout.strip())
            out["ram_gb"] = round(ram_bytes / 1024**3, 1)
            out["ram_kb"] = ram_bytes // 1024
        except Exception:
            pass
    else:
        try:
            with open("/proc/meminfo") as f:
                mi = f.read()
            m = re.search(r"MemTotal:\s+(\d+)", mi)
            if m:
                out["ram_kb"] = int(m.group(1))
                out["ram_gb"] = round(int(m.group(1)) / 1024 / 1024, 1)
        except Exception:
            pass
    # Disk
    try:
        for mnt in ("/srv", "/", "/var"):
            if os.path.exists(mnt):
                s = shutil.disk_usage(mnt)
                key = f"disk_{mnt.replace('/', '_') or '_root'}_total_gb"
                out[key] = round(s.total / 1024**3, 1)
    except Exception:
        pass
    # GPU detection
    gpus = []
    # Apple Silicon (macOS)
    if _IS_MACOS:
        try:
            r = subprocess.run(["system_profiler", "SPDisplaysDataType", "-json"],
                               capture_output=True, text=True, timeout=10)
            data = json.loads(r.stdout)
            for item in data.get("SPDisplaysDataType", []):
                gpus.append({
                    "vendor": "apple",
                    "name": item.get("sppci_model", "Apple Silicon GPU"),
                    "vram_mib": None,
                    "info": item,
                })
        except Exception:
            pass
    # NVIDIA
    if shutil.which("nvidia-smi"):
        try:
            r = subprocess.run(["nvidia-smi", "--query-gpu=name,memory.total,driver_version",
                                "--format=csv,noheader,nounits"], capture_output=True, text=True, timeout=5)
            for line in r.stdout.strip().splitlines():
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 2:
                    gpus.append({"vendor": "nvidia", "name": parts[0], "vram_mib": int(parts[1]),
                                 "driver": parts[2] if len(parts) > 2 else ""})
        except Exception:
            pass
    # AMD
    if shutil.which("rocm-smi"):
        try:
            r = subprocess.run(["rocm-smi", "--showproductname", "--json"], capture_output=True, text=True, timeout=5)
            data = json.loads(r.stdout) if r.stdout.strip() else {}
            for k, v in data.items():
                if isinstance(v, dict) and ("Card SKU" in v or "GPU ID" in v):
                    gpus.append({"vendor": "amd", "card_id": k, "info": v})
        except Exception:
            pass
    if not gpus:
        # try lspci (Linux only)
        try:
            r = subprocess.run(["lspci"], capture_output=True, text=True, timeout=5)
            for line in r.stdout.splitlines():
                if re.search(r"VGA|3D|Display", line):
                    gpus.append({"vendor": "unknown", "lspci": line.strip()})
        except Exception:
            pass
    out["gpus"] = gpus
    # Container runtimes
    out["has_docker"] = bool(shutil.which("docker"))
    out["has_podman"] = bool(shutil.which("podman"))
    out["has_buildx"] = (bool(shutil.which("docker")) and
                         subprocess.run(["docker", "buildx", "version"], capture_output=True).returncode == 0
                         ) if shutil.which("docker") else False
    # CI runners
    out["has_gitlab_runner"] = bool(shutil.which("gitlab-runner"))
    # XDNA NPU (HYDRA Ryzen 8700G / cixmini Cix Sky1)
    try:
        r = subprocess.run(["lspci"], capture_output=True, text=True, timeout=5)
        if re.search(r"IPU|NPU|Neural|XDNA|AI Engine", r.stdout, re.I):
            out["has_npu"] = True
    except Exception:
        pass
    return out


def dynamic_load() -> dict:
    """Snapshot of current load."""
    out = {"ts": time.time()}
    try:
        out["load_1min"], out["load_5min"], out["load_15min"] = os.getloadavg()
    except Exception:
        pass
    if _IS_MACOS:
        try:
            r = subprocess.run(["vm_stat"], capture_output=True, text=True, timeout=5)
            stats: dict[str, int] = {}
            for line in r.stdout.splitlines():
                m = re.match(r"Pages\s+(\w[\w\s]+):\s+(\d+)", line)
                if m:
                    stats[m.group(1).strip()] = int(m.group(2))
            page_size = 4096
            try:
                r2 = subprocess.run(["sysctl", "-n", "hw.pagesize"],
                                    capture_output=True, text=True, timeout=5)
                page_size = int(r2.stdout.strip())
            except Exception:
                pass
            total_pages = sum(stats.values()) or 1
            free_pages = stats.get("free", 0) + stats.get("speculative", 0)
            wired = stats.get("wired down", 0)
            active = stats.get("active", 0)
            used_pages = wired + active
            out["ram_used_pct"] = round(100 * used_pages / max(total_pages, 1), 1)
            out["ram_free_gb"] = round(free_pages * page_size / 1024**3, 2)
        except Exception:
            pass
    else:
        try:
            with open("/proc/meminfo") as f:
                mi = f.read()
            for key in ("MemTotal", "MemFree", "MemAvailable", "Buffers", "Cached"):
                m = re.search(rf"{key}:\s+(\d+)", mi)
                if m:
                    out[key.lower() + "_kb"] = int(m.group(1))
            if "memtotal_kb" in out and "memavailable_kb" in out:
                used = out["memtotal_kb"] - out["memavailable_kb"]
                out["ram_used_pct"] = round(100 * used / out["memtotal_kb"], 1)
        except Exception:
            pass
    try:
        # Legacy single-volume fields (back-compat for older dashboards)
        s = shutil.disk_usage("/srv" if os.path.exists("/srv") else "/")
        out["srv_used_pct"] = round(100 * (s.total - s.free) / s.total, 1)
        out["srv_free_gb"] = round(s.free / 1024**3, 1)
    except Exception:
        pass
    # Multi-volume disk snapshot — enumerate all real filesystems (skip
    # pseudo/loop/tmpfs mounts). Each entry: {mount, total_gb, used_pct, free_gb, fstype}.
    try:
        volumes = []
        seen = set()
        def _add_vol(mnt):
            try:
                if mnt in seen or not os.path.isdir(mnt):
                    return
                s = shutil.disk_usage(mnt)
                if s.total < 1024**3:   # skip <1GB (probably pseudo)
                    return
                seen.add(mnt)
                volumes.append({
                    "mount": mnt,
                    "total_gb": round(s.total / 1024**3, 1),
                    "free_gb": round(s.free / 1024**3, 1),
                    "used_pct": round(100 * (s.total - s.free) / s.total, 1),
                })
            except Exception:
                pass
        if _IS_MACOS:
            # macOS: parse `df -k`
            try:
                r = subprocess.run(["df", "-k"], capture_output=True, text=True, timeout=5)
                for line in r.stdout.splitlines()[1:]:
                    parts = line.split()
                    if len(parts) < 6:
                        continue
                    fs = parts[0]; mnt = parts[-1]
                    if fs.startswith("/dev/") and not mnt.startswith(("/System", "/private/var/vm")):
                        _add_vol(mnt)
            except Exception:
                pass
        else:
            # Linux: parse /proc/mounts
            try:
                with open("/proc/mounts") as f:
                    for line in f:
                        parts = line.split()
                        if len(parts) < 3:
                            continue
                        dev, mnt, fstype = parts[0], parts[1], parts[2]
                        if fstype in {"tmpfs","devtmpfs","proc","sysfs","cgroup","cgroup2",
                                      "devpts","mqueue","pstore","bpf","tracefs","debugfs",
                                      "configfs","fusectl","rpc_pipefs","squashfs","autofs",
                                      "binfmt_misc","hugetlbfs","fuse.gvfsd-fuse","fuse.portal",
                                      "overlay","nsfs"}:
                            continue
                        # Skip nfs auto-mount points that aren't actually mounted
                        if "x-systemd.automount" in line and fstype == "autofs":
                            continue
                        if not dev.startswith(("/dev/", "192.168.", "10.", "/")):
                            continue
                        _add_vol(mnt)
            except Exception:
                pass
        # Always include / explicitly
        _add_vol("/")
        # Dedupe by (total_gb, free_gb) — bind-mounts and overlay views share
        # the same underlying device size. Keep the shortest mount path as
        # the canonical one (usually the actual mount, not a bind).
        if volumes:
            by_sig: dict = {}
            for v in volumes:
                sig = (v.get("total_gb"), v.get("free_gb"))
                if sig not in by_sig or len(v.get("mount","")) < len(by_sig[sig].get("mount","")):
                    by_sig[sig] = v
            volumes = sorted(by_sig.values(), key=lambda x: x.get("mount") or "")
        out["volumes"] = volumes
    except Exception:
        pass
    # CPU utilization snapshot (instantaneous; system_watcher polls every ~15s)
    try:
        if _IS_MACOS:
            # parse `iostat -c2 1` second sample → user+sys
            r = subprocess.run(["iostat", "-c", "2", "1"], capture_output=True, text=True, timeout=8)
            lines = [l for l in r.stdout.splitlines() if l.strip()]
            if len(lines) >= 2:
                parts = lines[-1].split()
                # us sy id format
                if len(parts) >= 5:
                    try:
                        out["cpu_used_pct"] = float(parts[-5]) + float(parts[-4])
                    except Exception:
                        pass
        else:
            # /proc/stat — single-sample using `top -bn2 -d 0.5`-style isn't portable;
            # use two reads of /proc/stat 200ms apart
            def _read_cpu():
                with open("/proc/stat") as f:
                    line = f.readline()
                vals = [int(x) for x in line.split()[1:]]
                idle = vals[3] + vals[4]   # idle + iowait
                total = sum(vals)
                return idle, total
            i1, t1 = _read_cpu()
            time.sleep(0.25)
            i2, t2 = _read_cpu()
            dt = t2 - t1
            di = i2 - i1
            if dt > 0:
                out["cpu_used_pct"] = round(100.0 * (1 - di/dt), 1)
    except Exception:
        pass
    # uptime
    if _IS_MACOS:
        try:
            r = subprocess.run(["sysctl", "-n", "kern.boottime"], capture_output=True, text=True, timeout=5)
            m = re.search(r"sec\s*=\s*(\d+)", r.stdout)
            if m:
                out["uptime_sec"] = time.time() - int(m.group(1))
        except Exception:
            pass
    else:
        try:
            with open("/proc/uptime") as f:
                out["uptime_sec"] = float(f.read().split()[0])
        except Exception:
            pass
    # Runtime GPU metrics (added 2026-05-26 for dashboard GPU status panel).
    # nvidia-smi → name, util%, mem MiB, temp C; same shape for rocm-smi (AMD).
    try:
        gpus_rt = []
        if shutil.which("nvidia-smi"):
            try:
                r = subprocess.run(
                    ["nvidia-smi", "--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw",
                     "--format=csv,noheader,nounits"],
                    capture_output=True, text=True, timeout=5,
                )
                for line in r.stdout.strip().splitlines():
                    parts = [p.strip() for p in line.split(",")]
                    if len(parts) >= 5:
                        try:
                            gpus_rt.append({
                                "vendor": "nvidia",
                                "name": parts[0],
                                "util_pct": float(parts[1]) if parts[1] else None,
                                "mem_used_mib": int(parts[2]) if parts[2] else None,
                                "mem_total_mib": int(parts[3]) if parts[3] else None,
                                "temp_c": float(parts[4]) if parts[4] else None,
                                "power_w": float(parts[5]) if len(parts) > 5 and parts[5] not in ("", "[Not Supported]") else None,
                            })
                        except Exception:
                            pass
            except Exception:
                pass
        if shutil.which("rocm-smi"):
            try:
                r = subprocess.run(["rocm-smi", "--showproductname", "--showuse", "--showmemuse",
                                    "--showtemp", "--showpower", "--json"],
                                   capture_output=True, text=True, timeout=5)
                data = json.loads(r.stdout) if r.stdout.strip() else {}
                for k, v in data.items():
                    if not isinstance(v, dict): continue
                    name = v.get("Card SKU") or v.get("Card series") or v.get("Card model") or k
                    try:
                        util = float((v.get("GPU use (%)") or "0").replace("%",""))
                    except: util = None
                    try:
                        mem_used = int(float(v.get("VRAM Total Used Memory (B)") or 0) / (1024*1024))
                    except: mem_used = None
                    try:
                        mem_total = int(float(v.get("VRAM Total Memory (B)") or 0) / (1024*1024))
                    except: mem_total = None
                    temp = None
                    for tk in v:
                        if "temperature" in tk.lower() and "edge" in tk.lower():
                            try: temp = float(v[tk]); break
                            except: pass
                    try:
                        power = float((v.get("Average Graphics Package Power (W)") or "").replace("W","").strip() or 0)
                    except: power = None
                    gpus_rt.append({
                        "vendor": "amd", "name": name, "util_pct": util,
                        "mem_used_mib": mem_used, "mem_total_mib": mem_total,
                        "temp_c": temp, "power_w": power,
                    })
            except Exception:
                pass
        out["gpus_runtime"] = gpus_rt
    except Exception:
        pass
    return out


_telemetry_cache: dict = {}
_telemetry_cache_at = 0.0


def current_load() -> dict:
    """Return telemetry, optionally caching expensive probes while refreshing loadavg."""
    global _telemetry_cache, _telemetry_cache_at
    now = time.monotonic()
    if (
        TELEMETRY_CACHE_SECONDS > 0
        and _telemetry_cache
        and now - _telemetry_cache_at < TELEMETRY_CACHE_SECONDS
    ):
        load = dict(_telemetry_cache)
        try:
            load["load_1min"], load["load_5min"], load["load_15min"] = os.getloadavg()
        except (AttributeError, OSError):
            pass
        return load
    load = dynamic_load()
    _telemetry_cache = dict(load)
    _telemetry_cache_at = now
    return load


def capabilities_from_specs(specs: dict) -> list[str]:
    caps = ["system", "host"]
    if specs.get("has_docker"):     caps.append("docker")
    if specs.get("has_podman"):     caps.append("podman")
    if specs.get("has_buildx"):     caps.extend(["build", "buildx", "multi-arch-build"])
    if specs.get("has_gitlab_runner"): caps.append("ci-runner")
    if specs.get("has_npu"):        caps.extend(["npu", "xdna"])
    for g in specs.get("gpus", []) or []:
        v = g.get("vendor", "")
        if v == "nvidia": caps.extend(["gpu", "nvidia-gpu", "cuda"])
        elif v == "amd":  caps.extend(["gpu", "amd-gpu", "rocm", "vulkan-compute"])
        elif v == "intel": caps.extend(["gpu", "intel-igpu"])
        elif v == "apple": caps.extend(["gpu", "apple-gpu", "metal", "mlx"])
    if specs.get("ram_gb", 0) >= 32:  caps.append("ram-32g+")
    if specs.get("ram_gb", 0) >= 64:  caps.append("ram-64g+")
    if specs.get("ram_gb", 0) >= 128: caps.append("ram-128g+")
    if specs.get("cpu_count", 0) >= 16: caps.append("cpu-16t+")
    if specs.get("cpu_count", 0) >= 32: caps.append("cpu-32t+")
    if specs.get("os") == "Darwin":  caps.append("macos")
    if specs.get("arch") == "arm64" or specs.get("arch") == "aarch64": caps.append("arm64")
    if CLAIM_JOBS: caps.append("local-process")
    return caps


def register():
    global _urn, HIVE_BUS_TOKEN
    specs = static_specs()
    caps = capabilities_from_specs(specs)
    load = dynamic_load()
    body = {
        "runtime": "system",
        "kind": "system",
        "host": AGENT_HOST,
        "pid": os.getpid(),
        "model": "n/a",
        "provider": "local",
        "autonomy_level": "autonomous" if CLAIM_JOBS else "interactive",
        "capabilities": caps,
        "version": platform.platform(),
        "metadata": {
            "specs": specs,
            "load": load,
            "daemon": "system_watcher.py",
        },
    }
    # system is a NEW runtime — RUNTIME_KIND_MAP may not include it; service falls back to allowing kind==runtime
    code, resp = _http("POST", "/v1/agents/register", body)
    if code == 200 and resp:
        _urn = resp["urn"]
        HIVE_BUS_TOKEN = resp.get("agent_token") or HIVE_BUS_TOKEN
        print(f"[sysw] registered urn={_urn} caps={caps}", flush=True)
    else:
        print(f"[sysw] register failed code={code} resp={resp}", flush=True)
        sys.exit(1)


def heartbeat(load: dict | None = None):
    load = load or current_load()
    # Push fresh load into agent metadata so /v1/hosts always has current data.
    # Heartbeat endpoint merges {"load": load} into existing metadata (server-side update).
    _http("POST", "/v1/agents/heartbeat", {
        "urn": _urn,
        "metadata": {"load": load},
    })
    # NOTE: previous code also POSTed to /v1/messages as topic=system.load,
    # but that flooded the bus (11 hosts × 4 msgs/min = 2640/hr of pure
    # telemetry). The load is already in agent metadata via heartbeat above
    # and /v1/hosts surfaces it; SSE listeners can subscribe to agent.metadata
    # events instead. Broadcast removed 2026-05-26.
    return load


def load_allows_claim(load: dict) -> tuple[bool, str]:
    """Fail closed when the load sample is absent or exceeds the configured gate."""
    value = load.get("load_1min")
    if value is None:
        if ALLOW_CLAIM_WITHOUT_LOAD:
            return True, "load_1min unavailable; development opt-out enabled"
        return False, "load_1min unavailable"
    try:
        current = float(value)
    except (TypeError, ValueError):
        return False, f"load_1min is invalid: {value!r}"
    if current > LOAD_THRESHOLD:
        return False, f"load_1min={current:.2f} exceeds threshold={LOAD_THRESHOLD:.2f}"
    return True, f"load_1min={current:.2f} threshold={LOAD_THRESHOLD:.2f}"


def claim_next_job() -> dict | None:
    query = urllib.parse.urlencode({
        "agent_urn": _urn,
        "execution_type": "local-process",
        "job_kinds": ",".join(JOB_KINDS),
    })
    code, resp = _http("POST", f"/v1/jobs/next?{query}")
    if code == 200 and isinstance(resp, dict) and resp.get("id"):
        return resp
    if code not in {0, 204}:
        print(f"[sysw] claim failed code={code} resp={resp}", flush=True)
    return None


def patch_job(job_id: str, status: str, result: dict | None = None) -> bool:
    body = {"status": status, "claimed_by": _urn}
    if result is not None:
        body["result"] = result
    code, resp = _http("PATCH", f"/v1/jobs/{job_id}", body)
    if code not in {200, 201, 204}:
        print(f"[sysw] job patch failed id={job_id} status={status} code={code} resp={resp}", flush=True)
        return False
    return True


def finalize_job(job_id: str, status: str, result: dict, attempts: int = 5) -> bool:
    """Persist completion evidence and retry the terminal transition."""
    if status != "done":
        for attempt in range(attempts):
            if patch_job(job_id, status, result):
                return True
            if attempt + 1 < attempts:
                _sleep_while_running(min(0.25 * (2 ** attempt), 4.0))
        return False
    receipt_result = dict(result)
    receipt_result["_execution_receipt"] = {
        "version": 1,
        "execution_id": job_id,
        "completed": True,
        "terminal_status": status,
    }
    delay = 0.25
    receipt_saved = False
    for attempt in range(attempts):
        if patch_job(job_id, status, receipt_result):
            return True
        # A running PATCH both renews ownership and gives the bus durable proof
        # that the side effect completed. Its reaper finalizes this receipt
        # instead of putting the job back in the execution queue.
        receipt_saved = patch_job(job_id, "running", receipt_result) or receipt_saved
        if attempt + 1 < attempts:
            _sleep_while_running(delay)
            delay = min(delay * 2, 4.0)
    if receipt_saved:
        print(f"[sysw] terminal patch deferred to bus receipt reconciliation id={job_id}", flush=True)
    return False


def _validated_process(job: dict) -> tuple[list[str], str, dict[str, str], int]:
    spec = job.get("execution")
    if not isinstance(spec, dict) or spec.get("type") != "local-process":
        raise ValueError("job has no execution.type='local-process' specification")
    argv = spec.get("argv")
    if not isinstance(argv, list) or not argv or len(argv) > 256:
        raise ValueError("execution.argv must contain 1..256 arguments")
    if any(not isinstance(arg, str) or "\x00" in arg or len(arg) > 8192 for arg in argv):
        raise ValueError("execution.argv contains an invalid argument")
    executable = argv[0]
    if os.path.basename(executable) != executable or executable not in ALLOWED_EXECUTABLES:
        raise ValueError(
            f"executable {executable!r} is not allowed; configure SYSTEM_WATCHER_ALLOWED_EXECUTABLES"
        )
    resolved_executable = shutil.which(executable)
    if not resolved_executable:
        raise ValueError(f"allowed executable {executable!r} is not installed or not on PATH")

    relative_cwd = spec.get("cwd", ".")
    if not isinstance(relative_cwd, str) or not relative_cwd or os.path.isabs(relative_cwd):
        raise ValueError("execution.cwd must be a non-empty path relative to JOB_WORK_ROOT")
    cwd = os.path.realpath(os.path.join(JOB_WORK_ROOT, relative_cwd))
    try:
        inside_root = os.path.commonpath([JOB_WORK_ROOT, cwd]) == JOB_WORK_ROOT
    except ValueError:
        inside_root = False
    if not inside_root or not os.path.isdir(cwd):
        raise ValueError(
            f"execution.cwd resolves outside JOB_WORK_ROOT or is not a directory: {relative_cwd!r}"
        )

    requested_env = spec.get("env", {})
    if not isinstance(requested_env, dict):
        raise ValueError("execution.env must be an object")
    rejected = sorted(set(requested_env) - ALLOWED_JOB_ENV)
    if rejected:
        raise ValueError(
            f"execution.env contains disallowed names {rejected}; configure SYSTEM_WATCHER_ALLOWED_JOB_ENV"
        )
    if any(
        not isinstance(key, str) or not isinstance(value, str)
        or "\x00" in key or "\x00" in value or len(value) > 32768
        for key, value in requested_env.items()
    ):
        raise ValueError("execution.env contains an invalid name or value")
    env = {key: os.environ[key] for key in PASSTHROUGH_ENV if key in os.environ}
    env.update(requested_env)

    timeout = spec.get("timeout_seconds") or JOB_TIMEOUT_MAX
    if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout < 1:
        raise ValueError("execution.timeout_seconds must be a positive integer")
    timeout = min(timeout, JOB_TIMEOUT_MAX)
    return [resolved_executable, *argv[1:]], cwd, env, timeout


def _stop_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(proc.pid, signal.SIGTERM)
        else:
            proc.terminate()
        proc.wait(timeout=10)
    except Exception:
        try:
            if os.name == "posix":
                os.killpg(proc.pid, signal.SIGKILL)
            else:
                proc.kill()
            proc.wait(timeout=5)
        except Exception:
            pass


def execute_job(job: dict) -> tuple[str, dict]:
    """Run one validated process while renewing its claim lease."""
    job_id = str(job.get("id") or "")
    started = time.time()
    try:
        argv, cwd, env, timeout = _validated_process(job)
    except Exception as exc:
        return "failed", {"exit_code": -2, "worker_error": "invalid_execution_spec", "error": str(exc)}

    if not patch_job(job_id, "running", {
        "phase": "starting",
        "executor": "local-process",
        "_execution_receipt": {
            "version": 1, "execution_id": job_id, "completed": False,
        },
    }):
        return "failed", {
            "exit_code": -3,
            "worker_error": "running_transition_failed",
            "error": "refused to execute because the claimed job could not transition to running",
        }

    print(f"[sysw] executing job={job_id} executable={os.path.basename(argv[0])} cwd={cwd}", flush=True)
    with tempfile.TemporaryFile(mode="w+b") as stdout_file, tempfile.TemporaryFile(
        mode="w+b"
    ) as stderr_file:
        try:
            proc = subprocess.Popen(
                argv,
                cwd=cwd,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=stdout_file,
                stderr=stderr_file,
                start_new_session=(os.name == "posix"),
            )
        except Exception as exc:
            return "failed", {"exit_code": -1, "worker_error": "spawn_failed", "error": str(exc)}

        lease_interval = JOB_LEASE_RENEW_INTERVAL
        try:
            initial_lease_remaining = float(job.get("claim_lease_expires_at")) - time.time()
            if initial_lease_remaining > 0:
                lease_interval = min(lease_interval, max(1.0, initial_lease_remaining / 3.0))
        except (TypeError, ValueError):
            pass
        next_lease = time.monotonic() + lease_interval
        next_heartbeat = time.monotonic() + HEARTBEAT_INTERVAL
        stop_reason = ""
        while proc.poll() is None:
            now_monotonic = time.monotonic()
            elapsed = time.time() - started
            if not _running:
                stop_reason = "worker_shutdown"
                break
            if elapsed >= timeout:
                stop_reason = "timeout"
                break
            if now_monotonic >= next_heartbeat:
                heartbeat()
                next_heartbeat = now_monotonic + HEARTBEAT_INTERVAL
            if now_monotonic >= next_lease:
                renewed = patch_job(job_id, "running", {
                    "phase": "running", "executor": "local-process", "elapsed_sec": round(elapsed, 1),
                    "_execution_receipt": {
                        "version": 1, "execution_id": job_id, "completed": False,
                    },
                })
                if not renewed:
                    stop_reason = "lease_renewal_failed"
                    break
                next_lease = now_monotonic + lease_interval
            time.sleep(0.25)

        if stop_reason:
            _stop_process(proc)
        return_code = proc.poll()
        stdout_file.seek(0, os.SEEK_END)
        stdout_size = stdout_file.tell()
        stdout_file.seek(max(0, stdout_size - 12000))
        stdout = stdout_file.read().decode("utf-8", errors="replace")
        stderr_file.seek(0, os.SEEK_END)
        stderr_size = stderr_file.tell()
        stderr_file.seek(max(0, stderr_size - 4000))
        stderr = stderr_file.read().decode("utf-8", errors="replace")

    result = {
        "exit_code": return_code if return_code is not None else -1,
        "stdout": stdout,
        "stderr": stderr,
        "duration_sec": round(time.time() - started, 1),
        "executor": "local-process",
        "executable": os.path.basename(argv[0]),
        "cwd": os.path.relpath(cwd, JOB_WORK_ROOT),
    }
    if stop_reason == "worker_shutdown":
        result["worker_error"] = stop_reason
        return "queued", result
    if stop_reason:
        result["worker_error"] = stop_reason
        return "failed", result
    return ("done" if return_code == 0 else "failed"), result


def _sleep_while_running(seconds: float) -> None:
    deadline = time.monotonic() + max(0, seconds)
    while _running and time.monotonic() < deadline:
        time.sleep(min(0.25, deadline - time.monotonic()))


def main():
    if CLAIM_JOBS:
        if LOAD_THRESHOLD < 0:
            raise SystemExit("LOAD_THRESHOLD must be non-negative")
        if JOB_POLL_INTERVAL <= 0 or JOB_LEASE_RENEW_INTERVAL <= 0 or JOB_TIMEOUT_MAX <= 0:
            raise SystemExit(
                "JOB_POLL_INTERVAL, JOB_LEASE_RENEW_INTERVAL, and JOB_TIMEOUT_MAX must be positive"
            )
        if not JOB_KINDS:
            raise SystemExit("SYSTEM_WATCHER_JOB_KINDS must name at least one job kind")
        if not ALLOWED_EXECUTABLES:
            raise SystemExit("SYSTEM_WATCHER_ALLOWED_EXECUTABLES must not be empty")
        if not os.path.isdir(JOB_WORK_ROOT):
            raise SystemExit(f"JOB_WORK_ROOT is not a directory: {JOB_WORK_ROOT}")
    print(
        f"[sysw] starting on {AGENT_HOST} HIVE_URL={HIVE_URL} CLAIM_JOBS={CLAIM_JOBS} "
        f"LOAD_THRESHOLD={LOAD_THRESHOLD:.2f}",
        flush=True,
    )
    register()
    while _running:
        load = heartbeat()
        delay = HEARTBEAT_INTERVAL
        if CLAIM_JOBS:
            allowed, reason = load_allows_claim(load)
            if allowed:
                job = claim_next_job()
                if job:
                    status, result = execute_job(job)
                    finalize_job(job["id"], status, result)
                    delay = 0
                else:
                    delay = JOB_POLL_INTERVAL
            else:
                print(f"[sysw] claim gated: {reason}", flush=True)
        _sleep_while_running(delay)
    print("[sysw] clean shutdown", flush=True)


if __name__ == "__main__":
    main()
