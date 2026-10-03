"""Host route drop-ins must agree with FleetOps zoder-config.

FleetOps ``gitops/fleet/zoder-config/deploy.py`` writes the same
``20-host-route.conf`` with ``coder`` / ``gemma4-31b``. A host file here that
pins anything else is reinstalled by ``hive-worker-deploy.sh`` on every
reconcile, so the two GitOps sources flap. On 2026-10-03 HYDRA's file pinned
``qwen38`` as reviewer -- the coder model reviewing its own work.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ROUTES = sorted(ROOT.glob("hosts/*/hive-worker-zeroclaw-route.conf"))


def _env(path: Path) -> dict[str, str]:
    env = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("Environment="):
            key, _, value = line.removeprefix("Environment=").partition("=")
            env[key] = value
    return env


def test_route_files_exist():
    assert ROUTES


def test_host_routes_use_the_fleet_reviewer():
    for path in ROUTES:
        env = _env(path)
        assert env.get("HIVE_ZODER_AGENT") == "coder", path
        assert env.get("HIVE_ZODER_REVIEWER") == "gemma4-31b", path
