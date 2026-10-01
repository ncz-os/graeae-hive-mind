"""Strict jobs require pinned readiness evidence and isolated model roles."""

import copy
import hashlib
import json
import os
import subprocess
from unittest.mock import Mock, patch

import pytest

os.environ["WORKER_KIND"] = "zeroclaw"

import hive_worker as worker


def normalized():
    secondary = {
        "provider_id": "nvidia-eih",
        "model_id": "nvidia/nemotron-3-ultra-550b-a55b",
        "base_url": "https://integrate.api.nvidia.com/v1",
    }
    return {
        "schema_version": 1,
        "coder": {
            "provider_id": "tydeus-coder",
            "model_id": "qwen38",
            "base_url": "http://192.168.207.73:8006/v1",
        },
        "reviewer": {
            "provider_id": "cerberus-reviewer",
            "model_id": "gemma4-31b-test",
            "base_url": "http://192.168.207.96:8080/v1",
        },
        "secondary": secondary,
        "eih_reviewers": [secondary.copy()],
    }


@pytest.fixture
def validator(tmp_path, monkeypatch):
    def install(data=None, source=None):
        if source is None:
            source = "import json\nprint(" + repr(json.dumps(data)) + ")\n"
        path = tmp_path / "role_readiness.py"
        path.write_text(source)
        monkeypatch.setenv("HIVE_STRICT_ROLE_VALIDATOR", str(path))
        monkeypatch.setenv("HIVE_STRICT_ROLE_CONFIG", str(tmp_path / "manifest.json"))
        monkeypatch.setenv(
            "HIVE_STRICT_ROLE_VALIDATOR_SHA256",
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        return path

    return install


def test_validated_pool_is_separate_from_author(validator):
    data = normalized()
    data["eih_reviewers"].append(
        dict(data["secondary"], model_id="google/gemma-4-31b-it")
    )
    validator(data)
    routes = worker.load_strict_routes()
    assert routes["author"] == "qwen38"
    assert routes["reviewer"] == "gemma4-31b-test"
    assert routes["allowed_routes"].split(",")[0] == "tydeus-coder=qwen38"
    assert "qwen38" not in routes["reviewer_allowed_routes"]
    assert "cerberus-reviewer" not in routes["reviewer_pool_routes"]
    assert routes["reviewer_pool_routes"].split(",") == [
        "nvidia-eih=nvidia/nemotron-3-ultra-550b-a55b",
        "nvidia-eih=google/gemma-4-31b-it",
    ]


@pytest.mark.parametrize(
    "change",
    [
        lambda d: None,
        lambda d: [],
        lambda d: dict(d, schema_version=True),
        lambda d: dict(d, reviewer=[]),
        lambda d: dict(d, coder=dict(d["coder"], model_id="nemotron35")),
        lambda d: dict(d, secondary=d["reviewer"]),
        lambda d: dict(d, eih_reviewers=[]),
        lambda d: dict(d, eih_reviewers=d["eih_reviewers"] * 2),
        lambda d: dict(d, eih_reviewers=[dict(d["secondary"], provider_id="minimax")]),
        lambda d: dict(d, reviewer=dict(d["reviewer"], model_id="x,provider=y")),
    ],
)
def test_invalid_normalized_output_blocks(validator, change):
    validator(change(copy.deepcopy(normalized())))
    with pytest.raises(ValueError, match="strict role readiness unavailable"):
        worker.load_strict_routes()


@pytest.mark.parametrize("digest", [None, "", "bad", "0" * 64])
def test_missing_or_wrong_pin_never_executes(validator, monkeypatch, digest):
    validator(normalized())
    if digest is None:
        monkeypatch.delenv("HIVE_STRICT_ROLE_VALIDATOR_SHA256")
    else:
        monkeypatch.setenv("HIVE_STRICT_ROLE_VALIDATOR_SHA256", digest)
    run = Mock()
    monkeypatch.setattr(worker.subprocess, "run", run)
    with pytest.raises(ValueError):
        worker.load_strict_routes()
    run.assert_not_called()


@pytest.mark.parametrize(
    "source",
    ["raise SystemExit(1)", "print('malformed')", "print('x' * 16385)"],
)
def test_failed_validator_blocks_without_leaking_output(validator, source):
    validator(source=source)
    with pytest.raises(ValueError, match="^strict role readiness unavailable$"):
        worker.load_strict_routes()


def test_timeout_blocks(validator, monkeypatch):
    validator(normalized())
    monkeypatch.setattr(
        worker.subprocess,
        "run",
        Mock(side_effect=subprocess.TimeoutExpired("validator", 10)),
    )
    with pytest.raises(ValueError):
        worker.load_strict_routes()


def test_executes_hashed_bytes_instead_of_reopening_helper(validator, monkeypatch):
    path = validator(normalized())
    checked = path.read_bytes()
    actual_run = subprocess.run

    def replace_then_run(command, **kwargs):
        path.write_text("raise SystemExit(99)")
        assert command[1] == "-"
        assert kwargs["input"] == checked
        return actual_run(command, **kwargs)

    monkeypatch.setattr(worker.subprocess, "run", replace_then_run)
    assert worker.load_strict_routes()["author"] == "qwen38"


@pytest.mark.parametrize(
    "flags,expected",
    [
        ("--allowed-routes", False),
        ("--allowed-routes --reviewer-allowed-routes", False),
        (
            "--allowed-routes-old --reviewer-allowed-routes --reviewer-pool-routes",
            False,
        ),
        ("--allowed-routes --reviewer-allowed-routes --reviewer-pool-routes", True),
    ],
)
def test_all_role_flags_required(flags, expected):
    result = subprocess.CompletedProcess([], 0, flags)
    with patch.object(worker.subprocess, "run", return_value=result):
        assert worker._driver_supports_strict_routes("zoder") is expected
