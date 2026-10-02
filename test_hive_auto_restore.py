"""Focused retry policy checks for the Hive auto-restore timer."""

import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "hive_auto_restore", Path(__file__).with_name("hive-auto-restore.py")
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
auto_retry_allowed = module.auto_retry_allowed


def test_build_failures_do_not_retry_without_opt_in():
    assert not auto_retry_allowed(
        {"kind": "build:zeroclaw", "result": {"exit_code": 1}}
    )
    assert not auto_retry_allowed({"kind": "build:zeroclaw", "result": None})
    assert not auto_retry_allowed(
        {"kind": "build:zeroclaw", "result": {"retryable": "true"}}
    )


def test_build_retry_requires_boolean_true():
    assert auto_retry_allowed({"kind": "build:zeroclaw", "result": {"retryable": True}})


def test_nonbuild_retry_policy_is_unchanged():
    assert auto_retry_allowed({"kind": "review:code", "result": {"exit_code": 1}})
