from __future__ import annotations


def _is_positive_int(val) -> bool:
    return isinstance(val, int) and not isinstance(val, bool) and val > 0


def _is_non_negative_int(val) -> bool:
    return isinstance(val, int) and not isinstance(val, bool) and val >= 0


def _normalize_status_str(val) -> str:
    if not isinstance(val, str):
        return ""
    # Strip case, replace spaces with hyphens, replace underscores with hyphens
    val = val.strip().lower().replace(" ", "-").replace("_", "-")
    return val


def _is_blocked_result(result) -> bool:
    if not isinstance(result, dict):
        return True

    # Check explicit markers: non-empty values are holds
    for key in (
        "held",
        "hold",
        "manual_review",
        "requires_manual_review",
        "needs_review",
        "superseded",
        "superseded_by",
    ):
        val = result.get(key)
        if val is not None and val is not False and val != "":
            return True

    # Check reason/status strings
    for key in ("reason", "status", "error", "message"):
        val = result.get(key)
        if isinstance(val, str):
            norm = _normalize_status_str(val)
            if norm in ("hold", "held", "superseded", "manual-review", "needs-review"):
                return True

    return False


def auto_retry_allowed(job: dict) -> bool:
    """
    Returns True if the job is eligible for auto-restore.
    Fails closed on missing/malformed budget fields.
    """
    # 0. Status check
    if job.get("status") != "failed":
        return False

    # 1. Budget checks (fail closed)
    max_retries = job.get("max_retries")
    retry_count = job.get("retry_count")

    if not _is_positive_int(max_retries):
        return False
    if not _is_non_negative_int(retry_count):
        return False

    # 2. Exhaustion checks
    # Server-side exhaustion
    if retry_count >= max_retries:
        return False

    # 3. Result checks
    result = job.get("result")
    if _is_blocked_result(result):
        return False

    # 4. Kind-specific checks
    kind = job.get("kind") or ""
    if kind.startswith("build"):
        # Build kinds require result.retryable is literal True
        if not isinstance(result, dict):
            return False
        if result.get("retryable") is not True:
            return False
    else:
        # Non-build failed jobs may preserve retry eligibility only when budget and hold checks pass.
        # Explicit retryable=False blocks any kind.
        if isinstance(result, dict) and result.get("retryable") is False:
            return False

    return True
