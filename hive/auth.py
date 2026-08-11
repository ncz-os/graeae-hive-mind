"""Shared environment compatibility for bundled Hive HTTP clients."""

from __future__ import annotations

import os


def client_token(singular: str, plural: str) -> str:
    """Prefer the canonical singular value, accepting one legacy set member."""
    one = (os.environ.get(singular, "") or "").strip()
    many = [item.strip() for item in (os.environ.get(plural, "") or "").split(",") if item.strip()]
    if one and many and one not in many:
        raise RuntimeError(
            f"conflicting {singular} and {plural}: when both are set, "
            f"{singular} must also appear in {plural}"
        )
    return one or (many[0] if many else "")
