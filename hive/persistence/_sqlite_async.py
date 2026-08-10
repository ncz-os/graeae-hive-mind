"""Internal import boundary for the optional :mod:`aiosqlite` dependency."""

from __future__ import annotations

try:
    import aiosqlite as _aiosqlite
except ModuleNotFoundError as exc:
    if exc.name != "aiosqlite":
        raise
    raise ModuleNotFoundError(
        "SQLite persistence requires the optional 'aiosqlite' package; "
        "install it with 'python -m pip install aiosqlite'."
    ) from exc


# Keep the driver's public surface available through this indirection.  The
# explicit aliases cover the API used by the service and improve discovery for
# type checkers; __getattr__ preserves compatibility for other driver members.
connect = _aiosqlite.connect
Error = _aiosqlite.Error


def __getattr__(name: str):
    return getattr(_aiosqlite, name)


__all__ = ["connect", "Error"]
