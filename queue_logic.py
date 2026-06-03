"""Pure queue-dispatch helpers for the GRAEAE hive bus.

No I/O, no DB, no framework imports — just the decision logic, so it is unit-
testable in isolation and wired into agent_bus.py (submit dedup + dequeue
eligibility) separately.

Two concerns:
  * ``dedup_key`` — a stable hash that identifies a job for de-duplication,
    SCOPED by tenant + kind + input (+ version + time-window). An explicit
    ``idempotency_key`` overrides (caller owns rerun semantics). This is what
    stops the 129-duplicate-SHA fan-out (a barrage of near-identical submits all
    executing) without suppressing legitimately-distinct reruns.
  * HARD vs SOFT capability matching — HARD labels (hardware/binary constraints,
    e.g. ``hard:gpu``, ``hard:build-rust``) are strict: a worker missing one is
    NOT eligible. SOFT labels (affinity/workspace, e.g. ``soft:workspace-riskybiz``)
    only bias the score. A load signal lowers the score so work spreads instead
    of thundering-herding onto one capable worker. This is the fix for both the
    host-pin starvation AND the fully-open dead-letter thrash.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

_SEP = "\x1f"  # unit separator — unambiguous field delimiter for the hash basis
LOAD_PENALTY = 0.25


def dedup_key(
    *,
    tenant: str | None = None,
    kind: str | None = None,
    description: str | None = None,
    version: str = "",
    window: str = "",
    idempotency_key: str | None = None,
) -> str:
    """Stable sha256 dedup key. ``idempotency_key`` (if given) is authoritative;
    otherwise the key is scoped by tenant+kind+normalized-input+version+window."""
    if idempotency_key:
        basis = "idk" + _SEP + idempotency_key
    else:
        basis = _SEP.join(
            [tenant or "_", kind or "", (description or "").strip(), version or "", window or ""]
        )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def _split_labels(labels) -> tuple[set[str], set[str]]:
    """Partition labels into (hard, soft). Bare labels default to SOFT."""
    hard: set[str] = set()
    soft: set[str] = set()
    for label in labels or []:
        if label.startswith("hard:"):
            hard.add(label[len("hard:") :])
        elif label.startswith("soft:"):
            soft.add(label[len("soft:") :])
        else:
            soft.add(label)
    return hard, soft


@dataclass(frozen=True)
class MatchResult:
    eligible: bool
    score: float
    reason: str


def match(
    job_labels,
    worker_caps,
    *,
    worker_load: float = 0.0,
) -> MatchResult:
    """Decide whether ``worker_caps`` can run a job needing ``job_labels``.

    HARD labels must ALL be present (or the worker advertises ``*``), else the
    worker is INELIGIBLE (strict). SOFT labels raise the score (affinity).
    ``worker_load`` (0..1, fraction busy) lowers the score so the dispatcher can
    spread work and avoid herding onto a single capable worker.
    """
    caps = set(worker_caps or [])
    star = "*" in caps
    hard, soft = _split_labels(job_labels)

    if not star and not hard.issubset(caps):
        missing = sorted(hard - caps)
        return MatchResult(False, 0.0, f"missing hard caps: {missing}")

    if soft:
        affinity = len(soft & caps) / len(soft)
    else:
        affinity = 1.0
    score = affinity - LOAD_PENALTY * max(0.0, worker_load)
    return MatchResult(True, score, f"hard ok; soft {len(soft & caps)}/{len(soft)}; load {worker_load}")
