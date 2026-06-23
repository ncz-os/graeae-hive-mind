"""Plain-python tests for queue_logic (pure stdlib; run: python3 test_queue_logic.py)."""
import sys
import importlib.util

spec = importlib.util.spec_from_file_location("queue_logic", sys.argv[1] if len(sys.argv) > 1 else "queue_logic.py")
m = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = m
spec.loader.exec_module(m)

dedup_key, match = m.dedup_key, m.match
n = 0


def check(c, label):
    global n
    assert c, "FAIL: " + label
    n += 1


# ── dedup_key ──
k1 = dedup_key(tenant="t", kind="fix:x", description="do the thing")
check(k1 == dedup_key(tenant="t", kind="fix:x", description="do the thing"), "deterministic")
check(k1 == dedup_key(tenant="t", kind="fix:x", description="  do the thing  "), "whitespace-normalized")
check(k1 != dedup_key(tenant="t2", kind="fix:x", description="do the thing"), "tenant-scoped")
check(k1 != dedup_key(tenant="t", kind="fix:y", description="do the thing"), "kind-scoped")
check(k1 != dedup_key(tenant="t", kind="fix:x", description="other"), "input-scoped")
check(
    dedup_key(tenant="t", kind="k", description="d", version="v2")
    != dedup_key(tenant="t", kind="k", description="d", version="v1"),
    "version-scoped",
)
# idempotency key authoritative + lets legitimate reruns differ
check(
    dedup_key(idempotency_key="run-1", tenant="t", kind="k", description="d")
    != dedup_key(idempotency_key="run-2", tenant="t", kind="k", description="d"),
    "idempotency distinguishes reruns",
)
check(len(k1) == 64, "sha256 hex")

# ── HARD matching (strict) ──
r = match(["hard:gpu"], ["gpu", "python"])
check(r.eligible and r.reason.startswith("hard ok"), "hard present -> eligible")
r = match(["hard:gpu"], ["python"])
check(not r.eligible and "missing hard" in r.reason, "hard missing -> ineligible")
r = match(["hard:build-rust", "hard:gpu"], ["gpu"])
check(not r.eligible, "partial hard -> ineligible")
r = match(["hard:gpu"], ["*"])
check(not r.eligible, "star wildcard does NOT bypass hard (honest-caps mandate)")
check(match(["riskyeats"], ["*"]).eligible, "star still eligible for bare/soft labels")

# ── SOFT preference (never blocks) ──
r = match(["soft:workspace-riskybiz"], ["python"])
check(r.eligible, "missing soft still eligible")
hit = match(["soft:ws"], ["ws", "python"]).score
miss = match(["soft:ws"], ["python"]).score
check(hit > miss, "soft match scores higher")
# bare label defaults to soft (does not block)
check(match(["riskyeats"], ["python"]).eligible, "bare label is soft (non-blocking)")

# ── load penalty (anti-herd) ──
idle = match(["soft:ws"], ["ws"], worker_load=0.0).score
busy = match(["soft:ws"], ["ws"], worker_load=0.8).score
check(idle > busy, "higher load -> lower score")

# ── combined: hard gate + soft/load ordering ──
r = match(["hard:gpu", "soft:ws"], ["gpu", "ws"], worker_load=0.0)
check(r.eligible and r.score > 0.5, "hard ok + full soft + idle")

print(f"ALL {n} CHECKS PASSED")
