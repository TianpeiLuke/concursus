"""Tests for the OPT-IN, PURE idle-runtime culler (:class:`IdleRuntimeCuller`).

The culler answers ONE question purely: given ``{runtime -> last_active_ts}``, the wall clock
``now_ts``, the in-flight ``active`` set, and a per-runtime ``tiers`` map, WHICH idle runtimes are
eligible to reclaim? It COMPUTES the set only — teardown is the caller's, and a reclaimed runtime's
identity persists in the durable ledger so it re-provisions on next invoke.

The KiRoom defenses it copies EXACTLY:

* NEVER cull an ``active`` (in-flight) runtime, whatever its ``last_active`` reads.
* Validate WALL-CLOCK ``elapsed >= floor`` before culling; ``elapsed < floor`` RESCHEDULES (keep) —
  drift-safe (a negative/skewed elapsed keeps rather than reclaims).

Two idle floors: ``standing`` (and the single most-recently-active) runtimes are held to the LONG
floor; everything else to the SHORT floor. Everything is offline — a pure function, no AWS.
"""

import pytest

from concursus.governor.ktlo import (
    CULL_TIER_EPHEMERAL,
    CULL_TIER_STANDING,
    IdleRuntimeCuller,
    KTLODaemonError,
)


# == DEFENSE 2: wall-clock elapsed < floor RESCHEDULES (keeps) =================
def test_elapsed_below_floor_keeps_the_runtime():
    """A runtime idle for LESS than its floor is NOT culled — it is rescheduled (kept)."""
    culler = IdleRuntimeCuller(long_floor_s=3600.0, short_floor_s=300.0)
    now = 1000.0
    # idle 200s < the 300s short floor => keep.
    assert culler.cull({"r1": now - 200.0}, now) == set()


def test_elapsed_at_or_above_floor_culls():
    """A runtime idle for >= its short floor is eligible to reclaim."""
    culler = IdleRuntimeCuller(
        long_floor_s=3600.0, short_floor_s=300.0, protect_most_recent=False
    )
    now = 1000.0
    # idle exactly 300s (== floor) and 500s (> floor) => both cull; disable most-recent
    # protection so both ephemerals are judged purely on their own elapsed.
    result = culler.cull(
        {"at_floor": now - 300.0, "over_floor": now - 500.0},
        now,
    )
    assert result == {"at_floor", "over_floor"}


def test_negative_elapsed_from_clock_skew_keeps_drift_safe():
    """A ``last_active`` stamped in the FUTURE (clock skew) yields negative elapsed < floor => keep."""
    culler = IdleRuntimeCuller(long_floor_s=3600.0, short_floor_s=300.0)
    now = 1000.0
    # last_active 50s in the future => elapsed = -50 < any floor => rescheduled (kept), never culled.
    assert culler.cull({"skewed": now + 50.0}, now) == set()


# == DEFENSE 1: an active (in-flight) runtime is NEVER culled ==================
def test_active_runtime_is_never_culled_even_when_long_idle():
    """An in-flight runtime is untouchable regardless of how idle its last_active reads."""
    culler = IdleRuntimeCuller(long_floor_s=3600.0, short_floor_s=300.0)
    now = 1_000_000.0
    last_active = {"busy": 0.0, "idle": 0.0}  # both wildly past any floor
    # "busy" is in-flight => kept; "idle" is reclaimable.
    assert culler.cull(last_active, now, active={"busy"}) == {"idle"}


# == the STANDING long floor ===================================================
def test_standing_tier_held_to_long_floor():
    """A ``standing`` runtime is held to the LONG floor: idle past the SHORT floor but under the
    LONG floor keeps it, while an ephemeral peer at the same idle age is culled."""
    culler = IdleRuntimeCuller(long_floor_s=3600.0, short_floor_s=300.0)
    now = 10_000.0
    idle_age = 1800.0  # 30 min: > short floor (300s), < long floor (3600s)
    last_active = {"std": now - idle_age, "eph": now - idle_age}
    tiers = {"std": CULL_TIER_STANDING, "eph": CULL_TIER_EPHEMERAL}
    # standing kept (under long floor); ephemeral culled (over short floor). Protect the freshest
    # (both equal) deterministically by disabling most-recent protection so tiers alone decide.
    result = culler.cull(last_active, now, tiers=tiers)
    assert "eph" in result
    assert "std" not in result


def test_standing_tier_culled_once_long_floor_cleared():
    """A ``standing`` runtime IS reclaimed once idle beyond the LONG floor."""
    culler = IdleRuntimeCuller(long_floor_s=3600.0, short_floor_s=300.0)
    now = 100_000.0
    last_active = {"std": now - 7200.0}  # 2h idle > 1h long floor
    assert culler.cull(last_active, now, tiers={"std": CULL_TIER_STANDING}) == {"std"}


# == most-recently-active protection (a KiRoom defense) ========================
def test_most_recent_runtime_held_to_long_floor():
    """The single most-recently-active runtime is held to the LONG floor even without a tier —
    a stale peer past the SHORT floor is culled while the freshest is protected."""
    culler = IdleRuntimeCuller(long_floor_s=3600.0, short_floor_s=300.0)
    now = 10_000.0
    # both idle past the short floor; "fresh" is the most recent (smaller idle age).
    last_active = {"fresh": now - 400.0, "stale": now - 500.0}
    result = culler.cull(last_active, now)
    # "fresh" is protected by the long floor (400s < 3600s); "stale" culled (500s > 300s short).
    assert result == {"stale"}


def test_protect_most_recent_can_be_disabled():
    """With ``protect_most_recent=False`` the most-recently-active runtime is judged on the SHORT
    floor like any other ephemeral."""
    culler = IdleRuntimeCuller(
        long_floor_s=3600.0, short_floor_s=300.0, protect_most_recent=False
    )
    now = 10_000.0
    last_active = {"fresh": now - 400.0, "stale": now - 500.0}
    # both past the short floor and no most-recent protection => both culled.
    assert culler.cull(last_active, now) == {"fresh", "stale"}


# == purity / totality =========================================================
def test_cull_is_pure_does_not_mutate_inputs():
    """``cull`` mutates none of its inputs and is repeatable (same inputs => same set)."""
    culler = IdleRuntimeCuller(long_floor_s=3600.0, short_floor_s=300.0)
    now = 10_000.0
    last_active = {"a": now - 500.0, "b": now - 100.0}
    active = {"b"}
    tiers = {"a": CULL_TIER_EPHEMERAL}
    snap_last = dict(last_active)
    first = culler.cull(last_active, now, active=active, tiers=tiers)
    second = culler.cull(last_active, now, active=active, tiers=tiers)
    assert first == second
    assert last_active == snap_last  # inputs untouched
    assert active == {"b"}


def test_empty_inputs_yield_empty_cull_set():
    """No runtimes => nothing to cull (a total function on the empty input)."""
    culler = IdleRuntimeCuller(long_floor_s=3600.0, short_floor_s=300.0)
    assert culler.cull({}, 1000.0) == set()


def test_negative_floor_rejected():
    """A negative idle floor is rejected at construction (floors are wall-clock seconds >= 0)."""
    with pytest.raises(KTLODaemonError):
        IdleRuntimeCuller(long_floor_s=-1.0, short_floor_s=300.0)
    with pytest.raises(KTLODaemonError):
        IdleRuntimeCuller(long_floor_s=3600.0, short_floor_s=-1.0)


def test_floor_for_reports_the_selected_floor():
    """``floor_for`` reports the LONG floor for a standing / most-recent runtime and SHORT else."""
    culler = IdleRuntimeCuller(long_floor_s=3600.0, short_floor_s=300.0)
    assert culler.floor_for("x", {"x": CULL_TIER_STANDING}) == 3600.0
    assert culler.floor_for("x", {"x": CULL_TIER_EPHEMERAL}) == 300.0
    assert culler.floor_for("x", {}) == 300.0
    assert culler.floor_for("x", {}, most_recent="x") == 3600.0
