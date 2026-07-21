"""Tests for the OPT-IN KTLO admission primitives (all PURE, all persisted through the SSOT).

These three gates are STANDALONE, default-OFF admission helpers a caller consults BEFORE dispatching
an episode; none of them is wired into the default :class:`KTLODaemon` path, so the daemon behaves
byte-for-byte as before unless a caller constructs and uses one.

* :class:`FireBudgetGate` — a PURE per-(source, entity) fire-budget gate. ``can_fire`` never
  mutates; consumption is a SEPARATE ``commit_fire`` a caller runs only AFTER its own durable
  commit, so an un-dispatched episode never burns budget. State is the append-only StateStore.
* :class:`ProvenanceGuard` — drops events the fleet itself emitted (breaks the self-trigger loop).
* :class:`EpisodeAdmissionGate` — a :class:`DetectionMode` (new_items | state_change | diff) over a
  persisted seen-key set; ``admit`` is a pure read, ``commit`` folds the signal in after dispatch.

Everything is offline: an :class:`InProcessStateStore` per gate, no AWS.
"""

import pytest

from concursus import (
    DetectionMode,
    EpisodeAdmissionGate,
    FireBudgetGate,
    ProvenanceGuard,
)
from concursus.governor import (
    DetectionMode as DetectionModeFromSubpkg,
    EpisodeAdmissionGate as EpisodeAdmissionGateFromSubpkg,
    FireBudgetGate as FireBudgetGateFromSubpkg,
    ProvenanceGuard as ProvenanceGuardFromSubpkg,
)
from concursus.state.statestore import InProcessStateStore

# Exported from BOTH the top-level package and the subpackage (same objects).
assert FireBudgetGate is FireBudgetGateFromSubpkg
assert ProvenanceGuard is ProvenanceGuardFromSubpkg
assert EpisodeAdmissionGate is EpisodeAdmissionGateFromSubpkg
assert DetectionMode is DetectionModeFromSubpkg


# == FireBudgetGate: PURE can_fire + separate durable commit ===================
def test_can_fire_is_pure_and_does_not_consume_budget():
    """``can_fire`` NEVER mutates state — repeated calls stay ``True`` until a fire is committed."""
    gate = FireBudgetGate(InProcessStateStore())
    for _ in range(5):
        assert gate.can_fire("src", "entity", max_fires=1) is True
    # No commit happened => no budget consumed.
    assert gate.fires("src", "entity") == 0


def test_max_fires_cap_enforced_only_after_commit():
    """A per-(source, entity) cap of ``max_fires`` admits exactly that many committed fires."""
    gate = FireBudgetGate(InProcessStateStore())
    assert gate.can_fire("s", "e", max_fires=2) is True
    gate.commit_fire("s", "e")
    assert gate.can_fire("s", "e", max_fires=2) is True
    gate.commit_fire("s", "e")
    # Cap reached — third fire is inadmissible.
    assert gate.can_fire("s", "e", max_fires=2) is False
    assert gate.fires("s", "e") == 2


def test_budget_is_per_source_entity_pair():
    """Budget cells are independent per (source, entity) — one exhausting does not affect another."""
    gate = FireBudgetGate(InProcessStateStore())
    gate.commit_fire("srcA", "e1")
    assert gate.can_fire("srcA", "e1", max_fires=1) is False
    # Different entity, and different source — both still fresh.
    assert gate.can_fire("srcA", "e2", max_fires=1) is True
    assert gate.can_fire("srcB", "e1", max_fires=1) is True


def test_cooldown_blocks_refire_until_elapsed():
    """A positive ``cooldown_s`` blocks a re-fire until it has elapsed since the last committed fire."""
    clock = {"t": 1000.0}
    gate = FireBudgetGate(InProcessStateStore(), clock=lambda: clock["t"])
    # High cap so only the cooldown can block.
    assert gate.can_fire("s", "e", cooldown_s=60.0, max_fires=100) is True
    gate.commit_fire("s", "e")
    # 30s later — still cooling down.
    clock["t"] = 1030.0
    assert gate.can_fire("s", "e", cooldown_s=60.0, max_fires=100) is False
    # 60s later — cooldown elapsed.
    clock["t"] = 1060.0
    assert gate.can_fire("s", "e", cooldown_s=60.0, max_fires=100) is True


def test_max_fires_none_disables_cap_cooldown_only():
    """``max_fires=None`` disables the count cap — only the cooldown gates re-fires."""
    clock = {"t": 0.0}
    gate = FireBudgetGate(InProcessStateStore(), clock=lambda: clock["t"])
    for _ in range(10):
        assert gate.can_fire("s", "e", cooldown_s=5.0, max_fires=None) is True
        gate.commit_fire("s", "e")
        clock["t"] += 5.0  # advance past the cooldown each time
    assert gate.fires("s", "e") == 10


def test_budget_survives_a_fresh_gate_over_the_same_store():
    """The budget is persisted in the StateStore SSOT — a fresh gate over the same store sees it
    (models a resume: replay the log, reconstruct the projection, re-consult the gate)."""
    store = InProcessStateStore()
    FireBudgetGate(store).commit_fire("s", "e")
    # A brand-new gate object over the SAME store reads the committed budget.
    resumed = FireBudgetGate(store)
    assert resumed.fires("s", "e") == 1
    assert resumed.can_fire("s", "e", max_fires=1) is False


def test_commit_after_durable_work_pattern():
    """The intended contract: can_fire -> do durable work -> commit_fire ONLY on success, so a
    failed/undispatched episode never burns budget."""
    gate = FireBudgetGate(InProcessStateStore())

    def dispatch(should_fail):
        if not gate.can_fire("s", "e", max_fires=3):
            return "skipped"
        try:
            if should_fail:
                raise RuntimeError("episode raised before durable commit")
        except RuntimeError:
            return "failed"  # NO commit — budget preserved
        gate.commit_fire("s", "e")
        return "fired"

    assert dispatch(should_fail=True) == "failed"
    assert gate.fires("s", "e") == 0  # failure did not consume budget
    assert dispatch(should_fail=False) == "fired"
    assert gate.fires("s", "e") == 1


# == ProvenanceGuard: drop self-triggered events ===============================
def test_provenance_guard_drops_self_emitted_events():
    """An event stamped with one of the fleet's own provenance ids is dropped (self-trigger guard)."""
    guard = ProvenanceGuard({"fleet-A", "fleet-B"})
    external = {"id": "x1", "emitted_by": "some-other-system"}
    self_made = {"id": "x2", "emitted_by": "fleet-A"}
    assert guard.is_self_triggered(self_made) is True
    assert guard.is_self_triggered(external) is False
    admitted = guard.admit([external, self_made])
    assert admitted == [external]


def test_provenance_guard_checks_multiple_conventional_keys():
    """Provenance is checked across several conventional keys (emitted_by / provenance / ...)."""
    guard = ProvenanceGuard({"me"})
    assert guard.is_self_triggered({"provenance": "me"}) is True
    assert guard.is_self_triggered({"source_fleet": "me"}) is True
    assert guard.is_self_triggered({"fleet_id": "me"}) is True
    assert guard.is_self_triggered({"emitted_by": "you"}) is False


def test_provenance_guard_custom_keys():
    """A caller can override which keys carry provenance."""
    guard = ProvenanceGuard({"me"}, provenance_keys=("origin",))
    assert guard.is_self_triggered({"origin": "me"}) is True
    # The default keys are NOT consulted when overridden.
    assert guard.is_self_triggered({"emitted_by": "me"}) is False


def test_provenance_guard_empty_fleet_admits_everything():
    """A guard over no fleet ids drops nothing (pure pass-through)."""
    guard = ProvenanceGuard([])
    batch = [{"emitted_by": "anyone"}, {"id": "z"}]
    assert guard.admit(batch) == batch


# == EpisodeAdmissionGate: detection modes over a persisted seen set ===========
def test_new_items_mode_admits_each_key_once():
    """``new_items`` admits a key the FIRST time only; a re-poll of the same key is dropped."""
    gate = EpisodeAdmissionGate(DetectionMode.NEW_ITEMS, InProcessStateStore())
    sig = {"id": "ticket-1"}
    assert gate.admit(sig) is True
    gate.commit(sig)  # dispatched — fold it in
    assert gate.admit(sig) is False  # already seen
    assert gate.admit({"id": "ticket-2"}) is True  # a new key


def test_admit_is_pure_until_commit():
    """``admit`` NEVER mutates the seen set — only ``commit`` does."""
    gate = EpisodeAdmissionGate("new_items", InProcessStateStore())
    sig = {"id": "k"}
    assert gate.admit(sig) is True
    assert gate.admit(sig) is True  # still admissible: admit did not consume
    gate.commit(sig)
    assert gate.admit(sig) is False


def test_state_change_mode_refires_only_on_changed_state():
    """``state_change`` re-fires only when the entity's state hash DIFFERS from the last seen."""
    gate = EpisodeAdmissionGate(DetectionMode.STATE_CHANGE, InProcessStateStore())
    v1 = {"id": "order-9", "state": {"status": "OPEN"}}
    assert gate.admit(v1) is True
    gate.commit(v1)
    # Same state re-polled — no change, no re-fire.
    assert gate.admit({"id": "order-9", "state": {"status": "OPEN"}}) is False
    # State changed — re-fire.
    v2 = {"id": "order-9", "state": {"status": "CLOSED"}}
    assert gate.admit(v2) is True
    gate.commit(v2)
    assert gate.admit(v2) is False


def test_diff_mode_admits_only_new_items():
    """``diff`` admits when the signal carries item keys not previously seen, and ``diff`` returns
    exactly those new keys."""
    gate = EpisodeAdmissionGate(DetectionMode.DIFF, InProcessStateStore())
    first = {"items": ["a", "b"]}
    assert gate.admit(first) is True
    assert gate.diff(first) == ["a", "b"]
    gate.commit(first)
    # A poll with the same items => nothing new.
    assert gate.admit({"items": ["a", "b"]}) is False
    assert gate.diff({"items": ["a", "b"]}) == []
    # A poll adding "c" => admit, and only "c" is new.
    grown = {"items": ["a", "b", "c"]}
    assert gate.admit(grown) is True
    assert gate.diff(grown) == ["c"]


def test_admission_seen_set_survives_a_fresh_gate_over_the_same_store():
    """The seen-key set is persisted in the SSOT — a fresh gate over the same store sees it
    (models a resume)."""
    store = InProcessStateStore()
    g1 = EpisodeAdmissionGate(DetectionMode.NEW_ITEMS, store)
    g1.commit({"id": "seen-before"})
    resumed = EpisodeAdmissionGate(DetectionMode.NEW_ITEMS, store)
    assert resumed.admit({"id": "seen-before"}) is False
    assert resumed.admit({"id": "novel"}) is True


def test_detection_mode_is_str_enum():
    """``DetectionMode`` is a ``str`` subclass, so ``== "new_items"`` and str() keep working."""
    assert DetectionMode.NEW_ITEMS == "new_items"
    assert str(DetectionMode.DIFF) == "diff"
    # A bare string is accepted at construction.
    gate = EpisodeAdmissionGate("state_change", InProcessStateStore())
    assert gate.mode == DetectionMode.STATE_CHANGE


def test_invalid_detection_mode_rejected():
    """An unknown detection mode is rejected at construction."""
    with pytest.raises(ValueError):
        EpisodeAdmissionGate("bogus_mode", InProcessStateStore())


# == default-off: the primitives never touch the default daemon path ===========
def test_admission_primitives_default_to_offline_inprocess_store():
    """Each gate defaults to a zero-dependency in-process store when none is passed (offline)."""
    assert FireBudgetGate().can_fire("s", "e") is True
    assert EpisodeAdmissionGate("new_items").admit({"id": "x"}) is True
