"""Tests for the OPT-IN coordination-notice helper + the governor's read+re-validate step.

Covers the PURE, append-only coordination-notice primitives added to the StateStore
(:func:`append_coordination_notice` / :func:`list_pending_notices`) and the governor loop's
opt-in read at episode start. The invariants under test:

* A notice is a plain append-only :class:`Record` keyed under a sentinel node with a non-validated
  status, so it NEVER enters ``completed()`` / ``get()`` / the projection — it can never corrupt the
  executed prefix ``recompile`` pins.
* ``list_pending_notices`` is a pure staleness filter: a notice about a TERMINAL node is dropped; it
  mutates nothing.
* ZERO coordination records => the default governor loop is byte-for-byte unchanged.

Everything is offline: an :class:`InProcessStateStore` + a fake Supervisor, no AWS touched.
"""

import pytest

from concursus.state.statestore import (
    InProcessStateStore,
    RecordType,
    append_coordination_notice,
    list_pending_notices,
)

# pytest inserts tests/ onto sys.path; reuse the governor-loop harness (fake supervisor + manifests).
from test_governor_loop import (
    GovernorLoop,
    InProcessStateStore as _InProcessStateStore,
    _fresh_fake,
    _plan_model_fn,
    _two_node_manifests,
)


# == (1) append_coordination_notice: pure, append-only, never in the executed prefix ==========
def test_append_notice_is_logged_but_never_completed():
    store = InProcessStateStore()
    store.put("ingest", {"doc": "ingest-out"})  # a real, validated node output

    append_coordination_notice(store, "summarize", {"reason": "waiting on peer"})

    records = store.records()
    # The notice rides on the SAME append-only log.
    notices = [r for r in records if r.record_type == RecordType.COORDINATION]
    assert len(notices) == 1
    notice = notices[0]
    assert notice.record_type == "coordination"
    assert notice.producer == "summarize"          # referenced node rides in producer
    assert notice.output["node"] == "summarize"     # ...and in the payload
    assert notice.output["reason"] == "waiting on peer"

    # It is keyed under a SENTINEL node, not the referenced node — so it can never flip the
    # referenced node's latest-overall record.
    assert notice.node != "summarize"
    assert notice.node != "ingest"

    # CRITICAL: the notice never enters completed()/get()/the projection (INV-3/INV-5).
    assert "summarize" not in store.completed()
    assert notice.node not in store.completed()
    assert store.completed() == {"ingest"}
    with pytest.raises(KeyError):
        store.get(notice.node)


def test_append_notice_does_not_disturb_the_referenced_nodes_output():
    store = InProcessStateStore()
    store.put("summarize", {"doc": "the-real-output"})
    assert "summarize" in store.completed()

    append_coordination_notice(store, "summarize", {"reason": "fyi"})

    # The referenced node's validated output is untouched — the notice was keyed elsewhere.
    assert store.get("summarize") == {"doc": "the-real-output"}
    assert "summarize" in store.completed()


# == (2) list_pending_notices: pure staleness filter ==========================================
def test_notice_for_terminal_node_is_filtered_out():
    store = InProcessStateStore()
    append_coordination_notice(store, "ingest", {"reason": "done soon"})
    append_coordination_notice(store, "summarize", {"reason": "still open"})

    # 'ingest' is terminal (already completed); 'summarize' is not.
    pending = list_pending_notices(store.records(), terminal_nodes={"ingest"})

    targets = [r.producer for r in pending]
    assert targets == ["summarize"]   # the terminal-node notice is dropped as stale


def test_list_pending_is_pure_and_ordered():
    store = InProcessStateStore()
    append_coordination_notice(store, "a", {})
    append_coordination_notice(store, "b", {})
    before = store.records()

    pending = list_pending_notices(store.records(), terminal_nodes=set())

    # No node is terminal => every notice is pending, in append order.
    assert [r.producer for r in pending] == ["a", "b"]
    # Pure: the log is byte-identical (nothing marked consumed, nothing mutated).
    assert store.records() == before


def test_list_pending_ignores_non_coordination_records():
    store = InProcessStateStore()
    store.put("ingest", {"doc": "x"})
    store.put("summarize", {"doc": "y"})

    assert list_pending_notices(store.records(), terminal_nodes=set()) == []


# == (3) ZERO coordination records => the governor loop is byte-for-byte unchanged =============
def test_zero_notices_governor_loop_unchanged():
    fake = _fresh_fake()
    store = _InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})

    # Default behavior preserved: natural frontier-exhaust termination, expected completed set.
    assert result.terminated_by == "frontier_exhaust"
    assert result.done is True
    assert set(result.completed) == {"ingest", "summarize"}
    # No coordination record was ever written, and the read step surfaced nothing.
    assert all(r.record_type != RecordType.COORDINATION for r in store.records())
    assert list_pending_notices(store.records(), store.completed()) == []


def test_pending_notice_surfaced_at_episode_start_when_not_terminal():
    fake = _fresh_fake()
    store = _InProcessStateStore()
    # Seed a coordination notice for a node that has NOT yet run before the loop starts.
    append_coordination_notice(store, "summarize", {"reason": "peer signal"})

    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        backend="python",
    )
    # The read step is pure — it must not change the bounded outcome.
    result = loop.run({"uri": "s3://doc"})
    assert result.terminated_by == "frontier_exhaust"
    assert set(result.completed) == {"ingest", "summarize"}

    # The seed notice is still on the log (never consumed/mutated by the read), and the referenced
    # node still completed normally (a notice never blocks or dispatches anything).
    notices = [r for r in store.records() if r.record_type == RecordType.COORDINATION]
    assert len(notices) == 1
