"""Tests for the frozen typed run-event contract shared by the governor's EventSink emitter and its
readers (:class:`~concursus.state.statestore.RunEvent` / :class:`RunEventKind`).

The governor emits episode-BOUNDARY events through the opt-in :class:`EventSink` Protocol. Emitter
and readers share ONE closed vocabulary (:class:`RunEventKind`); :func:`check_run_event_alignment`
is the build-time drift guard that fails if the emitter ever sends a kind the readers don't know —
so an emitter/reader mismatch fails at test/build time, never silently at runtime.

Everything is offline: a fake Supervisor + an :class:`InProcessStateStore`, no AWS touched.
"""

import pytest

from concursus import (
    RUN_EVENT_KINDS,
    GovernorLoop,
    RunEvent,
    RunEventContractError,
    RunEventKind,
    check_run_event_alignment,
)
from concursus.governor.loop import GOV_EVENT_KINDS
from concursus.state.statestore import InProcessStateStore

# Reuse the offline governor-loop fixtures (fake Supervisor + two-node manifests + plan model fn).
from test_governor_loop import (  # noqa: E402
    _fresh_fake,
    _plan_model_fn,
    _two_node_manifests,
)


# -- the closed vocabulary --------------------------------------------------
def test_run_event_kind_is_a_str_enum_with_the_three_boundary_kinds():
    """RunEventKind is a str-subclass enum whose members == their bare string values."""
    assert RunEventKind.EPISODE_START == "episode_start"
    assert RunEventKind.EPISODE_END == "episode_end"
    assert RunEventKind.DECISION == "decision"
    # str() / f-string render as the bare value (no 'RunEventKind.' prefix), like RecordStatus.
    assert str(RunEventKind.DECISION) == "decision"
    assert f"{RunEventKind.EPISODE_START}" == "episode_start"
    assert RUN_EVENT_KINDS == frozenset({"episode_start", "episode_end", "decision"})


def test_run_event_is_a_plain_dict_typeddict():
    """RunEvent is a TypedDict — instances are plain dicts (a VALUE, never a live handle)."""
    ev: RunEvent = {"type": "episode_start", "run_id": "r", "round": 0,
                    "completed": [], "frontier": ["a"]}
    assert isinstance(ev, dict)
    assert ev["type"] in RUN_EVENT_KINDS


# -- the build-time drift guard ---------------------------------------------
def test_check_run_event_alignment_accepts_the_closed_set():
    """The exact closed vocabulary passes the guard (and enum members are accepted too)."""
    check_run_event_alignment(RUN_EVENT_KINDS)  # bare strings
    check_run_event_alignment(list(RunEventKind))  # enum members (str subclass)


def test_check_run_event_alignment_rejects_a_drifted_kind():
    """A kind outside the closed vocabulary raises — the emitter/reader mismatch fails at build."""
    with pytest.raises(RunEventContractError) as exc:
        check_run_event_alignment({"episode_start", "made_up_kind"})
    assert "made_up_kind" in str(exc.value)


def test_emitter_kinds_are_a_subset_of_the_reader_vocabulary():
    """The build-time invariant: every kind the GovernorLoop emitter emits is a RunEventKind the
    readers switch on. This is the drift guard that would fail if the emitter grew a new kind
    without the readers' closed vocabulary being extended to match."""
    check_run_event_alignment(GOV_EVENT_KINDS)
    assert GOV_EVENT_KINDS <= RUN_EVENT_KINDS


# -- end-to-end: the emitter emits exactly the typed contract ---------------
class _RecordingSink:
    """An :class:`EventSink` that records every emitted event."""

    def __init__(self):
        self.events = []

    def emit(self, event):
        self.events.append(dict(event))


def _run_with_sink(sink):
    fake = _fresh_fake()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=InProcessStateStore(),
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        backend="python",
        event_sink=sink,
    )
    loop.run({"uri": "s3://doc"})
    return sink


def test_governor_emits_only_kinds_in_the_closed_vocabulary():
    """A live run's emitted event 'type's are all members of the closed RunEventKind vocabulary —
    so the shared contract holds end-to-end, not just in the static guard."""
    sink = _run_with_sink(_RecordingSink())
    emitted = {e["type"] for e in sink.events}
    assert emitted, "expected at least one boundary event"
    # The end-to-end emitter output passes the same build-time drift guard the readers rely on.
    check_run_event_alignment(emitted)
    assert emitted <= RUN_EVENT_KINDS


def test_emitted_events_match_the_run_event_shape():
    """Every emitted event carries the boundary-invariant RunEvent keys, with per-kind extras only
    where the contract defines them (done/progressed on episode_end; route on decision)."""
    sink = _run_with_sink(_RecordingSink())
    for e in sink.events:
        # Boundary-invariant keys always present.
        assert e["type"] in RUN_EVENT_KINDS
        assert isinstance(e["run_id"], str)
        assert isinstance(e["round"], int)
        assert isinstance(e["completed"], list)
        assert isinstance(e["frontier"], list)
        # No live handle leaked through the VALUE.
        assert "plan" not in e and "state" not in e
    end = next(e for e in sink.events if e["type"] == RunEventKind.EPISODE_END)
    assert "done" in end and "progressed" in end
    decision = next(e for e in sink.events if e["type"] == RunEventKind.DECISION)
    assert "route" in decision
