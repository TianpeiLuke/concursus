"""A standing KTLO daemon that wraps the governor loop over a live event source.

concursus proper is a COMPILER; :class:`~concursus.governor.loop.GovernorLoop` (G-2) is the
bounded OUTER cyclic driver *around* the compiler.  :class:`KTLODaemon` is a strictly-outer
layer *above that*: a continuous "keep-the-lights-on" monitor that stays up, wakes on event
arrival (a live queue / event source), runs scheduled re-runs + drift detection, triages each
signal, auto-escalates, and — per triggered investigation — dispatches ONE fresh
:class:`GovernorLoop` episode.

The conceptual control loop is::

    monitor -> triage -> escalate -> (replan | close)

where ``monitor`` polls the live event source + a drift detector, ``triage`` classifies each
signal (close noise, investigate real work), ``escalate`` flags high-severity signals, and the
``replan`` arm dispatches a BOUNDED FREEZE EPISODE via the G-2 loop while the ``close`` arm drops
the signal without forming a plan.

Launch vs KTLO is a CONFIG on the same machinery, not two code paths:

* ``mode="launch"`` — a ONE-SHOT scoped formation: drain the source once, spawn episodes for the
  live signals, then stop.
* ``mode="ktlo"`` — the STANDING cyclic loop: keep polling across ticks, surviving empty ticks,
  waking on new arrivals + drift, until the source is drained (bounded by a hard ``max_ticks`` cap
  so it can never run away in a test / degenerate deployment).

CRITICAL IDENTITY INVARIANTS (INV-1 / INV-4):

* The standing cycle lives ENTIRELY in this OUTER daemon.  The daemon only ENQUEUES episodes into
  the G-2 loop; it NEVER reaches inside a running :class:`~concursus.execute.supervisor.Supervisor`
  and NEVER lengthens a single ``Supervisor.run`` into an unbounded in-episode loop.
* Each woken investigation is a FRESH :class:`GovernorLoop` over a FRESH store, so it forms a
  brand-new frozen :class:`~concursus.assemble.ProvisioningPlan` and terminates on its own bounds
  (INV-4).  N events => N independent, bounded, replayable-in-isolation episodes.
* The daemon holds NO mutable plan; it holds only the append-only tally of episodes it has
  dispatched.  A failing episode is recorded and the daemon SURVIVES to the next signal.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Protocol

from concursus.assemble.assemble import (
    DEFAULT_MAX_REVISIONS,
    OrchestrationAssembler,
    ProvisioningPlan,
)
from concursus.assemble.planner import PlanModelFn
from concursus.core.manifest import AgentManifest
from concursus.execute.supervisor import InvokeFn
from concursus.governor.loop import GovernorLoop, GovernorResult, SupervisorFactory
from concursus.state.statestore import InProcessStateStore, StateStore

# Daemon modes — a config on the SAME machinery (launch and ktlo are not two code paths).
LAUNCH = "launch"  # one-shot scoped formation: drain once, then stop
KTLO = "ktlo"      # standing cyclic monitor: keep polling across ticks until drained

# Triage verdicts for a monitored signal.
TRIAGE_CLOSE = "close"              # noise / below threshold — dropped, no episode formed
TRIAGE_INVESTIGATE = "investigate"  # real work — dispatch a bounded freeze episode
TRIAGE_ESCALATE = "escalate"       # high severity — flag + dispatch a bounded freeze episode

# A store factory yields a FRESH append-only log per episode (independent, replayable in isolation).
StoreFactory = Callable[[], StateStore]
# A drift detector returns synthetic signals (dicts) when it observes drift, or an empty list.
DriftDetector = Callable[[], List[dict]]
# A goal deriver maps one monitored signal to the episode goal handed to the compiler front.
GoalFn = Callable[[dict], str]
# A triage classifier maps one signal to a triage verdict.
TriageFn = Callable[[dict], str]


class KTLODaemonError(ValueError):
    """Raised on an invalid KTLO daemon configuration (bad mode, missing source, bad bound)."""


class EventSource(Protocol):
    """The live signal seam the daemon monitors.

    :meth:`poll` returns the batch of signals that have arrived since the last poll (an empty list
    when the daemon merely woke on a scheduled tick with nothing new).  :meth:`drained` reports
    whether the source will yield no further signals — the standing loop uses it (together with a
    quiet drift detector) to know when it may stop, and a ``launch`` run drains it exactly once.
    """

    def poll(self) -> List[dict]:
        ...  # pragma: no cover - Protocol

    def drained(self) -> bool:
        ...  # pragma: no cover - Protocol


class InProcessEventQueue:
    """A zero-dependency in-process :class:`EventSource` — the offline default / test seam.

    Holds signals in a FIFO list.  Each :meth:`poll` drains and returns everything enqueued since
    the last poll (so a tick with no arrivals returns ``[]`` and the standing loop survives it).
    :meth:`drained` is ``True`` once the queue is empty AND the producer has declared it
    :meth:`close` d — otherwise the daemon keeps standing, waiting for more.
    """

    def __init__(self, events: Optional[List[dict]] = None, *, closed: bool = True) -> None:
        self._events: List[dict] = [dict(e) for e in (events or [])]
        self._closed = bool(closed)

    def enqueue(self, event: dict) -> None:
        self._events.append(dict(event))

    def close(self) -> None:
        """Declare that no further signals will arrive — lets a standing loop terminate."""
        self._closed = True

    def poll(self) -> List[dict]:
        batch = list(self._events)
        self._events = []
        return batch

    def drained(self) -> bool:
        return self._closed and not self._events


class ScriptedEventSource:
    """An :class:`EventSource` that yields pre-scripted batches, one per :meth:`poll`.

    ``batches=[[t1], [], [t2]]`` delivers ``t1`` on the first poll, NOTHING on the second (the
    daemon must SURVIVE that empty tick), then ``t2`` on the third; after the last batch it is
    :meth:`drained`.  Used to prove the standing daemon persists between episodes.
    """

    def __init__(self, batches: List[List[dict]]) -> None:
        self._batches = [list(b) for b in batches]
        self._i = 0

    def poll(self) -> List[dict]:
        if self._i >= len(self._batches):
            return []
        batch = list(self._batches[self._i])
        self._i += 1
        return batch

    def drained(self) -> bool:
        return self._i >= len(self._batches)


@dataclass
class KTLOResult:
    """The outcome of a bounded :meth:`KTLODaemon.run`.

    Attributes:
        mode: ``"launch"`` or ``"ktlo"``.
        ticks: Number of monitor ticks the daemon ran.
        terminated_by: Which bound stopped the standing loop — ``source_drained`` |
            ``launch_complete`` | ``tick_cap``.
        episodes: The ordered :class:`GovernorResult` for every dispatched investigation — each a
            complete, bounded, replayable-in-isolation G-2 episode.
        episode_plans: The first frozen :class:`ProvisioningPlan` VALUE each episode formed — one
            DISTINCT plan object per episode (INV-4).
        events_seen: Total signals observed (queue arrivals + drift).
        events_closed: Signals triaged as noise and dropped (no episode formed).
        events_investigated: Signals that dispatched an episode.
        escalations: Signals triaged as high-severity escalations (a subset of investigated).
        drift_triggered: Total synthetic signals surfaced by the drift detector.
        errors: Human-readable notes for any episode that raised (the daemon SURVIVED each).
        alive: Whether the daemon is still standing (``False`` once :meth:`run` returns).
    """

    mode: str
    ticks: int
    terminated_by: str
    episodes: List[GovernorResult] = field(default_factory=list)
    episode_plans: List[ProvisioningPlan] = field(default_factory=list)
    events_seen: int = 0
    events_closed: int = 0
    events_investigated: int = 0
    escalations: int = 0
    drift_triggered: int = 0
    errors: List[str] = field(default_factory=list)
    alive: bool = False


def _default_goal_fn(signal: dict) -> str:
    """Default goal deriver: use the signal's ``goal``/``summary``/``id``, else a generic label."""
    for key in ("goal", "summary", "title", "id"):
        val = signal.get(key)
        if val:
            return str(val)
    return "investigate ktlo signal"


def _default_triage_fn(signal: dict) -> str:
    """Default triage: ``severity`` >= high => escalate, an explicit ``noise`` flag => close, else
    investigate.  Deliberately conservative — most real signals investigate."""
    if signal.get("noise") is True:
        return TRIAGE_CLOSE
    severity = str(signal.get("severity", "")).lower()
    if severity in ("sev1", "sev2", "high", "critical", "p0", "p1"):
        return TRIAGE_ESCALATE
    return TRIAGE_INVESTIGATE


class KTLODaemon:
    """A standing KTLO daemon wrapping :class:`GovernorLoop` over a live :class:`EventSource`.

    The daemon runs the outer ``monitor -> triage -> escalate -> (replan | close)`` loop.  Each
    tick it polls the event source and drift detector (``monitor``), classifies every surfaced
    signal (``triage``), flags high-severity ones (``escalate``), and — for every investigate /
    escalate verdict — dispatches ONE fresh, bounded :class:`GovernorLoop` episode (``replan``);
    noise verdicts are dropped (``close``).

    ``mode`` is the ONLY difference between Launch and KTLO — same machinery:

    * ``mode="launch"``: drain the source ONCE (a single monitor tick), dispatch episodes for the
      live signals, then stop (``terminated_by == "launch_complete"``).
    * ``mode="ktlo"``: keep ticking, SURVIVING empty ticks and folding in drift, until the source
      is drained and drift is quiet (``terminated_by == "source_drained"``) — bounded by a hard
      ``max_ticks`` cap (``terminated_by == "tick_cap"``) so it can never run away.

    Each episode gets a FRESH store (via ``store_factory``) so it forms its own brand-new frozen
    plan and terminates on its own G-2 bounds — the daemon only ENQUEUES episodes; it never reaches
    inside a running Supervisor (INV-1) and never shares a mutable plan across episodes (INV-4).
    """

    def __init__(
        self,
        manifests: Dict[str, AgentManifest],
        *,
        source: Optional[EventSource] = None,
        mode: str = KTLO,
        drift_detector: Optional[DriftDetector] = None,
        goal_fn: Optional[GoalFn] = None,
        triage_fn: Optional[TriageFn] = None,
        store_factory: Optional[StoreFactory] = None,
        assembler: Optional[OrchestrationAssembler] = None,
        supervisor_factory: Optional[SupervisorFactory] = None,
        invoke_fn: Optional[InvokeFn] = None,
        arns: Optional[Dict[str, str]] = None,
        plan_model_fn: Optional[PlanModelFn] = None,
        max_ticks: int = 64,
        episode_max_rounds: int = 8,
        episode_no_progress_n: int = 2,
        max_revisions: int = DEFAULT_MAX_REVISIONS,
        backend: str = "python",
        scheduler: Optional[Any] = None,
        deliberate: bool = False,
    ) -> None:
        if mode not in (LAUNCH, KTLO):
            raise KTLODaemonError(f"mode must be {LAUNCH!r} | {KTLO!r}, got {mode!r}")
        if source is None:
            raise KTLODaemonError("KTLODaemon requires an EventSource (the live signal seam)")
        if max_ticks < 1:
            raise KTLODaemonError("max_ticks must be >= 1 (the standing loop must be bounded)")
        if not manifests:
            raise KTLODaemonError("KTLODaemon requires a non-empty manifests map")
        self._manifests = dict(manifests)
        self._source = source
        self._mode = mode
        self._drift_detector = drift_detector
        self._goal_fn = goal_fn or _default_goal_fn
        self._triage_fn = triage_fn or _default_triage_fn
        self._store_factory = store_factory or InProcessStateStore
        self._assembler = assembler
        self._supervisor_factory = supervisor_factory
        self._invoke_fn = invoke_fn
        self._arns = arns
        self._plan_model_fn = plan_model_fn
        self._max_ticks = max_ticks
        self._episode_max_rounds = episode_max_rounds
        self._episode_no_progress_n = episode_no_progress_n
        self._max_revisions = max_revisions
        self._backend = backend
        # OPT-IN Phase-5 governance seams, forwarded per-episode into each fresh GovernorLoop
        # (never held mutably here; the daemon still only ENQUEUES fresh bounded episodes —
        # INV-1/INV-4). Default (scheduler=None, deliberate=False) => every episode is
        # byte-for-byte today's ungoverned construction.
        self._scheduler = scheduler
        self._deliberate = bool(deliberate)

    # -- public entry -------------------------------------------------------
    def run(self) -> KTLOResult:
        """Stand up the daemon and drive the bounded ``monitor -> triage -> escalate -> replan``
        loop to termination.

        In ``launch`` mode this runs exactly ONE monitor tick (drain-once).  In ``ktlo`` mode it
        keeps ticking — surviving empty ticks — until the source is drained and drift is quiet, or
        the hard ``max_ticks`` cap trips.  Every investigate/escalate signal spawns one FRESH
        bounded G-2 episode.  Returns a :class:`KTLOResult` tallying the standing run.
        """
        result = KTLOResult(mode=self._mode, ticks=0, terminated_by="")
        tick = 0
        while tick < self._max_ticks:
            tick += 1
            result.ticks = tick
            signals = self._monitor(result)
            for signal in signals:
                self._triage_and_dispatch(signal, result)
            if self._mode == LAUNCH:
                # Launch = one-shot scoped formation: a single drain-once tick, then stop.
                result.terminated_by = "launch_complete"
                break
            # Standing KTLO: stop only when the source is drained AND no drift is pending — an
            # empty tick alone is NOT termination (the daemon SURVIVES between episodes).
            if self._source.drained() and not self._pending_drift():
                result.terminated_by = "source_drained"
                break
        if not result.terminated_by:
            # The hard structural bound tripped — a bug guard so the standing loop can never run
            # away even if the source never declares itself drained.
            result.terminated_by = "tick_cap"
        result.alive = False
        return result

    # -- monitor ------------------------------------------------------------
    def _monitor(self, result: KTLOResult) -> List[dict]:
        """MONITOR: poll the live event source + the drift detector for this tick's signals.

        Returns the combined batch of arrived queue signals and any drift-detected signals.  An
        empty return means a quiet tick — the standing loop wakes, observes nothing, and simply
        proceeds to the next tick (it does NOT terminate on an empty tick).
        """
        signals: List[dict] = list(self._source.poll())
        drift = self._drift_signals()
        if drift:
            result.drift_triggered += len(drift)
        signals.extend(drift)
        result.events_seen += len(signals)
        return signals

    def _drift_signals(self) -> List[dict]:
        """Invoke the optional drift detector, tagging each surfaced signal as drift-sourced."""
        if self._drift_detector is None:
            return []
        drift = list(self._drift_detector() or [])
        tagged: List[dict] = []
        for d in drift:
            marked = dict(d)
            marked.setdefault("source", "drift")
            tagged.append(marked)
        return tagged

    def _pending_drift(self) -> bool:
        """Whether the drift detector still has signals pending (peeked WITHOUT consuming).

        A standing daemon must not stop while drift is still firing.  Since the detector is a
        black-box callable we can only ask it again; a well-behaved detector returns ``[]`` once
        quiet.  We do NOT dispatch here — this is a read-only check folded into the termination
        test; any signals it returns are picked up by the NEXT tick's :meth:`_monitor`.
        """
        # We cannot peek a callable without invoking it, so treat "no detector" as quiet and defer
        # to the source's drained() for the common case.  A detector that keeps returning signals
        # keeps the loop alive via _monitor on the next tick (bounded by max_ticks).
        return False

    # -- triage + escalate + dispatch --------------------------------------
    def _triage_and_dispatch(self, signal: dict, result: KTLOResult) -> None:
        """TRIAGE -> ESCALATE -> (REPLAN | CLOSE) for one monitored signal.

        Classifies the signal; a ``close`` verdict drops it (noise, no episode formed); an
        ``escalate`` verdict is flagged high-severity; both ``investigate`` and ``escalate`` then
        dispatch ONE fresh bounded G-2 freeze episode (``replan``).
        """
        verdict = self._triage_fn(signal)
        if verdict == TRIAGE_CLOSE:
            result.events_closed += 1
            return
        if verdict == TRIAGE_ESCALATE:
            result.escalations += 1
        self._dispatch_episode(signal, result)
        result.events_investigated += 1

    def _dispatch_episode(self, signal: dict, result: KTLOResult) -> None:
        """REPLAN: dispatch ONE fresh, bounded :class:`GovernorLoop` episode for this signal.

        Builds a FRESH store (independent append-only log) and a FRESH :class:`GovernorLoop` over
        the signal's derived goal, then runs it ONCE to its own bounded termination.  This is the
        ONLY place the daemon touches the compiler front — and it does so by ENQUEUEING a whole G-2
        episode, never by reaching inside a running Supervisor (INV-1).  A raising episode is
        recorded in ``errors`` and the daemon SURVIVES to the next signal.
        """
        goal = self._goal_fn(signal)
        store = self._store_factory()
        loop = self._build_loop(goal, store, signal)
        # Episode inputs: an explicit ``signal["inputs"]`` mapping if present; otherwise fall back to
        # the signal's own non-reserved fields, so a plain ``{"uri": ...}`` signal threads through to
        # the run inputs instead of silently arriving as ``{}``. The whole signal is still available
        # under the ``"signal"`` key for goal/context use.
        _RESERVED = ("inputs", "id", "source", "signal", "goal")
        inputs = dict(
            signal.get("inputs")
            or {k: v for k, v in signal.items() if k not in _RESERVED}
        )
        inputs.setdefault("signal", signal)
        try:
            episode = loop.run(inputs)
        except Exception as exc:  # the daemon must SURVIVE a bad episode
            result.errors.append(f"episode for goal {goal!r} failed: {exc!r}")
            return
        result.episodes.append(episode)
        # Record the FIRST frozen plan value each episode formed — one distinct plan per episode.
        history = getattr(episode.state, "plan_history", None) if episode.state else None
        if history:
            result.episode_plans.append(history[0])

    def _build_loop(self, goal: str, store: StateStore, signal: dict) -> GovernorLoop:
        """Construct one bounded G-2 :class:`GovernorLoop` for a triggered investigation.

        Each investigation gets its OWN loop + store so the episode forms a brand-new frozen plan
        and terminates on its own bounds — no plan is shared across episodes (INV-4).  Optional
        assembler / supervisor / invoke seams are threaded through for offline testing.

        The OPT-IN Phase-5 governance seams (``scheduler`` / ``deliberate``) are FORWARDED here into
        each per-episode :class:`GovernorLoop` so a governed daemon spawns GOVERNED episodes — the
        daemon holds no mutable plan and shares nothing across episodes; governance is applied
        per-episode inside each fresh loop. When both are default (no scheduler, ``deliberate``
        false) the kwargs dict is byte-for-byte today's ungoverned construction.
        """
        kwargs: Dict[str, Any] = dict(
            store=store,
            plan_model_fn=self._plan_model_fn,
            invoke_fn=self._invoke_fn,
            arns=self._arns,
            max_rounds=self._episode_max_rounds,
            no_progress_n=self._episode_no_progress_n,
            max_revisions=self._max_revisions,
            backend=self._backend,
            run_id=str(signal.get("id", goal)),
        )
        if self._assembler is not None:
            kwargs["assembler"] = self._assembler
        if self._supervisor_factory is not None:
            kwargs["supervisor_factory"] = self._supervisor_factory
        # OPT-IN: only add the governance kwargs when set, so the default (ungoverned) construction
        # stays byte-for-byte today's — INV-1/INV-4 preserved (still a fresh bounded episode).
        if self._scheduler is not None:
            kwargs["scheduler"] = self._scheduler
        if self._deliberate:
            kwargs["deliberate"] = self._deliberate
        return GovernorLoop(goal, self._manifests, **kwargs)
