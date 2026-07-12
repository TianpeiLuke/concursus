"""The governor's fixed cyclic control loop — the OUTER driver around the compiler.

concursus is the SUBSTRATE of the OPC (One-Person-Company) operating model — a
director-not-operator system of persistent, governed crews.  The COMPILER is ONE organ:
:meth:`OrchestrationAssembler.assemble` /
:meth:`~concursus.assemble.OrchestrationAssembler.recompile` turn a DAG + manifests into a
frozen :class:`~concursus.assemble.ProvisioningPlan` VALUE, and
:meth:`~concursus.execute.supervisor.Supervisor.run` executes that plan in a SINGLE static
forward pass.  :class:`GovernorLoop` is the RUNTIME-GOVERNANCE organ: a strictly-outer bounded cycle
*around* the compiler.  Running an outer control loop is NOT a refusal to govern — it is HOW concursus
governs at OPC scale, safely and auditably: it never reaches inside a running Supervisor, never mutates
a frozen plan, and never turns the compiler into a runtime governor.

The topology is FIXED and compiled once::

    planner -> router -> run_episode -> collect -> route_after_collect
                                                     -> {planner | router | synthesize} -> END

All dynamism lives in :class:`~concursus.governor.state.GovernorState` + the append-only
:class:`~concursus.state.statestore.StateStore` log; the topology never changes.

THE SWAP (identity-preserving):

* ``planner`` forms a NEW frozen :class:`ProvisioningPlan` at the compiler front — first round via
  :func:`~concursus.assemble.planner.plan_from_goal` + ``assemble``, later rounds via
  ``recompile`` (a fresh, monotonic, revision-bumped VALUE).  It never edits a prior plan
  (INV-3/INV-4).
* ``run_episode`` calls :meth:`Supervisor.run` ONCE to completion over that frozen plan — a single
  static pass, no cycle inside the supervisor (INV-1).
* ``collect`` folds the episode's outputs into the append-only :class:`StateStore` log; it never
  mutates the frozen plan (INV-5).  The executed prefix is re-derived from ``store.completed()``
  each round, never cached mutably.
* ``router`` is a pass-through for now (G-6 fills it).

Termination is BOUNDED three ways so the loop MUST terminate: frontier-exhaustion (every node in
the plan order is completed), a ``no_progress_n`` stall bound, a ``max_rounds`` round budget, and a
hard structural ``step_cap`` analogue (like ``DKSEngine``) that can never be exceeded even if a
node/route misbehaves.

LangGraph is an OPTIONAL backend, imported LAZILY inside :meth:`_build_langgraph`; when it is
unavailable (or ``backend="python"``) the SAME node functions and routing execute via a pure-Python
fallback driver (a bounded while-loop).  concursus imports and its full suite passes with NEITHER
langgraph NOR any LLM installed.
"""

from __future__ import annotations

import json
import types
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Protocol

from concursus.assemble.assemble import (
    DEFAULT_MAX_REVISIONS,
    OrchestrationAssembler,
    ProvisioningPlan,
)
from concursus.assemble.planner import PlanModelFn, plan_from_goal
from concursus.core.dag import AgentDAG
from concursus.core.manifest import AgentManifest
from concursus.execute.supervisor import InvokeFn, Supervisor
from concursus.governor.state import GovernorState
from concursus.state.statestore import (
    InProcessStateStore,
    MemoryStateStore,
    StateStore,
    content_hash,
)

# The fixed governor cycle nodes (the linear chain; ``synthesize`` is the terminal node reached
# from the routing edge after ``collect``).
GOV_NODES = ("planner", "router", "run_episode", "collect")

# Routing sentinel for the pure-Python fallback driver (mirrors langgraph's END).
_END = "__end__"

# The supervisor-construction seam: build a runnable episode supervisor over one frozen plan.
SupervisorFactory = Callable[..., object]

# The trail_id scope-address separator (mirrors ``concursus.governor.scope.SCOPE_SEP``); duplicated
# as a literal so the read-only cockpit accessors need no module-top scope/filevault import.
SCOPE_SEP = "."

# An empty duck-typed plan for :meth:`GovernorLoop.cockpit` BEFORE the first run: a Supervisor needs
# a plan exposing ``order`` + ``wiring`` for its one-time structural gate, and ``summary`` reads
# ``order``. Never dispatched (the cockpit never calls ``run``), so an empty order is inert. Its
# ``revision`` is unused because the cockpit is built with ``plan=self._last_plan`` (``None`` pre-run,
# so DirectorCockpit reports ``revision=None``).
_EMPTY_PLAN = types.SimpleNamespace(order=[], wiring={}, revision=None)


class CheckpointStore(Protocol):
    """The OUTER-altitude resume seam: persist/load a plain-dict round checkpoint by run id.

    A checkpoint is a small pure-Python dict — ``{plan_version, iteration, no_progress, round,
    prev_completed, replan_reason}`` — a POINTER into the round sequence, NEVER a mutable plan
    snapshot.  On restart the loop re-fetches the frozen plan BY VERSION (by deterministically
    replaying the compiler front against the surviving append-only log), so the checkpoint stays a
    version+log pointer and never a serialized-then-mutated plan (INV-3/INV-5).

    Implementations must be idempotent: :meth:`save` overwrites the single latest checkpoint for a
    ``run_id``; :meth:`load` returns it (or ``None`` before the first save).  The default
    :class:`InProcessCheckpointStore` is zero-dependency and offline; a langgraph ``MemorySaver``
    lives only on the optional langgraph path.
    """

    def save(self, run_id: str, checkpoint: Dict[str, Any]) -> None:
        ...  # pragma: no cover - Protocol

    def load(self, run_id: str) -> Optional[Dict[str, Any]]:
        ...  # pragma: no cover - Protocol


class InProcessCheckpointStore:
    """Zero-dependency in-process :class:`CheckpointStore` — the offline default.

    Holds the latest checkpoint dict per ``run_id`` in a plain dict.  Copies on save AND on load so
    a caller can never mutate the stored checkpoint in place (it is a VALUE, mirroring how a frozen
    plan is a value).  A langgraph ``MemorySaver`` is used only on the optional langgraph path.
    """

    def __init__(self) -> None:
        self._by_run: Dict[str, Dict[str, Any]] = {}

    def save(self, run_id: str, checkpoint: Dict[str, Any]) -> None:
        self._by_run[str(run_id)] = dict(checkpoint)

    def load(self, run_id: str) -> Optional[Dict[str, Any]]:
        ckpt = self._by_run.get(str(run_id))
        return dict(ckpt) if ckpt is not None else None


class GovernorLoopError(ValueError):
    """Raised on an invalid governor-loop configuration or an unknown backend."""


@dataclass
class GovernorResult:
    """The outcome of a bounded :meth:`GovernorLoop.run`.

    Attributes:
        rounds: Number of completed episodes (Supervisor.run passes).
        terminated_by: Which bound stopped the loop — ``frontier_exhaust`` | ``no_progress`` |
            ``unmatched_stall`` | ``round_cap`` | ``step_cap``.  ``unmatched_stall`` is the specific
            no_progress case where an UNMATCHED held node (a mis-registered agent) blocked the
            frontier so it never advanced at all — legible instead of an indistinguishable stall.
        done: Whether the plan's frontier was exhausted (all nodes completed).
        completed: Sorted list of completed node ids (re-derived from the log).
        frontier: The still-open frontier at termination.
        outputs: The LAST episode's returned outputs.
        state: The persistent :class:`GovernorState` (holds the full plan-value sequence).
        trace: The ordered node-visit trace.
        supervisor_runs: How many times a Supervisor was run (one per round; INV-1).
        backend: ``"langgraph"`` or ``"python"``.
        escalated: Sorted list of node ids the OPT-IN Trust-Ladder scheduler HELD (escalated
            below-bar) at any round instead of dispatching — a governance surface the cockpit
            exception queue can read.  Always empty on the default (no-scheduler) path (INV: opt-in).
        unmatched: Sorted list of node ids the scheduler HELD because NO standing agent matched them
            (unmatched, distinct from below-bar escalation).  An unmatched node blocks the frontier
            forever, so surfacing it separately lets the cockpit explain a no_progress stall that
            ``escalated`` alone would not.  Always empty on the default (no-scheduler) path.
    """

    rounds: int
    terminated_by: str
    done: bool
    completed: List[str]
    frontier: List[str]
    outputs: Dict[str, dict]
    state: GovernorState
    trace: List[str]
    supervisor_runs: int
    backend: str
    escalated: List[str] = field(default_factory=list)
    unmatched: List[str] = field(default_factory=list)


def _default_supervisor_factory(
    *, plan, manifests, store, invoke_fn, arns, session_id, held=None
) -> Supervisor:
    """Default seam: a real :class:`Supervisor` bound to the governor's store (offline-friendly).

    ``held`` is the OPT-IN Trust-Ladder ROUTER's set of nodes escalated/unmatched THIS round (I-1) —
    empty/``None`` on the default path, so today's byte-for-byte behavior is preserved (no ``held``
    kwarg reaches the Supervisor when the set is empty). When a scheduler withholds a node, the
    held-set is handed to the Supervisor's OPT-IN ``held`` skip param: :meth:`Supervisor.run` skips
    that node like a resume skip — it is NEVER invoked and NOTHING is written to the log for it (INV-1;
    a pure non-dispatch, not a failure). The frozen ``plan.order`` is NEVER mutated (INV-3) — the node
    stays in the plan and in the still-open frontier for a later round once its trust is re-earned.
    The held node is surfaced on :attr:`GovernorResult.escalated`.

    Holding is NOT done by omitting the node's ARN: the Supervisor re-derives an ARN from the
    manifest (``registry.agent_runtime_arn``) when the supplied ``arns`` dict lacks one, so stripping
    ``arns`` would either be a no-op (the manifest-carried ARN dispatches the below-bar node anyway —
    a silent governance bypass) or, when there is no manifest ARN, trip the placeholder ARN-integrity
    gate and RAISE under the default ``on_error='raise'`` (crashing the whole episode). The explicit
    skip param is the only correct non-dispatch that both leaves the plan unmutated and never invokes.
    """
    held_set = set(held or ())
    kwargs: Dict[str, Any] = dict(
        invoke_fn=invoke_fn,
        arns=arns,
        state_store=store,
        session_id=session_id,
    )
    if held_set:
        kwargs["held"] = held_set
    return Supervisor(plan, manifests, **kwargs)


class GovernorLoop:
    """The fixed cyclic outer driver around the concursus compiler.

    Each round forms a fresh frozen plan at the compiler front (``planner``), runs one static
    Supervisor episode over it (``run_episode``), folds the outputs into the append-only log
    (``collect``), then decides — bounded — whether to replan or synthesize.  The frozen plan is
    NEVER mutated mid-episode; growth happens BETWEEN episodes via ``recompile`` (INV-3/INV-4).

    The append-only :class:`StateStore` log is the SOLE structural anchor of the executed prefix
    (INV-5).  The backend is chosen behind the Protocol seam: pass ``store=`` to inject any store
    verbatim; pass ``memory_id`` (+ ``actor_id``, and the runtime ``session_id`` as the durable
    ``sessionId``) to select the SHIPPED AgentCore-backed :class:`MemoryStateStore` so the log
    survives micro-VM teardown and G-4 dual-resume replays a DURABLE log; otherwise the offline
    :class:`InProcessStateStore` is used.  This is WIRING only — no new writer.
    """

    def __init__(
        self,
        goal: str,
        manifests: Dict[str, AgentManifest],
        *,
        store: Optional[StateStore] = None,
        checkpointer: Optional[CheckpointStore] = None,
        assembler: Optional[OrchestrationAssembler] = None,
        scheduler: Optional["TrustLadderScheduler"] = None,
        supervisor_factory: Optional[SupervisorFactory] = None,
        invoke_fn: Optional[InvokeFn] = None,
        arns: Optional[Dict[str, str]] = None,
        plan_model_fn: Optional[PlanModelFn] = None,
        deliberate: bool = False,
        trail_factory: Optional[Callable[[], Any]] = None,
        investigator: Optional[Callable[[Any], Any]] = None,
        deliberate_retriever: Optional[Any] = None,
        deliberate_max_rounds: Optional[int] = None,
        deliberate_depth_cap: Optional[int] = None,
        deliberate_confidence_floor: Optional[float] = None,
        session_id: Optional[str] = None,
        memory_id: Optional[str] = None,
        actor_id: Optional[str] = None,
        memory_client: Any = None,
        max_rounds: int = 8,
        no_progress_n: int = 2,
        max_revisions: int = DEFAULT_MAX_REVISIONS,
        confidence_threshold: float = 0.5,
        backend: str = "auto",
        run_id: str = "governor",
        checkpoint_every: int = 0,
    ) -> None:
        if backend not in ("auto", "python", "langgraph"):
            raise GovernorLoopError(
                f"backend must be 'auto' | 'python' | 'langgraph', got {backend!r}"
            )
        if not goal or not str(goal).strip():
            raise GovernorLoopError("GovernorLoop requires a non-empty goal")
        if max_rounds < 1:
            raise GovernorLoopError("max_rounds must be >= 1 (the loop must be bounded and progress)")
        if no_progress_n < 1:
            raise GovernorLoopError("no_progress_n must be >= 1")
        self._goal = goal
        self._manifests = dict(manifests)
        self._session_id = session_id
        self._store: StateStore = self._select_store(
            store,
            memory_id=memory_id,
            actor_id=actor_id,
            memory_client=memory_client,
        )
        self._checkpointer = checkpointer
        self._run_id = str(run_id) if run_id else "governor"
        self._assembler = assembler or OrchestrationAssembler()
        # -- OPT-IN Trust-Ladder ROUTER seam (I-1) --------------------------
        # When None (default), _router stays BYTE-FOR-BYTE today's pass-through and NO node is ever
        # held — all 357 existing tests are unchanged. When a scheduler IS provided, _router calls
        # its propose_frontier each round to partition the still-open frontier into cleared
        # (compile_next) vs HELD (escalated/unmatched) nodes; held nodes are kept OUT of the episode's
        # dispatch WITHOUT being removed from the frozen plan (INV-3) — the supervisor is handed the
        # held-set via its OPT-IN skip param so run() skips those nodes (a pure non-dispatch: never an
        # invoke, never a log write), rather than by stripping ARNs (which the manifest-ARN fallback
        # would silently defeat or the placeholder integrity gate would turn into a hard raise). The
        # plan.order is NEVER mutated (recompile may only ADD, never drop — dropping a planned node
        # would raise MonotonicityError), so holding is achieved by non-dispatch, not by shrinking the
        # plan.
        self._scheduler = scheduler
        self._supervisor_factory = supervisor_factory or _default_supervisor_factory
        self._invoke_fn = invoke_fn
        self._arns = arns
        self._plan_model_fn = plan_model_fn
        # -- OPT-IN pre-freeze deliberation seam (I-0) ----------------------
        # When False (default), first-round plan authoring is BYTE-FOR-BYTE today's single-shot
        # plan_from_goal path (all existing tests unchanged). When True, round-1 authors the DAG via
        # the bounded SEED -> deliberate/adjust -> converge(SIGNOFF) -> lower_to_dag path
        # (reasoning.deliberate.form_plan) that only emits a frozen AgentDAG AFTER the debate
        # converges, then hands it to assemble exactly as before. The adjustment loop is dynamic but
        # STRICTLY BEFORE assemble and TERMINATES in a frozen DAG (INV-1/INV-2/INV-3/INV-4). Later
        # rounds STILL use recompile, unchanged.
        self._deliberate = bool(deliberate)
        self._trail_factory = trail_factory
        self._investigator = investigator
        self._deliberate_retriever = deliberate_retriever
        self._deliberate_max_rounds = deliberate_max_rounds
        self._deliberate_depth_cap = deliberate_depth_cap
        self._deliberate_confidence_floor = deliberate_confidence_floor
        self._max_rounds = max_rounds
        self._no_progress_n = no_progress_n
        self._max_revisions = max_revisions
        self._confidence_threshold = confidence_threshold
        self._backend = backend
        # -- OPT-IN auto-checkpoint cadence (C-4 optimization) --------------
        # 0 (default) => today's behavior byte-for-byte: no auto-checkpoint, every warm resume is a
        # full log rebuild. When > 0 AND the store supports checkpoint-compaction (MemoryStateStore /
        # FileVaultStateStore expose ``checkpoint()``), COLLECT calls store.checkpoint() once every N
        # completed rounds so a long-running loop's append-only log stays bounded for warm resume
        # (O(events-since-last-checkpoint), not O(whole log)) WITHOUT the caller having to remember to
        # checkpoint. A checkpoint is a derived, append-only compaction of the same log (INV-5) — the
        # raw events are never deleted, so this is a pure resume-cost optimization, never a semantic
        # change. Stores without checkpoint() (the in-process default) silently no-op.
        if int(checkpoint_every) < 0:
            raise GovernorLoopError("checkpoint_every must be >= 0 (0 disables auto-checkpoint)")
        self._checkpoint_every = int(checkpoint_every)
        # -- READ-ONLY cockpit/scope seam (I-3) -----------------------------
        # The CURRENT run's final frozen plan VALUE, stashed by run() so a caller can build a
        # DirectorCockpit / read the scope projections over the LIVE run WITHOUT re-assembling or
        # dispatching anything. None until the first run() completes. This is a pointer to an
        # already-frozen plan value — never a mutable snapshot (INV-3).
        self._last_plan: Optional[ProvisioningPlan] = None
        # The CURRENT run's read-only governance sets, stashed by run() so a DirectorCockpit
        # can surface them in its exception queue WITHOUT re-running or mutating anything (INV-5).
        # Empty until the first run() completes; default-empty => today's failed-only cockpit queue.
        self._last_escalated: List[str] = []
        self._last_unmatched: List[str] = []

    # -- durable-store selection (C-2) --------------------------------------
    def _select_store(
        self,
        store: Optional[StateStore],
        *,
        memory_id: Optional[str],
        actor_id: Optional[str],
        memory_client: Any,
    ) -> StateStore:
        """Select the append-only :class:`StateStore` backend behind the Protocol seam.

        Precedence (no new writer — the governor just picks which SHIPPED store to hold):

        1. An explicitly-passed ``store`` wins verbatim — the caller may inject any
           :class:`StateStore` (an :class:`InProcessStateStore`, a pre-built
           :class:`MemoryStateStore`, or a fake), so the offline default and every existing test
           are untouched.
        2. Otherwise, when ``memory_id`` is supplied, build the SHIPPED AgentCore-backed
           :class:`MemoryStateStore` so the append-only log survives micro-VM teardown and G-4
           dual-resume works against a DURABLE log. ``sessionId`` is pinned to the runtime
           ``session_id`` (the outer run's identity) so a FRESH store over the same
           ``(memory_id, actor_id, session_id)`` replays the prior run's ``completed()`` / ``get()``
           — the log, not the loop, is the sole structural anchor of the executed prefix (INV-5).
           ``memory_client`` is passed straight through (a fake AgentCore client in tests, ``None``
           to let :class:`MemoryStateStore` construct the boto3 data-plane client lazily) — no AWS
           is touched here.
        3. Otherwise fall back to the zero-dependency offline :class:`InProcessStateStore`.

        This is WIRING only: it selects a store that already exists; it introduces no new persistence
        path and writes nothing at construction time.
        """
        if store is not None:
            return store
        if memory_id is not None:
            if not self._session_id:
                raise GovernorLoopError(
                    "a MemoryStateStore backend (memory_id set) requires a non-empty session_id "
                    "as the durable sessionId so a fresh store can resume the same run"
                )
            if not actor_id:
                raise GovernorLoopError(
                    "a MemoryStateStore backend (memory_id set) requires a non-empty actor_id"
                )
            return MemoryStateStore(
                memory_id=memory_id,
                session_id=self._session_id,
                actor_id=actor_id,
                client=memory_client,
            )
        return InProcessStateStore()

    # -- public entry -------------------------------------------------------
    def run(self, inputs: Optional[dict] = None) -> GovernorResult:
        """Drive the bounded outer cycle to termination and return a :class:`GovernorResult`.

        Tries the LangGraph backend when ``backend`` is ``"auto"`` (falling back to pure Python if
        langgraph is not importable) or ``"langgraph"`` (raising if it is missing); ``"python"``
        forces the fallback.  Either backend runs the SAME node functions and routing.
        """
        graph = None
        if self._backend in ("auto", "langgraph"):
            graph = self._build_langgraph()
            if graph is None and self._backend == "langgraph":
                raise GovernorLoopError(
                    "backend='langgraph' requested but langgraph is not installed; "
                    "install the optional extra or use backend='python'"
                )
        ctx = self._initial_ctx(inputs)
        # OUTER-altitude resume: if a checkpoint survives for this run id, rebuild the round
        # bookkeeping + re-fetch the frozen plan BY VERSION (never a stored snapshot) so the loop
        # picks up at the correct ROUND (INV-3/INV-5). The INNER altitude (log-replay skip of
        # already-committed nodes) is handled unchanged inside each Supervisor episode (INV-1).
        self._maybe_restore(ctx)
        if graph is not None:
            ctx = self._run_langgraph(graph, ctx)
            backend = "langgraph"
        else:
            ctx = self._drive_python(ctx)
            backend = "python"
        # Stash the run's final frozen plan VALUE for the read-only cockpit/scope accessors (I-3).
        # A pointer to an already-frozen value — never re-assembled, never mutated (INV-3).
        self._last_plan = ctx.get("plan")
        # Stash the run's read-only governance sets for the cockpit exception queue (INV-5). These are
        # the SAME values surfaced on GovernorResult below — read-only VALUES, never re-derived.
        self._last_escalated = sorted(ctx.get("escalated") or [])
        self._last_unmatched = sorted(ctx.get("unmatched") or [])
        return GovernorResult(
            rounds=int(ctx["round"]),
            terminated_by=str(ctx["terminated_by"]),
            done=bool(ctx["done"]),
            completed=list(ctx["completed"]),
            frontier=list(ctx["frontier"]),
            outputs=dict(ctx["outputs"]),
            state=ctx["state"],
            trace=list(ctx["trace"]),
            supervisor_runs=int(ctx["supervisor_runs"]),
            backend=backend,
            escalated=sorted(ctx.get("escalated") or []),
            unmatched=sorted(ctx.get("unmatched") or []),
        )

    # -- READ-ONLY cockpit / scope over the LIVE run (I-3) ------------------
    def cockpit(self, *, vault_path: Optional[str] = None) -> "DirectorCockpit":
        """Return a read-only :class:`DirectorCockpit` over this loop's CURRENT run.

        Builds a :class:`~concursus.execute.supervisor.Supervisor` bound to the loop's OWN append-only
        :class:`StateStore` and the run's final frozen plan VALUE, then hands it to the cockpit. This
        is a PURE read surface (INV-5): it constructs a Supervisor only so the cockpit can call its
        shipped read models (``summary()`` / ``summary_line()`` / ``index()``) — it NEVER calls
        :meth:`Supervisor.run`, never assembles or recompiles a plan, never dispatches a node, and
        never ``put``s to the store. The Supervisor reads the SAME store the loop wrote, so its
        ``summary().failed`` / completed set are the live run's, and rendering the cockpit leaves
        ``store.records()`` byte-identical.

        Call after :meth:`run` so the run's frozen plan is available; before the first run the plan is
        ``None`` and the cockpit's ``revision`` reads ``None`` (still a valid read-only surface over an
        empty log). ``vault_path`` is optional and, when given, lets the cockpit render the idempotent
        precedent hub — a select-nothing/seed-nothing projection.
        """
        supervisor = Supervisor(
            self._last_plan if self._last_plan is not None else _EMPTY_PLAN,
            self._manifests,
            invoke_fn=self._invoke_fn,
            arns=self._arns,
            state_store=self._store,
            session_id=self._session_id,
        )
        # Imported here (not at module top) so importing loop.py never pulls the cockpit projection.
        from concursus.governor.cockpit import DirectorCockpit

        return DirectorCockpit(
            supervisor=supervisor,
            vault_path=vault_path,
            plan=self._last_plan,
            escalated=self._last_escalated,
            unmatched=self._last_unmatched,
        )

    def programs_index(self, vault_path: str, *, sep: str = SCOPE_SEP) -> Dict[str, dict]:
        """Return the PROGRAM-grain projection over the live run's vault (read-only).

        A thin pass-through to :func:`~concursus.governor.scope.build_programs_index`: it rolls the
        per-run precedent notes under ``<vault>/precedents/`` up by ``program_key``. Pure read (INV-5)
        — selects nothing, seeds nothing, drives no dispatch, and is regenerated from the notes each
        call (same notes -> byte-identical output). ``vault_path`` is required because the offline
        default store holds no vault directory; a run that distilled its precedents into a vault passes
        that path here.
        """
        from concursus.governor.scope import build_programs_index

        return build_programs_index(vault_path, sep=sep)

    def leverage_view(self, vault_path: str, *, sep: str = SCOPE_SEP) -> Dict[str, object]:
        """Return the 1:N director-leverage view over the live run's vault (read-only).

        Pass-through to :func:`~concursus.governor.scope.director_leverage_view`: program count, total
        hosted runs, per-program run counts, and a cross-program status rollup. Selects nothing, seeds
        nothing, drives no dispatch (INV-5).
        """
        from concursus.governor.scope import director_leverage_view

        return director_leverage_view(vault_path, sep=sep)

    # -- initial context ----------------------------------------------------
    def _initial_ctx(self, inputs: Optional[dict]) -> dict:
        """The initial graph state: the persistent governor state (seeded lazily) + loop control."""
        return {
            "inputs": dict(inputs or {}),
            "state": None,        # GovernorState — seeded by the first planner round
            "dag": None,          # the authored DAG, reused across recompiles
            "plan": None,         # mirror of state.current_frozen_plan for the round
            "round": 0,           # completed episodes
            "frontier": [],
            "completed": [],
            "prev_completed": 0,  # completed-node count as of the prior round (progress bound)
            "no_progress": 0,     # consecutive stalled rounds
            "progressed": True,
            "replan_reason": None,  # why the NEXT round replans (failure|contradiction|low_confidence)
            "outputs": {},        # the LAST episode's outputs
            "supervisor_runs": 0,
            "done": False,
            "terminated_by": "",
            "trace": [],
            "held": set(),      # nodes the OPT-IN scheduler held THIS round (escalated/unmatched)
            "escalated": [],    # accumulated escalated (below-bar, held) governance surface, all rounds
            "unmatched": [],    # accumulated UNMATCHED (no standing agent, held) surface, all rounds
        }

    # ================================================= dual-altitude resume (OUTER checkpoint)
    def _maybe_restore(self, ctx: dict) -> None:
        """OUTER-altitude resume: rebuild round bookkeeping + re-fetch the plan BY VERSION.

        No-op when no :class:`CheckpointStore` is configured or no checkpoint survives for this
        ``run_id`` — the loop then starts fresh (default behavior unchanged). When a checkpoint DOES
        survive, we do NOT deserialize a stored plan snapshot (there is none). Instead we RE-FETCH
        the frozen plan by version: re-author the deterministic DAG, ``assemble`` revision 0, then
        replay ``recompile`` ``iteration`` times against the SURVIVING append-only log (the sole
        structural anchor; INV-5) so the reconstructed :class:`GovernorState` carries the checkpointed
        ``plan_version`` forward — never reset to 0 — and every already-executed node stays PINNED
        (INV-3/INV-4). The round/stall/progress counters are restored so the outer loop picks up at
        the correct ROUND; the INNER altitude (log-replay skip of committed nodes) is handled
        unchanged inside each Supervisor episode (INV-1), so committed nodes are never re-invoked.
        """
        if self._checkpointer is None:
            return
        ckpt = self._checkpointer.load(self._run_id)
        if not ckpt:
            return
        # Re-author the deterministic DAG (reused across recompiles) and re-freeze revision 0. The
        # same authoring seam as round-1, but with ``resume=True`` so the DAG re-derivation is
        # IDEMPOTENT: the single-shot path (deliberate=False) is stateless and reproduces round-1's
        # DAG byte-for-byte; the deliberation path (deliberate=True) is re-run against a FRESH, empty
        # HypothesisTrail — never the persistent ``trail_factory`` dir — because re-seeding a debate
        # into an already-populated trail bumps its monotonic seq counter, shifting node names (h1 ->
        # h4) and making the round-0 order DIFFER from the surviving log's committed nodes, which
        # ``recompile`` would then reject with MonotonicityError. Round-1 authored from an empty
        # trail, so a fresh empty trail on resume reproduces the same node names (INV-3/INV-4).
        dag = self._author_first_dag(resume=True)
        plan = self._assembler.assemble(dag, self._manifests)
        state = GovernorState(current_frozen_plan=plan, store=self._store)
        # Replay the compiler front to re-fetch the plan at the checkpointed version. Each recompile
        # PINS the surviving executed prefix (re-derived from the log this instant) and bumps the
        # revision by one, so after ``iteration`` steps state.plan_version == the checkpointed value.
        iteration = int(ckpt.get("iteration", 0))
        for _ in range(iteration):
            completed, content_hashes = self._executed_prefix_from_log()
            plan = self._assembler.recompile(
                state.current_frozen_plan,
                completed=completed,
                content_hashes=content_hashes,
                dag=dag,
                manifests=self._manifests,
                max_revisions=self._max_revisions,
            )
            state.advance(plan, reason=ckpt.get("replan_reason") or "resume", progressed=True)
        ctx["dag"] = dag
        ctx["state"] = state
        ctx["plan"] = state.current_frozen_plan
        # Restore the outer loop counters so we resume at the correct ROUND (not round 0).
        ctx["round"] = int(ckpt.get("round", 0))
        ctx["no_progress"] = int(ckpt.get("no_progress", 0))
        ctx["prev_completed"] = int(ckpt.get("prev_completed", len(self._store.completed())))
        ctx["replan_reason"] = ckpt.get("replan_reason")
        # Seed the PRIOR-round completed SET from the surviving log (not just its count). The OPT-IN
        # Trust-Ladder re-earn in _collect anchors "newly completed this round" on ctx["completed"];
        # left at its _initial_ctx default of [] the first post-resume collect would treat EVERY
        # surviving-prefix node as freshly completed and re-earn its GOV-side trust a second time
        # (a spurious promotion that could lift a below-bar side-effecting agent over its autonomy
        # bar after a crash). Anchoring on the surviving prefix re-earns only nodes that finish in
        # the resumed rounds — the "re-earn each node exactly ONCE, the round it finishes" contract.
        # Harmless on the default (no-scheduler) path: ctx["completed"] gates only the re-earn loop.
        ctx["completed"] = sorted(self._store.completed())

    def _save_checkpoint(self, ctx: dict) -> None:
        """Persist a plain-dict OUTER checkpoint: a version + log POINTER, never a plan snapshot.

        Stores ``plan_version`` + ``iteration`` (from the swapped-in :class:`GovernorState`) plus the
        round/stall/progress counters — everything needed to re-fetch the plan by version and resume
        at the correct round. It deliberately holds NO :class:`ProvisioningPlan` object: the plan is
        re-derived from the surviving append-only log on restart (INV-3/INV-5). No-op when no
        checkpointer is configured.
        """
        if self._checkpointer is None:
            return
        state: Optional[GovernorState] = ctx.get("state")
        checkpoint = {
            "plan_version": int(state.plan_version) if state is not None else 0,
            "iteration": int(state.iteration) if state is not None else 0,
            "round": int(ctx.get("round", 0)),
            "no_progress": int(ctx.get("no_progress", 0)),
            "prev_completed": int(ctx.get("prev_completed", 0)),
            "replan_reason": ctx.get("replan_reason"),
        }
        self._checkpointer.save(self._run_id, checkpoint)

    # ================================================= executed prefix (log-derived)
    def _executed_prefix_from_log(self) -> "tuple[set[str], Dict[str, str]]":
        """Re-derive the executed prefix FROM THE APPEND-ONLY LOG this round (never cached; INV-5).

        Returns ``(completed, content_hashes)`` where ``completed`` is ``store.completed()`` (the
        nodes whose LATEST record validated) and ``content_hashes`` is the ``{node: content_hash}``
        provenance snapshot for exactly those nodes, taken from the log's ``Record`` s. Both are
        recomputed from ``store.completed()`` / ``store.records()`` on every call — the governor
        holds NO mutable copy of the prefix, so the append-only log stays the sole structural anchor
        of what has executed (INV-5). The content hashes are read-only provenance handed to
        ``recompile``; they do not relax its monotonic guard.
        """
        completed = set(self._store.completed())
        content_hashes: Dict[str, str] = {}
        for record in self._store.records():
            if record.node not in completed:
                continue
            if record.status != "validated":
                continue
            chash = record.content_hash
            if chash is None:
                chash = content_hash(record.output)
            # Records are in append order; the latest validated write per node wins (mirrors the
            # store's own latest-validated projection that backs completed()).
            content_hashes[record.node] = chash
        return completed, content_hashes

    # ================================================= first-round DAG authoring (I-0)
    def _author_first_dag(self, *, resume: bool = False):
        """Author the round-1 DAG at the compiler FRONT — single-shot by default, deliberated opt-in.

        Default (``deliberate=False``): byte-for-byte today's path — a single
        :func:`plan_from_goal` call that emits the DAG once, no pre-signoff adjustment. Stateless, so
        it is trivially idempotent under ``resume``.

        Opt-in (``deliberate=True``): the DYNAMIC pre-freeze path. Build a :class:`HypothesisTrail`
        (via the injected ``trail_factory`` or a fresh temp run dir) and run
        :func:`~concursus.reasoning.deliberate.form_plan`, whose bounded SEED -> read-frontier ->
        dispatch -> digest -> verdict -> re-read loop ADJUSTS the plan and only lowers to an
        immutable :class:`AgentDAG` AFTER the debate CONVERGES (SIGNOFF) — ``form_plan`` lowers via
        ``lower_to_dag`` which RAISES :class:`ThreadNotResolved` on an open frontier, so a DAG is
        returned only from a converged debate. The loop is dynamic but STRICTLY BEFORE ``assemble``
        and TERMINATES in a frozen DAG (bounded by ``max_rounds``/``depth_cap``); it never touches
        :meth:`Supervisor.run` (INV-1), adds no compiler while-loop (INV-2 — the cycle is the bounded
        DKS deliberation), and the emitted DAG is frozen by ``assemble`` exactly as before
        (INV-3/INV-4). The injected seams default to deterministic stubs, so it runs with NEITHER
        langgraph NOR any LLM.

        ``resume=True`` (called only from :meth:`_maybe_restore`) forces IDEMPOTENT re-authoring: the
        deliberation is re-run against a FRESH, empty :class:`HypothesisTrail` (a throwaway temp dir),
        NEVER the caller's ``trail_factory``. Re-seeding a debate into an already-populated persistent
        trail advances its monotonic seq counter, so node ids (``__h1`` -> ``__h4``) — and thus the
        round-0 plan order — would diverge from the surviving log's committed nodes and ``recompile``
        would raise :class:`MonotonicityError`. Round-1 authored from an empty trail; a fresh empty
        trail on resume reproduces the identical node names, so the executed prefix stays pinned.
        """
        if not self._deliberate:
            return plan_from_goal(self._goal, plan_model_fn=self._plan_model_fn)
        # Imported here (not at module top) so the default single-shot path never imports the
        # reasoning tier — the deliberation is a strictly-opt-in front-end.
        from concursus.reasoning.deliberate import form_plan
        from concursus.reasoning.trailstore import HypothesisTrail

        if self._trail_factory is not None and not resume:
            trail = self._trail_factory()
        else:
            # No factory, OR a resume: author from a FRESH empty trail so the node ids reproduce
            # round-1's exactly (a persistent trail_factory would otherwise bump its seq counter and
            # break monotonicity on resume).
            import tempfile

            run_dir = tempfile.mkdtemp(prefix="concursus_deliberate_")
            trail = HypothesisTrail(run_dir)
        kwargs: Dict[str, Any] = {}
        if self._deliberate_retriever is not None:
            kwargs["retriever"] = self._deliberate_retriever
        if self._investigator is not None:
            kwargs["investigator"] = self._investigator
        if self._deliberate_max_rounds is not None:
            kwargs["max_rounds"] = self._deliberate_max_rounds
        if self._deliberate_depth_cap is not None:
            kwargs["depth_cap"] = self._deliberate_depth_cap
        if self._deliberate_confidence_floor is not None:
            kwargs["confidence_floor"] = self._deliberate_confidence_floor
        return self._reconcile_dag_with_manifests(form_plan(trail, self._goal, **kwargs))

    def _reconcile_dag_with_manifests(self, dag):
        """Ensure the authored DAG carries the manifests' ground-truth agent topology (INV-safe).

        The deliberation tier (``form_plan``) decides the *approach* and, with the default no-LLM
        stub, converges to a GOAL-shaped DAG (one advisory ``approach_*`` node) that does NOT name
        the registered agents or their ``depends_on`` edges. The manifests are the ground truth:
        :meth:`~concursus.assemble.OrchestrationAssembler.assemble` type-gates every manifest edge
        via :func:`~concursus.core.resolve.check_alignment`, which REQUIRES the DAG to carry each
        declared ``producer -> consumer`` edge — so an un-reconciled deliberated DAG raises
        ``AlignmentError`` the moment any manifest declares a dependency.

        This fold makes the deliberated plan assemblable WITHOUT weakening the freeze contract, and
        WITHOUT leaving un-provisionable nodes: :meth:`~concursus.assemble.OrchestrationAssembler.assemble`
        also requires EVERY DAG node to have a manifest (``AssemblyError`` otherwise), so a
        deliberation-only advisory node (e.g. the default stub's ``approach_*`` root) cannot remain.
        The manifests are authoritative: this rebuilds the DAG as EXACTLY the manifest topology —
        every manifest node + its ``depends_on`` edges — and DROPS any deliberated node that maps to
        no manifest (it was advisory approach context, never an executable agent). When the
        deliberated DAG already decomposed into the real agents (an LLM investigator), the manifest
        nodes/edges are identical, so this reproduces that same topology.

        Rationale: the deliberation decides the APPROACH (and, once its outputs are captured as
        precedent, informs later planning), while the manifest set is the ground-truth agent
        topology the compiler provisions. This runs at the compiler FRONT, strictly before
        ``assemble`` (INV-2), returns a frozen ``AgentDAG`` (INV-3/INV-4), and never touches
        ``Supervisor.run`` (INV-1). With no manifests it returns the deliberated DAG unchanged.
        """
        if not self._manifests:
            return dag
        reconciled = AgentDAG()
        for node in self._manifests:
            reconciled.add_node(node)
        for node, manifest in self._manifests.items():
            for edge in getattr(manifest, "depends_on", []) or []:
                producer = str(edge.get("from", "")).partition(".")[0]
                if producer and producer in self._manifests:
                    reconciled.add_edge(producer, node)
        return reconciled.validate()

    # ================================================= node functions
    def _planner(self, ctx: dict) -> dict:
        """PLANNER: emit a NEW frozen plan at the compiler front (assemble first, recompile after).

        First round: author a DAG via :func:`plan_from_goal` and freeze it with ``assemble`` into a
        brand-new :class:`GovernorState`.  Later rounds: form the next frozen plan via ``recompile``
        (a fresh, monotonic, revision-bumped VALUE that PINS the executed prefix) and swap it in with
        :meth:`GovernorState.advance` — which never edits the prior plan object (INV-3/INV-4).
        """
        state: Optional[GovernorState] = ctx["state"]
        if state is None:
            # First round: author + freeze at the compiler front (no LLM needed by default).
            # The DAG is authored single-shot by default, or via the bounded pre-freeze deliberation
            # when deliberate=True — both return a frozen AgentDAG ready for assemble (INV-3/INV-4).
            dag = self._author_first_dag()
            plan = self._assembler.assemble(dag, self._manifests)
            state = GovernorState(current_frozen_plan=plan, store=self._store)
            ctx["dag"] = dag
            ctx["state"] = state
        else:
            # Later rounds (RE-ENTRY): re-declare the executed prefix FROM THE LOG and recompile.
            #
            # The prefix is ALWAYS re-derived from the append-only StateStore log this round — the
            # completed node-set AND its content-hash provenance — never read from a mutable cache
            # in governor state (INV-5: the log is the SOLE structural anchor of the executed
            # prefix). ``recompile`` then PINS every completed node to its prior entry/wiring and
            # confines growth to the still-open frontier, so the executed slice replays verbatim.
            prior = state.current_frozen_plan
            completed, content_hashes = self._executed_prefix_from_log()
            plan = self._assembler.recompile(
                prior,
                completed=completed,
                content_hashes=content_hashes,
                dag=ctx["dag"],
                manifests=self._manifests,
                max_revisions=self._max_revisions,
            )
            state.advance(
                plan,
                reason=ctx.get("replan_reason") or "replan",
                progressed=bool(ctx["progressed"]),
            )
        ctx["plan"] = state.current_frozen_plan
        ctx["trace"].append("planner")
        return ctx

    def _router(self, ctx: dict) -> dict:
        """ROUTER: OPT-IN Trust-Ladder frontier gate; a pure pass-through when no scheduler is set.

        Default path (``scheduler is None``): BYTE-FOR-BYTE today's pass-through — it selects
        nothing, holds nothing, and mutates no plan, so all existing tests are unchanged.

        Opt-in path (a :class:`~concursus.governor.scheduler.TrustLadderScheduler` was injected): call
        ``propose_frontier(ctx["plan"], completed=store.completed())`` to partition the still-open
        frontier into ``compile_next`` (cleared to dispatch this round) vs ``escalated``/``unmatched``
        (HELD). The :class:`FrontierProposal` is stashed on ``ctx["proposal"]`` and the held-set on
        ``ctx["held"]`` so :meth:`_run_episode` hands the held-set to the supervisor's OPT-IN skip
        param — the node stays in the frozen ``plan.order`` (INV-3: never dropped, so ``recompile``
        never raises MonotonicityError), it is simply skipped (never invoked) this round. Held/escalated nodes are
        accumulated onto ``ctx["escalated"]`` (a governance surface the cockpit exception queue reads,
        exposed on :attr:`GovernorResult.escalated`); this NEVER dispatches them. As the escalated
        agents' trust is re-earned by ``update_trust`` between rounds, a later round's proposal clears
        them and they dispatch — INV-safe INCREMENTAL GROWTH, never a plan mutation.
        """
        if self._scheduler is not None and ctx.get("plan") is not None:
            proposal = self._scheduler.propose_frontier(
                ctx["plan"], completed=self._store.completed()
            )
            held = set(proposal.escalated) | set(proposal.unmatched)
            ctx["proposal"] = proposal
            ctx["held"] = held
            # Accumulate the escalated (held) governance surface across rounds, de-duplicated and
            # sorted — the cockpit exception queue reads this off GovernorResult.escalated.
            seen = set(ctx.get("escalated") or [])
            seen.update(proposal.escalated)
            ctx["escalated"] = sorted(seen)
            # Also accumulate UNMATCHED held nodes (no standing agent) on a SEPARATE surface. An
            # unmatched node is withheld forever and can stall the loop to no_progress; surfacing it
            # distinctly lets the cockpit exception queue explain a stall that ``escalated`` alone
            # (below-bar only) would leave invisible.
            seen_unmatched = set(ctx.get("unmatched") or [])
            seen_unmatched.update(proposal.unmatched)
            ctx["unmatched"] = sorted(seen_unmatched)
        ctx["trace"].append("router")
        return ctx

    def _run_episode(self, ctx: dict) -> dict:
        """RUN_EPISODE: run one static Supervisor pass over the current frozen plan (INV-1).

        Builds an episode supervisor over the CURRENT frozen plan VALUE and calls ``run`` ONCE to
        completion.  The supervisor stays a single static forward pass — the governor never reaches
        inside it, adds no back-edge, and mutates no plan.  A fresh episode is replayable in
        isolation (INV-4); its returned outputs are folded into the cross-episode log by COLLECT.
        """
        factory_kwargs = dict(
            plan=ctx["plan"],
            manifests=self._manifests,
            store=self._store,
            invoke_fn=self._invoke_fn,
            arns=self._arns,
            session_id=self._session_id,
        )
        # OPT-IN only: hand the ROUTER's held-set to the factory so held (escalated/unmatched) nodes
        # are built without a live binding and are NOT dispatched this round (the plan is unmutated;
        # INV-3). When no scheduler is configured the factory is called EXACTLY as today — no ``held``
        # kwarg — so existing supervisor factories (which take no ``held``) are byte-for-byte
        # unaffected.
        if self._scheduler is not None:
            factory_kwargs["held"] = set(ctx.get("held") or ())
        supervisor = self._supervisor_factory(**factory_kwargs)
        outputs = supervisor.run(ctx["inputs"])
        ctx["outputs"] = dict(outputs or {})
        ctx["round"] = int(ctx["round"]) + 1
        ctx["supervisor_runs"] = int(ctx["supervisor_runs"]) + 1
        ctx["trace"].append("run_episode")
        return ctx

    def _collect(self, ctx: dict) -> dict:
        """COLLECT: fold outputs into the append-only log; re-derive the frontier (never mutate the plan).

        Appends the episode's outputs to the append-only :class:`StateStore` log via ``put`` — never
        a mutated plan structure (INV-5).  The executed prefix is then RE-DERIVED from
        ``store.completed()`` (never cached mutably), and forward progress is measured against the
        prior round's completed-count for the stall bound.  The frozen plan object is left untouched.

        The store is the SINGLE persistence point.  A store-bound Supervisor (the default seam) has
        ALREADY persisted every validated node during ``run`` — re-putting those here would grow the
        append-only log with redundant dedup records O(rounds x nodes).  So COLLECT only persists a
        node whose output is NOT yet in ``store.completed()`` (i.e. a decoupled supervisor that did
        not write through to the shared store); already-completed nodes are left as the log has them.

        OPT-IN Trust-Ladder re-earn (I-2): when a scheduler is wired, every node that FIRST completed
        THIS round re-earns GOV-side trust via :meth:`TrustLadderScheduler.update_trust`, keyed on the
        episode outcome for that node. This is the ONLY place earned trust moves across episodes — it
        lives GOV-side, in this collect node, NEVER in the compiler, and NEVER calls the create-time
        :func:`~concursus.build.trust.evaluate_deploy_gate` per-invocation (INV-5). "Newly completed
        this round" is ``completed`` minus the PRIOR round's completed set (carried on
        ``ctx["completed"]``, seeded ``[]`` for round 1) — NOT minus ``store.completed()`` sampled at
        the top of this method, because a store-bound Supervisor has already written every node THROUGH
        during ``run``, so that sample would already include this round's completions and re-earn
        nothing. Anchoring on the prior round's set re-earns each node exactly ONCE, the round it
        finishes, whether it was written through by the supervisor or folded in by ``put`` below. When
        no scheduler is set, ``update_trust`` is never touched and collect is byte-for-byte unchanged.
        """
        prev_completed_set = set(ctx.get("completed") or [])
        already = self._store.completed()
        for node, out in ctx["outputs"].items():
            if isinstance(out, dict) and node not in already:
                self._store.put(node, out)
        completed = self._store.completed()
        # -- OPT-IN Trust-Ladder re-earn (I-2) ------------------------------
        # For each node that FIRST completed this round (in the new completed set but NOT in the prior
        # round's completed set), move its earned trust from the episode outcome. GOV-side ONLY
        # (INV-5): update_trust never calls the compiler / evaluate_deploy_gate per-invocation.
        if self._scheduler is not None:
            # Map each plan NODE (task label) to the AGENT NAME that served it. The scheduler keys
            # its earned-trust ladder by AGENT NAME (decide/earned_grade read self._earned[agent]),
            # NOT by the task label — a registered agent may serve a capability label that differs
            # from its own name. Re-earning under the raw node id would write a junk key the next
            # round's decide() never reads, so the earned grade would never move: an escalated
            # below-bar agent could never clear the bar and a failing agent could never demote. We
            # resolve node->agent from this round's FrontierProposal decisions (which carry both
            # .node and .agent), falling back to a fresh decide()/match and finally to the node id.
            node_to_agent = self._agent_name_for_node(ctx)
            for node in completed:
                if node in prev_completed_set:
                    continue  # completed in an earlier round — do not re-earn every round
                outcome = ctx["outputs"].get(node)
                if not isinstance(outcome, dict):
                    # Non-dict / absent episode output: read the node's log record as the outcome.
                    outcome = self._store.get(node)
                self._scheduler.update_trust(node_to_agent(node), outcome)
        order = list(ctx["plan"].order)
        ctx["completed"] = sorted(completed)
        ctx["frontier"] = [n for n in order if n not in completed]
        progressed = len(completed) > int(ctx["prev_completed"])
        ctx["progressed"] = progressed
        ctx["no_progress"] = 0 if progressed else int(ctx["no_progress"]) + 1
        ctx["prev_completed"] = len(completed)
        ctx["done"] = len(ctx["frontier"]) == 0
        # Fold the episode's replan SIGNAL out of the outputs + log (never a mutated plan; INV-5).
        # A failure / contradiction / low-confidence reading tells route_after_collect to loop back
        # to the PLANNER, which recompiles a fresh frozen plan re-planning the affected frontier.
        ctx["replan_reason"] = self._detect_replan_reason(ctx, completed)
        # OUTER-altitude checkpoint: persist the round's version + log pointer (never a plan
        # snapshot) so a crashed governor resumes at THIS round boundary (INV-3/INV-5). No-op absent
        # a checkpointer.
        self._save_checkpoint(ctx)
        # STORE-altitude auto-checkpoint (C-4 cadence): every ``checkpoint_every`` completed rounds,
        # compact the append-only StateStore log so a long-running loop's warm resume stays bounded
        # (O(events-since-checkpoint)). A no-op unless opt-in AND the store supports compaction.
        self._maybe_auto_checkpoint(int(ctx.get("round", 0)))
        ctx["trace"].append("collect")
        return ctx

    def _maybe_auto_checkpoint(self, round_no: int) -> None:
        """Compact the store's log every ``checkpoint_every`` rounds (C-4), when opt-in + supported.

        Disabled by default (``checkpoint_every == 0``) so behavior is byte-for-byte unchanged.
        When enabled, fires at each Nth completed round on a store that exposes ``checkpoint()``
        (MemoryStateStore / FileVaultStateStore); the in-process default store has no such method
        and is silently skipped. A checkpoint is a derived, append-only compaction of the same log
        (INV-5) — never a mutation or deletion — so this only bounds warm-resume cost, never changes
        the run's semantics. Any checkpoint error is swallowed: a failed compaction must never break
        a live episode (the loop degrades to a full-log warm resume, which is always correct).
        """
        if self._checkpoint_every <= 0 or round_no <= 0:
            return
        if round_no % self._checkpoint_every != 0:
            return
        cp = getattr(self._store, "checkpoint", None)
        if not callable(cp):
            return
        try:
            cp()
        except Exception:  # pragma: no cover - defensive: compaction is best-effort, never fatal
            pass

    def _agent_name_for_node(self, ctx: dict) -> Callable[[str], str]:
        """Build a node->agent-name resolver so trust re-earns land under the ladder's real key.

        The scheduler keys earned trust by the matched AGENT NAME (``decide``/``earned_grade`` read
        ``self._earned[agent]``); the plan carries task-label NODE ids, which may differ from the
        serving agent's name. This resolves each node to its agent using, in order: (1) this round's
        :class:`FrontierProposal` decisions stashed on ``ctx["proposal"]`` (each
        :class:`ScheduleDecision` carries both ``.node`` and ``.agent``), (2) a fresh
        ``scheduler.decide(node)`` when the proposal lacks the node, and (3) the node id itself as a
        last-resort fallback (preserving today's behavior when node==agent-name). Read-only: it
        selects nothing and mutates no plan.
        """
        by_node: Dict[str, str] = {}
        proposal = ctx.get("proposal")
        for decision in getattr(proposal, "decisions", ()) or ():
            agent = getattr(decision, "agent", None)
            if agent:
                by_node[decision.node] = agent

        def resolve(node: str) -> str:
            agent = by_node.get(node)
            if agent:
                return agent
            try:
                decision = self._scheduler.decide(node)
            except Exception:
                return node
            return getattr(decision, "agent", None) or node

        return resolve

    # ================================================= replan-reason signal
    def _detect_replan_reason(self, ctx: dict, completed: "set[str]") -> Optional[str]:
        """Read the episode's replan SIGNAL from its outputs + the append-only log (read-only).

        The governor replans BETWEEN episodes when the just-run episode surfaces one of three
        signals, in priority order:

        * ``"failure"`` — a node reported ``ok=False`` / carried an ``error``, or the store holds a
          ``status="failed"`` record for a node that is NOT in ``completed`` (a genuine terminal
          failure or a ``blocked_on`` skip, mirroring :meth:`Supervisor.summary`'s ``failed`` map).
        * ``"contradiction"`` — two or more nodes disagree: they emit distinct non-null values under
          the same verdict key (``verdict`` / ``decision`` / ``label``).
        * ``"low_confidence"`` — a node's numeric ``confidence`` is below
          :attr:`_confidence_threshold`.

        Returns the reason string, or ``None`` when the episode looks clean. This SELECTS nothing and
        SEEDS nothing: it only reads ``ctx["outputs"]`` and ``self._store`` and returns a label the
        routing edge acts on. The frozen plan is never touched here.
        """
        outputs = ctx.get("outputs") or {}

        # (1) failure — from the outputs' own ok/error flags OR the log's failed records.
        for out in outputs.values():
            if isinstance(out, dict) and (out.get("ok") is False or out.get("error")):
                return "failure"
        for record in self._store.records():
            if record.status == "failed" and record.node not in completed:
                return "failure"

        # (2) contradiction — distinct non-null verdicts across nodes disagree.
        for key in ("verdict", "decision", "label"):
            seen = {
                json.dumps(out[key], sort_keys=True, default=str)
                for out in outputs.values()
                if isinstance(out, dict) and out.get(key) is not None
            }
            if len(seen) > 1:
                return "contradiction"

        # (3) low confidence — any node below the threshold.
        for out in outputs.values():
            if isinstance(out, dict) and isinstance(out.get("confidence"), (int, float)):
                if float(out["confidence"]) < self._confidence_threshold:
                    return "low_confidence"

        return None

    def _route_after_collect(self, ctx: dict) -> str:
        """The bounded routing edge: replan on a signal, else synthesize on a termination bound.

        A replan SIGNAL from :meth:`_collect` (``failure`` / ``contradiction`` / ``low_confidence``)
        routes back to PLANNER, which calls the EXISTING ``recompile`` to form the next monotonic,
        revision-bumped frozen plan — re-planning the affected frontier while PINNING the executed
        prefix.  The signal OVERRIDES frontier-exhaustion (a contradiction/low-confidence episode can
        have every node "completed" yet still warrant a replan), but the HARD bounds still terminate
        the loop unconditionally: the ``max_rounds`` round budget and the ``no_progress_n`` stall
        bound are checked FIRST so a persistently-failing signal can NEVER run away.

        With no replan signal the routing is exactly as before: SYNTHESIZE on frontier-exhaustion,
        the stall bound, or the round budget — whichever trips first — else loop back to PLANNER.
        The topology never changes; only ``state`` decides.
        """
        if ctx["replan_reason"]:
            # A replan signal loops back to the compiler front — but never past the hard bounds.
            if int(ctx["round"]) >= self._max_rounds:
                ctx["terminated_by"] = "round_cap"
                return "synthesize"
            if int(ctx["no_progress"]) >= self._no_progress_n:
                ctx["terminated_by"] = self._no_progress_label(ctx)
                return "synthesize"
            return "planner"
        if ctx["done"]:
            ctx["terminated_by"] = "frontier_exhaust"
            return "synthesize"
        if int(ctx["no_progress"]) >= self._no_progress_n:
            ctx["terminated_by"] = self._no_progress_label(ctx)
            return "synthesize"
        if int(ctx["round"]) >= self._max_rounds:
            ctx["terminated_by"] = "round_cap"
            return "synthesize"
        return "planner"

    def _no_progress_label(self, ctx: dict) -> str:
        """Distinguish a mis-registration stall from a generic stall (J-3, opt-in-safe).

        A no_progress stall is normally opaque: the frontier simply stopped advancing.  When the
        OPT-IN scheduler HELD a node as UNMATCHED (no standing agent) and that node blocked the
        frontier so hard that it NEVER advanced at all (nothing ever completed), the stall is caused
        by a mis-registered agent, not a genuinely un-plannable frontier.  Label that specific case
        ``unmatched_stall`` so the cockpit can explain it, while every other stall keeps the generic
        ``no_progress`` label unchanged.

        The guard is deliberately narrow so it can never re-label an existing path: it fires ONLY
        when ``ctx["unmatched"]`` is non-empty AND no node ever completed (the frontier never
        advanced).  A run where some nodes completed before stalling on an unmatched node — or any
        run with no scheduler, where ``unmatched`` is always empty — stays ``no_progress``.
        """
        never_advanced = len(ctx.get("completed") or []) == 0
        if ctx.get("unmatched") and never_advanced:
            return "unmatched_stall"
        return "no_progress"

    def _synthesize(self, ctx: dict) -> dict:
        """SYNTHESIZE: finalize the terminal summary from the log + plan-value sequence (read-only)."""
        ctx["completed"] = sorted(self._store.completed())
        ctx["trace"].append("synthesize")
        return ctx

    # -- node ordering (shared by both backends) ----------------------------
    def _node_fns(self) -> Dict[str, Callable[[dict], dict]]:
        return {
            "planner": self._planner,
            "router": self._router,
            "run_episode": self._run_episode,
            "collect": self._collect,
            "synthesize": self._synthesize,
        }

    # ================================================= pure-Python fallback
    def _step_cap(self) -> int:
        """A hard structural bound: ``max_rounds`` cycles of the fixed chain, plus generous slack.

        This is a safety analogue of ``DKSEngine``'s step cap — the loop can NEVER run away even if a
        node/route misbehaves, because the round budget + stall + frontier bounds already terminate
        it well before this cap.  Reaching the cap is a bug guard, reported as ``terminated_by``.
        """
        chain = len(GOV_NODES) + 1  # +1 for the routing hop back to planner
        return self._max_rounds * chain + chain + 8

    def _drive_python(self, ctx: dict) -> dict:
        """The bounded pure-Python driver — a while-loop over the node functions with the SAME routing.

        Runs when langgraph is absent (or ``backend='python'``).  Walks the fixed linear chain
        ``planner -> router -> run_episode -> collect``; after ``collect`` the routing edge decides
        ``planner`` (replan) or ``synthesize`` (terminate).  The step cap is a hard structural bound
        so the loop can never run away.
        """
        fns = self._node_fns()
        chain = list(GOV_NODES)
        step_cap = self._step_cap()
        node = "planner"
        steps = 0
        while node != _END and steps < step_cap:
            ctx = fns[node](ctx)
            if node == "collect":
                node = self._route_after_collect(ctx)
            elif node == "synthesize":
                node = _END
            else:
                node = chain[chain.index(node) + 1]
            steps += 1
        if steps >= step_cap and node != _END:
            # Hard guard tripped before a natural bound — finalize deterministically.
            ctx["terminated_by"] = "step_cap"
            ctx = self._synthesize(ctx)
        return ctx

    # ================================================= optional LangGraph
    def _build_langgraph(self):
        """Lazily build a LangGraph ``StateGraph`` mirroring the fallback, or ``None`` if unavailable.

        LangGraph is imported INSIDE this method so importing concursus never requires it.  Any
        import/build error returns ``None`` so :meth:`run` transparently falls back to pure Python.
        """
        try:  # pragma: no cover - exercised only when langgraph is installed
            from langgraph.graph import StateGraph, END
        except Exception:
            return None
        try:  # pragma: no cover - exercised only when langgraph is installed
            fns = self._node_fns()
            builder = StateGraph(dict)
            for name, fn in fns.items():
                builder.add_node(name, fn)
            builder.set_entry_point("planner")
            builder.add_edge("planner", "router")
            builder.add_edge("router", "run_episode")
            builder.add_edge("run_episode", "collect")
            builder.add_conditional_edges(
                "collect",
                lambda ctx: self._route_after_collect(ctx),
                {"planner": "planner", "synthesize": "synthesize"},
            )
            builder.add_edge("synthesize", END)
            return builder.compile()
        except Exception:
            return None

    def _run_langgraph(self, graph, ctx: dict) -> dict:  # pragma: no cover - needs langgraph
        """Invoke the compiled LangGraph with a recursion limit matching the round budget."""
        recursion_limit = self._step_cap()
        try:
            return graph.invoke(ctx, config={"recursion_limit": recursion_limit})
        except Exception:
            # A langgraph runtime failure must never break the outer loop — fall back.
            return self._drive_python(self._initial_ctx(ctx.get("inputs")))
