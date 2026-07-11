"""The governor's fixed cyclic control loop — the OUTER driver around the compiler.

concursus proper is a COMPILER: :meth:`OrchestrationAssembler.assemble` /
:meth:`~concursus.assemble.OrchestrationAssembler.recompile` turn a DAG + manifests into a
frozen :class:`~concursus.assemble.ProvisioningPlan` VALUE, and
:meth:`~concursus.execute.supervisor.Supervisor.run` executes that plan in a SINGLE static
forward pass.  :class:`GovernorLoop` is a NEW, strictly-outer layer that runs a bounded cycle
*around* the compiler.  It never reaches inside a running Supervisor, never mutates a frozen
plan, and never turns the compiler into a runtime governor.

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
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Protocol

from concursus.assemble.assemble import (
    DEFAULT_MAX_REVISIONS,
    OrchestrationAssembler,
    ProvisioningPlan,
)
from concursus.assemble.planner import PlanModelFn, plan_from_goal
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
            ``round_cap`` | ``step_cap``.
        done: Whether the plan's frontier was exhausted (all nodes completed).
        completed: Sorted list of completed node ids (re-derived from the log).
        frontier: The still-open frontier at termination.
        outputs: The LAST episode's returned outputs.
        state: The persistent :class:`GovernorState` (holds the full plan-value sequence).
        trace: The ordered node-visit trace.
        supervisor_runs: How many times a Supervisor was run (one per round; INV-1).
        backend: ``"langgraph"`` or ``"python"``.
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


def _default_supervisor_factory(
    *, plan, manifests, store, invoke_fn, arns, session_id
) -> Supervisor:
    """Default seam: a real :class:`Supervisor` bound to the governor's store (offline-friendly)."""
    return Supervisor(
        plan,
        manifests,
        invoke_fn=invoke_fn,
        arns=arns,
        state_store=store,
        session_id=session_id,
    )


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
        supervisor_factory: Optional[SupervisorFactory] = None,
        invoke_fn: Optional[InvokeFn] = None,
        arns: Optional[Dict[str, str]] = None,
        plan_model_fn: Optional[PlanModelFn] = None,
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
        self._supervisor_factory = supervisor_factory or _default_supervisor_factory
        self._invoke_fn = invoke_fn
        self._arns = arns
        self._plan_model_fn = plan_model_fn
        self._max_rounds = max_rounds
        self._no_progress_n = no_progress_n
        self._max_revisions = max_revisions
        self._confidence_threshold = confidence_threshold
        self._backend = backend

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
        )

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
        # Re-author the deterministic DAG (reused across recompiles) and re-freeze revision 0.
        dag = plan_from_goal(self._goal, plan_model_fn=self._plan_model_fn)
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
            dag = plan_from_goal(self._goal, plan_model_fn=self._plan_model_fn)
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
        """ROUTER: a pass-through for now (G-6 fills it) — it selects nothing and mutates no plan."""
        ctx["trace"].append("router")
        return ctx

    def _run_episode(self, ctx: dict) -> dict:
        """RUN_EPISODE: run one static Supervisor pass over the current frozen plan (INV-1).

        Builds an episode supervisor over the CURRENT frozen plan VALUE and calls ``run`` ONCE to
        completion.  The supervisor stays a single static forward pass — the governor never reaches
        inside it, adds no back-edge, and mutates no plan.  A fresh episode is replayable in
        isolation (INV-4); its returned outputs are folded into the cross-episode log by COLLECT.
        """
        supervisor = self._supervisor_factory(
            plan=ctx["plan"],
            manifests=self._manifests,
            store=self._store,
            invoke_fn=self._invoke_fn,
            arns=self._arns,
            session_id=self._session_id,
        )
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
        """
        already = self._store.completed()
        for node, out in ctx["outputs"].items():
            if isinstance(out, dict) and node not in already:
                self._store.put(node, out)
        completed = self._store.completed()
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
        ctx["trace"].append("collect")
        return ctx

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
                ctx["terminated_by"] = "no_progress"
                return "synthesize"
            return "planner"
        if ctx["done"]:
            ctx["terminated_by"] = "frontier_exhaust"
            return "synthesize"
        if int(ctx["no_progress"]) >= self._no_progress_n:
            ctx["terminated_by"] = "no_progress"
            return "synthesize"
        if int(ctx["round"]) >= self._max_rounds:
            ctx["terminated_by"] = "round_cap"
            return "synthesize"
        return "planner"

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
