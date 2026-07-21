"""The **plan-author front** — generate an ``AgentDAG`` from a goal (AI-22).

Concursus is a COMPILER: ``AgentDAG -> assemble -> frozen ProvisioningPlan -> Supervisor.run``
as a static topological walk, and resume is replay. This module is the *front* of that compiler
— the (optionally generative) step that authors the ``AgentDAG`` the assembler then validates,
freezes, and lowers. It is emit -> validate -> freeze -> replay, never the head of a cyclic
runtime loop: :func:`plan_from_goal` produces a topology ONCE, at compile time, and hands it to
:meth:`~concursus.assemble.OrchestrationAssembler.assemble`, which is the sole authority that
validates + freezes it. The planner never dispatches, never emits mid-run, and never mutates a
running plan.

Identity guard (non-negotiable):

- The LLM is an **injected, optional** callable — the ``plan_model_fn`` seam. Default ``None``
  means importing/using concursus needs no model: with no ``plan_model_fn`` the planner falls
  back to a trivial DETERMINISTIC linear template so the suite runs with zero LLM present.
- Retrieved **precedents** (AI-17) and **operator_directives** are read-only CONTEXT passed to
  the ``plan_model_fn``; they never run a topology and never touch a live plan.
- The output is a plain :class:`~concursus.dag.AgentDAG` — a pure topology. Any correctness
  gating (alignment, cycle check, wiring) is the assembler's job, not the planner's.
"""

from __future__ import annotations

from typing import Callable, List, Mapping, Optional, Sequence

from ..core.dag import AgentDAG, DAGError

#: The injected plan-author seam: ``(goal, precedents, operator_directives) -> plan spec``.
#: A "plan spec" is a plain mapping the planner lowers into an :class:`~concursus.dag.AgentDAG`:
#: ``{"nodes": [id, ...], "edges": [[from, to], ...]}`` (an already-built ``AgentDAG`` is also
#: accepted and returned as-is). The callable is where an LLM would live; it is NEVER imported
#: or constructed here — the caller injects it, so concursus depends on no model.
PlanModelFn = Callable[
    [str, Sequence[Mapping[str, object]], Mapping[str, object]], object
]


class PlanAuthorError(ValueError):
    """Raised when a goal cannot be authored into a valid :class:`~concursus.dag.AgentDAG`."""


def _dag_from_spec(spec: object) -> AgentDAG:
    """Lower a plan-model output into an :class:`AgentDAG`.

    Accepts either an already-built :class:`AgentDAG` (returned as-is) or a plain mapping
    ``{"nodes": [...], "edges": [[from, to], ...]}`` (lowered via :meth:`AgentDAG.from_dict`).
    Raises :class:`PlanAuthorError` on anything else, or on an invalid topology.
    """
    if isinstance(spec, AgentDAG):
        dag = spec
    elif isinstance(spec, Mapping):
        try:
            dag = AgentDAG.from_dict(dict(spec))
        except (DAGError, KeyError, TypeError, IndexError) as exc:
            raise PlanAuthorError(f"plan_model_fn returned an invalid plan spec: {exc}") from exc
    else:
        raise PlanAuthorError(
            "plan_model_fn must return an AgentDAG or a {'nodes': [...], 'edges': [...]} "
            f"mapping (got {type(spec).__name__})"
        )
    if not dag.nodes:
        raise PlanAuthorError("authored plan has no nodes")
    try:
        dag.validate()  # cheap acyclicity check; the assembler re-validates + type-gates + freezes
    except DAGError as exc:
        raise PlanAuthorError(f"authored plan is not a valid DAG: {exc}") from exc
    return dag


def _fallback_template(goal: str) -> AgentDAG:
    """A trivial DETERMINISTIC plan used when no ``plan_model_fn`` is injected.

    Emits a single-source node whose id derives from the goal, so importing/using concursus needs
    no LLM. This is a genuine (if minimal) valid ``AgentDAG`` — a real ``plan_model_fn`` replaces
    it with a richer topology. It never calls out to any model.

    This is the historical single-node default (kept for back-compat). The multi-node
    :func:`_template_decompose` is the opt-in capability decomposer (``plan_from_goal(..., decompose=True)``).
    """
    node = _slug(goal) or "plan"
    dag = AgentDAG()
    dag.add_node(node)
    return dag


#: Complexity-contract defaults for an AUTHORED plan (P1.3). A sub-task is one bounded unit of
#: work; these cap the authored DAG's size/shape so a runaway decomposition is rejected at author
#: time (never a runtime cap). Overridable per call.
DEFAULT_MAX_NODES = 12
DEFAULT_MAX_DEPTH = 6
DEFAULT_MAX_FANOUT = 6

#: Keyword → extra capability stages, so a goal is decomposed into a domain-shaped capability DAG
#: (agent-agnostic task labels, never manifest keys). Deterministic, offline, no LLM.
_SHAPE_KEYWORDS = {
    "investigate": ("scope", "gather_evidence", "hypothesize", "verify"),
    "diagnos": ("scope", "gather_evidence", "hypothesize", "verify"),
    "root cause": ("scope", "gather_evidence", "hypothesize", "verify"),
    "model": ("scope_data", "build_model", "calibrate", "evaluate"),
    "detect": ("scope_data", "build_model", "calibrate", "evaluate"),
    "launch": ("scope", "design", "review", "rollout"),
    "program": ("scope", "design", "review", "rollout"),
    "migrat": ("audit_source", "transform", "validate_parity"),
    "report": ("gather", "analyze", "draft"),
    "summar": ("gather", "analyze", "draft"),
}


def _stages_from_precedent(precedents: "Optional[Sequence[Mapping[str, object]]]") -> tuple:
    """Borrow a capability-stage shape from the most-relevant retrieved precedent ( C3).

    Cross-domain PRIMING: a new domain with no keyword match can warm-start its decomposition from a
    structurally-adjacent prior run. Reads the FIRST precedent's executed ``nodes`` (a
    ``RetrievedPrecedent.to_dict()`` payload, ``{"precedent": {"nodes": [...]}}``, or a bare payload),
    strips each node's ``<prefix>__<stage>`` down to its ``<stage>`` suffix, and returns the ordered,
    de-duplicated stage tuple. Returns ``()`` when no precedent carries a usable capability shape (so
    the caller falls back to keyword routing / the generic shape). Deterministic + offline.
    """
    for entry in precedents or ():
        if not isinstance(entry, Mapping):
            continue
        payload = entry.get("precedent") if isinstance(entry.get("precedent"), Mapping) else entry
        nodes = payload.get("nodes") if isinstance(payload, Mapping) else None
        if not isinstance(nodes, (list, tuple)):
            continue
        stages: list = []
        for node in nodes:
            stage = str(node).split("__", 1)[1] if "__" in str(node) else None
            if stage and stage not in stages:
                stages.append(stage)
        if len(stages) > 1:  # a real multi-stage capability shape (not a single opaque node)
            return tuple(stages)
    return ()


def _template_decompose(
    goal: str,
    *,
    precedents: "Optional[Sequence[Mapping[str, object]]]" = None,
    max_nodes: int = DEFAULT_MAX_NODES,
    max_depth: int = DEFAULT_MAX_DEPTH,
    max_fanout: int = DEFAULT_MAX_FANOUT,
) -> AgentDAG:
    """A DETERMINISTIC, offline default template decomposer (P1.1/P1.2).

    Decomposes ``goal`` into a small *linear* chain of agent-agnostic **capability** task nodes
    (task labels, never agent/manifest names — P1.2), so the scheduler has real tasks to bind
    rather than a single opaque node. The shape is chosen in priority order: (C3) borrow an adjacent
    retrieved ``precedent``'s stage shape if one is supplied; else keyword-route against
    :data:`_SHAPE_KEYWORDS`; else a generic ``ingest -> analyze -> synthesize -> format`` fallback.
    No LLM, no AWS — a real ``plan_model_fn`` UPGRADES this, it does not enable it.

    The emitted DAG is passed through :func:`_check_complexity` so it honors the per-sub-task
    complexity contract (P1.3).
    """
    text = str(goal).strip().lower()
    # C3: prefer a cross-domain precedent's stage shape (warm-start a new domain from adjacent
    # experience) when the goal keywords don't already name a specific shape.
    stages: tuple = ()
    for kw, shape in _SHAPE_KEYWORDS.items():
        if kw in text:
            stages = shape
            break
    if not stages:
        stages = _stages_from_precedent(precedents)
    if not stages:
        stages = ("ingest", "analyze", "synthesize", "format")

    # A goal-scoped prefix keeps node ids stable + readable without embedding agent identity.
    # Strip any trailing '_' left by the 24-char truncation so the '__<stage>' boundary is a clean
    # double-underscore (not '..._' + '__' -> a spurious leading '_' on the stage).
    prefix = _slug(goal)[:24].rstrip("_") or "task"
    dag = AgentDAG()
    prev: Optional[str] = None
    for stage in stages:
        node = f"{prefix}__{stage}"
        dag.add_node(node)
        if prev is not None:
            dag.add_edge(prev, node)  # a linear capability chain (fan-out 1, bounded depth)
        prev = node
    _check_complexity(dag, max_nodes=max_nodes, max_depth=max_depth, max_fanout=max_fanout)
    return dag


def _check_complexity(
    dag: AgentDAG,
    *,
    max_nodes: int = DEFAULT_MAX_NODES,
    max_depth: int = DEFAULT_MAX_DEPTH,
    max_fanout: int = DEFAULT_MAX_FANOUT,
) -> AgentDAG:
    """Enforce the per-sub-task complexity contract on an AUTHORED DAG (P1.3).

    Author-time only (never a runtime cap): rejects a decomposition that is too large or too
    wide/deep to be a clean set of bounded unit sub-tasks. Raises :class:`PlanAuthorError` on a
    violation; returns ``dag`` unchanged otherwise.
    """
    nodes = dag.nodes
    if len(nodes) > max_nodes:
        raise PlanAuthorError(
            f"authored plan has {len(nodes)} nodes; exceeds max_nodes={max_nodes} "
            "(decompose into fewer, coarser capability sub-tasks)"
        )
    # Max fan-out: no single producer should feed more than max_fanout consumers.
    for node in nodes:
        fanout = len(dag.get_dependents(node))
        if fanout > max_fanout:
            raise PlanAuthorError(
                f"node {node!r} fans out to {fanout} consumers; exceeds max_fanout={max_fanout}"
            )
    # Max depth: longest producer->consumer chain (the DAG is already validated acyclic upstream).
    depth = _longest_path(dag)
    if depth > max_depth:
        raise PlanAuthorError(
            f"authored plan has depth {depth}; exceeds max_depth={max_depth}"
        )
    return dag


def _longest_path(dag: AgentDAG) -> int:
    """Longest producer->consumer chain length (node count) in an acyclic ``dag``."""
    memo: dict = {}

    def depth(node: str) -> int:
        if node in memo:
            return memo[node]
        deps = dag.get_dependencies(node)
        memo[node] = 1 + max((depth(d) for d in deps), default=0)
        return memo[node]

    return max((depth(n) for n in dag.nodes), default=0)


def _slug(text: str) -> str:
    """Lowercase, keep ``[a-z0-9_]``, collapse runs to a single ``_`` — a stable node id."""
    out: List[str] = []
    prev_us = False
    for ch in str(text).strip().lower():
        if ch.isalnum():
            out.append(ch)
            prev_us = False
        elif not prev_us:
            out.append("_")
            prev_us = True
    return "".join(out).strip("_")


def plan_from_goal(
    goal: str,
    *,
    precedents: Optional[Sequence[Mapping[str, object]]] = None,
    operator_directives: Optional[Mapping[str, object]] = None,
    plan_model_fn: Optional[PlanModelFn] = None,
    decompose: bool = False,
    max_nodes: int = DEFAULT_MAX_NODES,
    max_depth: int = DEFAULT_MAX_DEPTH,
    max_fanout: int = DEFAULT_MAX_FANOUT,
) -> AgentDAG:
    """Author an :class:`~concursus.dag.AgentDAG` for ``goal`` — the compiler's generative FRONT.

    This is the emit step of emit -> validate -> freeze -> replay. It produces a topology ONCE
    (compile time); the returned DAG is meant to be handed straight to
    :meth:`~concursus.assemble.OrchestrationAssembler.assemble`, which is the sole authority that
    validates + type-gates + freezes it into a replayable :class:`~concursus.assemble.ProvisioningPlan`.
    The planner itself dispatches nothing and mutates no running plan.

    Args:
        goal: A free-text description of the team's objective.
        precedents: Read-only prior-run context (AI-17 ``RetrievedPrecedent.to_dict()`` payloads,
            or the ``ProvisioningPlan.precedents`` list). Passed to ``plan_model_fn`` as context;
            never executed, never a live plan.
        operator_directives: Read-only operator constraints/preferences (e.g. required nodes,
            budget hints). Passed to ``plan_model_fn`` as context.
        plan_model_fn: The INJECTED plan-author callable (the LLM seam). Default ``None`` — when
            absent, a deterministic template is used so concursus imports/runs with NO
            model present. When supplied, it is called
            ``plan_model_fn(goal, precedents, operator_directives)`` and must return an
            :class:`AgentDAG` or a ``{"nodes": [...], "edges": [...]}`` mapping.
        decompose: Opt-in (default ``False`` — byte-identical to the historical single-node
            fallback). When ``True`` AND no ``plan_model_fn`` is injected, the deterministic
            :func:`_template_decompose` emits a multi-node **capability** DAG (P1.1/P1.2) subject
            to the complexity contract (P1.3). An injected ``plan_model_fn`` always takes
            precedence over the template, regardless of ``decompose``.
        max_nodes / max_depth / max_fanout: the per-sub-task complexity-contract caps applied to
            the AUTHORED DAG (P1.3); enforced for both the template decomposer and an injected
            model's output.

    Returns:
        A validated (acyclic, non-empty) :class:`AgentDAG` ready to ``assemble``.

    Raises:
        PlanAuthorError: if ``goal`` is empty, a ``plan_model_fn`` returns an invalid spec, or the
            authored DAG violates the complexity contract.
    """
    if not goal or not str(goal).strip():
        raise PlanAuthorError("plan_from_goal requires a non-empty goal")

    ctx_precedents: Sequence[Mapping[str, object]] = list(precedents or [])
    ctx_directives: Mapping[str, object] = dict(operator_directives or {})

    if plan_model_fn is None:
        # No model configured. Default: the historical single-node fallback (back-compat).
        # Opt-in ``decompose=True``: the deterministic multi-node capability template.
        if not decompose:
            return _fallback_template(goal)
        return _template_decompose(
            goal, precedents=ctx_precedents,
            max_nodes=max_nodes, max_depth=max_depth, max_fanout=max_fanout
        )

    spec = plan_model_fn(goal, ctx_precedents, ctx_directives)
    dag = _dag_from_spec(spec)
    # The complexity contract applies to an injected model's output too (only when caps are the
    # non-default explicit intent OR decompose was requested — keep the default byte-identical for
    # existing model-injected callers by only enforcing when decompose=True).
    if decompose:
        _check_complexity(dag, max_nodes=max_nodes, max_depth=max_depth, max_fanout=max_fanout)
    return dag
