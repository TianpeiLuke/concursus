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
    """
    node = _slug(goal) or "plan"
    dag = AgentDAG()
    dag.add_node(node)
    return dag


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
            absent, a trivial DETERMINISTIC template is used so concursus imports/runs with NO
            model present. When supplied, it is called
            ``plan_model_fn(goal, precedents, operator_directives)`` and must return an
            :class:`AgentDAG` or a ``{"nodes": [...], "edges": [...]}`` mapping.

    Returns:
        A validated (acyclic, non-empty) :class:`AgentDAG` ready to ``assemble``.

    Raises:
        PlanAuthorError: if ``goal`` is empty, or ``plan_model_fn`` returns an invalid spec.
    """
    if not goal or not str(goal).strip():
        raise PlanAuthorError("plan_from_goal requires a non-empty goal")

    ctx_precedents: Sequence[Mapping[str, object]] = list(precedents or [])
    ctx_directives: Mapping[str, object] = dict(operator_directives or {})

    if plan_model_fn is None:
        # No model configured — deterministic fallback. concursus needs no LLM to import or run.
        return _fallback_template(goal)

    spec = plan_model_fn(goal, ctx_precedents, ctx_directives)
    return _dag_from_spec(spec)
