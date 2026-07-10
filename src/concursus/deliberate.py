"""**deliberate** — the plan-formation phase IN FRONT OF the compiler (Phase 5, AI-28/30/31).

This is the top of the reasoning tier: it ties the ``.3`` hypothesis trail
(:mod:`~concursus.trailstore`), the bounded DKS engine (:mod:`~concursus.dks_engine`), the
disposable per-round inner graph (:mod:`~concursus.inner_graph`), and the compile-time
precedent retriever (:mod:`~concursus.precedent`) into one loop that FORMS a plan by deliberation
and then LOWERS the converged conclusion into a frozen :class:`~concursus.dag.AgentDAG`.

The concursus identity guard, held here at its highest-risk point:

* **SEED (AI-28)** starts a NEW plan-formation episode from a goal (optionally primed by retrieved
  precedents). It is triggered by a goal/ticket ONLY — never by a user retrieval query
  (retrieval-to-DKS is an anti-pattern).
* **LOWER (AI-30)** is a PURE DETERMINISTIC fold (no LLM) that may run only over a CONVERGED debate
  (:func:`~concursus.trailstore.require_resolved` raises :class:`ThreadNotResolved` on an open
  frontier). It reads the Dung grounded labels (AI-26) — IN hypotheses become the task
  decomposition, REJECT the dead-ends — and emits an IMMUTABLE :class:`AgentDAG` the existing
  :class:`~concursus.assemble.OrchestrationAssembler` freezes and the static
  :class:`~concursus.supervisor.Supervisor` replays. Re-opening ``.3`` is a NEW episode, never a
  mutation of a live plan.
* **form_plan (AI-31)** runs the bounded SEED -> READ FRONTIER -> DISPATCH -> DIGEST -> VERDICT ->
  RE-READ loop until the frontier empties (bounded by ``max_rounds`` / ``depth_cap`` /
  ``confidence_floor``), then LOWERS. The loop is dynamic but STRICTLY BEFORE ``assemble`` and
  MUST terminate in a frozen plan; after LOWER, execution is a static topo walk. **It never touches
  ``Supervisor.run``.**

All model/agent work enters through INJECTED seams (``engine`` / ``investigator`` / ``retriever``)
with deterministic-stub defaults, so the whole driver runs end-to-end with NEITHER langgraph NOR
any LLM installed. Real reasoning wires a real LLM investigator to those seams; the scaffolding is
correct-but-inert without one. Pure stdlib.
"""

from __future__ import annotations

from typing import Callable, List, Optional, Sequence

from .dag import AgentDAG
from .dks_engine import DKSEngine
from .inner_graph import (
    InnerGraphDigest,
    compile_inner_graph,
    dispatch_frontier,
)
from .trailstore import (
    Candidate,
    Hypothesis,
    HypothesisTrail,
    ThreadNotResolved,
    require_resolved,
)

# An investigator resolves one open hypothesis: it returns either a verdict spec
# ``{"verdict": "ACCEPT|REJECT|UNDEC", "evidence": {...}}`` or a list of child candidates.
Investigator = Callable[[Hypothesis], object]

_DEFAULT_MAX_ROUNDS = 8
_DEFAULT_DEPTH_CAP = 5
_DEFAULT_CONFIDENCE_FLOOR = 0.6


# --------------------------------------------------------------------------- AI-28: SEED
def seed(
    trail: HypothesisTrail,
    goal: str,
    *,
    retriever: Optional[object] = None,
    limit: int = 3,
) -> List[str]:
    """Seed root hypotheses under ``.3`` from a goal, optionally primed by retrieved precedents.

    A NEW plan-formation episode: combines the ``goal`` with candidate approaches drawn from prior
    resolved runs (via an AI-17 :class:`~concursus.precedent.PrecedentRetriever`, or any object with
    a compatible ``retrieve`` method; ``None`` => goal only) and calls
    :meth:`~concursus.trailstore.HypothesisTrail.fanout_root_hypotheses`. Returns the seeded root ids.

    IDENTITY: SEED is triggered by a goal/ticket, NEVER by a user retrieval query — retrieval is
    only a PRIMING read here (it surfaces candidate decompositions), it does not itself start the
    write cycle. A caller wanting to *look up* a precedent should call the retriever directly, not
    ``seed``.
    """
    if not goal or not str(goal).strip():
        raise ValueError("seed requires a non-empty goal (a plan-formation episode needs a target)")

    candidates: List[Candidate] = [{"text": f"Approach: {goal}", "confidence": 0.0}]
    if retriever is not None:
        for rp in _retrieve_candidates(retriever, goal, limit=limit):
            candidates.append(rp)
    return trail.fanout_root_hypotheses(goal, candidates)


def _retrieve_candidates(retriever: object, goal: str, *, limit: int) -> List[Candidate]:
    """Turn retrieved precedents into seed candidates (best-effort; a bad retriever yields none)."""
    retrieve = getattr(retriever, "retrieve", None)
    if not callable(retrieve):
        return []
    try:
        hits = retrieve(goal, limit=limit)
    except TypeError:
        hits = retrieve(goal)
    out: List[Candidate] = []
    for h in hits or []:
        payload = getattr(h, "payload", None) or {}
        trail_id = getattr(h, "trail_id", None) or payload.get("trail_id") or "precedent"
        out.append(
            {
                "text": f"Precedent {trail_id}: reuse/adapt the prior decomposition",
                "confidence": 0.0,
                "precedent": trail_id,
            }
        )
    return out


# --------------------------------------------------------------------------- AI-30: LOWER
def lower_to_dag(
    trail: HypothesisTrail,
    root: str,
    *,
    require_resolved_first: bool = True,
    depth_cap: int = _DEFAULT_DEPTH_CAP,
    confidence_floor: float = _DEFAULT_CONFIDENCE_FLOOR,
) -> AgentDAG:
    """Lower a CONVERGED ``.3`` debate into an IMMUTABLE :class:`AgentDAG` (a pure, no-LLM fold).

    The surviving **IN**-labelled hypotheses (from the Dung grounded extension, AI-26) become the
    task decomposition — one DAG node per accepted hypothesis, edged parent -> child along the
    accepted sub-tree; **OUT** (REJECT) hypotheses are dead-ends and dropped. The result is handed to
    the existing :class:`~concursus.assemble.OrchestrationAssembler`, which freezes it; the static
    :class:`~concursus.supervisor.Supervisor` then replays it (resume=replay holds).

    ``require_resolved_first`` (default ``True``) calls
    :func:`~concursus.trailstore.require_resolved` and RAISES :class:`ThreadNotResolved` on an open
    frontier — you may only lower from a converged debate, never a live one. Re-opening ``.3`` is a
    NEW formation episode.
    """
    if require_resolved_first:
        require_resolved(trail, root, depth_cap=depth_cap, confidence_floor=confidence_floor)

    labels = trail.compute_grounded_extension(root)
    model = trail.hypotheses(root)

    dag = AgentDAG()
    accepted = [hid for hid, lab in labels.items() if lab == "in" and hid in model]
    # Deterministic order: by address (materialized path sorts parents before children).
    accepted.sort()

    node_of = {}
    for hid in accepted:
        name = _node_name(model[hid], hid)
        node_of[hid] = name
        dag.add_node(name)

    for hid in accepted:
        parent = model[hid].parent
        if parent is not None and parent in node_of and node_of[parent] != node_of[hid]:
            dag.add_edge(node_of[parent], node_of[hid])

    # A degenerate debate that accepted nothing still yields a valid (empty) DAG; callers that
    # require at least one node can check dag.nodes. validate() confirms acyclicity.
    return dag.validate()


def _node_name(hyp: Hypothesis, hid: str) -> str:
    """A stable, filesystem/DAG-safe node name for an accepted hypothesis."""
    base = "".join(
        ch if (ch.isascii() and (ch.isalnum() or ch == "_")) else "_"
        for ch in (hyp.text or "").strip().lower()
    ).strip("_")
    base = "_".join(filter(None, base.split("_")))[:40]
    suffix = hid.rsplit("/", 1)[-1]
    if not base:
        return f"step_{suffix}"
    return f"{base}__{suffix}"


# --------------------------------------------------------------------------- AI-31: the driver
def form_plan(
    trail: HypothesisTrail,
    goal: str,
    *,
    retriever: Optional[object] = None,
    engine: Optional[DKSEngine] = None,
    investigator: Optional[Investigator] = None,
    max_rounds: int = _DEFAULT_MAX_ROUNDS,
    depth_cap: int = _DEFAULT_DEPTH_CAP,
    confidence_floor: float = _DEFAULT_CONFIDENCE_FLOOR,
    concurrency_ceiling: int = 4,
    digest: Optional[InnerGraphDigest] = None,
) -> AgentDAG:
    """Form a plan by BOUNDED deliberation, then LOWER it into a frozen :class:`AgentDAG` (AI-31).

    Runs, for each seeded root, the SEED -> READ FRONTIER -> DISPATCH (inner graph) -> DIGEST ->
    VERDICT -> RE-READ loop until the frontier empties or ``max_rounds`` is spent (a hard budget —
    no unbounded expansion), then :func:`lower_to_dag`. All model/agent work is INJECTED: an
    ``engine`` (a :class:`~concursus.dks_engine.DKSEngine`, built over ``trail`` with the given
    ``investigator`` if not supplied) drives the verdicts; the per-round fan-out uses
    :mod:`~concursus.inner_graph`. Defaults are deterministic stubs, so the driver runs end-to-end
    with NEITHER langgraph NOR any LLM.

    IDENTITY (the crux): the loop is dynamic but STRICTLY BEFORE ``assemble`` and TERMINATES in a
    frozen plan. It never touches ``Supervisor.run``. After LOWER, execution is a static topo walk.
    """
    roots = seed(trail, goal, retriever=retriever)

    eng = engine or DKSEngine(
        trail,
        investigator=investigator,
        max_rounds=max_rounds,
        depth_cap=depth_cap,
        confidence_floor=confidence_floor,
    )

    # Drive each root's debate to convergence (bounded). The engine's own loop is bounded; we also
    # cap the outer merge-dispatch passes so a pathological engine can never hang the driver.
    for root in roots:
        for _pass in range(max_rounds):
            frontier = trail.open_frontier(
                root, depth_cap=depth_cap, confidence_floor=confidence_floor
            )
            if not frontier:
                break
            # DISPATCH + DIGEST: one investigator per open hypothesis, results to the .2 lane.
            graph = compile_inner_graph(
                trail,
                root,
                concurrency_ceiling=concurrency_ceiling,
                depth_cap=depth_cap,
                confidence_floor=confidence_floor,
            )
            dispatch_frontier(graph, investigator, digest=digest)
            # VERDICT: the bounded DKS engine reads the frontier and writes verdicts (.3).
            eng.run(root)

    # LOWER every converged root into one DAG (raises ThreadNotResolved if any stayed open).
    return _lower_roots(trail, roots, depth_cap=depth_cap, confidence_floor=confidence_floor)


def _lower_roots(
    trail: HypothesisTrail,
    roots: Sequence[str],
    *,
    depth_cap: int,
    confidence_floor: float,
) -> AgentDAG:
    """Fold every root's converged debate into ONE immutable AgentDAG (union of accepted nodes)."""
    merged = AgentDAG()
    for root in roots:
        sub = lower_to_dag(
            trail, root, depth_cap=depth_cap, confidence_floor=confidence_floor
        )
        for name in sub.nodes:
            if name not in merged.nodes:
                merged.add_node(name)
        for frm, to in sub.edges:
            merged.add_edge(frm, to)
    return merged.validate()


__all__ = ["seed", "lower_to_dag", "form_plan", "Investigator"]
