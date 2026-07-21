"""**deliberate** — the plan-formation phase IN FRONT OF the compiler (Phase 5, AI-28/30/31).

This is the top of the reasoning tier: it ties the ``.3`` hypothesis trail
(:mod:`~concursus.trailstore`), the bounded DKS engine (:mod:`~concursus.dks_engine`), the
disposable per-round inner graph (:mod:`~concursus.inner_graph`), and the compile-time
precedent retriever (:mod:`~concursus.precedent`) into one loop that FORMS a plan by deliberation
and then LOWERS the converged conclusion into a frozen :class:`~concursus.dag.AgentDAG`.

This is the DELIBERATION organ of the OPC substrate — where concursus forms plans by bounded
reasoning before the compiler commits them. The plan/execute discipline held here is NOT a refusal
to reason (nor an identity ceiling on the reasoning tier); it is HOW concursus governs bounded
deliberation safely and auditably:

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

from typing import Callable, List, Mapping, Optional, Sequence

from ..core.dag import AgentDAG, DAGError
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
_DEFAULT_REUSE_THRESHOLD = 0.6  # a retrieved precedent at/above this score is reused, not re-derived


def seed(
    trail: HypothesisTrail,
    goal: str,
    *,
    retriever: Optional[object] = None,
    limit: int = 3,
    reuse_threshold: float = _DEFAULT_REUSE_THRESHOLD,
    confidence_floor: float = _DEFAULT_CONFIDENCE_FLOOR,
) -> List[str]:
    """Seed the ``.3`` root for a goal, REUSING a strong retrieved precedent instead of appending it.

    A NEW plan-formation episode. Two modes, decided by the best retrieved precedent:

    * **Reuse (prune-not-append):** when the retriever returns a precedent scoring at/above
      ``reuse_threshold`` that carries a decomposition (its ``nodes``/``results``), the goal root is
      seeded **pre-decomposed** — the prior's steps are fanned out as children already **confident**
      (``confidence_floor``), so :meth:`~concursus.trailstore.HypothesisTrail.open_frontier`
      immediately excludes them. The debate reuses the prior structure rather than re-deriving it,
      so warm plan-formation is **cheaper** than a cold one — the compounding the flywheel promises.
    * **Cold / weak-precedent:** with no retriever, or no precedent clearing the bar, a single
      ``Approach: <goal>`` root is seeded at confidence ``0.0`` for the investigator to decompose —
      **byte-for-byte the pre-existing behavior** (a cold start is unchanged).

    This replaces the earlier *append-a-sibling-precedent-root* wiring, which made a warm start cost
    MORE than a cold one (an extra root to adjudicate, nothing pruned). Returns the seeded root ids.

    SAFE-GOVERNANCE: SEED is triggered by a goal/ticket, NEVER by a user retrieval query — the
    retriever is a PRIMING read only; it does not itself start the write cycle. A caller wanting to
    *look up* a precedent should call the retriever directly, not ``seed``.
    """
    if not goal or not str(goal).strip():
        raise ValueError("seed requires a non-empty goal (a plan-formation episode needs a target)")

    reuse = _best_reusable_precedent(retriever, goal, limit=limit, reuse_threshold=reuse_threshold)
    if reuse is None:
        # Cold / weak-precedent: the pre-existing single-approach-root behavior, unchanged.
        return trail.fanout_root_hypotheses(goal, [{"text": f"Approach: {goal}", "confidence": 0.0}])

    trail_id, steps = reuse
    # Seed one goal root, then REUSE the prior decomposition as already-confident children so the
    # frontier is empty for them (no re-investigation) — the prune-and-replace that makes warm<cold.
    roots = trail.fanout_root_hypotheses(
        goal, [{"text": f"Reuse precedent {trail_id} for: {goal}", "confidence": 0.0}]
    )
    root = roots[0]
    reuse_conf = max(confidence_floor, _DEFAULT_CONFIDENCE_FLOOR)
    trail.fanout_hypotheses(
        root,
        [{"text": f"Reuse step: {step}", "confidence": reuse_conf} for step in steps],
    )
    return roots


def _best_reusable_precedent(
    retriever: object, goal: str, *, limit: int, reuse_threshold: float
):
    """The highest-scoring retrieved precedent that clears ``reuse_threshold`` AND carries a
    decomposition, as ``(trail_id, [step, ...])`` — or ``None`` (cold start). Best-effort: a missing
    retriever, a bad ``retrieve``, or a payload without steps all yield ``None``."""
    if retriever is None:
        return None
    retrieve = getattr(retriever, "retrieve", None)
    if not callable(retrieve):
        return None
    try:
        hits = retrieve(goal, limit=limit)
    except TypeError:
        hits = retrieve(goal)
    for h in hits or []:  # retriever returns hits ranked best-first
        score = getattr(h, "score", None)
        if score is not None and score < reuse_threshold:
            continue
        payload = getattr(h, "payload", None) or {}
        steps = payload.get("nodes") or list((payload.get("results") or {}).keys())
        if not steps:
            continue
        trail_id = getattr(h, "trail_id", None) or payload.get("trail_id") or "precedent"
        return str(trail_id), [str(s) for s in steps]
    return None


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


# --------------------------------------------------------------------------- OPT-IN: static fan-out unroll
def unroll_static_fanout(
    dag: AgentDAG,
    unroll: Optional[Mapping[str, int]] = None,
) -> AgentDAG:
    """Compile-time virtualization: unroll STATICALLY-BOUNDED fan-out into frozen parallel branches.

    OPT-IN and DEFAULT-OFF. Given ``unroll = {base_node: N}`` (a DECLARED, data-INDEPENDENT fan-out
    count ``N``), each named ``base`` node is expanded, IN THIS ONE COMPILE PASS, into ``N`` frozen
    parallel branches — the sub-node is cloned under namespaced ids ``f"{base}__fe{i}"`` (``i`` in
    ``0..N-1``) — plus:

    * a **scatter**: every upstream producer of ``base`` fans its (shared) input to all ``N`` clones,
      so the branches read the same inputs (a static shared-input scatter, not a runtime split); and
    * a **gather**: a synthetic join node ``f"{base}__gather"`` that collects the ``N`` clone outputs,
      onto which every original downstream consumer of ``base`` is re-pointed.

    The result is a NEW frozen :class:`AgentDAG` whose :meth:`~concursus.core.dag.AgentDAG.validate`
    passes, so the static :class:`~concursus.supervisor.Supervisor` runs the ``N`` branches + the
    gather in ONE pass over the frozen ``plan.order`` — NO runtime graph mutation, NO dynamic split.
    This is purely a compile-time rewrite of the topology BEFORE ``assemble`` freezes it.

    Gating (INV — default path byte-for-byte unchanged):

    * ``unroll`` absent / empty => the input ``dag`` is returned UNCHANGED (same object), so a caller
      that never asks for unrolling gets a byte-identical plan.
    * Only ``N >= 2`` unrolls (``N == 1`` is a degenerate no-op: the base node is left in place); a
      base id not present in ``dag`` or a non-int / ``N < 1`` count raises :class:`DAGError` (a
      spec error, caught at compile, never a silent mis-compile). Unbounded / data-dependent fan-out
      is OUT of scope — ``N`` MUST be a declared static bound.
    """
    if not unroll:
        return dag  # default: no spec => byte-for-byte unchanged (same object)

    # Validate the spec up front (fail closed on a bad declared bound) before any rewrite.
    for base, count in unroll.items():
        if base not in dag.nodes:
            raise DAGError(f"unroll spec names unknown node {base!r} (add it to the DAG first)")
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise DAGError(
                f"unroll[{base!r}] must be a declared static fan-out int >= 1 (got {count!r}); "
                "unbounded / data-dependent fan-out is out of scope"
            )

    targets = {base: n for base, n in unroll.items() if n >= 2}  # N==1 => degenerate no-op

    out = AgentDAG()
    # 1) Nodes: clone each unrolled base into N branch nodes + a gather; copy the rest verbatim.
    for node in dag.nodes:
        n = targets.get(node)
        if n is None:
            out.add_node(node)
            continue
        for i in range(n):
            out.add_node(f"{node}__fe{i}")
        out.add_node(f"{node}__gather")

    # 2) Edges: rewrite each original edge around the unrolled bases.
    #    - producer -> base            becomes producer -> every clone (SCATTER shared input)
    #    - base -> consumer            becomes gather -> consumer (GATHER feeds downstream)
    #    - producer/consumer both plain edges copy verbatim.
    for frm, to in dag.edges:
        frm_n = targets.get(frm)
        to_n = targets.get(to)
        srcs = [f"{frm}__gather"] if frm_n is not None else [frm]
        dsts = (
            [f"{to}__fe{i}" for i in range(to_n)] if to_n is not None else [to]
        )
        for s in srcs:
            for d in dsts:
                out.add_edge(s, d)

    # 3) Gather wiring: each clone feeds its base's synthetic gather (the synthetic join).
    for base, n in targets.items():
        gather = f"{base}__gather"
        for i in range(n):
            out.add_edge(f"{base}__fe{i}", gather)

    return out.validate()


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

    SAFE-GOVERNANCE (the crux): the loop is dynamic but STRICTLY BEFORE ``assemble`` and TERMINATES
    in a frozen plan. It never touches ``Supervisor.run``. After LOWER, execution is a static topo
    walk.
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


__all__ = ["seed", "lower_to_dag", "unroll_static_fanout", "form_plan", "Investigator"]
