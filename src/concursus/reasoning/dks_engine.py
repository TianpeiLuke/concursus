"""The **DKS engine** — the Phase-5 deliberation state machine (AI-24 + AI-27 + AI-32).

Concursus is the **substrate of the OPC (One-Person-Company) operating model** — a
director-not-operator system of persistent, governed crews. This module is its **DELIBERATION
organ**: it FORMS a plan by bounded reasoning STRICTLY BEFORE the compiler commits it. Its
contract is tight not as an identity ceiling but because a tight plan-formation boundary is HOW
concursus governs deliberation at OPC scale safely and auditably: reasoning stays STRICTLY OUTSIDE
execution, so a debate can never mutate a running plan.

* Everything here belongs to **PLAN-FORMATION**, STRICTLY BEFORE
  :meth:`~concursus.assemble.OrchestrationAssembler.assemble`. The engine drives a *deliberation*
  over a run's ``.3`` reasoning branch (:class:`~concursus.trailstore.HypothesisTrail`) until it
  CONVERGES (:meth:`~concursus.trailstore.HypothesisTrail.open_frontier` empties). It **never**
  dispatches an agent and is **NEVER** wired inside :meth:`~concursus.supervisor.Supervisor.run`
  (which stays a single forward topo pass over a frozen ``plan.order``). After a converged debate a
  later LOWER step (AI-30) distills an IMMUTABLE :class:`~concursus.dag.AgentDAG` — guarded by
  :func:`~concursus.trailstore.require_resolved` so you may only lower a converged debate.
* Every loop is **BOUNDED** (``max_rounds`` / ``depth_cap`` / ``confidence_floor``) and TERMINATES
  when the frontier empties or the round budget is spent — no unbounded expansion.
* SEED (a fan-out) is triggered by a NEW goal/ticket only; a user retrieval query must never
  trigger this multi-turn write cycle (retrieval-to-DKS is an anti-pattern).

Three layers, all pure stdlib — concursus imports and its full suite passes with NEITHER langgraph
NOR any LLM installed:

* **AI-24 — the cyclic state machine.** :class:`DKSEngine` runs the deliberation cycle
  ``observe -> name -> structure -> operationalize -> test -> challenge -> improve -> compile ->
  re-observe`` with confidence-gated conditional edges, carrying an MDP-ish state
  :class:`DKSState` ``s_t = (n, r, c, f)`` (node-count / Dung-label fractions / calibration /
  per-rule quality) as a small pointer (~1 KB — the trailstore is the durable backend). LangGraph
  is an **OPTIONAL, lazily-imported** backend; when it is unavailable the SAME node functions and
  routing run via a pure-Python fallback driver (a bounded while-loop). The per-node LLM/agent work
  is an **INJECTED callable seam** (``investigator=``) defaulting to a deterministic stub, so tests
  and import need no model.
* **AI-27 — the confidence gate + CCS scoring.** :func:`compute_ccs` scores a hypothesis
  ``CCS = alpha*llm_conf + beta*homophily + gamma*coherence``; :func:`route_by_confidence` maps a
  score to a routing band (``>= 0.85`` single-agent auto-accept / ``0.50-0.85`` two-agent
  argue+counter / ``< 0.50`` human-escalation). A FIXED HEURISTIC policy — pure function,
  planning-time only, never re-routes a committed plan.
* **AI-32 — RL-policy + MOOG counter-argument seams (future).** Two clean, documented injection
  points, both defaulting to the heuristic / no-op: (a) ``policy=`` lets a learned
  contextual-bandit / PPO-Options policy over the DKS-MDP state replace :func:`route_by_confidence`;
  (b) ``counter_argument_fn=`` on the CHALLENGE step lets a MOOG counter-argument generator add
  attacks/counter-hypotheses. Seams only — no RL training code lives here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence

from .trailstore import (
    Hypothesis,
    HypothesisTrail,
    LABEL_IN,
    LABEL_OUT,
    LABEL_UNDEC,
    require_resolved,
)

# ---------------------------------------------------------------- routing bands
# The three AI-27 routing bands (what to do with a decision at a given confidence).
BAND_AUTO_ACCEPT = "auto_accept"      # >= 0.85 — single-agent auto-accept
BAND_ARGUE_COUNTER = "argue_counter"  # 0.50-0.85 — two-agent argue + counter
BAND_ESCALATE = "escalate"            # < 0.50 — human-escalation

# The default heuristic thresholds (AI-27). A learned ``policy=`` (AI-32) may override.
_HIGH_BAND = 0.85
_LOW_BAND = 0.50

# The nine-step deliberation cycle (AI-24). ``re-observe`` is the loop-back edge to ``observe``.
DKS_NODES = (
    "observe",
    "name",
    "structure",
    "operationalize",
    "test",
    "challenge",
    "improve",
    "compile",
)

# The routing sentinel for the pure-Python fallback driver (mirrors langgraph's END).
_END = "__end__"


class DKSEngineError(ValueError):
    """Raised on an invalid DKS-engine configuration or an unknown backend."""


# ============================================================ AI-27: CCS + gate
@dataclass(frozen=True)
class CCSWeights:
    """The convex weights of the Confidence-Coherence Score ``alpha*llm + beta*homophily + gamma*coherence``.

    Defaults weight the model's own confidence most heavily, with homophily (agreement with the
    node's grounded neighbourhood) and coherence (how decided the framework is) as tie-breakers.
    """

    alpha: float = 0.5   # weight on the LLM/self confidence
    beta: float = 0.25   # weight on homophily (agreement with same-label neighbours)
    gamma: float = 0.25  # weight on coherence (how few UNDEC labels remain)


def compute_ccs(
    llm_conf: float,
    homophily: float,
    coherence: float,
    weights: CCSWeights = CCSWeights(),
) -> float:
    """The **Confidence-Coherence Score** ``CCS = alpha*llm_conf + beta*homophily + gamma*coherence``.

    A pure, planning-time function over three ``[0, 1]`` signals; inputs are clamped so the result
    is a well-behaved ``[0, 1]`` scalar the confidence gate can route on.
    """
    llm_conf = _clamp01(llm_conf)
    homophily = _clamp01(homophily)
    coherence = _clamp01(coherence)
    return weights.alpha * llm_conf + weights.beta * homophily + weights.gamma * coherence


# The AI-32 policy seam: a learned routing policy over ``(score, state)`` returning a band.
RoutePolicy = Callable[[float, Optional["DKSState"]], str]


def route_by_confidence(
    score: float,
    *,
    state: Optional["DKSState"] = None,
    policy: Optional[RoutePolicy] = None,
) -> str:
    """Route a CCS ``score`` to a band — the AI-27 FIXED HEURISTIC gate, with an AI-32 policy seam.

    Heuristic bands: ``>= 0.85`` -> :data:`BAND_AUTO_ACCEPT` (single-agent auto-accept),
    ``0.50-0.85`` -> :data:`BAND_ARGUE_COUNTER` (two-agent argue + counter), ``< 0.50`` ->
    :data:`BAND_ESCALATE` (human-escalation). Pure and planning-time only — it never re-routes a
    committed plan. If a learned ``policy=`` is injected (AI-32: a contextual-bandit / PPO-Options
    policy over the DKS-MDP ``state``) it fully overrides the heuristic; the policy's band is
    validated so a rogue policy cannot inject an unknown route.
    """
    if policy is not None:
        band = policy(score, state)
        if band not in (BAND_AUTO_ACCEPT, BAND_ARGUE_COUNTER, BAND_ESCALATE):
            raise DKSEngineError(
                f"routing policy returned unknown band {band!r}; expected one of "
                f"{(BAND_AUTO_ACCEPT, BAND_ARGUE_COUNTER, BAND_ESCALATE)}"
            )
        return band
    if score >= _HIGH_BAND:
        return BAND_AUTO_ACCEPT
    if score >= _LOW_BAND:
        return BAND_ARGUE_COUNTER
    return BAND_ESCALATE


# ============================================================ AI-24: MDP state
@dataclass
class DKSState:
    """The MDP-ish deliberation state ``s_t = (n, r, c, f)`` — a small pointer (~1 KB).

    The durable deliberation lives in the :class:`~concursus.trailstore.HypothesisTrail`; this is a
    compact, serializable snapshot a routing/RL policy (AI-32) can observe.

    Attributes:
        node_count: ``n`` — hypotheses in the deliberation subtree.
        label_fractions: ``r`` — Dung grounded-label fractions ``{in, out, undec}`` (sum ~1).
        calibration: ``c`` — mean agreement between self-confidence and the recorded verdict over
            resolved hypotheses (``1.0`` = perfectly calibrated; ``0.0`` = fully mis-calibrated).
        rule_quality: ``f`` — per-verdict-kind resolved fractions ``{ACCEPT, REJECT, UNDEC}``.
        round: The deliberation round (cycle count) this snapshot was taken at.
        frontier_size: The open-frontier size at snapshot time.
        last_node: The last deliberation node executed before the snapshot.
    """

    node_count: int = 0
    label_fractions: Dict[str, float] = field(
        default_factory=lambda: {LABEL_IN: 0.0, LABEL_OUT: 0.0, LABEL_UNDEC: 0.0}
    )
    calibration: float = 0.0
    rule_quality: Dict[str, float] = field(
        default_factory=lambda: {"ACCEPT": 0.0, "REJECT": 0.0, "UNDEC": 0.0}
    )
    round: int = 0
    frontier_size: int = 0
    last_node: str = ""

    def to_dict(self) -> dict:
        """A JSON-friendly view of the state pointer."""
        return {
            "n": self.node_count,
            "r": dict(self.label_fractions),
            "c": self.calibration,
            "f": dict(self.rule_quality),
            "round": self.round,
            "frontier_size": self.frontier_size,
            "last_node": self.last_node,
        }


@dataclass
class DKSResult:
    """The outcome of a bounded :meth:`DKSEngine.run` deliberation."""

    root: str
    rounds: int
    converged: bool
    frontier: List[str]
    state: DKSState
    trace: List[str]
    backend: str  # "langgraph" or "python"

    @property
    def resolved(self) -> bool:
        """Whether the deliberation converged (its open frontier is empty)."""
        return self.converged


# The AI-24 per-node work seam: given a hypothesis, return either a verdict spec
# ``{"verdict": "ACCEPT|REJECT|UNDEC", "evidence": {...}}`` (closes it) or a list of child
# candidates (fans sharper children). Defaults to a deterministic stub — no LLM required.
Investigator = Callable[[Hypothesis], object]

# The AI-32 MOOG counter-argument seam on the CHALLENGE step: given a hypothesis and the trail,
# return a list of counter-hypothesis candidates to fan (and attack the target), or ``None`` /
# ``[]`` for a no-op. Defaults to a no-op.
CounterArgumentFn = Callable[[Hypothesis, HypothesisTrail], Optional[Sequence[object]]]


def _default_investigator(h: Hypothesis) -> dict:
    """The deterministic default per-node worker — closes every hypothesis ``UNDEC``.

    Needs no LLM/agent: it makes the engine terminate immediately on a fresh frontier so import and
    tests are model-free. Real deployments inject an LLM/agent ``investigator=``.
    """
    return {"verdict": "UNDEC", "evidence": {"reason": "default deterministic stub"}}


def _default_counter_argument_fn(h: Hypothesis, trail: HypothesisTrail) -> None:
    """The default MOOG counter-argument seam — a no-op (adds no counter-arguments)."""
    return None


class DKSEngine:
    """The AI-24 cyclic deliberation state machine over a :class:`HypothesisTrail`.

    Runs ``observe -> name -> structure -> operationalize -> test -> challenge -> improve ->
    compile -> re-observe`` with a confidence-gated loop-back edge, carrying a compact
    :class:`DKSState` ``s_t=(n, r, c, f)``. The loop is BOUNDED (``max_rounds`` / ``depth_cap`` /
    ``confidence_floor``) and TERMINATES when the trail's open frontier empties or the round budget
    is spent. It runs at PLANNING TIME only and is NEVER wired into
    :meth:`~concursus.supervisor.Supervisor.run`.

    LangGraph is an OPTIONAL backend imported lazily inside :meth:`run`; when unavailable (or
    ``backend="python"``) the SAME node functions and routing execute via a pure-Python fallback
    driver. All heavy per-node work is the injected ``investigator=`` seam; routing is the injected
    ``policy=`` seam (AI-32); counter-arguments are the injected ``counter_argument_fn=`` seam
    (AI-32). All three default to a deterministic stub / heuristic / no-op, so constructing and
    running the engine needs NEITHER langgraph NOR any LLM.
    """

    def __init__(
        self,
        trail: HypothesisTrail,
        *,
        investigator: Optional[Investigator] = None,
        policy: Optional[RoutePolicy] = None,
        counter_argument_fn: Optional[CounterArgumentFn] = None,
        weights: CCSWeights = CCSWeights(),
        max_rounds: int = 8,
        depth_cap: int = 5,
        confidence_floor: float = 0.6,
        backend: str = "auto",
    ) -> None:
        if backend not in ("auto", "python", "langgraph"):
            raise DKSEngineError(
                f"backend must be 'auto' | 'python' | 'langgraph', got {backend!r}"
            )
        if max_rounds < 1:
            raise DKSEngineError("max_rounds must be >= 1 (the loop must be bounded and progress)")
        self._trail = trail
        self._investigator = investigator or _default_investigator
        self._policy = policy
        self._counter_argument_fn = counter_argument_fn or _default_counter_argument_fn
        self._weights = weights
        self._max_rounds = max_rounds
        self._depth_cap = depth_cap
        self._confidence_floor = confidence_floor
        self._backend = backend

    # -- public entry -------------------------------------------------------
    def run(self, root: str) -> DKSResult:
        """Drive the bounded deliberation to termination and return the :class:`DKSResult`.

        Tries the LangGraph backend when ``backend`` is ``"auto"`` (falling back to pure Python if
        langgraph is not importable) or ``"langgraph"`` (raising if it is missing); ``"python"``
        forces the fallback. Either backend runs the SAME node functions and routing. On return the
        trail is left in whatever converged/round-capped state the loop reached — a caller may then
        assert convergence with :func:`~concursus.trailstore.require_resolved` before an AI-30 LOWER.
        """
        # Validate the root exists (raises TrailStoreError otherwise) and seed the state.
        self._trail.hypotheses(root)
        graph = None
        if self._backend in ("auto", "langgraph"):
            graph = self._build_langgraph()
            if graph is None and self._backend == "langgraph":
                raise DKSEngineError(
                    "backend='langgraph' requested but langgraph is not installed; "
                    "install the optional 'reasoning' extra or use backend='python'"
                )
        ctx = self._initial_ctx(root)
        if graph is not None:
            ctx = self._run_langgraph(graph, ctx)
            backend = "langgraph"
        else:
            ctx = self._drive_python(ctx)
            backend = "python"
        return DKSResult(
            root=root,
            rounds=int(ctx["round"]),
            converged=bool(ctx["done"]),
            frontier=list(ctx["frontier"]),
            state=ctx["s_t"],
            trace=list(ctx["trace"]),
            backend=backend,
        )

    def lower_guard(self, root: str) -> None:
        """The AI-30 hand-off guard: assert the deliberation has CONVERGED before lowering.

        A thin delegation to :func:`~concursus.trailstore.require_resolved` (raises
        :class:`~concursus.trailstore.ThreadNotResolved` on a non-empty frontier). AI-30 LOWER must
        call this before distilling the ``.3`` debate into an immutable
        :class:`~concursus.dag.AgentDAG`; the dependency is one-directional (this engine never
        imports AI-30). Re-opening the branch is a NEW formation episode, never live mutation.
        """
        require_resolved(
            self._trail, root, depth_cap=self._depth_cap, confidence_floor=self._confidence_floor
        )

    # -- initial context ----------------------------------------------------
    def _initial_ctx(self, root: str) -> dict:
        """The initial graph state: the small MDP pointer plus loop control flags."""
        return {
            "root": root,
            "round": 0,
            "frontier": [],
            "s_t": DKSState(),
            "done": False,
            "last_band": "",
            "trace": [],
        }

    # ================================================= AI-24 node functions
    # Each node takes the graph-state dict and returns the FULL updated dict, so the same functions
    # drive BOTH backends (langgraph's dict-merge overwrites overlapping keys; the python driver
    # simply reassigns). The trail is the durable backend (captured on ``self``), not carried in the
    # ~1 KB state pointer.
    def _observe(self, ctx: dict) -> dict:
        """OBSERVE: start a cycle — bump the round and read the current open frontier."""
        ctx["round"] = int(ctx["round"]) + 1
        ctx["frontier"] = self._open_frontier(ctx["root"])
        ctx["trace"].append("observe")
        return ctx

    def _name(self, ctx: dict) -> dict:
        """NAME: label the frontier against the Dung grounded extension (read-only snapshot)."""
        ctx["s_t"] = self._snapshot(ctx, "name")
        ctx["trace"].append("name")
        return ctx

    def _structure(self, ctx: dict) -> dict:
        """STRUCTURE: refresh the grounded-label fractions of the argumentation framework."""
        ctx["s_t"] = self._snapshot(ctx, "structure")
        ctx["trace"].append("structure")
        return ctx

    def _operationalize(self, ctx: dict) -> dict:
        """OPERATIONALIZE: mark the frontier as testable (read-only; the seam is the TEST node)."""
        ctx["trace"].append("operationalize")
        return ctx

    def _test(self, ctx: dict) -> dict:
        """TEST: the resolving workhorse — ask the injected ``investigator`` about each open leaf.

        For each frontier hypothesis the investigator returns either a verdict spec (which closes it
        via :meth:`~concursus.trailstore.HypothesisTrail.write_verdict`) or a list of child
        candidates (which fans sharper children, BOUNDED by ``depth_cap``). An empty/None result
        closes the leaf ``UNDEC`` so the loop always makes progress and terminates.
        """
        model = self._trail.hypotheses(ctx["root"])
        for hid in list(ctx["frontier"]):
            if hid not in model:
                continue
            outcome = self._investigator(model[hid])
            if isinstance(outcome, dict) and "verdict" in outcome:
                self._trail.write_verdict(hid, outcome["verdict"], outcome.get("evidence"))
            elif outcome:
                self._trail.fanout_hypotheses(hid, list(outcome))
            else:
                self._trail.write_verdict(
                    hid, "UNDEC", {"reason": "investigator returned nothing"}
                )
        ctx["trace"].append("test")
        return ctx

    def _challenge(self, ctx: dict) -> dict:
        """CHALLENGE: the AI-27 gate + AI-32 MOOG counter-argument seam.

        Recomputes the still-open frontier, scores each hypothesis with :func:`compute_ccs`, and
        routes it with :func:`route_by_confidence` (heuristic, or the injected ``policy=``). For a
        :data:`BAND_ARGUE_COUNTER` decision (the two-agent argue+counter band) the injected
        ``counter_argument_fn=`` (MOOG, default no-op) may return counter-hypothesis candidates,
        which are fanned as children and wired as attacks — BOUNDED by ``depth_cap``. The default
        no-op leaves the trail untouched.
        """
        model = self._trail.hypotheses(ctx["root"])
        labels = self._grounded(ctx["root"])
        last_band = ""
        # Route over THIS round's frontier snapshot (captured at OBSERVE) — TEST may already have
        # resolved these leaves; the gate still decides the band and the MOOG seam may counter them.
        for hid in list(ctx["frontier"]):
            if hid not in model:
                continue
            h = model[hid]
            score = self._ccs_for(hid, model, labels)
            band = route_by_confidence(score, state=ctx["s_t"], policy=self._policy)
            last_band = band
            if band == BAND_ARGUE_COUNTER:
                counters = self._counter_argument_fn(h, self._trail)
                if counters:
                    if h.depth < self._depth_cap:  # bounded — never fan past the depth cap
                        kids = self._trail.fanout_hypotheses(hid, list(counters))
                        for kid in kids:
                            self._trail.attack(kid, hid)  # a counter attacks its target
        ctx["last_band"] = last_band
        ctx["trace"].append("challenge")
        return ctx

    def _improve(self, ctx: dict) -> dict:
        """IMPROVE: refine the state pointer after the round's mutations (a documented no-op seam).

        Left deliberately non-mutating: convergence is owned by TEST + the bounded caps, so IMPROVE
        never force-closes a hypothesis (that would defeat the ``max_rounds`` budget). It is the
        natural hook for a future refinement policy; today it only refreshes the MDP snapshot.
        """
        ctx["s_t"] = self._snapshot(ctx, "improve")
        ctx["trace"].append("improve")
        return ctx

    def _compile(self, ctx: dict) -> dict:
        """COMPILE: recompute the frontier, decide convergence, and finalize the ``s_t`` snapshot.

        ``done`` is ``True`` iff the open frontier is empty (the debate has CONVERGED). The routing
        edge that follows (:meth:`_route_after_compile`) loops back to OBSERVE unless ``done`` or the
        ``max_rounds`` budget is spent.
        """
        ctx["frontier"] = self._open_frontier(ctx["root"])
        ctx["done"] = len(ctx["frontier"]) == 0
        ctx["s_t"] = self._snapshot(ctx, "compile")
        ctx["trace"].append("compile")
        return ctx

    def _route_after_compile(self, ctx: dict) -> str:
        """The confidence-gated loop edge: terminate on convergence or the round budget, else re-observe."""
        if ctx["done"] or int(ctx["round"]) >= self._max_rounds:
            return _END
        return "observe"

    # -- node ordering (shared by both backends) ----------------------------
    def _node_fns(self) -> Dict[str, Callable[[dict], dict]]:
        return {
            "observe": self._observe,
            "name": self._name,
            "structure": self._structure,
            "operationalize": self._operationalize,
            "test": self._test,
            "challenge": self._challenge,
            "improve": self._improve,
            "compile": self._compile,
        }

    # ================================================= pure-Python fallback
    def _drive_python(self, ctx: dict) -> dict:
        """The bounded pure-Python driver — a while-loop over the node functions with the SAME routing.

        Runs when langgraph is absent (or ``backend='python'``). The step cap is a hard structural
        bound (``max_rounds`` cycles of the fixed 8-node chain, plus slack) so the loop can NEVER
        run away even if a node/route misbehaves.
        """
        fns = self._node_fns()
        chain = list(DKS_NODES)
        step_cap = self._max_rounds * (len(chain) + 1) + len(chain) + 8
        node = "observe"
        steps = 0
        while node != _END and steps < step_cap:
            ctx = fns[node](ctx)
            if node == "compile":
                node = self._route_after_compile(ctx)
            else:
                node = chain[chain.index(node) + 1]
            steps += 1
        return ctx

    # ================================================= optional LangGraph
    def _build_langgraph(self):
        """Lazily build a LangGraph ``StateGraph`` mirroring the fallback, or ``None`` if unavailable.

        LangGraph is imported INSIDE this method so importing concursus never requires it. Any
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
            builder.set_entry_point("observe")
            chain = list(DKS_NODES)
            for i in range(len(chain) - 1):
                builder.add_edge(chain[i], chain[i + 1])
            builder.add_conditional_edges(
                "compile",
                lambda ctx: self._route_after_compile(ctx),
                {"observe": "observe", _END: END},
            )
            return builder.compile()
        except Exception:
            return None

    def _run_langgraph(self, graph, ctx: dict) -> dict:  # pragma: no cover - needs langgraph
        """Invoke the compiled LangGraph with a recursion limit matching the round budget."""
        recursion_limit = self._max_rounds * (len(DKS_NODES) + 1) + len(DKS_NODES) + 8
        try:
            return graph.invoke(ctx, config={"recursion_limit": recursion_limit})
        except Exception:
            # A langgraph runtime failure must never break plan-formation — fall back.
            return self._drive_python(self._initial_ctx(ctx["root"]))

    # ================================================= trail-derived helpers
    def _open_frontier(self, root: str) -> List[str]:
        return self._trail.open_frontier(
            root, depth_cap=self._depth_cap, confidence_floor=self._confidence_floor
        )

    def _grounded(self, root: str) -> Dict[str, str]:
        return self._trail.compute_grounded_extension(root)

    def _snapshot(self, ctx: dict, last_node: str) -> DKSState:
        """Build the MDP-ish state pointer ``s_t=(n, r, c, f)`` from the current trail."""
        root = ctx["root"]
        model = self._trail.hypotheses(root)
        labels = self._grounded(root)
        n = len(model)
        # r — Dung grounded-label fractions.
        counts = {LABEL_IN: 0, LABEL_OUT: 0, LABEL_UNDEC: 0}
        for lab in labels.values():
            counts[lab] = counts.get(lab, 0) + 1
        total_lab = sum(counts.values()) or 1
        label_fractions = {k: counts[k] / total_lab for k in (LABEL_IN, LABEL_OUT, LABEL_UNDEC)}
        # c — calibration over resolved hypotheses (self-confidence vs. verdict target).
        resolved = [h for h in model.values() if h.resolved]
        if resolved:
            agree = sum(1.0 - abs(h.confidence - _verdict_target(h.verdict)) for h in resolved)
            calibration = agree / len(resolved)
        else:
            calibration = 0.0
        # f — per-verdict-kind resolved fractions.
        rule_quality = {"ACCEPT": 0.0, "REJECT": 0.0, "UNDEC": 0.0}
        if resolved:
            for h in resolved:
                if h.verdict in rule_quality:
                    rule_quality[h.verdict] += 1.0
            for k in rule_quality:
                rule_quality[k] /= len(resolved)
        return DKSState(
            node_count=n,
            label_fractions=label_fractions,
            calibration=_clamp01(calibration),
            rule_quality=rule_quality,
            round=int(ctx["round"]),
            frontier_size=len(ctx.get("frontier", [])),
            last_node=last_node,
        )

    def _ccs_for(
        self, hid: str, model: Dict[str, Hypothesis], labels: Dict[str, str]
    ) -> float:
        """The CCS of one hypothesis: self-confidence, homophily, and framework coherence."""
        h = model[hid]
        llm_conf = h.confidence
        # homophily — fraction of the OTHER labelled nodes sharing this node's grounded label.
        my_label = labels.get(hid, LABEL_UNDEC)
        others = [nid for nid in labels if nid != hid]
        if others:
            same = sum(1 for nid in others if labels[nid] == my_label)
            homophily = same / len(others)
        else:
            homophily = 1.0
        # coherence — how decided the framework is (few UNDEC labels).
        total = len(labels) or 1
        undec = sum(1 for lab in labels.values() if lab == LABEL_UNDEC)
        coherence = 1.0 - (undec / total)
        return compute_ccs(llm_conf, homophily, coherence, self._weights)


# ------------------------------------------------------------------ helpers
def _clamp01(x: float) -> float:
    """Clamp a scalar into ``[0, 1]``."""
    try:
        x = float(x)
    except (TypeError, ValueError):
        return 0.0
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _verdict_target(verdict: Optional[str]) -> float:
    """Map a verdict to a ``[0, 1]`` confidence target for calibration (ACCEPT=1, REJECT=0, UNDEC=0.5)."""
    if verdict == "ACCEPT":
        return 1.0
    if verdict == "REJECT":
        return 0.0
    return 0.5
