"""The per-decision Trust-Ladder scheduler (S10-G6) — the governor ROUTER's matcher.

The governor's fixed cycle (``planner -> router -> run_episode -> collect``) forms a fresh
frozen :class:`~concursus.assemble.ProvisioningPlan` each round and runs ONE static Supervisor
episode over it.  This module supplies the ``router`` node's decision logic: at dispatch it
matches each ready frontier step to a *standing* agent (via the READ-ONLY
:class:`~concursus.governor.registry.AgentRegistry`, G-7), reads that agent's authoritative
*earned* trust off a GOV-side trust ladder, and decides — per decision — whether the step is
cleared to dispatch or must be **escalated** (a below-bar decision is escalated L1->L3 rather
than silently dispatched).

IDENTITY INVARIANTS (non-negotiable):

* INV-3 / INV-4: the scheduler PROPOSES a frontier — :meth:`propose_frontier` returns a
  :class:`FrontierProposal` VALUE (which nodes are cleared to compile next).  That proposal is
  INPUT to the NEXT ``recompile``; the scheduler NEVER mutates a frozen plan, never calls
  ``assemble``/``recompile`` itself, and never reaches into a running Supervisor.  After a
  ``propose_frontier`` call the plan object is byte-identical.
* INV-5 (memory seam): ``update_trust`` — the ONLY place trust is (re)earned — lives GOV-side
  ONLY.  The compiler NEVER runs it.  :func:`~concursus.build.trust.evaluate_deploy_gate` is the
  CREATE-TIME seed the ladder READS exactly once per agent (never per invocation); the earned
  ladder is a GOV-side value, and the registry/plan are re-read each decision, never cached into
  a mutable structural prefix.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

from concursus.build.trust import GateDecision, TrustGrade, evaluate_deploy_gate
from concursus.governor.registry import AgentRegistry, AgentVersion

# Per-decision actions the ladder can take for one ready step.
DISPATCH = "dispatch"      # cleared: earned trust meets the bar — propose to compile next
ESCALATE = "escalate"      # below bar: escalate (L1->L3) — NOT dispatched this round
UNMATCHED = "unmatched"    # no standing agent serves the step — needs provision, not dispatched


class SchedulerError(RuntimeError):
    """Raised on an invalid Trust-Ladder scheduler configuration or decision."""


@dataclass(frozen=True)
class ScheduleDecision:
    """One per-decision outcome: how a single ready step was resolved this round (a VALUE)."""

    node: str
    action: str                                # DISPATCH | ESCALATE | UNMATCHED
    agent: Optional[str] = None                # matched standing agent name
    version: Optional[int] = None              # matched current version number
    grade: Optional[TrustGrade] = None         # authoritative earned trust of the matched agent
    bar: Optional[TrustGrade] = None           # required autonomy floor for this decision
    escalated_to: Optional[TrustGrade] = None  # the grade a below-bar decision escalates to
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node": self.node,
            "action": self.action,
            "agent": self.agent,
            "version": self.version,
            "grade": self.grade.name if self.grade is not None else None,
            "bar": self.bar.name if self.bar is not None else None,
            "escalated_to": self.escalated_to.name if self.escalated_to is not None else None,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class FrontierProposal:
    """A frontier proposal: which ready nodes are cleared to compile next (an INPUT to recompile).

    This is a pure VALUE the ROUTER hands forward; it NEVER mutates a plan.  ``compile_next`` is the
    set of nodes cleared to dispatch, ``escalated`` the below-bar nodes held for escalation, and
    ``unmatched`` the nodes with no standing agent.  :meth:`to_dict` yields a plain dict suitable to
    hand to the next ``recompile`` round.
    """

    compile_next: Tuple[str, ...] = ()
    escalated: Tuple[str, ...] = ()
    unmatched: Tuple[str, ...] = ()
    decisions: Tuple[ScheduleDecision, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "compile_next": list(self.compile_next),
            "escalated": list(self.escalated),
            "unmatched": list(self.unmatched),
            "decisions": [d.to_dict() for d in self.decisions],
        }


@dataclass(frozen=True)
class Binding:
    """A resolved task→agent binding ( P2.4) — the scheduler's *binder* output.

    Unlike :class:`ScheduleDecision` (a gate outcome: dispatch|escalate|unmatched of a *first-match*
    agent), a ``Binding`` is the chosen ``(agent, version)`` for a task selected from the full
    candidate set by trust-PRIORITY (then availability). It is a pure VALUE — the input a post-bind
    compile (P4) consumes; the scheduler still never mutates a frozen plan.
    """

    node: str
    action: str                                # DISPATCH | ESCALATE | UNMATCHED
    agent: Optional[str] = None
    version: Optional[int] = None
    grade: Optional[TrustGrade] = None
    bar: Optional[TrustGrade] = None
    load: Optional[int] = None                 # in-flight/queued count for the chosen agent (P2.3)
    candidates: Tuple[str, ...] = ()           # all capable agent names considered (P2.1)
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node": self.node,
            "action": self.action,
            "agent": self.agent,
            "version": self.version,
            "grade": self.grade.name if self.grade is not None else None,
            "bar": self.bar.name if self.bar is not None else None,
            "load": self.load,
            "candidates": list(self.candidates),
            "reason": self.reason,
        }


class TrustLadderScheduler:
    """The per-decision Trust-Ladder scheduler — the governor router's agent matcher.

    Holds the GOV-side *earned* trust ladder (the only mutable trust store; ``build/trust`` stays
    the create-time seed).  Each decision re-reads the registry process table and the earned ladder;
    nothing structural is cached.  :meth:`propose_frontier` returns a :class:`FrontierProposal`
    VALUE; :meth:`update_trust` re-earns trust GOV-side after collect.
    """

    def __init__(
        self,
        registry: AgentRegistry,
        *,
        manifests: Optional[Mapping[str, Any]] = None,
        min_autonomy: TrustGrade = TrustGrade.L1_CANARY,
        escalation_grade: TrustGrade = TrustGrade.L3_AUTONOMOUS,
        require_approval: bool = False,
        load_fn: Optional[Any] = None,
    ) -> None:
        self._registry = registry
        self._manifests: Dict[str, Any] = dict(manifests or {})
        self._min_autonomy = TrustGrade.parse(min_autonomy)
        self._escalation_grade = TrustGrade.parse(escalation_grade)
        self._require_approval = bool(require_approval)
        # P2.3: OPTIONAL availability/load signal — ``load_fn(agent_name) -> int`` (in-flight or
        # queued count). Default ``None`` => no load info, so ranking is pure trust-priority and
        # behaves deterministically. Read-only; never mutates anything.
        self._load_fn = load_fn
        # The GOV-side EARNED trust ladder — the ONLY mutable trust store. build/trust stays the
        # create-time seed; this is (re)earned by update_trust and NEVER by the compiler.
        self._earned: Dict[str, TrustGrade] = {}

    # -- create-time SEED (reads evaluate_deploy_gate ONCE per agent) --------
    def seed_grade(self, name: str) -> TrustGrade:
        """The authoritative earned grade for ``name``, seeded lazily from the create-time gate.

        The create-time :func:`evaluate_deploy_gate` is consulted at most ONCE per agent — here,
        to seed the earned ladder from the manifest's author-declared ``trust_seed``. Thereafter the
        earned grade is a GOV-side value read by :meth:`earned_grade` and re-earned only by
        :meth:`update_trust`; the create-time gate is NEVER called per-invocation.
        """
        if name in self._earned:
            return self._earned[name]
        manifest = self._manifests.get(name)
        seed = getattr(manifest, "trust_seed", None) if manifest is not None else None
        seed = TrustGrade.parse(seed) if seed is not None else TrustGrade.L0_SHADOW
        side_effecting = self._side_effecting(name)
        # Consult the create-time gate ONCE to establish the seed's live/shadow standing. A held or
        # shadowed seed floors the earned grade at L0_SHADOW; a cleared seed keeps its declared grade.
        decision: GateDecision = evaluate_deploy_gate(
            side_effecting=side_effecting,
            trust_seed=seed,
            min_autonomy=None,
            require_approval=False,
        )
        grade = seed if decision.mode == "live" else TrustGrade.L0_SHADOW
        self._earned[name] = grade
        return grade

    def earned_grade(self, name: str) -> TrustGrade:
        """Re-fetch the authoritative earned grade for ``name`` (never cached structurally).

        Seeds lazily on first read (via :meth:`seed_grade`), then returns the GOV-side ladder value.
        Every decision re-reads through here, so an :meth:`update_trust` between rounds is reflected
        immediately without any structural cache to invalidate.
        """
        return self.seed_grade(name)

    # -- per-decision matching (the ROUTER's core) --------------------------
    def decide(self, node: str) -> ScheduleDecision:
        """Resolve ONE ready step: match to a standing agent, read earned trust, dispatch|escalate.

        Re-reads the READ-ONLY registry process table to match ``node`` (a task label) to the CURRENT
        version of a standing agent, then reads that agent's authoritative EARNED trust off the
        GOV-side ladder. If the earned trust meets the required bar the step is cleared to
        :data:`DISPATCH`; if it is below bar the decision is :data:`ESCALATE`d (to
        ``escalation_grade``, e.g. L1->L3) rather than silently dispatched. An unmatched task is
        :data:`UNMATCHED` (needs provision, not dispatch). This reads the create-time gate NOT AT ALL.
        """
        match: Optional[AgentVersion] = self._registry.match_task(node)
        if match is None:
            return ScheduleDecision(
                node=node,
                action=UNMATCHED,
                reason=f"no standing agent serves task {node!r}",
            )
        grade = self.earned_grade(match.name)
        bar = self._required_bar(match.name)
        if self._require_approval and self._side_effecting(match.name):
            return ScheduleDecision(
                node=node, action=ESCALATE, agent=match.name, version=match.version,
                grade=grade, bar=bar, escalated_to=self._escalation_grade,
                reason=f"side-effecting agent {match.name!r} held for approval",
            )
        if grade < bar:
            return ScheduleDecision(
                node=node, action=ESCALATE, agent=match.name, version=match.version,
                grade=grade, bar=bar, escalated_to=self._escalation_grade,
                reason=(f"earned trust {grade.name} of {match.name!r} is below the required "
                        f"{bar.name}; escalating to {self._escalation_grade.name}"),
            )
        return ScheduleDecision(
            node=node, action=DISPATCH, agent=match.name, version=match.version,
            grade=grade, bar=bar,
            reason=f"earned trust {grade.name} of {match.name!r} clears {bar.name}",
        )

    def propose_frontier(
        self,
        plan: Any,
        *,
        completed: Iterable[str],
        ready: Optional[Iterable[str]] = None,
    ) -> FrontierProposal:
        """PROPOSE which ready nodes are cleared to compile next — a VALUE, never a plan mutation.

        Reads ``plan.order`` (never writes it), skips ``completed`` nodes, decides each remaining
        ready node via :meth:`decide`, and returns a :class:`FrontierProposal` VALUE partitioning the
        frontier into ``compile_next`` (cleared to dispatch), ``escalated`` (below-bar), and
        ``unmatched``. That proposal is INPUT to the next ``recompile`` — the scheduler NEVER calls
        assemble/recompile itself and NEVER mutates the frozen plan (INV-3/INV-4).
        """
        done = set(completed)
        if ready is not None:
            frontier = [n for n in ready if n not in done]
        else:
            frontier = [n for n in list(getattr(plan, "order", [])) if n not in done]
        compile_next: List[str] = []
        escalated: List[str] = []
        unmatched: List[str] = []
        decisions: List[ScheduleDecision] = []
        for node in frontier:
            decision = self.decide(node)
            decisions.append(decision)
            if decision.action == DISPATCH:
                compile_next.append(node)
            elif decision.action == ESCALATE:
                escalated.append(node)
            else:
                unmatched.append(node)
        return FrontierProposal(
            compile_next=tuple(compile_next),
            escalated=tuple(escalated),
            unmatched=tuple(unmatched),
            decisions=tuple(decisions),
        )

    # -- GOV-side trust update (NEVER the compiler; NEVER per-invocation gate) -
    def update_trust(self, name: str, outcome: Any) -> TrustGrade:
        """Re-earn trust GOV-side after collect from an episode outcome; return the new grade.

        This is the ONLY place earned trust changes, and it lives GOV-side ONLY: the compiler NEVER
        runs it and it NEVER calls the create-time :func:`evaluate_deploy_gate`. A clean outcome
        (``ok`` truthy / not a failure) promotes the earned grade by one rung (capped at
        ``escalation_grade``); a failing outcome demotes by one rung (floored at ``L0_SHADOW``). The
        new grade is written to the GOV-side ladder so the NEXT round's :meth:`decide` reads it.
        """
        current = self.earned_grade(name)
        ok = self._outcome_ok(outcome)
        if ok:
            nxt = min(int(current) + 1, int(self._escalation_grade))
        else:
            nxt = max(int(current) - 1, int(TrustGrade.L0_SHADOW))
        grade = TrustGrade(nxt)
        self._earned[name] = grade
        return grade

    # -- P2: the BINDER (candidate set × trust-priority × availability) -----
    def decide_ranked(self, node: str) -> Binding:
        """Bind ONE task by candidate-set × trust-PRIORITY × availability (P2.1/2.2/2.3).

        Unlike :meth:`decide` (first-match then gate), this pulls the FULL candidate set via
        ``registry.match_all(node)``, keeps those clearing the bar, ranks them best-trust-first
        (tie-break: least ``load_fn`` if supplied, then agent name for determinism), and returns a
        :class:`Binding`. If every candidate is below bar → ``ESCALATE``; if none serve → ``UNMATCHED``.
        A pure VALUE — reads the read-only registry + earned ladder, mutates nothing.
        """
        candidates = self._registry.match_all(node)
        if not candidates:
            return Binding(node=node, action=UNMATCHED,
                           reason=f"no standing agent serves task {node!r}")
        cand_names = tuple(c.name for c in candidates)
        bar = self._required_bar_over(candidates)
        # partition into cleared vs below-bar (each candidate judged against its own side-effecting bar)
        cleared = []
        for c in candidates:
            grade = self.earned_grade(c.name)
            c_bar = self._required_bar(c.name)
            if self._require_approval and self._side_effecting(c.name):
                continue
            if grade >= c_bar:
                cleared.append((c, grade, c_bar))
        if not cleared:
            return Binding(node=node, action=ESCALATE, bar=bar, candidates=cand_names,
                           reason=f"all {len(candidates)} capable agents for {node!r} are below bar")
        # rank: highest trust first; tie-break least load then name (deterministic)
        def _load(name: str) -> int:
            if self._load_fn is None:
                return 0
            try:
                return int(self._load_fn(name))
            except Exception:  # pragma: no cover - a bad load_fn must not break scheduling
                return 0
        cleared.sort(key=lambda t: (-int(t[1]), _load(t[0].name), t[0].name))
        best, grade, c_bar = cleared[0]
        return Binding(
            node=node, action=DISPATCH, agent=best.name, version=best.version,
            grade=grade, bar=c_bar, load=_load(best.name), candidates=cand_names,
            reason=(f"bound {best.name!r} v{best.version} (trust {grade.name} clears {c_bar.name}); "
                    f"best of {len(cleared)}/{len(candidates)} cleared candidates"),
        )

    def _required_bar_over(self, candidates: List[AgentVersion]) -> TrustGrade:
        """The strictest bar among a candidate set (for reporting on an escalation)."""
        bar = TrustGrade.L0_SHADOW
        for c in candidates:
            b = self._required_bar(c.name)
            if int(b) > int(bar):
                bar = b
        return bar

    def propose_bindings(
        self,
        plan: Any,
        *,
        completed: Iterable[str],
        ready: Optional[Iterable[str]] = None,
    ) -> Dict[str, Binding]:
        """Bind every ready frontier task → a chosen agent (P2.4) — a VALUE, never a plan mutation.

        The binder analogue of :meth:`propose_frontier`: reads ``plan.order`` (never writes it), skips
        ``completed`` nodes, and returns ``{node: Binding}`` for the frontier. This is the input a
        post-bind compile (P4) consumes; the scheduler NEVER calls assemble/recompile and NEVER
        mutates the frozen plan (INV-3/INV-4). ``UNMATCHED`` bindings are what a Create arrow (P3)
        turns into an on-demand spawn.
        """
        done = set(completed)
        if ready is not None:
            frontier = [n for n in ready if n not in done]
        else:
            frontier = [n for n in list(getattr(plan, "order", [])) if n not in done]
        return {node: self.decide_ranked(node) for node in frontier}

    # -- internals ----------------------------------------------------------
    @staticmethod
    def _outcome_ok(outcome: Any) -> bool:
        """Read a clean/failed signal off an episode outcome dict (read-only)."""
        if isinstance(outcome, Mapping):
            if outcome.get("ok") is False or outcome.get("error"):
                return False
            if outcome.get("status") == "failed":
                return False
        return True

    def _required_bar(self, name: str) -> TrustGrade:
        """The autonomy floor for a decision about ``name`` (L0 for non-side-effecting agents)."""
        if not self._side_effecting(name):
            return TrustGrade.L0_SHADOW
        return self._min_autonomy

    def _side_effecting(self, name: str) -> bool:
        manifest = self._manifests.get(name)
        return bool(getattr(manifest, "side_effecting", False)) if manifest is not None else False


# -- B4: the adaptive-strictness dial ----------------------------
def make_trust_strictness(
    scheduler: "TrustLadderScheduler",
    *,
    strict_below: TrustGrade = TrustGrade.L2_GUARDED,
) -> Callable[[str], bool]:
    """A ``node -> bool`` predicate for the compiler/QA STRICTNESS DIAL ( B4).

    Returns ``True`` (apply the strict contract) for a node whose serving agent's EARNED trust is
    BELOW ``strict_below`` — i.e. WEAK / unproven agents (default: below ``L2_GUARDED``, so L0/L1)
    get the strict deep gates (type-align, single-writer, output-QA), while STRONG / proven ones
    (>= the bar) run the lean path. This is the [35e2b1a1b1] resolution in code: strictness ∝
    1/strength, read off the SAME Trust Ladder that governs autonomy.

    The grade is read live via ``scheduler.earned_grade(node)`` (author/compile-time, GOV-side).
    An UNKNOWN / never-seeded node — no evidence yet — is treated as WEAK (returns ``True``, the
    conservative default: an unproven role earns the strict contract until it proves otherwise).
    Wire it as ``OrchestrationAssembler(..., strict_fn=make_trust_strictness(sched))`` and/or
    ``Supervisor(..., acceptance_fn=make_trust_strictness(sched))``.
    """
    bar = TrustGrade.coerce(strict_below) if not isinstance(strict_below, TrustGrade) else strict_below

    def is_strict(node: str) -> bool:
        try:
            grade = scheduler.earned_grade(node)
        except Exception:  # noqa: BLE001 - an unresolvable node is treated as weak (strict)
            return True
        return int(grade) < int(bar)

    return is_strict


# -- SPIKE B: the trust-tiered PAYLOAD dial (make_payload_tier) -----------------
class Tier(Enum):
    """The payload-detail tier for a node. Payload detail ∝ 1/trust:

    * ``HIGH`` — lean: goal + precise I/O + acceptance only (a proven agent chooses its method).
    * ``GUARDED`` — I/O + guardrails only (no full SOP/examples).
    * ``LOW`` — full context: SOP + tools + guardrails + examples (a weak/unproven agent coached).
    * ``PROGRAMMATIC`` — a fixed tool-call interface (orthogonal to trust; matched, not coached).
    """

    HIGH = "high"
    GUARDED = "guarded"
    LOW = "low"
    PROGRAMMATIC = "programmatic"


#: The monotone context-lattice: which ``contract.context`` keys survive at each tier (FZ
#: 35e2b1a1b2a1 §3). ``dimension 1`` (I/O + acceptance) lives in ``contract`` itself and is
#: invariant — this projects only ``dimension 2`` (the coaching context) + ``dimension 3``
#: (``tool_calls``, programmatic only). A tier's set is a subset of the LOWER (weaker) tier's.
_TIER_CONTEXT_KEYS: Dict[Tier, Optional[frozenset]] = {
    Tier.HIGH: frozenset(),                                                  # lean: nothing
    Tier.GUARDED: frozenset({"guardrails"}),                                 # guardrails only
    Tier.LOW: None,                                                          # None => keep all
    Tier.PROGRAMMATIC: frozenset({"tool_calls"}),                            # the fixed interface
}


def project_context(full_context: Mapping[str, Any], tier: Tier) -> Dict[str, Any]:
    """Project a node's declared ``contract.context`` down to the subset its ``tier`` keeps (B2).

    Pure function (no scheduler / no I/O). ``LOW`` keeps everything (a weak agent is fully coached);
    ``GUARDED`` keeps only ``guardrails``; ``HIGH`` keeps nothing (a proven agent runs lean);
    ``PROGRAMMATIC`` keeps only ``tool_calls`` (the fixed interface). Absent/empty context or an
    unknown tier returns ``{}``. This realizes the monotone lattice: a higher tier's kept-set is a
    subset of a lower tier's, so a promotion only ever REMOVES context and a demotion only ADDS it.
    """
    if not isinstance(full_context, Mapping) or not full_context:
        return {}
    keep = _TIER_CONTEXT_KEYS.get(tier, frozenset())
    if keep is None:  # LOW — keep the whole declared context
        return dict(full_context)
    return {k: full_context[k] for k in keep if k in full_context}


def manifest_is_programmatic(manifests: Mapping[str, Any]) -> Callable[[str], bool]:
    """A ``node -> bool`` predicate reading the ``registry.programmatic`` manifest flag (FZ
    35e4a3a1b F4). A node whose manifest sets ``registry.programmatic: true`` is a script-like
    tool-agent — :func:`make_payload_tier` gives it the fixed ``PROGRAMMATIC`` tier regardless of
    earned trust (matched, not coached). Default (flag absent/false) => ``False``, so a normal
    agent is tiered by trust. Pure read; wire as ``make_payload_tier(sched, manifest_is_programmatic(m))``."""
    def is_prog(node: str) -> bool:
        manifest = manifests.get(node)
        registry = getattr(manifest, "registry", None) if manifest is not None else None
        return bool(registry.get("programmatic")) if isinstance(registry, Mapping) else False

    return is_prog


def make_payload_tier(
    scheduler: "TrustLadderScheduler",
    is_programmatic: Optional[Callable[[str], bool]] = None,
    *,
    strict_below: TrustGrade = TrustGrade.L2_GUARDED,
) -> Callable[[str], Tier]:
    """A ``node -> Tier`` selector — the 4-value generalization of :func:`make_trust_strictness`
    . A 2-D decision: (earned trust grade) × (is the agent script-like?).

    * ``is_programmatic(node)`` truthy -> :attr:`Tier.PROGRAMMATIC` (ORTHOGONAL to trust — a
      script-like tool-agent gets the fixed tool-call interface regardless of grade).
    * else by earned trust: ``>= strict_below`` (default ``L2_GUARDED``) -> :attr:`Tier.HIGH` when
      strictly above the bar, :attr:`Tier.GUARDED` at the bar; below -> :attr:`Tier.LOW`; an
      UNKNOWN / never-seeded node -> :attr:`Tier.LOW` (conservative, matching
      :func:`make_trust_strictness`'s weak-until-proven default).

    Read live via ``scheduler.earned_grade(node)`` (author/compile-time, GOV-side). Wire the result
    into a payload-context overlay (SPIKE B) and, once the full contract lands, as the single dial
    that also drives verification depth (§6), so payload tier and QA strictness never drift.
    """
    bar = TrustGrade.coerce(strict_below) if not isinstance(strict_below, TrustGrade) else strict_below

    def tier_of(node: str) -> Tier:
        if is_programmatic is not None:
            try:
                if is_programmatic(node):
                    return Tier.PROGRAMMATIC
            except Exception:  # noqa: BLE001 - a bad predicate must not break tiering
                pass
        try:
            grade = scheduler.earned_grade(node)
        except Exception:  # noqa: BLE001 - unknown/unresolvable node => conservative LOW (weak)
            return Tier.LOW
        if int(grade) > int(bar):
            return Tier.HIGH
        if int(grade) == int(bar):
            return Tier.GUARDED
        return Tier.LOW

    return tier_of


# -- The PURE state->Decision scheduling core (first-class structured non-dispatch reasons) --
#
# :func:`compute_schedule` is a TOTAL, DETERMINISTIC function of an already-resolved ``state``
# VALUE.  It performs NO I/O: it never reads the registry, never reads the GOV-side trust ladder,
# never touches a plan.  Every gate result the frontier depends on (are deps complete? does earned
# trust clear the bar? does the round budget admit the node?) is precomputed by the CALLER and
# handed in via ``state``; this core only PARTITIONS.  That keeps it trivially testable and keeps
# the "which nodes run" policy a pure value transform, separate from the impure matching that
# :meth:`TrustLadderScheduler.decide` / :meth:`propose_frontier` perform against the live registry
# (whose ``dispatch``/``escalate``/``unmatched`` taxonomy is deliberately UNCHANGED — this core is
# an orthogonal, opt-in addition, not a replacement, so the default path is byte-for-byte the same).

#: The FIRST-CLASS structured non-dispatch reasons. A declined node ALWAYS carries exactly one of
#: these — a non-dispatch is never a bare "not run", it is a named, machine-checkable cause.
DECLINE_DEPS_UNMET = "deps_unmet"                  # a prerequisite the node depends on is not complete
DECLINE_TRUST_GATE_FAILED = "trust_gate_failed"    # earned trust is below the node's required bar
DECLINE_BUDGET_EXHAUSTED = "budget_exhausted"      # the round/agent budget cannot admit the node

#: Gate precedence: the EARLIEST failing gate wins, so the reported reason is the most fundamental
#: blocker (a node with unmet deps AND no budget is reported ``deps_unmet`` — fix deps first).
_DECLINE_PRECEDENCE: Tuple[Tuple[str, str, str], ...] = (
    # (flag key, reason constant, default detail template)
    ("deps_met", DECLINE_DEPS_UNMET, "one or more prerequisites are not yet complete"),
    ("trust_ok", DECLINE_TRUST_GATE_FAILED, "earned trust is below the required bar"),
    ("budget_ok", DECLINE_BUDGET_EXHAUSTED, "the round/agent budget cannot admit this node"),
)


def _read(obj: Any, key: str, default: Any) -> Any:
    """Read ``key`` off a mapping OR a duck-typed object (read-only; never mutates ``obj``)."""
    if isinstance(obj, Mapping):
        return obj.get(key, default)
    return getattr(obj, key, default)


@dataclass(frozen=True)
class DeclinedNode:
    """One non-dispatched node with its FIRST-CLASS structured reason (a VALUE).

    ``reason`` is always exactly one of the ``DECLINE_*`` constants — a declined node is never a
    bare omission, it is a named cause a caller can branch on. ``detail`` is optional human-readable
    elaboration and is NEVER load-bearing (callers key off ``reason``).
    """

    node: str
    reason: str
    detail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {"node": self.node, "reason": self.reason, "detail": self.detail}


@dataclass(frozen=True)
class Decision:
    """The PURE scheduling decision: which frontier nodes DISPATCH, and which are DECLINED (a VALUE).

    ``dispatch`` is the ordered tuple of node labels cleared to run this round; ``declined`` is the
    ordered tuple of :class:`DeclinedNode` values, each carrying exactly one structured reason. Input
    order from ``state`` is preserved in both. :meth:`to_dict` yields a plain-dict form suitable to
    log to the append-only StateStore or hand to the next round.
    """

    dispatch: Tuple[str, ...] = ()
    declined: Tuple[DeclinedNode, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dispatch": list(self.dispatch),
            "declined": [d.to_dict() for d in self.declined],
        }

    def declined_by(self, reason: str) -> Tuple[str, ...]:
        """The node labels declined for a specific structured ``reason`` (order-preserving)."""
        return tuple(d.node for d in self.declined if d.reason == reason)


def compute_schedule(state: Any) -> Decision:
    """PURE ``state -> Decision``: partition a resolved frontier into dispatch vs first-class declines.

    A TOTAL, DETERMINISTIC function of the ``state`` VALUE with NO I/O — no registry read, no trust
    ladder read, no plan mutation. Every gate result is precomputed by the caller and carried in
    ``state``; this core only partitions, so "which nodes run" is a pure value transform.

    ``state`` is a mapping OR a duck-typed object exposing ``nodes`` — an ordered iterable of node
    descriptors. Each descriptor is a mapping OR object with:

    * ``node`` (str, REQUIRED) — the task label.
    * ``deps_met`` (bool, default ``True``) — are all prerequisites complete?
    * ``trust_ok`` (bool, default ``True``) — does earned trust clear the node's bar?
    * ``budget_ok`` (bool, default ``True``) — does the round/agent budget admit it?
    * ``detail`` (str, optional) — human-readable elaboration attached to a decline.

    A node is DISPATCHed iff all three gates pass; otherwise it is DECLINED with exactly ONE
    structured reason, chosen by precedence ``deps_unmet > trust_gate_failed > budget_exhausted``
    (the earliest failing gate wins, so the reported cause is the most fundamental blocker). Input
    order is preserved in both ``dispatch`` and ``declined``. A missing/empty ``nodes`` yields an
    empty :class:`Decision`.
    """
    nodes = _read(state, "nodes", None)
    if nodes is None:
        return Decision()
    dispatch: List[str] = []
    declined: List[DeclinedNode] = []
    for entry in nodes:
        name = _read(entry, "node", None)
        if name is None:
            raise SchedulerError(f"schedule state node is missing a 'node' label: {entry!r}")
        blocked = False
        for flag, reason, default_detail in _DECLINE_PRECEDENCE:
            if not bool(_read(entry, flag, True)):
                detail = _read(entry, "detail", "") or default_detail
                declined.append(DeclinedNode(node=name, reason=reason, detail=detail))
                blocked = True
                break
        if not blocked:
            dispatch.append(name)
    return Decision(dispatch=tuple(dispatch), declined=tuple(declined))
