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
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

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
    """A resolved task→agent binding (FZ 35e2b3 P2.4) — the scheduler's *binder* output.

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
