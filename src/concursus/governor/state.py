"""Persistent outer-loop state for the governor cycle.

The governor drives a bounded cycle *around* the compiler.  Its persistent
state is deliberately NOT a mutable compiler plan: it holds the SEQUENCE of
frozen :class:`ProvisioningPlan` VALUEs produced across rounds (by version),
plus accumulated episode evidence, plus a POINTER to the append-only
:class:`StateStore` log (the sole structural anchor of the executed prefix).

Identity invariants this class upholds:

* INV-3 / INV-4: a frozen plan is never mutated mid-episode.  New evidence =>
  a NEW plan formed at the compiler front (via ``assemble``/``recompile``) and
  swapped in with :meth:`advance`, which bumps :attr:`plan_version` WITHOUT
  touching the prior plan object.  Prior plan values remain byte-identical.
* INV-5: the executed prefix is re-derived from the held :class:`StateStore`
  each round; it is never cached mutably here.  This state holds a plan VALUE
  by version + a log pointer, never a live plan dict to deserialize-mutate-
  reserialize.

There is deliberately NO ``set_output``-style API and no method that edits a
plan in place.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from concursus.assemble.assemble import ProvisioningPlan
from concursus.state.statestore import StateStore


@dataclass
class GovernorState:
    """Outer-loop state: a sequence of frozen plan VALUEs + a log pointer.

    Attributes
    ----------
    current_frozen_plan:
        The frozen :class:`ProvisioningPlan` VALUE for the current round. Never
        edited in place.  Mirrors the tail of :attr:`plan_history`.
    store:
        POINTER to the append-only :class:`StateStore` log — the sole
        structural anchor of the executed prefix.  Held, not copied inline.
    plan_version:
        Mirrors ``current_frozen_plan.revision``.
    iteration:
        Number of governor rounds/episodes run so far.
    no_progress:
        Consecutive rounds that made no forward progress (for stall bounds).
    replan_reason:
        Why the most recent replan happened (``None`` before any replan).
    plan_history:
        The full ordered SEQUENCE of frozen plan VALUEs, oldest first. Each
        entry is a distinct, replayable-in-isolation frozen plan.
    """

    current_frozen_plan: ProvisioningPlan
    store: StateStore
    plan_version: int = 0
    iteration: int = 0
    no_progress: int = 0
    replan_reason: Optional[str] = None
    plan_history: List[ProvisioningPlan] = field(default_factory=list)

    def __post_init__(self) -> None:
        # plan_version always mirrors the current frozen plan's revision.
        self.plan_version = self.current_frozen_plan.revision
        # Seed the history with the current plan value (oldest first) so the
        # sequence is complete from round zero.
        if not self.plan_history:
            self.plan_history = [self.current_frozen_plan]

    def advance(
        self,
        next_plan: ProvisioningPlan,
        *,
        reason: Optional[str] = None,
        progressed: bool = True,
    ) -> "GovernorState":
        """Swap in a newly-assembled/recompiled plan; bump the version.

        Does NOT edit the prior plan object — that value is preserved verbatim
        in :attr:`plan_history` and remains byte-identical after this call. The
        held :class:`StateStore` pointer is unchanged; the executed prefix stays
        re-derivable from the append-only log (INV-5).
        """
        # The prior plan value stays in history untouched (INV-3/INV-4). We
        # never edit next_plan or the prior plan here — we only re-point which
        # frozen VALUE is "current" and record the new one in the sequence.
        self.plan_history = self.plan_history + [next_plan]
        self.current_frozen_plan = next_plan
        self.plan_version = next_plan.revision
        self.iteration += 1
        self.no_progress = 0 if progressed else self.no_progress + 1
        self.replan_reason = reason
        return self
