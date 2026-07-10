"""Create-time **deploy gate** — decide ``live`` | ``shadow`` | ``hold`` for one agent node.

Concursus is a compiler, not a runtime governor. This module supplies the *author-declared*,
*create-time* trust vocabulary the deploy actuator consults **exactly once per node per deploy**
to decide whether a side-effecting agent may stand up on its live (``DEFAULT``) endpoint, must
land on a non-default **shadow** endpoint instead, or must be **held** for escalation.

IDENTITY (non-negotiable): this gate fires ONCE at provision time for an author-declared node.
It is NEVER a per-invocation check, it NEVER re-earns or updates trust from a run outcome, and
it NEVER chooses among competing agents. :class:`TrustGrade` is a static seed the manifest
author declares; :func:`evaluate_deploy_gate` is a pure function of that seed plus the caller's
policy (``min_autonomy`` / ``require_approval``). No AWS, no state, pure stdlib.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Optional, Union

# Deploy modes the gate can return.
LIVE = "live"  # provision to the live DEFAULT endpoint (today's behavior)
SHADOW = "shadow"  # provision, but to a non-DEFAULT (shadow) endpoint — observed, not promoted
HOLD = "hold"  # do not provision; escalate for approval

# Endpoint qualifiers.
DEFAULT_QUALIFIER = "DEFAULT"
SHADOW_QUALIFIER = "SHADOW"


class TrustGrade(IntEnum):
    """The autonomy a manifest author declares (or an operator floor requires) for a node.

    Ordered L0 < L1 < L2 < L3. ``L0_SHADOW`` is "cleared to run, but only in shadow" — a
    side-effecting agent seeded at L0 that clears the caller's floor still deploys to a
    non-default endpoint rather than going live. ``L3_AUTONOMOUS`` is full live autonomy.
    """

    L0_SHADOW = 0
    L1_CANARY = 1
    L2_GUARDED = 2
    L3_AUTONOMOUS = 3

    @classmethod
    def parse(cls, value: Union["TrustGrade", int, str]) -> "TrustGrade":
        """Coerce an int (0-3), a grade name (``L0_SHADOW``/``L0``/``SHADOW``), or a
        :class:`TrustGrade` into a :class:`TrustGrade`. Raises ``ValueError`` on anything else."""
        if isinstance(value, TrustGrade):
            return value
        if isinstance(value, bool):  # bool is an int subclass — reject it explicitly
            raise ValueError(f"invalid trust_seed {value!r} (expected a grade, got a bool)")
        if isinstance(value, int):
            return cls(value)  # raises ValueError if out of the 0-3 range
        if isinstance(value, str):
            key = value.strip().upper()
            if key in cls.__members__:
                return cls[key]
            if key.isdigit():
                return cls(int(key))
            for member in cls:
                short, _, long = member.name.partition("_")
                if key in (short, long):
                    return member
        raise ValueError(f"invalid trust_seed {value!r} (expected a TrustGrade, int, or name)")


@dataclass(frozen=True)
class GateDecision:
    """The outcome of the create-time deploy gate for one node.

    Attributes:
        mode: :data:`LIVE`, :data:`SHADOW`, or :data:`HOLD`.
        qualifier: The endpoint qualifier to deploy to (``DEFAULT`` for live, ``SHADOW`` for a
            shadow deploy, ``None`` for a held node — nothing is deployed).
        reason: A human-legible explanation (empty for a plain live deploy).
    """

    mode: str
    qualifier: Optional[str]
    reason: str = ""


def evaluate_deploy_gate(
    *,
    side_effecting: bool,
    trust_seed: TrustGrade,
    min_autonomy: Optional[TrustGrade] = None,
    require_approval: bool = False,
) -> GateDecision:
    """Decide ``live`` | ``shadow`` | ``hold`` for one node at create time (pure).

    Rules (evaluated once per deploy, never per invocation):

    - A non-side-effecting agent is never gated — always :data:`LIVE`.
    - With no caller policy (``min_autonomy`` is ``None`` **and** ``require_approval`` is
      ``False``), the result is :data:`LIVE` — this keeps today's deploy byte-for-byte unchanged.
    - ``require_approval`` holds any side-effecting node for explicit approval (:data:`HOLD`).
    - A side-effecting node whose ``trust_seed`` is below ``min_autonomy`` is held (:data:`HOLD`).
    - A side-effecting node that clears the floor but is only seeded ``L0_SHADOW`` deploys to a
      non-default endpoint (:data:`SHADOW`) — "cleared, but not live".
    - Otherwise the node deploys live (:data:`LIVE`).
    """
    if not side_effecting:
        return GateDecision(LIVE, DEFAULT_QUALIFIER)
    if min_autonomy is None and not require_approval:
        return GateDecision(LIVE, DEFAULT_QUALIFIER)
    if require_approval:
        return GateDecision(
            HOLD,
            None,
            f"side-effecting agent held: --require-approval set "
            f"(trust_seed={trust_seed.name})",
        )
    assert min_autonomy is not None  # narrowed by the guards above
    if trust_seed < min_autonomy:
        return GateDecision(
            HOLD,
            None,
            f"side-effecting agent held: trust_seed {trust_seed.name} is below the required "
            f"min_autonomy {min_autonomy.name}",
        )
    if trust_seed <= TrustGrade.L0_SHADOW:
        return GateDecision(
            SHADOW,
            SHADOW_QUALIFIER,
            f"side-effecting agent cleared but not live (trust_seed {trust_seed.name}); "
            "deploying to the shadow endpoint",
        )
    return GateDecision(LIVE, DEFAULT_QUALIFIER)
