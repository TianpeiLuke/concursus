"""The **deploy ledger** — a persisted, fingerprint-keyed record of what has been stood up.

Concursus is a compiler; its records are the source of truth and its derived artifacts are
disposable. This module adds a small, JSON-backed ledger that answers **one** create-time
question: *"have I already stood up this exact content (name + hosting fingerprint)?"* If yes,
deploy can skip the build + ``CreateAgentRuntime`` and report ``action="reused"`` — even across
separate CLI invocations, because the answer lives on disk.

IDENTITY (non-negotiable): this is **persistence-only**, modeled on a registry's persistence
tier and nothing more. It deliberately DROPS ``lookup(capability)`` / ``resolve(consumer,
rights)`` / ``get_trust`` — it never answers "which standing agent can do task X?" (dispatch
time). It only answers the content-identity question at create time. The ledger is append-only
for audit (old rows for a name are retained, never overwritten); the newest matching row wins on
lookup. It is a rebuildable convenience over the run/deploy records — deleting the file loses no
canonical state. Pure stdlib (``json`` + atomic ``os.replace``); no AWS, no ``datetime.now()`` at
import — the ``deployed_at`` timestamp is always caller-supplied.

TYPED REJECTIONS + DESIRED-VS-CONFIRMED (additive; opt-in): alongside the confirmation rows above,
the ledger can also record **typed rejections** — a structured ``{node, code, reason,
confirmed_at}`` entry (``code`` ∈ ``unsupported | invalid | timeout | actuator_error``) keyed to a
plan node that was *not* stood up. This lets a caller run a **desired-vs-confirmed** reconcile
(:meth:`DeployLedger.reconcile`): given the plan's desired ``{node: fingerprint}``, it reports which
nodes are confirmed (a matching content row exists), which diverged, and *why* (the newest typed
rejection for the node, when one was recorded). Rejections are the same append-only, atomic,
rebuildable-convenience discipline as confirmations — they are audit/projection, never a second
authoritative copy of run state. The reuse key itself is single-sourced through
:func:`deploy_identity` so the confirmation lookup and the reconcile query can never drift apart.
The default (no-rejection) code path is byte-for-byte unchanged: the ``rejections`` key is written
only when at least one rejection exists.

TWO-PHASE CRASH-SAFE ACTUATION (additive; opt-in): a third append-only log records **reservations**
— a ``{node, fingerprint, runtime_name, status, arn, at}`` entry per phase transition of a two-phase
``CreateAgentRuntime``. The actuator writes a ``reserving`` entry BEFORE the AWS call, then a
``confirmed`` entry (with the real ARN) after it returns; a crash in between leaves the ``reserving``
entry as the newest for its key — a *dangling reservation* that :meth:`DeployLedger.pending_reservations`
surfaces so the next deploy's reconciler can either adopt the runtime created under the deterministic
``runtime_name`` (append ``confirmed``) or compensate it (append ``compensated``). Same append-only,
atomic, rebuildable-convenience discipline as confirmations/rejections; the ``reservations`` key is
written only when at least one exists, so the default path stays byte-for-byte unchanged.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# The persisted schema version — bumped if the row shape changes (audit rows are retained as-is).
_LEDGER_VERSION = 1

# The columns one ledger row carries. ``(name, fingerprint)`` is the identity key.
_ROW_FIELDS = (
    "name",
    "fingerprint",
    "arn",
    "image_uri",
    "role_arn",
    "deployed_at",
    "action",
)

# The columns one typed-rejection entry carries (``(node, code)`` is not an identity — a node may
# be rejected more than once; the newest entry for a node wins on :meth:`why_rejected`).
_REJECTION_FIELDS = (
    "node",
    "code",
    "reason",
    "confirmed_at",
)

# The columns one two-phase reservation entry carries (``(node, fingerprint)`` is the reservation
# key; a node/content may be reserved more than once, and the newest entry for that key decides
# whether it is still pending — see :meth:`DeployLedger.pending_reservations`). ``at`` is the
# caller-supplied timestamp of THIS entry (reserve/confirm/compensate), never a clock read.
_RESERVATION_FIELDS = (
    "node",
    "fingerprint",
    "runtime_name",
    "status",
    "arn",
    "at",
)

# The four typed rejection codes. A caller MUST use one of these — an unknown code is coerced to
# ``actuator_error`` (the catch-all) so a projection over the ledger can rely on a closed set.
REJECT_UNSUPPORTED = "unsupported"
REJECT_INVALID = "invalid"
REJECT_TIMEOUT = "timeout"
REJECT_ACTUATOR_ERROR = "actuator_error"
REJECTION_CODES = (
    REJECT_UNSUPPORTED,
    REJECT_INVALID,
    REJECT_TIMEOUT,
    REJECT_ACTUATOR_ERROR,
)

# The three two-phase reservation statuses. A ``reserving`` entry is written BEFORE the actuator is
# called; on success it is superseded by a ``confirmed`` entry carrying the real ARN; on a crash it
# is left dangling for the next deploy's reconciler, which either adopts (``confirmed``) or clears
# it (``compensated``). ``reserving`` is the only non-terminal status — the reconciler acts only on
# the newest-status-per-key that is still ``reserving`` (see :meth:`DeployLedger.pending_reservations`).
RESERVE_RESERVING = "reserving"
RESERVE_CONFIRMED = "confirmed"
RESERVE_COMPENSATED = "compensated"
RESERVATION_STATUSES = (
    RESERVE_RESERVING,
    RESERVE_CONFIRMED,
    RESERVE_COMPENSATED,
)


def content_reuse_allowed(context_mode: str = "") -> bool:
    """Whether a resolved content-reuse policy PERMITS reusing an already-stood-up node.

    The reuse path (:meth:`DeployLedger.lookup` / :meth:`DeployLedger.has`) consults this before it
    honors a matching content row. Only the EXPLICIT literal ``"isolation"`` refuses reuse (forcing a
    re-provision of that node); every other value — including the empty default ``""`` (no policy
    given) and ``"reuse"`` — permits it. The empty default is intentional: an existing caller that
    passes no policy is byte-for-byte unchanged (a matching row is still reused). The refusal is thus
    gated behind an EXPLICITLY-resolved ``"isolation"`` reaching the ledger (typically via
    :func:`~concursus.core.resolve.resolve_context_mode`), never the inherited default — so
    existing call sites and tests keep today's reuse behavior. Pure: a total function of its input.
    """
    return context_mode != "isolation"


def deploy_identity(name: str, fingerprint: str) -> Tuple[str, str]:
    """The single, canonical reuse key for a deployed node: ``(name, fingerprint)``.

    This is the ONE source of the content-identity used both to confirm reuse (:meth:`DeployLedger.
    lookup`) and to reconcile desired-vs-confirmed (:meth:`DeployLedger.reconcile`) — so those two
    queries can never drift on how a node's identity is computed. It intentionally does not fold in
    a clock, an ARN, or any dispatch-time selector: identity is *content only* (the name plus the
    hosting fingerprint produced by :func:`concursus.build.build.fingerprint`). Pure,
    offline, and stable (equal inputs → equal key).
    """
    return (str(name), str(fingerprint))


@dataclass
class DeployRow:
    """One append-only ledger row — a single ``CreateAgentRuntime`` outcome, keyed by content.

    ``(name, fingerprint)`` is the identity: a later deploy of the same name with the same
    hosting fingerprint is the *same content* and can be reused. ``deployed_at`` is a
    caller-supplied timestamp (ISO string or epoch) — the ledger never reads the clock itself.
    """

    name: str
    fingerprint: str
    arn: Optional[str] = None
    image_uri: Optional[str] = None
    role_arn: Optional[str] = None
    deployed_at: Optional[Union[str, int, float]] = None
    action: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DeployRow":
        return cls(**{k: data.get(k) for k in _ROW_FIELDS})


@dataclass
class DeployRejection:
    """One append-only typed-rejection entry — a plan node that was NOT stood up, and why.

    ``node`` is the plan node id the rejection is keyed to; ``code`` is one of
    :data:`REJECTION_CODES` (an unknown code is coerced to :data:`REJECT_ACTUATOR_ERROR`);
    ``reason`` is a free-text explanation; ``confirmed_at`` is a caller-supplied timestamp (ISO
    string or epoch) — the ledger never reads the clock itself. A node may be rejected more than
    once (retries, changed inputs); entries are retained for audit and the newest wins on
    :meth:`DeployLedger.why_rejected`.
    """

    node: str
    code: str
    reason: Optional[str] = None
    confirmed_at: Optional[Union[str, int, float]] = None

    def __post_init__(self) -> None:
        # Coerce to the closed set so a projection can rely on a known code (never crash on a typo).
        if self.code not in REJECTION_CODES:
            self.code = REJECT_ACTUATOR_ERROR

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DeployRejection":
        return cls(**{k: data.get(k) for k in _REJECTION_FIELDS})


@dataclass
class DeployReservation:
    """One append-only two-phase-actuation entry — a phase transition for a ``(node, fingerprint)``.

    Crash-safe actuation writes a ``status="reserving"`` entry BEFORE calling the actuator, then a
    ``status="confirmed"`` (carrying the real ``arn``) after it returns, or a
    ``status="compensated"`` when the next deploy's reconciler clears a dangling reservation. A crash
    between the reserve and the confirm leaves the ``reserving`` entry as the newest for its key —
    that is exactly the pending state :meth:`DeployLedger.pending_reservations` surfaces. ``runtime_name``
    is the deterministic AgentCore name the actuator would use, so the reconciler can look up (adopt)
    a runtime that the pre-crash actuator may already have created. ``at`` is a caller-supplied
    timestamp (ISO string or epoch) — the ledger never reads the clock itself.
    """

    node: str
    fingerprint: str
    runtime_name: Optional[str] = None
    status: str = RESERVE_RESERVING
    arn: Optional[str] = None
    at: Optional[Union[str, int, float]] = None

    def __post_init__(self) -> None:
        # Coerce to the closed set so a projection can rely on a known status (never crash on a typo).
        if self.status not in RESERVATION_STATUSES:
            self.status = RESERVE_RESERVING

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DeployReservation":
        return cls(**{k: data.get(k) for k in _RESERVATION_FIELDS})


@dataclass(frozen=True)
class Reconciliation:
    """The result of a desired-vs-confirmed reconcile (:meth:`DeployLedger.reconcile`).

    ``confirmed`` maps a node to the fingerprint the ledger has a confirmation row for (matching the
    desired fingerprint). ``diverged`` maps a node to *why* it is not confirmed: the newest typed
    :class:`DeployRejection` recorded for it, or ``None`` when the node was simply never stood up
    and never rejected (missing, no recorded reason). A pure projection — read-only over the ledger.
    """

    confirmed: Dict[str, str] = None  # type: ignore[assignment]
    diverged: Dict[str, Optional[DeployRejection]] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        # dataclass(frozen=True) blocks normal assignment; use object.__setattr__ for the defaults.
        if self.confirmed is None:
            object.__setattr__(self, "confirmed", {})
        if self.diverged is None:
            object.__setattr__(self, "diverged", {})

    @property
    def all_confirmed(self) -> bool:
        """True iff every desired node is confirmed (nothing diverged)."""
        return not self.diverged


class DeployLedger:
    """A persisted, fingerprint-keyed deploy ledger (persistence-only).

    Rows are loaded from ``path`` on construction and re-loaded transparently before each read
    so two :class:`DeployLedger` instances over the same file see each other's writes (the file
    is the source of truth, not the in-memory list). Writes are atomic (temp file in the same
    directory + ``os.replace``) and append-only — an existing row for a ``(name, fingerprint)``
    is retained for audit; the newest row wins on :meth:`lookup`.
    """

    def __init__(self, path: Union[str, Path]) -> None:
        self._path = Path(path)
        self._rows: List[DeployRow] = []
        self._rejections: List[DeployRejection] = []
        self._reservations: List[DeployReservation] = []
        self._load()

    # -- persistence --------------------------------------------------------
    def _load(self) -> None:
        """Load rows (and typed rejections + reservations) from disk; a missing/empty/corrupt file is empty."""
        self._rows = []
        self._rejections = []
        self._reservations = []
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return  # treat an unreadable ledger as empty — it is disposable/rebuildable
        if not isinstance(data, dict):
            return
        for raw in data.get("rows", []):
            if isinstance(raw, dict) and raw.get("name") and raw.get("fingerprint") is not None:
                self._rows.append(DeployRow.from_dict(raw))
        for raw in data.get("rejections", []):
            if isinstance(raw, dict) and raw.get("node") and raw.get("code") is not None:
                self._rejections.append(DeployRejection.from_dict(raw))
        for raw in data.get("reservations", []):
            if isinstance(raw, dict) and raw.get("node") and raw.get("fingerprint") is not None:
                self._reservations.append(DeployReservation.from_dict(raw))

    def _flush(self) -> None:
        """Write the whole ledger atomically (temp file in the same dir + ``os.replace``).

        The ``rejections``/``reservations`` keys are each emitted only when at least one exists, so a
        ledger that has never recorded one is byte-for-byte identical to the pre-feature format on disk.
        """
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload: Dict[str, Any] = {
            "version": _LEDGER_VERSION,
            "rows": [row.to_dict() for row in self._rows],
        }
        if self._rejections:
            payload["rejections"] = [r.to_dict() for r in self._rejections]
        if self._reservations:
            payload["reservations"] = [r.to_dict() for r in self._reservations]
        text = json.dumps(payload, indent=2, sort_keys=True)
        tmp = self._path.with_name(self._path.name + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, self._path)

    # -- create-time queries ------------------------------------------------
    def lookup(
        self, name: str, fingerprint: str, *, context_mode: str = ""
    ) -> Optional[DeployRow]:
        """Return the newest row for ``(name, fingerprint)``, or ``None`` if never deployed.

        This is the *only* create-time content-identity check. It re-reads the file first so a row
        written by another process/instance is visible. Identity is computed through the single
        canonical :func:`deploy_identity` key so it cannot drift from :meth:`reconcile`.

        ``context_mode`` (opt-in, default ``""``) is the caller's RESOLVED content-reuse policy for
        this node (see :func:`~concursus.core.resolve.resolve_context_mode`). When it is the
        explicit literal ``"isolation"`` the node is refused content-reuse — this returns ``None``
        even when a matching row exists, forcing a re-provision. Every other value (the empty default,
        ``"reuse"``) permits reuse via :func:`content_reuse_allowed`, so an existing caller that
        passes no policy is byte-for-byte unchanged.
        """
        if not content_reuse_allowed(context_mode):
            return None
        self._load()
        key = deploy_identity(name, fingerprint)
        for row in reversed(self._rows):
            if deploy_identity(row.name, row.fingerprint) == key:
                return row
        return None

    def has(self, name: str, fingerprint: str, *, context_mode: str = "") -> bool:
        """True iff this exact content has already been stood up (see :meth:`lookup`).

        Honors the same opt-in ``context_mode`` gate as :meth:`lookup`: an explicit ``"isolation"``
        refuses reuse and returns ``False`` even when a matching row exists.
        """
        return self.lookup(name, fingerprint, context_mode=context_mode) is not None

    # -- append ------------------------------------------------------------
    def record(
        self,
        *,
        name: str,
        fingerprint: str,
        deployed_at: Union[str, int, float],
        arn: Optional[str] = None,
        image_uri: Optional[str] = None,
        role_arn: Optional[str] = None,
        action: Optional[str] = None,
    ) -> DeployRow:
        """Append one deploy outcome and persist atomically; return the stored row.

        Append-only: an existing row for the same ``(name, fingerprint)`` is retained for audit
        rather than overwritten. ``deployed_at`` is required and caller-supplied — the ledger
        never calls the clock itself.
        """
        row = DeployRow(
            name=name,
            fingerprint=fingerprint,
            arn=arn,
            image_uri=image_uri,
            role_arn=role_arn,
            deployed_at=deployed_at,
            action=action,
        )
        self._load()  # fold in any concurrent writes before appending our own
        self._rows.append(row)
        self._flush()
        return row

    def rows(self) -> List[DeployRow]:
        """All rows in the ledger, oldest first (append-only audit history)."""
        self._load()
        return list(self._rows)

    # -- typed rejections (additive; opt-in) --------------------------------
    def record_rejection(
        self,
        *,
        node: str,
        code: str,
        confirmed_at: Union[str, int, float],
        reason: Optional[str] = None,
    ) -> DeployRejection:
        """Append one typed rejection for a plan ``node`` and persist atomically; return it.

        ``code`` must be one of :data:`REJECTION_CODES` (``unsupported | invalid | timeout |
        actuator_error``); an unrecognized code is coerced to :data:`REJECT_ACTUATOR_ERROR`.
        Append-only and audit-first, exactly like :meth:`record`: a node may be rejected more than
        once (retries, changed inputs) and every entry is retained; the newest wins on
        :meth:`why_rejected`. ``confirmed_at`` is required and caller-supplied — the ledger never
        reads the clock itself.
        """
        entry = DeployRejection(
            node=node,
            code=code,
            reason=reason,
            confirmed_at=confirmed_at,
        )
        self._load()  # fold in any concurrent writes before appending our own
        self._rejections.append(entry)
        self._flush()
        return entry

    def rejections(self) -> List[DeployRejection]:
        """All typed rejections in the ledger, oldest first (append-only audit history)."""
        self._load()
        return list(self._rejections)

    def why_rejected(self, node: str) -> Optional[DeployRejection]:
        """The newest typed rejection recorded for ``node``, or ``None`` if it was never rejected.

        Re-reads the file first so a rejection written by another process/instance is visible.
        """
        self._load()
        for entry in reversed(self._rejections):
            if entry.node == node:
                return entry
        return None

    # -- two-phase crash-safe actuation (additive; opt-in) ------------------
    def reserve(
        self,
        *,
        node: str,
        fingerprint: str,
        runtime_name: Optional[str],
        at: Union[str, int, float],
    ) -> DeployReservation:
        """PHASE 1 — append a ``status="reserving"`` entry BEFORE the actuator is called; return it.

        This durably records intent so that if the process crashes mid-actuation the next deploy's
        reconciler can find the dangling reservation (:meth:`pending_reservations`) and either adopt
        the runtime the actuator may already have created (under the deterministic ``runtime_name``)
        or compensate it away. Append-only and atomic, exactly like :meth:`record`. ``at`` is
        required and caller-supplied — the ledger never reads the clock itself.
        """
        entry = DeployReservation(
            node=node,
            fingerprint=fingerprint,
            runtime_name=runtime_name,
            status=RESERVE_RESERVING,
            arn=None,
            at=at,
        )
        self._load()  # fold in any concurrent writes before appending our own
        self._reservations.append(entry)
        self._flush()
        return entry

    def confirm_reservation(
        self,
        *,
        node: str,
        fingerprint: str,
        arn: Optional[str],
        at: Union[str, int, float],
        runtime_name: Optional[str] = None,
    ) -> DeployReservation:
        """PHASE 3 — append a ``status="confirmed"`` entry (carrying the real ``arn``); return it.

        Supersedes the earlier ``reserving`` entry for the same ``(node, fingerprint)`` key so the
        key is no longer pending (newest-status-per-key wins). Append-only: the ``reserving`` entry is
        retained for audit, never overwritten. ``at`` is required and caller-supplied.
        """
        entry = DeployReservation(
            node=node,
            fingerprint=fingerprint,
            runtime_name=runtime_name,
            status=RESERVE_CONFIRMED,
            arn=arn,
            at=at,
        )
        self._load()
        self._reservations.append(entry)
        self._flush()
        return entry

    def compensate_reservation(
        self,
        *,
        node: str,
        fingerprint: str,
        at: Union[str, int, float],
        runtime_name: Optional[str] = None,
        arn: Optional[str] = None,
    ) -> DeployReservation:
        """Recovery — append a ``status="compensated"`` entry clearing a dangling reservation; return it.

        The reconciler writes this when a ``reserving`` entry could NOT be adopted (no runtime was
        created under the deterministic name), releasing the dangling reservation so it is no longer
        pending. Append-only and atomic; ``at`` is required and caller-supplied.
        """
        entry = DeployReservation(
            node=node,
            fingerprint=fingerprint,
            runtime_name=runtime_name,
            status=RESERVE_COMPENSATED,
            arn=arn,
            at=at,
        )
        self._load()
        self._reservations.append(entry)
        self._flush()
        return entry

    def reservations(self) -> List[DeployReservation]:
        """All reservation entries in the ledger, oldest first (append-only audit history)."""
        self._load()
        return list(self._reservations)

    def pending_reservations(self) -> List[DeployReservation]:
        """The still-``reserving`` reservations — one per ``(node, fingerprint)`` key, oldest first.

        A key is *pending* iff its NEWEST reservation entry is ``reserving`` (a later ``confirmed`` or
        ``compensated`` entry for the same key resolves it). These are exactly the dangling
        reservations a crash left behind — what the reconciler must adopt or compensate. Re-reads the
        file first so a reservation written by another process/instance is visible. Pure projection.
        """
        self._load()
        newest: Dict[Tuple[str, str], DeployReservation] = {}
        for entry in self._reservations:
            newest[deploy_identity(entry.node, entry.fingerprint)] = entry
        pending = [e for e in self._reservations if newest[deploy_identity(e.node, e.fingerprint)] is e]
        return [e for e in pending if e.status == RESERVE_RESERVING]

    # -- desired-vs-confirmed reconcile (additive; opt-in) ------------------
    def reconcile(self, desired: Dict[str, str]) -> Reconciliation:
        """Reconcile a plan's desired ``{node: fingerprint}`` against what the ledger confirms.

        For each desired node, a confirmation row for its exact ``(node, fingerprint)`` content
        (via the single canonical :func:`deploy_identity` key — so this can never disagree with
        :meth:`lookup`) lands it in :attr:`Reconciliation.confirmed`. A node with no matching
        confirmation is *diverged*: it maps to the newest typed :class:`DeployRejection` recorded
        for it, or ``None`` when it was simply never stood up and never rejected. A pure projection
        over the append-only log — read-only, allocates nothing on disk.
        """
        self._load()
        confirmed: Dict[str, str] = {}
        diverged: Dict[str, Optional[DeployRejection]] = {}
        for node, fingerprint in desired.items():
            key = deploy_identity(node, fingerprint)
            if any(deploy_identity(r.name, r.fingerprint) == key for r in self._rows):
                confirmed[node] = fingerprint
            else:
                diverged[node] = self._latest_rejection(node)
        return Reconciliation(confirmed=confirmed, diverged=diverged)

    def _latest_rejection(self, node: str) -> Optional[DeployRejection]:
        """Newest in-memory rejection for ``node`` (no reload — callers reload first)."""
        for entry in reversed(self._rejections):
            if entry.node == node:
                return entry
        return None
