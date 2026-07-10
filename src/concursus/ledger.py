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
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

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
        self._load()

    # -- persistence --------------------------------------------------------
    def _load(self) -> None:
        """Load rows from disk (a missing/empty/corrupt file yields an empty ledger)."""
        self._rows = []
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return  # treat an unreadable ledger as empty — it is disposable/rebuildable
        for raw in data.get("rows", []) if isinstance(data, dict) else []:
            if isinstance(raw, dict) and raw.get("name") and raw.get("fingerprint") is not None:
                self._rows.append(DeployRow.from_dict(raw))

    def _flush(self) -> None:
        """Write the whole ledger atomically (temp file in the same dir + ``os.replace``)."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": _LEDGER_VERSION,
            "rows": [row.to_dict() for row in self._rows],
        }
        text = json.dumps(payload, indent=2, sort_keys=True)
        tmp = self._path.with_name(self._path.name + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, self._path)

    # -- create-time queries ------------------------------------------------
    def lookup(self, name: str, fingerprint: str) -> Optional[DeployRow]:
        """Return the newest row for ``(name, fingerprint)``, or ``None`` if never deployed.

        This is the *only* query the ledger answers — a create-time content-identity check. It
        re-reads the file first so a row written by another process/instance is visible.
        """
        self._load()
        for row in reversed(self._rows):
            if row.name == name and row.fingerprint == fingerprint:
                return row
        return None

    def has(self, name: str, fingerprint: str) -> bool:
        """True iff this exact content has already been stood up (see :meth:`lookup`)."""
        return self.lookup(name, fingerprint) is not None

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
