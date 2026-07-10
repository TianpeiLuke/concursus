"""The **RunIndex** — traverse the Folgezettel execution tree and query run state by metadata.

A run's :class:`~concursus.statestore.StateStore` log is a flat list of :class:`Record`s. This
module derives two orthogonal indexes over it (rebuildable, like the slipbox's ``unified.db`` +
``entry_folgezettel_trails`` master index):

- a **metadata index** — inverted postings over the typed metadata (``node`` / ``status`` /
  ``record_type`` / ``schema`` / ``producer``), so ``query(status="failed")`` is a lookup, not a
  payload scan (the local analogue of AgentCore ``list_events`` metadata filters); and
- a **Folgezettel tree** over each record's materialized-path ``address`` (default the ``node``
  name; a retry / fan-out / branch appends a ``"/"`` segment). The parent is prefix-derivable
  (strip the last segment), so :meth:`ancestors` / :meth:`descendants` / :meth:`children` /
  :meth:`siblings` / :meth:`traverse` reconstruct the retry/fan-out/branch execution tree from
  addresses alone — the run-state analogue of the ``slipbox-traverse-folgezettel`` skill.

This is a distinct structure from :class:`~concursus.rungraph.RunGraph`: RunGraph is the *data*
dependency DAG (producer→consumer via ``AgentRef``); RunIndex is the *execution* tree (a node's
retries/fan-outs/branches) plus the metadata query surface. Pure-Python, no third-party deps.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Set

from .statestore import Record, _ADDR_SEP, _is_newer

# The metadata fields the inverted index covers (queryable without deserializing payloads).
INDEXED_FIELDS = ("node", "status", "record_type", "schema", "producer")


class RunIndexError(ValueError):
    """Raised by :meth:`RunIndex.validate` when the materialized-path addresses violate
    concursus's honest-tree invariants (an orphaned sub-address, an unknown root segment, or —
    when requested — a non-contiguous attempt sequence)."""


def address_of(record: Record) -> str:
    """The record's Folgezettel address — its explicit ``address`` or, by default, its ``node``."""
    return record.address or record.node


class RunIndex:
    """A dual index over a run's records: metadata query + Folgezettel-tree traversal."""

    SEP = _ADDR_SEP

    def __init__(self, records: Iterable[Record]) -> None:
        self._records: List[Record] = list(records)
        self._by_field: Dict[str, Dict[str, List[Record]]] = {f: {} for f in INDEXED_FIELDS}
        self._by_address: Dict[str, List[Record]] = {}
        self._addresses: Set[str] = set()  # every address AND all its ancestor prefixes
        for r in self._records:
            for f in INDEXED_FIELDS:
                value = getattr(r, f, None)
                if value is not None:
                    self._by_field[f].setdefault(value, []).append(r)
            addr = address_of(r)
            self._by_address.setdefault(addr, []).append(r)
            parts = addr.split(self.SEP)
            for i in range(1, len(parts) + 1):
                self._addresses.add(self.SEP.join(parts[:i]))

    @classmethod
    def from_records(cls, records: Iterable[Record]) -> "RunIndex":
        return cls(records)

    @classmethod
    def from_store(cls, store) -> "RunIndex":
        """Build the index over a :class:`StateStore`'s current log (duck-typed ``.records()``)."""
        return cls(store.records())

    # -- metadata query -----------------------------------------------------
    def query(self, **filters) -> List[Record]:
        """Records matching every filter (AND). Indexed fields use the inverted postings; any
        other field falls back to a linear scan. Returns log order."""
        matched: Optional[Set[int]] = None
        residual: Dict[str, object] = {}
        for key, value in filters.items():
            if key in INDEXED_FIELDS:
                ids = {id(r) for r in self._by_field[key].get(value, [])}
                matched = ids if matched is None else (matched & ids)
            else:
                residual[key] = value
        pool = (
            self._records
            if matched is None
            else [r for r in self._records if id(r) in matched]
        )
        if residual:
            pool = [
                r for r in pool if all(getattr(r, k, None) == v for k, v in residual.items())
            ]
        return pool

    def by(self, field_name: str) -> Dict[str, List[Record]]:
        """The inverted postings for one indexed field (``value -> [records]``)."""
        return {k: list(v) for k, v in self._by_field.get(field_name, {}).items()}

    def latest(self, node: str, *, status: Optional[str] = "validated") -> Optional[Record]:
        """The newest record for ``node`` (optionally filtered to a ``status``); ``None`` if none."""
        best: Optional[Record] = None
        for r in self._by_field["node"].get(node, []):
            if status is not None and r.status != status:
                continue
            if _is_newer(r, best):
                best = r
        return best

    def nodes(self) -> Set[str]:
        """Every node id present in the log."""
        return set(self._by_field["node"].keys())

    # -- structural layout guard --------------------------------------------
    def validate(self, *, check_attempts: bool = False) -> "RunIndex":
        """Assert concursus's honest-tree invariants over the materialized-path addresses.

        Two invariants always hold on a well-formed run:

        1. **No orphaned sub-address.** Every NON-root record address's parent-prefix (strip the
           last ``"/"`` segment) must correspond to a REAL record's address — not a bare prefix
           :meth:`__init__` synthesized for a retry/fan-out sub-address whose parent never
           executed. (``_addresses`` back-fills ancestor prefixes for traversal; a prefix with no
           record in ``_by_address`` means the tree was addressed dishonestly.)
        2. **Known root.** Every root segment (the first path segment of every address) must name
           a known node — a materialized path may only be rooted at an executed DAG node.

        With ``check_attempts=True`` it additionally asserts each node's attempts form a
        contiguous ``1..N`` sequence (no gap / no missing retry).

        Returns ``self`` for chaining (mirrors ``AgentDAG.validate`` / ``AgentManifest.validate``);
        raises :class:`RunIndexError` on the first violation. This is a static binding-INTEGRITY
        assertion over an ALREADY-materialized run — it reads addresses and fails; it never
        repairs, re-addresses, or mutates the index, and it deliberately carries NONE of
        HiveFleet's reserved-branch (``.1``/``.2``/``.3``) permission semantics (meaningless for
        concursus's single writer).
        """
        known_nodes = self.nodes()
        real = set(self._by_address)  # addresses carrying at least one real record

        for addr in sorted(real):
            root = addr.split(self.SEP, 1)[0]
            if root not in known_nodes:
                raise RunIndexError(
                    f"address {addr!r} is rooted at {root!r}, which is not a known node "
                    f"(known: {sorted(known_nodes)})"
                )
            if self.SEP in addr:
                parent = addr.rsplit(self.SEP, 1)[0]
                if parent not in real:
                    raise RunIndexError(
                        f"orphaned address {addr!r}: its parent {parent!r} has no record "
                        f"(a synthesized sub-address whose parent never executed)"
                    )

        if check_attempts:
            for node, recs in self._by_field["node"].items():
                attempts = sorted({r.attempt for r in recs})
                expected = list(range(1, len(attempts) + 1))
                if attempts != expected:
                    raise RunIndexError(
                        f"node {node!r} has non-contiguous attempts {attempts} "
                        f"(expected {expected})"
                    )
        return self

    # -- Folgezettel tree ---------------------------------------------------
    def addresses(self) -> List[str]:
        """Every address in the tree (records' addresses plus their ancestor prefixes), sorted."""
        return sorted(self._addresses)

    def record_at(self, address: str) -> Optional[Record]:
        """The newest record sitting exactly at ``address`` (``None`` if it is a bare prefix)."""
        best: Optional[Record] = None
        for r in self._by_address.get(address, []):
            if _is_newer(r, best):
                best = r
        return best

    def parent(self, address: str) -> Optional[str]:
        """The prefix-derivable parent address (``None`` for a root)."""
        if self.SEP not in address:
            return None
        return address.rsplit(self.SEP, 1)[0]

    def children(self, address: str) -> List[str]:
        """Direct child addresses (exactly one segment deeper)."""
        prefix = address + self.SEP
        depth = address.count(self.SEP) + 1
        return sorted(
            a for a in self._addresses if a.startswith(prefix) and a.count(self.SEP) == depth
        )

    def siblings(self, address: str) -> List[str]:
        """Addresses sharing this address's parent (excluding itself)."""
        parent = self.parent(address)
        pool = self.children(parent) if parent is not None else self.roots()
        return [a for a in pool if a != address]

    def ancestors(self, address: str) -> List[str]:
        """The ancestor chain, nearest parent first up to the root."""
        out: List[str] = []
        cur = address
        while self.SEP in cur:
            cur = cur.rsplit(self.SEP, 1)[0]
            out.append(cur)
        return out

    def descendants(self, address: str) -> List[str]:
        """Every address strictly below ``address`` (its whole subtree), sorted."""
        prefix = address + self.SEP
        return sorted(a for a in self._addresses if a.startswith(prefix))

    def subtree(self, address: str) -> List[str]:
        """``address`` (if present) plus all its descendants."""
        head = [address] if address in self._addresses else []
        return head + self.descendants(address)

    def roots(self) -> List[str]:
        """Top-level addresses (no parent) — one per executed DAG node, sorted."""
        return sorted(a for a in self._addresses if self.SEP not in a)

    def leaves(self) -> List[str]:
        """Addresses with no children (the tips of the execution tree)."""
        return sorted(a for a in self._addresses if not self.children(a))

    def traverse(self, address: str) -> Dict[str, List[str]]:
        """The full neighbourhood of ``address`` (the ``traverse-folgezettel`` analogue): its
        root-first ancestor chain, direct children, whole descendant subtree, and siblings."""
        return {
            "ancestors": list(reversed(self.ancestors(address))),  # root-first
            "children": self.children(address),
            "descendants": self.descendants(address),
            "siblings": self.siblings(address),
        }


class PrecedentIndex:
    """A read-only, cross-run retrieval index over distilled precedent notes (the AI-16 in-process
    analogue of :func:`~concursus.distill.render_precedent_hub`).

    Where :class:`RunIndex` indexes ONE run's log, this indexes the SET of per-run precedent
    payloads — one entry per run/family, keyed by ``trail_id`` — so accumulated runs become
    retrievable precedent (query by ``status``, look up one run, list all). It is a pure
    projection: it reads the precedent notes and selects/queries, never a live router or scheduler
    (it starts and seeds no run). Deleting the notes empties it; nothing is a source of truth here.
    """

    def __init__(self, precedents: Iterable[Dict[str, object]]) -> None:
        self._by_trail: Dict[str, Dict[str, object]] = {}
        for payload in precedents:
            tid = str(payload.get("trail_id") or "")
            if tid:
                self._by_trail[tid] = dict(payload)

    @classmethod
    def from_vault(cls, vault_path) -> "PrecedentIndex":
        """Build the index over every precedent note under ``<vault>/precedents/`` (source of truth)."""
        from .distill import _precedents_by_trail

        return cls(_precedents_by_trail(vault_path).values())

    def trails(self) -> List[str]:
        """Every distilled run/family id, sorted."""
        return sorted(self._by_trail)

    def get(self, trail_id: str) -> Optional[Dict[str, object]]:
        """The precedent payload for one run/family (``None`` if not distilled)."""
        return self._by_trail.get(trail_id)

    def query(self, *, status: Optional[str] = None) -> List[Dict[str, object]]:
        """Precedent payloads, optionally filtered to a run ``status`` (``completed`` / ``partial``
        / ``failed``); trail-id order."""
        out = [self._by_trail[t] for t in sorted(self._by_trail)]
        if status is not None:
            out = [p for p in out if p.get("status") == status]
        return out
