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
