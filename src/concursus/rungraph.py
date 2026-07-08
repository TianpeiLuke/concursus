"""The **run graph** — a queryable dependency graph rebuilt from recorded AgentRef edges.

Where the StateStore records each validated node output plus the resolved ``consumes`` edges
(``"producer:$.path"`` strings), this layer projects those records into a plain directed
graph and answers the structural questions the supervisor needs: transitive
:meth:`~RunGraph.upstream` / :meth:`~RunGraph.downstream` (the blast radius that must re-run
when a node changes), a pre-dispatch structural :meth:`~RunGraph.validate` (cycles + dangling
AgentRefs — the complement of ``supervisor.validate_output``), and a bounded, nearest-first
:meth:`~RunGraph.context_order` for graph-aware context assembly. Pure Python — no networkx,
no AWS.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Set, Tuple

Edge = Tuple[str, str, str]  # (producer, consumer, jsonpath)


class RunGraphError(ValueError):
    """Raised when a run graph is structurally invalid (a cycle or a dangling AgentRef)."""


# -- run graph --------------------------------------------------------------
@dataclass
class RunGraph:
    """A directed graph of a run's data dependencies, projected from StateStore records.

    Attributes:
        nodes: Every node id — each record's node plus every producer it references.
        edges: ``(producer, consumer, jsonpath)`` triples, one per resolved AgentRef.
    """

    nodes: Set[str] = field(default_factory=set)
    edges: List[Edge] = field(default_factory=list)

    # -- construction -------------------------------------------------------
    @classmethod
    def from_records(cls, records: Iterable[Any]) -> "RunGraph":
        """Build the graph from StateStore records (duck-typed on ``.node`` + ``.consumes``).

        Each record contributes its ``node`` as a node; every ``consumes`` entry
        ``"producer:$.path"`` is split on its FIRST ``":"`` into ``(producer, path)`` and
        yields the edge ``(producer, node, path)``. Referenced producers are added as nodes
        too, so a consumer that names an unwritten producer still surfaces (see
        :meth:`validate`).
        """
        nodes: Set[str] = set()
        edges: List[Edge] = []
        for record in records:
            consumer = record.node
            nodes.add(consumer)
            for ref in record.consumes:
                producer, _, path = ref.partition(":")
                edges.append((producer, consumer, path))
                nodes.add(producer)
        return cls(nodes=nodes, edges=edges)

    @classmethod
    def from_edges(cls, nodes: Iterable[str], edges: Iterable[Edge]) -> "RunGraph":
        """Build the graph directly from an explicit node set and ``(producer, consumer, path)`` edges."""
        return cls(nodes=set(nodes), edges=[(p, c, path) for p, c, path in edges])

    # -- adjacency ----------------------------------------------------------
    def _children(self) -> Dict[str, Set[str]]:
        """Forward adjacency ``{producer: {consumers}}`` (deduped)."""
        adj: Dict[str, Set[str]] = {}
        for producer, consumer, _ in self.edges:
            adj.setdefault(producer, set()).add(consumer)
        return adj

    def _parents(self) -> Dict[str, Set[str]]:
        """Reverse adjacency ``{consumer: {producers}}`` (deduped)."""
        adj: Dict[str, Set[str]] = {}
        for producer, consumer, _ in self.edges:
            adj.setdefault(consumer, set()).add(producer)
        return adj

    @staticmethod
    def _reachable(start: str, adjacency: Dict[str, Set[str]]) -> Set[str]:
        """Transitive closure of ``start`` over ``adjacency`` (excluding ``start`` itself)."""
        seen: Set[str] = set()
        stack = [n for n in adjacency.get(start, ())]
        while stack:
            node = stack.pop()
            if node == start or node in seen:
                continue
            seen.add(node)
            stack.extend(adjacency.get(node, ()))
        return seen

    # -- queries ------------------------------------------------------------
    def upstream(self, node: str) -> Set[str]:
        """Transitive producers (ancestors) of ``node`` via the reverse adjacency."""
        return self._reachable(node, self._parents())

    def downstream(self, node: str) -> Set[str]:
        """Transitive consumers (descendants) of ``node`` via the forward adjacency."""
        return self._reachable(node, self._children())

    # -- structural gate ----------------------------------------------------
    def validate(self) -> None:
        """Assert the graph is a valid DAG with no dangling edges.

        Raises :class:`RunGraphError` if any edge names a producer absent from :attr:`nodes`
        (a dangling AgentRef), or if the graph contains a cycle. This is the structural
        complement to output-schema validation: nothing should dispatch until it passes.
        """
        for producer, consumer, _ in self.edges:
            if producer not in self.nodes:
                raise RunGraphError(
                    f"dangling edge: {consumer!r} consumes unknown producer {producer!r} "
                    "(no such node)"
                )
        if self._has_cycle():
            raise RunGraphError(
                "run graph contains a cycle; dependency graphs must be acyclic"
            )

    def _has_cycle(self) -> bool:
        """Detect a cycle among :attr:`nodes` via Kahn's algorithm (matching ``dag.py``)."""
        children = self._children()
        indeg: Dict[str, int] = {n: 0 for n in self.nodes}
        for node in self.nodes:
            for consumer in children.get(node, ()):
                if consumer in indeg:
                    indeg[consumer] += 1
        ready = [n for n, d in indeg.items() if d == 0]
        visited = 0
        while ready:
            node = ready.pop()
            visited += 1
            for consumer in children.get(node, ()):
                if consumer in indeg:
                    indeg[consumer] -= 1
                    if indeg[consumer] == 0:
                        ready.append(consumer)
        return visited != len(self.nodes)

    # -- context assembly ---------------------------------------------------
    def context_order(
        self, node: str, *, max_depth: int = 2, max_nodes: int = 20
    ) -> List[str]:
        """Producers relevant to ``node``, nearest-first, deduped, and bounded.

        A breadth-first walk over the reverse adjacency (direct producers first, then their
        producers, …), excluding ``node`` itself, sorted within each hop for determinism,
        capped at ``max_depth`` hops and ``max_nodes`` results. Pure Python — the app-layer
        traversal the Memory-backed store has no recursive query for.
        """
        parents = self._parents()
        result: List[str] = []
        seen: Set[str] = {node}
        frontier = [node]
        depth = 0
        while frontier and depth < max_depth:
            depth += 1
            next_frontier: List[str] = []
            for current in frontier:
                for producer in sorted(parents.get(current, ())):
                    if producer in seen:
                        continue
                    if len(result) >= max_nodes:
                        return result
                    seen.add(producer)
                    result.append(producer)
                    next_frontier.append(producer)
            frontier = next_frontier
        return result
