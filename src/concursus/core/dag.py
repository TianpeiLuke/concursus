"""The AgentDAG — a pure, backend-agnostic directed acyclic graph of agents/tasks.

Nodes are agent ids (strings); edges are data dependencies (``a -> b`` means *b depends
on a*). The DAG is the topology Concursus compiles into a dispatch order: a topological
sort gives a valid run order, independent nodes can run concurrently, and a join waits for
all inbound edges. This layer carries no AWS/AgentCore coupling — it is the reused core of
the design (the analog of cursus's ``PipelineDAG``).
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Set


class DAGError(ValueError):
    """Raised on an invalid DAG (unknown node, or a cycle)."""


class AgentDAG:
    """A directed acyclic graph whose nodes are agent/task ids.

    Example:
        >>> dag = AgentDAG()
        >>> for n in ["ingest", "summarize", "critique", "format"]:
        ...     dag.add_node(n)
        >>> dag.add_edge("ingest", "summarize")
        >>> dag.add_edge("summarize", "critique")
        >>> dag.add_edge("critique", "format")
        >>> dag.topological_sort()
        ['ingest', 'summarize', 'critique', 'format']
    """

    def __init__(self) -> None:
        self._nodes: Set[str] = set()
        self._edges: List[tuple] = []  # (from_node, to_node)

    # -- construction -------------------------------------------------------
    def add_node(self, name: str) -> "AgentDAG":
        if not isinstance(name, str) or not name.strip():
            raise DAGError("node name must be a non-empty string")
        self._nodes.add(name)
        return self

    def add_edge(self, from_node: str, to_node: str) -> "AgentDAG":
        """Add a data dependency ``from_node -> to_node`` (to_node depends on from_node)."""
        for n in (from_node, to_node):
            if n not in self._nodes:
                raise DAGError(f"unknown node: {n!r} (add_node first)")
        if from_node == to_node:
            raise DAGError(f"self-loop not allowed: {from_node!r}")
        if (from_node, to_node) not in self._edges:
            self._edges.append((from_node, to_node))
        return self

    # -- inspection ---------------------------------------------------------
    @property
    def nodes(self) -> List[str]:
        """Node ids in a valid dispatch (TOPOLOGICAL) order — producers before consumers.

        Ties among ready nodes break by name, so the order is deterministic. Falls back to a plain
        name-sort for a CYCLIC graph (so ``nodes`` never raises — ``to_dict`` / ``if not dag.nodes``
        stay safe on an invalid DAG; cycles are still rejected by :meth:`validate` /
        :meth:`topological_sort`). For a single node this equals ``[that_node]``.
        """
        try:
            return self.topological_sort()
        except DAGError:
            return sorted(self._nodes)

    @property
    def edges(self) -> List[tuple]:
        return list(self._edges)

    def get_dependencies(self, node: str) -> List[str]:
        """Direct upstream producers of ``node``."""
        return sorted(f for f, t in self._edges if t == node)

    def get_dependents(self, node: str) -> List[str]:
        """Direct downstream consumers of ``node``."""
        return sorted(t for f, t in self._edges if f == node)

    def sources(self) -> List[str]:
        """Nodes with no dependencies (entry points)."""
        return sorted(n for n in self._nodes if not self.get_dependencies(n))

    def sinks(self) -> List[str]:
        """Nodes with no dependents (terminals)."""
        return sorted(n for n in self._nodes if not self.get_dependents(n))

    # -- ordering -----------------------------------------------------------
    def topological_sort(self) -> List[str]:
        """Return a valid dispatch order (Kahn's algorithm). Raises on a cycle."""
        indeg: Dict[str, int] = {n: 0 for n in self._nodes}
        for _, t in self._edges:
            indeg[t] += 1
        ready = sorted(n for n, d in indeg.items() if d == 0)
        order: List[str] = []
        while ready:
            n = ready.pop(0)
            order.append(n)
            for t in self.get_dependents(n):
                indeg[t] -= 1
                if indeg[t] == 0:
                    ready.append(t)
            ready.sort()
        if len(order) != len(self._nodes):
            raise DAGError("DAG contains a cycle; agent topologies must be acyclic")
        return order

    def validate(self) -> "AgentDAG":
        """Assert the graph is a valid DAG (no cycles); returns self."""
        self.topological_sort()
        return self

    # -- serialization ------------------------------------------------------
    def to_dict(self) -> dict:
        return {"nodes": self.nodes, "edges": [list(e) for e in self._edges]}

    @classmethod
    def from_dict(cls, data: dict) -> "AgentDAG":
        dag = cls()
        for n in data.get("nodes", []):
            dag.add_node(n)
        for e in data.get("edges", []):
            dag.add_edge(e[0], e[1])
        return dag

    def __repr__(self) -> str:  # pragma: no cover
        return f"AgentDAG(nodes={len(self._nodes)}, edges={len(self._edges)})"
