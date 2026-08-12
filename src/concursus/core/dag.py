"""The AgentDAG — a pure, backend-agnostic directed acyclic graph of agents/tasks.

Nodes are agent ids (strings); edges are data dependencies (``a -> b`` means *b depends
on a*). The DAG is the topology Concursus compiles into a dispatch order: a topological
sort gives a valid run order, independent nodes can run concurrently, and a join waits for
all inbound edges. This layer carries no AWS/AgentCore coupling — it is the reused core of
the design (the analog of cursus's ``PipelineDAG``).
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Set


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
        #: Per-node authoring attributes — ``{id: {task, inputs, outputs}}``.
        #: I/O is a NODE property, never an edge property: a node declares what it emits and what
        #: it consumes, and an edge asserts only adjacency. :func:`resolve.check_alignment` remains
        #: the sole authority that the producer's declared output satisfies the consumer's declared
        #: input. Kept in a parallel map so :attr:`nodes` still returns ``List[str]`` and every
        #: existing caller is untouched.
        self._attrs: Dict[str, dict] = {}

    # -- construction -------------------------------------------------------
    def add_node(
        self,
        name: str,
        *,
        task: "Optional[str]" = None,
        inputs: "Optional[Dict[str, Any]]" = None,
        outputs: "Optional[Dict[str, Any]]" = None,
    ) -> "AgentDAG":
        """Add a node, optionally with its authored task and declared I/O.

        All three attributes are OPTIONAL and default to absent, so ``add_node("x")`` behaves
        exactly as before. ``task`` is the per-node instruction the compiler vends to the agent.
        ``inputs`` / ``outputs`` are flat field maps in the form
        ``{field: {"type": ..., "content_type": ..., "required": ...}}`` — the same shape
        :func:`resolve._field_type` and ``AgentHarness._write_outputs`` read, so one declaration
        serves the edge gate, the artifact write, and the prompt.
        """
        if not isinstance(name, str) or not name.strip():
            raise DAGError("node name must be a non-empty string")
        self._nodes.add(name)
        attrs = {}
        if task is not None:
            attrs["task"] = str(task)
        if inputs:
            attrs["inputs"] = dict(inputs)
        if outputs:
            attrs["outputs"] = dict(outputs)
        if attrs:
            self._attrs.setdefault(name, {}).update(attrs)
        return self

    def node_attrs(self, name: str) -> dict:
        """The authored attributes for ``name`` — ``{}`` for a node added without any."""
        return dict(self._attrs.get(name, {}))

    @property
    def attrs(self) -> "Dict[str, dict]":
        """All authored node attributes, as ``{id: {task, inputs, outputs}}`` (copy)."""
        return {n: dict(a) for n, a in self._attrs.items()}

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

    # -- cycle analysis (ADDITIVE; does NOT change the acyclic default) -----
    def classify_cycle_edges(self) -> Set[tuple]:
        """Return the set of ``(from, to)`` edges that lie on a cycle — a pure, deterministic,
        ORDER-INDEPENDENT classification via iterative Tarjan SCC.

        An edge is a *cycle edge* (a legal "back-edge" candidate) iff its endpoints share a
        strongly-connected component of size > 1, plus any self-loop ``(n, n)``. All other edges
        are DAG (tree/forward/cross) edges. On an acyclic graph this returns the empty set.

        Why SCC and not a single-pass DFS back-edge walk?
            The strongly-connected components of a digraph are a UNIQUE partition of the nodes —
            they do not depend on where a traversal starts or the order neighbors are visited. So
            the returned edge set is *canonical*: the same graph always yields the same answer,
            regardless of ``add_node`` / ``add_edge`` insertion order. A single-pass DFS instead
            labels edges "back edges" relative to its own visit order, so two DFS runs from
            different roots can flag *different* edges of the very same cycle — a non-deterministic,
            root-dependent result unsuitable for a reproducible compile step. This method is
            implemented with an explicit work stack (iterative Tarjan) so it is also safe on deep
            or large topologies without hitting Python's recursion limit.

        This is purely READ-ONLY and additive: it does not mutate the graph and does NOT change
        :meth:`topological_sort` / :meth:`validate` / :attr:`nodes`. Cycle REJECTION stays the
        default — ``assemble`` / the governor still call :meth:`validate` at freeze time to reject
        any cyclic topology. This method is the opt-in hook they can instead call to *classify*
        the offending edges (e.g. to permit a declared, bounded back-edge) rather than fail; it
        never itself raises on a cycle.
        """
        # Build a deterministic adjacency over real nodes; peel off self-loops.
        adj: Dict[str, List[str]] = {n: [] for n in self._nodes}
        self_loops: Set[tuple] = set()
        for f, t in self._edges:
            if f == t:
                self_loops.add((f, t))
            elif f in adj:
                adj[f].append(t)
        for succ in adj.values():
            succ.sort()

        index_counter = 0
        index: Dict[str, int] = {}
        lowlink: Dict[str, int] = {}
        on_stack: Dict[str, bool] = {}
        scc_stack: List[str] = []
        scc_id: Dict[str, int] = {}
        scc_size: Dict[int, int] = {}
        next_scc = 0

        for start in sorted(self._nodes):
            if start in index:
                continue
            # Explicit work stack of (node, next-neighbor-position) — iterative Tarjan.
            work: List[list] = [[start, 0]]
            while work:
                frame = work[-1]
                node, pos = frame[0], frame[1]
                if pos == 0:
                    index[node] = index_counter
                    lowlink[node] = index_counter
                    index_counter += 1
                    scc_stack.append(node)
                    on_stack[node] = True
                neighbors = adj[node]
                recursed = False
                while pos < len(neighbors):
                    w = neighbors[pos]
                    pos += 1
                    if w not in index:
                        frame[1] = pos  # resume after this neighbor
                        work.append([w, 0])
                        recursed = True
                        break
                    if on_stack.get(w):
                        lowlink[node] = min(lowlink[node], index[w])
                if recursed:
                    continue
                # All neighbors explored: settle ``node``.
                if lowlink[node] == index[node]:
                    while True:
                        w = scc_stack.pop()
                        on_stack[w] = False
                        scc_id[w] = next_scc
                        scc_size[next_scc] = scc_size.get(next_scc, 0) + 1
                        if w == node:
                            break
                    next_scc += 1
                work.pop()
                if work:
                    parent = work[-1][0]
                    lowlink[parent] = min(lowlink[parent], lowlink[node])

        cycle_edges: Set[tuple] = set(self_loops)
        for f, t in self._edges:
            if f == t:
                continue  # already captured as a self-loop
            cid = scc_id.get(f)
            if cid is not None and cid == scc_id.get(t) and scc_size.get(cid, 0) > 1:
                cycle_edges.add((f, t))
        return cycle_edges

    # -- serialization ------------------------------------------------------
    def to_dict(self) -> dict:
        """Serialize to ``{nodes, edges}``, plus ``node_attrs`` only when some node has attributes.

        Omitting an empty ``node_attrs`` keeps the payload byte-identical for every DAG authored
        without per-node declarations, so existing snapshots and record artifacts do not move.
        """
        out: dict = {"nodes": self.nodes, "edges": [list(e) for e in self._edges]}
        if self._attrs:
            out["node_attrs"] = {n: dict(a) for n, a in self._attrs.items()}
        return out

    @classmethod
    def from_dict(cls, data: dict) -> "AgentDAG":
        """Lower ``{nodes, edges}`` into a DAG, accepting BOTH node forms.

        A node may be a bare ``"id"`` string (the historical form — no declarations) or a mapping
        ``{"id": ..., "task": ..., "inputs": {...}, "outputs": {...}}``. A bare string is exactly
        equivalent to a mapping carrying only ``id``, so an authoring model that ignores the richer
        contract still produces a valid plan. ``node_attrs`` is also accepted as a sidecar map for
        round-tripping :meth:`to_dict`.
        """
        dag = cls()
        for n in data.get("nodes", []):
            if isinstance(n, Mapping):
                node_id = n.get("id") or n.get("name")
                if not isinstance(node_id, str) or not node_id.strip():
                    raise DAGError(f"node mapping has no usable 'id': {dict(n)!r}")
                dag.add_node(
                    node_id,
                    task=n.get("task"),
                    inputs=n.get("inputs"),
                    outputs=n.get("outputs"),
                )
            else:
                dag.add_node(n)
        for node_id, attrs in (data.get("node_attrs") or {}).items():
            if node_id in dag._nodes and isinstance(attrs, Mapping):
                dag.add_node(
                    node_id,
                    task=attrs.get("task"),
                    inputs=attrs.get("inputs"),
                    outputs=attrs.get("outputs"),
                )
        for e in data.get("edges", []):
            dag.add_edge(e[0], e[1])
        return dag

    def __repr__(self) -> str:  # pragma: no cover
        return f"AgentDAG(nodes={len(self._nodes)}, edges={len(self._edges)})"
