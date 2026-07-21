"""Smoke tests for the Concursus declarative core."""

import pytest

from concursus import (
    MAX_SUPPORTED_CONTRACT_VERSION,
    AgentCapabilities,
    AgentDAG,
    AgentManifest,
    DAGError,
    ManifestError,
    __version__,
)
from concursus.governor.registry import _declared_capabilities


def test_version_resolves():
    assert __version__ and __version__ != "0.0.0"


def test_dag_topological_order():
    dag = AgentDAG()
    for n in ["ingest", "summarize", "critique", "format"]:
        dag.add_node(n)
    dag.add_edge("ingest", "summarize")
    dag.add_edge("summarize", "critique")
    dag.add_edge("critique", "format")
    assert dag.topological_sort() == ["ingest", "summarize", "critique", "format"]
    assert dag.sources() == ["ingest"]
    assert dag.sinks() == ["format"]
    assert dag.get_dependencies("critique") == ["summarize"]


def test_dag_detects_cycle():
    dag = AgentDAG()
    for n in ["a", "b"]:
        dag.add_node(n)
    dag.add_edge("a", "b")
    dag.add_edge("b", "a")
    with pytest.raises(DAGError):
        dag.topological_sort()


def test_dag_round_trips():
    dag = AgentDAG.from_dict({"nodes": ["a", "b"], "edges": [["a", "b"]]})
    assert dag.to_dict() == {"nodes": ["a", "b"], "edges": [["a", "b"]]}


def test_dag_rejects_unknown_edge_node():
    dag = AgentDAG().add_node("a")
    with pytest.raises(DAGError):
        dag.add_edge("a", "missing")


# -- classify_cycle_edges (ADDITIVE, opt-in cycle classification) -----------
def test_classify_cycle_edges_empty_on_acyclic():
    """A valid DAG has no cycle edges — and topological_sort/validate still succeed."""
    dag = AgentDAG()
    for n in ["a", "b", "c", "d"]:
        dag.add_node(n)
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    dag.add_edge("c", "d")
    assert dag.classify_cycle_edges() == set()
    # The acyclic default path is untouched.
    assert dag.topological_sort() == ["a", "b", "c", "d"]
    dag.validate()


def test_classify_cycle_edges_flags_two_cycle():
    """Both edges of a 2-node cycle share an SCC of size > 1 and are flagged."""
    dag = AgentDAG()
    for n in ["a", "b"]:
        dag.add_node(n)
    dag.add_edge("a", "b")
    dag.add_edge("b", "a")
    assert dag.classify_cycle_edges() == {("a", "b"), ("b", "a")}


def test_classify_cycle_edges_only_flags_scc_edges():
    """A cycle {a,b,c} with an acyclic tail c->d->e flags ONLY the three SCC edges."""
    dag = AgentDAG()
    for n in ["a", "b", "c", "d", "e"]:
        dag.add_node(n)
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    dag.add_edge("c", "a")  # closes the SCC {a, b, c}
    dag.add_edge("c", "d")  # tail out of the SCC
    dag.add_edge("d", "e")
    assert dag.classify_cycle_edges() == {("a", "b"), ("b", "c"), ("c", "a")}


def test_classify_cycle_edges_two_disjoint_sccs_excludes_bridge():
    """Independent SCCs are both flagged; a bridge edge between them is a cross edge, not flagged."""
    dag = AgentDAG()
    for n in ["a", "b", "x", "y", "z"]:
        dag.add_node(n)
    dag.add_edge("a", "b")
    dag.add_edge("b", "a")  # SCC {a, b}
    dag.add_edge("x", "y")
    dag.add_edge("y", "z")
    dag.add_edge("z", "x")  # SCC {x, y, z}
    dag.add_edge("b", "x")  # bridge — endpoints are in different SCCs
    assert dag.classify_cycle_edges() == {
        ("a", "b"),
        ("b", "a"),
        ("x", "y"),
        ("y", "z"),
        ("z", "x"),
    }


def test_classify_cycle_edges_is_order_independent():
    """The SCC partition is canonical: shuffling node/edge insertion order never changes
    the classified edge set (unlike a root-dependent single-pass DFS back-edge walk)."""
    import random

    nodes = ["a", "b", "c", "d", "e"]
    edges = [("a", "b"), ("b", "c"), ("c", "a"), ("c", "d"), ("d", "e")]

    def build(node_order, edge_order):
        g = AgentDAG()
        for n in node_order:
            g.add_node(n)
        for e in edge_order:
            g.add_edge(*e)
        return g

    expected = {("a", "b"), ("b", "c"), ("c", "a")}
    rng = random.Random(1234)
    for _ in range(50):
        no = nodes[:]
        eo = edges[:]
        rng.shuffle(no)
        rng.shuffle(eo)
        assert build(no, eo).classify_cycle_edges() == expected


def test_classify_cycle_edges_captures_self_loop():
    """A self-loop (which bypasses the add_edge guard, e.g. via a programmatic/deserialized
    build) is classified as a cycle edge even though its SCC has size 1."""
    dag = AgentDAG()
    for n in ["a", "b"]:
        dag.add_node(n)
    dag.add_edge("a", "b")
    dag._edges.append(("a", "a"))  # inject past the add_edge self-loop guard
    assert dag.classify_cycle_edges() == {("a", "a")}


def test_classify_cycle_edges_does_not_mutate_or_disturb_default():
    """It is read-only: edges/nodes are unchanged and validate() still REJECTS the cycle
    (rejection stays the default; classification is the opt-in freeze-time hook)."""
    dag = AgentDAG()
    for n in ["a", "b"]:
        dag.add_node(n)
    dag.add_edge("a", "b")
    dag.add_edge("b", "a")
    before_edges = dag.edges
    before_nodes = set(dag._nodes)
    dag.classify_cycle_edges()
    assert dag.edges == before_edges
    assert set(dag._nodes) == before_nodes
    with pytest.raises(DAGError):
        dag.validate()


def test_classify_cycle_edges_safe_on_deep_chain():
    """Iterative Tarjan handles a chain far deeper than Python's recursion limit."""
    n = 3000
    dag = AgentDAG()
    for i in range(n):
        dag.add_node(f"n{i}")
    for i in range(n - 1):
        dag.add_edge(f"n{i}", f"n{i + 1}")
    # No cycle yet.
    assert dag.classify_cycle_edges() == set()
    # Add one long back-edge; the tail nodes now form one SCC.
    dag.add_edge(f"n{n - 1}", f"n{n - 500}")
    cycle = dag.classify_cycle_edges()
    assert (f"n{n - 1}", f"n{n - 500}") in cycle
    assert len(cycle) == 500


def test_manifest_validates():
    m = AgentManifest.from_dict(
        {
            "name": "summarize",
            "registry": {
                "container_uri": "acct.dkr.ecr.us-east-1.amazonaws.com/x:latest",
                "protocol": "HTTP",
            },
            "contract": {
                "inputs": {"document": {"type": "string"}},
                "outputs": {"summary": {"type": "string"}},
            },
        }
    ).validate()
    assert m.protocol == "HTTP"
    assert "summary" in m.output_schema


def test_manifest_requires_output_schema():
    with pytest.raises(ManifestError):
        AgentManifest.from_dict(
            {"name": "x", "registry": {"container_uri": "y"}, "contract": {}}
        ).validate()


def test_manifest_requires_hosting_binding():
    with pytest.raises(ManifestError):
        AgentManifest.from_dict(
            {"name": "x", "contract": {"outputs": {"o": {"type": "string"}}}}
        ).validate()


# -- capabilities (OPTIONAL typed runtime-declaration block) ----------------
def _valid_manifest_dict(name="a", **extra):
    d = {
        "name": name,
        "registry": {"container_uri": "img", "protocol": "HTTP"},
        "contract": {"inputs": {}, "outputs": {"o": {"type": "string"}}},
    }
    d.update(extra)
    return d


def test_manifest_capabilities_default_is_empty_and_falsy():
    """No capabilities: block => empty AgentCapabilities that is falsy (byte-for-byte
    identical to before wherever a manifest's truthiness/capabilities are inspected)."""
    m = AgentManifest.from_dict(_valid_manifest_dict()).validate()
    assert isinstance(m.capabilities, AgentCapabilities)
    assert m.capabilities == AgentCapabilities()
    assert bool(m.capabilities) is False
    assert m.capabilities.to_dict() == {"features": [], "tools": [], "egress_hosts": []}


def test_manifest_parses_typed_capabilities():
    m = AgentManifest.from_dict(
        _valid_manifest_dict(
            capabilities={
                "features": ["stream"],
                "tools": ["search", "calc"],
                "egress_hosts": ["api.example.com"],
            }
        )
    ).validate()
    assert bool(m.capabilities) is True
    assert m.capabilities.features == ("stream",)
    assert m.capabilities.tools == ("search", "calc")
    assert m.capabilities.egress_hosts == ("api.example.com",)


def test_manifest_partial_capabilities_fill_defaults():
    m = AgentManifest.from_dict(
        _valid_manifest_dict(capabilities={"tools": ["search"]})
    ).validate()
    assert m.capabilities.tools == ("search",)
    assert m.capabilities.features == ()
    assert m.capabilities.egress_hosts == ()
    assert bool(m.capabilities) is True


def test_manifest_capabilities_unknown_key_rejected():
    with pytest.raises(ManifestError):
        AgentManifest.from_dict(_valid_manifest_dict(capabilities={"bogus": ["x"]}))


def test_manifest_capabilities_bare_string_rejected():
    """A bare string for a list-valued key is a common author mistake — reject it."""
    with pytest.raises(ManifestError):
        AgentManifest.from_dict(_valid_manifest_dict(capabilities={"tools": "search"}))


def test_manifest_capabilities_non_mapping_rejected():
    with pytest.raises(ManifestError):
        AgentManifest.from_dict(_valid_manifest_dict(capabilities=["stream"]))


def test_empty_capabilities_does_not_perturb_governor_capability_fallback():
    """The governor registry falls back to getattr(manifest, 'capabilities'); the empty
    default must stay falsy there so an agent still serves only its own name."""
    m = AgentManifest.from_dict(_valid_manifest_dict(name="ingest")).validate()
    # No registry.capabilities and a falsy typed field => governor sees no extra labels.
    assert _declared_capabilities(m) == set()


# -- contract_version (fail-closed forward-compat gate) ---------------------
def test_manifest_contract_version_defaults_to_max_supported():
    m = AgentManifest.from_dict(_valid_manifest_dict()).validate()
    assert m.contract_version == MAX_SUPPORTED_CONTRACT_VERSION


def test_manifest_accepts_supported_contract_version():
    m = AgentManifest.from_dict(
        _valid_manifest_dict(contract_version=MAX_SUPPORTED_CONTRACT_VERSION)
    ).validate()
    assert m.contract_version == MAX_SUPPORTED_CONTRACT_VERSION


def test_manifest_rejects_future_contract_version():
    with pytest.raises(ManifestError):
        AgentManifest.from_dict(
            _valid_manifest_dict(contract_version=MAX_SUPPORTED_CONTRACT_VERSION + 1)
        ).validate()


def test_manifest_rejects_non_int_contract_version():
    with pytest.raises(ManifestError):
        AgentManifest.from_dict(_valid_manifest_dict(contract_version="1")).validate()
    # bool is an int subclass but is not a valid schema revision.
    with pytest.raises(ManifestError):
        AgentManifest.from_dict(_valid_manifest_dict(contract_version=True)).validate()


# -- from_yaml keeps parsing the new fields (absent => defaults) ------------
def test_from_yaml_parses_new_fields_and_absent_defaults(tmp_path):
    with_fields = tmp_path / "svc.agent.yaml"
    with_fields.write_text(
        "registry:\n"
        "  container_uri: img\n"
        "  protocol: HTTP\n"
        "contract:\n"
        "  inputs: {}\n"
        "  outputs:\n"
        "    o:\n"
        "      type: string\n"
        "capabilities:\n"
        "  tools: [search]\n"
        "  egress_hosts: [api.example.com]\n"
        "contract_version: 1\n"
    )
    m = AgentManifest.from_yaml(str(with_fields)).validate()
    assert m.name == "svc"
    assert m.capabilities.tools == ("search",)
    assert m.capabilities.egress_hosts == ("api.example.com",)
    assert m.contract_version == 1

    # An existing manifest with neither field parses to the defaults, unchanged.
    plain = tmp_path / "plain.agent.yaml"
    plain.write_text(
        "registry:\n"
        "  container_uri: img\n"
        "contract:\n"
        "  outputs:\n"
        "    o:\n"
        "      type: string\n"
    )
    m2 = AgentManifest.from_yaml(str(plain)).validate()
    assert bool(m2.capabilities) is False
    assert m2.contract_version == MAX_SUPPORTED_CONTRACT_VERSION


# -- context_mode (optional per-agent content-reuse policy) -----------------
def test_manifest_context_mode_defaults_to_empty_inherit():
    m = AgentManifest.from_dict(_valid_manifest_dict()).validate()
    assert m.context_mode == ""


def test_manifest_accepts_valid_context_modes():
    for mode in ("", "reuse", "isolation"):
        m = AgentManifest.from_dict(_valid_manifest_dict(context_mode=mode)).validate()
        assert m.context_mode == mode


def test_manifest_rejects_invalid_context_mode():
    with pytest.raises(ManifestError):
        AgentManifest.from_dict(_valid_manifest_dict(context_mode="recycle")).validate()


def test_from_yaml_parses_context_mode(tmp_path):
    p = tmp_path / "ctx.agent.yaml"
    p.write_text(
        "registry:\n"
        "  container_uri: img\n"
        "contract:\n"
        "  outputs:\n"
        "    o:\n"
        "      type: string\n"
        "context_mode: isolation\n"
    )
    m = AgentManifest.from_yaml(str(p)).validate()
    assert m.context_mode == "isolation"
