"""Tests for the dependency resolver — extract, edge compilation, and type-gating."""

import pytest

from concursus import AgentDAG, AgentManifest
from concursus.core.resolve import (
    AgentRef,
    AlignmentError,
    check_alignment,
    extract,
    resolve_edges,
)


# -- extract ----------------------------------------------------------------
def test_extract_dollar_field():
    assert extract({"summary": "hi"}, "$.summary") == "hi"


def test_extract_dotted():
    assert extract({"a": {"b": {"c": 1}}}, "$.a.b.c") == 1


def test_extract_list_index():
    assert extract({"a": {"b": [10, 20, 30]}}, "$.a.b[1]") == 20


def test_extract_without_dollar_prefix():
    assert extract({"a": 1}, "a") == 1


def test_extract_bare_dollar_returns_whole_object():
    obj = {"a": 1, "b": 2}
    assert extract(obj, "$") == obj


def test_extract_missing_key_raises_keyerror():
    with pytest.raises(KeyError):
        extract({"a": 1}, "$.b")


def test_extract_index_out_of_range_raises_indexerror():
    with pytest.raises(IndexError):
        extract({"a": [1]}, "$.a[5]")


# -- fixtures ---------------------------------------------------------------
def _chain():
    """A well-formed 3-node chain: ingest -> summarize -> critique.

    ``ingest`` uses a flat output schema; ``summarize`` uses the nested
    ``{"properties": {...}}`` shape — exercising both accepted forms.
    """
    dag = AgentDAG()
    for n in ["ingest", "summarize", "critique"]:
        dag.add_node(n)
    dag.add_edge("ingest", "summarize")
    dag.add_edge("summarize", "critique")

    manifests = {
        "ingest": AgentManifest.from_dict(
            {
                "name": "ingest",
                "registry": {"container_uri": "x", "protocol": "HTTP"},
                "contract": {
                    "inputs": {"uri": {"type": "string"}},
                    "outputs": {"document": {"type": "string"}},
                },
            }
        ),
        "summarize": AgentManifest.from_dict(
            {
                "name": "summarize",
                "registry": {"container_uri": "x", "protocol": "HTTP"},
                "contract": {
                    "inputs": {"document": {"type": "string"}},
                    "outputs": {"properties": {"summary": {"type": "string"}}},
                },
                "spec": {"depends_on": [{"from": "ingest.document", "to": "document"}]},
            }
        ),
        "critique": AgentManifest.from_dict(
            {
                "name": "critique",
                "registry": {"container_uri": "x", "protocol": "HTTP"},
                "contract": {
                    "inputs": {"summary": {"type": "string"}},
                    "outputs": {"critique": {"type": "string"}},
                },
                "spec": {"depends_on": [{"from": "summarize.summary", "to": "summary"}]},
            }
        ),
    }
    return dag, manifests


# -- resolve_edges ----------------------------------------------------------
def test_resolve_edges_builds_agent_refs():
    dag, manifests = _chain()
    wiring = resolve_edges(dag, manifests)
    assert wiring["ingest"] == []
    assert wiring["summarize"] == [
        AgentRef(producer="ingest", path="$.document", input_name="document")
    ]
    assert wiring["critique"] == [
        AgentRef(producer="summarize", path="$.summary", input_name="summary")
    ]


def test_resolve_edges_covers_every_dag_node():
    dag, manifests = _chain()
    assert set(resolve_edges(dag, manifests)) == set(dag.nodes)


def test_resolve_edges_preserves_nested_path():
    dag = AgentDAG()
    dag.add_node("p").add_node("c").add_edge("p", "c")
    manifests = {
        "p": AgentManifest.from_dict(
            {
                "name": "p",
                "registry": {"container_uri": "x"},
                "contract": {"outputs": {"data": {"type": "object"}}},
            }
        ),
        "c": AgentManifest.from_dict(
            {
                "name": "c",
                "registry": {"container_uri": "x"},
                "contract": {
                    "inputs": {"v": {"type": "string"}},
                    "outputs": {"o": {"type": "string"}},
                },
                "spec": {"depends_on": [{"from": "p.data.items[0]", "to": "v"}]},
            }
        ),
    }
    assert resolve_edges(dag, manifests)["c"] == [
        AgentRef(producer="p", path="$.data.items[0]", input_name="v")
    ]


# -- check_alignment --------------------------------------------------------
def test_check_alignment_passes_well_formed_chain():
    dag, manifests = _chain()
    assert check_alignment(dag, manifests) is None


def test_check_alignment_rejects_unknown_producer():
    dag, manifests = _chain()
    manifests["summarize"].spec["depends_on"] = [{"from": "ghost.document", "to": "document"}]
    with pytest.raises(AlignmentError, match="unknown producer 'ghost'"):
        check_alignment(dag, manifests)


def test_check_alignment_rejects_output_field_not_in_schema():
    dag, manifests = _chain()
    manifests["summarize"].spec["depends_on"] = [{"from": "ingest.missing", "to": "document"}]
    with pytest.raises(AlignmentError, match="does not declare output field 'missing'"):
        check_alignment(dag, manifests)


def test_check_alignment_rejects_undeclared_consumer_input():
    dag, manifests = _chain()
    manifests["summarize"].spec["depends_on"] = [{"from": "ingest.document", "to": "nope"}]
    with pytest.raises(AlignmentError, match="target input 'nope'"):
        check_alignment(dag, manifests)


def test_check_alignment_rejects_missing_dag_edge():
    dag, manifests = _chain()
    # Declare a valid producer/field/input but omit the ingest -> critique DAG edge.
    manifests["critique"].contract["inputs"]["document"] = {"type": "string"}
    manifests["critique"].spec["depends_on"].append(
        {"from": "ingest.document", "to": "document"}
    )
    with pytest.raises(AlignmentError, match="no edge 'ingest' -> 'critique'"):
        check_alignment(dag, manifests)


def test_check_alignment_accepts_flat_output_schema():
    dag = AgentDAG()
    dag.add_node("p").add_node("c").add_edge("p", "c")
    manifests = {
        "p": AgentManifest.from_dict(
            {
                "name": "p",
                "registry": {"container_uri": "x"},
                "contract": {"outputs": {"result": {"type": "string"}}},
            }
        ),
        "c": AgentManifest.from_dict(
            {
                "name": "c",
                "registry": {"container_uri": "x"},
                "contract": {
                    "inputs": {"r": {"type": "string"}},
                    "outputs": {"o": {"type": "string"}},
                },
                "spec": {"depends_on": [{"from": "p.result", "to": "r"}]},
            }
        ),
    }
    assert check_alignment(dag, manifests) is None


# -- B2: opt-in deep type-alignment gate -------------------------
def _typed_edge(producer_out_type, consumer_in_type):
    """A 2-node chain p -> c whose edge carries the given producer-output / consumer-input types."""
    dag = AgentDAG()
    dag.add_node("p").add_node("c").add_edge("p", "c")
    manifests = {
        "p": AgentManifest.from_dict(
            {
                "name": "p",
                "registry": {"container_uri": "x", "protocol": "HTTP"},
                "contract": {"outputs": {"result": {"type": producer_out_type}}},
            }
        ),
        "c": AgentManifest.from_dict(
            {
                "name": "c",
                "registry": {"container_uri": "x", "protocol": "HTTP"},
                "contract": {
                    "inputs": {"r": {"type": consumer_in_type}},
                    "outputs": {"o": {"type": "string"}},
                },
                "spec": {"depends_on": [{"from": "p.result", "to": "r"}]},
            }
        ),
    }
    return dag, manifests


def test_strict_types_passes_well_typed_chain():
    """A chain whose edge types match passes the deep gate (and the default name-level gate)."""
    dag, manifests = _chain()  # all string -> string
    assert check_alignment(dag, manifests, strict_types=True) is None


def test_strict_types_rejects_concrete_mismatch():
    """A producer 'string' fed into a consumer 'integer' input is caught ONLY under strict_types."""
    dag, manifests = _typed_edge("string", "integer")
    # Default (name-level) gate passes — the field names line up.
    assert check_alignment(dag, manifests) is None
    # The deep gate catches the concrete type mismatch.
    with pytest.raises(AlignmentError, match="type-INCOMPATIBLE"):
        check_alignment(dag, manifests, strict_types=True)


def test_strict_types_conservative_on_unknown_types():
    """An unknown/absent type on either side passes the deep gate (cannot prove incompatible)."""
    # Producer declares no type; consumer declares 'integer' -> unknown producer type => passes.
    dag, manifests = _typed_edge(None, "integer")
    assert check_alignment(dag, manifests, strict_types=True) is None
    # Consumer declares no type -> unknown consumer type => passes.
    dag2, manifests2 = _typed_edge("string", None)
    assert check_alignment(dag2, manifests2, strict_types=True) is None


def test_strict_types_union_overlap_passes():
    """A JSON-Schema union type passes when the producer type overlaps the consumer's accepted set."""
    dag, manifests = _typed_edge("string", ["string", "null"])
    assert check_alignment(dag, manifests, strict_types=True) is None
    # A disjoint union is still rejected.
    dag2, manifests2 = _typed_edge("boolean", ["string", "integer"])
    with pytest.raises(AlignmentError, match="type-INCOMPATIBLE"):
        check_alignment(dag2, manifests2, strict_types=True)


# -- B1: opt-in single-writer (non-overlap) gate -----------------
def _two_writers_one_input():
    """A diamond where BOTH `a` and `b` feed the SAME consumer input `doc` of `c` — a single-writer
    violation (the second edge would silently overwrite the first at run time)."""
    dag = AgentDAG()
    for n in ["a", "b", "c"]:
        dag.add_node(n)
    dag.add_edge("a", "c")
    dag.add_edge("b", "c")
    manifests = {
        "a": AgentManifest.from_dict(
            {"name": "a", "registry": {"container_uri": "x", "protocol": "HTTP"},
             "contract": {"outputs": {"out": {"type": "string"}}}}
        ),
        "b": AgentManifest.from_dict(
            {"name": "b", "registry": {"container_uri": "x", "protocol": "HTTP"},
             "contract": {"outputs": {"out": {"type": "string"}}}}
        ),
        "c": AgentManifest.from_dict(
            {"name": "c", "registry": {"container_uri": "x", "protocol": "HTTP"},
             "contract": {
                 "inputs": {"doc": {"type": "string"}},
                 "outputs": {"o": {"type": "string"}},
             },
             "spec": {"depends_on": [
                 {"from": "a.out", "to": "doc"},
                 {"from": "b.out", "to": "doc"},   # SECOND edge to the same input 'doc'
             ]}}
        ),
    }
    return dag, manifests


def test_single_writer_rejects_double_fed_input():
    """Two producers feeding one consumer input is caught ONLY under single_writer."""
    dag, manifests = _two_writers_one_input()
    # Default gate passes — both edges are individually well-formed (producer/field/input/DAG-edge).
    assert check_alignment(dag, manifests) is None
    # The non-overlap gate rejects the second writer.
    with pytest.raises(AlignmentError, match="single-writer violation"):
        check_alignment(dag, manifests, single_writer=True)


def test_single_writer_passes_normal_chain():
    """A one-producer-per-input chain passes the single-writer gate (nothing is double-fed)."""
    dag, manifests = _chain()
    assert check_alignment(dag, manifests, single_writer=True) is None


def test_single_writer_and_strict_types_compose():
    """B1 and B2 are independent, composable gates: both can be on at once."""
    dag, manifests = _chain()  # well-typed AND single-writer-clean
    assert check_alignment(dag, manifests, single_writer=True, strict_types=True) is None
    # A double-fed input still trips single_writer even with strict_types on.
    dag2, manifests2 = _two_writers_one_input()
    with pytest.raises(AlignmentError, match="single-writer violation"):
        check_alignment(dag2, manifests2, single_writer=True, strict_types=True)


# -- answer-carrying AlignmentError attributes ------------------------------
def test_alignment_error_defaults_are_none():
    """A bare AlignmentError carries every structured attribute as None (no answer to carry)."""
    err = AlignmentError("boom")
    assert err.node is None
    assert err.producer is None
    assert err.field is None
    assert err.expected is None
    assert err.candidates is None


def test_unknown_producer_error_carries_valid_producers():
    """The unknown-producer rejection names the offending consumer + the set of KNOWN producers."""
    dag, manifests = _chain()
    manifests["summarize"].spec["depends_on"] = [{"from": "ghost.document", "to": "document"}]
    with pytest.raises(AlignmentError) as ei:
        check_alignment(dag, manifests)
    err = ei.value
    assert err.node == "summarize"
    assert err.producer == "ghost"
    # candidates = the producer ids the caller could actually pick from.
    assert set(err.candidates) == {"ingest", "summarize", "critique"}


def test_missing_output_field_error_carries_declared_fields():
    """The missing-output-field rejection names the field + the producer's DECLARED output fields."""
    dag, manifests = _chain()
    manifests["summarize"].spec["depends_on"] = [{"from": "ingest.missing", "to": "document"}]
    with pytest.raises(AlignmentError) as ei:
        check_alignment(dag, manifests)
    err = ei.value
    assert err.node == "summarize"
    assert err.producer == "ingest"
    assert err.field == "missing"
    assert err.candidates == ("document",)  # the producer's only declared output field


def test_undeclared_input_error_carries_declared_inputs():
    """The undeclared-input rejection names the bad input + the consumer's DECLARED inputs."""
    dag, manifests = _chain()
    manifests["summarize"].spec["depends_on"] = [{"from": "ingest.document", "to": "nope"}]
    with pytest.raises(AlignmentError) as ei:
        check_alignment(dag, manifests)
    err = ei.value
    assert err.node == "summarize"
    assert err.field == "nope"
    assert err.candidates == ("document",)  # the consumer's only declared input


def test_type_incompatible_error_carries_expected_and_producer_type():
    """The deep-gate rejection carries the consumer's expected type + the producer's declared type."""
    dag, manifests = _typed_edge("string", "integer")
    with pytest.raises(AlignmentError) as ei:
        check_alignment(dag, manifests, strict_types=True)
    err = ei.value
    assert err.node == "c"
    assert err.producer == "p"
    assert err.field == "r"
    assert err.expected == "integer"        # what the consumer input demands
    assert err.candidates == ("string",)    # what the producer actually emits


def test_single_writer_error_carries_both_writers():
    """The single-writer rejection carries the offending input + BOTH competing producers."""
    dag, manifests = _two_writers_one_input()
    with pytest.raises(AlignmentError) as ei:
        check_alignment(dag, manifests, single_writer=True)
    err = ei.value
    assert err.node == "c"
    assert err.field == "doc"
    assert set(err.candidates) == {"a", "b"}


# -- FZ: opt-in compile-time capability gate (require_capabilities) ---------
def _capability_chain(*, requires=None, declares=None):
    """A single-node manifest ``solo`` whose spec.requires + capabilities are parametrized."""
    dag = AgentDAG()
    dag.add_node("solo")
    contract = {"inputs": {}, "outputs": {"result": {"type": "string"}}}
    data = {
        "name": "solo",
        "registry": {"container_uri": "x", "protocol": "HTTP"},
        "contract": contract,
    }
    if requires is not None:
        data["spec"] = {"requires": requires}
    if declares is not None:
        data["capabilities"] = declares
    return dag, {"solo": AgentManifest.from_dict(data)}


def test_require_capabilities_off_by_default_is_a_noop():
    """A manifest that requires a capability its runtime does NOT declare still passes by default —
    the gate is opt-in, so the default path is byte-for-byte unchanged."""
    dag, manifests = _capability_chain(requires={"tools": ["search"]}, declares=None)
    assert check_alignment(dag, manifests) is None  # default off => no capability check


def test_require_capabilities_passes_when_all_declared():
    """When the agent's capabilities cover every required label, the gate passes."""
    dag, manifests = _capability_chain(
        requires={"tools": ["search"], "features": ["stream"]},
        declares={"tools": ["search", "write"], "features": ["stream"]},
    )
    assert check_alignment(dag, manifests, require_capabilities=True) is None


def test_require_capabilities_rejects_missing_capability():
    """A required capability absent from the agent's declared inventory fails at compile."""
    dag, manifests = _capability_chain(
        requires={"tools": ["search", "browse"]},
        declares={"tools": ["search"]},  # 'browse' is not declared
    )
    with pytest.raises(AlignmentError, match="capability gate violation") as ei:
        check_alignment(dag, manifests, require_capabilities=True)
    err = ei.value
    assert err.node == "solo"
    assert err.field == "tools"
    assert err.expected == ("browse",)          # the still-missing label(s)
    assert err.candidates == ("search",)        # what the agent actually declares


def test_require_capabilities_bare_list_reads_as_features():
    """A bare ``spec.requires`` list is treated as required FEATURES (the common shorthand)."""
    dag, manifests = _capability_chain(requires=["stream"], declares={"features": []})
    with pytest.raises(AlignmentError, match="requires features") as ei:
        check_alignment(dag, manifests, require_capabilities=True)
    assert ei.value.field == "features"
    # And it passes once the feature is declared.
    dag2, manifests2 = _capability_chain(requires=["stream"], declares={"features": ["stream"]})
    assert check_alignment(dag2, manifests2, require_capabilities=True) is None


def test_require_capabilities_no_requires_block_is_always_clean():
    """A manifest with NO spec.requires imposes nothing even with the gate on (conservative)."""
    dag, manifests = _capability_chain(requires=None, declares=None)
    assert check_alignment(dag, manifests, require_capabilities=True) is None


def test_require_capabilities_gates_egress_hosts():
    """The gate covers all three capability kinds, including egress_hosts."""
    dag, manifests = _capability_chain(
        requires={"egress_hosts": ["api.example.com"]},
        declares={"egress_hosts": []},
    )
    with pytest.raises(AlignmentError, match="requires egress_hosts") as ei:
        check_alignment(dag, manifests, require_capabilities=True)
    assert ei.value.field == "egress_hosts"
