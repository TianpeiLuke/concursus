"""Tests for the run graph — projecting recorded AgentRef edges into a queryable DAG.

A lightweight ``Rec`` stand-in (``.node`` + ``.consumes``) drives :meth:`RunGraph.from_records`
so these tests never import ``statestore`` or touch AWS. Coverage: ``consumes`` parsing,
transitive ``upstream``/``downstream`` over a chain and a diamond, the structural ``validate``
gate (DAG passes; cycle + dangling edge raise), and bounded nearest-first ``context_order``.
"""

from collections import namedtuple

import pytest

from concursus.rungraph import RunGraph, RunGraphError

# A minimal StateStore-record stand-in so the test does not import statestore.
Rec = namedtuple("Rec", "node consumes")


# -- fixtures ---------------------------------------------------------------
def _chain_records():
    """A 4-node chain a -> b -> c -> d (each consumes its single predecessor)."""
    return [
        Rec("a", []),
        Rec("b", ["a:$.o"]),
        Rec("c", ["b:$.o"]),
        Rec("d", ["c:$.o"]),
    ]


def _diamond_records():
    """A diamond a -> b, a -> c, b -> d, c -> d."""
    return [
        Rec("a", []),
        Rec("b", ["a:$.o"]),
        Rec("c", ["a:$.o"]),
        Rec("d", ["b:$.left", "c:$.right"]),
    ]


# -- from_records -----------------------------------------------------------
def test_from_records_parses_consumes_into_edges():
    g = RunGraph.from_records(_diamond_records())
    assert g.nodes == {"a", "b", "c", "d"}
    assert ("a", "b", "$.o") in g.edges
    assert ("a", "c", "$.o") in g.edges
    assert ("b", "d", "$.left") in g.edges
    assert ("c", "d", "$.right") in g.edges


def test_from_records_splits_on_first_colon_only():
    g = RunGraph.from_records([Rec("consumer", ["producer:$.a.b"])])
    assert ("producer", "consumer", "$.a.b") in g.edges


def test_from_records_adds_referenced_producer_as_node():
    g = RunGraph.from_records([Rec("consumer", ["ghost:$.x"])])
    assert "ghost" in g.nodes
    assert "consumer" in g.nodes


# -- upstream / downstream --------------------------------------------------
def test_upstream_transitive_on_chain():
    g = RunGraph.from_records(_chain_records())
    assert g.upstream("d") == {"a", "b", "c"}
    assert g.upstream("b") == {"a"}
    assert g.upstream("a") == set()


def test_downstream_transitive_on_chain():
    g = RunGraph.from_records(_chain_records())
    assert g.downstream("a") == {"b", "c", "d"}
    assert g.downstream("c") == {"d"}
    assert g.downstream("d") == set()


def test_upstream_downstream_on_diamond():
    g = RunGraph.from_records(_diamond_records())
    assert g.upstream("d") == {"a", "b", "c"}  # transitive through both arms, deduped
    assert g.downstream("a") == {"b", "c", "d"}
    assert g.upstream("b") == {"a"}
    assert g.downstream("b") == {"d"}


def test_from_edges_roundtrip():
    g = RunGraph.from_edges({"a", "b", "c"}, [("a", "b", "$.o"), ("b", "c", "$.o")])
    assert g.upstream("c") == {"a", "b"}
    assert g.downstream("a") == {"b", "c"}


# -- validate ---------------------------------------------------------------
def test_validate_passes_a_dag():
    assert RunGraph.from_records(_diamond_records()).validate() is None


def test_validate_raises_on_cycle():
    g = RunGraph.from_edges({"a", "b"}, [("a", "b", "$.o"), ("b", "a", "$.o")])
    with pytest.raises(RunGraphError, match="cycle"):
        g.validate()


def test_validate_raises_on_self_loop_cycle():
    g = RunGraph.from_edges({"a"}, [("a", "a", "$.o")])
    with pytest.raises(RunGraphError, match="cycle"):
        g.validate()


def test_validate_raises_on_dangling_edge():
    # The consumer references a producer that is not present as a node.
    g = RunGraph.from_edges({"consumer"}, [("ghost", "consumer", "$.o")])
    with pytest.raises(RunGraphError, match="ghost"):
        g.validate()


# -- context_order ----------------------------------------------------------
def test_context_order_diamond_is_nearest_first_and_deduped():
    g = RunGraph.from_records(_diamond_records())
    # Direct producers of d (b, c) come before their shared producer (a); a appears once.
    assert g.context_order("d") == ["b", "c", "a"]


def test_context_order_excludes_the_node_itself():
    g = RunGraph.from_records(_diamond_records())
    assert "d" not in g.context_order("d")


def test_context_order_respects_max_depth():
    g = RunGraph.from_records(_diamond_records())
    # Depth 1 stops at the direct producers; a (2 hops away) is excluded.
    assert g.context_order("d", max_depth=1) == ["b", "c"]


def test_context_order_respects_max_nodes():
    g = RunGraph.from_records(_diamond_records())
    assert g.context_order("d", max_nodes=2) == ["b", "c"]


def test_context_order_chain_depth_cap():
    g = RunGraph.from_records(_chain_records())
    assert g.context_order("d", max_depth=2) == ["c", "b"]
    assert g.context_order("d", max_depth=3) == ["c", "b", "a"]


def test_context_order_empty_for_source_node():
    g = RunGraph.from_records(_diamond_records())
    assert g.context_order("a") == []
