"""Tests for the OPT-IN compile-time static-fan-out unroll (``deliberate.unroll_static_fanout``).

Pins the crux: a DECLARED, data-INDEPENDENT fan-out ``N`` is unrolled at COMPILE TIME into ``N``
frozen parallel branches (namespaced ``base__fe{i}``) plus a synthetic ``base__gather`` join — a
scatter (shared input to all clones) + gather (clones -> join) rewrite of the FROZEN topology, so
the static Supervisor runs them in one pass over ``plan.order`` (no runtime graph mutation). The
default (no unroll spec) leaves the DAG byte-for-byte unchanged, and the assembler still freezes.
"""

from __future__ import annotations

import pytest

from concursus import AgentDAG, AgentManifest
from concursus.assemble.assemble import OrchestrationAssembler, ProvisioningPlan
from concursus.core.dag import DAGError
from concursus.reasoning.deliberate import unroll_static_fanout


def _agent(name):
    """A minimal valid container-hosted HTTP manifest (no depends_on => trivially aligned)."""
    return AgentManifest.from_dict(
        {
            "name": name,
            "registry": {
                "container_uri": "acct.dkr.ecr.us-east-1.amazonaws.com/agents:latest",
                "protocol": "HTTP",
                "entry": f"agents.{name}:run",
                "role_arn": "arn:aws:iam::123456789012:role/agent",
            },
            "contract": {"inputs": {}, "outputs": {"result": {"type": "string"}}},
        }
    )


def _pipe():
    """producer -> worker -> consumer (worker is the fan-out base)."""
    dag = AgentDAG()
    for n in ("producer", "worker", "consumer"):
        dag.add_node(n)
    dag.add_edge("producer", "worker")
    dag.add_edge("worker", "consumer")
    return dag


# -- the unroll transform ----------------------------------------------------
def test_unroll_yields_n_frozen_branches_plus_a_gather():
    dag = _pipe()
    out = unroll_static_fanout(dag, {"worker": 3})

    # N=3 frozen clones under namespaced ids + one synthetic gather; the base is gone.
    clones = {f"worker__fe{i}" for i in range(3)}
    assert clones <= set(out.nodes)
    assert "worker__gather" in out.nodes
    assert "worker" not in out.nodes

    # SCATTER: the shared upstream input fans to every clone.
    for i in range(3):
        assert "producer" in out.get_dependencies(f"worker__fe{i}")
    # GATHER: every clone feeds the synthetic join, which feeds the downstream consumer.
    assert set(out.get_dependencies("worker__gather")) == clones
    assert out.get_dependencies("consumer") == ["worker__gather"]

    # FROZEN: the rewritten topology is a valid (acyclic) DAG — one static pass, no cycles.
    order = out.topological_sort()
    assert set(order) == set(out.nodes)


def test_unrolled_dag_assembles_and_freezes():
    dag = unroll_static_fanout(_pipe(), {"worker": 4})
    manifests = {node: _agent(node) for node in dag.nodes}

    plan = OrchestrationAssembler().assemble(dag, manifests)

    assert isinstance(plan, ProvisioningPlan)
    # The 4 branches + the gather are all in the single frozen dispatch order.
    for i in range(4):
        assert f"worker__fe{i}" in plan.order
    assert "worker__gather" in plan.order
    # A gather runs strictly after every branch it joins (static one-pass order).
    g = plan.order.index("worker__gather")
    for i in range(4):
        assert plan.order.index(f"worker__fe{i}") < g


# -- gating: default off + spec errors ---------------------------------------
def test_no_spec_returns_dag_unchanged():
    dag = _pipe()
    # Absent / empty spec => the SAME object, byte-for-byte unchanged (opt-in, default off).
    assert unroll_static_fanout(dag) is dag
    assert unroll_static_fanout(dag, {}) is dag
    assert unroll_static_fanout(dag, None) is dag


def test_n_equals_one_is_a_degenerate_noop():
    dag = _pipe()
    out = unroll_static_fanout(dag, {"worker": 1})
    # N==1 leaves the base in place (no clones, no gather).
    assert "worker" in out.nodes
    assert "worker__gather" not in out.nodes
    assert set(out.nodes) == set(dag.nodes)


def test_bad_spec_raises():
    dag = _pipe()
    with pytest.raises(DAGError):
        unroll_static_fanout(dag, {"nope": 2})  # unknown node
    with pytest.raises(DAGError):
        unroll_static_fanout(dag, {"worker": 0})  # not a positive static bound
    with pytest.raises(DAGError):
        unroll_static_fanout(dag, {"worker": True})  # bool is not a fan-out count
