"""Tests for the generative plan-author front (AI-22) and the AI-21 plan-approval gate.

Identity guard under test: the planner is the validated FRONT of the compiler
(emit -> validate -> freeze -> replay), the LLM is an INJECTED/optional ``plan_model_fn`` seam
(default ``None`` needs no model), and the approval gate is a between-phases pause that is
OFF by default (today's ``run --execute`` unchanged).
"""

import argparse
import io
import sys

import pytest

from concursus import AgentDAG, AgentManifest
from concursus.assemble import OrchestrationAssembler
from concursus.planner import PlanAuthorError, plan_from_goal


# -- fixtures ---------------------------------------------------------------
def _agent(name, inputs, outputs, depends_on=None, **registry):
    reg = {
        "container_uri": "acct.dkr.ecr.us-east-1.amazonaws.com/agents:latest",
        "protocol": "HTTP",
        "entry": f"agents.{name}:run",
        "role_arn": "arn:aws:iam::123456789012:role/agent",
    }
    reg.update(registry)
    data = {"name": name, "registry": reg, "contract": {"inputs": inputs, "outputs": outputs}}
    if depends_on is not None:
        data["spec"] = {"depends_on": depends_on}
    return AgentManifest.from_dict(data)


# -- AI-22: plan_from_goal ---------------------------------------------------
def test_plan_from_goal_with_stub_model_returns_assemblable_dag():
    """A stub plan_model_fn emits a topology that assembles + freezes with NO LLM present."""

    seen = {}

    def stub_model(goal, precedents, directives):
        seen["goal"] = goal
        seen["precedents"] = list(precedents)
        seen["directives"] = dict(directives)
        return {
            "nodes": ["ingest", "summarize"],
            "edges": [["ingest", "summarize"]],
        }

    dag = plan_from_goal(
        "summarize a document",
        precedents=[{"trail_id": "r1", "method": "lexical"}],
        operator_directives={"require": "ingest"},
        plan_model_fn=stub_model,
    )
    assert isinstance(dag, AgentDAG)
    assert dag.topological_sort() == ["ingest", "summarize"]
    # precedents + directives are passed to the model as read-only context
    assert seen["goal"] == "summarize a document"
    assert seen["precedents"] == [{"trail_id": "r1", "method": "lexical"}]
    assert seen["directives"] == {"require": "ingest"}

    # the emitted DAG is the validated FRONT of the compiler: it assembles + freezes.
    manifests = {
        "ingest": _agent("ingest", {"uri": {"type": "string"}}, {"document": {"type": "string"}}),
        "summarize": _agent(
            "summarize",
            {"document": {"type": "string"}},
            {"summary": {"type": "string"}},
            depends_on=[{"from": "ingest.document", "to": "document"}],
        ),
    }
    plan = OrchestrationAssembler().assemble(dag, manifests)
    assert plan.order == ["ingest", "summarize"]


def test_plan_from_goal_accepts_agentdag_from_model():
    built = AgentDAG()
    built.add_node("solo")

    plan_dag = plan_from_goal("do a thing", plan_model_fn=lambda g, p, d: built)
    assert plan_dag is built


def test_plan_from_goal_without_model_falls_back_deterministically():
    """plan_model_fn=None => trivial deterministic template, no LLM needed to import/run."""
    dag = plan_from_goal("Summarize The Document!")
    assert isinstance(dag, AgentDAG)
    # deterministic, goal-derived, valid single-node topology
    assert dag.nodes == ["summarize_the_document"]
    assert dag.topological_sort() == ["summarize_the_document"]
    # stable: same goal -> same plan
    assert plan_from_goal("Summarize The Document!").nodes == dag.nodes


def test_plan_from_goal_empty_goal_raises():
    with pytest.raises(PlanAuthorError, match="non-empty goal"):
        plan_from_goal("   ")


def test_plan_from_goal_invalid_model_spec_raises():
    with pytest.raises(PlanAuthorError, match="invalid plan spec|must return"):
        plan_from_goal("x", plan_model_fn=lambda g, p, d: 42)


def test_plan_from_goal_rejects_cyclic_model_output():
    def cyclic(goal, precedents, directives):
        return {"nodes": ["a", "b"], "edges": [["a", "b"], ["b", "a"]]}

    with pytest.raises(PlanAuthorError):
        plan_from_goal("cycle", plan_model_fn=cyclic)


# -- AI-21: plan-approval gate (default OFF leaves _cmd_run unchanged) -------
def _run_args(**overrides):
    """A minimal argparse.Namespace mimicking the `run` subparser defaults."""
    base = dict(
        manifests=[],
        dag=None,
        account=None,
        region=None,
        inputs=None,
        execute=True,
        vault=None,
        lean_form=False,
        memory_id=None,
        actor_id=None,
        approve=False,
        yes=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


class _FakePlan:
    order = ["a"]
    wiring = {"a": []}

    def to_dict(self):
        return {"order": ["a"], "entries": {}, "wiring": {"a": []}}


def test_approval_gate_default_off_never_prompts(monkeypatch):
    """With --approve absent, _cmd_run runs the supervisor without any approval pause."""
    from concursus import cli

    invoked = {"gate": 0, "run": 0}

    def _gate(plan, args):
        invoked["gate"] += 1
        return True

    class _Sup:
        def run(self, inputs):
            invoked["run"] += 1
            return {"a": {"ok": True}}

        def summary_line(self):
            return ""

    monkeypatch.setattr(cli, "_plan_approval_gate", _gate)
    monkeypatch.setattr(cli, "_assemble", lambda args: ({}, _FakePlan()))
    monkeypatch.setattr(cli, "_load_inputs", lambda v: {})
    monkeypatch.setattr(cli, "_make_run_supervisor", lambda a, p, m: _Sup())

    rc = cli._cmd_run(_run_args(approve=False))
    assert rc == 0
    assert invoked["gate"] == 0  # gate never consulted when off
    assert invoked["run"] == 1  # supervisor ran exactly as today


def test_approval_gate_abort_prevents_invoke(monkeypatch):
    """--approve with a declined gate aborts BEFORE any supervisor.run (nothing billed)."""
    from concursus import cli

    ran = {"run": 0}

    class _Sup:
        def run(self, inputs):
            ran["run"] += 1
            return {}

    monkeypatch.setattr(cli, "_plan_approval_gate", lambda plan, args: False)
    monkeypatch.setattr(cli, "_assemble", lambda args: ({}, _FakePlan()))
    monkeypatch.setattr(cli, "_load_inputs", lambda v: {})
    monkeypatch.setattr(cli, "_make_run_supervisor", lambda a, p, m: _Sup())

    rc = cli._cmd_run(_run_args(approve=True))
    assert rc == 0
    assert ran["run"] == 0  # aborted: nothing invoked


def test_approval_gate_yes_approves_non_interactively(capsys):
    """--yes approves the frozen-plan preview without a TTY prompt."""
    from concursus import cli

    assert cli._plan_approval_gate(_FakePlan(), _run_args(approve=True, yes=True)) is True
    out = capsys.readouterr().out
    assert "PLAN PREVIEW" in out


def test_approval_gate_non_tty_without_yes_aborts(monkeypatch):
    """No TTY + no --yes => the gate refuses (never auto-approves a billed run)."""
    from concursus import cli

    monkeypatch.setattr(sys, "stdin", io.StringIO(""))  # not a tty (StringIO has no isatty->True)
    assert cli._plan_approval_gate(_FakePlan(), _run_args(approve=True, yes=False)) is False
