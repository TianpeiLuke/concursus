"""Tests for the OPT-IN agent-facing control surface (-CS).

The control surface is a thin, read-mostly, in-process handle over the SSOT:
  * read verbs (query_plan / tail_log / search_runs / precedents) are ALWAYS on;
  * actuating verbs (deploy / run / recompile) route THROUGH injected existing actuators only,
    are resolved from the COMPILED plan/scope (never an env var), gated by NON-REGISTRATION
    (a verb the scope omits is absent), by an explicit activation gate, and by a monotonic
    TrustGrade clamp that only ever opts DOWN.
"""
from __future__ import annotations

import types

import pytest

from concursus.core.resolve import resolve_edges
from concursus.execute.supervisor import Supervisor
from concursus.state.statestore import InProcessStateStore
from concursus.governor.cockpit import ControlSurface, ControlSurfaceError
from concursus.governor.scope import (
    ACTUATING_VERBS,
    READ_VERBS,
    RECURSIVE_VERBS,
    ControlScope,
)
from concursus.build.trust import TrustGrade, clamp_trust_grade

from concursus import AgentDAG, AgentManifest


# ---------------------------------------------------------------- fixtures
def _manifests():
    ingest = AgentManifest.from_dict({
        "name": "ingest",
        "registry": {"container_uri": "x", "protocol": "HTTP"},
        "contract": {
            "inputs": {"uri": {"type": "string", "required": True}},
            "outputs": {"document": {"type": "string", "required": True}},
        },
        "spec": {"depends_on": []},
    })
    summarize = AgentManifest.from_dict({
        "name": "summarize",
        "registry": {"container_uri": "x", "protocol": "HTTP"},
        "contract": {
            "inputs": {"document": {"type": "string", "required": True}},
            "outputs": {"summary": {"type": "string", "required": True}},
        },
        "spec": {"depends_on": [{"from": "ingest.document", "to": "document"}]},
    })
    return {"ingest": ingest, "summarize": summarize}


def _dag():
    dag = AgentDAG()
    dag.add_node("ingest")
    dag.add_node("summarize")
    dag.add_edge("ingest", "summarize")
    return dag


def _fake_invoker(*args, **kwargs):
    blob = " ".join([str(a) for a in args] + [str(v) for v in kwargs.values()])
    if "summarize" in blob:
        raise ValueError("boom summarize")
    return {"document": "doc-body"}


def _plan(dag, manifests, *, revision=0, precedents=None):
    return types.SimpleNamespace(
        order=dag.topological_sort(),
        wiring=resolve_edges(dag, manifests),
        revision=revision,
        precedents=list(precedents or []),
    )


def _run():
    manifests = _manifests()
    dag = _dag()
    plan = _plan(dag, manifests, precedents=[{"trail_id": "t.p.g.run1", "status": "completed"}])
    store = InProcessStateStore()
    sup = Supervisor(
        plan,
        manifests,
        invoke_fn=_fake_invoker,
        arns={"ingest": "arn:ingest", "summarize": "arn:summarize"},
        state_store=store,
        on_error="record",
        session_id="S" * 40,
    )
    sup.run({"uri": "s3://doc"})
    return sup, store, plan


# ---------------------------------------------------- verb taxonomy invariants
def test_verb_sets_are_disjoint_and_recursive_is_actuating():
    assert READ_VERBS.isdisjoint(ACTUATING_VERBS)
    assert RECURSIVE_VERBS <= ACTUATING_VERBS  # recompile is an actuating verb
    assert "deploy" in ACTUATING_VERBS and "recompile" in RECURSIVE_VERBS


# ---------------------------------------------------- non-registration gate
def test_scope_omitting_deploy_has_no_deploy_verb():
    """The headline invariant: a compiled scope that omits 'deploy' yields NO deploy verb."""
    sup, store, plan = _run()
    # authorize run + recompile, but NOT deploy.
    scope = ControlScope.from_plan(plan, authorize=["run", "recompile"], trust_ceiling=2)
    surface = ControlSurface(supervisor=sup, scope=scope, plan=plan)

    assert "deploy" not in surface.verbs()
    assert not surface.has_verb("deploy")
    # read verbs + the two authorized actuators are present.
    assert set(READ_VERBS) <= set(surface.verbs())
    assert "run" in surface.verbs() and "recompile" in surface.verbs()


def test_default_scope_is_read_only():
    """No authorization => only the always-on read verbs; every actuating verb is absent."""
    sup, store, plan = _run()
    scope = ControlScope.from_plan(plan)  # no authorize=, no plan.control_verbs
    surface = ControlSurface(supervisor=sup, scope=scope, plan=plan)

    assert set(surface.verbs()) == set(READ_VERBS)
    for verb in ACTUATING_VERBS:
        assert not surface.has_verb(verb)


def test_bound_read_from_plan_attr_not_env(monkeypatch):
    """The bound is read from the frozen plan/scope, NOT an env var — setting an env has no effect,
    and a plan.control_verbs attribute drives the bound when no explicit authorize= is given."""
    sup, store, plan = _run()
    monkeypatch.setenv("HIVE_CONTROL_VERBS", "deploy,run,recompile")
    plan_with_attr = types.SimpleNamespace(
        order=list(plan.order), wiring=plan.wiring, revision=plan.revision,
        precedents=plan.precedents, control_verbs=["run"], trust_ceiling=1,
    )
    scope = ControlScope.from_plan(plan_with_attr)
    surface = ControlSurface(supervisor=sup, scope=scope, plan=plan_with_attr)

    # Env var is ignored; only the plan's compiled control_verbs bind (and 'deploy' stays absent).
    assert "run" in surface.verbs()
    assert "deploy" not in surface.verbs()
    assert scope.trust_ceiling == 1


def test_unknown_authorized_verb_is_dropped():
    sup, store, plan = _run()
    scope = ControlScope.from_plan(plan, authorize=["deploy", "not_a_verb"])
    assert scope.actuating == frozenset({"deploy"})


# ---------------------------------------------------- trust clamp (monotonic)
def test_clamp_never_escalates_above_compiled_grade():
    for compiled in TrustGrade:
        for requested in TrustGrade:
            got = clamp_trust_grade(compiled, requested)
            assert got <= compiled, f"clamp escalated: {requested} over ceiling {compiled}"
            assert got == min(compiled, requested)


def test_effective_trust_clamps_down_only():
    sup, store, plan = _run()
    scope = ControlScope.from_plan(plan, authorize=["deploy"], trust_ceiling=1)  # ceiling = L1
    surface = ControlSurface(supervisor=sup, scope=scope, plan=plan)

    # Requesting full autonomy is clamped down to the compiled L1 ceiling.
    assert surface.effective_trust(TrustGrade.L3_AUTONOMOUS) == TrustGrade.L1_CANARY
    # Requesting below the ceiling is honored (opting down is allowed).
    assert surface.effective_trust(TrustGrade.L0_SHADOW) == TrustGrade.L0_SHADOW
    # At the ceiling => unchanged.
    assert surface.effective_trust(TrustGrade.L1_CANARY) == TrustGrade.L1_CANARY


def test_effective_trust_passthrough_when_no_ceiling():
    sup, store, plan = _run()
    scope = ControlScope.from_plan(plan, authorize=["deploy"])  # no trust_ceiling compiled
    surface = ControlSurface(supervisor=sup, scope=scope, plan=plan)
    assert surface.effective_trust(TrustGrade.L3_AUTONOMOUS) == TrustGrade.L3_AUTONOMOUS


# ---------------------------------------------------- read verbs always on
def test_read_verbs_available_and_do_not_mutate_log(tmp_path):
    sup, store, plan = _run()
    scope = ControlScope.from_plan(plan)  # read-only bound
    surface = ControlSurface(supervisor=sup, scope=scope, vault_path=str(tmp_path), plan=plan)

    before = [repr(r) for r in store.records()]

    qp = surface.query_plan()
    assert qp["order"] == list(plan.order)
    assert qp["revision"] == plan.revision

    tail = surface.tail_log(0)
    assert tail["count"] >= 1  # the run wrote records

    assert surface.search_runs("summarize") == [] or isinstance(surface.search_runs("summarize"), list)
    assert surface.precedents() == [{"trail_id": "t.p.g.run1", "status": "completed"}]

    after = [repr(r) for r in store.records()]
    assert before == after  # read-mostly: nothing mutated the SSOT


def test_precedents_returns_a_copy_never_mutates_plan():
    sup, store, plan = _run()
    scope = ControlScope.from_plan(plan)
    surface = ControlSurface(supervisor=sup, scope=scope, plan=plan)
    got = surface.precedents()
    got.append({"tampered": True})
    got[0]["status"] = "MUTATED"
    # The frozen plan's precedents are untouched by mutating the returned copy.
    assert plan.precedents == [{"trail_id": "t.p.g.run1", "status": "completed"}]


def test_search_runs_empty_without_vault():
    sup, store, plan = _run()
    scope = ControlScope.from_plan(plan)
    surface = ControlSurface(supervisor=sup, scope=scope, plan=plan)  # no vault_path
    assert surface.search_runs("anything") == []


# ---------------------------------------------------- activation + actuator routing
def test_actuating_verb_needs_activation_and_routes_through_injected_actuator():
    sup, store, plan = _run()
    calls = []

    def _deploy_actuator(*args, **kwargs):
        calls.append((args, kwargs))
        return "deployed"

    scope = ControlScope.from_plan(plan, authorize=["deploy"], trust_ceiling=1)
    surface = ControlSurface(
        supervisor=sup, scope=scope, plan=plan, actuators={"deploy": _deploy_actuator}
    )

    # Not yet activated -> refused.
    with pytest.raises(ControlSurfaceError):
        surface.invoke("deploy")
    assert calls == []

    # Activate, then invoke: the requested L3 is clamped down to the L1 ceiling before the actuator.
    surface.activate("deploy")
    assert surface.is_active("deploy")
    result = surface.invoke("deploy", "node-x", trust=TrustGrade.L3_AUTONOMOUS)
    assert result == "deployed"
    assert calls[0][0] == ("node-x",)
    assert calls[0][1]["trust"] == TrustGrade.L1_CANARY  # clamped, never escalated


def test_unauthorized_verb_cannot_be_activated_or_invoked():
    sup, store, plan = _run()
    scope = ControlScope.from_plan(plan, authorize=["run"])  # deploy NOT authorized
    surface = ControlSurface(
        supervisor=sup, scope=scope, plan=plan, actuators={"deploy": lambda *a, **k: "x"}
    )
    with pytest.raises(ControlSurfaceError):
        surface.activate("deploy")
    with pytest.raises(ControlSurfaceError):
        surface.invoke("deploy")


def test_authorized_but_no_actuator_is_offline_by_default():
    sup, store, plan = _run()
    scope = ControlScope.from_plan(plan, authorize=["run"])
    surface = ControlSurface(supervisor=sup, scope=scope, plan=plan)  # no actuators wired
    surface.activate("run")
    with pytest.raises(ControlSurfaceError):
        surface.invoke("run")  # authorized + activated, but offline (no injected actuator)


def test_invoke_rejects_read_verbs():
    sup, store, plan = _run()
    scope = ControlScope.from_plan(plan, authorize=["deploy"])
    surface = ControlSurface(supervisor=sup, scope=scope, plan=plan)
    for read_verb in READ_VERBS:
        with pytest.raises(ControlSurfaceError):
            surface.invoke(read_verb)


def test_activate_rejects_non_actuating_verb():
    sup, store, plan = _run()
    scope = ControlScope.from_plan(plan, authorize=["deploy"])
    surface = ControlSurface(supervisor=sup, scope=scope, plan=plan)
    with pytest.raises(ControlSurfaceError):
        surface.activate("query_plan")


def test_recursive_verb_gated_like_other_actuators():
    """recompile (recursive) is subject to the same non-registration + activation gates."""
    sup, store, plan = _run()
    scope = ControlScope.from_plan(plan, authorize=["recompile"])
    seen = []
    surface = ControlSurface(
        supervisor=sup, scope=scope, plan=plan,
        actuators={"recompile": lambda *a, **k: seen.append(k) or "recompiled"},
    )
    with pytest.raises(ControlSurfaceError):
        surface.invoke("recompile")  # not activated
    surface.activate("recompile")
    assert surface.invoke("recompile", requested_trust=TrustGrade.L3_AUTONOMOUS) == "recompiled"


def test_surface_cannot_mutate_frozen_plan_order():
    sup, store, plan = _run()
    scope = ControlScope.from_plan(plan, authorize=["deploy"], trust_ceiling=0)
    surface = ControlSurface(supervisor=sup, scope=scope, plan=plan)
    before = list(plan.order)
    surface.query_plan()
    surface.tail_log(0)
    surface.verbs()
    assert list(plan.order) == before
