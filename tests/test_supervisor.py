"""Tests for the Supervisor — the eager AgentRef run loop over a provisioning plan.

A fake :data:`~concursus.supervisor.InvokeFn` (keyed by agentRuntimeArn) drives a 3-node
chain offline; no boto3 is imported. It records every call so the tests can assert the
forward threading of outputs, the stable session id, and output-schema validation.
"""

import json
import types

import pytest

from concursus import AgentDAG, AgentManifest
from concursus.core.resolve import AgentRef, resolve_edges
from concursus.state.rungraph import RunGraphError
from concursus.state.statestore import InProcessStateStore
from concursus.execute.supervisor import (
    _ARN_PLACEHOLDER,
    SchemaError,
    Supervisor,
    validate_output,
)


# -- fixtures ---------------------------------------------------------------
def _chain():
    """A well-formed 3-node chain: ingest -> summarize -> critique.

    ``ingest`` uses a flat output schema (with a per-property ``required`` flag); ``summarize``
    uses the nested ``{"properties": {...}}`` shape plus a top-level ``required`` list — both
    accepted forms are exercised. ``summarize`` also pins a non-default ``qualifier``.
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
                    "outputs": {"document": {"type": "string", "required": True}},
                },
            }
        ),
        "summarize": AgentManifest.from_dict(
            {
                "name": "summarize",
                "registry": {"container_uri": "x", "protocol": "HTTP", "qualifier": "PROD"},
                "contract": {
                    "inputs": {"document": {"type": "string"}},
                    "outputs": {
                        "properties": {"summary": {"type": "string"}},
                        "required": ["summary"],
                    },
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
                    "outputs": {"critique": {"type": "string", "required": True}},
                },
                "spec": {"depends_on": [{"from": "summarize.summary", "to": "summary"}]},
            }
        ),
    }
    return dag, manifests


def _plan(dag, manifests):
    """A ProvisioningPlan-like stand-in exposing the duck-typed ``.order`` + ``.wiring``."""
    return types.SimpleNamespace(
        order=dag.topological_sort(),
        wiring=resolve_edges(dag, manifests),
    )


class FakeInvoker:
    """A fake :data:`InvokeFn` returning a canned dict per arn and recording every call."""

    def __init__(self, outputs_by_arn):
        self.outputs_by_arn = outputs_by_arn
        self.calls = []  # (arn, qualifier, session_id, payload_dict)

    def __call__(self, arn, qualifier, session_id, payload_bytes):
        payload = json.loads(payload_bytes)
        assert isinstance(payload_bytes, (bytes, bytearray))
        self.calls.append((arn, qualifier, session_id, payload))
        return dict(self.outputs_by_arn[arn])

    def payload_for(self, arn):
        for got_arn, _q, _s, payload in self.calls:
            if got_arn == arn:
                return payload
        raise AssertionError(f"no call for arn {arn!r}")


_ARNS = {
    "ingest": "arn:ingest",
    "summarize": "arn:summarize",
    "critique": "arn:critique",
}


def _fake_outputs():
    return {
        "arn:ingest": {"document": "DOC", "extra": 1},
        "arn:summarize": {"summary": "SUM"},
        "arn:critique": {"critique": "OK"},
    }


# -- run loop: forward threading via AgentRef -------------------------------
def test_run_threads_upstream_output_into_downstream_payload():
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())
    sup = Supervisor(_plan(dag, manifests), manifests, invoke_fn=fake, arns=_ARNS)

    sup.run({"uri": "s3://doc"})

    # (a) each downstream payload received the upstream field via its AgentRef wiring.
    assert fake.payload_for("arn:ingest") == {"uri": "s3://doc"}  # source: top-level inputs
    assert fake.payload_for("arn:summarize")["document"] == "DOC"
    assert fake.payload_for("arn:critique")["summary"] == "SUM"


def test_run_returns_every_node_output():
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())
    sup = Supervisor(_plan(dag, manifests), manifests, invoke_fn=fake, arns=_ARNS)

    outputs = sup.run({"uri": "s3://doc"})

    # (b) final outputs contain every node.
    assert set(outputs) == {"ingest", "summarize", "critique"}
    assert outputs["ingest"] == {"document": "DOC", "extra": 1}
    assert outputs["summarize"] == {"summary": "SUM"}
    assert outputs["critique"] == {"critique": "OK"}


def test_run_accepts_per_node_external_inputs():
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())
    sup = Supervisor(_plan(dag, manifests), manifests, invoke_fn=fake, arns=_ARNS)

    sup.run({"ingest": {"uri": "s3://explicit"}})

    assert fake.payload_for("arn:ingest") == {"uri": "s3://explicit"}


def test_run_passes_manifest_qualifier():
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())
    sup = Supervisor(_plan(dag, manifests), manifests, invoke_fn=fake, arns=_ARNS)

    sup.run({"uri": "s3://doc"})

    by_arn = {arn: qualifier for arn, qualifier, _s, _p in fake.calls}
    assert by_arn["arn:summarize"] == "PROD"  # registry.qualifier honored
    assert by_arn["arn:ingest"] == "DEFAULT"  # default qualifier


# -- session id: stable + >= 33 chars ---------------------------------------
def test_session_id_is_stable_and_long_across_invokes():
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())
    sup = Supervisor(_plan(dag, manifests), manifests, invoke_fn=fake, arns=_ARNS)

    sup.run({"uri": "s3://doc"})

    session_ids = {session_id for _a, _q, session_id, _p in fake.calls}
    # (d) one stable session id propagated to every invoke, and it is >= 33 chars.
    assert len(fake.calls) == 3
    assert session_ids == {sup.session_id}
    assert len(sup.session_id) >= 33


def test_generated_session_id_defaults_long():
    dag, manifests = _chain()
    sup = Supervisor(_plan(dag, manifests), manifests, invoke_fn=FakeInvoker({}))
    assert len(sup.session_id) >= 33


def test_supplied_session_id_is_used():
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())
    fixed = "S" * 40
    sup = Supervisor(
        _plan(dag, manifests), manifests, invoke_fn=fake, arns=_ARNS, session_id=fixed
    )
    sup.run({"uri": "s3://doc"})
    assert {s for _a, _q, s, _p in fake.calls} == {fixed}


# -- output validation ------------------------------------------------------
def test_validate_output_flat_required_flag():
    validate_output({"summary": "hi"}, {"summary": {"type": "string", "required": True}})
    with pytest.raises(SchemaError, match="missing required field"):
        validate_output({"other": 1}, {"summary": {"type": "string", "required": True}})


def test_validate_output_nested_required_list():
    schema = {"properties": {"summary": {"type": "string"}}, "required": ["summary"]}
    validate_output({"summary": "hi"}, schema)
    with pytest.raises(SchemaError):
        validate_output({}, schema)


def test_validate_output_rejects_non_dict():
    with pytest.raises(SchemaError, match="must be a JSON object"):
        validate_output("not-a-dict", {"o": {"required": True}})


def test_validate_output_no_required_fields_passes():
    validate_output({}, {"summary": {"type": "string"}})


def test_run_raises_schema_error_when_required_output_missing():
    dag, manifests = _chain()
    broken = _fake_outputs()
    broken["arn:summarize"] = {"not_summary": "oops"}  # violates required ["summary"]
    fake = FakeInvoker(broken)
    sup = Supervisor(_plan(dag, manifests), manifests, invoke_fn=fake, arns=_ARNS)

    with pytest.raises(SchemaError, match="summary"):
        sup.run({"uri": "s3://doc"})


# -- failure tolerance: on_error / max_attempts / blocked-skip --------------
class FlakyInvoker(FakeInvoker):
    """A FakeInvoker that raises for the first ``fail_times`` calls to a given arn."""

    def __init__(self, outputs_by_arn, *, flaky_arn, fail_times, exc=None):
        super().__init__(outputs_by_arn)
        self._flaky_arn = flaky_arn
        self._fail_times = fail_times
        self._seen = 0
        self._exc = exc or RuntimeError("transport boom")

    def __call__(self, arn, qualifier, session_id, payload_bytes):
        if arn == self._flaky_arn:
            self._seen += 1
            if self._seen <= self._fail_times:
                raise self._exc
        return super().__call__(arn, qualifier, session_id, payload_bytes)


def test_run_default_still_raises_on_schema_error_regression():
    # Regression guard for the fail-fast contract: the DEFAULT path must raise unchanged.
    dag, manifests = _chain()
    broken = _fake_outputs()
    broken["arn:summarize"] = {"not_summary": "oops"}
    fake = FakeInvoker(broken)
    sup = Supervisor(_plan(dag, manifests), manifests, invoke_fn=fake, arns=_ARNS)
    with pytest.raises(SchemaError, match="summary"):
        sup.run({"uri": "s3://doc"})


def test_on_error_record_writes_failed_record_and_continues():
    # summarize's output violates its schema; with on_error='record' the run does NOT raise.
    dag, manifests = _chain()
    broken = _fake_outputs()
    broken["arn:summarize"] = {"not_summary": "oops"}
    fake = FakeInvoker(broken)
    store = InProcessStateStore()
    sup = Supervisor(
        _plan(dag, manifests),
        manifests,
        invoke_fn=fake,
        arns=_ARNS,
        state_store=store,
        on_error="record",
    )

    outputs = sup.run({"uri": "s3://doc"})

    # ingest completed and is returned; summarize failed + critique blocked -> pruned from return.
    assert set(outputs) == {"ingest"}
    assert outputs["ingest"] == {"document": "DOC", "extra": 1}
    assert "summarize" not in outputs
    assert "critique" not in outputs

    # exactly one failed record was written for summarize.
    failed = [r for r in store.records() if r.node == "summarize" and r.status == "failed"]
    assert len(failed) == 1

    # summary() is operator-legible and read-only.
    s = sup.summary()
    assert s["completed"] == 1
    assert s["total"] == 3
    assert "summarize" in s["failed"]
    assert "critique" in s["failed"]


def test_max_attempts_retries_flaky_node_then_succeeds():
    # summarize's transport fails twice, then succeeds on the 3rd attempt.
    dag, manifests = _chain()
    fake = FlakyInvoker(_fake_outputs(), flaky_arn="arn:summarize", fail_times=2)
    store = InProcessStateStore()
    sup = Supervisor(
        _plan(dag, manifests),
        manifests,
        invoke_fn=fake,
        arns=_ARNS,
        state_store=store,
        on_error="record",
        max_attempts=3,
    )

    outputs = sup.run({"uri": "s3://doc"})

    # all three nodes completed; the retried node re-invoked the SAME arn (never branched).
    assert set(outputs) == {"ingest", "summarize", "critique"}
    assert outputs["summarize"] == {"summary": "SUM"}
    assert outputs["critique"] == {"critique": "OK"}
    # summarize was stored exactly once (only the successful attempt is put()).
    summarize = [r for r in store.records() if r.node == "summarize"]
    assert len(summarize) == 1
    assert summarize[0].status == "validated"


def test_max_attempts_exhausted_records_single_failed_record():
    dag, manifests = _chain()
    fake = FlakyInvoker(_fake_outputs(), flaky_arn="arn:summarize", fail_times=99)
    store = InProcessStateStore()
    sup = Supervisor(
        _plan(dag, manifests),
        manifests,
        invoke_fn=fake,
        arns=_ARNS,
        state_store=store,
        on_error="record",
        max_attempts=2,
    )

    sup.run({"uri": "s3://doc"})

    # exactly one failed record after exhausting 2 attempts (not one per attempt).
    failed = [r for r in store.records() if r.node == "summarize" and r.status == "failed"]
    assert len(failed) == 1
    # the transport was actually retried max_attempts times.
    assert fake._seen == 2


def test_blocked_downstream_skipped_and_independent_branch_returns():
    # diamond a->b, a->c, b->d, c->d. 'b' fails -> d is blocked on b; c is independent, returns.
    dag, manifests = _diamond()
    fake = FlakyInvoker(_diamond_outputs(), flaky_arn="arn:b", fail_times=99)
    store = InProcessStateStore()
    sup = Supervisor(
        _plan(dag, manifests),
        manifests,
        invoke_fn=fake,
        arns=_DIAMOND_ARNS,
        state_store=store,
        on_error="record",
    )

    outputs = sup.run({"seed": "s3://doc"})

    # a and c completed (independent branch); b failed; d blocked on b -> both pruned from return.
    assert set(outputs) == {"a", "c"}
    assert outputs["a"] == {"doc": "DOC"}
    assert outputs["c"] == {"sc": "SC"}
    assert "b" not in outputs
    assert "d" not in outputs

    # d's failed record carries a blocked_on reason naming its missing producer b.
    d_failed = [r for r in store.records() if r.node == "d" and r.status == "failed"]
    assert len(d_failed) == 1
    assert "b" in (d_failed[0].blocked_on or "")

    # summary_line is operator-legible.
    line = sup.summary_line()
    assert line.startswith("completed 2/4")
    assert "node d blocked on b" in line


def test_run_default_max_attempts_one_does_not_retry():
    # Default max_attempts=1 with on_error='record' means no retry (single invoke, then fail).
    dag, manifests = _chain()
    fake = FlakyInvoker(_fake_outputs(), flaky_arn="arn:summarize", fail_times=99)
    sup = Supervisor(
        _plan(dag, manifests), manifests, invoke_fn=fake, arns=_ARNS, on_error="record"
    )
    sup.run({"uri": "s3://doc"})
    assert fake._seen == 1  # invoked once, no retry


def test_invalid_on_error_and_max_attempts_rejected():
    dag, manifests = _chain()
    with pytest.raises(ValueError, match="on_error"):
        Supervisor(_plan(dag, manifests), manifests, on_error="nope")
    with pytest.raises(ValueError, match="max_attempts"):
        Supervisor(_plan(dag, manifests), manifests, max_attempts=0)


# -- arn fallback -----------------------------------------------------------
def test_arn_falls_back_to_manifest_registry():
    dag, manifests = _chain()
    manifests["ingest"].registry["agent_runtime_arn"] = "arn:from-manifest"
    outputs = {
        "arn:from-manifest": {"document": "DOC"},
        "arn:summarize": {"summary": "S"},
        "arn:critique": {"critique": "OK"},
    }
    fake = FakeInvoker(outputs)
    # Supply arns only for the two downstream nodes; ingest falls back to its manifest arn.
    sup = Supervisor(
        _plan(dag, manifests),
        manifests,
        invoke_fn=fake,
        arns={"summarize": "arn:summarize", "critique": "arn:critique"},
    )
    sup.run({"uri": "s3://doc"})
    assert fake.payload_for("arn:from-manifest") == {"uri": "s3://doc"}


# -- AI-10: dispatch-time ARN binding-integrity assertion -------------------
def test_arn_resolver_default_none_leaves_happy_path_unchanged():
    # (i) With no arn_resolver (default), the run behaves byte-for-byte as before.
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())
    sup = Supervisor(_plan(dag, manifests), manifests, invoke_fn=fake, arns=_ARNS)

    outputs = sup.run({"uri": "s3://doc"})

    assert set(outputs) == {"ingest", "summarize", "critique"}
    assert outputs["critique"] == {"critique": "OK"}
    assert len(fake.calls) == 3  # every node invoked, no extra integrity gating


def test_arn_resolver_default_none_fail_fast_regression_still_raises():
    # (i) The default fail-fast contract is untouched when no arn_resolver is passed.
    dag, manifests = _chain()
    broken = _fake_outputs()
    broken["arn:summarize"] = {"not_summary": "oops"}
    fake = FakeInvoker(broken)
    sup = Supervisor(_plan(dag, manifests), manifests, invoke_fn=fake, arns=_ARNS)
    with pytest.raises(SchemaError, match="summary"):
        sup.run({"uri": "s3://doc"})


def test_placeholder_arn_records_failure_and_does_not_invoke():
    # (ii) A node whose compiled ARN is the unprovisioned placeholder is NOT invoked; under
    # on_error='record' a failed record is written and independent upstreams still complete.
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())
    store = InProcessStateStore()
    # Supply arns only for ingest + summarize; 'critique' falls back to the placeholder.
    sup = Supervisor(
        _plan(dag, manifests),
        manifests,
        invoke_fn=fake,
        arns={"ingest": "arn:ingest", "summarize": "arn:summarize"},
        state_store=store,
        on_error="record",
    )

    outputs = sup.run({"uri": "s3://doc"})

    # critique never invoked (its arn is the placeholder); ingest + summarize completed.
    assert set(outputs) == {"ingest", "summarize"}
    assert "arn:critique" not in {arn for arn, _q, _s, _p in fake.calls}
    assert _ARN_PLACEHOLDER not in {arn for arn, _q, _s, _p in fake.calls}

    failed = [r for r in store.records() if r.node == "critique" and r.status == "failed"]
    assert len(failed) == 1
    assert "no provisioned runtime ARN" in failed[0].output["error"]


def test_placeholder_arn_default_raises_clear_error():
    # (ii) On the default fail-fast path, an unprovisioned ARN raises a clear binding error.
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())
    sup = Supervisor(
        _plan(dag, manifests),
        manifests,
        invoke_fn=fake,
        arns={"summarize": "arn:summarize", "critique": "arn:critique"},  # ingest -> placeholder
    )
    with pytest.raises(RuntimeError, match="no provisioned runtime ARN"):
        sup.run({"uri": "s3://doc"})
    assert fake.calls == []  # ingest (source) failed the integrity gate before any invoke


def test_arn_resolver_mismatch_records_failure_and_does_not_invoke_refetched_arn():
    # (iii) A resolver returning a DIFFERENT arn fails the integrity assertion; the run must NOT
    # invoke the re-fetched arn (no in-run rebind of the frozen binding).
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())
    store = InProcessStateStore()

    def resolver(node, manifest):
        if node == "summarize":
            return "arn:summarize-REBOUND"  # authoritative differs from the compiled arn
        return _ARNS[node]

    sup = Supervisor(
        _plan(dag, manifests),
        manifests,
        invoke_fn=fake,
        arns=_ARNS,
        state_store=store,
        on_error="record",
        arn_resolver=resolver,
    )

    outputs = sup.run({"uri": "s3://doc"})

    # summarize failed (stale binding); critique blocked on it; only ingest returns.
    assert set(outputs) == {"ingest"}
    invoked = {arn for arn, _q, _s, _p in fake.calls}
    assert "arn:summarize" not in invoked  # the compiled arn was not invoked
    assert "arn:summarize-REBOUND" not in invoked  # and CRITICALLY the re-fetched arn was not

    failed = [r for r in store.records() if r.node == "summarize" and r.status == "failed"]
    assert len(failed) == 1
    assert "stale" in failed[0].output["error"]


def test_arn_resolver_mismatch_default_raises_re_compile_error():
    # (iii) On the default fail-fast path, a stale compiled ARN raises rather than rebinding.
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())

    def resolver(node, manifest):
        return "arn:ingest-REBOUND" if node == "ingest" else _ARNS[node]

    sup = Supervisor(
        _plan(dag, manifests), manifests, invoke_fn=fake, arns=_ARNS, arn_resolver=resolver
    )
    with pytest.raises(RuntimeError, match="stale; re-compile"):
        sup.run({"uri": "s3://doc"})
    assert fake.calls == []  # failed integrity gate before any invoke


def test_arn_resolver_confirming_compiled_arn_invokes_normally():
    # (iv) A resolver that CONFIRMS the compiled arn lets every node invoke as usual.
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())
    seen = []

    def resolver(node, manifest):
        seen.append(node)
        return _ARNS[node]  # authoritative == compiled for every node

    sup = Supervisor(
        _plan(dag, manifests), manifests, invoke_fn=fake, arns=_ARNS, arn_resolver=resolver
    )

    outputs = sup.run({"uri": "s3://doc"})

    assert set(outputs) == {"ingest", "summarize", "critique"}
    assert outputs["critique"] == {"critique": "OK"}
    assert sorted(seen) == ["critique", "ingest", "summarize"]  # resolver consulted per node
    assert len(fake.calls) == 3


# -- C-3: pre-dispatch structural validation (dangling AgentRef) ------------
def test_dispatch_rejects_dangling_agentref_before_invoke():
    # A plan whose wiring names a producer NOT present in plan.order is structurally invalid;
    # the shipped RunGraph.validate must reject it at construction, before any invoke fires.
    dag, manifests = _chain()
    good = _plan(dag, manifests)
    # Inject a dangling wire: 'critique' consumes a 'ghost' node that is not in plan.order.
    dangling_wiring = dict(good.wiring)
    dangling_wiring["critique"] = list(dangling_wiring.get("critique", [])) + [
        AgentRef(producer="ghost", path="$.x", input_name="summary")
    ]
    bad_plan = types.SimpleNamespace(order=list(good.order), wiring=dangling_wiring)

    fake = FakeInvoker(_fake_outputs())
    # The structural gate fires in __init__ (before run), so no invoke ever happens.
    with pytest.raises(RunGraphError, match="ghost"):
        Supervisor(bad_plan, manifests, invoke_fn=fake, arns=_ARNS)
    assert fake.calls == []  # no invoke fired: rejected pre-dispatch


def test_dispatch_accepts_well_formed_plan_structure():
    # Regression guard: a well-formed plan passes the C-3 structural gate untouched.
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())
    sup = Supervisor(_plan(dag, manifests), manifests, invoke_fn=fake, arns=_ARNS)
    outputs = sup.run({"uri": "s3://doc"})
    assert set(outputs) == {"ingest", "summarize", "critique"}


# -- state store seam: diamond DAG + graph-aware context + resume -----------
def _diamond():
    """A diamond DAG a -> b, a -> c, b -> d, c -> d (d fans in from both b and c)."""
    dag = AgentDAG()
    for n in ["a", "b", "c", "d"]:
        dag.add_node(n)
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")

    def _m(name, inputs, outputs, deps=None):
        spec = {"depends_on": deps} if deps else {}
        return AgentManifest.from_dict(
            {
                "name": name,
                "registry": {"container_uri": "x", "protocol": "HTTP"},
                "contract": {"inputs": inputs, "outputs": outputs},
                "spec": spec,
            }
        )

    manifests = {
        "a": _m(
            "a", {"seed": {"type": "string"}}, {"doc": {"type": "string", "required": True}}
        ),
        "b": _m(
            "b",
            {"doc": {"type": "string"}},
            {"sb": {"type": "string", "required": True}},
            [{"from": "a.doc", "to": "doc"}],
        ),
        "c": _m(
            "c",
            {"doc": {"type": "string"}},
            {"sc": {"type": "string", "required": True}},
            [{"from": "a.doc", "to": "doc"}],
        ),
        "d": _m(
            "d",
            {"sb": {"type": "string"}, "sc": {"type": "string"}},
            {"sd": {"type": "string", "required": True}},
            [{"from": "b.sb", "to": "sb"}, {"from": "c.sc", "to": "sc"}],
        ),
    }
    return dag, manifests


_DIAMOND_ARNS = {"a": "arn:a", "b": "arn:b", "c": "arn:c", "d": "arn:d"}


def _diamond_outputs():
    return {
        "arn:a": {"doc": "DOC"},
        "arn:b": {"sb": "SB"},
        "arn:c": {"sc": "SC"},
        "arn:d": {"sd": "SD"},
    }


def test_run_diamond_threads_through_state_store_and_context():
    dag, manifests = _diamond()
    fake = FakeInvoker(_diamond_outputs())
    store = InProcessStateStore()
    sup = Supervisor(
        _plan(dag, manifests), manifests, invoke_fn=fake, arns=_DIAMOND_ARNS, state_store=store
    )

    outputs = sup.run({"seed": "s3://doc"})

    # final outputs cover every node, fanning both b and c into d.
    assert set(outputs) == {"a", "b", "c", "d"}
    assert outputs["a"] == {"doc": "DOC"}
    assert outputs["d"] == {"sd": "SD"}
    assert fake.payload_for("arn:d") == {"sb": "SB", "sc": "SC"}

    # graph-aware context of the sink is its transitive upstream outputs (a, b, c).
    ctx = sup.context("d")
    assert set(ctx) == {"a", "b", "c"}
    assert ctx["a"] == {"doc": "DOC"}
    assert ctx["b"] == {"sb": "SB"}
    assert ctx["c"] == {"sc": "SC"}


def test_run_resumes_and_skips_already_completed_node():
    dag, manifests = _diamond()
    fake = FakeInvoker(_diamond_outputs())
    store = InProcessStateStore()
    store.put("a", {"doc": "PRESET"})  # 'a' already recorded -> completed()

    sup = Supervisor(
        _plan(dag, manifests), manifests, invoke_fn=fake, arns=_DIAMOND_ARNS, state_store=store
    )
    outputs = sup.run({"seed": "s3://doc"})

    # 'a' was skipped (never invoked); its preset output threaded into b and c.
    invoked = {arn for arn, _q, _s, _p in fake.calls}
    assert "arn:a" not in invoked
    assert invoked == {"arn:b", "arn:c", "arn:d"}
    assert outputs["a"] == {"doc": "PRESET"}
    assert fake.payload_for("arn:b") == {"doc": "PRESET"}
