"""Tests for the FZ 35e4a3a1b SPIKE A capture front — the CaptureEnvelope + dispatcher.

SPIKE A is the evidence gate for the note-capture track: it validates that a source-agnostic
``CaptureEnvelope`` + a ~dict dispatcher can route an artifact to the ALREADY-SHIPPED
``filevault``/``distill`` writers, targeting Hive's OWN memory store (a run dir) — no new runtime,
no LangGraph. These exercise the plan round-trip (envelope -> capture() -> a valid ``_plan.md``
that is NOT a run record) and the dispatcher's unknown-source guard.
"""

from __future__ import annotations

import pytest

from concursus import AgentDAG, AgentManifest
from concursus.assemble.assemble import OrchestrationAssembler
from concursus.state.capture import (
    PLAN,
    CaptureEnvelope,
    CaptureError,
    adapt_plan,
    capture,
)
from concursus.state.filevault import _note_to_record


def _agent(name, inputs, outputs, depends_on=None):
    reg = {"container_uri": "img", "protocol": "HTTP", "entry": f"agents.{name}:run"}
    data = {"name": name, "registry": reg, "contract": {"inputs": inputs, "outputs": outputs}}
    if depends_on is not None:
        data["spec"] = {"depends_on": depends_on}
    return AgentManifest.from_dict(data)


def _frozen_plan():
    """A real frozen 2-node plan: ingest -> summarize (assembled, not stubbed)."""
    dag = AgentDAG()
    dag.add_node("ingest")
    dag.add_node("summarize")
    dag.add_edge("ingest", "summarize")
    manifests = {
        "ingest": _agent("ingest", {"uri": {"type": "string"}}, {"document": {"type": "string"}}),
        "summarize": _agent(
            "summarize",
            {"document": {"type": "string"}},
            {"summary": {"type": "string"}},
            depends_on=[{"from": "ingest.document", "to": "document"}],
        ),
    }
    return OrchestrationAssembler().assemble(dag, manifests)


# -- SA·T2: the plan round-trip through the envelope + dispatcher -----------
def test_adapt_plan_captures_a_valid_plan_note(tmp_path):
    """adapt_plan -> capture() writes <run_dir>/_plan.md via the shipped seam."""
    env = adapt_plan(_frozen_plan(), str(tmp_path), trail_id="run_x", date="2026-07-16")
    assert isinstance(env, CaptureEnvelope) and env.source_kind == PLAN

    path = capture(env)
    assert path.endswith("_plan.md")
    text = open(path).read()
    # a real slipbox plan note: the frozen topology is rendered + it is stamped a non-record.
    assert "```mermaid" in text
    for node in ("ingest", "summarize"):
        assert node in text
    assert "concursus_note_kind" in text


def test_captured_plan_note_is_not_parsed_back_as_a_record(tmp_path):
    """INV: the plan note is a projection, NOT a run Record — it never leaks into replay."""
    path = capture(adapt_plan(_frozen_plan(), str(tmp_path), trail_id="run_x"))
    with pytest.raises(ValueError):
        _note_to_record(open(path).read())


def test_capture_is_deterministic_and_idempotent(tmp_path):
    """Re-capturing the same plan overwrites the single _plan.md (dedup by filename)."""
    env = adapt_plan(_frozen_plan(), str(tmp_path), trail_id="run_x", date="2026-07-16")
    p1 = capture(env)
    p2 = capture(env)
    assert p1 == p2
    assert list(tmp_path.glob("_plan.md"))  # exactly the one note


# -- SA·T1: the dispatcher guards an unknown / not-yet-wired source ---------
def test_capture_rejects_unknown_source_kind(tmp_path):
    env = CaptureEnvelope("binding", {"x": 1}, str(tmp_path))  # a named-but-unwired source
    with pytest.raises(CaptureError):
        capture(env)


def test_envelope_requires_source_kind_and_run_dir(tmp_path):
    with pytest.raises(CaptureError):
        CaptureEnvelope("", {}, str(tmp_path))
    with pytest.raises(CaptureError):
        CaptureEnvelope(PLAN, {}, "")


# -- Phase 1 T3: capture_payload_note + redact -----------------------------
def test_adapt_payload_captures_a_redacted_non_record_note(tmp_path):
    from concursus.state.capture import adapt_payload
    from concursus.state.filevault import _note_to_record

    payload = {"doc": "s3://x", "sop": ["read", "summarize"], "customer_id": "C-90431"}
    env = adapt_payload("summarize", payload, str(tmp_path), trust_tier="LOW", trail_id="run_x")
    path = capture(env)
    assert path.endswith("__payload.md") and "summarize" in path
    text = open(path).read()
    # PII masked; tier + a non-PII field present; stamped a non-record.
    assert "<redacted>" in text and "C-90431" not in text
    assert "LOW" in text and "sop" in text
    assert "concursus_note_kind" in text
    with pytest.raises(ValueError):
        _note_to_record(text)  # never parsed back as a Record


def test_redact_masks_default_and_custom_keys():
    from concursus.state.filevault import redact

    out = redact({"a": 1, "secret": "k", "customer_id": "c"})
    assert out["a"] == 1 and out["secret"] == "<redacted>" and out["customer_id"] == "<redacted>"
    out2 = redact({"keep": 1, "drop": 2}, deny=["drop"])
    assert out2["keep"] == 1 and out2["drop"] == "<redacted>"


# -- Phase 1 T6: reciprocal backlinks --------------------------------------
def test_reciprocal_backlinks_link_producer_to_consumer(tmp_path):
    from concursus.state.filevault import (
        FileVaultStateStore,
        add_reciprocal_backlinks,
        _slug,
    )

    run = tmp_path / "run"
    fv = FileVaultStateStore(run)
    fv.put("ingest", {"document": "d"}, meta={"producer": "ingest"})
    fv.put("summarize", {"summary": "s"},
           meta={"producer": "summarize", "consumes": ["ingest:$.document"]})

    n = add_reciprocal_backlinks(run)
    assert n == 1  # one producer (ingest) amended
    pnote = run / f"{_slug('ingest')}__a1.md"
    text = pnote.read_text()
    assert "## Consumed By" in text
    assert "consumed by summarize" in text
    # idempotent: a second pass does not duplicate the section
    add_reciprocal_backlinks(run)
    assert pnote.read_text().count("## Consumed By") == 1


# -- Phase 1 T4 + T5: the post-run trigger + gate --------------------------
def test_capture_run_captures_plan_payloads_and_backlinks(tmp_path):
    from concursus.state.capture import capture_run, gate_run_dir
    from concursus.state.filevault import FileVaultStateStore

    run = tmp_path / "run"
    fv = FileVaultStateStore(run)
    fv.put("ingest", {"document": "d"}, meta={"producer": "ingest"})
    fv.put("summarize", {"summary": "s"},
           meta={"producer": "summarize", "consumes": ["ingest:$.document"]})

    result = capture_run(
        str(run),
        plan=_frozen_plan(),
        payloads={"summarize": {"doc": "d", "sop": ["read"]}},
        trust_tiers={"summarize": "LOW"},
        trail_id="run_x",
    )
    assert any(p.endswith("_plan.md") for p in result["paths"])
    assert any(p.endswith("__payload.md") and "summarize" in p for p in result["paths"])
    assert result["backlinks"] == 1
    # T5 gate: the run dir is clean (no missing frontmatter, no dangling links).
    verdict = gate_run_dir(str(run))
    assert verdict["ok"] is True and verdict["checked"] > 0


def test_gate_flags_a_dangling_link(tmp_path):
    from concursus.state.capture import gate_run_dir

    run = tmp_path / "run"
    run.mkdir()
    (run / "a__a1.md").write_text("---\nx: 1\n---\n# A\n\n## Related Notes\n\n- [x](missing__a1.md)\n")
    verdict = gate_run_dir(str(run))
    assert verdict["ok"] is False
    assert any("dangling link" in i for i in verdict["issues"])


# -- Phase 3 I1 + I2: the tracks meet (author -> persist -> read back) ------
def _tiered_plan(tmp_path):
    """A frozen plan whose compiler-authored payload_contract tiers 'summarize' at LOW (L1)."""
    from concursus import DeployLedger, TrustGrade
    from concursus.assemble.assemble import OrchestrationAssembler
    from concursus.governor.registry import AgentRegistry
    from concursus.governor.scheduler import TrustLadderScheduler, make_payload_tier

    m = AgentManifest.from_dict({
        "name": "summarize",
        "registry": {"container_uri": "img", "protocol": "HTTP", "entry": "a.summarize:run"},
        "contract": {
            "inputs": {"doc": {"type": "string"}},
            "outputs": {"summary": {"type": "string", "required": True}},
            "context": {"sop": ["read", "summarize"], "guardrails": ["cite"]},
        },
        "trust_seed": TrustGrade.L1_CANARY,
    })
    dag = AgentDAG()
    dag.add_node("summarize")
    ledger = DeployLedger(tmp_path / "l.json")
    ledger.record(name="summarize", fingerprint="f", arn="arn:s", deployed_at="2026-07-01")
    reg = AgentRegistry(ledger)
    reg.register_agent(m)
    sched = TrustLadderScheduler(reg, manifests={"summarize": m})
    return OrchestrationAssembler(payload_tier_fn=make_payload_tier(sched)).assemble(dag, {"summarize": m})


def test_capture_run_persists_the_frozen_payload_contract(tmp_path):
    """I1: capture_run derives payload notes from the plan's compiler-authored payload_contract."""
    from concursus.state.capture import capture_run

    run = tmp_path / "run"
    plan = _tiered_plan(tmp_path)
    result = capture_run(str(run), plan=plan, trail_id="run_x")  # no explicit payloads
    payload_paths = [p for p in result["paths"] if p.endswith("__payload.md")]
    assert len(payload_paths) == 1 and "summarize" in payload_paths[0]
    text = open(payload_paths[0]).read()
    assert "LOW" in text                 # the compiler-authored tier persisted
    assert "sop" in text and "cite" in text  # the tiered static context persisted


def test_load_payload_tiers_reads_back(tmp_path):
    """I2 (FUTURE hook): the persisted tier round-trips back into {node: tier}."""
    from concursus.state.capture import capture_run, load_payload_tiers

    run = tmp_path / "run"
    capture_run(str(run), plan=_tiered_plan(tmp_path), trail_id="run_x")
    tiers = load_payload_tiers(str(run))
    assert tiers.get("summarize") == "LOW"
