"""Tests for ``get_run_snapshot`` + ``redact_snapshot`` — the at-rest run-slice read (rundb).

``get_run_snapshot`` is a PURE, offline read projection over one run's note SSOT: it returns an
ordered, JSON-serializable slice optionally narrowed by agent (node) and/or a step window. These
tests pin the contract the task requires: an agent filter selects only that node's records, a step
window selects only the records in the inclusive ordinal window, and the optional ``redact_snapshot``
egress helper masks a seeded secret (and warns).
"""

from __future__ import annotations

import json
import logging
import re

from concursus.state.filevault import FileVaultStateStore
from concursus.state.rundb import get_run_snapshot, redact_snapshot


def _run(vault, session, *, slipbox_form=False):
    """Drive a small multi-agent run (``analyze`` runs twice — one retry) and return the run dir.

    The snapshot's ``step`` is the ordinal in the deterministic AT-REST order :func:`load_records`
    returns (the append-only notes persist no wall-clock seq), so the tests below derive the exact
    node->step mapping from the full snapshot rather than hardcoding it.
    """
    store = FileVaultStateStore.from_config(
        vault_path=vault, session_id=session, slipbox_form=slipbox_form, date="2026-07-21"
    )
    store.put("ingest", {"document": "alpha beta"}, meta={"producer": "ingest"})
    store.put("analyze", {"score": 1}, meta={"producer": "analyze", "consumes": ["ingest:$.document"]})
    store.put("analyze", {"score": 2}, meta={"producer": "analyze", "consumes": ["ingest:$.document"]})
    store.put("summarize", {"summary": "gamma"}, meta={"producer": "summarize", "consumes": ["analyze:$.score"]})
    return store._dir


def test_snapshot_is_json_serializable_and_ordered(tmp_path):
    run_dir = _run(tmp_path, "concursus-" + "s" * 40)
    snap = get_run_snapshot(run_dir)

    assert snap["total"] == 4
    assert snap["count"] == 4
    # Steps are a contiguous 1..N ordinal over the whole run, in ascending order.
    assert [r["step"] for r in snap["records"]] == [1, 2, 3, 4]
    assert {r["node"] for r in snap["records"]} == {"ingest", "analyze", "summarize"}
    # A pure read projection must be JSON-serializable (no live handles, enums -> str).
    json.dumps(snap)


def test_agent_filter_selects_only_that_node(tmp_path):
    run_dir = _run(tmp_path, "concursus-" + "t" * 40)
    full = get_run_snapshot(run_dir)
    analyze_steps = sorted(r["step"] for r in full["records"] if r["node"] == "analyze")

    snap = get_run_snapshot(run_dir, agent="analyze")
    assert snap["agent"] == "analyze"
    assert {r["node"] for r in snap["records"]} == {"analyze"}
    # Exactly the analyze records, and the whole-run ordinals are preserved (not re-based to 1..k).
    assert [r["step"] for r in snap["records"]] == analyze_steps
    assert sorted(r["attempt"] for r in snap["records"]) == [1, 2]  # both attempts of the retry
    assert snap["total"] == 4  # total is the full run, not the filtered count

    # An agent with no records yields an empty (but well-formed) slice.
    empty = get_run_snapshot(run_dir, agent="nope")
    assert empty["count"] == 0 and empty["records"] == []


def test_step_window_selects_inclusive_ordinal_range(tmp_path):
    run_dir = _run(tmp_path, "concursus-" + "u" * 40)
    by_step = {r["step"]: r["node"] for r in get_run_snapshot(run_dir)["records"]}

    window = get_run_snapshot(run_dir, step=(2, 3))
    assert [r["step"] for r in window["records"]] == [2, 3]
    assert [r["node"] for r in window["records"]] == [by_step[2], by_step[3]]

    single = get_run_snapshot(run_dir, step=4)
    assert [r["step"] for r in single["records"]] == [4]
    assert single["records"][0]["node"] == by_step[4]

    # Open-ended lower bound: everything from step 3 onward.
    tail = get_run_snapshot(run_dir, step=(3, None))
    assert [r["step"] for r in tail["records"]] == [3, 4]

    # Combined agent + window: analyze records intersected with step 1..2.
    analyze_steps = sorted(r["step"] for r in get_run_snapshot(run_dir, agent="analyze")["records"])
    lo = analyze_steps[0]
    combined = get_run_snapshot(run_dir, agent="analyze", step=(lo, lo))
    assert [r["step"] for r in combined["records"]] == [lo]


def test_absent_run_dir_yields_empty_snapshot(tmp_path):
    snap = get_run_snapshot(tmp_path / "no_such_run")
    assert snap["total"] == 0 and snap["count"] == 0 and snap["records"] == []


def test_redact_masks_seeded_secret_and_warns(tmp_path, caplog):
    run_dir = _run(tmp_path, "concursus-" + "v" * 40)
    # Seed a secret into a real run record's output, then read it back through the snapshot.
    store = FileVaultStateStore.from_config(
        vault_path=tmp_path, session_id="concursus-" + "v" * 40, slipbox_form=False
    )
    store.put("leak", {"token": "AKIA-SEED-SECRET-123", "note": "harmless"})
    snap = get_run_snapshot(run_dir, agent="leak")
    assert "AKIA-SEED-SECRET-123" in json.dumps(snap)  # present before redaction

    pattern = re.compile(r"AKIA-[A-Z0-9-]+")
    with caplog.at_level(logging.WARNING):
        safe = redact_snapshot(snap, pattern)

    dumped = json.dumps(safe)
    assert "AKIA-SEED-SECRET-123" not in dumped
    assert "[REDACTED]" in dumped
    assert "harmless" in dumped  # non-matching content is untouched
    assert any("masked" in rec.message for rec in caplog.records)  # egress WARN fired

    # No match -> unchanged copy, no warning.
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        unchanged = redact_snapshot({"a": "clean"}, r"NOTHING")
    assert unchanged == {"a": "clean"}
    assert not caplog.records


def test_snapshot_does_not_mutate_run(tmp_path):
    """INV-5: the read projection mutates nothing on disk (byte-for-byte stable notes)."""
    run_dir = _run(tmp_path, "concursus-" + "w" * 40)
    before = {p.name: p.read_bytes() for p in sorted(run_dir.glob("*.md"))}
    get_run_snapshot(run_dir)
    get_run_snapshot(run_dir, agent="analyze", step=(1, 2))
    after = {p.name: p.read_bytes() for p in sorted(run_dir.glob("*.md"))}
    assert before == after
