"""Tests for the program/portfolio scope stack (S12-G9).

The programs index is a PURE-GOV, READ-ONLY projection over the per-run
precedent notes — the program-grain analogue of ``render_precedent_hub``. It
aggregates runs by program, is byte-identical on regeneration, and drives no
dispatch (INV-5).
"""

from __future__ import annotations

import types

from concursus.governor.scope import (
    SCOPE_LEVELS,
    ScopeAddress,
    ScopeError,
    build_programs_index,
    director_leverage_view,
    programs_dir,
    render_programs_index,
)
from concursus.state.distill import distill_run
from concursus.state.statestore import Record


# --------------------------------------------------------------------------- helpers
def _distill(vault, trail_id, *, completed, total):
    """Distill one finished run into a precedent note under ``<vault>/precedents/``."""
    outcome = {
        "total": total,
        "completed": completed,
        "completed_nodes": [f"n{i}" for i in range(completed)],
        "failed": {} if completed >= total else {"n_fail": ""},
    }
    result = {f"n{i}": {"ok": True} for i in range(completed)}
    return distill_run(
        result, [], vault_path=str(vault), trail_id=trail_id, outcome=outcome
    )


# --------------------------------------------------------------------------- ScopeAddress
def test_scope_address_push_and_trail_id_roundtrip():
    addr = ScopeAddress()
    addr = addr.push("acme").push("payments").push("fraud").push("run-1")
    assert addr.org == "acme"
    assert addr.portfolio == "payments"
    assert addr.program == "fraud"
    assert addr.task == "run-1"
    assert addr.depth() == 4
    tid = addr.to_trail_id()
    assert ScopeAddress.from_trail_id(tid).to_dict() == addr.to_dict()


def test_scope_address_program_key_is_org_portfolio_program():
    addr = ScopeAddress.from_trail_id("acme.payments.fraud.run-9")
    assert addr.program_key() == "acme.payments.fraud"


def test_scope_push_beyond_task_raises():
    addr = ScopeAddress().push("a").push("b").push("c").push("d")
    try:
        addr.push("e")
    except ScopeError:
        pass
    else:
        raise AssertionError("expected ScopeError on over-deep push")


def test_scope_levels_order():
    assert SCOPE_LEVELS == ("org", "portfolio", "program", "task")


# --------------------------------------------------------------------------- programs index
def test_programs_index_aggregates_runs_by_program(tmp_path):
    # Two programs under one portfolio, plus a second portfolio.
    _distill(tmp_path, "acme.payments.fraud.run-1", completed=3, total=3)
    _distill(tmp_path, "acme.payments.fraud.run-2", completed=1, total=3)
    _distill(tmp_path, "acme.payments.chargeback.run-1", completed=2, total=2)

    index = build_programs_index(tmp_path)

    assert set(index) == {"acme.payments.fraud", "acme.payments.chargeback"}
    fraud = index["acme.payments.fraud"]
    assert fraud["org"] == "acme"
    assert fraud["portfolio"] == "payments"
    assert fraud["program"] == "fraud"
    assert fraud["runs"] == ["acme.payments.fraud.run-1", "acme.payments.fraud.run-2"]
    assert fraud["run_count"] == 2
    # status rollup: one completed run + one partial run
    assert fraud["status_counts"]["completed"] == 1
    assert fraud["status_counts"]["partial"] == 1

    cb = index["acme.payments.chargeback"]
    assert cb["run_count"] == 1
    assert cb["status_counts"]["completed"] == 1


def test_programs_index_is_idempotent_readonly_projection(tmp_path):
    _distill(tmp_path, "acme.payments.fraud.run-1", completed=3, total=3)
    _distill(tmp_path, "acme.risk.velocity.run-1", completed=2, total=4)

    p1 = render_programs_index(tmp_path)
    first = programs_dir(tmp_path).joinpath("_index.md").read_text(encoding="utf-8")

    # Regenerating from the SAME notes yields a byte-identical projection...
    p2 = render_programs_index(tmp_path)
    second = programs_dir(tmp_path).joinpath("_index.md").read_text(encoding="utf-8")
    assert p1 == p2
    assert first == second

    # ...and it drove no dispatch: no runs/ tree, no new precedent notes created.
    assert not (tmp_path / "runs").exists()
    prec_notes = [p.name for p in (tmp_path / "precedents").glob("*.md")]
    assert len(prec_notes) == 2  # the two distilled runs, unchanged by rendering


def test_render_programs_index_slipbox_form_has_frontmatter(tmp_path):
    _distill(tmp_path, "acme.payments.fraud.run-1", completed=1, total=1)
    render_programs_index(tmp_path, slipbox_form=True, date="2026-07-11")
    text = programs_dir(tmp_path).joinpath("_index.md").read_text(encoding="utf-8")
    assert text.startswith("---\n")
    assert "entry_point" in text


def test_programs_index_empty_vault(tmp_path):
    assert build_programs_index(tmp_path) == {}
    render_programs_index(tmp_path)
    text = programs_dir(tmp_path).joinpath("_index.md").read_text(encoding="utf-8")
    assert "no programs" in text.lower()


# --------------------------------------------------------------------------- leverage view
def test_director_leverage_view_is_one_to_many(tmp_path):
    _distill(tmp_path, "acme.payments.fraud.run-1", completed=3, total=3)
    _distill(tmp_path, "acme.payments.fraud.run-2", completed=1, total=3)
    _distill(tmp_path, "acme.risk.velocity.run-1", completed=2, total=4)

    view = director_leverage_view(tmp_path)
    assert view["program_count"] == 2
    assert view["run_count"] == 3
    assert view["runs_per_program"] == {
        "acme.payments.fraud": 2,
        "acme.risk.velocity": 1,
    }
    # cross-program status rollup sums the per-program tallies
    assert view["status_counts"]["completed"] == 1
    assert view["status_counts"]["partial"] == 2
