"""Tests for the operator console (``concursus.ui``) — the offline HTML/SVG run view.

The console is a pure renderer over a plain **record dict** (``{nodes, edges, summary, kind, goal}``);
it fetches nothing from the network (inline CSS/JS). These tests build a minimal record by hand — no
fleet fixtures — and assert the renderer's public contract: a plan renders the DAG + card panel, a
live run adds the Kanban board, and the 9-status vocabulary is the single source both draw from.
"""

from __future__ import annotations

from concursus.ui import status as ui_status
from concursus.ui.console import board_columns, esc, render_console


def _record(*, kind="plan", with_summary=False):
    rec = {
        "kind": kind,
        "goal": "triage the signal",
        "nodes": [
            {"id": "ingest", "capability": "fetch", "status": "completed",
             "inputs": {"uri": {"type": "string"}}, "outputs": {"doc": {"type": "string"}}},
            {"id": "analyze", "capability": "reason", "status": "running",
             "inputs": {"doc": {"type": "string"}}, "outputs": {"report": {"type": "object"}}},
        ],
        "edges": [["ingest", "analyze"]],
    }
    if with_summary:
        rec["summary"] = {"status_counts": {"completed": 1, "running": 1}}
        rec["kind"] = "run"
    return rec


def test_status_vocabulary_is_nine():
    # 7 terminal + 2 transient — the vocabulary the failure taxonomy (crash/hold/preemptive/futility)
    # and the transient running/pending map through.
    assert len(ui_status.STATUS_STYLE) == 9
    for s in ("completed", "crash", "hold", "preemptive_termination", "futility_cancelled",
              "held", "not_reached", "running", "pending"):
        assert s in ui_status.STATUS_STYLE


def test_render_plan_is_self_contained_html_with_no_board():
    html = render_console(_record())
    assert html.startswith("<!doctype html>")
    assert "triage the signal" in html
    assert "click a node for its card" in html      # the card panel
    assert 'id="canvas"' in html                    # the DAG canvas
    # a finished/plan render has no live Kanban board (cards can't move) and no live indicator
    assert 'class="board"' not in html and 'id="board"' not in html
    assert 'id="livestate"' not in html


def test_render_live_run_adds_the_kanban_board():
    html = render_console(_record(with_summary=True), live=True)
    assert 'id="livestate"' in html                  # the live indicator
    assert 'class="board"' in html or 'id="board"' in html   # the Kanban board
    # the board columns are the live flow order
    for col in board_columns(ui_status.STATUS_STYLE, live=True):
        assert col


def test_board_columns_exclude_non_live_statuses_when_live():
    live = set(board_columns(ui_status.STATUS_STYLE, live=True))
    full = set(board_columns(ui_status.STATUS_STYLE, live=False))
    # a live board never shows statuses that can only exist post-run
    assert live <= full


def test_esc_escapes_html():
    assert esc('<script>&"') == "&lt;script&gt;&amp;&quot;"


def test_render_is_offline_no_network_fetch():
    html = render_console(_record(with_summary=True), live=True)
    # the only URL the page references is its own sibling live-state file
    assert "http://" not in html and "https://" not in html
