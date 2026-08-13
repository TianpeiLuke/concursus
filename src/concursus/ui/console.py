"""Operator console: DAG + node card + live Kanban board, rendered from one record dict.

Implements phases **U0** (vendor the console as a second renderer), **U1** (map the
record onto the DAG so all nine statuses render distinguishably), **U1c** (port the node card
and raise it in a right-hand panel on a node click) and **U1d** (the live Kanban board).

Provenance
----------
The DAG layout and the three-pane shell are adapted from a longest-path layout
``prototype/ui_console.py`` — stdlib-only, hand-rolled SVG, no CDN and no npm. Vendored rather
than imported: a reference layout algorithm is a separate package and the layout is ~40 lines. Its reasoning
vocabulary (``role`` / ``roleTag`` / ``defeats`` / ``prior`` / rejected-branch dimming) has no
Concursus counterpart and is dropped rather than mapped (Q5).

The one dependency rule
-----------------------
This module consumes the **record dict** that ``tests/dag_render.py`` produces and never imports
the record layer, so the dependency runs one way only: shipped presentation code does not reach
into test support. The status colour vocabulary is **not** duplicated here either — it lives in
:mod:`concursus.ui.status`, which ``dag_render`` re-exports, so the mermaid ``classDef``,
the HTML badge, the text report, the DAG node and the Kanban column all read one table and can
never disagree about what a colour means. The only presentation this module owns is the per-status
**glyph** (:data:`MARKS`), because nothing else has an opinion about glyphs.

Deliberately **not** re-exported from ``concursus.__all__``: this is an operator surface,
not part of the compiler's public API, and ``tests/test_api_surface.py`` guards that boundary.

Live versus finished
--------------------
The **Kanban board is live-only** (owner decision, note 48). A Kanban's content is cards *moving
between columns*, which only happens while a run is in flight; a finished run is a static grouping
in which nothing ever moves. The **DAG serves both** modes, and clicking a node raises that node's
full card in the right-hand panel — which is how a finished run still gets card-shaped detail
without a board. Pass ``live=True`` to emit the board and the poll loop.
"""

from __future__ import annotations

import html
import json
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from concursus.ui.status import STATUS_STYLE

__all__ = [
    "COLUMN_ORDER",
    "LIVE_EXCLUDED",
    "MARKS",
    "TRANSIENT",
    "board_columns",
    "render_console",
]

#: Board column order — a **flow** order, deliberately NOT ``dag_render.STATUS_ORDER``.
#: ``STATUS_ORDER`` is documented as "best outcome first" and begins
#: ``completed, running, pending, …``; laid out left-to-right that makes a card travel *backwards*
#: (``pending`` → ``running`` → ``completed`` would be right-to-left). This order reads as the run
#: drains: not-yet-started, in flight, done, then the ways a node can fail to be done.
#: Membership is still validated against the record's own style table by :func:`board_columns`, so
#: the two orders can never disagree about *which* statuses exist (constraint 1).
COLUMN_ORDER: Tuple[str, ...] = (
    "pending",
    "running",
    "completed",
    "hold",
    "held",
    "preemptive_termination",
    "futility_cancelled",
    "crash",
    "not_reached",
)

#: The two non-terminal statuses. They exist only while a run is in flight, so they can never
#: appear in a finished record.
TRANSIENT: Tuple[str, ...] = ("pending", "running")

#: Statuses that cannot occur on a LIVE board: ``statuses_from_store(live=True)`` maps every
#: unresolved node to ``pending``, so ``not_reached`` is never assigned live. A live board therefore
#: has **eight** columns, not nine (constraint 2).
LIVE_EXCLUDED: Tuple[str, ...] = ("not_reached",)

#: Per-status glyph. The only presentation this module owns — colour and label come from the
#: record. Nine distinct glyphs, because the defect Q5 found was that a monitor-terminated
#: node and a completed node rendered the *same* mark.
MARKS: Dict[str, str] = {
    "completed": "\u2713",  # ✓
    "running": "\u25b8",  # ▸
    "pending": "\u00b7",  # ·
    "preemptive_termination": "\u25a0",  # ■ monitor stopped it
    "futility_cancelled": "\u2298",  # ⊘ provably unconsumable
    "crash": "\u2717",  # ✗
    "hold": "\u23f8",  # ⏸ blocked on an input
    "held": "\u26bf",  # ⚿ governance non-dispatch
    "not_reached": "\u25cb",  # ○
}

_FALLBACK_MARK = "?"
_FALLBACK_STYLE = {"label": "unknown", "color": "#6b7280", "fill": "#14161b"}


def esc(value: Any) -> str:
    """HTML-escape ``value``, rendering ``None`` as an em dash."""
    if value is None:
        return "\u2014"
    return html.escape(str(value), quote=True)


def board_columns(style: Optional[Mapping[str, Any]] = None, *, live: bool) -> List[str]:
    """The board's columns, in flow order, for the status vocabulary in use.

    Membership comes from ``style`` (defaulting to the canonical
    :data:`~concursus.ui.status.STATUS_STYLE`) and only the *order* comes from
    :data:`COLUMN_ORDER`; any status the table declares that this module does not know about is
    appended rather than dropped, so a new failure class shows up as an unstyled column instead of
    vanishing. When ``live`` is set, :data:`LIVE_EXCLUDED` statuses are omitted because they cannot
    occur in flight.
    """
    declared = list(STATUS_STYLE if style is None else style)
    excluded = set(LIVE_EXCLUDED) if live else set()
    ordered = [s for s in COLUMN_ORDER if s in declared and s not in excluded]
    extra = [s for s in declared if s not in COLUMN_ORDER and s not in excluded]
    return ordered + extra


def _style_for(style: Mapping[str, Any], status: Optional[str]) -> Mapping[str, Any]:
    entry = (style or {}).get(status or "")
    return entry if isinstance(entry, Mapping) else _FALLBACK_STYLE


def _badges(node: Mapping[str, Any], style: Mapping[str, Any]) -> str:
    """Status + binding + tier badges. Each is emitted only when its field is present."""
    out: List[str] = []
    status = node.get("status")
    if status:
        st = _style_for(style, status)
        out.append(
            f'<span class="badge status" data-role="status" style="color:{esc(st.get("color"))};'
            f'border-color:{esc(st.get("color"))}">{esc(st.get("label"))}</span>'
        )
    binding = node.get("binding") or {}
    if binding.get("agent"):
        out.append(
            f'<span class="badge agent">{esc(binding["agent"])} '
            f'v{esc(binding.get("version"))}</span>'
        )
    if binding.get("action"):
        out.append(f'<span class="badge">{esc(binding["action"])}</span>')
    if binding.get("grade"):
        out.append(
            f'<span class="badge grade">{esc(binding["grade"])} / '
            f'bar {esc(binding.get("bar"))}</span>'
        )
    if node.get("context_tier"):
        out.append(f'<span class="badge tier">tier {esc(node["context_tier"])}</span>')
    return "".join(out) or '<span class="badge">unbound</span>'


def _field_names(block: Any) -> List[str]:
    """Field names out of a contract block, accepting **both** declared shapes.

    Mirrors ``harness._schema_properties`` (P3): a schema may nest its fields under
    ``properties`` or list them flat. Reading ``block.keys()`` naively renders the literal word
    ``properties`` as the field name, which is what the card used to show.
    """
    if not isinstance(block, Mapping):
        return []
    props = block.get("properties")
    if isinstance(props, Mapping):
        return list(props)
    return [k for k in block if k != "required"]


def _kv_rows(node: Mapping[str, Any]) -> str:
    """The card's key/value rows.

    Every row is emitted under a truthiness guard, so a node with no record (``held`` /
    ``not_reached`` write none at all) simply shows fewer rows rather than rows with blank values
    (constraint 4).
    """
    rows: List[str] = []
    binding = node.get("binding") or {}
    if node.get("blocked_on"):
        rows.append(f'<div class="kv"><b>reason:</b> {esc(node["blocked_on"])}</div>')
    if node.get("failure_class"):
        rows.append(f'<div class="kv"><b>failure:</b> {esc(node["failure_class"])}</div>')
    if binding.get("candidates"):
        rows.append(
            f'<div class="kv"><b>candidates:</b> {esc(", ".join(binding["candidates"]))}</div>'
        )
    if binding.get("load") is not None:
        rows.append(f'<div class="kv"><b>load:</b> {esc(binding["load"])}</div>')
    if binding.get("reason"):
        rows.append(f'<div class="kv"><b>why:</b> {esc(binding["reason"])}</div>')
    hosting = node.get("hosting")
    if hosting:
        rows.append(
            f'<div class="kv"><b>hosting:</b> {esc(hosting.get("protocol"))} '
            f':{esc(hosting.get("port"))} \u00b7 {esc(hosting.get("build_mode"))}</div>'
        )
    io_bits: List[str] = []
    inputs = _field_names(node.get("inputs"))
    outputs = _field_names(node.get("outputs"))
    if inputs:
        io_bits.append("in: " + ", ".join(inputs))
    if outputs:
        io_bits.append("out: " + ", ".join(outputs))
    if io_bits:
        rows.append(f'<div class="kv"><b>I/O:</b> {esc(" | ".join(io_bits))}</div>')
    if node.get("depends_on"):
        rows.append(f'<div class="kv"><b>consumes:</b> {esc(", ".join(node["depends_on"]))}</div>')
    return "".join(rows)


def _full_card(node: Mapping[str, Any], style: Mapping[str, Any]) -> str:
    """The full node card — the right-hand panel's body (U1c).

    Ported from ``dag_render.render_html``'s existing card: capability + id header, binding badges,
    kv rows, a collapsible task prompt, and a status-driven left border and background.
    """
    status = node.get("status")
    st = _style_for(style, status)
    tint = ""
    if status:
        tint = (
            f' style="border-left-color:{esc(st.get("color"))};'
            f'background:{esc(st.get("fill"))}"'
        )
    prompt = node.get("task_prompt")
    prompt_html = (
        f"<details><summary>task prompt</summary>"
        f'<pre class="prompt">{esc(prompt)}</pre></details>'
        if prompt
        else ""
    )
    capability = node.get("capability")
    # the id is redundant when the capability IS the id (common in fixtures)
    id_html = f' <span class="id">{esc(node["id"])}</span>' if capability != node["id"] else ""
    return (
        f'<article class="card{" status" if status else ""}" data-node="{esc(node["id"])}"{tint}>'
        f'<h3>{esc(capability)}{id_html}</h3>'
        f'<div class="badges">{_badges(node, style)}</div>'
        f"{_kv_rows(node)}{prompt_html}</article>"
    )


def _board_cell(node: Mapping[str, Any], style: Mapping[str, Any]) -> str:
    """A compact card for a board column.

    Deliberately *not* the full card: a column cell cannot carry a task prompt legibly, so the cell
    shows identity plus status and the right-hand panel carries the detail. Same data, two
    renderings.
    """
    status = node.get("status")
    st = _style_for(style, status)
    capability = node.get("capability")
    id_html = f'<span class="id">{esc(node["id"])}</span>' if capability != node["id"] else ""
    return (
        f'<article class="cell" data-cell="{esc(node["id"])}" data-status="{esc(status)}" '
        f'tabindex="0" style="border-left-color:{esc(st.get("color"))};'
        f'background:{esc(st.get("fill"))}">'
        f'<b>{esc(capability)}</b>{id_html}</article>'
    )


def _rendered_columns(nodes: Sequence[Mapping[str, Any]], style: Mapping[str, Any],
                      *, live: bool) -> List[str]:
    """The columns actually rendered: the flow-order set, plus any status present in the data.

    Shared by the board and the embedded payload so the JS's count/move lookups can never reference
    a column the board did not draw.
    """
    columns = board_columns(style, live=live)
    if not live:
        return columns
    for node in nodes:
        status = node.get("status") or "pending"
        if status not in columns:
            columns.append(status)
    return columns


def _board(nodes: Sequence[Mapping[str, Any]], style: Mapping[str, Any],
           counts: Mapping[str, Any], columns: Sequence[str]) -> str:
    """The live Kanban board: one column per reachable status, in flow order (U1d).

    Empty columns are still rendered. At the first tick every card sits in ``pending`` and the rest
    are legitimately empty, so emptiness is the normal opening state rather than something to hide
    — and an empty ``crash`` column is itself information.

    **No card is ever dropped.** The live column set omits ``not_reached`` because
    ``statuses_from_store(live=True)`` never assigns it — but ``run_to_record`` builds its statuses
    with ``live=False``, so a record rendered live *can* still carry one. :func:`_rendered_columns`
    therefore appends any status present in the data, even one the live set excludes: a card with
    nowhere to go would otherwise vanish from the board without a trace.
    """
    by_status: Dict[str, List[str]] = {}
    for node in nodes:
        status = node.get("status") or "pending"
        by_status.setdefault(status, []).append(_board_cell(node, style))
    out: List[str] = []
    for col in columns:
        st = _style_for(style, col)
        cells = "".join(by_status.get(col, []))
        out.append(
            f'<section class="col" data-col="{esc(col)}">'
            f'<header style="color:{esc(st.get("color"))};'
            f'border-bottom-color:{esc(st.get("color"))}">'
            f'<span class="mark">{esc(MARKS.get(col, _FALLBACK_MARK))}</span> '
            f'{esc(st.get("label"))} '
            f'<span class="n" data-count="{esc(col)}">{esc(counts.get(col, 0))}</span>'
            f'</header><div class="stack" data-stack="{esc(col)}">{cells}</div></section>'
        )
    return f'<div class="board" id="board">{"".join(out)}</div>'


def _legend(style: Mapping[str, Any], counts: Mapping[str, Any], *, live: bool) -> str:
    chips: List[str] = []
    for status in board_columns(style, live=live):
        st = _style_for(style, status)
        chips.append(
            f'<span class="badge status" data-legend="{esc(status)}" '
            f'style="color:{esc(st.get("color"))};border-color:{esc(st.get("color"))}">'
            f'{esc(MARKS.get(status, _FALLBACK_MARK))} {esc(st.get("label"))} \u00b7 '
            f'<b data-legend-count="{esc(status)}">{esc(counts.get(status, 0))}</b></span>'
        )
    return f'<div class="legend" id="legend">{"".join(chips)}</div>'


def _viz_payload(record: Mapping[str, Any], *, live: bool, columns: Sequence[str]) -> str:
    """The JSON the page embeds — the plan-side facts the poll feed deliberately omits.

    ``live_snapshot`` ships only four per-node keys (``status`` / ``failure_class`` /
    ``blocked_on`` / ``seq``) because re-shipping prompts and manifests several times a second
    would dominate its size. Embedding the record here is what keeps a *live* card fully populated:
    static facts come from this payload at load, and the poll overlays only what changes.
    """
    payload = {
        "kind": record.get("kind"),
        "live": live,
        "order": list(record.get("order", [])),
        "style": dict(STATUS_STYLE),
        "marks": MARKS,
        "columns": list(columns),
        "summary": dict(record.get("summary") or {}),
        "transitions": list(record.get("transitions") or []),
        "nodes": [
            {
                "id": node["id"],
                "capability": node.get("capability"),
                "depends_on": list(node.get("depends_on") or []),
                "status": node.get("status"),
            }
            for node in record.get("nodes", [])
        ],
        "edges": [list(edge) for edge in record.get("edges", [])],
    }
    return json.dumps(payload, default=str).replace("</", "<\\/")


_STYLE_CSS = """
*{box-sizing:border-box}
body{margin:0;font:13px/1.5 -apple-system,system-ui,sans-serif;color:#e5e7eb;background:#0b1220}
header.top{padding:10px 16px;border-bottom:1px solid #1e293b;display:flex;gap:14px;
  align-items:baseline;flex-wrap:wrap}
header.top h1{font-size:15px;margin:0}
.meta{color:#94a3b8;font-size:12px}
.legend{display:flex;gap:6px;flex-wrap:wrap;margin-left:auto}
.badge{display:inline-block;padding:1px 7px;border:1px solid #334155;border-radius:999px;
  font-size:11px;color:#cbd5e1;white-space:nowrap}
.wrap{display:flex;align-items:stretch;min-height:300px}
.canvas{flex:1;overflow:auto;padding:14px}
.panel{width:360px;border-left:1px solid #1e293b;background:#0d1526;padding:12px;overflow:auto}
.panel .hint{color:#475569}
.card{border:1px solid #1e293b;border-left:3px solid #334155;border-radius:8px;padding:10px 12px;
  background:#0f172a}
.card h3{margin:0 0 6px;font-size:13px}
.card .id{color:#64748b;font-family:monospace;font-weight:400}
.card .badges{display:flex;gap:5px;flex-wrap:wrap;margin-bottom:6px}
.kv{color:#94a3b8;font-size:12px;margin:2px 0}
.kv b{color:#cbd5e1;font-weight:600}
pre.prompt{white-space:pre-wrap;background:#0b1220;border:1px solid #1e293b;border-radius:6px;
  padding:8px;font-size:11px;max-height:40vh;overflow:auto}
.board{display:flex;gap:8px;overflow-x:auto;padding:12px;border-top:1px solid #1e293b}
.col{min-width:150px;flex:1}
.col header{font-size:11px;font-weight:700;letter-spacing:.3px;padding-bottom:4px;
  border-bottom:2px solid #334155;margin-bottom:6px;display:flex;gap:5px;align-items:center}
.col header .n{margin-left:auto;color:#94a3b8;font-weight:600}
.stack{display:flex;flex-direction:column;gap:5px;min-height:28px}
.cell{border:1px solid #1e293b;border-left:3px solid #334155;border-radius:6px;padding:5px 7px;
  cursor:pointer;font-size:12px;display:flex;flex-direction:column}
.cell .id{color:#64748b;font-family:monospace;font-size:10px}
.cell.sel,.card.sel{outline:2px solid #7dd3fc}
.n.sel rect{stroke-width:2.5}
.livestate{font-size:11px;color:#94a3b8}
#cardpool{display:none}
"""

# --- the DAG renderer + live loop -------------------------------------------------------------
# Adapted from a longest-path layout dag(): longest-path layering with multi-parent accumulation, cubic
# edge paths, one arrowhead marker in <defs>. Two substantive changes (Q5):
#   * addressing is `id`, not the folgezettel `fz`
#   * the mark AND the stroke come from the record's nine-way style table, replacing the
#     two-state `outcome==='failed' || status==='error'` test that rendered every Concursus
#     status as a green success tick
# Every SVG attribute is quoted: unquoted attrs on <text> break the SVG namespace parser, which
# renders boxes with no words.
_APP_JS = r"""
var D = JSON.parse(document.getElementById('viz-data').textContent);
var STYLE = D.style || {}, MARKS = D.marks || {};
var NODES = D.nodes || [], EDGES = D.edges || [];
var byId = {}; NODES.forEach(function (n) { byId[n.id] = n; });
var idx = {}; NODES.forEach(function (n, i) { idx[n.id] = i; });
var SEL = null;

function esc(s) {
  return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
function clip(s, n) { s = String(s || ''); return s.length > n ? s.slice(0, n - 1) + '\u2026' : s; }
function sty(st) { return STYLE[st] || { label: st || 'unknown', color: '#6b7280', fill: '#14161b' }; }
function mark(st) { return MARKS[st] || '?'; }

/* ---- DAG: longest-path layering, multi-parent aware ---- */
function dag() {
  var par = {}; NODES.forEach(function (n) { par[n.id] = []; });
  EDGES.forEach(function (e) { if (byId[e[0]] && byId[e[1]]) par[e[1]].push(e[0]); });
  var dep = {};
  function Dp(id) {
    if (dep[id] != null) return dep[id];
    var ps = par[id] || [];
    if (!ps.length) { dep[id] = 0; return 0; }
    var m = 0;
    dep[id] = 0;  /* cycle guard: a revisit reads 0 rather than recursing forever */
    ps.forEach(function (p) { m = Math.max(m, Dp(p) + 1); });
    dep[id] = m; return m;
  }
  NODES.forEach(function (n) { Dp(n.id); });
  var byD = {};
  NODES.forEach(function (n) { (byD[dep[n.id]] = byD[dep[n.id]] || []).push(n); });
  var layers = Object.keys(byD).map(Number).sort(function (a, b) { return a - b; });
  var NW = 210, NH = 52, rowGap = 88, colGap = 232, padX = 28, padY = 24;
  var maxCols = 1;
  layers.forEach(function (d) { maxCols = Math.max(maxCols, byD[d].length); });
  var W = Math.max(padX * 2 + maxCols * colGap, 360), H = padY * 2 + layers.length * rowGap;
  var pos = {};
  layers.forEach(function (d) {
    var row = byD[d], rowW = row.length * colGap, x0 = (W - rowW) / 2 + (colGap - NW) / 2;
    row.forEach(function (n, i) { pos[n.id] = { x: x0 + i * colGap, y: padY + d * rowGap }; });
  });
  var s = '<svg width="' + W + '" height="' + H + '" style="display:block;margin:auto"><defs>'
    + '<marker id="ar" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">'
    + '<path d="M0,0L6,3L0,6" fill="#64748b"/></marker></defs>';
  EDGES.forEach(function (e) {
    var a = pos[e[0]], b = pos[e[1]];
    if (!a || !b) return;
    var x1 = a.x + NW / 2, y1 = a.y + NH, x2 = b.x + NW / 2, y2 = b.y, my = (y1 + y2) / 2;
    s += '<path class="e" data-to="' + esc(e[1]) + '" d="M' + x1 + ' ' + y1 + ' C' + x1 + ' '
      + my + ' ' + x2 + ' ' + my + ' ' + x2 + ' ' + y2
      + '" fill="none" stroke="#475569" stroke-width="1.5" marker-end="url(#ar)"/>';
  });
  NODES.forEach(function (n) {
    var p = pos[n.id]; if (!p) return;
    var st = n.status || (D.live ? 'pending' : 'not_reached'), s2 = sty(st);
    var lbl = clip(n.capability || n.id, 24);
    /* the id line is redundant when the capability IS the id (common in fixtures) */
    var idLine = (n.capability && n.capability !== n.id)
      ? '<text class="id" x="' + (p.x + 14) + '" y="' + (p.y + 38) + '" font-size="10"'
        + ' fill="#64748b" font-family="monospace">' + esc(n.id) + '</text>'
      : '';
    s += '<g class="n" id="gn-' + idx[n.id] + '" data-node-g="' + esc(n.id) + '"'
      + ' style="cursor:pointer" tabindex="0">'
      + '<rect x="' + p.x + '" y="' + p.y + '" width="' + NW + '" height="' + NH + '" rx="9"'
      + ' fill="' + esc(s2.fill) + '" stroke="' + esc(s2.color) + '" stroke-width="1.5"/>'
      + '<rect x="' + p.x + '" y="' + p.y + '" width="4" height="' + NH + '" rx="2"'
      + ' fill="' + esc(s2.color) + '"/>'
      + '<text class="mk" x="' + (p.x + NW - 12) + '" y="' + (p.y + 21) + '" font-size="13"'
      + ' fill="' + esc(s2.color) + '" text-anchor="end">' + mark(st) + '</text>'
      + '<text x="' + (p.x + 14) + '" y="' + (p.y + 21) + '" font-size="12" fill="#e5e7eb"'
      + ' font-weight="600">' + esc(lbl) + '</text>'
      + idLine
      + '<text class="st" x="' + (p.x + NW - 12) + '" y="' + (p.y + 38) + '" font-size="10"'
      + ' fill="' + esc(s2.color) + '" text-anchor="end">' + esc(s2.label) + '</text>'
      + '</g>';
  });
  return s + '</svg>';
}

/* ---- click a node -> raise its FULL card in the right-hand panel (U1c) ---- */
function select(id) {
  SEL = id;
  var pool = document.getElementById('cardpool');
  var card = pool ? pool.querySelector('[data-node="' + (window.CSS && CSS.escape ? CSS.escape(id) : id) + '"]') : null;
  var panel = document.getElementById('panel');
  if (!panel) return;
  panel.innerHTML = card ? card.outerHTML
    : '<div class="hint">no card for ' + esc(id) + '</div>';
  Array.prototype.forEach.call(document.querySelectorAll('g.n'), function (g) {
    g.classList.toggle('sel', g.getAttribute('data-node-g') === id);
  });
  Array.prototype.forEach.call(document.querySelectorAll('[data-cell]'), function (c) {
    c.classList.toggle('sel', c.getAttribute('data-cell') === id);
  });
}

function wire() {
  Array.prototype.forEach.call(document.querySelectorAll('g.n'), function (g) {
    var id = g.getAttribute('data-node-g');
    g.addEventListener('click', function () { select(id); });
    g.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); select(id); }
    });
  });
  Array.prototype.forEach.call(document.querySelectorAll('[data-cell]'), function (c) {
    var id = c.getAttribute('data-cell');
    c.addEventListener('click', function () { select(id); });
  });
}

/* ---- apply a status map to graph + cells + cards + counts ---- */
/* Cards are (re)placed by their CURRENT status rather than by replaying `from -> to` moves, so a
   transition log whose first entry per node is {from: null} cannot make the whole fleet lurch at
   t=0: that entry is a placement, not a move (constraint 3). Replay (U1b) consumes
   D.transitions and must make the same distinction explicitly. */
function apply(statuses, summary) {
  NODES.forEach(function (n) {
    var st = (statuses[n.id] || {}).status || n.status || (D.live ? 'pending' : 'not_reached');
    n.status = st;
    var s2 = sty(st);
    var g = document.getElementById('gn-' + idx[n.id]);
    if (g) {
      var r = g.querySelector('rect');
      if (r) { r.setAttribute('stroke', s2.color); r.setAttribute('fill', s2.fill); }
      var mk = g.querySelector('.mk');
      if (mk) { mk.textContent = mark(st); mk.setAttribute('fill', s2.color); }
      var stx = g.querySelector('.st');
      if (stx) { stx.textContent = s2.label; stx.setAttribute('fill', s2.color); }
    }
    var cell = document.querySelector('[data-cell="' + (window.CSS && CSS.escape ? CSS.escape(n.id) : n.id) + '"]');
    if (cell) {
      cell.setAttribute('data-status', st);
      cell.style.borderLeftColor = s2.color;
      cell.style.background = s2.fill;
      var stack = document.querySelector('[data-stack="' + (window.CSS && CSS.escape ? CSS.escape(st) : st) + '"]');
      if (stack && cell.parentNode !== stack) stack.appendChild(cell);   /* the card MOVES column */
    }
    var card = document.querySelector('#cardpool [data-node="' + (window.CSS && CSS.escape ? CSS.escape(n.id) : n.id) + '"]');
    if (card) {
      card.style.borderLeftColor = s2.color;
      card.style.background = s2.fill;
      var b = card.querySelector('[data-role="status"]');
      if (b) { b.textContent = s2.label; b.style.color = s2.color; b.style.borderColor = s2.color; }
    }
  });
  var counts = (summary || {}).status_counts || {};
  (D.columns || []).forEach(function (col) {
    var n = counts[col] == null ? 0 : counts[col];
    var a = document.querySelector('[data-count="' + (window.CSS && CSS.escape ? CSS.escape(col) : col) + '"]');
    if (a) a.textContent = n;
    var b = document.querySelector('[data-legend-count="' + (window.CSS && CSS.escape ? CSS.escape(col) : col) + '"]');
    if (b) b.textContent = n;
  });
  if (SEL) select(SEL);
}

/* ---- live poll: <page>.live.json, published by dag_render.LivePublisher ---- */
function poll() {
  var url = location.pathname.replace(/\.html?$/, '') + '.live.json';
  var badge = document.getElementById('livestate');
  var stop = false;
  function tick() {
    fetch(url + '?_=' + Date.now(), { cache: 'no-store' }).then(function (r) {
      if (!r.ok) throw new Error('no feed');
      return r.json();
    }).then(function (snap) {
      apply(snap.statuses || {}, snap.summary);
      if (snap.transitions) D.transitions = snap.transitions;
      if (badge) {
        badge.textContent = (snap.done ? 'finished' : 'live')
          + ' \u00b7 ' + (snap.elapsed == null ? '' : snap.elapsed + 's')
          + ' \u00b7 tick ' + (snap.seq == null ? '' : snap.seq);
      }
      if (snap.done) stop = true;
    }).catch(function () {
      if (badge && !badge.textContent) badge.textContent = 'no live feed';
    }).then(function () {
      if (!stop) setTimeout(tick, 500);
    });
  }
  tick();
}

document.getElementById('canvas').innerHTML = dag();
wire();
apply({}, D.summary);
if (D.live) poll();
"""


def render_console(
    record: Mapping[str, Any],
    *,
    title: Optional[str] = None,
    live: bool = False,
) -> str:
    """Render the operator console for ``record`` as one self-contained HTML page.

    ``record`` is the dict produced by ``dag_render.plan_to_record`` / ``run_to_record``. Nothing
    is fetched from a network: the CSS and JS are inline and the only request the page ever makes
    is to its own sibling ``<page>.live.json`` when ``live`` is set.

    Set ``live=True`` for a run that is still in flight: that adds the Kanban board and the poll
    loop. A finished run renders the DAG plus the click-to-card panel and no board, because a board
    whose cards can never move is a static grouping rather than a Kanban.
    """
    style = dict(STATUS_STYLE)
    nodes = list(record.get("nodes") or [])
    summary = dict(record.get("summary") or {})
    counts = dict(summary.get("status_counts") or {})
    is_run = bool(record.get("kind") == "run" or summary)
    ttl = title or (f"{record.get('goal') or 'plan'} \u00b7 {'run' if is_run else 'plan'}")

    pool = "".join(_full_card(node, style) for node in nodes)
    columns = _rendered_columns(nodes, style, live=live)
    board = _board(nodes, style, counts, columns) if live else ""
    legend = _legend(style, counts, live=live) if is_run else ""
    livestate = '<span class="livestate" id="livestate"></span>' if live else ""

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(ttl)}</title><style>{_STYLE_CSS}</style></head>
<body>
<header class="top">
  <h1>{esc(ttl)}</h1>
  <div class="meta">nodes: {len(nodes)} &nbsp;\u00b7&nbsp;
    model: <code>{esc(record.get('model_id') or 'deterministic/offline')}</code>{
    ' &nbsp;·&nbsp; ' + livestate if livestate else ''}</div>
  {legend}
</header>
<div class="wrap">
  <div class="canvas" id="canvas"></div>
  <aside class="panel" id="panel"><div class="hint">click a node for its card</div></aside>
</div>
{board}
<div id="cardpool">{pool}</div>
<script type="application/json" id="viz-data">{_viz_payload(record, live=live, columns=columns)}</script>
<script>{_APP_JS}</script>
</body></html>
"""
