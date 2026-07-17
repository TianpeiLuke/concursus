"""The **FileVaultStateStore** — a persistent, on-disk :class:`~concursus.statestore.StateStore`.

Concursus's default :class:`~concursus.statestore.InProcessStateStore` is pure in-memory (run
state vanishes on process exit) and its :class:`~concursus.statestore.MemoryStateStore` persists
opaque AgentCore Blob events — neither writes a durable, human-readable *note* to disk. This
backend closes that gap: it writes **one round-trip-exact markdown note per record event** under
``<vault>/runs/<session>/`` and reloads them to resume, so a run survives process exit and its log
is greppable/inspectable offline — without AWS. It is the offline / air-gapped / CI / debuggable
durability tier, opt-in behind the same 4-method :class:`StateStore` Protocol.

Design (FZ 35e1b1): it *reuses* the statestore marshalling seam rather than reinventing it —
:func:`~concursus.statestore._build_metadata` / :func:`~concursus.statestore._event_to_record`
(shared with :class:`MemoryStateStore`, so the file and AgentCore backends differ only in
transport), :func:`~concursus.statestore.content_hash`, and
:func:`~concursus.statestore._index_records`. The **authoritative payload is an embedded,
base64-wrapped JSON blob**, never the rendered YAML/body — so an arbitrary ``output`` dict
(newlines, quotes, ``---``, ``[](.md)`` link syntax, numeric-looking strings) round-trips
exactly; the frontmatter and body are lossy display/index copies never re-ingested. Writes are
atomic (temp + ``os.replace``); a reentrant lock + generation-token OCC over ``.lock`` / ``.gen``
sidecars keeps concurrent writers over one vault from clobbering. Pure-Python, stdlib only.

The on-disk notes stay the single source of truth; :mod:`concursus.rundb` builds a *derived,
rebuildable* SQLite graph/index over them (never a second source).
"""

from __future__ import annotations

import base64
import json
import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set

from .statestore import (
    Record,
    _ADDR_SEP,
    _apply_meta,
    _build_metadata,
    _DEDUP_RECORD_TYPE,
    _event_to_record,
    _index_records,
    content_hash,
)

# The authoritative embedded-JSON marker: everything after it on the line is base64 of the JSON
# truth (mirrors HiveFleet's ``b64:`` embedded-note discipline — display frontmatter is lossy).
_BLOB_PREFIX = "b64:"

# Sidecars living beside the run dir for the cross-process write guard.
_LOCK_NAME = ".concursus.lock"
_GEN_NAME = ".concursus.gen"

# The vault posture: by DEFAULT the store emits notes conformant to the Abuse SlipBox format
# (``slipbox_form=True`` — validate under ``check_note_format.py`` / ``validate_fz_trails.py``) so a
# run's on-disk notes read as a genuine, indexer-ingestible slipbox trail (FZ/lineage/building_block/
# Related-Notes + a ``_run.md`` entry point). Pass ``slipbox_form=False`` to emit the lean machine
# schema (``node`` / ``attempt`` / ``status`` / ``consumes`` / ``payload``) — a leaner, round-trip-exact
# durable log — when the run's notes are not meant to be human-browsable/indexer-ingestible.
_SLIPBOX_TOPICS = ["Multi-Agent Orchestration", "Concursus Run State"]

# A record's status maps onto the SlipBox status vocabulary (validated→active, failed→draft).
_SLIPBOX_STATUS = {"validated": "active", "failed": "draft", "superseded": "superseded"}

# The durable run-plan snapshot note (AI-18): a 'model'+navigation note written ALONGSIDE ``_run.md``
# under the run dir. It is NOT a run record — it carries no ``payload``/``meta`` blob and is stamped
# with ``concursus_note_kind: run_plan`` so :func:`_note_to_record` REFUSES to parse it (raises), and
# the ``*.md`` record globs in :meth:`FileVaultStateStore._load` / :mod:`concursus.rundb` therefore
# skip it (their loaders catch the parse error) without corrupting ``load_records``.
_PLAN_NOTE_NAME = "_plan.md"
_PLAN_NOTE_MARKER = "concursus_note_kind"
_PLAN_NOTE_KIND = "run_plan"

# The durable per-agent PAYLOAD-contract note (FZ 35e4a3a1b Phase 1 T3): the frozen invoke payload
# a node was dispatched with (the b2 contract — wired inputs + tiered static context + tool_calls +
# trust_tier). Like the plan snapshot it is NOT a run record — stamped ``concursus_note_kind:
# payload`` so :func:`_note_to_record` REFUSES it (same guard) and the record loaders skip it.
_PAYLOAD_NOTE_KIND = "payload"

# Machine-finding keys a renderer surfaces IF THEY HAPPEN to be present in an agent's output dict.
# Purely reflective: the renderer copies whatever the agent emitted — it NEVER derives, judges, or
# generates a verdict/hypothesis of its own (that reasoning tier is Phase 5, deliberately excluded).
_FINDING_KEYS = ("root_cause", "failure_mode", "family", "confidence")


def _building_block_for(record: Record) -> str:
    """Derive the SlipBox ``building_block`` for a record from its kind (mirrors HiveFleet's
    ``building_block_of``): a failed record is a ``counter_argument`` (a refuted attempt), a
    content-hash dedup no-op is ``navigation`` (a structural marker, not new evidence), and any
    other validated agent output is an ``empirical_observation`` (a produced result). This is
    *derived*, never hardcoded, so the note's building_block reflects what the record actually is.
    """
    if record.status == "failed":
        return "counter_argument"
    if record.record_type == _DEDUP_RECORD_TYPE:
        return "navigation"
    return "empirical_observation"


# --------------------------------------------------------------------------- Luhmann FZ helpers
# A run's records form a per-run Folgezettel trail: the run root is FZ ``"1"`` and each record is a
# write-order child (``1a``, ``1b`` … bijective base-26 past 26), so notes carry a valid
# ``folgezettel:`` / ``lineage:`` the SlipBox tooling accepts. Concursus addresses are ``/``-paths
# (not dotted ordinals like HiveFleet), so FZ position is assigned by write order, not re-based.
def _int_to_letters(n: int) -> str:
    """Bijective base-26: ``1→a … 26→z, 27→aa`` (a total, reversible ordinal→letter map)."""
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(ord("a") + rem) + out
    return out


def _fz_for(position: int) -> str:
    """The FZ string for the ``position``-th (1-based) record in a run: ``1a``, ``1b`` … under root ``1``."""
    return "1" + _int_to_letters(position)


def _trail_id(session_id: str) -> str:
    """A SlipBox ``lineage:`` path id / trail slug for a run — matches the grammar ``^[a-z][a-z0-9_]*``.

    Lowercases, folds every non-``[a-z0-9_]`` char to ``_``, and prefixes ``run_`` when the result
    does not start with a letter, so a run's notes share one valid, injective-enough trail id.
    """
    slug = "".join(ch if (ch.isascii() and (ch.isalnum() or ch == "_")) else "_" for ch in session_id.lower())
    if not slug or not slug[0].isalpha():
        slug = "run_" + slug
    return slug


def _slug(raw: str, *, maxlen: int = 80) -> str:
    """A collision-resistant, filesystem-safe slug of ``raw``.

    Keeps ``[A-Za-z0-9._-]`` verbatim, maps every other run of characters to ``-``, and appends a
    short content hash so distinct inputs that fold to the same safe stem never collide (the
    injective-slug *purpose* of HiveFleet's ``slug_component`` — without the Luhmann FZ form,
    since concursus addresses are already ``/``-materialized paths).
    """
    safe_chars = []
    for ch in raw:
        safe_chars.append(ch if (ch.isalnum() or ch in "._-") else "-")
    safe = "".join(safe_chars).strip("-") or "x"
    digest = content_hash({"_": raw})[:8]
    stem = safe[:maxlen]
    return f"{stem}__{digest}"


def _encode_blob(output: dict) -> str:
    """Base64 the canonical JSON of ``{node: output}``-style payloads for lossless embedding."""
    raw = json.dumps(output, sort_keys=True).encode("utf-8")
    return _BLOB_PREFIX + base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_blob(token: str) -> dict:
    """Inverse of :func:`_encode_blob` (tolerates a missing prefix)."""
    if token.startswith(_BLOB_PREFIX):
        token = token[len(_BLOB_PREFIX) :]
    raw = base64.urlsafe_b64decode(token.encode("ascii"))
    return json.loads(raw.decode("utf-8"))


def _compact_value(value: object, *, maxlen: int = 120) -> str:
    """A one-line, truncated display of a scalar/collection for the human summary (never parsed)."""
    if isinstance(value, str):
        text = value
    else:
        text = json.dumps(value, sort_keys=True)
    text = " ".join(text.split())  # collapse newlines/runs of whitespace to keep it one line
    return text if len(text) <= maxlen else text[: maxlen - 1] + "…"


def _machine_findings(output: dict) -> Dict[str, object]:
    """The subset of :data:`_FINDING_KEYS` that HAPPEN to be present in ``output``.

    Purely reflective — it copies whatever the agent emitted and derives/judges nothing (a run's
    verdict/hypothesis tier is Phase 5, deliberately excluded here). Returns ``{}`` when the output
    carries none of the finding keys (the common case), so a normal note gains no findings block.
    """
    if not isinstance(output, dict):
        return {}
    return {k: output[k] for k in _FINDING_KEYS if k in output}


def _did_observed_outcome(record: Record) -> List[str]:
    """A compact **Did → Observed → Outcome** summary of a record for the note body (AI-18).

    A record-only projection: *Did* = which node ran (record kind + attempt), *Observed* = a
    truncated one-line digest of the produced output's top-level keys, *Outcome* = the record's
    status. It reads only the record's own fields — it generates no verdict/hypothesis and makes
    no runtime decision. The authoritative value stays the ``payload`` blob; this is display only.
    """
    lines = ["### Did → Observed → Outcome", ""]
    lines.append(
        f"- **Did**: ran `{record.node}` (attempt {record.attempt}, record type "
        f"`{record.record_type}`)."
    )
    output = record.output if isinstance(record.output, dict) else {}
    if output:
        fields = ", ".join(
            f"`{k}`={_compact_value(output[k], maxlen=60)}" for k in sorted(output)
        )
        observed = _compact_value(fields, maxlen=280)
    else:
        observed = _compact_value(record.output)
    lines.append(f"- **Observed**: {observed}")
    lines.append(f"- **Outcome**: status `{record.status}`.")
    findings = _machine_findings(output)
    if findings:
        lines.append("")
        lines.append("### Machine Findings")
        lines.append("")
        lines.append(
            "> Copied verbatim from the agent's output (reflective only — no verdict is generated)."
        )
        lines.append("")
        for key in _FINDING_KEYS:
            if key in findings:
                lines.append(f"- **{key}**: {_compact_value(findings[key])}")
    lines.append("")
    return lines


def _record_to_note(
    record: Record,
    *,
    slipbox_form: bool = True,
    position: int = 1,
    trail_id: str = "run",
    date: str = "",
    related: Optional[List[str]] = None,
) -> str:
    """Render a :class:`Record` as a round-trip-exact markdown note.

    Two forms share one authoritative ``payload`` line (``b64:<base64(json({node: output}))>``,
    byte-identical to :class:`MemoryStateStore`'s Blob) — :func:`_note_to_record` reads only that
    blob plus the machine frontmatter keys, never the display fields, so the round-trip is exact
    either way:

    * ``slipbox_form=False`` (opt-out) emits the lean machine schema (``node`` / ``attempt`` /
      ``status`` / ``consumes`` / ``payload``) — a smaller, non-indexed durable log;
    * ``slipbox_form=True`` (default) emits a note conformant to the Abuse SlipBox format —
      P.A.R.A. ``tags`` / ``keywords`` / ``topics`` / ``building_block`` / valid ``status`` /
      ``folgezettel`` / ``lineage`` / ``access_control_group``, a typed H1, and a
      ``## Related Notes`` section — so it validates under ``check_note_format.py`` and reads as a
      genuine, indexer-ingestible slipbox trail.
    """
    machine = _build_metadata(record)  # the authoritative, all-string run-state keys
    # Two authoritative, lossless lines the reader reconstructs from — the HiveFleet discipline:
    # ``payload`` = the output blob, ``meta`` = the record's metadata. Every display field below
    # (SlipBox frontmatter, H1, body) is a lossy copy that :func:`_note_to_record` never reads.
    blob_line = f"payload: {_encode_blob({record.node: record.output})}"
    meta_line = f"meta: {_encode_blob(machine)}"

    if not slipbox_form:
        lines = ["---"]
        for key in sorted(machine):
            lines.append(f"{key}: {json.dumps(machine[key])}")
        lines += [meta_line, blob_line, "---", "",
                  f"# {record.node} (attempt {record.attempt}, {record.status})",
                  "", "> Derived display copy — the `payload` frontmatter blob is the source of truth.",
                  "", "```json", json.dumps(record.output, indent=2, sort_keys=True), "```", ""]
        return "\n".join(lines)

    fz = _fz_for(position)
    status = _SLIPBOX_STATUS.get(record.status, "active")
    tags = ["resource", "concursus", "run_state", record.record_type]
    keywords = [
        f"node {record.node}",
        f"attempt {record.attempt}",
        record.schema or "agent output",
        f"status {record.status}",
    ]
    # Frontmatter: SlipBox display/index fields first, then the authoritative machine keys +
    # payload blob (kept so the round-trip stays exact). check_note_format ignores unknown keys.
    fm: Dict[str, object] = {
        "tags": tags,
        "keywords": keywords,
        "topics": _SLIPBOX_TOPICS,
        "language": "json",
        "date of note": date,
        "status": status,
        "building_block": _building_block_for(record),
        "folgezettel": fz,
        "lineage": [f"{trail_id}:{fz}"],
        "node": record.node,
        "attempt": str(record.attempt),
        "record_status": record.status,
        "record_type": record.record_type,
        "content_hash": record.content_hash or "",
    }
    if record.schema is not None:
        fm["schema"] = record.schema
    if record.producer is not None:
        fm["producer"] = record.producer
    if record.consumes:
        fm["consumes"] = list(record.consumes)
    if record.address is not None:
        fm["address"] = record.address
    fm["access_control_group"] = ["general"]

    lines = ["---"]
    for key, value in fm.items():
        if isinstance(value, list):
            if not value:
                lines.append(f"{key}: []")
            else:
                lines.append(f"{key}:")
                lines.extend(f"  - {json.dumps(v)}" for v in value)
        else:
            lines.append(f"{key}: {json.dumps(value)}")
    lines.append(meta_line)
    lines.append(blob_line)
    lines.append("---")
    lines.append("")
    lines.append(f"# Run State: {record.node} (attempt {record.attempt}, {record.status})")
    lines.append("")
    lines.append(
        f"The `{record.node}` node's output on this run (record type `{record.record_type}`). "
        "The authoritative value is the `payload` frontmatter blob; the summary and JSON below are "
        "derived, human-readable display copies."
    )
    lines.append("")
    # A compact Did -> Observed -> Outcome digest (+ any machine findings the output HAPPENS to
    # carry) — a record-only projection that augments the raw JSON dump; it generates no verdict.
    lines.extend(_did_observed_outcome(record))
    lines.append("```json")
    lines.append(json.dumps(record.output, indent=2, sort_keys=True))
    lines.append("```")
    lines.append("")
    lines.append("## Related Notes")
    lines.append("")
    rel = list(related or [])
    if not rel:
        rel = ["[Run entry point](_run.md)"]  # never an orphan — always link the run entry
    for item in rel:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def _note_to_record(text: str) -> Record:
    """Parse a note written by :func:`_record_to_note` back into an exact :class:`Record`.

    Reads ONLY the two authoritative frontmatter blobs — ``meta:`` (the record's metadata) and
    ``payload:`` (the output) — and ignores every SlipBox display field (tags/keywords/H1/body),
    so the round-trip is exact regardless of the on-disk *form*. Reconstructs the AgentCore-shaped
    event dict and defers to :func:`~concursus.statestore._event_to_record` — the same marshalling
    :class:`MemoryStateStore` uses, so the file and Memory backends never drift. Falls back to the
    flat machine keys for a legacy note written before the ``meta`` blob existed.
    """
    if not text.startswith("---"):
        raise ValueError("note missing frontmatter")
    _, _, rest = text.partition("---\n")
    fm_block, _, _ = rest.partition("\n---")

    # A non-record note (the run-plan snapshot) is explicitly stamped so it is never parsed back as
    # a run Record — the loaders catch this and skip it, so it never corrupts ``load_records``.
    if f"{_PLAN_NOTE_MARKER}:" in fm_block:
        raise ValueError(f"not a run record ({_PLAN_NOTE_MARKER} note)")

    meta_token = ""
    payload_token = ""
    flat: Dict[str, str] = {}
    for line in fm_block.splitlines():
        stripped = line.strip()
        if not stripped or ":" not in stripped or stripped.startswith("- "):
            continue
        key, _, raw = stripped.partition(":")
        key, raw = key.strip(), raw.strip()
        if key == "meta":
            meta_token = raw
        elif key == "payload":
            payload_token = raw
        elif not raw:  # a YAML list header (``key:``) — skip its ``- item`` lines above
            continue
        else:
            try:
                flat[key] = json.loads(raw)
            except json.JSONDecodeError:
                flat[key] = raw

    meta = _decode_blob(meta_token) if meta_token else flat
    node = meta.get("node", "")
    blob = _decode_blob(payload_token) if payload_token else {node: {}}
    event = {
        "metadata": meta,
        "payload": [{"blob": json.dumps(blob)}],
        "eventId": meta.get("event_id"),
        "eventTimestamp": _coerce_int(meta.get("timestamp")),
    }
    return _event_to_record(event)


def _coerce_int(value: object) -> Optional[int]:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------- AI-18: ingestion renderers
# Write-time / read-only NOTE PROJECTIONS over already-frozen run state. None of them influences
# dispatch order or makes a runtime decision — they render what has already happened (or, for the
# plan snapshot, the already-FROZEN compiled plan). The log->note promotion trigger is FAILURE-only.


def _plan_mermaid(order: List[str], wiring: Dict[str, list]) -> List[str]:
    """Render a frozen plan's ``order`` + ``wiring`` as a Mermaid DAG (producer→consumer edges).

    Reads only the compiled topology (never AWS, never a live plan): each ``wiring`` edge becomes a
    ``producer --> consumer`` arrow labelled with its consumer input; isolated nodes are emitted as
    bare nodes so the whole ``order`` is visible. Pure display — it drives no dispatch.
    """
    lines = ["```mermaid", "graph TD"]
    node_ids = {name: f"n{i}" for i, name in enumerate(order)}
    extra = [n for n in wiring if n not in node_ids]
    for i, name in enumerate(extra):
        node_ids[name] = f"x{i}"
    for name in list(order) + extra:
        lines.append(f'    {node_ids[name]}["{name}"]')
    seen_edge = set()
    for consumer, refs in wiring.items():
        for ref in refs:
            producer = ref["producer"] if isinstance(ref, dict) else getattr(ref, "producer", "")
            input_name = (
                ref.get("input_name") if isinstance(ref, dict) else getattr(ref, "input_name", "")
            )
            if producer not in node_ids:
                node_ids[producer] = f"p{len(node_ids)}"
                lines.append(f'    {node_ids[producer]}["{producer}"]')
            key = (producer, consumer, input_name)
            if key in seen_edge:
                continue
            seen_edge.add(key)
            label = f"|{input_name}|" if input_name else ""
            lines.append(f"    {node_ids[producer]} -->{label} {node_ids[consumer]}")
    lines.append("```")
    return lines


def capture_run_plan_note(
    plan,
    run_dir,
    *,
    trail_id: str = "run",
    date: str = "",
    slipbox_form: bool = True,
) -> str:
    """**AI-18.** Persist a compiled :class:`~concursus.assemble.ProvisioningPlan` as a durable
    ``model``+navigation note (``<run_dir>/_plan.md``); return its path.

    Genuinely unbuilt before this: the plan is never written to disk, so a run's topology snapshot
    is lost on teardown. This captures it ALONGSIDE ``_run.md`` — a Mermaid DAG of ``plan.order`` +
    ``wiring`` in the body plus the plan's COMPACT ``to_summary_dict()`` projection (which DROPS the
    bulky per-node ``BuildPlanEntry`` deploy payload — wrapper source / dockerfile /
    ``create_agent_runtime`` — keeping only a hosting digest, so a note is never megabytes).

    It is NOT a run record: the note carries no ``payload``/``meta`` blob and is stamped
    ``concursus_note_kind: run_plan``, so :func:`_note_to_record` REFUSES to parse it and the record
    loaders (:meth:`FileVaultStateStore._load`, :func:`~concursus.rundb.load_records`) skip it — it
    never corrupts ``load_records`` or a resume/replay. Pure write-time projection of the frozen
    plan: it influences no dispatch and mutates nothing.
    """
    summary = plan.to_summary_dict()
    order = summary["order"]
    wiring = summary["wiring"]

    lines: List[str] = []
    if slipbox_form:
        fm = {
            "tags": ["resource", "concursus", "run_state", "run_plan"],
            "keywords": ["concursus plan", "provisioning plan", "run topology snapshot"],
            "topics": _SLIPBOX_TOPICS,
            "language": "markdown",
            "date of note": date,
            "status": "active",
            "building_block": "model",  # a compiled topology model (not a run record)
            "folgezettel": "1",
            "lineage": [f"{trail_id}:1"],
            _PLAN_NOTE_MARKER: _PLAN_NOTE_KIND,  # the non-record stamp: never parsed as a Record
            "access_control_group": ["general"],
        }
        lines.append("---")
        for key, value in fm.items():
            if isinstance(value, list):
                lines.append(f"{key}:")
                lines.extend(f"  - {json.dumps(v)}" for v in value)
            else:
                lines.append(f"{key}: {json.dumps(value)}")
        lines.append("---")
        lines.append("")
    else:
        # Lean form still carries the non-record stamp so it is never parsed back as a Record.
        lines += ["---", f"{_PLAN_NOTE_MARKER}: {json.dumps(_PLAN_NOTE_KIND)}", "---", ""]

    lines.append(f"# Run Plan: {trail_id}")
    lines.append("")
    lines.append(
        "A durable snapshot of this run's FROZEN provisioning plan — its dispatch `order` and "
        "resolved data `wiring`, rendered below as a DAG. The bulky per-node deploy payload "
        "(wrapper source / dockerfile / `create_agent_runtime` request) is intentionally DROPPED; "
        "only a compact hosting digest is kept. This is a read-only topology snapshot: it drives "
        "no dispatch."
    )
    lines.append("")
    lines.append("## Topology")
    lines.append("")
    lines.extend(_plan_mermaid(order, wiring))
    lines.append("")
    lines.append("## Dispatch Order")
    lines.append("")
    for i, name in enumerate(order, 1):
        entry = summary["entries"].get(name, {})
        proto = entry.get("protocol") or "?"
        mode = entry.get("build_mode") or "?"
        lines.append(f"{i}. `{name}` — {mode}/{proto}")
    lines.append("")
    lines.append("## Compact Plan Summary")
    lines.append("")
    lines.append(
        "> The full byte-exact plan is `ProvisioningPlan.to_dict()`; this drops the deploy payload."
    )
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(summary, indent=2, sort_keys=True))
    lines.append("```")
    lines.append("")

    path = Path(run_dir) / _PLAN_NOTE_NAME
    Path(run_dir).mkdir(parents=True, exist_ok=True)
    FileVaultStateStore._atomic_write(path, "\n".join(lines))
    return str(path)


def capture_agent_response_note(
    record: Record,
    *,
    slipbox_form: bool = True,
    position: int = 1,
    trail_id: str = "run",
    date: str = "",
    related: Optional[List[str]] = None,
) -> str:
    """**AI-18.** Render one agent response :class:`Record` as a durable note.

    A thin, named seam over :func:`_record_to_note` (the round-trip-exact renderer): the slipbox
    body carries the compact **Did → Observed → Outcome** summary plus any machine findings the
    output HAPPENS to carry, while the authoritative ``payload``/``meta`` b64 blobs stay untouched,
    so :func:`_note_to_record` reloads it byte-exact. Record-only — it generates no verdict or
    hypothesis.
    """
    return _record_to_note(
        record,
        slipbox_form=slipbox_form,
        position=position,
        trail_id=trail_id,
        date=date,
        related=related,
    )


def capture_agent_log_note(
    record: Record,
    *,
    slipbox_form: bool = True,
    position: int = 1,
    trail_id: str = "run",
    date: str = "",
    related: Optional[List[str]] = None,
) -> Optional[str]:
    """**AI-18 — selective-ingestion POLICY.** Promote a raw agent log to a durable note ONLY on
    failure; otherwise return ``None`` (the verbose log stays a derived, non-promoted sidecar).

    For concursus the ONLY promotion trigger for a raw log is ``status == "failed"`` — a failed
    record is already a ``counter_argument`` note (see :func:`_building_block_for`). This
    deliberately does NOT build a ``.3``-verdict-citation promotion path: turning a log into a
    cited verdict is the reasoning tier (Phase 5), out of scope for these write-time renderers. A
    non-failed log is not worth a durable note here — the run's append-only record log already
    captures it — so this returns ``None`` (the caller keeps it as a gitignored/derived sidecar or
    drops it). When the record IS failed, it renders through the same round-trip-exact path as any
    other response note, so the promoted note reloads byte-exact.
    """
    if record.status != "failed":
        return None  # POLICY: non-failure logs are never promoted to a durable note
    return _record_to_note(
        record,
        slipbox_form=slipbox_form,
        position=position,
        trail_id=trail_id,
        date=date,
        related=related,
    )


# A record_type -> renderer dispatch table (AI-18 umbrella), parallel to :func:`_building_block_for`'s
# switch: pick the note renderer by the record's kind. Unknown kinds fall back to the response
# renderer (widen-and-render, mirroring statestore's widen-and-warn for unknown record types).
_RENDERER_BY_RECORD_TYPE = {
    _DEDUP_RECORD_TYPE: capture_agent_response_note,  # a no-op re-put: a navigation marker note
    "agent_output": capture_agent_response_note,
    "checkpoint": capture_agent_response_note,
}


def capture_run_output_note(
    record: Record,
    *,
    slipbox_form: bool = True,
    position: int = 1,
    trail_id: str = "run",
    date: str = "",
    related: Optional[List[str]] = None,
) -> str:
    """**AI-18.** Dispatch a run-output :class:`Record` to the renderer for its ``record_type``.

    A thin umbrella over the per-kind renderers (parallel to :func:`_building_block_for`'s
    kind-switch): it picks a renderer by ``record.record_type`` — a ``failed`` record still routes
    through the response renderer here (this umbrella renders *every* record; the FAILURE-only
    promotion policy lives in :func:`capture_agent_log_note`). Unknown record types fall back to
    the response renderer. Read-only projection — it makes no runtime decision.
    """
    renderer = _RENDERER_BY_RECORD_TYPE.get(record.record_type, capture_agent_response_note)
    return renderer(
        record,
        slipbox_form=slipbox_form,
        position=position,
        trail_id=trail_id,
        date=date,
        related=related,
    )


def redact(payload: Mapping[str, Any], *, deny: Optional[List[str]] = None) -> Dict[str, Any]:
    """Return a shallow-redacted copy of a payload for durable capture (FZ 35e4a3a1b T3, PII gate).

    A payload can carry sensitive run inputs (case data). Before a payload is written to a durable
    note, drop any top-level key in ``deny`` (default :data:`_DEFAULT_REDACT_KEYS`) and mask its
    presence with a ``"<redacted>"`` sentinel so the note records THAT an input existed without its
    value. Deterministic + pure (no I/O). Nested redaction is intentionally out of scope for the
    spike — a caller with structured PII passes an explicit ``deny`` list. This is the prerequisite
    the counter + FZ 35e5a flagged for persisting payloads.
    """
    denied = set(deny if deny is not None else _DEFAULT_REDACT_KEYS)
    out: Dict[str, Any] = {}
    for key, value in dict(payload).items():
        out[key] = "<redacted>" if key in denied else value
    return out


#: Default top-level payload keys masked by :func:`redact` (common PII / secret carriers).
_DEFAULT_REDACT_KEYS = ("pii", "secret", "credentials", "customer_id", "case_data", "raw_input")


def capture_payload_note(
    node: str,
    payload: Mapping[str, Any],
    run_dir,
    *,
    trust_tier: str = "",
    trail_id: str = "run",
    date: str = "",
    related: Optional[List[str]] = None,
    redact_keys: Optional[List[str]] = None,
    slipbox_form: bool = True,
) -> str:
    """**FZ 35e4a3a1b T3.** Persist a node's frozen invoke PAYLOAD as a durable audit note; return
    its path (``<run_dir>/<node>__payload.md``).

    Renders the b2 payload contract — the (redacted) invoke payload the node was dispatched with
    plus the ``trust_tier`` the compiler selected — as a slipbox note. Like the plan snapshot it is
    NOT a run record: it is stamped ``concursus_note_kind: payload`` so :func:`_note_to_record`
    REFUSES to parse it (the record loaders skip it) — it can never leak into a resume/replay. PII
    is masked via :func:`redact` BEFORE the note is written. Pure post-run write; mutates nothing.
    """
    safe = redact(payload, deny=redact_keys)
    lines: List[str] = []
    if slipbox_form:
        fm = {
            "tags": ["resource", "concursus", "run_state", "payload"],
            "keywords": ["concursus payload", "invoke payload contract", "trust tier"],
            "topics": _SLIPBOX_TOPICS,
            "language": "markdown",
            "date of note": date,
            "status": "active",
            "building_block": "empirical_observation",  # an audit record of what was asked
            "folgezettel": "1",
            "lineage": [f"{trail_id}:1"],
            _PLAN_NOTE_MARKER: _PAYLOAD_NOTE_KIND,  # non-record stamp: never parsed as a Record
            "access_control_group": ["general"],
        }
        lines.append("---")
        for key, value in fm.items():
            if isinstance(value, list):
                lines.append(f"{key}:")
                lines.extend(f"  - {json.dumps(v)}" for v in value)
            else:
                lines.append(f"{key}: {json.dumps(value)}")
        lines.append("---")
        lines.append("")
    else:
        lines += ["---", f"{_PLAN_NOTE_MARKER}: {json.dumps(_PAYLOAD_NOTE_KIND)}", "---", ""]

    lines.append(f"# Invoke Payload: {node}")
    lines.append("")
    tier_note = f" (trust tier: `{trust_tier}`)" if trust_tier else ""
    lines.append(
        f"The frozen invoke payload `{node}` was dispatched with{tier_note} — a redacted audit "
        "snapshot of the b2 payload contract (wired inputs + tiered static context + tool_calls). "
        "This is a read-only projection: it drove no dispatch and mutates nothing."
    )
    lines.append("")
    if trust_tier:
        lines.append(f"## Trust Tier")
        lines.append("")
        lines.append(f"- `{trust_tier}`")
        lines.append("")
    lines.append("## Payload (redacted)")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(safe, indent=2, sort_keys=True, default=str))
    lines.append("```")
    lines.append("")
    if related:
        lines.append("## Related Notes")
        lines.append("")
        lines.extend(f"- {link}" for link in related)
        lines.append("")

    path = Path(run_dir) / f"{_slug(node)}__payload.md"
    Path(run_dir).mkdir(parents=True, exist_ok=True)
    FileVaultStateStore._atomic_write(path, "\n".join(lines))
    return str(path)


#: The heading a reciprocal-backlink post-pass appends under each producer note (FZ 35e4a3a1b T6).
_CONSUMED_BY_HEADING = "## Consumed By"


def add_reciprocal_backlinks(run_dir) -> int:
    """**FZ 35e4a3a1b T6.** Add reciprocal "consumed by" backlinks over a finished run's notes.

    ``filevault``'s ``_related_for`` links a note FORWARD to each producer it ``consumes`` — but a
    producer note is a dead-end w.r.t. WHO consumed it. This post-pass closes that gap: it reads
    every record note under ``run_dir``, derives the producer→consumers relation from the recorded
    ``consumes`` edges (already on disk — a projection, not new data), and appends a ``## Consumed
    By`` section to each producer's note listing links to its downstream consumers. Returns the
    number of producer notes amended. Idempotent — a re-run replaces the section rather than
    duplicating it. Pure post-run write over ``run_dir``; mutates no live plan, writes no Record
    (the amended notes stay their same round-trip-exact records — the added section is display-only,
    ignored by ``_note_to_record`` which reads only the ``meta``/``payload`` blobs).
    """
    run = Path(run_dir)
    if not run.exists():
        return 0
    # Reconstruct records from disk (skip navigation / stamped non-record notes).
    records: List[Record] = []
    for note in sorted(run.glob("*.md")):
        if note.name == "_run.md":
            continue
        try:
            records.append(_note_to_record(note.read_text(encoding="utf-8")))
        except (ValueError, json.JSONDecodeError, OSError):
            continue  # a stamped non-record (plan/payload) or malformed file — skip
    if not records:
        return 0
    _, latest_validated, attempts = _index_records(records)

    # Build producer -> [consumer, ...] from the consumes edges.
    consumers_of: Dict[str, List[str]] = {}
    for r in records:
        for edge in r.consumes:
            producer = edge.partition(":")[0]
            consumers_of.setdefault(producer, [])
            if r.node not in consumers_of[producer]:
                consumers_of[producer].append(r.node)

    amended = 0
    for producer, consumers in consumers_of.items():
        attempt = attempts.get(producer)
        if attempt is None:
            continue  # producer never validated on disk — nothing to amend
        pnote = run / f"{_slug(producer)}__a{attempt}.md"
        if not pnote.exists():
            continue
        links = []
        for consumer in sorted(consumers):
            c_attempt = attempts.get(consumer)
            if c_attempt is None:
                continue
            links.append(f"- [consumed by {consumer}]({_slug(consumer)}__a{c_attempt}.md)")
        if not links:
            continue
        text = pnote.read_text(encoding="utf-8")
        section = _CONSUMED_BY_HEADING + "\n\n" + "\n".join(links) + "\n"
        # Idempotent: replace an existing ## Consumed By section rather than duplicating it.
        idx = text.find(_CONSUMED_BY_HEADING)
        if idx != -1:
            new_text = text[:idx].rstrip() + "\n\n" + section
        else:
            new_text = text.rstrip() + "\n\n" + section
        FileVaultStateStore._atomic_write(pnote, new_text)
        amended += 1
    return amended


class FileVaultStateStore:
    """A persistent, on-disk :class:`StateStore` — durable markdown notes, resume by reload.

    Mirrors :class:`InProcessStateStore`'s ``put`` semantics (append-only log + a
    ``{node: latest validated output}`` projection, attempt auto-increment, content-hash dedup)
    and adds durability: each ``put`` writes one immutable note file (atomically), and a fresh
    store over an existing vault lazily reloads it before the first read (resume = replay with a
    filesystem transport). Concurrent writers over one vault are serialized by a reentrant lock
    plus a generation-token OCC read-fresh over ``.gen``.
    """

    def __init__(self, run_dir, *, slipbox_form: bool = True, trail_id: str = "run", date: str = "") -> None:
        self._dir = Path(run_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._slipbox_form = slipbox_form
        self._trail_id = trail_id
        self._date = date
        self._records: List[Record] = []
        self._projection: Dict[str, dict] = {}
        self._attempts: Dict[str, int] = {}
        self._clock: int = 0
        self._loaded = False
        self._lock = threading.RLock()
        self._depth = 0  # critical-section reentrancy depth (guarded by _lock)
        self._own_gen = -1

    @classmethod
    def from_config(
        cls, *, vault_path, session_id: str, slipbox_form: bool = True, date: str = ""
    ) -> "FileVaultStateStore":
        """Bind a run to ``<vault_path>/runs/<slug(session_id)>/`` (persistence-by-default posture).

        Emits SlipBox-conformant notes by default (``slipbox_form=True`` — validate under
        ``check_note_format.py``, indexer-ingestible); pass ``slipbox_form=False`` for the lean
        machine schema (a smaller, non-indexed round-trip-exact durable log). The run's
        ``trail_id`` (SlipBox lineage path id) is derived from ``session_id``. Callers that want
        ephemeral behaviour keep the bare :class:`InProcessStateStore` default; this is the
        explicit persistent choice (mirrors ``MemoryService.from_config``).
        """
        run_dir = Path(vault_path) / "runs" / _slug(session_id)
        return cls(run_dir, slipbox_form=slipbox_form, trail_id=_trail_id(session_id), date=date)

    # -- run identity (for post-run distillation; read-only accessors) ------
    @property
    def run_dir(self) -> Path:
        """The run's on-disk directory (the note substrate for this run)."""
        return self._dir

    @property
    def trail_id(self) -> str:
        """The run's SlipBox lineage/trail id (the family key for cross-run precedent)."""
        return self._trail_id

    # -- write --------------------------------------------------------------
    def put(self, node: str, output: dict, *, meta: Optional[dict] = None) -> None:
        with self._critical(write=True):
            self._ensure_loaded_locked()
            self._clock += 1
            attempt = self._attempts.get(node, 0) + 1
            self._attempts[node] = attempt
            chash = content_hash(output)
            dedup = node in self._projection and content_hash(self._projection[node]) == chash

            record = Record(
                node=node,
                output=dict(output),
                attempt=attempt,
                content_hash=chash,
                timestamp=self._clock,
            )
            _apply_meta(record, meta)
            if dedup and record.record_type == "agent_output":
                record.record_type = _DEDUP_RECORD_TYPE

            self._write_note(record)
            self._records.append(record)
            if record.status == "validated":
                self._projection[node] = record.output

    # -- reads --------------------------------------------------------------
    def get(self, node: str) -> dict:
        with self._critical():
            self._ensure_loaded_locked()
            if node not in self._projection:
                raise KeyError(node)
            return self._projection[node]

    def completed(self) -> Set[str]:
        with self._critical():
            self._ensure_loaded_locked()
            latest_overall, _, _ = _index_records(self._records)
            return {n for n, r in latest_overall.items() if r.status == "validated"}

    def records(self) -> List[Record]:
        with self._critical():
            self._ensure_loaded_locked()
            return list(self._records)

    # -- persistence --------------------------------------------------------
    def _note_filename(self, record: Record) -> str:
        addr = record.address or record.node
        return f"{_slug(addr)}__a{record.attempt}.md"

    def _write_note(self, record: Record) -> None:
        position = len(self._records) + 1  # 1-based write order → the record's FZ position
        related = self._related_for(record)
        text = _record_to_note(
            record,
            slipbox_form=self._slipbox_form,
            position=position,
            trail_id=self._trail_id,
            date=self._date,
            related=related,
        )
        self._atomic_write(self._dir / self._note_filename(record), text)
        if self._slipbox_form:
            self._write_run_entry()

    def _related_for(self, record: Record) -> List[str]:
        """The ``## Related Notes`` links for a record's note: the run entry point plus each
        upstream producer it ``consumes`` (as a link to that producer's latest note on disk)."""
        links = ["[Run entry point](_run.md)"]
        for edge in record.consumes:
            producer = edge.partition(":")[0]
            latest = self._attempts.get(producer)
            if latest:
                fname = f"{_slug(producer)}__a{latest}.md"
                links.append(f"[consumes {producer}]({fname})")
        return links

    def _write_run_entry(self) -> None:
        """Regenerate the run's ``_run.md`` Folgezettel entry point — a SlipBox-conformant
        navigation note listing every record in the run (so no note is an orphan and the run
        reads as a genuine trail with a root)."""
        rows = [
            f"- [{r.node} a{r.attempt}]({self._note_filename(r)}) — {r.status}"
            for r in self._records
        ]
        fm = {
            "tags": ["resource", "concursus", "run_state", "entry_point"],
            "keywords": ["concursus run", "run state trail", "folgezettel entry point"],
            "topics": _SLIPBOX_TOPICS,
            "language": "markdown",
            "date of note": self._date,
            "status": "active",
            "building_block": "navigation",
            "folgezettel": "1",
            "lineage": [f"{self._trail_id}:1"],
            "access_control_group": ["general"],
        }
        lines = ["---"]
        for key, value in fm.items():
            if isinstance(value, list):
                lines.append(f"{key}:")
                lines.extend(f"  - {json.dumps(v)}" for v in value)
            else:
                lines.append(f"{key}: {json.dumps(value)}")
        lines += ["---", "", "# Run State: trail entry point", "",
                  f"The Folgezettel root of this concursus run (trail `{self._trail_id}`). "
                  "Each record below is one node output, addressed as a child of this root.", ""]
        lines += rows if rows else ["- (no records yet)"]
        lines.append("")
        self._atomic_write(self._dir / "_run.md", "\n".join(lines))

    @staticmethod
    def _atomic_write(path: Path, text: str) -> None:
        """Write ``text`` to ``path`` atomically (temp file in the same dir + ``os.replace``)."""
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, path)

    def _load(self) -> None:
        """Rebuild the in-memory state from the note files (resume = replay over the filesystem).

        Reads every ``*.md`` under the run dir, parses each into a :class:`Record`, then runs the
        same tail as :meth:`MemoryStateStore.replay`: :func:`_index_records` → projection /
        attempts. Records are ordered by their monotonic ``timestamp`` so last-write-wins is stable.
        """
        records: List[Record] = []
        for note in sorted(self._dir.glob("*.md")):
            if note.name in ("_run.md", _PLAN_NOTE_NAME):
                continue  # navigation / plan-snapshot notes, not records
            try:
                records.append(_note_to_record(note.read_text(encoding="utf-8")))
            except (ValueError, json.JSONDecodeError, OSError):
                continue  # skip a malformed / partial file rather than abort the whole reload
        records.sort(key=lambda r: (r.timestamp if r.timestamp is not None else 0))

        _, latest_validated, attempts = _index_records(records)
        self._records = records
        self._projection = {node: r.output for node, r in latest_validated.items()}
        self._attempts = attempts
        self._clock = max((r.timestamp or 0 for r in records), default=0)
        self._own_gen = self._read_gen()  # sync to committed generation (OCC baseline)
        self._loaded = True

    def _ensure_loaded_locked(self) -> None:
        if not self._loaded:
            self._load()

    # -- cross-process write guard (RLock + generation-token OCC) -----------
    def _critical(self, *, write: bool = False):
        return _Critical(self, write=write)

    def _read_gen(self) -> int:
        try:
            return int((self._dir / _GEN_NAME).read_text(encoding="utf-8").strip() or "0")
        except (OSError, ValueError):
            return 0

    def _bump_gen(self) -> None:
        self._own_gen = self._read_gen() + 1
        self._atomic_write(self._dir / _GEN_NAME, str(self._own_gen))


class _Critical:
    """Reentrant critical section: the in-process ``RLock`` plus a cross-process advisory lock and
    a generation-token OCC read-fresh. On the OUTERMOST entry it takes an exclusive ``fcntl``
    lock on ``.lock``, and if a peer advanced ``.gen`` since our last write it reloads before
    mutating (so a write always allocates over the current committed state); it bumps ``.gen`` on
    exit. Degrades to the RLock alone where ``fcntl`` is unavailable (non-POSIX)."""

    def __init__(self, store: "FileVaultStateStore", *, write: bool) -> None:
        self._store = store
        self._write = write
        self._fh = None
        self._outermost = False

    def __enter__(self) -> "_Critical":
        store = self._store
        store._lock.acquire()
        self._outermost = store._depth == 0
        store._depth += 1
        if not self._outermost:
            return self
        try:
            import fcntl  # POSIX only

            self._fh = open(store._dir / _LOCK_NAME, "w")
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        except (ImportError, OSError):
            self._fh = None
        # OCC read-fresh: a peer advanced the on-disk generation → reload before we touch state.
        if store._loaded and store._read_gen() != store._own_gen:
            store._load()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        store = self._store
        try:
            if self._outermost:
                if self._write and exc_type is None:
                    store._bump_gen()
                if self._fh is not None:
                    try:
                        import fcntl

                        fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
                    finally:
                        self._fh.close()
                        self._fh = None
        finally:
            store._depth -= 1
            store._lock.release()
