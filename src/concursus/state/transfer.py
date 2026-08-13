"""Session-end knowledge-transfer connector — the ``slipbox_transfer`` terminal node.

The two paired pieces that make a run's episodic memory flow into permanent Slipbox notes via a
knowledge-consolidation sub-agent, on both request-completion and termination:

- **C1** — :func:`build_slipbox_transfer_manifest` authors the ``slipbox_transfer`` agent manifest
  (an MCP node whose ``contract.outputs`` + per-field ``acceptance`` rules mirror the consolidation
  sub-agent's REAL job dict, so a session cannot report green unless the transfer job reached
  ``state == "complete"`` with a non-empty ``result_path``), and
  :func:`wire_slipbox_transfer_terminal` wires it as the sole DAG sink that ``depends_on`` the run's
  current sinks (manifest ``spec.depends_on`` AND the matching DAG edges
  :func:`~concursus.core.resolve.check_alignment` requires, with a distinct ``to`` input per producer
  so the single-writer gate passes).
- **C4** — :func:`register_slipbox_foundry` makes the consolidation sub-agent a standing crew member
  the router can bind to ``slipbox_transfer`` (registry capability metadata + one append-only
  ``DeployLedger`` row so ``match_task`` resolves it).
- **C2** — :func:`export_run_log` copies a finished run's episodic notes into the sub-agent's local
  ingestion inbox; :func:`distill_export` wires the cross-run precedent.
- **C3** — :class:`TransferTriggerSink` (fired at ``synthesize`` via a
  :class:`~concursus.governor.FanOutEventSink`) + :func:`sweep_untransferred_runs` (the
  reaper/next-boot backstop) fire the export at the strictly-outer moment for both exit paths.
- **Rollup** — :func:`session_overall_ok` is false unless the transfer ran and was accepted.

**Identity guard (INV-1..5).** Everything here is a COMPILE-TIME manifest/DAG builder, a
registry/ledger seed, a post-run external write, or a strictly-outer episode-boundary observer —
never an in-``Supervisor.run`` mutation. Nothing runs unless a caller explicitly invokes it, so a
run that does not build the transfer node is byte-for-byte unchanged. The consolidation sub-agent's
job-dict key set the C1 contract mirrors is captured in :data:`CONSOLIDATOR_JOB_DICT_KEYS` so a
contract-parity test can assert the manifest never invents a field.

Exports come in three shapes: :func:`export_run_log` (raw episodic notes to a local inbox),
:func:`export_run_digest` (the consolidation-digestible digest bundle, via
:func:`~concursus.state.filevault.render_digest_view_note`), and
:func:`export_run_log_to_object_store` (the S3/``ObjectStore`` push variant). All are opt-in and
INV-4-safe (read notes, write an external target; never re-put a Record).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Set

if TYPE_CHECKING:  # type hints only — avoid import cost / cycles at runtime
    from concursus.build.ledger import DeployLedger
    from concursus.core.dag import AgentDAG
    from concursus.core.manifest import AgentManifest
    from concursus.governor.registry import AgentRegistry

#: The canonical node id / task label of the terminal transfer node.
SLIPBOX_TRANSFER_NODE = "slipbox_transfer"

#: The capability labels the consolidation sub-agent serves.
SLIPBOX_FOUNDRY_CAPABILITIES: Set[str] = {SLIPBOX_TRANSFER_NODE, "slipbox_foundry"}

#: The EXACT key set the consolidation sub-agent returns from its submit/get-job MCP tools. A
#: manifest's declared output keys MUST be a subset of this — the transfer contract mirrors the real
#: API, never invents a field (the contract-parity guarantee). ``created``/``events`` are the additive
#: keys the submit / get-job wrappers attach.
CONSOLIDATOR_JOB_DICT_KEYS: Set[str] = {
    "job_id",
    "state",
    "lane",
    "source_event_id",
    "capability",
    "attempts",
    "commit_attempts",
    "cancel_requested",
    "last_error",
    "result_path",
    "supersedes_job_id",
    "created",
    "events",
}

#: The terminal SUCCESS state. The acceptance gate demands the transfer job settle here with a
#: non-empty ``result_path``.
CONSOLIDATOR_COMPLETE_STATE = "complete"

#: The prefix of the ingestion objective for one session's bundle.
_EXPORT_OBJECTIVE_PREFIX = "hive-session-"

#: The per-run success marker. NOT ``*.md`` -> invisible to the note globs / loaders / export set.
_TRANSFER_MARKER_NAME = ".slipbox_transferred"


# -- C1: the terminal-node manifest + acceptance contract -------------------
def build_slipbox_transfer_manifest(
    *,
    agent_runtime_arn: Optional[str] = None,
    container_uri: Optional[str] = None,
    role_arn: Optional[str] = None,
    qualifier: str = "DEFAULT",
    name: str = SLIPBOX_TRANSFER_NODE,
) -> "AgentManifest":
    """Author the ``slipbox_transfer`` terminal-node manifest (C1); return a validated manifest.

    An MCP node whose ``contract.outputs`` mirror the consolidation sub-agent's real job dict and
    whose per-field ``acceptance`` rules make the node MANDATORY + FAIL-CLOSED: the run cannot report
    the transfer node green unless ``state == "complete"`` (enum) with a non-empty ``result_path``
    (non_empty). ``side_effecting`` (it writes the external vault), so it enters the Trust Ladder at
    ``L0_SHADOW``. Provide exactly one AgentCore hosting handle (``agent_runtime_arn`` to reuse, or
    ``container_uri`` + ``role_arn`` to provision); passing neither raises ``ManifestError`` from
    ``validate()`` (fail-closed — never a fabricated ARN). ``qualifier`` defaults to ``DEFAULT``.
    """
    from concursus.core.manifest import AgentManifest

    registry: Dict[str, Any] = {"protocol": "MCP", "qualifier": qualifier}
    if agent_runtime_arn:
        registry["agent_runtime_arn"] = agent_runtime_arn
    if container_uri:
        registry["container_uri"] = container_uri
    if role_arn:
        registry["role_arn"] = role_arn

    contract = {
        "outputs": {
            "properties": {
                "job_id": {"type": "string"},
                "state": {"type": "string", "acceptance": {"enum": [CONSOLIDATOR_COMPLETE_STATE]}},
                "result_path": {"type": "string", "acceptance": {"non_empty": True}},
                "last_error": {"type": ["string", "null"]},
            }
        }
    }
    manifest = AgentManifest.from_dict(
        {"name": name, "registry": registry, "side_effecting": True, "contract": contract}
    )
    return manifest.validate()


def wire_slipbox_transfer_terminal(
    dag: "AgentDAG",
    manifest: "AgentManifest",
    *,
    producer_outputs: Optional[Dict[str, str]] = None,
    node: str = SLIPBOX_TRANSFER_NODE,
) -> "AgentManifest":
    """Wire ``slipbox_transfer`` as the sole terminal sink over the run's current sinks (C1).

    Adds the transfer node to ``dag``, and for EACH producer adds both the DAG edge
    ``producer -> node`` AND a ``spec.depends_on`` entry with a DISTINCT ``to`` input name — both
    required by :func:`~concursus.core.resolve.check_alignment`. After wiring, ``node`` is the DAG's
    only sink. The per-producer inputs are declared on the MANIFEST (``contract.inputs``), which is
    where this ``check_alignment`` reads consumer inputs from — the DAG node here carries no I/O.

    ``producer_outputs`` maps each current sink to the output field the edge should read (the caller
    supplies it because a bare DAG node carries no declared outputs here); a producer absent from the
    map falls back to the conventional ``result`` field. Returns the mutated ``manifest`` (its
    ``spec.depends_on`` + ``contract.inputs`` populated); the ``dag`` is mutated in place. Mutates
    only the passed-in pre-freeze ``dag`` + ``manifest`` — never a frozen plan (INV-1).
    """
    sinks = dag.sinks()
    if not sinks:
        raise ValueError(
            "wire_slipbox_transfer_terminal: no producers to depend on "
            "(an empty DAG has no run to transfer)"
        )
    outs = dict(producer_outputs or {})

    node_inputs: Dict[str, Dict[str, Any]] = {}
    depends_on: List[Dict[str, str]] = []
    for producer in sinks:
        out_field = outs.get(producer, "result")
        input_name = f"from_{producer}"
        node_inputs[input_name] = {"type": "object"}
        depends_on.append({"from": f"{producer}.{out_field}", "to": input_name})

    dag.add_node(node)
    for producer in sinks:
        dag.add_edge(producer, node)

    # Declare the consumer inputs + depends_on on the manifest (this check_alignment reads inputs
    # from manifest.inputs, and requires BOTH sides — manifest depends_on AND the DAG edge — to agree).
    contract = dict(getattr(manifest, "contract", {}) or {})
    contract["inputs"] = node_inputs
    manifest.contract = contract
    spec = dict(getattr(manifest, "spec", {}) or {})
    spec["depends_on"] = depends_on
    manifest.spec = spec
    return manifest


def slipbox_transfer_acceptance_fn(node: str) -> bool:
    """The opt-in ``acceptance_fn`` predicate that QA-gates ONLY the transfer node (C1).

    Pass to ``Supervisor(check_acceptance=True, acceptance_fn=slipbox_transfer_acceptance_fn)`` so a
    non-``complete`` ``state`` or an empty ``result_path`` on the transfer node raises ``SchemaError``
    (recorded-failed, earns no trust), while every OTHER node runs unguarded. ``check_acceptance`` is
    the master switch — this predicate is inert without it, so it is default-off.
    """
    return node == SLIPBOX_TRANSFER_NODE


# -- C4: register the consolidation sub-agent -------------------------------
def register_slipbox_foundry(
    registry: "AgentRegistry",
    ledger: "DeployLedger",
    *,
    manifest: Optional["AgentManifest"] = None,
    fingerprint: str = "slipbox-foundry-dev",
    deployed_at: str = "1970-01-01T00:00:00Z",
    arn: Optional[str] = None,
) -> "AgentManifest":
    """Register the consolidation sub-agent as a standing agent serving ``slipbox_transfer`` (C4).

    ``registry.register_agent(manifest, capabilities=SLIPBOX_FOUNDRY_CAPABILITIES)`` teaches the
    registry which tasks the NAMED agent serves; ``ledger.record(...)`` is the standing row that
    makes ``match_task("slipbox_transfer")`` resolve it. The manifest ``name`` and the ledger ``name``
    MUST be byte-identical (capabilities key on the agent NAME) — both use ``manifest.name`` here. The
    ``arn`` propagates to ``AgentVersion.arn`` (the runtime handle). Both writes are append-only
    (INV-4). Default-off: nothing calls this unless a caller opts into standing-up the transfer crew.
    """
    if manifest is None:
        manifest = _build_slipbox_foundry_manifest(agent_runtime_arn=arn)
    registry.register_agent(manifest, capabilities=set(SLIPBOX_FOUNDRY_CAPABILITIES))
    ledger.record(name=manifest.name, fingerprint=fingerprint, deployed_at=deployed_at, arn=arn)
    return manifest


def _build_slipbox_foundry_manifest(*, agent_runtime_arn: Optional[str] = None) -> "AgentManifest":
    """A minimal standing consolidation-sub-agent manifest for the dev registry/ledger path (C4)."""
    from concursus.core.manifest import AgentManifest

    registry: Dict[str, Any] = {"protocol": "MCP", "capabilities": sorted(SLIPBOX_FOUNDRY_CAPABILITIES)}
    if agent_runtime_arn:
        registry["agent_runtime_arn"] = agent_runtime_arn
    return AgentManifest.from_dict(
        {
            "name": "SlipboxFoundry",
            "registry": registry,
            "side_effecting": True,
            "contract": {"outputs": {"properties": {"result_path": {"type": "string"}}}},
        }
    )


# -- C2: episodic-log export (local inbox path) -----------------------------
def _write_export_note_if_changed(out_path, text: str) -> None:
    """Write ``text`` to ``out_path`` ONLY when its content would change — else leave the file (and
    its inode/mtime) untouched. Idempotency-critical: the consolidation sub-agent's ingestion dedup
    binds the source file's inode/mtime/size, so an unconditional atomic overwrite would churn
    identity on re-export and duplicate the digestion job. Skipping the identical write keeps the
    source identity stable so re-admission dedups."""
    from concursus.state.filevault import FileVaultStateStore

    try:
        if out_path.exists() and out_path.read_text(encoding="utf-8") == text:
            return
    except OSError:
        pass
    FileVaultStateStore._atomic_write(out_path, text)


def export_run_log(
    run_dir,
    target_dir,
    *,
    admit_fn: Optional[Any] = None,
    objective: Optional[str] = None,
    trail_id: str = "run",
) -> Dict[str, Any]:
    """Export a finished run's episodic notes to ``target_dir`` (C2); return an export result.

    Copies every TOP-LEVEL ``*.md`` note under ``run_dir`` (byte-identical) into ``target_dir`` — the
    run's slipbox-form notes ARE the multi-member corpus the consolidation sub-agent admits as one
    objective. The glob is NON-recursive, so the derived sidecar trees (``versions/``, ``index/``) are
    never exported. Re-export is IDEMPOTENT via :func:`_write_export_note_if_changed`.

    ``admit_fn`` (OPT-IN, default ``None``): an injected ``admit_bundle``-shaped callable
    ``admit_fn(members, objective=...)``. Called with the exported member paths AFTER they are on
    disk. When ``None``, this is a pure file-drop. ``objective`` defaults to
    ``hive-session-<trail_id>``. INV-4 safe (reads notes, writes an external dir).
    """
    from pathlib import Path

    src = Path(run_dir)
    dst = Path(target_dir)
    obj = objective or f"{_EXPORT_OBJECTIVE_PREFIX}{trail_id}"
    members: List[str] = []
    if src.is_dir():
        dst.mkdir(parents=True, exist_ok=True)
        for note in sorted(src.glob("*.md")):  # NON-recursive: sidecar trees are subdirs, excluded
            if not note.is_file():
                continue
            out_path = dst / note.name
            _write_export_note_if_changed(out_path, note.read_text(encoding="utf-8"))
            members.append(str(out_path))

    admitted = None
    if admit_fn is not None and members:
        admitted = admit_fn(members, objective=obj)
    return {"members": members, "objective": obj, "admitted": admitted}


def distill_export(store, *, vault_path=None) -> str:
    """Wire the (otherwise-unwired) cross-run precedent distillation for the export (C2).

    A thin EXPLICIT caller for :func:`~concursus.state.distill.distill_store`. Folds the finished
    ``store`` into one compact precedent note under ``<vault>/precedents/`` so the transfer also feeds
    the plan-author's cross-run retrieval path. Pure post-run write (reads the finished store, writes
    OUTSIDE any run dir), INV-4 safe."""
    from concursus.state.distill import distill_store

    return distill_store(store, vault_path=vault_path)


#: Run-dir sidecar trees the export must NOT copy (derived/rebuilt state, not episodic notes).
_EXPORT_SKIP_DIRS = ("versions", "index")


def _is_sidecar_member(note) -> bool:
    """True iff ``note`` lives under a derived sidecar tree (:data:`_EXPORT_SKIP_DIRS`) — a
    belt-and-braces guard so a derived note can never leak into the export even if the glob were
    ever made recursive."""
    return any(part in _EXPORT_SKIP_DIRS for part in note.parts)


def export_run_digest(
    records,
    target_dir,
    *,
    admit_fn: Optional[Any] = None,
    objective: Optional[str] = None,
    trail_id: str = "run",
    date: str = "",
) -> Dict[str, Any]:
    """Export a run as CONSOLIDATION-DIGESTIBLE notes to ``target_dir`` (C2 digest variant); returns
    a result dict shaped like :func:`export_run_log`.

    Ships the run's digest bundle (prose, BB-classified, backticked, provenance-cited), NOT the raw
    JSON-blob record notes — so the plan-phase can section them and a consolidation digester's
    identifier-grounding finds the identifiers verbatim. Renders one
    :func:`~concursus.state.filevault.render_digest_view_note` per record, then optionally admits the
    bundle via the injected ``admit_fn`` exactly as :func:`export_run_log` does.

    Pure post-run projection (INV-4): each note is a NON-record digest view (stamped so the loaders
    skip it), so this never mutates the log or affects replay. An empty ``records`` writes nothing.
    Re-export is IDEMPOTENT: the digest renderer writes atomically (new inode every call), so instead
    of rendering straight into ``target_dir`` we render into a STAGING temp dir, then land each note
    via :func:`_write_export_note_if_changed` — leaving an unchanged note's inode/mtime intact so the
    consolidation sub-agent dedups the re-admission.
    """
    import tempfile
    from pathlib import Path

    from concursus.state.filevault import render_digest_view_note

    dst = Path(target_dir)
    obj = objective or f"{_EXPORT_OBJECTIVE_PREFIX}{trail_id}"
    members: List[str] = []
    recs = list(records or [])
    if recs:
        dst.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory() as staging:
            for record in recs:
                # render into staging (churns a throwaway inode), then land idempotently in dst
                staged = render_digest_view_note(record, staging, trail_id=trail_id, date=date)
                out_path = dst / Path(staged).name
                _write_export_note_if_changed(out_path, Path(staged).read_text(encoding="utf-8"))
                members.append(str(out_path))

    admitted = None
    if admit_fn is not None and members:
        admitted = admit_fn(members, objective=obj)
    return {"members": members, "objective": obj, "admitted": admitted}


def export_run_log_to_object_store(
    run_dir,
    object_store,
    *,
    prefix: str,
    trail_id: str = "run",
) -> Dict[str, Any]:
    """Push a finished run's episodic notes to an S3 (or any :class:`ObjectStore`) prefix (C2 push
    variant).

    The operator's PUSH variant of the inbox export: instead of dropping notes into a local inbox, it
    writes each note to ``<prefix>/<name>`` via the injected ``object_store`` — an
    ``execute.object_store.ObjectStore`` (``S3Store`` for real S3, ``FileStore`` for local/tests;
    either exposes ``async put_object(uri, data, content_type)``). The async put is wrapped with
    ``asyncio.run``, so this is a plain synchronous call.

    Concursus never hard-depends on boto3 here — the caller injects the store (tests use ``FileStore``,
    prod wires ``S3Store``). Selects the same top-level, non-sidecar ``*.md`` notes as
    :func:`export_run_log`. Returns ``{"members": [<uri>], "prefix": <str>}``. NOTE: a consolidation
    sub-agent that reads a local inbox needs a corresponding S3-PULL lane to ingest from ``prefix`` —
    that lane is out of scope here (this is the push half). INV-4 safe: reads notes, writes an external
    object store; no Record, no plan mutation.
    """
    import asyncio
    from pathlib import Path

    src = Path(run_dir)
    base = prefix.rstrip("/")
    uris: List[str] = []
    if src.is_dir():
        for note in sorted(src.glob("*.md")):
            if not note.is_file() or _is_sidecar_member(note):
                continue
            uri = f"{base}/{note.name}"
            data = note.read_bytes()
            asyncio.run(object_store.put_object(uri, data, "text/markdown"))
            uris.append(uri)
    return {"members": uris, "prefix": base, "trail_id": trail_id}


# -- C3: the session-end transfer triggers ----------------------------------
def run_needs_transfer(run_dir) -> bool:
    """True iff ``run_dir`` is a real run dir whose transfer has NOT yet been marked done (C3).

    The gate the reaper/next-boot backstop consults. Pure read."""
    from pathlib import Path

    d = Path(run_dir)
    return d.is_dir() and not (d / _TRANSFER_MARKER_NAME).exists()


def mark_transferred(run_dir, *, objective: str = "", when: str = "") -> str:
    """Write the per-run transfer success marker; return its path (C3 idempotency stamp).

    A tiny non-``.md`` sentinel — the note loaders/globs never see it, so it can never leak into a
    bundle or be parsed as a Record."""
    import json as _json
    from pathlib import Path

    from concursus.state.filevault import FileVaultStateStore

    path = Path(run_dir) / _TRANSFER_MARKER_NAME
    FileVaultStateStore._atomic_write(
        path, _json.dumps({"transferred": True, "objective": objective, "when": when}) + "\n"
    )
    return str(path)


def transfer_run(
    run_dir,
    target_dir,
    *,
    admit_fn: Optional[Any] = None,
    trail_id: str = "run",
    when: str = "",
) -> Dict[str, Any]:
    """Export one run's episodic memory to the inbox and MARK it transferred (the digest primitive).

    The single reusable action C3's triggers fire: it runs :func:`export_run_log`, and on success
    writes the idempotency marker. Idempotent: a run already marked is a no-op (returns
    ``{"skipped": True}``). Pure post-run (INV-4)."""
    if not run_needs_transfer(run_dir):
        return {"skipped": True, "members": [], "objective": "", "admitted": None, "marker": None}
    result = export_run_log(run_dir, target_dir, admit_fn=admit_fn, trail_id=trail_id)
    result["marker"] = mark_transferred(run_dir, objective=result.get("objective", ""), when=when)
    return result


class TransferTriggerSink:
    """(C3) An opt-in :class:`EventSink` that fires the session-end transfer at ``synthesize``.

    Wire it (alongside a phase-note sink via :class:`~concursus.governor.FanOutEventSink`) as the
    governor's ``event_sink``. On the episode-boundary ``decision`` event whose ``route`` is
    ``synthesize`` — the strictly-outer end of the run — it exports the run's episodic notes to
    ``target_dir`` (optionally admitting them via the injected ``admit_fn``) and marks the run
    transferred. ``route`` is read from the plain-dict event, NOT ``episode_end.done``. Observer-only
    (INV-3): reads the frozen event VALUE, writes the external inbox + a run-dir marker; never touches
    ctx/plan/log. Fires at most once per run (marker-gated); errors are swallowed by the loop's emit
    guard, and the backstop still catches an un-marked run."""

    def __init__(
        self,
        run_dir,
        target_dir,
        *,
        admit_fn: Optional[Any] = None,
        trail_id: str = "run",
        date: str = "",
    ) -> None:
        self._run_dir = run_dir
        self._target_dir = target_dir
        self._admit_fn = admit_fn
        self._trail_id = trail_id
        self._date = date
        self.last_result: Optional[Dict[str, Any]] = None

    def emit(self, event) -> None:
        ev = dict(event or {})
        if ev.get("type") != "decision" or ev.get("route") != "synthesize":
            return
        self.last_result = transfer_run(
            self._run_dir, self._target_dir,
            admit_fn=self._admit_fn, trail_id=self._trail_id, when=self._date,
        )


def recover_trail_id(run_dir) -> Optional[str]:
    """Recover a run's REAL ``trail_id`` from its persisted notes (the ``lineage:`` prefix).

    The run dir is named ``_slug(session_id)`` while the run's actual ``trail_id`` is a DIFFERENT
    transform, so ``run_dir.name`` is NOT the trail_id. The authoritative trail_id is stamped into
    every note's ``lineage: ["<trail_id>:<fz>"]`` frontmatter; this reads the first note's first
    lineage entry and returns the segment before the ``:``. Returns ``None`` when no note/lineage is
    found. Pure read."""
    from pathlib import Path

    d = Path(run_dir)
    if not d.is_dir():
        return None
    for note in sorted(d.glob("*.md")):
        try:
            text = note.read_text(encoding="utf-8")
        except OSError:
            continue
        in_lineage = False
        for line in text.splitlines():
            stripped = line.strip()
            if stripped == "---" and in_lineage:
                break
            if stripped.startswith("lineage:"):
                in_lineage = True
                continue
            if in_lineage and stripped.startswith("- "):
                entry = stripped[2:].strip().strip('"').strip("'")
                prefix = entry.split(":", 1)[0].strip()
                if prefix:
                    return prefix
            elif in_lineage and stripped and not stripped.startswith("- "):
                in_lineage = False
    return None


def sweep_untransferred_runs(
    runs_root,
    target_dir_for,
    *,
    admit_fn: Optional[Any] = None,
    trail_id_for: Optional[Any] = None,
    when: str = "",
) -> List[Dict[str, Any]]:
    """(C3) Backstop: transfer every run under ``runs_root`` that has no success marker.

    The reaper-caller / next-boot pass. A graceful-synthesize miss or a hard microVM teardown can
    leave a durable run log with no ``slipbox_transfer`` success; this sweeps ``<runs_root>/*`` and,
    for each run still needing transfer, exports + marks it — so at-least-once (trigger, reaper, next
    boot) converges to exactly-once. ``target_dir_for(run_dir, trail_id)`` maps a run to its inbox
    target. The ``trail_id`` is recovered from the run's notes (:func:`recover_trail_id`), NOT from
    ``run_dir.name`` (a different transform), so the backstop objective MATCHES the graceful
    trigger's and the sub-agent dedups. ``trail_id_for(run_dir)`` overrides the recovery. Pure post-run."""
    from pathlib import Path

    root = Path(runs_root)
    results: List[Dict[str, Any]] = []
    if not root.is_dir():
        return results
    for run_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        if not run_needs_transfer(run_dir):
            continue
        if trail_id_for is not None:
            trail_id = trail_id_for(str(run_dir))
        else:
            trail_id = recover_trail_id(run_dir) or run_dir.name
        target = target_dir_for(str(run_dir), trail_id)
        results.append(
            transfer_run(str(run_dir), target, admit_fn=admit_fn, trail_id=trail_id, when=when)
        )
    return results


# -- Rollup: a session is not green unless the transfer ran + was accepted ---
def transfer_node_ok(store, *, node: str = SLIPBOX_TRANSFER_NODE) -> bool:
    """True iff the ``slipbox_transfer`` node completed with a terminal-SUCCESS job (rollup).

    A pure read over a finished ``store``: the node must be in ``completed()`` (so it passed the C1
    acceptance gate) AND its recorded output must carry ``state == "complete"`` with a non-empty
    ``result_path`` (defense-in-depth re-check). No I/O beyond the store reads."""
    try:
        if node not in store.completed():
            return False
        output = store.get(node) or {}
    except Exception:  # noqa: BLE001 - a store read hiccup is not a green transfer
        return False
    if not isinstance(output, dict):
        return False
    return output.get("state") == CONSOLIDATOR_COMPLETE_STATE and bool(output.get("result_path"))


def session_overall_ok(
    store,
    *,
    node: str = SLIPBOX_TRANSFER_NODE,
    plan_order: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Compute the session's transfer-inclusive verdict; return a rollup dict.

    **A session cannot report green unless the transfer ran and was accepted.** Returns
    ``{"overall_ok", "transfer_ok", "transfer_present", "work_complete", "completed",
    "total_completed"}``. ``overall_ok`` is ``transfer_ok`` (the ``slipbox_transfer`` node completed
    with a terminal-success job) AND ``work_complete``. Pass ``plan_order`` to ALSO require every
    planned node completed. Fail-closed: no transfer node => not green. Pure read; INV-safe."""
    try:
        completed = sorted(store.completed())
    except Exception:  # noqa: BLE001
        completed = []
    completed_set = set(completed)
    transfer_present = node in completed_set
    transfer_ok = transfer_node_ok(store, node=node)
    work_complete = True
    if plan_order is not None:
        work_complete = all(n in completed_set for n in plan_order)
    return {
        "overall_ok": bool(transfer_ok and work_complete),
        "transfer_ok": bool(transfer_ok),
        "transfer_present": bool(transfer_present),
        "work_complete": bool(work_complete),
        "completed": completed,
        "total_completed": len(completed),
    }


__all__ = [
    "SLIPBOX_TRANSFER_NODE",
    "SLIPBOX_FOUNDRY_CAPABILITIES",
    "CONSOLIDATOR_JOB_DICT_KEYS",
    "CONSOLIDATOR_COMPLETE_STATE",
    "build_slipbox_transfer_manifest",
    "wire_slipbox_transfer_terminal",
    "slipbox_transfer_acceptance_fn",
    "register_slipbox_foundry",
    "export_run_log",
    "export_run_digest",
    "export_run_log_to_object_store",
    "distill_export",
    "run_needs_transfer",
    "mark_transferred",
    "transfer_run",
    "TransferTriggerSink",
    "recover_trail_id",
    "sweep_untransferred_runs",
    "transfer_node_ok",
    "session_overall_ok",
]
