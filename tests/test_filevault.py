"""Tests for the FileVaultStateStore (persistent on-disk slipbox) + the derived run DB.

Exercises the durability gap that InProcessStateStore (in-memory) and MemoryStateStore (opaque
AgentCore Blob events) leave open: round-trip-exact note serialization of arbitrary outputs,
resume-by-reload across a fresh store, StateStore-Protocol parity with InProcessStateStore, and a
derived SQLite run DB rebuilt entirely from the notes.
"""

import json
import os
import sqlite3
import time

import pytest

from concursus.state import filevault as _fv
from concursus.state.filevault import (
    FileVaultStateStore,
    RunHeartbeatLock,
    RunLockHeldError,
    _decode_blob,
    _encode_blob,
    _note_to_record,
    _record_to_note,
)
from concursus.state.rundb import build_run_db, load_records
from concursus.state.rungraph import RunGraph
from concursus.state.runindex import RunIndex
from concursus.state.statestore import InProcessStateStore, Record


# -- round-trip exactness ---------------------------------------------------
def test_blob_roundtrip_survives_yaml_hostile_content():
    payload = {
        "text": "line1\nline2: value --- with dashes",
        "quote": 'he said "hi"',
        "link": "[a note](../x.md)",
        "numeric_string": "007",
        "nested": {"a": [1, 2, {"b": None}]},
    }
    assert _decode_blob(_encode_blob(payload)) == payload


def test_note_roundtrip_is_exact():
    rec = Record(
        node="summarize",
        output={"summary": "multi\nline\n---\ntext", "n": 3, "s": "0123"},
        attempt=2,
        status="validated",
        schema="summarize-agent",
        producer="summarize",
        consumes=["ingest:$.document", "translate:$.text"],
        content_hash="abc",
        timestamp=5,
        address="summarize",
    )
    back = _note_to_record(_record_to_note(rec))
    assert back.node == rec.node
    assert back.output == rec.output  # exact — the whole point
    assert back.attempt == rec.attempt
    assert back.status == rec.status
    assert back.schema == rec.schema
    assert back.producer == rec.producer
    assert back.consumes == rec.consumes
    assert back.address == rec.address


# -- StateStore parity with the in-process default --------------------------
def test_put_get_completed_match_inprocess(tmp_path):
    fv = FileVaultStateStore(tmp_path / "run")
    ip = InProcessStateStore()
    for store in (fv, ip):
        store.put("a", {"x": 1}, meta={"producer": "a"})
        store.put("b", {"y": 2}, meta={"producer": "b", "consumes": ["a:$.x"]})
    assert fv.get("a") == ip.get("a") == {"x": 1}
    assert fv.completed() == ip.completed() == {"a", "b"}
    assert len(fv.records()) == len(ip.records()) == 2


def test_missing_node_raises_keyerror(tmp_path):
    fv = FileVaultStateStore(tmp_path / "run")
    with pytest.raises(KeyError):
        fv.get("nope")


def test_dedup_marks_identical_reput(tmp_path):
    fv = FileVaultStateStore(tmp_path / "run")
    fv.put("a", {"x": 1})
    fv.put("a", {"x": 1})  # identical -> dedup
    recs = fv.records()
    assert len(recs) == 2
    assert recs[0].record_type == "agent_output"
    assert recs[1].record_type == "dedup"
    assert recs[1].attempt == 2


def test_failed_record_excluded_from_completed(tmp_path):
    fv = FileVaultStateStore(tmp_path / "run")
    fv.put("a", {"error": "boom"}, meta={"status": "failed"})
    assert fv.completed() == set()
    with pytest.raises(KeyError):
        fv.get("a")


# -- persistence + resume ---------------------------------------------------
def test_notes_written_to_disk(tmp_path):
    run = tmp_path / "run"
    fv = FileVaultStateStore(run, slipbox_form=True)  # opt into the authentic-SlipBox form
    fv.put("a", {"x": 1})
    fv.put("b", {"y": 2})
    record_notes = sorted(p for p in run.glob("*.md") if p.name != "_run.md")
    assert len(record_notes) == 2
    assert (run / "_run.md").exists()  # SlipBox-form writes a Folgezettel entry point
    # every record note is readable markdown carrying both authoritative blobs
    for n in record_notes:
        text = n.read_text()
        assert text.startswith("---")
        assert "\npayload: b64:" in text
        assert "\nmeta: b64:" in text


def test_default_form_is_slipbox_and_roundtrips(tmp_path):
    """The DEFAULT posture is the authentic slipbox form: a ``_run.md`` entry point plus
    per-record notes carrying SlipBox scaffolding (building_block/folgezettel/Related-Notes), and
    the durable log still round-trips arbitrary outputs exactly and resumes from a fresh store."""
    run = tmp_path / "run"
    fv = FileVaultStateStore(run)  # no slipbox_form -> default
    assert fv._slipbox_form is True
    fv.put("summarize", {"summary": "multi\nline\n---\ntext", "n": 3, "s": "0123"},
           meta={"producer": "summarize", "consumes": ["ingest:$.document"]})

    # The SlipBox form writes a Folgezettel entry point + a conformant record note.
    assert (run / "_run.md").exists()
    record_notes = [p for p in run.glob("*.md") if p.name != "_run.md"]
    assert len(record_notes) == 1
    text = record_notes[0].read_text()
    assert "\npayload: b64:" in text and "\nmeta: b64:" in text
    assert "building_block:" in text
    assert "folgezettel:" in text
    assert "## Related Notes" in text

    # Resume-by-reload from a fresh store reconstructs the frontier and the exact output.
    resumed = FileVaultStateStore(run)
    assert resumed.completed() == {"summarize"}
    assert resumed.get("summarize") == {"summary": "multi\nline\n---\ntext", "n": 3, "s": "0123"}


def test_lean_form_opt_out_omits_slipbox_scaffolding_and_roundtrips(tmp_path):
    """``slipbox_form=False`` is the lean opt-out: no ``_run.md`` entry point and none of the
    SlipBox scaffolding, yet the durable log still round-trips arbitrary outputs exactly."""
    run = tmp_path / "run"
    fv = FileVaultStateStore(run, slipbox_form=False)
    assert fv._slipbox_form is False
    fv.put("summarize", {"summary": "multi\nline\n---\ntext", "n": 3, "s": "0123"},
           meta={"producer": "summarize", "consumes": ["ingest:$.document"]})

    assert not (run / "_run.md").exists()
    record_notes = [p for p in run.glob("*.md")]
    assert len(record_notes) == 1
    text = record_notes[0].read_text()
    assert "\npayload: b64:" in text and "\nmeta: b64:" in text
    assert "building_block:" not in text
    assert "folgezettel:" not in text
    assert "## Related Notes" not in text

    resumed = FileVaultStateStore(run, slipbox_form=False)
    assert resumed.completed() == {"summarize"}
    assert resumed.get("summarize") == {"summary": "multi\nline\n---\ntext", "n": 3, "s": "0123"}


def test_resume_by_reload_from_fresh_store(tmp_path):
    run = tmp_path / "run"
    first = FileVaultStateStore(run)
    first.put("a", {"x": 1}, meta={"producer": "a"})
    first.put("b", {"y": 2}, meta={"producer": "b", "consumes": ["a:$.x"]})

    # A brand-new store over the same dir reconstructs the prior run's frontier and outputs.
    resumed = FileVaultStateStore(run)
    assert resumed.completed() == {"a", "b"}
    assert resumed.get("a") == {"x": 1}
    assert resumed.get("b") == {"y": 2}
    # consumes edges survived, so the run graph rebuilds
    graph = RunGraph.from_records(resumed.records())
    assert "a" in graph.upstream("b")


def test_from_config_scopes_by_session(tmp_path):
    fv = FileVaultStateStore.from_config(vault_path=tmp_path, session_id="concursus-" + "z" * 33)
    fv.put("a", {"x": 1})
    runs = list((tmp_path / "runs").iterdir())
    assert len(runs) == 1 and runs[0].is_dir()


def test_reput_after_resume_increments_attempt(tmp_path):
    run = tmp_path / "run"
    FileVaultStateStore(run).put("a", {"x": 1})
    resumed = FileVaultStateStore(run)
    resumed.put("a", {"x": 2})  # new output, next attempt
    recs = [r for r in resumed.records() if r.node == "a"]
    assert sorted(r.attempt for r in recs) == [1, 2]
    assert resumed.get("a") == {"x": 2}


# -- derived run DB ---------------------------------------------------------
def test_run_db_mirrors_records_and_edges(tmp_path):
    run = tmp_path / "run"
    fv = FileVaultStateStore(run)
    fv.put("ingest", {"document": "d"}, meta={"producer": "ingest"})
    fv.put("summarize", {"summary": "s"}, meta={"producer": "summarize", "consumes": ["ingest:$.document"]})

    db_path = build_run_db(run)
    con = sqlite3.connect(db_path)
    try:
        nodes = {r[0] for r in con.execute("SELECT node FROM records")}
        assert nodes == {"ingest", "summarize"}
        edges = con.execute("SELECT producer, consumer FROM consumes_edges").fetchall()
        assert ("ingest", "summarize") in edges
        proj = dict(con.execute("SELECT node, output_json FROM projection"))
        assert json.loads(proj["summarize"]) == {"summary": "s"}
        addrs = {r[0] for r in con.execute("SELECT address FROM run_addresses")}
        assert {"ingest", "summarize"} <= addrs
    finally:
        con.close()


def test_slipbox_form_frontmatter_is_conformant(tmp_path):
    fv = FileVaultStateStore.from_config(
        vault_path=tmp_path, session_id="concursus-" + "a" * 40, date="2026-07-10",
        slipbox_form=True,  # opt into the authentic-SlipBox form under test
    )
    fv.put("summarize", {"summary": "s"},
           meta={"producer": "summarize", "consumes": ["ingest:$.document"], "schema": "sum"})
    import glob
    run = glob.glob(str(tmp_path / "runs" / "*"))[0]
    text = open(glob.glob(run + "/summarize*a1.md")[0]).read()
    # SlipBox-required fields present with valid values (aligns with check_note_format.py)
    assert '\ntags:\n  - "resource"' in text            # valid P.A.R.A. first tag
    assert "\nkeywords:\n" in text and "\ntopics:\n" in text
    assert '\nstatus: "active"' in text                  # valid status (not raw 'validated')
    assert '\nbuilding_block: "empirical_observation"' in text  # DERIVED, not hardcoded
    assert '\nfolgezettel: "1' in text and "\nlineage:\n" in text
    assert '\naccess_control_group:\n  - "general"' in text
    assert "\n# Run State: summarize" in text            # typed H1
    assert "\n## Related Notes\n" in text                # not an orphan


def test_building_block_is_derived_from_record_kind(tmp_path):
    import glob
    fv = FileVaultStateStore.from_config(
        vault_path=tmp_path, session_id="concursus-" + "b" * 40, date="2026-07-10",
        slipbox_form=True,  # building_block is a SlipBox-form field
    )
    fv.put("ok", {"x": 1})                                   # validated -> empirical_observation
    fv.put("bad", {"e": "boom"}, meta={"status": "failed"})  # failed -> counter_argument
    fv.put("ok", {"x": 1})                                   # identical re-put -> dedup -> navigation
    run = glob.glob(str(tmp_path / "runs" / "*"))[0]
    bb = {}
    for f in glob.glob(run + "/*.md"):
        if f.endswith("_run.md"):
            continue
        t = open(f).read()
        node = json.loads(t.split('\nnode: ')[1].split('\n')[0])
        m = t.split('\nbuilding_block: ')[1].split('\n')[0]
        bb.setdefault(node, set()).add(json.loads(m))
    assert "empirical_observation" in bb["ok"]
    assert "navigation" in bb["ok"]          # the dedup re-put
    assert bb["bad"] == {"counter_argument"}


def test_run_db_parity_with_runindex(tmp_path):
    run = tmp_path / "run"
    fv = FileVaultStateStore(run)
    fv.put("a", {"x": 1})
    fv.put("a", {"x": 1})  # dedup, attempt 2
    fv.put("b", {"error": "x"}, meta={"status": "failed"})

    db_path = build_run_db(run)
    con = sqlite3.connect(db_path)
    try:
        failed = {r[0] for r in con.execute("SELECT node FROM records WHERE status='failed'")}
    finally:
        con.close()

    idx = RunIndex.from_records(load_records(run))
    assert failed == {r.node for r in idx.query(status="failed")}


# -- forward-only note-schema evolution -------------------------------------
def _rec(**kw):
    base = dict(
        node="n", output={"a": 1, "s": "x\ny---z", "z": "007"}, attempt=3,
        status="validated", consumes=["p:$.x"], content_hash="h", timestamp=7, address="n",
    )
    base.update(kw)
    return Record(**base)


def test_default_note_has_no_schema_version_stamp_and_roundtrips():
    """The stamp is OPT-IN — the DEFAULT bytes carry no ``schema_version`` key (byte-identical to
    the pre-migration format) and still round-trip exactly."""
    rec = _rec()
    for slipbox in (True, False):
        note = _record_to_note(rec, slipbox_form=slipbox)
        assert "schema_version" not in note
        back = _note_to_record(note)
        assert back.output == rec.output
        assert back.node == rec.node and back.attempt == rec.attempt
        assert back.consumes == rec.consumes


def test_absent_schema_version_reads_as_v1():
    """A note with no ``schema_version`` (every note ever written before the field existed) is
    treated as v1 — the baseline — so legacy notes stay readable."""
    note = _record_to_note(_rec())
    assert "schema_version" not in note
    assert _fv._note_schema_version({}) == 1
    assert _fv._note_schema_version({"schema_version": "not-an-int"}) == 1  # malformed → v1


def test_schema_version_stamp_is_opt_in_and_current_version_roundtrips_byte_exact():
    """Opting into the stamp emits ``schema_version: <current>`` in both forms; a current-version
    note is not migrated (registry empty at v1) so the round-trip stays exact."""
    rec = _rec()
    for slipbox in (True, False):
        stamped = _record_to_note(rec, slipbox_form=slipbox, stamp_schema_version=True)
        assert f"schema_version: {_fv._NOTE_SCHEMA_VERSION}" in stamped
        back = _note_to_record(stamped)
        assert back.output == rec.output and back.consumes == rec.consumes


def test_no_migrations_registered_while_v1_is_current():
    """v1 is the current schema, so the forward-only migration registry is empty (read is a no-op)."""
    assert _fv._NOTE_SCHEMA_VERSION == 1
    assert _fv._NOTE_MIGRATIONS == {}
    # An already-current record's meta is returned unchanged (no-op).
    meta = {"node": "n", "attempt": "1"}
    assert _fv._migrate_note_meta(meta, _fv._NOTE_SCHEMA_VERSION) is meta


def test_forward_only_migration_upgrades_older_note_on_read(monkeypatch):
    """A registered v1→v2 hook upgrades an OLDER (unstamped == v1) note to the current shape as it
    is parsed — the on-disk log is never rewritten, evolution is applied on read. Models a realistic
    backfill: v2 requires ``producer`` to default to ``node`` when a v1 note omitted it."""
    def backfill_producer(meta):
        m = dict(meta)
        m.setdefault("producer", m.get("node", ""))
        return m

    monkeypatch.setattr(_fv, "_NOTE_SCHEMA_VERSION", 2)
    monkeypatch.setattr(_fv, "_NOTE_MIGRATIONS", {1: backfill_producer})
    rec = _rec(node="orig", producer=None)  # a v1 note with no producer
    old_note = _record_to_note(rec)  # unstamped → read back as v1 → migrated to v2
    assert "schema_version" not in old_note
    back = _note_to_record(old_note)
    assert back.producer == "orig"  # the v1→v2 backfill hook ran on read
    assert back.output == rec.output  # payload blob untouched by the meta migration


def test_migration_stops_when_no_forward_hook(monkeypatch):
    """A gap in the migration chain stops forward-upgrade defensively rather than guessing."""
    monkeypatch.setattr(_fv, "_NOTE_SCHEMA_VERSION", 3)
    monkeypatch.setattr(_fv, "_NOTE_MIGRATIONS", {})  # no v1→v2 hook registered
    meta = {"node": "n"}
    assert _fv._migrate_note_meta(meta, 1) == {"node": "n"}  # unchanged, no crash


def test_current_version_note_not_migrated(monkeypatch):
    """A note stamped at the CURRENT version is never passed through the migration hooks."""
    calls = []
    monkeypatch.setattr(_fv, "_NOTE_SCHEMA_VERSION", 2)
    monkeypatch.setattr(
        _fv, "_NOTE_MIGRATIONS",
        {1: lambda meta: (calls.append(1), meta)[1]},
    )
    rec = _rec(node="orig")
    cur_note = _record_to_note(rec, stamp_schema_version=True)  # stamped schema_version: 2
    assert "schema_version: 2" in cur_note
    back = _note_to_record(cur_note)
    assert back.node == "orig"  # no migration applied
    assert calls == []


def test_schema_version_survives_store_resume(tmp_path):
    """A store whose notes carry the opt-in stamp still resumes exactly (the stamp is display-only
    to the record round-trip)."""
    run = tmp_path / "run"
    fv = FileVaultStateStore(run)
    fv.put("a", {"x": 1})
    # hand-stamp an equivalent note to prove a stamped note reloads identically
    note = _record_to_note(_rec(node="a", output={"x": 1}, attempt=1, consumes=[], address="a"),
                           stamp_schema_version=True)
    _fv.FileVaultStateStore._atomic_write(run / "a__a1.md", note)
    resumed = FileVaultStateStore(run)
    assert resumed.get("a") == {"x": 1}
    assert resumed.completed() == {"a"}


# -- opt-in run-dir heartbeat ownership lease -------------------------------
def test_heartbeat_lock_acquire_writes_host_pid_epoch(tmp_path):
    run = tmp_path / "run"
    lock = RunHeartbeatLock(run)
    lock.acquire()
    assert lock.path.exists()
    host, pid, epoch = lock.read()
    assert pid == os.getpid()
    assert host and isinstance(epoch, float)
    assert lock.owner_token == f"{host}:{pid}"


def test_heartbeat_lock_lives_at_run_dir_root_and_is_not_a_record(tmp_path):
    """The lease is a dotfile sidecar at the run-dir root, not a ``*.md`` note — the record loaders
    (which glob ``*.md``) never see it, so it never leaks into a resume/replay."""
    run = tmp_path / "run"
    fv = FileVaultStateStore(run)
    fv.put("a", {"x": 1})
    lock = RunHeartbeatLock(run)
    lock.acquire()
    assert lock.path.parent == run
    assert lock.path.name.startswith(".")
    assert lock.path not in list(run.glob("*.md"))
    # resume is unaffected by the presence of the lease
    resumed = FileVaultStateStore(run)
    assert resumed.completed() == {"a"}


def test_heartbeat_lock_live_peer_blocks_acquire(tmp_path):
    run = tmp_path / "run"
    owner = RunHeartbeatLock(run)
    owner.acquire()
    other = RunHeartbeatLock(run, pid=os.getpid() + 100000)  # a different, "live" owner
    assert other.is_held_by_other()
    with pytest.raises(RunLockHeldError):
        other.acquire()


def test_heartbeat_lock_expired_lease_is_reclaimable(tmp_path):
    run = tmp_path / "run"
    stale = RunHeartbeatLock(run, ttl_seconds=0.0, pid=os.getpid() + 100000)
    stale.acquire()  # stamps an immediately-expired lease
    time.sleep(0.01)
    reclaimer = RunHeartbeatLock(run, ttl_seconds=0.0)
    assert not reclaimer.is_held_by_other()  # expired → stale → reclaimable
    reclaimer.acquire()
    assert reclaimer.read()[1] == os.getpid()


def test_heartbeat_lock_dead_pid_is_reclaimable(tmp_path):
    run = tmp_path / "run"
    dead = RunHeartbeatLock(run, pid=2147480000)  # a pid essentially certain to be dead on this host
    dead._stamp()
    reclaimer = RunHeartbeatLock(run)
    assert not reclaimer.is_held_by_other()  # same-host dead pid → reclaimable
    reclaimer.acquire()
    assert reclaimer.read()[1] == os.getpid()


def test_heartbeat_lock_refresh_advances_epoch(tmp_path):
    run = tmp_path / "run"
    lock = RunHeartbeatLock(run)
    lock.acquire()
    e1 = lock.read()[2]
    time.sleep(0.01)
    lock.refresh()
    e2 = lock.read()[2]
    assert e2 > e1


def test_heartbeat_lock_refresh_rejects_foreign_lease(tmp_path):
    run = tmp_path / "run"
    RunHeartbeatLock(run, pid=os.getpid() + 100000).acquire()  # someone else owns it
    with pytest.raises(RunLockHeldError):
        RunHeartbeatLock(run).refresh()


def test_heartbeat_lock_release_removes_only_own_lease(tmp_path):
    run = tmp_path / "run"
    lock = RunHeartbeatLock(run)
    lock.acquire()
    lock.release()
    assert not lock.path.exists()
    # releasing a foreign lease is a no-op (does not delete someone else's lease)
    foreign = RunHeartbeatLock(run, pid=os.getpid() + 100000)
    foreign.acquire()
    RunHeartbeatLock(run).release()  # our (nonexistent) lease → no-op
    assert foreign.path.exists()


def test_heartbeat_lock_context_manager_acquires_and_releases(tmp_path):
    run = tmp_path / "run"
    with RunHeartbeatLock(run) as lock:
        assert lock.path.exists()
        held_path = lock.path
    assert not held_path.exists()


def test_heartbeat_lock_is_not_engaged_by_default_store_path(tmp_path):
    """The DEFAULT StateStore write/resume path never creates the heartbeat lease (opt-in only)."""
    run = tmp_path / "run"
    fv = FileVaultStateStore(run)
    fv.put("a", {"x": 1})
    FileVaultStateStore(run).completed()  # a resume
    assert not (run / _fv._HEARTBEAT_NAME).exists()


# -- OPT-IN append-only note version timeline ---------------------
def test_default_store_never_creates_a_versions_dir(tmp_path):
    """The DEFAULT store (``versioned=False``) is byte-identical to before: re-writing a note (the
    ``_run.md`` entry point on every put; a re-put) creates NO ``versions/`` timeline."""
    run = tmp_path / "run"
    fv = FileVaultStateStore(run)  # no versioned kwarg -> default OFF
    assert fv._versioned is False
    fv.put("a", {"x": 1})
    fv.put("a", {"x": 2})  # re-writes _run.md and adds a second record note
    assert not (run / _fv._VERSIONS_DIR_NAME).exists()


def test_versioned_store_appends_a_version_when_content_changes(tmp_path):
    """``versioned=True`` snapshots the ``_run.md`` entry point into the append-only timeline each
    time it is re-written with different content — newest version last, distinct content hashes."""
    run = tmp_path / "run"
    fv = FileVaultStateStore(run, versioned=True)
    fv.put("a", {"x": 1})
    fv.put("b", {"y": 2})  # _run.md re-written with a second row -> a new version

    versions = _fv.read_note_versions(run, "_run.md")
    assert [v["version"] for v in versions] == [1, 2]
    assert len({v["content_hash"] for v in versions}) == 2  # genuinely different content
    # the second version's content is exactly the current live _run.md bytes
    assert versions[-1]["content"] == (run / "_run.md").read_text()


def test_append_note_version_dedups_unchanged_content(tmp_path):
    """Appending the SAME content as the head is a no-op (returns ``None``) — only a changed note
    grows the timeline (append-only, content-hash de-duped)."""
    run = tmp_path / "run"
    run.mkdir()
    first = _fv.append_note_version(run, "note.md", "hello v1")
    assert first is not None
    again = _fv.append_note_version(run, "note.md", "hello v1")  # identical
    assert again is None
    changed = _fv.append_note_version(run, "note.md", "hello v2")  # different
    assert changed is not None
    assert [v["version"] for v in _fv.read_note_versions(run, "note.md")] == [1, 2]


def test_version_snapshot_roundtrips_yaml_hostile_content(tmp_path):
    """A version snapshot embeds the full note text in an authoritative ``b64:`` blob, so arbitrary
    content (frontmatter delimiters, code fences, links) round-trips byte-exact."""
    run = tmp_path / "run"
    run.mkdir()
    hostile = "---\nkey: value\n---\n# H1\n\n```markdown\nnested ``` fence\n```\n[x](../y.md)\n"
    _fv.append_note_version(run, "n.md", hostile)
    versions = _fv.read_note_versions(run, "n.md")
    assert len(versions) == 1
    assert versions[0]["content"] == hostile  # exact — the whole point


def test_version_notes_are_not_records_and_never_leak_into_resume(tmp_path):
    """A version snapshot is stamped a non-record: ``_note_to_record`` refuses it, and the record
    loaders (non-recursive ``*.md`` glob) never descend into ``versions/`` — so resume is unaffected."""
    run = tmp_path / "run"
    fv = FileVaultStateStore(run, versioned=True)
    fv.put("a", {"x": 1})
    fv.put("a", {"x": 2})

    vfiles = list((run / _fv._VERSIONS_DIR_NAME).rglob("v*.md"))
    assert vfiles  # the timeline exists
    for vf in vfiles:
        with pytest.raises(ValueError):
            _note_to_record(vf.read_text())

    # a fresh (also-versioned) store over the same dir resumes exactly, ignoring versions/
    resumed = FileVaultStateStore(run, versioned=True)
    assert resumed.completed() == {"a"}
    assert resumed.get("a") == {"x": 2}
    assert len([r for r in resumed.records() if r.node == "a"]) == 2  # only the 2 real records


def test_revert_note_writes_a_prior_version_forward(tmp_path):
    """``revert_note`` writes a prior version's content FORWARD as a new latest version (stamped
    ``reverted_from``) and restores the live note — never rewriting history: the version we reverted
    away from is still on the timeline."""
    run = tmp_path / "run"
    run.mkdir()
    _fv.append_note_version(run, "doc.md", "content-A")   # v1
    _fv.append_note_version(run, "doc.md", "content-B")   # v2
    (run / "doc.md").write_text("content-B")              # live head is B

    new_path = _fv.revert_note(run, "doc.md", 1)          # revert to A -> appended as v3
    assert new_path.endswith("v003.md")

    versions = _fv.read_note_versions(run, "doc.md")
    assert [v["version"] for v in versions] == [1, 2, 3]
    assert versions[-1]["reverted_from"] == 1             # typed forward-revert provenance
    assert versions[-1]["content"] == "content-A"         # the reverted-to content, forward
    assert versions[1]["reverted_from"] is None           # history intact: v2 (=B) untouched
    assert versions[1]["content"] == "content-B"
    assert (run / "doc.md").read_text() == "content-A"    # live note restored to A


def test_revert_note_rejects_unknown_version(tmp_path):
    """Reverting to a version not on the timeline raises rather than fabricating one."""
    run = tmp_path / "run"
    run.mkdir()
    _fv.append_note_version(run, "doc.md", "only-v1")
    with pytest.raises(ValueError):
        _fv.revert_note(run, "doc.md", 99)


def test_revert_note_can_skip_restoring_the_live_note(tmp_path):
    """``restore_live=False`` still appends the forward-revert version but leaves the live head as-is
    (a pure history annotation)."""
    run = tmp_path / "run"
    run.mkdir()
    _fv.append_note_version(run, "doc.md", "A")
    _fv.append_note_version(run, "doc.md", "B")
    (run / "doc.md").write_text("B")
    _fv.revert_note(run, "doc.md", 1, restore_live=False)
    assert (run / "doc.md").read_text() == "B"  # live head untouched
    assert _fv.read_note_versions(run, "doc.md")[-1]["content"] == "A"  # but timeline records A
