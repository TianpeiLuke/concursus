"""Tests for the persisted deploy ledger (concursus.ledger) — pure stdlib, no AWS."""

import json

from concursus.build.ledger import (
    REJECT_ACTUATOR_ERROR,
    REJECT_INVALID,
    REJECT_TIMEOUT,
    REJECT_UNSUPPORTED,
    REJECTION_CODES,
    DeployLedger,
    DeployRejection,
    DeployRow,
    Reconciliation,
    deploy_identity,
)


def test_record_round_trips_across_instances(tmp_path):
    path = tmp_path / "deploy.json"
    led = DeployLedger(path)
    led.record(
        name="summarize",
        fingerprint="fp-abc",
        deployed_at="2026-07-10T00:00:00Z",
        arn="arn:aws:bedrock-agentcore:us-east-1:111:runtime/summarize-xyz",
        image_uri="111.dkr.ecr.us-east-1.amazonaws.com/summarize:latest",
        role_arn="arn:aws:iam::111:role/concursus-summarize-exec",
        action="created",
    )
    # A fresh instance over the same file sees the persisted row.
    reloaded = DeployLedger(path)
    row = reloaded.lookup("summarize", "fp-abc")
    assert row is not None
    assert row.arn.endswith("summarize-xyz")
    assert row.image_uri.endswith("summarize:latest")
    assert row.deployed_at == "2026-07-10T00:00:00Z"
    assert row.action == "created"


def test_matching_name_fingerprint_is_reused_across_instances(tmp_path):
    path = tmp_path / "deploy.json"
    DeployLedger(path).record(
        name="a", fingerprint="fp1", deployed_at=1, arn="arn-1", action="created"
    )
    other = DeployLedger(path)
    assert other.has("a", "fp1") is True
    assert other.lookup("a", "fp1").arn == "arn-1"
    # A different fingerprint for the same name is NOT a match (content changed).
    assert other.has("a", "fp2") is False
    # A different name is NOT a match.
    assert other.has("b", "fp1") is False


def test_write_is_atomic_no_tmp_left_behind(tmp_path):
    path = tmp_path / "deploy.json"
    led = DeployLedger(path)
    led.record(name="a", fingerprint="fp1", deployed_at=1)
    assert path.exists()
    assert not (tmp_path / "deploy.json.tmp").exists()  # temp file was os.replace'd away
    # The persisted payload is valid JSON with a version + rows.
    data = json.loads(path.read_text())
    assert data["version"] >= 1
    assert data["rows"][0]["name"] == "a"


def test_append_only_retains_old_rows_newest_wins(tmp_path):
    path = tmp_path / "deploy.json"
    led = DeployLedger(path)
    led.record(name="a", fingerprint="fp1", deployed_at=1, arn="arn-old", action="created")
    # Re-deploying the SAME content appends a new row rather than overwriting (audit history).
    led.record(name="a", fingerprint="fp1", deployed_at=2, arn="arn-new", action="updated")
    rows = led.rows()
    assert [r.arn for r in rows] == ["arn-old", "arn-new"]  # both retained, oldest first
    assert led.lookup("a", "fp1").arn == "arn-new"  # newest matching row wins


def test_missing_file_is_empty_ledger(tmp_path):
    led = DeployLedger(tmp_path / "does-not-exist.json")
    assert led.rows() == []
    assert led.lookup("a", "fp1") is None


def test_corrupt_file_is_treated_as_empty(tmp_path):
    path = tmp_path / "deploy.json"
    path.write_text("{ this is not valid json", encoding="utf-8")
    led = DeployLedger(path)
    assert led.rows() == []  # unreadable ledger is disposable — reload as empty, do not crash
    # And a subsequent record still works (overwrites the garbage with a valid ledger).
    led.record(name="a", fingerprint="fp1", deployed_at=1)
    assert DeployLedger(path).has("a", "fp1")


def test_concurrent_instances_both_persist(tmp_path):
    # Two instances over the same file: the second folds in the first's write before appending.
    path = tmp_path / "deploy.json"
    a = DeployLedger(path)
    b = DeployLedger(path)
    a.record(name="a", fingerprint="fp1", deployed_at=1)
    b.record(name="b", fingerprint="fp2", deployed_at=2)  # b reloads, keeps a's row, adds its own
    final = DeployLedger(path)
    names = {r.name for r in final.rows()}
    assert names == {"a", "b"}


def test_deploy_row_to_from_dict_round_trip():
    row = DeployRow(name="a", fingerprint="fp1", arn="arn-1", deployed_at=1, action="created")
    assert DeployRow.from_dict(row.to_dict()) == row


# -- canonical reuse-identity (single source) -------------------------------
def test_deploy_identity_is_the_reuse_key_and_stable():
    # The reuse key is content-only (name + fingerprint), stable, and stringifies its inputs.
    assert deploy_identity("a", "fp1") == ("a", "fp1")
    assert deploy_identity("a", "fp1") == deploy_identity("a", "fp1")
    assert deploy_identity("a", "fp1") != deploy_identity("a", "fp2")
    assert deploy_identity("a", "fp1") != deploy_identity("b", "fp1")


def test_lookup_uses_the_canonical_identity_key(tmp_path):
    # A row confirmed via record() must be found through deploy_identity — the same key reconcile
    # uses — so the two queries can never drift on how identity is computed.
    path = tmp_path / "deploy.json"
    led = DeployLedger(path)
    led.record(name="summarize", fingerprint="fp-abc", deployed_at=1, arn="arn-1")
    row = led.lookup("summarize", "fp-abc")
    assert row is not None
    assert deploy_identity(row.name, row.fingerprint) == deploy_identity("summarize", "fp-abc")


# -- typed rejections -------------------------------------------------------
def test_record_rejection_round_trips_across_instances(tmp_path):
    path = tmp_path / "deploy.json"
    led = DeployLedger(path)
    led.record_rejection(
        node="charge",
        code=REJECT_UNSUPPORTED,
        confirmed_at="2026-07-21T00:00:00Z",
        reason="protocol A2A not supported by the actuator",
    )
    # A fresh instance over the same file sees the persisted rejection.
    reloaded = DeployLedger(path)
    entry = reloaded.why_rejected("charge")
    assert entry is not None
    assert entry.code == REJECT_UNSUPPORTED
    assert entry.reason == "protocol A2A not supported by the actuator"
    assert entry.confirmed_at == "2026-07-21T00:00:00Z"


def test_record_rejection_coerces_unknown_code_to_actuator_error(tmp_path):
    led = DeployLedger(tmp_path / "deploy.json")
    entry = led.record_rejection(node="n", code="totally-bogus", confirmed_at=1)
    assert entry.code == REJECT_ACTUATOR_ERROR
    assert entry.code in REJECTION_CODES
    # And it persists coerced.
    assert DeployLedger(tmp_path / "deploy.json").why_rejected("n").code == REJECT_ACTUATOR_ERROR


def test_rejections_are_append_only_newest_wins(tmp_path):
    path = tmp_path / "deploy.json"
    led = DeployLedger(path)
    led.record_rejection(node="n", code=REJECT_TIMEOUT, confirmed_at=1, reason="first")
    led.record_rejection(node="n", code=REJECT_INVALID, confirmed_at=2, reason="second")
    entries = led.rejections()
    assert [e.reason for e in entries] == ["first", "second"]  # both retained, oldest first
    assert led.why_rejected("n").code == REJECT_INVALID  # newest wins
    assert led.why_rejected("never-rejected") is None


def test_rejection_and_confirmation_coexist_and_persist(tmp_path):
    # Confirmations and rejections are two append-only logs in the same file, independent.
    path = tmp_path / "deploy.json"
    led = DeployLedger(path)
    led.record(name="a", fingerprint="fp1", deployed_at=1, arn="arn-a")
    led.record_rejection(node="b", code=REJECT_TIMEOUT, confirmed_at=2, reason="slow")
    final = DeployLedger(path)
    assert final.has("a", "fp1") is True
    assert final.why_rejected("b").code == REJECT_TIMEOUT
    data = json.loads(path.read_text())
    assert data["rows"][0]["name"] == "a"
    assert data["rejections"][0]["node"] == "b"


def test_default_path_omits_rejections_key_byte_for_byte(tmp_path):
    # A ledger that never records a rejection must be byte-for-byte the pre-rejection format:
    # no "rejections" key is written at all.
    path = tmp_path / "deploy.json"
    led = DeployLedger(path)
    led.record(name="a", fingerprint="fp1", deployed_at=1)
    data = json.loads(path.read_text())
    assert "rejections" not in data
    assert set(data) == {"version", "rows"}


def test_deploy_rejection_to_from_dict_round_trip():
    entry = DeployRejection(node="n", code=REJECT_INVALID, reason="bad schema", confirmed_at=7)
    assert DeployRejection.from_dict(entry.to_dict()) == entry


# -- desired-vs-confirmed reconcile -----------------------------------------
def test_reconcile_reports_confirmed_and_diverged_with_reasons(tmp_path):
    path = tmp_path / "deploy.json"
    led = DeployLedger(path)
    # 'ok' was stood up at the desired fingerprint; 'stale' at a DIFFERENT fingerprint (diverged).
    led.record(name="ok", fingerprint="fp-ok", deployed_at=1, arn="arn-ok")
    led.record(name="stale", fingerprint="fp-old", deployed_at=2, arn="arn-stale")
    # 'rejected' was never stood up but has a typed rejection explaining why.
    led.record_rejection(node="rejected", code=REJECT_UNSUPPORTED, confirmed_at=3, reason="nope")
    desired = {
        "ok": "fp-ok",
        "stale": "fp-new",  # desired changed → the recorded fp-old row does NOT confirm it
        "rejected": "fp-r",
        "missing": "fp-m",  # never stood up, never rejected
    }
    rec = led.reconcile(desired)
    assert isinstance(rec, Reconciliation)
    assert rec.confirmed == {"ok": "fp-ok"}
    assert set(rec.diverged) == {"stale", "rejected", "missing"}
    assert rec.diverged["rejected"].code == REJECT_UNSUPPORTED
    assert rec.diverged["rejected"].reason == "nope"
    assert rec.diverged["stale"] is None  # diverged content, but no recorded rejection reason
    assert rec.diverged["missing"] is None
    assert rec.all_confirmed is False


def test_reconcile_all_confirmed_when_every_node_matches(tmp_path):
    path = tmp_path / "deploy.json"
    led = DeployLedger(path)
    led.record(name="a", fingerprint="fp1", deployed_at=1)
    led.record(name="b", fingerprint="fp2", deployed_at=2)
    rec = led.reconcile({"a": "fp1", "b": "fp2"})
    assert rec.confirmed == {"a": "fp1", "b": "fp2"}
    assert rec.diverged == {}
    assert rec.all_confirmed is True


def test_reconcile_uses_newest_confirmation_and_rejection(tmp_path):
    # After a node is rejected then later stood up at the desired fp, reconcile confirms it (a
    # matching row exists) and does NOT surface the stale rejection.
    path = tmp_path / "deploy.json"
    led = DeployLedger(path)
    led.record_rejection(node="a", code=REJECT_TIMEOUT, confirmed_at=1, reason="was slow")
    led.record(name="a", fingerprint="fp1", deployed_at=2, arn="arn-a")
    rec = led.reconcile({"a": "fp1"})
    assert rec.confirmed == {"a": "fp1"}
    assert "a" not in rec.diverged


def test_reconcile_is_read_only_over_the_ledger(tmp_path):
    # Reconcile must not append or mutate — the on-disk payload is unchanged after a query.
    path = tmp_path / "deploy.json"
    led = DeployLedger(path)
    led.record(name="a", fingerprint="fp1", deployed_at=1)
    before = path.read_text()
    led.reconcile({"a": "fp1", "gone": "fp9"})
    assert path.read_text() == before  # pure projection, no write


def test_corrupt_file_ignores_rejections_too(tmp_path):
    path = tmp_path / "deploy.json"
    path.write_text("{ not valid json", encoding="utf-8")
    led = DeployLedger(path)
    assert led.rejections() == []
    assert led.why_rejected("x") is None
    # And a subsequent rejection still persists over the garbage.
    led.record_rejection(node="x", code=REJECT_INVALID, confirmed_at=1)
    assert DeployLedger(path).why_rejected("x").code == REJECT_INVALID
