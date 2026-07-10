"""Tests for the persisted deploy ledger (concursus.ledger) — pure stdlib, no AWS."""

import json

from concursus.build.ledger import DeployLedger, DeployRow


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
