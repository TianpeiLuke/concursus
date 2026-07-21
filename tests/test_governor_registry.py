"""Tests for the versioned agent registry (S9-G7).

The registry is a READ-ONLY process-table view over the shipped DeployLedger:
it derives standing versions from ledger rows, matches a task to an agent's
*current* version, and spawns/forks on demand by delegating to the shipped
provision_agent actuator (never a new compiler path, never a ledger write of
its own).
"""

from __future__ import annotations

import types

from concursus import AgentManifest, DeployLedger
from concursus.governor.registry import AgentRegistry, AgentVersion


def _manifest(name: str, capabilities=None):
    reg = {"container_uri": "img", "protocol": "HTTP"}
    if capabilities is not None:
        reg["capabilities"] = list(capabilities)
    return AgentManifest.from_dict(
        {
            "name": name,
            "registry": reg,
            "contract": {"inputs": {}, "outputs": {"doc": {"type": "string", "required": True}}},
        }
    )


def _ledger(tmp_path):
    return DeployLedger(tmp_path / "deploy_ledger.json")


def test_registry_matches_task_to_current_version(tmp_path):
    """Register two versions of an agent (two fingerprints) and confirm a task
    matches the higher/current version, not the older one."""
    ledger = _ledger(tmp_path)
    # v1 then v2 of the same named agent, distinguished by fingerprint.
    ledger.record(name="triage", fingerprint="fp-v1", arn="arn:v1", deployed_at="2026-07-01")
    ledger.record(name="triage", fingerprint="fp-v2", arn="arn:v2", deployed_at="2026-07-02")

    registry = AgentRegistry(ledger)
    registry.register_agent(_manifest("triage", capabilities={"classify-abuse"}))

    versions = registry.versions("triage")
    assert [v.version for v in versions] == [1, 2]
    assert [v.fingerprint for v in versions] == ["fp-v1", "fp-v2"]

    match = registry.match_task("classify-abuse")
    assert match is not None
    # Current version is v2 (newest fingerprint), and it is what a task resolves to.
    assert match.version == 2
    assert match.fingerprint == "fp-v2"
    assert match.arn == "arn:v2"

    # Process table exposes exactly the current version per name.
    table = registry.process_table()
    assert set(table) == {"triage"}
    assert table["triage"].fingerprint == "fp-v2"

    # An unknown task matches nothing.
    assert registry.match_task("no-such-task") is None


def test_registry_matches_agent_name_by_default(tmp_path):
    """Without explicit capabilities, an agent serves a task named after itself."""
    ledger = _ledger(tmp_path)
    ledger.record(name="ingest", fingerprint="fp-1", arn="arn:ingest", deployed_at="2026-07-01")
    registry = AgentRegistry(ledger)
    registry.register_agent(_manifest("ingest"))  # default caps => {"ingest"}
    match = registry.match_task("ingest")
    assert match is not None and match.name == "ingest"


def test_registry_read_only_over_ledger(tmp_path):
    """Registry reads must NEVER mutate the ledger rows."""
    ledger = _ledger(tmp_path)
    ledger.record(name="a", fingerprint="fp-a1", arn="arn:a1", deployed_at="2026-07-01")
    ledger.record(name="a", fingerprint="fp-a2", arn="arn:a2", deployed_at="2026-07-02")
    ledger.record(name="b", fingerprint="fp-b1", arn="arn:b1", deployed_at="2026-07-03")

    before = [r.to_dict() for r in ledger.rows()]

    registry = AgentRegistry(ledger)
    registry.register_agent(_manifest("a", capabilities={"task-a"}))
    registry.register_agent(_manifest("b", capabilities={"task-b"}))
    # Exercise every read path.
    registry.versions("a")
    registry.current("a")
    registry.names()
    registry.process_table()
    registry.match_task("task-a")
    registry.match_all("task-b")

    after = [r.to_dict() for r in ledger.rows()]
    assert before == after  # not a single row added, removed, or edited
    assert len(after) == 3


def test_registry_spawn_routes_through_provision_agent(tmp_path):
    """An unmatched task spawns on demand via provision_agent (the shipped
    actuator), which owns the ledger append; the registry re-reads it."""
    ledger = _ledger(tmp_path)
    registry = AgentRegistry(ledger)

    entry = types.SimpleNamespace(name="scorer", fingerprint="fp-scorer")
    calls = {}

    def fake_provision(entry, *, clients, ledger, manifest=None, **kw):
        # Emulate provision_agent's contract: it appends to the ledger.
        calls["called"] = True
        ledger.record(
            name=entry.name,
            fingerprint=entry.fingerprint,
            arn="arn:scorer",
            deployed_at="2026-07-05",
            action="created",
        )
        return {"node": entry.name, "arn": "arn:scorer", "action": "created"}

    # No standing agent yet.
    assert registry.match_task("score-risk") is None

    version = registry.ensure_task(
        "score-risk",
        entry=entry,
        clients=object(),
        capabilities={"score-risk"},
        provision_fn=fake_provision,
    )
    assert calls.get("called") is True
    assert isinstance(version, AgentVersion)
    assert version.name == "scorer"
    assert version.arn == "arn:scorer"
    assert version.serves("score-risk")

    # Now it is standing — a second ensure_task is a no-op (no re-provision).
    calls.clear()
    again = registry.ensure_task(
        "score-risk", entry=entry, clients=object(), provision_fn=fake_provision
    )
    assert calls.get("called") is None
    assert again.fingerprint == version.fingerprint


def test_registry_fork_adds_new_version(tmp_path):
    """Forking stands up a new version (new fingerprint) of an existing agent
    via provision_agent; the registry then sees the new current version."""
    ledger = _ledger(tmp_path)
    ledger.record(name="planner", fingerprint="fp-1", arn="arn:1", deployed_at="2026-07-01")
    registry = AgentRegistry(ledger)
    registry.register_agent(_manifest("planner", capabilities={"plan"}))

    entry = types.SimpleNamespace(name="planner", fingerprint="fp-2")

    def fake_provision(entry, *, clients, ledger, manifest=None, **kw):
        ledger.record(
            name=entry.name,
            fingerprint=entry.fingerprint,
            arn="arn:2",
            deployed_at="2026-07-06",
            action="updated",
        )
        return {"node": entry.name, "arn": "arn:2", "action": "updated"}

    forked = registry.fork(
        "planner", entry=entry, clients=object(), provision_fn=fake_provision
    )
    assert forked.version == 2
    assert forked.fingerprint == "fp-2"
    assert forked.arn == "arn:2"
    assert [v.version for v in registry.versions("planner")] == [1, 2]
