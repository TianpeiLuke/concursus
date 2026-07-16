"""Tests for net-new agent-manifest authoring (FZ 35e2b3 Phase 3b).

The deterministic skeleton authors a valid, low-trust manifest LLM-free; an injected
manifest_author_fn upgrades it; malformed author output is rejected.
"""

import pytest

from concursus.build.trust import TrustGrade
from concursus.core.manifest import AgentManifest, ManifestError
from concursus.governor.authoring import ManifestAuthorError, author_manifest


def test_skeleton_manifest_is_valid_and_low_trust():
    """Default (no author fn) -> a valid, provisionable skeleton at a LOW trust seed."""
    m = author_manifest("triage_alarm_burst")
    assert isinstance(m, AgentManifest)
    m.validate()  # must not raise
    assert m.trust_seed == TrustGrade.L0_SHADOW      # unproven -> must earn autonomy
    assert m.output_schema                            # mandatory type gate is present
    assert "triage_alarm_burst" in m.registry.get("capabilities", [])
    assert m.protocol == "HTTP"


def test_skeleton_not_side_effecting_by_default():
    """A freshly-authored role is non-side-effecting until declared otherwise (safe default)."""
    assert author_manifest("summarize").side_effecting is False


def test_injected_author_upgrades_the_skeleton():
    """An injected manifest_author_fn (the LLM seam) replaces the skeleton, and is validated."""
    def author(task, ctx):
        return {
            "name": "custom_role",
            "registry": {"container_uri": "img", "protocol": "MCP", "entry": "a.b:run"},
            "contract": {"inputs": {}, "outputs": {"answer": {"type": "string", "required": True}}},
            "trust_seed": "L1_CANARY",
        }
    m = author_manifest("do_x", manifest_author_fn=author)
    assert m.name == "custom_role" and m.protocol == "MCP"
    assert m.trust_seed == TrustGrade.L1_CANARY


def test_author_accepts_a_manifest_object():
    """An author fn may return an AgentManifest directly."""
    obj = AgentManifest.from_dict({
        "name": "direct",
        "registry": {"container_uri": "img", "protocol": "HTTP", "entry": "a.b:run"},
        "contract": {"inputs": {}, "outputs": {"r": {"type": "string", "required": True}}},
    })
    m = author_manifest("t", manifest_author_fn=lambda task, ctx: obj)
    assert m is obj


def test_bad_author_output_raises_manifest_author_error():
    """A non-manifest / non-mapping author output is rejected cleanly."""
    with pytest.raises(ManifestAuthorError):
        author_manifest("t", manifest_author_fn=lambda task, ctx: 42)


def test_author_validates_invalid_manifest():
    """An author fn returning a schema-less manifest fails validation (the mandatory output gate)."""
    def bad(task, ctx):
        # missing contract.outputs -> AgentManifest.validate must reject
        return {"name": "noout", "registry": {"container_uri": "img", "protocol": "HTTP",
                                               "entry": "a.b:run"},
                "contract": {"inputs": {}, "outputs": {}}}
    with pytest.raises(ManifestAuthorError):
        author_manifest("t", manifest_author_fn=bad)


def test_empty_task_rejected():
    with pytest.raises(ManifestAuthorError):
        author_manifest("   ")
