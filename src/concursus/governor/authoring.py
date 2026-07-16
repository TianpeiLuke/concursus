"""Net-new agent-manifest authoring (FZ 35e2b3 Phase 3b) — the deepest form of Create.

The Create capability (``registry.ensure_task``/``fork`` -> ``provision_agent`` ->
``CreateAgentRuntime``) can *provision* a manifest that already exists, but it cannot AUTHOR a role
that has never existed. This module closes that gap: given a capability/task label with no matching
manifest, it authors a valid :class:`~concursus.core.manifest.AgentManifest` (name, registry stub,
contract inputs + output schema, spec) so the role can then be provisioned and staffed.

Identity guard: authoring happens strictly BEFORE ``assemble`` (INV-2) and yields a plain manifest
value; it never touches ``Supervisor.run`` (INV-1) nor a running frozen plan (INV-3). A freshly
authored agent enters at a LOW create-time trust seed (``L0_SHADOW`` by default) — it must EARN
autonomy on the Trust Ladder before it can dispatch a side-effecting task.

The LLM is an INJECTED, OPTIONAL seam (``manifest_author_fn``). Default ``None`` -> a deterministic
TEMPLATE/skeleton manifest, so concursus authors a role LLM-free; a real author function UPGRADES
the skeleton with a synthesized prompt / SOPs / tools. This module imports no boto3 and no model.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping, Optional

from concursus.build.trust import TrustGrade
from concursus.core.manifest import AgentManifest

#: The injected manifest-author seam: ``(task, context) -> AgentManifest | dict``. Where an LLM
#: would synthesize a role's prompt/SOPs/tools/schema. NEVER imported or constructed here.
ManifestAuthorFn = Callable[[str, Mapping[str, Any]], Any]


class ManifestAuthorError(ValueError):
    """Raised when a task cannot be authored into a valid :class:`AgentManifest`."""


def _slug(text: str) -> str:
    """Lowercase ``[a-z0-9_]`` slug for a stable agent/role name."""
    out = []
    prev_us = False
    for ch in str(text).strip().lower():
        if ch.isalnum():
            out.append(ch)
            prev_us = False
        elif not prev_us:
            out.append("_")
            prev_us = True
    return "".join(out).strip("_") or "agent"


def _skeleton_manifest(
    task: str,
    *,
    inputs: Optional[Mapping[str, Any]] = None,
    trust_seed: TrustGrade = TrustGrade.L0_SHADOW,
) -> AgentManifest:
    """A DETERMINISTIC, offline skeleton manifest for a net-new role serving ``task``.

    Emits a valid, provisionable ``AgentManifest`` (container-hosted HTTP, a placeholder
    ``container_uri``, a minimal but non-empty output schema so ``check_alignment`` has a type
    gate, and the task as its declared capability) at a LOW trust seed. A real
    ``manifest_author_fn`` replaces this with a richer synthesized role.
    """
    name = _slug(task)
    data = {
        "name": name,
        "registry": {
            # A placeholder image: the role is authored + registered now; a real container_uri is
            # supplied at provision time (deploy is a separate, gated step).
            "container_uri": f"<to-provision>/{name}:latest",
            "protocol": "HTTP",
            "entry": f"agents.{name}:run",
            "capabilities": [task],
        },
        "contract": {
            "inputs": dict(inputs or {}),
            # A minimal-but-non-empty output schema — mandatory for the dependency-resolver gate.
            "outputs": {"result": {"type": "string", "required": True}},
        },
        "spec": {"depends_on": []},
        # A net-new agent starts UNPROVEN and must earn autonomy on the Trust Ladder.
        "trust_seed": trust_seed,
        "side_effecting": False,
    }
    return AgentManifest.from_dict(data)


def author_manifest(
    task: str,
    *,
    inputs: Optional[Mapping[str, Any]] = None,
    context: Optional[Mapping[str, Any]] = None,
    manifest_author_fn: Optional[ManifestAuthorFn] = None,
    trust_seed: TrustGrade = TrustGrade.L0_SHADOW,
) -> AgentManifest:
    """Author a valid :class:`AgentManifest` for a net-new role serving ``task`` (P3b.1).

    With no ``manifest_author_fn`` (the default), returns the deterministic
    :func:`_skeleton_manifest` — concursus authors a role LLM-free. When supplied, the injected
    function synthesizes the role (prompt/SOPs/tools/schema); its output (an ``AgentManifest`` or a
    ``from_dict`` mapping) is coerced and VALIDATED. The authored manifest always passes
    :meth:`AgentManifest.validate` and carries a low ``trust_seed`` (default ``L0_SHADOW``), so it
    must earn autonomy before it can dispatch a side-effecting task.

    Raises:
        ManifestAuthorError: if the author function returns something that is not a valid manifest.
    """
    if not task or not str(task).strip():
        raise ManifestAuthorError("author_manifest requires a non-empty task/capability label")

    if manifest_author_fn is None:
        return _skeleton_manifest(task, inputs=inputs, trust_seed=trust_seed).validate()

    produced = manifest_author_fn(task, dict(context or {}))
    if isinstance(produced, AgentManifest):
        manifest = produced
    elif isinstance(produced, Mapping):
        try:
            manifest = AgentManifest.from_dict(dict(produced))
        except Exception as exc:  # noqa: BLE001 - surface any malformed author output uniformly
            raise ManifestAuthorError(
                f"manifest_author_fn returned an invalid manifest mapping for {task!r}: {exc}"
            ) from exc
    else:
        raise ManifestAuthorError(
            "manifest_author_fn must return an AgentManifest or a from_dict mapping "
            f"(got {type(produced).__name__})"
        )
    try:
        manifest.validate()
    except Exception as exc:  # noqa: BLE001
        raise ManifestAuthorError(
            f"authored manifest for {task!r} failed validation: {exc}"
        ) from exc
    return manifest


__all__ = ["ManifestAuthorFn", "ManifestAuthorError", "author_manifest"]
