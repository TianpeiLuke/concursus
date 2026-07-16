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

from typing import TYPE_CHECKING, Any, Callable, Dict, Mapping, Optional

from concursus.build.trust import TrustGrade
from concursus.core.manifest import AgentManifest

if TYPE_CHECKING:  # pragma: no cover - hints only
    from concursus.core.dag import AgentDAG

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


#: The output field every staffed capability manifest emits (the skeleton's single output).
_CAP_OUTPUT = "result"


def staff_capability_dag(
    dag: "AgentDAG",
    *,
    bind_fn: "Optional[Callable[[str], Optional[str]]]" = None,
    manifest_author_fn: Optional[ManifestAuthorFn] = None,
    trust_seed: TrustGrade = TrustGrade.L0_SHADOW,
) -> Dict[str, AgentManifest]:
    """Turn an agent-agnostic CAPABILITY ``AgentDAG`` into an assemblable manifest set (FZ 35e2b3b
    A1–A3): the STAFFING step at the compiler front that un-collapses *binding* from *authoring*.

    A capability DAG (from ``plan_from_goal(..., decompose=True)``) has agent-agnostic task nodes and
    edges but NO manifests and NO ``depends_on`` wiring, so it cannot be assembled directly
    (``assemble`` requires a manifest per node + derives wiring from ``depends_on``). This synthesizes,
    for each node:

    - a MANIFEST keyed by the node id — via ``bind_fn(node)`` if it returns a standing agent name to
      bind (the SCHEDULER's job, A2/A3), else an authored skeleton (:func:`author_manifest`, the
      CREATE arrow for an UNMATCHED capability). Either way the manifest is keyed by the node id so
      the frozen ``plan.order`` stays the capability topology (the auditable artifact, [35e2b1a1a1a]).
    - its DATA-WIRING from the DAG edges: one input per upstream producer (named after the producer
      node) fed by ``<producer>.result``, plus the matching ``depends_on`` edge — so the staffed set
      type-aligns and ``assemble`` freezes it exactly like a hand-authored one.

    The result is a ``{node: AgentManifest}`` map ready for ``OrchestrationAssembler.assemble(dag,
    …)``. Pure + offline (INV-2): binds/authors VALUES, never dispatches, never mutates a running
    plan. ``bind_fn`` default ``None`` authors every node (the zero-bench cold-start path); a real
    binder (e.g. wrapping ``scheduler.decide_ranked``) returns an agent name to reuse a standing one.

    Note: this produces the assemblable ARTIFACT; wiring it as the governor loop's default authoring
    path (retiring ``_reconcile_dag_with_manifests``) is a separate, larger loop change — this
    function is the reusable core that change would call.
    """
    manifests: Dict[str, AgentManifest] = {}
    for node in dag.nodes:
        # One input per upstream producer, each receiving that producer's `result` output.
        producers = list(dag.get_dependencies(node))
        inputs = {p: {"type": "string"} for p in producers}
        bound = bind_fn(node) if bind_fn is not None else None
        if bound:
            # Bind to a standing agent, but KEY the manifest by the node id + carry the synthesized
            # wiring so the capability topology + edges survive into assemble. The bound agent name is
            # recorded in the registry stub for provenance.
            data = {
                "name": node,
                "registry": {
                    "container_uri": f"<bound>/{_slug(bound)}:latest",
                    "protocol": "HTTP",
                    "entry": f"agents.{_slug(bound)}:run",
                    "capabilities": [node],
                    "bound_agent": bound,
                },
                "contract": {
                    "inputs": inputs,
                    "outputs": {_CAP_OUTPUT: {"type": "string", "required": True}},
                },
                "spec": {"depends_on": [{"from": f"{p}.{_CAP_OUTPUT}", "to": p} for p in producers]},
                "trust_seed": trust_seed,
                "side_effecting": False,
            }
            manifests[node] = AgentManifest.from_dict(data).validate()
        else:
            # UNMATCHED capability -> author a skeleton, then graft the wiring + pin the name to the
            # node id. author_manifest slugs the name (collapsing "__"), but assemble REQUIRES
            # manifest.name == node id, so force it back to the exact capability node id.
            m = author_manifest(
                node, inputs=inputs, manifest_author_fn=manifest_author_fn, trust_seed=trust_seed
            )
            m.name = node
            m.spec["depends_on"] = [{"from": f"{p}.{_CAP_OUTPUT}", "to": p} for p in producers]
            manifests[node] = m.validate()
    return manifests


__all__ = [
    "ManifestAuthorFn",
    "ManifestAuthorError",
    "author_manifest",
    "staff_capability_dag",
]
