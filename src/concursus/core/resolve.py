"""Dependency resolution — wire declared ``depends_on`` edges into typed data references.

Each manifest may declare ``depends_on`` edges (``{"from": "producer.field.path", "to":
"input"}``). This layer (1) extracts a value from a producer's output JSON via a minimal
JSONPath, (2) compiles those edges into :class:`AgentRef` wiring per node, and (3) type-gates
the whole graph — every edge's producer, referenced output field, consumer input, and DAG
edge must line up, or :class:`AlignmentError` is raised. Pure core: no AWS, no third-party
dependencies.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Set

if TYPE_CHECKING:  # pragma: no cover - hints only, no runtime coupling
    from .dag import AgentDAG
    from .manifest import AgentManifest


class AlignmentError(ValueError):
    """Raised when a ``depends_on`` edge fails to type-align against the DAG/manifests."""


@dataclass(frozen=True)
class AgentRef:
    """A resolved wire: a producer output value routed into a consumer input.

    Attributes:
        producer: The upstream node id whose output supplies the value.
        path: A minimal JSONPath into the producer's output JSON (e.g. ``$.summary``).
        input_name: The consumer input field this value feeds.
    """

    producer: str
    path: str
    input_name: str


# -- JSONPath extraction ----------------------------------------------------
_TOKEN = re.compile(r"([^.\[\]]+)|\[(\d+)\]")


def _tokenize(path: str) -> List[Any]:
    """Split a normalized path into a list of string keys and int list-indices."""
    tokens: List[Any] = []
    for key, index in _TOKEN.findall(path):
        tokens.append(int(index) if index else key)
    return tokens


def extract(obj: Any, path: str) -> Any:
    """Read a value out of ``obj`` at ``path`` (a minimal JSONPath).

    Supports a leading ``$``/``$.``, dotted access (``a.b.c``) and list indices
    (``a.b[0]``). ``$`` (or the empty path) returns ``obj`` unchanged. Raises ``KeyError`` /
    ``IndexError`` when a segment is absent — the resolver relies on that to signal a broken
    wire at run time.
    """
    normalized = path.strip()
    if normalized.startswith("$"):
        normalized = normalized[1:]
    cursor = obj
    for token in _tokenize(normalized):
        cursor = cursor[token]
    return cursor


# -- edge compilation -------------------------------------------------------
def _split_from(spec: str) -> tuple:
    """Split a ``from`` spec on the FIRST dot into ``(producer, "$."+rest)``."""
    producer, _, rest = spec.partition(".")
    return producer, ("$." + rest if rest else "$")


def resolve_edges(
    dag: "AgentDAG", manifests: Dict[str, "AgentManifest"]
) -> Dict[str, List[AgentRef]]:
    """Compile every node's ``depends_on`` edges into :class:`AgentRef` wiring.

    Returns ``{node_id: [AgentRef, ...]}`` for every node in ``dag`` (empty list when a node
    declares no dependencies). Each edge ``{"from": "producer.field.path", "to": "input"}`` is
    split on its first dot into a producer id and a ``$.``-prefixed path.
    """
    wiring: Dict[str, List[AgentRef]] = {}
    for node in dag.nodes:
        refs: List[AgentRef] = []
        manifest = manifests.get(node)
        if manifest is not None:
            for edge in manifest.depends_on:
                producer, path = _split_from(edge["from"])
                refs.append(AgentRef(producer=producer, path=path, input_name=edge["to"]))
        wiring[node] = refs
    return wiring


def _output_properties(schema: Dict[str, Any]) -> Set[str]:
    """Declared property names of a JSON-Schema-ish output schema.

    Supports both a nested ``{"properties": {...}}`` schema and a flat ``{prop: {...}}`` map.
    """
    if not isinstance(schema, dict):
        return set()
    props = schema.get("properties")
    if isinstance(props, dict):
        return set(props.keys())
    return set(schema.keys())


def _top_field(path_rest: str) -> str:
    """The top-level output field named by a ``from`` spec's remainder (first path segment)."""
    return re.split(r"[.\[]", path_rest, maxsplit=1)[0]


def _field_type(schema: Dict[str, Any], field: str) -> Any:
    """The declared JSON-Schema ``type`` of ``field`` in a (nested or flat) schema, or ``None``.

    Returns ``None`` when the field carries no ``type`` annotation (or the entry is not a mapping)
    — an UNKNOWN type, which the strict gate treats as "cannot prove incompatible" and passes.
    """
    if not isinstance(schema, dict):
        return None
    props = schema.get("properties")
    table = props if isinstance(props, dict) else schema
    entry = table.get(field)
    if isinstance(entry, dict):
        return entry.get("type")
    return None


def _types_compatible(producer_type: Any, consumer_type: Any) -> bool:
    """True iff a producer output ``type`` may satisfy a consumer input ``type``.

    CONSERVATIVE: only a concrete, mutually-declared MISMATCH is incompatible. An unknown/absent
    type on either side (``None``) passes — the gate can only *prove* a violation, never guess one,
    so a manifest that omits type annotations is never newly rejected. Both a scalar (``"string"``)
    and a list-of-types (``["string", "null"]``, JSON-Schema union) are supported; a producer type
    is compatible if it overlaps the consumer's accepted set.
    """
    if producer_type is None or consumer_type is None:
        return True
    prod = set(producer_type) if isinstance(producer_type, (list, tuple)) else {producer_type}
    cons = set(consumer_type) if isinstance(consumer_type, (list, tuple)) else {consumer_type}
    return bool(prod & cons)


def check_alignment(
    dag: "AgentDAG",
    manifests: Dict[str, "AgentManifest"],
    *,
    strict_types: bool = False,
) -> None:
    """Type-gate every ``depends_on`` edge; raise :class:`AlignmentError` on any violation.

    For each edge on each manifest: (a) the producer must be a known manifest; (b) the
    referenced top-level output field must be a declared property of the producer's
    ``output_schema``; (c) the ``to`` input must be a declared input of the consumer; and
    (d) the DAG must carry the edge ``producer -> consumer``.

    ``strict_types`` (default ``False``) adds a DEEPER gate (FZ 35e2b3b B2): the producer output
    field's declared ``type`` must be COMPATIBLE with the consumer input's declared ``type`` — a
    concrete mismatch (e.g. producer ``"string"`` into consumer ``"integer"``) raises. It is
    conservative: an unknown/absent type on either side passes (see :func:`_types_compatible`), so
    turning it on never rejects a manifest that simply omits type annotations. Default off keeps
    the name-level gate byte-for-byte unchanged.
    """
    for node, manifest in manifests.items():
        consumer_inputs = manifest.inputs
        for edge in manifest.depends_on:
            producer, _, rest = str(edge["from"]).partition(".")
            input_name = edge["to"]

            producer_manifest = manifests.get(producer)
            if producer_manifest is None:
                raise AlignmentError(
                    f"{node}: depends_on references unknown producer {producer!r} "
                    "(no such manifest)"
                )

            field = _top_field(rest)
            properties = _output_properties(producer_manifest.output_schema)
            if field not in properties:
                raise AlignmentError(
                    f"{node}: producer {producer!r} does not declare output field {field!r} "
                    f"(declared: {sorted(properties)})"
                )

            if input_name not in consumer_inputs:
                raise AlignmentError(
                    f"{node}: depends_on target input {input_name!r} is not a declared input "
                    f"of {node!r} (declared: {sorted(consumer_inputs)})"
                )

            if producer not in dag.get_dependencies(node):
                raise AlignmentError(
                    f"{node}: manifest depends_on {producer!r} but the DAG has no edge "
                    f"{producer!r} -> {node!r}"
                )

            # B2 (opt-in): the DEEP gate — producer output type must be compatible with the
            # consumer input type. Only a concrete, mutually-declared mismatch raises; unknown
            # types pass (conservative), so this never rejects an un-annotated manifest.
            if strict_types:
                producer_type = _field_type(producer_manifest.output_schema, field)
                consumer_type = _field_type(consumer_inputs, input_name)
                if not _types_compatible(producer_type, consumer_type):
                    raise AlignmentError(
                        f"{node}: edge {producer}.{field} -> {input_name} is type-INCOMPATIBLE — "
                        f"producer declares {producer_type!r} but consumer input expects "
                        f"{consumer_type!r}"
                    )
