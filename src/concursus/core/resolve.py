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


def check_alignment(dag: "AgentDAG", manifests: Dict[str, "AgentManifest"]) -> None:
    """Type-gate every ``depends_on`` edge; raise :class:`AlignmentError` on any violation.

    For each edge on each manifest: (a) the producer must be a known manifest; (b) the
    referenced top-level output field must be a declared property of the producer's
    ``output_schema``; (c) the ``to`` input must be a declared input of the consumer; and
    (d) the DAG must carry the edge ``producer -> consumer``.
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
