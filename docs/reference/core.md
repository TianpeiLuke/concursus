# API Reference: `core`

*`AgentDAG`, `AgentManifest`, and the dependency resolver — the pure, backend-agnostic heart of Concursus.*

The `core` tier is the declarative core of `concursus`: no AWS, no AgentCore, no third-party coupling (the one exception is a lazy `yaml` import used only when loading a manifest *file*, plus a `TrustGrade` import from the sibling `build` package). Three modules cohere into the front of the compile pipeline:

| Module | Source | Owns |
|---|---|---|
| `core.dag` | [`../../src/concursus/core/dag.py`](../../src/concursus/core/dag.py) | The topology: an `AgentDAG` of agent-id nodes and data-dependency edges, with deterministic topological ordering (Kahn's algorithm) and cycle rejection. |
| `core.manifest` | [`../../src/concursus/core/manifest.py`](../../src/concursus/core/manifest.py) | A single agent's declarative `.agent.yaml`: its AgentCore hosting binding (`registry`), typed interface (`contract`), author-declared `depends_on` edges (`spec`), and deploy-gate metadata. |
| `core.resolve` | [`../../src/concursus/core/resolve.py`](../../src/concursus/core/resolve.py) | Dependency resolution: compile `depends_on` edges into typed `AgentRef` wiring (`resolve_edges`), type-gate the graph (`check_alignment`), and pull a producer's output value at run time (`extract`). |

> Reminder: **Concursus is a compiler, not a runtime governor.** These three modules run entirely *before* `assemble` — they describe and validate the topology; they never dispatch, never call a runtime, and never mutate a frozen plan.

To author the manifests these APIs consume, see [Guide: Authoring Agents](../guides/authoring-agents.md). For the next stage — turning a validated DAG + manifests into a frozen plan — see [`assemble`](assemble.md) and [`execute`](execute.md), and the [Compiling & Running](../guides/compiling-and-running.md) guide.

All eleven public symbols below are re-exported from the package root:

```python
from concursus import (
    AgentDAG, DAGError,
    AgentManifest, ManifestError, AgentCapabilities, MAX_SUPPORTED_CONTRACT_VERSION,
    AgentRef, AlignmentError, resolve_edges, resolve_context_mode, check_alignment,
)
```

> **The opt-in, default-off additions.** The `AgentCapabilities` block, the `contract_version` pin, the `context_mode` policy, the `AgentDAG.classify_cycle_edges` classifier, the `resolve_context_mode` cascade, and the answer-carrying `AlignmentError` attributes + `check_alignment(require_capabilities=…)` gate — the flexibility & robustness layer completed in 0.6.0 — are all additive. Omitting them (or leaving the new keyword args at their `False` / `None` defaults) leaves the default compile behavior **byte-for-byte unchanged** — Concursus stays a compiler that makes a single static pass over a frozen `plan.order`.

The helper `extract` is public but *not* re-exported at the root; import it from its module:

```python
from concursus.core.resolve import extract
```

---

## `core.dag`

Source: [`../../src/concursus/core/dag.py`](../../src/concursus/core/dag.py)

A pure directed acyclic graph whose nodes are agent/task id strings and whose edges are data dependencies. Edge direction convention: **`a -> b` means `b` depends on `a`** (`a` is the upstream producer).

| Symbol | Kind | Summary |
|---|---|---|
| [`DAGError`](#dagerror) | exception | Invalid DAG (bad node name, self-loop, edge to unknown node, or a cycle). |
| [`AgentDAG`](#agentdag) | class | The graph itself; built empty, mutated via chainable methods. |
| [`AgentDAG.add_node`](#agentdagadd_node) | method | Add a node (idempotent). |
| [`AgentDAG.add_edge`](#agentdagadd_edge) | method | Add a `from -> to` dependency edge (deduplicated). |
| [`AgentDAG.nodes`](#agentdagnodes) | property | All node ids, in topological (dispatch) order. |
| [`AgentDAG.edges`](#agentdagedges) | property | All `(from, to)` edge tuples, insertion order. |
| [`AgentDAG.get_dependencies`](#agentdagget_dependencies) | method | Direct upstream producers of a node. |
| [`AgentDAG.get_dependents`](#agentdagget_dependents) | method | Direct downstream consumers of a node. |
| [`AgentDAG.sources`](#agentdagsources) | method | Nodes with no dependencies (roots). |
| [`AgentDAG.sinks`](#agentdagsinks) | method | Nodes with no dependents (leaves). |
| [`AgentDAG.topological_sort`](#agentdagtopological_sort) | method | Deterministic dispatch order (Kahn's algorithm). |
| [`AgentDAG.validate`](#agentdagvalidate) | method | Assert acyclic; returns self. |
| [`AgentDAG.classify_cycle_edges`](#agentdagclassify_cycle_edges) | method | *(opt-in, additive)* Return the set of edges on a cycle (order-independent Tarjan SCC); does **not** change cycle rejection. |
| [`AgentDAG.to_dict`](#agentdagto_dict) | method | Serialize to a plain dict. |
| [`AgentDAG.from_dict`](#agentdagfrom_dict) | classmethod | Rebuild from a plain dict. |

### `DAGError`

```python
class DAGError(ValueError)
```

Raised on an invalid DAG: an unknown or empty node name, a self-loop, an edge referencing a node that has not been added, or a detected cycle. Subclasses `ValueError`, so callers may catch either `DAGError` or `ValueError`.

### `AgentDAG`

```python
class AgentDAG:
    def __init__(self) -> None
```

Constructed empty. Internally holds a `set` of node ids and a list of `(from_node, to_node)` edge tuples. All mutators return `self`, so construction is fluent.

```python
from concursus import AgentDAG

dag = AgentDAG()
for n in ["ingest", "summarize", "critique", "format"]:
    dag.add_node(n)
dag.add_edge("ingest", "summarize") \
   .add_edge("summarize", "critique") \
   .add_edge("critique", "format")
dag.topological_sort()   # ['ingest', 'summarize', 'critique', 'format']
```

#### `AgentDAG.add_node`

```python
def add_node(self, name: str) -> "AgentDAG"
```

Add a node. Idempotent — nodes are a set, so re-adding the same name is a no-op. Returns `self` for chaining.

- **Parameters:** `name` — a non-empty (after `.strip()`) string.
- **Returns:** `self`.
- **Raises:** `DAGError` — if `name` is not a `str`, or is empty / whitespace-only. (Note: this is *not* a silent no-op; a bad name raises.)

#### `AgentDAG.add_edge`

```python
def add_edge(self, from_node: str, to_node: str) -> "AgentDAG"
```

Add a data-dependency edge `from_node -> to_node` (i.e. `to_node` depends on `from_node`). Duplicate edges are silently deduplicated (an identical `(from, to)` pair is not appended twice). Returns `self` for chaining.

- **Parameters:** `from_node`, `to_node` — both must already exist as nodes (call `add_node` first).
- **Returns:** `self`.
- **Raises:**
  - `DAGError` — if either endpoint is not a known node.
  - `DAGError` — if `from_node == to_node` (self-loops are rejected).

#### `AgentDAG.nodes`

```python
@property
def nodes(self) -> List[str]
```

All node ids, returned in **topological (dispatch) order** — producers before consumers, with ties among equally-ready nodes broken by name (deterministic) — as a fresh list snapshot. This is `topological_sort()`'s ordering, so `nodes` matches the order the Supervisor walks. For a single node this is just `[that_node]`.

> **Behavioral change (0.4.4):** `nodes` previously returned a plain alphabetical name-sort; it now returns topological order. If a cached snapshot or downstream consumer assumed alphabetical ordering, it is now wrong. To recover a name-sort, call `sorted(dag.nodes)` explicitly.

For a **cyclic** graph `nodes` falls back to a plain name-sort rather than raising, so `to_dict()` and guards like `if not dag.nodes` stay safe on an invalid DAG. Cycles are still rejected by [`validate`](#agentdagvalidate) / [`topological_sort`](#agentdagtopological_sort). Read-only — mutating the returned list does not affect the DAG.

#### `AgentDAG.edges`

```python
@property
def edges(self) -> List[tuple]
```

All edges as `(from_node, to_node)` tuples, in **insertion order** (a shallow copy of the internal list). Read-only — mutating the returned list does not affect the DAG; add edges only through `add_edge`.

#### `AgentDAG.get_dependencies`

```python
def get_dependencies(self, node: str) -> List[str]
```

The direct upstream producers of `node` — every `f` for which an edge `(f, node)` exists. Returned **sorted**.

- **Note:** does *not* validate that `node` exists; an unknown node yields `[]` rather than raising.

#### `AgentDAG.get_dependents`

```python
def get_dependents(self, node: str) -> List[str]
```

The direct downstream consumers of `node` — every `t` for which an edge `(node, t)` exists. Returned **sorted**.

- **Note:** does *not* validate that `node` exists; an unknown node yields `[]` rather than raising.

#### `AgentDAG.sources`

```python
def sources(self) -> List[str]
```

Nodes with no dependencies (entry points / roots), computed as nodes whose `get_dependencies` is empty. Returned **sorted**.

```python
dag.sources()   # ['ingest']
```

#### `AgentDAG.sinks`

```python
def sinks(self) -> List[str]
```

Nodes with no dependents (terminals / leaves), computed as nodes whose `get_dependents` is empty. Returned **sorted**.

```python
dag.sinks()   # ['format']
```

#### `AgentDAG.topological_sort`

```python
def topological_sort(self) -> List[str]
```

Return a valid dispatch order via **Kahn's algorithm**. The ready set is re-sorted at every step, so the ordering is **deterministic**: among nodes of equal readiness the lexicographically smallest is emitted first (not insertion order). This is the order the Supervisor walks.

- **Returns:** a list of every node id in a valid topological order.
- **Raises:** `DAGError` — `"DAG contains a cycle; agent topologies must be acyclic"` if the graph has a cycle (i.e. fewer nodes are emitted than exist).

```python
dag.topological_sort()   # ['ingest', 'summarize', 'critique', 'format']
```

#### `AgentDAG.validate`

```python
def validate(self) -> "AgentDAG"
```

Assert the graph is a valid DAG by running `topological_sort` and discarding the result. Convenience wrapper; its only failure mode is a cycle. Returns `self` for chaining.

- **Returns:** `self`.
- **Raises:** `DAGError` — if the graph contains a cycle.

#### `AgentDAG.classify_cycle_edges`

```python
def classify_cycle_edges(self) -> Set[tuple]
```

*(Opt-in, additive, read-only.)* Return the set of `(from, to)` edges that lie on a cycle. An edge is a cycle edge iff its two endpoints share a strongly-connected component of size > 1, **plus** any self-loop `(n, n)`; all other edges (tree/forward/cross) are DAG edges. On an acyclic graph this returns the empty `set()`.

The classification is computed via an **iterative Tarjan SCC** pass and is therefore **order-independent and canonical**: because a digraph's strongly-connected components are a unique partition of its nodes, the returned edge set does not depend on where traversal starts or on `add_node` / `add_edge` insertion order — the same graph always yields the same answer. (A single-pass DFS back-edge walk would instead label edges relative to its own visit order, so two runs from different roots could flag *different* edges of the same cycle — unsuitable for a reproducible compile step.) The explicit work-stack implementation also stays safe on deep or large topologies without hitting Python's recursion limit.

- **Returns:** `Set[tuple]` — the `(from, to)` cycle edges; empty for an acyclic graph.
- **Raises:** nothing — unlike [`validate`](#agentdagvalidate) / [`topological_sort`](#agentdagtopological_sort), it never raises on a cycle.

> **Additive — the acyclic default is unchanged.** This is purely a classifier: it does not mutate the graph and does not touch [`topological_sort`](#agentdagtopological_sort) / [`validate`](#agentdagvalidate) / [`nodes`](#agentdagnodes). Cycle **rejection stays the default** — `assemble` and the governor still call `validate` at freeze time to reject any cyclic topology. This method is the opt-in hook a caller can invoke *instead* to classify the offending edges (e.g. to permit a declared, bounded back-edge) rather than fail.

```python
dag.classify_cycle_edges()   # set() for the acyclic example above
```

#### `AgentDAG.to_dict`

```python
def to_dict(self) -> dict
```

Serialize to `{"nodes": [...ids...], "edges": [[from, to], ...]}`. Nodes are emitted in `nodes`' **topological (dispatch) order** (name-sort fallback for a cyclic graph); edges are converted from tuples to 2-element lists in insertion order. JSON/YAML-friendly and round-trips with `from_dict`.

```python
dag.to_dict()
# {'nodes': ['ingest', 'summarize', 'critique', 'format'],
#  'edges': [['ingest', 'summarize'], ['summarize', 'critique'], ['critique', 'format']]}
```

#### `AgentDAG.from_dict`

```python
@classmethod
def from_dict(cls, data: dict) -> "AgentDAG"
```

Rebuild an `AgentDAG` from a `{"nodes": [...], "edges": [[from, to], ...]}` dict. All nodes are added first, then all edges (each edge read via `e[0]`, `e[1]`). Missing `"nodes"` / `"edges"` keys default to empty. Because `add_node` / `add_edge` run, the same validation applies.

- **Raises:** `DAGError` — if edge endpoints reference nodes not present in `"nodes"`, or any other `add_node` / `add_edge` validation fails.

```python
AgentDAG.from_dict(dag.to_dict())   # round-trips
```

---

## `core.manifest`

Source: [`../../src/concursus/core/manifest.py`](../../src/concursus/core/manifest.py)

Models a single agent's `.agent.yaml`: its AgentCore hosting binding (`registry`), typed interface (`contract` — inputs plus a **mandatory** output JSON Schema), author-declared dependency edges (`spec.depends_on`), and deploy-gate metadata (`trust_seed`, `side_effecting`, `escalate_boundary`). This is the declarative unit Concursus compiles into `CreateAgentRuntime` / `InvokeAgentRuntime` calls. For the authoring rules and the output-schema type gate, see [Authoring Agents](../guides/authoring-agents.md).

| Symbol | Kind | Summary |
|---|---|---|
| [`ManifestError`](#manifesterror) | exception | Invalid manifest (missing name, no hosting binding, bad protocol, missing output schema, unparseable `trust_seed`, malformed `capabilities`, out-of-range `contract_version`, or bad `context_mode`). |
| [`MAX_SUPPORTED_CONTRACT_VERSION`](#max_supported_contract_version) | constant | Newest `.agent.yaml` contract revision this compiler compiles (`1`); `validate()` fails closed above it. |
| [`AgentCapabilities`](#agentcapabilities) | dataclass (frozen) | *(opt-in)* Purely-declarative `features` / `tools` / `egress_hosts` inventory; empty default is falsy. |
| [`AgentManifest`](#agentmanifest) | dataclass | Parsed `<name>.agent.yaml`. |
| [`AgentManifest.protocol`](#agentmanifestprotocol) | property | Registry protocol, uppercased (defaults to `HTTP`). |
| [`AgentManifest.inputs`](#agentmanifestinputs) | property | Declared input fields (copy of `contract["inputs"]`). |
| [`AgentManifest.output_schema`](#agentmanifestoutput_schema) | property | Output JSON Schema (copy of `contract["outputs"]`). |
| [`AgentManifest.depends_on`](#agentmanifestdepends_on) | property | Author-declared dependency edges (copy of `spec["depends_on"]`). |
| [`AgentManifest.context`](#agentmanifestcontext) | property | Free-form static context (copy of `contract["context"]`) — dimension 2/3 of the payload contract. |
| [`AgentManifest.capabilities`](#agentmanifestcapabilities) | field | *(opt-in)* Typed [`AgentCapabilities`](#agentcapabilities) inventory; empty default. |
| [`AgentManifest.contract_version`](#agentmanifestcontract_version) | field | *(opt-in)* Manifest-schema revision pin; defaults to `MAX_SUPPORTED_CONTRACT_VERSION`. |
| [`AgentManifest.context_mode`](#agentmanifestcontext_mode) | field | *(opt-in)* Content-reuse policy: `"reuse"` \| `"isolation"` \| `""` (inherit). |
| [`AgentManifest.validate`](#agentmanifestvalidate) | method | Enforce the manifest contract; returns self. |
| [`AgentManifest.from_dict`](#agentmanifestfrom_dict) | classmethod | Build from a plain dict. |
| [`AgentManifest.from_yaml`](#agentmanifestfrom_yaml) | classmethod | Load a `.agent.yaml` file from disk. |

### `ManifestError`

```python
class ManifestError(ValueError)
```

Raised on an invalid agent manifest: missing name, no `container_uri` / `agent_runtime_arn`, a bad protocol, a missing output schema, or an unparseable `trust_seed`. Also raised on a malformed `capabilities` block, a non-int or too-new `contract_version`, or a `context_mode` outside the valid set. Subclasses `ValueError`.

### `MAX_SUPPORTED_CONTRACT_VERSION`

```python
MAX_SUPPORTED_CONTRACT_VERSION = 1
```

The newest `.agent.yaml` contract revision this compiler knows how to compile. A manifest may pin an optional [`contract_version`](#agentmanifestcontract_version); [`validate`](#agentmanifestvalidate) **fails closed** if that pin is *greater* than this constant (the manifest was authored against a newer compiler). Bump it only when the manifest schema itself changes. Re-exported at the package root.

### `AgentCapabilities`

```python
@dataclass(frozen=True)
class AgentCapabilities:
    features: Tuple[str, ...] = ()
    tools: Tuple[str, ...] = ()
    egress_hosts: Tuple[str, ...] = ()
```

*(Opt-in, default-off.)* A **frozen**, purely-declarative inventory of what an agent's *runtime* provides: three sequences of opaque author-declared labels — `features` (runtime features/behaviours enabled), `tools` (tool ids the runtime may call), and `egress_hosts` (network hosts the runtime may reach). The compiler **stores and shape-validates** this block but takes **no action** on it — it is documentation/attestation, not a runtime governor (Concursus is a compiler, not a runtime governor).

The empty default declares nothing extra and is **falsy** — `bool(AgentCapabilities()) is False` — so a manifest with **no** `capabilities:` block behaves byte-for-byte as before everywhere a manifest is inspected (notably the governor registry's capability-derivation fallback).

| Field | Meaning |
|---|---|
| `features` | Tuple of runtime feature/behaviour labels this agent enables. |
| `tools` | Tuple of tool ids the agent's runtime is allowed to call. |
| `egress_hosts` | Tuple of network hosts the runtime may reach. |

Two helpers:

- `to_dict() -> Dict[str, List[str]]` — a `{"features": [...], "tools": [...], "egress_hosts": [...]}` view (tuples widened to lists) for serialization.
- `from_obj(obj, *, agent="") -> AgentCapabilities` *(classmethod)* — build (and shape-validate) from `None` (→ empty), an existing `AgentCapabilities` (returned as-is), or a `dict`. Raises `ManifestError` on an unknown key (only `features` / `tools` / `egress_hosts` are allowed) or a non-list-of-strings value — notably a **bare string** is rejected (it is a common mistake for a one-element list).

```python
from concursus import AgentCapabilities

caps = AgentCapabilities(features=("web_search",), tools=("http.get",))
bool(AgentCapabilities())   # False — empty default is falsy
caps.to_dict()              # {'features': ['web_search'], 'tools': ['http.get'], 'egress_hosts': []}
```

### `AgentManifest`

```python
@dataclass
class AgentManifest:
    name: str
    registry: Dict[str, Any] = field(default_factory=dict)
    contract: Dict[str, Any] = field(default_factory=dict)
    spec: Dict[str, Any] = field(default_factory=dict)
    trust_seed: TrustGrade = TrustGrade.L0_SHADOW
    side_effecting: bool = False
    escalate_boundary: str = ""
    capabilities: AgentCapabilities = field(default_factory=AgentCapabilities)   # opt-in
    contract_version: int = MAX_SUPPORTED_CONTRACT_VERSION                        # opt-in
    context_mode: str = ""                                                       # opt-in
```

Parsed representation of a `<name>.agent.yaml`. A **mutable** (non-frozen) dataclass.

| Field | Meaning |
|---|---|
| `name` | The unique node id within an `AgentDAG`. |
| `registry` | The AgentCore hosting binding — e.g. `container_uri` (+ `role_arn`, `network_mode`, `protocol`, `qualifier`) to provision, or an `agent_runtime_arn` to reuse an already-deployed runtime. |
| `contract` | The typed I/O interface: `{"inputs": {...}, "outputs": {<json-schema>}}`, plus an optional free-form `context` (see [`.context`](#agentmanifestcontext)) — dimension 2/3 of the payload contract. |
| `spec` | Optional `{"depends_on": [{"from": "producer.field", "to": "input"}]}` edges. May also carry a `requires` block (mirror of [`capabilities`](#agentcapabilities)) consumed by the opt-in capability gate — see [`check_alignment`](#check_alignment). |
| `trust_seed` | The author-declared **create-time** autonomy ([`TrustGrade`](build.md), imported from `..build.trust`), consulted **once** at provision time by the deploy gate — never per-invocation. Defaults to `TrustGrade.L0_SHADOW`. |
| `side_effecting` | Whether this agent takes real-world side effects. Only side-effecting agents are gated at deploy time; the default `False` keeps a read-only agent's deploy ungated. |
| `escalate_boundary` | An opaque, informational label naming who/what a held deploy should escalate to; the compiler stores but does not act on it. |
| `capabilities` *(opt-in)* | OPTIONAL typed [`AgentCapabilities`](#agentcapabilities) inventory of the runtime's `features` / `tools` / `egress_hosts`. Empty default is falsy → byte-for-byte identical to before. See [`.capabilities`](#agentmanifestcapabilities). |
| `contract_version` *(opt-in)* | OPTIONAL manifest-schema revision pin; defaults to [`MAX_SUPPORTED_CONTRACT_VERSION`](#max_supported_contract_version). `validate()` fails closed if it exceeds what this compiler supports. See [`.contract_version`](#agentmanifestcontract_version). |
| `context_mode` *(opt-in)* | OPTIONAL per-agent content-reuse policy: `"reuse"`, `"isolation"`, or `""` (inherit — the default). Purely inherit → byte-for-byte identical to before. See [`.context_mode`](#agentmanifestcontext_mode). |

#### `AgentManifest.protocol`

```python
@property
def protocol(self) -> str
```

The registry protocol, `str()`-coerced then `.upper()`-cased; defaults to `"HTTP"` when unset. Expected values are `HTTP`, `MCP`, or `A2A`. Consulted by `validate()`.

- **Note:** because it uppercases, `"http"` or `"Http"` both pass; anything outside `{HTTP, MCP, A2A}` fails `validate()`.

#### `AgentManifest.inputs`

```python
@property
def inputs(self) -> Dict[str, Any]
```

The declared input fields Concursus injects into the invoke payload — a fresh copy of `contract["inputs"]` (empty dict if unset). Mutating the returned dict does not change the manifest.

#### `AgentManifest.output_schema`

```python
@property
def output_schema(self) -> Dict[str, Any]
```

The agent's output JSON Schema — a fresh copy of `contract["outputs"]` (empty dict if unset). This is the dependency resolver's **type gate** and is **mandatory** per `validate()`.

#### `AgentManifest.depends_on`

```python
@property
def depends_on(self) -> List[Dict[str, str]]
```

The author-declared dependency edges — a fresh copy of `spec["depends_on"]` (empty list if unset). Each edge is a `{"from": "producer.field.path", "to": "input"}` dict. Consumed by [`resolve_edges`](#resolve_edges) and [`check_alignment`](#check_alignment).

#### `AgentManifest.context`

```python
@property
def context(self) -> Dict[str, Any]
```

The agent's free-form **static context** — a copy of `contract["context"]` (empty dict if unset; an absent key yields `{}`, leaving the invoke payload unchanged). This is **dimension 2/3 of the payload contract**: dimension 1 (the typed `inputs` / `outputs` in `contract`) carries the wired data and is *never* tiered, while `context` carries the author-supplied prompt-shaping material that *is* tier-projectable. Well-known (all optional) keys are `sop`, `tools_available`, `guardrails`, `examples`, and `tool_calls`; the property does not enforce a schema over them.

- **Note:** consumed downstream by the governor's `project_context` ([`governor`](governor.md)) — which projects this dict down to a [`Tier`](governor.md)-appropriate subset — and overlaid **under** a node's external inputs by the assembler (`payload_tier_fn`, authored into the plan's `payload_contract`) and the [`Supervisor`](execute.md) (`payload_tier_fn`). Absent context or an un-tiered plan keeps the payload byte-for-byte unchanged.

#### `AgentManifest.capabilities`

```python
capabilities: AgentCapabilities = field(default_factory=AgentCapabilities)
```

*(Opt-in, default-off.)* The typed [`AgentCapabilities`](#agentcapabilities) inventory declaring the `features` / `tools` / `egress_hosts` this agent's runtime provides. Parsed from the manifest's optional `capabilities:` mapping via `AgentCapabilities.from_obj` (which shape-validates). The empty default declares nothing and is falsy, so an absent `capabilities:` block is byte-for-byte identical to before. Purely declarative — the compiler stores and shape-validates it but takes no runtime action. Read as the *declared* side of the opt-in capability gate on [`check_alignment`](#check_alignment).

#### `AgentManifest.contract_version`

```python
contract_version: int = MAX_SUPPORTED_CONTRACT_VERSION
```

*(Opt-in, default-off.)* The manifest-schema revision this `.agent.yaml` was authored against. Defaults to [`MAX_SUPPORTED_CONTRACT_VERSION`](#max_supported_contract_version) (currently `1`) when the key is absent, so an un-pinned manifest compiles exactly as before. [`validate`](#agentmanifestvalidate) **fails closed** if the pin is not an `int` (a `bool` is rejected too) or exceeds `MAX_SUPPORTED_CONTRACT_VERSION` — i.e. the manifest targets a newer compiler than this one.

#### `AgentManifest.context_mode`

```python
context_mode: str = ""   # one of "" (inherit) | "reuse" | "isolation"
```

*(Opt-in, default-off.)* The per-agent content-reuse policy, one of the literals in the module constant `CONTEXT_MODES = ("", "reuse", "isolation")`:

- `"reuse"` — this node's already-stood-up content may be reused;
- `"isolation"` — always re-provision this node, never reuse a prior deployment's content;
- `""` — **INHERIT** (the default): take no action on its own; defer to a team/group default, then a hardcoded `"isolation"` floor, via [`resolve_context_mode`](#resolve_context_mode).

The empty default is purely inherit and takes no action on its own, so an absent `context_mode:` is byte-for-byte identical to before. [`validate`](#agentmanifestvalidate) rejects any value outside `CONTEXT_MODES`.

##### The `.agent.yaml` shape (opt-in additions)

All three opt-in keys are optional; omit them and the manifest parses exactly as before:

```yaml
name: summarizer
registry:
  container_uri: 111122223333.dkr.ecr.us-east-1.amazonaws.com/summarizer:latest
  protocol: HTTP
contract:
  inputs:
    text: {type: string}
  outputs:
    properties:
      summary: {type: string}
# --- all OPTIONAL / default-off ---
contract_version: 1          # pin the manifest-schema revision (<= compiler's max)
context_mode: isolation      # "reuse" | "isolation" | "" (inherit)
capabilities:                # purely-declarative runtime inventory
  features: [web_search]
  tools: [http.get]
  egress_hosts: [example.com]
spec:
  requires:                  # mirror of capabilities; gated only when require_capabilities=True
    tools: [http.get]
```

#### `AgentManifest.validate`

```python
def validate(self) -> "AgentManifest"
```

Enforce the manifest contract and return `self`. Checks run in this order:

1. a non-empty `name`;
2. `registry` sets either `container_uri` (to provision) **or** `agent_runtime_arn` (to reuse);
3. `protocol` is one of `HTTP`, `MCP`, `A2A`;
4. a non-empty output schema (`contract.outputs`);
5. `contract_version` is an `int` (not a `bool`) and does not exceed [`MAX_SUPPORTED_CONTRACT_VERSION`](#max_supported_contract_version) — **fails closed** on a manifest authored against a newer compiler;
6. `capabilities` is an [`AgentCapabilities`](#agentcapabilities) instance (a no-op for the empty default);
7. `context_mode` is one of `CONTEXT_MODES` (`""` inherit is always OK).

- **Returns:** `self` (for chaining).
- **Raises:** `ManifestError` on the first failing check —
  - `"manifest requires a non-empty 'name'"`
  - `registry must set 'container_uri' ... or 'agent_runtime_arn' ...`
  - `protocol must be HTTP, MCP, or A2A ...`
  - `contract.outputs (a JSON Schema) is required ...`
  - `contract_version must be an int ...` / `contract_version {n} exceeds this compiler's MAX_SUPPORTED_CONTRACT_VERSION ...`
  - `capabilities must be an AgentCapabilities ...`
  - `context_mode must be one of ['', 'reuse', 'isolation'] ('' = inherit) ...`

> The output-schema check is a truthiness check (`not self.output_schema`), so an empty `{}` fails but *any* non-empty schema passes — `validate()` does not inspect the schema's internal structure.

> Checks 5–7 gate only the new opt-in fields; a manifest that omits `contract_version` / `capabilities` / `context_mode` hits their defaults (`MAX_SUPPORTED_CONTRACT_VERSION`, empty `AgentCapabilities`, `""`), all of which pass — so `validate()` on a manifest that uses none of the opt-in fields is byte-for-byte unchanged.

#### `AgentManifest.from_dict`

```python
@classmethod
def from_dict(cls, data: Dict[str, Any]) -> "AgentManifest"
```

Build an `AgentManifest` from a plain dict. Parses `trust_seed` via `TrustGrade.parse` (defaulting to `L0_SHADOW` when the key is absent or `None`), copies the `registry` / `contract` / `spec` dicts, coerces `side_effecting` to `bool` and `escalate_boundary` to `str`. Also parses the `capabilities` mapping via `AgentCapabilities.from_obj` (shape-validated — may raise `ManifestError`), takes `contract_version` verbatim (defaulting to `MAX_SUPPORTED_CONTRACT_VERSION` when absent or `None`), and coerces `context_mode` to `str` (`""` when absent).

- **Note:** `name` defaults to `""` if absent — `from_dict` does **not** call `validate()`, so an empty name constructs silently and only fails later on `validate()`. Likewise `contract_version` and `context_mode` are stored as-given here; their range/literal checks happen in `validate()`, not `from_dict`. (The `capabilities` shape check is the exception — it runs eagerly in `from_obj`.)
- **Raises:** `ManifestError` — if `trust_seed` is present but not a valid `TrustGrade` (an int `0-3` or a name like `"L0_SHADOW"`); it wraps the underlying `ValueError` from `TrustGrade.parse`.

```python
from concursus import AgentManifest

m = AgentManifest.from_dict({
    "name": "summarizer",
    "registry": {"container_uri": "111122223333.dkr.ecr...", "protocol": "HTTP"},
    "contract": {
        "inputs": {"text": {}},
        "outputs": {"properties": {"summary": {}}},
    },
}).validate()

m.protocol        # 'HTTP'
m.output_schema   # {'properties': {'summary': {}}}
m.trust_seed      # <TrustGrade.L0_SHADOW: 0>
```

#### `AgentManifest.from_yaml`

```python
@classmethod
def from_yaml(cls, path: str) -> "AgentManifest"
```

Load a `.agent.yaml` file from disk and build an `AgentManifest` via `from_dict`. If the YAML omits `name`, it defaults to the file stem — `os.path.basename(path).split(".", 1)[0]`, e.g. `summarizer.agent.yaml` → `summarizer`.

- **Note:** `os` and `yaml` are imported **lazily inside the method** — importing `manifest.py` never requires PyYAML; only *calling* `from_yaml` does. Reads UTF-8; an empty file yields `{}` data.
- **Note:** the name default splits the basename on the **first** dot, so `my.agent.yaml` → `my` (a multi-part stem is not preserved).
- **Returns:** an `AgentManifest` (not yet validated — call `.validate()` yourself).
- **Raises:**
  - `FileNotFoundError` / `OSError` — if the path cannot be opened.
  - `yaml.YAMLError` — on malformed YAML.
  - `ManifestError` — if `trust_seed` is invalid (propagated from `from_dict`).

```python
m = AgentManifest.from_yaml("agents/summarizer.agent.yaml").validate()
```

---

## `core.resolve`

Source: [`../../src/concursus/core/resolve.py`](../../src/concursus/core/resolve.py)

Dependency resolution: compile each manifest's declared `depends_on` edges into typed `AgentRef` wiring, and type-gate the whole graph so every edge's producer, referenced output field, consumer input, and DAG edge all line up. Also provides a minimal JSONPath `extract` used to pull a producer's output value at run time, and a pure `resolve_context_mode` cascade that resolves a node's effective content-reuse policy. Pure core — no AWS, no third-party dependencies.

| Symbol | Kind | Summary |
|---|---|---|
| [`AlignmentError`](#alignmenterror) | exception | A `depends_on` edge fails to type-align (raised only by `check_alignment`); answer-carrying. |
| [`AgentRef`](#agentref) | dataclass (frozen) | A resolved wire: producer output `path` → consumer `input_name`. |
| [`resolve_edges`](#resolve_edges) | function | Compile every node's `depends_on` edges into `AgentRef` wiring. |
| [`resolve_context_mode`](#resolve_context_mode) | function | *(opt-in)* Resolve a node's effective content-reuse policy via a strict precedence cascade. |
| [`check_alignment`](#check_alignment) | function | Type-gate every edge; raise on the first violation (opt-in deep type / single-writer / full-input-cover / capability gates). |
| [`extract`](#extract) | function | Read a value out of an object at a minimal JSONPath. |

### `AlignmentError`

```python
class AlignmentError(ValueError)
```

Raised when a `depends_on` edge fails to type-align against the DAG / manifests — an unknown producer, an undeclared producer output field, an undeclared consumer input, a missing DAG edge, or (with the opt-in deep gates) a type-incompatible or multiply-written input, an uncovered input, or a missing required capability. Subclasses `ValueError`. Raised only by [`check_alignment`](#check_alignment).

Every rejection is **answer-carrying**: besides the human message it carries structured attributes so a programmatic caller (e.g. the `staff_with_rebind` re-binder) can react **without** parsing the message text:

| Attribute | Type | Meaning |
|---|---|---|
| `.node` | `Optional[str]` | The offending consumer node id, when the failure is edge-specific; `None` otherwise. |
| `.producer` | `Optional[str]` | The implicated producer node id, when known; `None` otherwise. |
| `.field` | `Optional[str]` | The offending field: the referenced output field, the consumer input name, or the capability kind, depending on the rejection; `None` when not applicable. |
| `.expected` | `Any` | The expected type/shape (e.g. the consumer input's declared type, or the set of still-missing required capabilities); `None` when not applicable. |
| `.candidates` | `Optional[tuple]` | The set of **valid alternatives** the caller could pick from (e.g. the producer's declared output fields, the consumer's declared inputs, the known producer ids, or the capabilities the agent actually declares); `None` when not applicable. |

Back-compat: the positional-message constructor (`AlignmentError("...")`) is unchanged — all five attributes default to `None`. The `.node` / `.producer` pair predate the three newer fields, and their values on existing rejections are unchanged; the three newer fields are populated where applicable.

### `AgentRef`

```python
@dataclass(frozen=True)
class AgentRef:
    producer: str
    path: str
    input_name: str
```

A resolved wire: it routes a producer's output value (at JSONPath `path`) into a consumer's input field `input_name`. **Frozen** — immutable and hashable. Produced by `resolve_edges`.

| Field | Meaning |
|---|---|
| `producer` | The upstream node id whose output supplies the value. |
| `path` | A minimal JSONPath into the producer's output JSON (e.g. `$.summary`). |
| `input_name` | The consumer input field this value feeds. |

### `resolve_edges`

```python
def resolve_edges(
    dag: "AgentDAG", manifests: Dict[str, "AgentManifest"]
) -> Dict[str, List[AgentRef]]
```

Compile every DAG node's `depends_on` edges into `AgentRef` wiring. Iterates `dag.nodes` (topological/dispatch order) and returns `{node_id: [AgentRef, ...]}` for **every** node in the DAG — an empty list when a node declares no dependencies or has no matching manifest. Each edge `{"from": "producer.field.path", "to": "input"}` is split on its first dot into a producer id and a `$.`-prefixed path (e.g. `"summarizer.summary"` → producer `"summarizer"`, path `"$.summary"`; a bare `"summarizer"` → path `"$"`).

- **Returns:** `Dict[str, List[AgentRef]]` — one entry per DAG node, so callers can index every node id safely.
- **Raises:** `KeyError` — if an edge dict lacks the `"from"` or `"to"` key.

> `resolve_edges` does **no** type-checking — it builds wiring even for misaligned edges. Run `check_alignment` separately to type-gate.

```python
from concursus import resolve_edges

wiring = resolve_edges(dag, {"summarizer": m_sum, "critic": m_crit})
# {'summarizer': [],
#  'critic': [AgentRef(producer='summarizer', path='$.summary', input_name='draft')]}
```

### `resolve_context_mode`

```python
def resolve_context_mode(manifest: Any, team_default: str = "isolation") -> str
```

Resolve a node's effective content-reuse policy via a strict precedence cascade, returning one of `"reuse"` | `"isolation"`. Applies **per-agent → team/group default → hardcoded `"isolation"`** in order:

1. the manifest's own [`context_mode`](#agentmanifestcontext_mode) when it is a concrete policy (`"reuse"` / `"isolation"`);
2. otherwise `team_default` when it is a concrete policy (the group-level fallback);
3. otherwise the hardcoded `"isolation"` floor.

An empty/absent/unrecognized value at any level (`""`, `None`, a typo) is treated as **INHERIT** — it defers to the next level rather than being honored. So a manifest that never sets `context_mode` and a caller that passes the default `team_default` both resolve to `"isolation"`. Pure: no I/O, no AWS, no mutation — a total function of its two inputs. Re-exported at the package root.

- **Parameters:** `manifest` — anything with a `context_mode` attribute (read tolerantly via `getattr`, missing → `""`); `team_default` — the group-level fallback policy (default `"isolation"`).
- **Returns:** `"reuse"` or `"isolation"` — always a concrete policy, never `""`.

> This is a **pure classifier**; it never provisions, re-provisions, or mutates a plan. It is Concursus-as-compiler resolving a declared policy to a concrete value that a downstream build step (the deploy ledger — see [`build`](build.md)) reads; the default `"isolation"` floor keeps behavior conservative when nothing is declared.

```python
from concursus import AgentManifest, resolve_context_mode

resolve_context_mode(AgentManifest.from_dict({"name": "a", "context_mode": "reuse"}))   # 'reuse'
resolve_context_mode(AgentManifest.from_dict({"name": "b"}))                            # 'isolation' (inherit → floor)
resolve_context_mode(AgentManifest.from_dict({"name": "c"}), team_default="reuse")     # 'reuse' (inherit → team)
```

### `check_alignment`

```python
def check_alignment(
    dag: "AgentDAG",
    manifests: Dict[str, "AgentManifest"],
    *,
    strict_types: bool = False,
    single_writer: bool = False,
    strict_fn: "Optional[Callable[[str], bool]]" = None,
    full_input_cover: bool = False,
    require_capabilities: bool = False,
) -> None
```

Type-gate every `depends_on` edge across all supplied manifests. Returns `None` on success; raises `AlignmentError` on the **first** violation. It iterates `manifests.items()` (not `dag.nodes`), so it validates whatever manifests you hand it. For each edge, the name-level checks run in order **a → b → c → d**:

| # | Check | Failure message (abridged) |
|---|---|---|
| a | The producer must be a known manifest. | `depends_on references unknown producer ...` |
| b | The referenced top-level output field must be a declared property of the producer's `output_schema`. | `producer ... does not declare output field ...` |
| c | The `to` input must be a declared input of the consumer. | `depends_on target input ... is not a declared input ...` |
| d | The DAG must carry the edge `producer -> consumer` (checked via `dag.get_dependencies(node)`). | `manifest depends_on ... but the DAG has no edge ...` |

Five **opt-in** keyword gates (all default off, so leaving them unset keeps the name-level gate byte-for-byte unchanged; all are author/compile-time only, INV-2):

| Param | Type | Default | Meaning |
|---|---|---|---|
| `strict_types` | `bool` | `False` | **B2 — deep type gate.** In addition to check **b** (field is *declared*), require the producer output field's declared JSON-Schema `type` to be **compatible** with the consumer input's declared `type`. A concrete, mutually-declared mismatch (e.g. producer `"string"` into consumer `"integer"`) raises `AlignmentError` (with `.node` / `.producer` set). **Conservative:** an unknown/absent `type` on either side passes — the gate can only *prove* a violation, never guess one, so an un-annotated manifest is never newly rejected. Union types (`["string","null"]`) are supported via set-overlap. |
| `single_writer` | `bool` | `False` | **B1 — non-overlap gate.** No consumer input may be fed by more than one `depends_on` edge. Two edges targeting the same `input_name` are a single-writer violation (at run time the supervisor overlays `payload[input_name] = …` per edge, so a second writer silently last-wins — a non-deterministic data-flow bug); this catches it at compile time. Composable with `strict_types`. |
| `strict_fn` | `Optional[Callable[[str], bool]]` | `None` | **B4 — adaptive strictness dial.** Narrows the enabled deep gates (`strict_types` / `single_writer`) to the nodes for which `strict_fn(node)` is truthy; the deep checks are skipped for the rest. `None` applies the enabled checks to every node (byte-for-byte the un-dialed behavior). It **never relaxes** the name-level gate — checks **a → d** always run for every edge. Wire a trust-derived predicate ([`make_trust_strictness`](governor.md#make_trust_strictness)) so a WEAK/low-trust agent gets the strict contract while a proven agent gets the lean path. |
| `full_input_cover` | `bool` | `False` | **F2 — completeness gate.** The dimension-1 completeness quantifier: every *declared consumer input* must have a compile-visible supplier — a `depends_on` edge feeding it **or** a static [`contract.context`](#agentmanifestcontext) key of the same name. When `True`, an input with neither supplier raises `AlignmentError` (message names *full-input-cover*). Where checks **a → d** verify that each declared *edge* aligns, this verifies the *converse* — that no declared input is left unfed. Default off keeps the name+edge gate byte-for-byte unchanged. |
| `require_capabilities` | `bool` | `False` | **G6 — compile-time capability gate.** When a manifest declares REQUIRED capabilities — an author-declared `spec.requires` block mirroring the [`capabilities`](#agentcapabilities) inventory (`{features?, tools?, egress_hosts?}`, or a bare list read as `features`) — the agent's own [`.capabilities`](#agentmanifestcapabilities) block must declare **every** one, or the compile fails closed with an answer-carrying `AlignmentError` (`.field` = the capability kind, `.expected` = the still-missing labels, `.candidates` = the labels the agent actually declares). A manifest with no `spec.requires` imposes nothing, so default off — and even on, an agent that requires nothing — is byte-for-byte the prior behavior for un-annotated manifests. It declares nothing about the run and never touches AWS (INV-2 preserved). |

- **Returns:** `None`.
- **Raises:**
  - `AlignmentError` — on the first failing check: a name-level check (a, b, c, or d), a `single_writer` violation, a `strict_types` incompatibility, a `full_input_cover` uncovered input, or a `require_capabilities` missing capability. Every rejection sets the answer-carrying attributes where applicable (`.node` / `.producer` / `.field` / `.expected` / `.candidates`).
  - `KeyError` — if an edge dict lacks the `"to"` key. (Only `"from"` is `str()`-guarded; a missing `"to"` raises `KeyError`, not `AlignmentError`.)

> The output-property check accepts a JSON Schema written either as nested `{"properties": {...}}` **or** as a flat `{prop: {...}}` map. A manifest whose `depends_on` field lines up perfectly still fails check **d** if the corresponding `add_edge` was never called on the DAG.

```python
check_alignment(dag, manifests)   # returns None, or raises AlignmentError on the first bad edge
```

### `extract`

```python
def extract(obj: Any, path: str) -> Any
```

Read a value out of `obj` at a minimal JSONPath — used at run time to pull a producer's output value along an `AgentRef.path`. Supports a leading `$` / `$.`, dotted access (`a.b.c`), and list indices (`a.b[0]`). A bare `$` (or the empty path) returns `obj` unchanged. Not re-exported at the package root — import from `concursus.core.resolve`.

- **Parameters:** `obj` — the object to read from; `path` — the minimal JSONPath.
- **Returns:** the value at `path`, or `obj` itself for `$` / empty path.
- **Raises:** (by design — these signal a broken wire at run time; there is no custom error type, so callers catch these directly):
  - `KeyError` — when a dict segment is absent.
  - `IndexError` — when a list index is out of range.
  - `TypeError` — if a segment is applied to an object that does not support indexing.

```python
from concursus.core.resolve import extract

extract({"summary": {"items": ["a", "b"]}}, "$.summary.items[1]")   # 'b'
extract(payload, "$")                                               # payload unchanged
```

---

## Invariants at a glance

- **Edges connect existing nodes only** — `add_node` before `add_edge`; no self-loops; duplicate edges are silently deduplicated.
- **Deterministic ordering** — `get_dependencies`, `get_dependents`, `sources`, and `sinks` return name-sorted results; `topological_sort` and `nodes` return topological (dispatch) order (ties by name; `nodes` falls back to a name-sort for a cyclic graph); `edges` preserves insertion order.
- **Acyclic or bust** — `topological_sort` / `validate` raise `DAGError` on a cycle.
- **A valid manifest** has a non-empty `name`, exactly one of `container_uri` / `agent_runtime_arn`, a protocol in `{HTTP, MCP, A2A}`, and a non-empty output schema. Also: `contract_version` an `int` ≤ `MAX_SUPPORTED_CONTRACT_VERSION`, a well-typed `capabilities`, and a `context_mode` in `CONTEXT_MODES`. `from_dict` / `from_yaml` do **not** auto-validate.
- **`output_schema` is mandatory** because it is the resolver's type gate — resolution is meaningless without it.
- **`trust_seed` is create-time only** — consulted once at provision by the deploy gate, never per invocation; only `side_effecting=True` agents are gated at deploy time.
- **An edge aligns only when all four conditions hold** — known producer, declared producer output field, declared consumer input, and a matching `producer -> consumer` DAG edge.
- **`AgentRef` is frozen** — immutable and hashable.
- **Every opt-in addition is default-off** — `capabilities` / `contract_version` / `context_mode` on a manifest, `AgentDAG.classify_cycle_edges`, `resolve_context_mode`, the answer-carrying `AlignmentError` attributes, and `check_alignment(require_capabilities=…)`. Omitting them leaves the default compile **byte-for-byte unchanged**: cycle rejection stays the default (`classify_cycle_edges` only *classifies*), and Concursus remains a compiler that makes a single static pass over a frozen `plan.order`.

## See also

- [Guide: Authoring Agents](../guides/authoring-agents.md) — writing the `.agent.yaml` manifests these APIs consume.
- [Guide: Compiling & Running a Team](../guides/compiling-and-running.md) — where resolve → assemble → freeze → supervise fit together.
- [Core Concepts](../concepts.md) — the DAG / manifest / plan vocabulary and invariants.
- [`assemble` reference](assemble.md) — DAG + manifests → a frozen `ProvisioningPlan`.
- [`execute` reference](execute.md) — the `Supervisor` that walks the frozen plan in topological order.
- [Documentation index](../README.md) — the full doc set and reading order.
