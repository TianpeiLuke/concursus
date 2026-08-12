# Guide: Authoring Agents (`.agent.yaml`)

*Write agent manifests, declare dependencies, and satisfy the output-schema type gate.*

This guide covers the **declarative inputs** you author by hand — the `.agent.yaml`
manifests and the `AgentDAG` they wire into. These are the raw material the compiler consumes:
`AgentDAG` + per-agent `AgentManifest` → resolve → assemble → freeze. Remember the invariant —
**Concursus is a compiler, not a runtime governor**: everything here happens *before*
`assemble`, so getting the manifests and their dependency edges right is what makes a run
type-safe. If a wire is wrong, you find out at resolve time, not mid-flight.

Two small pure modules do the work, plus a topology layer:

- [`core/manifest.py`](../../src/concursus/core/manifest.py) — the `AgentManifest`
  model (`.agent.yaml`).
- [`core/dag.py`](../../src/concursus/core/dag.py) — the `AgentDAG` topology.
- [`core/resolve.py`](../../src/concursus/core/resolve.py) — `resolve_edges` +
  `check_alignment`, the dependency resolver and its type gate (plus `resolve_context_mode`,
  the content-reuse policy cascade).

Everything below imports from the package root:

```python
from concursus import (
    AgentDAG, DAGError,
    AgentManifest, ManifestError, AgentCapabilities, MAX_SUPPORTED_CONTRACT_VERSION,
    AgentRef, resolve_edges, check_alignment, AlignmentError, resolve_context_mode,
    TrustGrade,
)
```

---

## 1. The `AgentDAG` — topology of agents

An `AgentDAG` is a pure, backend-agnostic directed acyclic graph. Nodes are agent-id strings;
edges are **data dependencies**. The one direction convention to internalize:

> An edge `a -> b` means **`b` depends on `a`** — `a` is the upstream *producer*, `b` is the
> downstream *consumer*.

You build one imperatively; every mutator returns `self`, so construction chains:

```python
dag = AgentDAG()
for n in ("ingest", "summarizer", "critic"):
    dag.add_node(n)
dag.add_edge("ingest", "summarizer").add_edge("summarizer", "critic")

dag.topological_sort()   # ['ingest', 'summarizer', 'critic']  -> a valid dispatch order
dag.validate()           # raises DAGError on a cycle; returns self otherwise
```

### Nodes and edges

| Method | Behavior |
|---|---|
| `add_node(name)` | Adds a node; idempotent (nodes are a set). Rejects a non-`str` or empty/whitespace name with `DAGError`. |
| `add_edge(from_node, to_node)` | Adds `from_node -> to_node`. **Both endpoints must already exist** (`add_node` first) or it raises `DAGError`. Self-loops (`from == to`) raise `DAGError`. Duplicate edges are silently deduplicated. |

### Inspection (all deterministic)

| Accessor | Returns |
|---|---|
| `dag.nodes` | All node ids in **topological (dispatch) order** — producers before consumers, ties broken by name (fresh list). Falls back to a name-sort for a cyclic graph, so `nodes` never raises (cycles are still rejected by `validate`/`topological_sort`). A single-node graph is unchanged. |
| `dag.edges` | All `(from, to)` tuples in **insertion order** (a copy). |
| `dag.get_dependencies(node)` | Direct upstream producers of `node`, sorted. |
| `dag.get_dependents(node)` | Direct downstream consumers of `node`, sorted. |
| `dag.sources()` | Nodes with no dependencies (entry points), sorted. |
| `dag.sinks()` | Nodes with no dependents (terminals), sorted. |

`get_dependencies`/`get_dependents` do **not** validate that `node` exists — an unknown node
yields `[]` rather than raising.

### `topological_sort` and `validate`

`topological_sort()` runs Kahn's algorithm and keeps the ready set sorted at every step, so the
order is **deterministic**: lexicographically smallest among concurrently-ready nodes. If the
graph has a cycle, fewer nodes are emitted than exist and it raises:

```text
DAGError: DAG contains a cycle; agent topologies must be acyclic
```

`validate()` is a convenience wrapper — it runs `topological_sort()`, discards the result, and
returns `self`; its only failure mode is a cycle. `to_dict()` / `from_dict()` round-trip the
graph for JSON/YAML serialization.

---

## 2. The `AgentManifest` — one `.agent.yaml`

An `AgentManifest` is the parsed form of a single `<name>.agent.yaml` file. It is a plain
(non-frozen) dataclass with three dict-shaped sections plus deploy-gate metadata:

```python
@dataclass
class AgentManifest:
    name: str
    registry: Dict[str, Any] = field(default_factory=dict)   # AgentCore hosting binding
    contract: Dict[str, Any] = field(default_factory=dict)   # typed I/O interface
    spec: Dict[str, Any] = field(default_factory=dict)       # depends_on edges
    trust_seed: TrustGrade = TrustGrade.L0_SHADOW            # create-time autonomy
    side_effecting: bool = False                             # deploy-gate flag
    escalate_boundary: str = ""                              # informational label
    capabilities: AgentCapabilities = field(                 # opt-in runtime inventory (§8)
        default_factory=AgentCapabilities)
    contract_version: int = MAX_SUPPORTED_CONTRACT_VERSION   # opt-in schema pin (§8)
    context_mode: str = ""                                   # opt-in reuse policy; "" = inherit (§8)
```

The last three fields — `capabilities`, `contract_version`, and `context_mode` — are part of
the **opt-in additions** (the flexibility & robustness layer completed in v0.6.0). Every one is
**opt-in and default-off**: the empty `AgentCapabilities()` is falsy, `contract_version`
defaults to what this compiler already supports, and `context_mode` defaults to `""` (inherit,
which takes no action on its own). A `.agent.yaml` that omits all three constructs, validates,
resolves, and freezes **byte-for-byte** as it did before those fields existed. See §8 for how to
declare each.

### Field-by-field

| Field | Purpose | Notes |
|---|---|---|
| `name` | The agent's node id — **unique within the DAG**. | Required; a non-empty string. If a `.agent.yaml` omits it, `from_yaml` defaults it to the file stem (basename split on the *first* dot: `summarizer.agent.yaml` → `summarizer`). |
| `registry` | The AgentCore hosting binding. | Must set **exactly one of** `container_uri` (a container image to provision) *or* `agent_runtime_arn` (an existing runtime to reuse). May also carry `role_arn`, `network_mode`, `protocol`, and `qualifier`. |
| `contract` | The typed interface: `{"inputs": {...}, "outputs": {<json-schema>}}`. | `inputs` are the fields Concursus injects into the invoke payload; `outputs` is the **mandatory output JSON Schema** (§3). |
| `spec` | Optional `{"depends_on": [...]}` dependency edges. | Each edge is `{"from": "producer.field.path", "to": "input"}`. See §4. |
| `trust_seed` | Author-declared **create-time** autonomy of this node. | A `TrustGrade` (`L0_SHADOW`…`L3_AUTONOMOUS`). Consulted **once** at provision time by the deploy gate — never per-invocation. Defaults to `L0_SHADOW`. |
| `side_effecting` | Whether the agent takes real-world side effects (writes/sends/external calls). | Only side-effecting agents are gated at deploy time; the default `False` keeps a read-only agent's deploy ungated. |
| `escalate_boundary` | An opaque label naming who a held deploy escalates to. | Purely informational — the compiler stores it but does not act on it. |
| `capabilities` | **(opt-in)** A typed `AgentCapabilities` inventory of what this agent's *runtime* provides — `features` / `tools` / `egress_hosts`. | Purely declarative attestation; the compiler shape-validates and stores it but takes no runtime action. The empty default is **falsy**, so an absent block behaves exactly as before. See §8. |
| `contract_version` | **(opt-in)** The manifest-schema revision this `.agent.yaml` was authored against (an `int`). | Defaults to `MAX_SUPPORTED_CONTRACT_VERSION` (this compiler's newest known revision, currently `1`). `validate()` **fails closed** only if the pin *exceeds* what this compiler supports. See §8. |
| `context_mode` | **(opt-in)** This node's content-reuse policy — `"reuse"`, `"isolation"`, or `""` (inherit). | Defaults to `""` (inherit), which takes **no** action on its own; the effective policy is resolved by `resolve_context_mode` (§8). |

`trust_seed`, `side_effecting`, and `escalate_boundary` feed the deploy gate; see the
[Trust Ladder](../../src/concursus/build/trust.py) and the
[AgentCore deploy guide](deploying-to-agentcore.md).

### Accessor properties (all return copies)

| Property | Reads | Default |
|---|---|---|
| `manifest.protocol` | `registry["protocol"]`, `str`-coerced then `.upper()`ed | `"HTTP"` |
| `manifest.inputs` | `contract["inputs"]` (fresh dict copy) | `{}` |
| `manifest.output_schema` | `contract["outputs"]` (fresh dict copy) | `{}` |
| `manifest.depends_on` | `spec["depends_on"]` (fresh list copy) | `[]` |

Because these hand back copies, callers cannot mutate the manifest's internals through them.

### Loading and validating

```python
m = AgentManifest.from_yaml("agents/summarizer.agent.yaml")   # lazy `yaml` import here only
m.validate()                                                  # returns self; raises ManifestError
```

- **`from_dict(data)`** builds a manifest from a plain dict. It parses `trust_seed` via
  `TrustGrade.parse` (an absent/`None` key → `L0_SHADOW`), copies `registry`/`contract`/`spec`,
  and coerces `side_effecting`/`escalate_boundary`. It does **not** call `validate()` — an empty
  `name` constructs silently and only fails later at `validate()`.
- **`from_yaml(path)`** reads the file as UTF-8 (an empty file yields `{}`), defaults `name` to
  the file stem when unset, and delegates to `from_dict`. `yaml` (PyYAML) is imported lazily
  *inside* the method, so importing `manifest.py` never requires it — only calling `from_yaml`
  does.

### What `validate()` enforces

`validate()` checks these rules, **in this order**, and returns `self` on success:

1. `name` is present and non-empty (after strip).
2. `registry` sets `container_uri` **or** `agent_runtime_arn`.
3. `protocol` is one of `HTTP`, `MCP`, `A2A` (case-insensitive on input — `http` and `Http`
   both normalize and pass; anything else fails).
4. `output_schema` is non-empty (a truthiness check — `{}` fails, any non-empty schema passes;
   it does **not** deep-validate the schema's structure).
5. **(opt-in field)** `contract_version` is an `int` (a `bool` is rejected) and does **not** exceed
   `MAX_SUPPORTED_CONTRACT_VERSION`. The default pin equals that constant, so an un-pinned
   manifest always passes — see §8.
6. **(opt-in field)** `capabilities` is an `AgentCapabilities` instance (the default is, so an absent
   block always passes) — see §8.
7. **(opt-in field)** `context_mode` is one of `""`, `"reuse"`, `"isolation"` (the `""` default is
   inherit and always passes) — see §8.

Each failure raises `ManifestError` (a `ValueError` subclass). An invalid `trust_seed`
(anything `TrustGrade.parse` rejects) raises `ManifestError` from `from_dict`, not a bare
`ValueError`. Rules 5–7 are the opt-in additions; because each field's default is the
"unchanged" value, they never newly reject a manifest that omits them.

---

## 3. Why the output schema is mandatory

Rule 4 above is the load-bearing one. `contract.outputs` — the output JSON Schema — is
**required** because it is the dependency resolver's **type gate**. When agent `b` declares it
consumes a field of `a`'s output, `check_alignment` looks that field up in `a`'s
`output_schema`. Without a declared output schema, there is nothing to check the wire against —
dependency resolution would be meaningless. So a manifest with no `contract.outputs` fails
`validate()`:

```text
ManifestError: summarizer: contract.outputs (a JSON Schema) is required — it is the
dependency resolver's type gate
```

The resolver reads only the **top-level property names** of the schema (see §4), and it accepts
two shapes:

- **Nested JSON Schema** (recommended — this is real JSON Schema):
  ```yaml
  outputs:
    type: object
    properties:
      summary: {type: string}
    required: [summary]
  ```
- **Flat map** of `{property: subschema}`:
  ```yaml
  outputs:
    summary: {type: string}
  ```

Both declare a top-level output field named `summary`. Prefer the nested form so the same schema
is usable as a standard JSON Schema elsewhere.

### Optional per-field `acceptance` (the output-QA gate)

Beyond declaring an output field *exists*, a field's subschema may carry an optional `acceptance`
mapping that constrains its **value** — the machine-checkable slice of "did this agent actually do
its job?" These rules are consulted only by the post-run QA gate (`check_acceptance`), which the
`Supervisor` runs after `validate_output` **only when `check_acceptance=True`** (default off — a run
with no supervisor QA gate ignores `acceptance` entirely, byte-for-byte unchanged). The rules are
declarative and deterministic — no code eval:

| Rule | Meaning |
|---|---|
| `non_empty: true` | The value must be truthy/non-empty (a `None`, `""`, `[]`, or `{}` fails). |
| `min_length: N` | `len(value) >= N` (strings, lists, …). |
| `max_length: N` | `len(value) <= N`. |
| `enum: [...]` | The value must be one of the listed values. |
| `pattern: "re"` | A string must **fully** match the regex. |

```yaml
outputs:
  type: object
  properties:
    verdict:
      type: string
      acceptance:
        non_empty: true
        enum: ["approve", "deny", "escalate"]   # a present-but-wrong verdict is rejected
    score:
      type: number
  required: [verdict]
```

A field with **no** `acceptance` mapping is unconstrained (conservative). When the gate is on, a
present-but-wrong output raises `SchemaError` and rides the supervisor's existing retry/record path
— a QA miss is **not** admitted and earns no trust. The gate is fully opt-in and defaults off; see
the [Governor guide](governor.md) for wiring it (and its per-node `acceptance_fn` dial).

---

## 4. Declaring dependencies (`depends_on`)

You wire agents together in the **consumer's** manifest, under `spec.depends_on`. Each edge is:

```yaml
spec:
  depends_on:
    - from: "producer.field.path"   # where the value comes from
      to: "input"                   # which of MY inputs it feeds
```

- **`from`** names the upstream producer and a path into its output. It is split on the **first
  dot** into a producer id and a `$.`-prefixed [JSONPath](#jsonpath): `summarizer.summary` →
  producer `summarizer`, path `$.summary`; a bare `summarizer` (no dot) → path `$`.
- **`to`** names one of *this* agent's declared `contract.inputs` fields.

### `resolve_edges` — compile edges into `AgentRef` wiring

`resolve_edges(dag, manifests)` compiles every DAG node's `depends_on` into typed `AgentRef`
wires and returns `{node_id: [AgentRef, ...]}` for **every** node in the DAG (an empty list when
a node declares no dependencies or has no matching manifest):

```python
@dataclass(frozen=True)
class AgentRef:
    producer: str      # upstream node id supplying the value
    path: str          # minimal JSONPath into producer output, e.g. '$.summary'
    input_name: str    # consumer input field this value feeds
```

`AgentRef` is frozen (immutable, hashable). `resolve_edges` iterates `dag.nodes`, so the returned
dict is keyed in **topological (dispatch) order** (producers before consumers, ties by name). Note:
**it does not type-check** — it will
build wiring even for a misaligned edge. Type-checking is `check_alignment`'s job; run it
separately.

### `check_alignment` — the type gate

`check_alignment(dag, manifests)` returns `None` on success and raises `AlignmentError` (a
`ValueError` subclass) on the **first** violation. It iterates the **supplied `manifests`**
(not `dag.nodes`), and for each `depends_on` edge it checks four conditions, in order:

| # | Check | `AlignmentError` when… |
|---|---|---|
| a | Producer is a known manifest | `depends_on` references a producer with no manifest. |
| b | Referenced top-level output field is a declared property of the producer's `output_schema` | the producer does not declare that output field. |
| c | The `to` input is a declared input of the consumer | the target input is not in the consumer's `contract.inputs`. |
| d | The DAG carries the edge `producer -> consumer` | the manifest `depends_on` a producer but the DAG has no matching `add_edge`. |

Condition (d) is easy to trip: declaring a `depends_on` in the manifest is **not** enough — the
`AgentDAG` must also carry the corresponding `producer -> consumer` edge (the assembler adds
these for you; see [Compiling & Running](compiling-and-running.md)). The field check (b) uses
only the **top-level** field of the path — `summary.items[0]` checks that `summary` is declared,
not the nested `items`.

Every `AlignmentError` is **answer-carrying**: besides the human message it exposes structured
attributes (`node`, `producer`, `field`, `expected`, `candidates`) so a programmatic caller — e.g.
the `staff_with_rebind` re-binder in §7 — can react without parsing the message text. Optional
deeper gates layer on top of the four name-level checks (all default-off, so the base gate is
byte-for-byte unchanged): `strict_types=True` requires the producer and consumer *types* to be
compatible (a concrete mismatch raises; an unknown/absent type on either side passes),
`single_writer=True` rejects two edges feeding the same consumer input, `full_input_cover=True`
requires every declared input to have a compile-visible supplier, and `require_capabilities=True`
is the compile-time capability gate (§8).

<a id="jsonpath"></a>
### `extract` — pulling the value at run time

The path in an `AgentRef` is read at run time by `extract(obj, path)`, a minimal JSONPath
reader. It supports a leading `$`/`$.`, dotted access, and integer list indices; a bare `$` (or
empty path) returns `obj` unchanged:

```python
from concursus.core.resolve import extract
extract({"summary": {"items": ["a", "b"]}}, "$.summary.items[1]")   # 'b'
extract(payload, "$")                                               # payload, unchanged
```

`extract` raises `KeyError` / `IndexError` on an absent segment — a deliberate broken-wire
signal at run time.

---

## 5. A complete multi-agent example set

A three-agent pipeline: `ingest` fetches a document, `summarizer` condenses it, `critic` scores
the summary. It exercises both `registry` forms (`container_uri` vs `agent_runtime_arn`), the
deploy-gate metadata, multi-property outputs, and two `depends_on` wires.

### `agents/ingest.agent.yaml`

```yaml
name: ingest
registry:
  container_uri: "111122223333.dkr.ecr.us-east-1.amazonaws.com/ingest-agent:latest"
  role_arn: "arn:aws:iam::111122223333:role/ConcursusAgentRuntimeRole"
  protocol: HTTP
  qualifier: DEFAULT
contract:
  inputs:
    url: {type: string}
  outputs:
    type: object
    properties:
      document: {type: string}
    required: [document]
# no spec.depends_on — ingest is a source; its `url` input is supplied externally
```

### `agents/summarizer.agent.yaml`

```yaml
name: summarizer
registry:
  container_uri: "111122223333.dkr.ecr.us-east-1.amazonaws.com/summarizer-agent:latest"
  protocol: HTTP
contract:
  inputs:
    text: {type: string}
  outputs:
    type: object
    properties:
      summary: {type: string}
    required: [summary]
spec:
  depends_on:
    - from: ingest.document      # ingest's output field `document`
      to: text                   # feeds summarizer's input `text`
```

### `agents/critic.agent.yaml`

```yaml
name: critic
registry:
  # reuse an already-deployed AgentCore Runtime instead of provisioning a container
  agent_runtime_arn: "arn:aws:bedrock-agentcore:us-east-1:111122223333:runtime/critic-9f3a"
  protocol: HTTP
trust_seed: L1_CANARY            # create-time autonomy (int 0-3 or a name both parse)
side_effecting: true             # side-effecting -> gated at deploy time
escalate_boundary: "oncall-review"
contract:
  inputs:
    draft: {type: string}
  outputs:
    type: object
    properties:
      critique: {type: string}
      score: {type: number}
    required: [critique]
spec:
  depends_on:
    - from: summarizer.summary
      to: draft
```

### Loading, building the DAG, and resolving

```python
from concursus import AgentManifest, AgentDAG, resolve_edges, check_alignment

# 1. Load + validate each manifest, keyed by name.
manifests = {
    m.name: m
    for m in (
        AgentManifest.from_yaml("agents/ingest.agent.yaml").validate(),
        AgentManifest.from_yaml("agents/summarizer.agent.yaml").validate(),
        AgentManifest.from_yaml("agents/critic.agent.yaml").validate(),
    )
}

# 2. Build the DAG: one node per manifest, one edge per depends_on
#    (producer -> consumer). The assembler does this for you in a real compile;
#    it is spelled out here to show the a -> b convention.
dag = AgentDAG()
for name in manifests:
    dag.add_node(name)
for name, manifest in manifests.items():
    for edge in manifest.depends_on:
        producer = edge["from"].split(".", 1)[0]
        dag.add_edge(producer, name)
dag.validate()   # DAGError on a cycle

# 3. Compile the wiring, then type-gate it.
wiring = resolve_edges(dag, manifests)
check_alignment(dag, manifests)   # None on success, AlignmentError on the first bad edge
```

### The resolved result

`dag.topological_sort()` gives the deterministic dispatch order:

```python
['ingest', 'summarizer', 'critic']
```

and `resolve_edges` returns (keyed in topological/dispatch order, the same order as `dag.nodes`):

```python
{
    'ingest':     [],
    'summarizer': [AgentRef(producer='ingest',     path='$.document', input_name='text')],
    'critic':     [AgentRef(producer='summarizer', path='$.summary',   input_name='draft')],
}
```

`check_alignment` passes because, for every edge, the producer manifest exists, the referenced
output field is declared (`ingest.document`, `summarizer.summary`), the consumer input is
declared (`summarizer.text`, `critic.draft`), and the DAG carries the matching edge.

### What a broken wire looks like

If `summarizer.agent.yaml` had wired `to: prose` while its `contract.inputs` only declared
`text`, `check_alignment` would raise at condition (c):

```text
AlignmentError: summarizer: depends_on target input 'prose' is not a declared input of
'summarizer' (declared: ['text'])
```

Likewise, dropping `dag.add_edge("ingest", "summarizer")` while keeping the manifest
`depends_on` trips condition (d) — the manifest declares a dependency the DAG does not carry.

---

## 6. Authoring a net-new role (when there is no `.agent.yaml` yet)

Everything above assumes you *hand-write* a manifest: you know the role exists, and you author its
`.agent.yaml` by hand. But sometimes a plan surfaces a **capability gap** — a task that no standing
agent can serve and for which no manifest has ever been written. For that case the Governor's Create
capability can **author** a manifest instead of merely provisioning a declared one:

```python
from concursus.governor.authoring import author_manifest, ManifestAuthorError
from concursus import TrustGrade
```

> `author_manifest` lives in `governor/authoring.py`, not at the package root — it is a Create-side
> tool that runs strictly **before** `assemble` and yields a plain manifest value. It never touches
> `Supervisor.run` or a running frozen plan.

```python
author_manifest(
    task,                          # the capability/role label to author for (non-empty)
    *,
    inputs=None,                   # optional contract.inputs for the new role
    context=None,                  # optional context handed to an injected author fn
    manifest_author_fn=None,       # DEFAULT None -> deterministic offline skeleton
    trust_seed=TrustGrade.L0_SHADOW,  # create-time autonomy of the new role
) -> AgentManifest
```

By default (`manifest_author_fn=None`, off unless you inject one) it returns a **deterministic,
offline skeleton** — a valid, provisionable, container-hosted HTTP `AgentManifest` with:

- a placeholder `container_uri` of the form `"<to-provision>/<slug>:latest"` (a real image is
  supplied later, at the separate, gated provision step),
- an `entry` of `"agents.<slug>:run"` and `capabilities: [task]` in `registry`,
- a **minimal but non-empty** output schema — `{"result": {"type": "string", "required": True}}` —
  so the dependency resolver's type gate (§3) has something to check the wire against,
- `side_effecting=False`, at a **low `trust_seed`** (default `L0_SHADOW`).

```python
m = author_manifest("triage duplicate-refund claims")
m.name           # 'triage_duplicate_refund_claims'  (a stable [a-z0-9_] slug of the task)
m.output_schema  # {'result': {'type': 'string', 'required': True}}
m.trust_seed     # TrustGrade.L0_SHADOW
m.validate()     # already validated for you — returns self
```

To synthesize a richer role — a real prompt, SOPs, tools, and a fuller output schema — inject an
`manifest_author_fn(task, context) -> AgentManifest | dict`. That is the LLM seam; whatever it
returns (an `AgentManifest` or a `from_dict` mapping) is coerced and **always** re-validated against
the same `AgentManifest.validate()` rules from §2:

```python
def my_author(task, context):
    return {                                   # a from_dict mapping is fine
        "name": "refund_triager",
        "registry": {"container_uri": "…/refund-triager:latest", "protocol": "HTTP"},
        "contract": {
            "inputs": {"claim": {"type": "string"}},
            "outputs": {"type": "object", "properties": {"verdict": {"type": "string"}},
                        "required": ["verdict"]},
        },
    }

m = author_manifest("triage refund claims", manifest_author_fn=my_author)  # coerced + validated
```

Anything the author fn returns that is neither an `AgentManifest` nor a mapping, or that fails
`validate()` (or an empty `task`), raises **`ManifestAuthorError`** (a `ValueError` subclass).

**Authored, not declared — and still unproven.** The rest of this guide teaches roles you *declare*
by hand; `author_manifest` lets a role be **created** from a capability gap. But the two paths
converge at the same gate: an authored manifest is an ordinary `AgentManifest`, and because it
enters at `L0_SHADOW` it must **earn** autonomy on the [Trust Ladder](../../src/concursus/build/trust.py)
before it can dispatch a side-effecting task — exactly like a hand-authored one. Authoring closes
the *creation* gap; it does not shortcut the *trust* gate.

---

## 7. Staffing a whole capability DAG (decompose → bind → assemble)

`author_manifest` (§6) fills **one** capability gap. But the cold-start path produces a whole
agent-agnostic **capability `AgentDAG`** — from `plan_from_goal(..., decompose=True)` — with task
nodes and edges but **no** manifests and **no** `depends_on` wiring, so it cannot be assembled
directly. `governor.authoring.staff_capability_dag` turns that capability DAG into an assemblable
manifest set in one pass:

```python
from concursus.governor.authoring import (
    staff_capability_dag, staff_with_rebind, RebindExhausted,
)
from concursus import TrustGrade
```

> Both helpers live in `governor/authoring.py`, not at the package root. They are Create/compile-side
> tools that run strictly **before** `assemble`, are pure and offline (INV-2 — they bind/author
> *values*, never dispatch and never mutate a running frozen plan), and yield ordinary manifests.

```python
staff_capability_dag(
    dag,                              # a capability AgentDAG (nodes + edges, no manifests/wiring)
    *,
    bind_fn=None,                     # DEFAULT None -> author every node as an L0 skeleton
    manifest_author_fn=None,          # optional LLM author seam, forwarded to author_manifest
    trust_seed=TrustGrade.L0_SHADOW,  # create-time autonomy of each synthesized node
) -> Dict[str, AgentManifest]
```

Per node it synthesizes a manifest **keyed by the node id** — bound to a standing agent when
`bind_fn(node)` returns an agent name, else an authored `L0_SHADOW` skeleton (via
`author_manifest`) — plus its **data-wiring from the DAG edges**: one input per upstream producer
(named after the producer node) fed by `"<producer>.result"`, with the matching `depends_on` edge.
So the staffed set type-aligns and `assemble` freezes it exactly like a hand-authored one. With
`bind_fn=None` (the default) every node is authored, which makes the **zero-bench cold-start** path
work end-to-end: `decompose → staff → assemble` freezes a real multi-node plan with **zero**
hand-authored manifests.

```python
from concursus import OrchestrationAssembler
from concursus.assemble.planner import plan_from_goal

cap_dag = plan_from_goal("triage duplicate-refund claims", decompose=True)  # capability DAG
manifests = staff_capability_dag(cap_dag)          # author every node (no bench yet)
plan = OrchestrationAssembler().assemble(cap_dag, manifests)   # freezes a real multi-node plan
```

### `staff_with_rebind` — search ranked candidates, re-bind on a type mismatch

When you *do* have a bench — several candidate agents per capability — a single bind can pick a
team that fails the deep type gate. `staff_with_rebind` makes the compiler a **regulator** rather
than a mere validator: it strict-assembles and, on a type-alignment failure, advances the
**offending** node to its next candidate and retries — a bounded author-time search.

```python
staff_with_rebind(
    dag,                  # the capability AgentDAG
    candidates_fn,        # node -> [AgentManifest, ...] best-first (the ranked-candidates seam)
    *,
    assembler=None,       # OrchestrationAssembler to strict-assemble with (a default is used if None)
    max_rebinds=8,        # caps the bounded search (INV-2: an author-time loop, not a run loop)
) -> Dict[str, AgentManifest]
```

`candidates_fn(node)` returns that node's manifests best-first (e.g. the scheduler's trust-ranked
candidate set). Starting from every node's first candidate it strict-assembles; on an
`AlignmentError` naming an offending node it advances **that** node to its next candidate and
retries. It returns the type-aligning `{node: AgentManifest}` set (assemblable under
`strict_types`), or raises **`RebindExhausted`** (a `ValueError` subclass) if no combination aligns
within `max_rebinds`. This is the reject-and-**rebind** fix: the compiler rejects a misaligned team
and searches for one that assembles, instead of only reporting the failure.

Like everything in §6–§7, a staffed manifest is still an ordinary `AgentManifest` entering at
`L0_SHADOW` — it must earn autonomy on the [Trust Ladder](../../src/concursus/build/trust.py)
before dispatching a side-effecting task.

---

## 8. The opt-in blocks: capabilities, contract-version pin, and context mode

Three **optional** `.agent.yaml` blocks — `capabilities`, `contract_version`, and `context_mode` —
are the exception to nothing: every one is **opt-in and default-off**, and a manifest that declares
none of them constructs, validates, resolves, and freezes **byte-for-byte** as it did before they
existed. Nothing below changes a default — each subsection is a knob you may turn on, not a behavior
that turned on for you.

### `capabilities` — a runtime capability inventory

`capabilities` is a purely-declarative inventory of what an agent's *runtime* provides, parsed into
a frozen [`AgentCapabilities`](../../src/concursus/core/manifest.py) with three sequences of
opaque, author-declared labels:

| Key | Declares |
|---|---|
| `features` | Runtime features/behaviours this agent enables. |
| `tools` | Tool ids the agent's runtime is allowed to call. |
| `egress_hosts` | Network hosts the runtime may reach. |

```yaml
capabilities:
  features: ["structured-output", "retry"]
  tools: ["search", "fetch_document"]
  egress_hosts: ["api.internal.example.com"]
```

The compiler **stores and shape-validates** this block but takes **no** runtime action on it — it is
documentation/attestation, Concursus being a compiler and not a runtime governor. Shape validation
happens eagerly in `AgentCapabilities.from_obj` (called by `from_dict`): an **unknown key** or a value
that is not a list of strings raises `ManifestError`. A common mistake — a bare string where a list is
expected — is rejected on purpose:

```text
ManifestError: <agent>: capabilities.tools must be a list of strings (got str)
```

The empty default (`AgentCapabilities()`) declares nothing and is **falsy**
(`bool(AgentCapabilities()) is False`), so a manifest with **no** `capabilities:` block is
byte-for-byte identical to before everywhere a manifest is inspected. `AgentCapabilities.to_dict()`
round-trips it back to `{"features": [...], "tools": [...], "egress_hosts": [...]}` for serialization.

### The compile-time capability gate (`require_capabilities`)

The `capabilities` inventory is what an agent *provides*. Its mirror is what a manifest *requires* —
an optional `spec.requires` block in the **same** `{features?, tools?, egress_hosts?}` shape (a bare
list is read as `features`, the one-dimension shorthand). The gate is the opt-in
`require_capabilities` parameter of `check_alignment` (default `False`):

```python
check_alignment(dag, manifests, require_capabilities=True)
```

When on, for each manifest that declares `spec.requires`, **every** required label must appear in that
agent's own `capabilities` block, or the compile **fails closed** with an answer-carrying
`AlignmentError` — its `field` is the capability kind, `expected` is the still-missing labels, and
`candidates` is what the agent actually declares:

```yaml
# a manifest that requires a tool it does not attest
spec:
  requires:
    tools: ["fetch_document", "write_ticket"]
capabilities:
  tools: ["fetch_document"]        # 'write_ticket' is NOT declared
```

```text
AlignmentError: <agent>: manifest requires tools ['write_ticket'] but its capabilities.tools
declares ['fetch_document'] — a capability gate violation (the target agent's runtime does not
attest these)
```

This is a compile/author-time check only — it declares nothing about the run and never touches AWS.
It is **conservative and opt-in**: a manifest with no `spec.requires` imposes nothing, so leaving the
gate off (the default) — or even turning it on for an agent that requires nothing — is byte-for-byte
the prior behavior.

### `contract_version` — pin the manifest-schema revision

`contract_version` is an optional `int` naming the manifest-schema revision the `.agent.yaml` was
authored against. It defaults to `MAX_SUPPORTED_CONTRACT_VERSION` — the newest revision **this**
compiler knows how to compile (currently `1`) — so an un-pinned manifest is always in range.

```yaml
contract_version: 1   # optional; defaults to this compiler's MAX_SUPPORTED_CONTRACT_VERSION
```

`validate()` **fails closed** only when the pin *exceeds* the compiler's max — i.e. the manifest was
authored against a **newer** compiler than the one now reading it. A pin equal to or below the max
passes. (A non-`int`, including a `bool`, also fails.)

```text
ManifestError: <agent>: contract_version 2 exceeds this compiler's
MAX_SUPPORTED_CONTRACT_VERSION 1 — upgrade the compiler or lower the manifest's contract_version
```

Bump `MAX_SUPPORTED_CONTRACT_VERSION` only when the manifest schema itself changes; until you pin a
value, this field is invisible.

### `context_mode` — per-agent content-reuse policy

`context_mode` declares whether this node's already-stood-up content **may be reused** across
compiles, or whether the node is **always re-provisioned**. It is one of three literals
(`CONTEXT_MODES`):

| `context_mode` | Meaning |
|---|---|
| `"reuse"` | This node's prior content may be reused. |
| `"isolation"` | Always re-provision this node; never reuse a prior deployment's content. |
| `""` *(default)* | **INHERIT** — take no stance here; defer to a team/group default, then a hardcoded `"isolation"` floor. |

```yaml
context_mode: reuse   # optional; "" (inherit) is the default and takes no action on its own
```

The empty default is purely inherit and takes **no** action on its own, so an absent `context_mode:`
is byte-for-byte identical to before.

#### The resolution cascade (`resolve_context_mode`)

An author-declared `context_mode` is only *half* the answer — the **effective** policy is computed by
`resolve_context_mode(manifest, team_default="isolation")`, a pure function that applies a strict
precedence cascade and always returns a **concrete** `"reuse"` | `"isolation"`:

1. the manifest's own `context_mode`, when it is a concrete policy (`"reuse"` / `"isolation"`);
2. otherwise `team_default`, when *it* is a concrete policy (the group-level fallback);
3. otherwise the hardcoded **`"isolation"`** floor.

An empty/absent/unrecognized value at **any** level (`""`, `None`, a typo) is treated as INHERIT — it
defers to the next level rather than being honored. So a manifest that never sets `context_mode`, with
a caller that passes the default `team_default`, resolves to `"isolation"` — the safe floor:

```python
from concursus import resolve_context_mode, AgentManifest

reuse_node = AgentManifest.from_dict({"context_mode": "reuse"})
resolve_context_mode(reuse_node)                         # 'reuse'  (per-agent wins)

inherit_node = AgentManifest.from_dict({})               # no context_mode -> "" (inherit)
resolve_context_mode(inherit_node)                       # 'isolation'  (floor)
resolve_context_mode(inherit_node, team_default="reuse") # 'reuse'  (team default fills the gap)
```

`resolve_context_mode` is pure — no I/O, no AWS, no mutation — a total function of its two inputs, in
keeping with the compiler-not-governor invariant.

---

## Next steps

- [Guide: Compiling & Running a Team](compiling-and-running.md) — how `resolve_edges` +
  `check_alignment` feed `assemble`, freeze into a `ProvisioningPlan`, and run under the
  `Supervisor`.
- [Guide: The Reasoning Tier](reasoning.md) — form a plan by deliberation and *lower* it to a
  frozen `AgentDAG` before you ever author manifests by hand.
- [Guide: Deploying to AgentCore](deploying-to-agentcore.md) — how `trust_seed`,
  `side_effecting`, and the Trust Ladder gate a manifest's deploy.
- [Guide: The Governor](governor.md) — the strictly-outer runtime governance loop.
- [API Reference: core](../reference/core.md) — full symbol reference for `AgentDAG`,
  `AgentManifest`, and the resolver.
- Also useful: [Core Concepts](../concepts.md), [Getting Started](../getting-started.md),
  [Overview](../overview.md), and the [docs index](../README.md).
