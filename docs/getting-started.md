# Getting Started

*Install Concursus, declare your first agent team, compile it into a frozen plan, and run it — offline, from the Python API and the CLI.*

This is a hands-on tour. By the end you will have declared a two-agent team as an
[`AgentDAG`](../src/concursus/core/dag.py), described each agent with an `.agent.yaml`
manifest, compiled the two into a frozen `ProvisioningPlan`, and run the plan to completion —
all **offline**, with **no AWS and no LLM**. The one rule to keep in mind throughout:
**Concursus is a compiler, not a runtime governor.** A run is
`AgentDAG → assemble → frozen plan → Supervisor.run` — a single forward pass over an
immutable plan. Every generative or mutating step happens *strictly before* `assemble`.

> **The opt-in additions (default-off).** Everything in this quickstart is the **default
> happy-path**, and it is **byte-for-byte unchanged** by the flexibility & robustness layer
> completed in 0.6.0 — every addition is **opt-in and default-off**, so a run that turns none of
> them on behaves exactly as before, and the compiler framing above is untouched (still one static
> pass over a frozen `plan.order`, resume-by-replay). When you want more, start at the
> [overview](overview.md), then the relevant guide:
>
> - **Bounded within-node parallelism** — `Supervisor.run(inputs, *, parallel=N)` dispatches each
>   ready antichain on a bounded thread pool; `parallel=1` (the default) is exactly the serial pass,
>   and any `N` is still one static pass over the frozen plan. See
>   [Compiling & Running a Team](guides/compiling-and-running.md).
> - **Durable note-version history** — `FileVaultStateStore(versioned=True)` snapshots each distinct
>   note content into a `versions/` sidecar with forward-only revert; the default store never writes
>   it. See [Durable Run State](guides/durable-state.md).
> - **Richer manifests** — the optional `AgentManifest` fields `capabilities` and `context_mode`,
>   both inert at their empty defaults. See [Authoring Agents](guides/authoring-agents.md).

---

## 1. Install

The core is pure Python (only `pyyaml`); AWS and LangGraph are optional extras imported
lazily, so the core and the full test suite run with **neither** installed. Requires
**Python 3.10+**.

```bash
pip install concursus                 # pure core (pyyaml only) — compile + offline run
pip install 'concursus[agentcore]'    # + boto3, bedrock-agentcore — live deploy/invoke
pip install 'concursus[reasoning]'    # + langgraph — pre-freeze deliberation tier
```

Installing the package registers a console script, `concursus` (see §4).

---

## 2. Your first team — the DAG

An [`AgentDAG`](../src/concursus/core/dag.py) is the topology: nodes are agent ids, edges are
data dependencies. The edge convention is `add_edge(a, b)` means **`b` depends on `a`** (`a` is
the upstream producer). `topological_sort()` returns a deterministic dispatch order (Kahn's
algorithm, lexicographic among concurrently-ready nodes); `validate()` raises `DAGError` on a
cycle.

```python
from concursus import AgentDAG

dag = AgentDAG()
for agent in ["ingest", "summarize"]:
    dag.add_node(agent)                 # chainable; idempotent (nodes are a set)
dag.add_edge("ingest", "summarize")     # summarize depends on ingest

dag.topological_sort()   # ['ingest', 'summarize']  <- dispatch order
dag.validate()           # returns self; raises DAGError if the topology has a cycle
```

`add_edge` requires both endpoints to already exist as nodes (call `add_node` first),
rejects self-loops, and silently deduplicates repeat edges.

---

## 3. Describe, compile, and run — end to end, offline

### 3a. Describe each agent with `.agent.yaml`

Each agent is one [`AgentManifest`](../src/concursus/core/manifest.py) declaring three things:
its `registry` (how it is hosted on AgentCore), its `contract` (typed inputs + the
**mandatory** output JSON Schema), and an optional `spec.depends_on` (edges wiring an upstream
output field into this agent's input).

The output schema is required because it is the dependency resolver's **type gate** — a
manifest with an empty `contract.outputs` fails `validate()`.

`ingest.agent.yaml` — a source agent (no `depends_on`):

```yaml
name: ingest
registry:
  container_uri: 111122223333.dkr.ecr.us-east-1.amazonaws.com/ingest:latest
  protocol: HTTP                 # HTTP | MCP | A2A
  entry: agents.ingest:run       # module:function the synthesized serving wrapper calls
contract:
  inputs:
    uri: {type: string}
  outputs:                       # MANDATORY — the resolver's type gate
    document: {type: string}
```

`summarize.agent.yaml` — consumes `ingest`'s `document` output as its `document` input:

```yaml
name: summarize
registry:
  container_uri: 111122223333.dkr.ecr.us-east-1.amazonaws.com/summarize:latest
  protocol: HTTP
  entry: agents.summarize:run
contract:
  inputs:
    document: {type: string}
  outputs:
    properties:                  # nested {"properties": {...}} form is also accepted
      summary: {type: string}
    required: [summary]
spec:
  depends_on:
    - from: ingest.document      # producer.field
      to: document               # this agent's input field
```

A valid manifest needs a non-empty `name`, either a `container_uri` (to provision) or an
`agent_runtime_arn` (to reuse an already-deployed runtime) in `registry`, a `protocol` of
`HTTP`/`MCP`/`A2A`, and a non-empty output schema. When the compiler synthesizes a serving
wrapper for a `container_uri` agent it also needs `registry.entry` as `module:function` (the
callable the wrapper invokes). Both the flat `{field: {...}}` and the nested
`{"properties": {...}}` output-schema shapes are accepted. See the
[authoring guide](guides/authoring-agents.md) for the full contract and the alignment rules
that `depends_on` must satisfy.

### 3b. Load, assemble, and run

Load the manifests with
[`AgentManifest.from_yaml`](../src/concursus/core/manifest.py), compile with
[`OrchestrationAssembler`](../src/concursus/assemble/assemble.py), and run with the
[`Supervisor`](../src/concursus/execute/supervisor.py). `assemble` is pure and offline — it
validates the topology and every manifest, type-gates and resolves the `depends_on` edges into
`AgentRef` wiring, synthesizes one build entry per node, and freezes a topologically-ordered
`ProvisioningPlan`. It never touches AWS (the `account`/`region` are threaded only into the
previewable synthesized IAM roles).

```python
from concursus import AgentDAG, AgentManifest, OrchestrationAssembler, Supervisor

# 1. Load manifests, keyed by agent name.
paths = ["ingest.agent.yaml", "summarize.agent.yaml"]
manifests = {m.name: m for m in map(AgentManifest.from_yaml, paths)}

# 2. Declare the topology.
dag = AgentDAG()
for name in manifests:
    dag.add_node(name)
dag.add_edge("ingest", "summarize")

# 3. Compile → a frozen ProvisioningPlan (validates + type-gates + freezes; no AWS).
plan = OrchestrationAssembler(account="111122223333", region="us-east-1").assemble(dag, manifests)
plan.order   # ['ingest', 'summarize']  <- the dispatch order

# 4. Run — fully offline. Inject a fake invoke transport so no boto3 is needed, and hand in
#    ARNs so the "deploy first" integrity gate passes (nothing is really deployed here).
def fake_invoke(arn, qualifier, session_id, payload_bytes):
    # payload_bytes is JSON; return a dict that satisfies the agent's output schema.
    if arn == "arn:ingest":
        return {"document": "DOC-123"}
    return {"summary": "a short summary"}

sup = Supervisor(
    plan,
    manifests,
    invoke_fn=fake_invoke,
    arns={"ingest": "arn:ingest", "summarize": "arn:summarize"},
)
outputs = sup.run({"uri": "s3://bucket/doc.txt"})
```

`Supervisor.run` returns `{node_id: output_dict}` for the nodes that completed:

```python
outputs
# {
#     "ingest":    {"document": "DOC-123"},
#     "summarize": {"summary": "a short summary"},
# }
```

The `Supervisor` uses the offline `InProcessStateStore` by default (an append-only log in
memory; no AWS). It walks `plan.order`, overlays each node's external inputs with its
resolved upstream outputs, invokes the transport, shape-checks the result against the
manifest output schema, and threads it forward. You can query the transitive upstream context
of any node with `Supervisor.context`:

```python
sup.context("summarize")
# {"ingest": {"document": "DOC-123"}}   # {producer: latest validated output}, nearest-first
```

> **Why `invoke_fn` and `arns`?** The default transport lazily binds boto3 (the
> `[agentcore]` extra) and calls a live AgentCore runtime — so an offline run must inject its
> own `invoke_fn`. And because nothing has been deployed, each node's ARN is still a
> placeholder; the dispatch-time integrity gate rejects a placeholder ARN with *"deploy
> first"*, so we pass real-looking `arns=` to satisfy it. This is exactly how the test suite
> drives a run with no AWS. For a live run you install `[agentcore]`, `deploy` the agents,
> and let the resolved runtime ARNs flow from the plan.

---

## 4. The same flow from the CLI

The installed console script is `concursus`, with five verbs. `info`, `validate`, and
`plan` never touch AWS. `deploy` and `run` are **dry-run by default** and print what they
*would* do; only `--execute` binds boto3 and takes real side effects.

| Verb | What it does | Touches AWS? |
|---|---|---|
| `info` | Print the version banner and command overview. | No |
| `validate` | Load + `validate()` each `.agent.yaml` (OK/FAIL per file). | No |
| `plan` | Assemble and print the JSON `ProvisioningPlan`. | No |
| `deploy` | Dry-run the provisioning steps; `--execute` runs IAM → ECR → `CreateAgentRuntime`. | Only with `--execute` |
| `run` | Dry-run the topological dispatch; `--execute` runs live `InvokeAgentRuntime`. | Only with `--execute` |

```bash
concursus info                                    # overview
concursus validate ingest.agent.yaml summarize.agent.yaml

# Compile only — prints the frozen plan as JSON, no AWS.
concursus plan ingest.agent.yaml summarize.agent.yaml \
    --account 111122223333 --region us-east-1

# Deploy: dry-run first (default), then for real with --execute.
concursus deploy *.agent.yaml                     # prints what it WOULD do
concursus deploy *.agent.yaml --execute           # role → ECR image → CreateAgentRuntime

# Run: dry-run the dispatch (default), then live with --execute.
concursus run *.agent.yaml --inputs '@inputs.json'            # prints the dispatch plan
concursus run *.agent.yaml --inputs '@inputs.json' --execute  # live InvokeAgentRuntime
```

Edges are inferred from each manifest's `depends_on`; pass `--dag 'FROM->TO'` (repeatable) to
override them explicitly. `--inputs` accepts either an inline JSON object or `@path/to.json`.
The full flag catalog — `--vault`, `--memory-id`, `--approve`/`--yes`, `--min-autonomy`,
`--source-dir`, `--tag` — is in the [CLI guide](guides/cli.md).

---

## 5. Developing Concursus

Concursus is a pure-Python package with a stdlib-only test suite (no AWS, no LangGraph, no
network). Clone the repo, install it editable with the `dev` extra, and run `pytest`:

```bash
git clone https://github.com/TianpeiLuke/concursus
pip install -e '.[dev]'       # pytest + build + mypy + black
pytest                        # the full offline suite
```

The tests inject fakes for every AWS/transport seam, so the whole matrix runs with only the
pure core installed. See the [CHANGELOG](../CHANGELOG.md) for the release history and the
opt-in additions layered on top of the default `plan → deploy → run` path.

---

## Next steps

- [Guide: Authoring Agents (`.agent.yaml`)](guides/authoring-agents.md) — the full manifest
  contract and how `depends_on` edges satisfy the output-schema type gate.
- [Guide: Compiling & Running a Team](guides/compiling-and-running.md) — the compile pipeline
  (resolve → assemble → freeze → supervise), recompile, and `plan_from_goal` (including the
  opt-in `decompose=True` multi-node capability planner, **off by default**). Also the opt-in
  **compiler contract gates** on `OrchestrationAssembler` / `check_alignment` — `strict_types`
  (deep producer→consumer type compatibility) and `single_writer` (reject two edges into one
  input) — plus the post-run **output-QA gate** `Supervisor(check_acceptance=True)` (declarative
  `acceptance` constraints). All three default **off**; when off the name-level gate and run are
  byte-for-byte unchanged.
- [Guide: The Governor](guides/governor.md) — the pre-freeze governance seams, all **opt-in and
  off by default**: the Trust Ladder scheduler's candidate-set binder (`decide_ranked` /
  `propose_bindings`), net-new agent role authoring (`author_manifest`), and `auto_create`
  spawn-on-`UNMATCHED`. Also the cold-start **capability-staffing** front — `staff_capability_dag`
  (decompose → bind → assemble with zero hand-authored manifests) and `staff_with_rebind`
  (reject-and-rebind author-time search) — the `GovernorLoop(decompose=True, bind_fn=…,
  record_frontier=True)` live path, and the **adaptive strictness dial** `make_trust_strictness`
  that wires the contract/QA gates to fire only for weakly-trusted nodes (`strict_fn=` /
  `acceptance_fn=`). All default **off**.
- [Guide: Durable Run State](guides/durable-state.md) — the `StateStore` seam, the three
  backends, and replay-resume.
- [Guide: Knowledge Transfer](guides/knowledge-transfer.md) — the opt-in `state.transfer`
  egress that flows a finished run's episodic notes out into a permanent external Slipbox via a
  knowledge-consolidation sub-agent.
- [Guide: Command-Line Interface](guides/cli.md) — the full `concursus` reference.
- [API Reference: core](reference/core.md) — `AgentDAG`, `AgentManifest`, and the resolver.
- [Overview](overview.md) and the [documentation index](README.md).
