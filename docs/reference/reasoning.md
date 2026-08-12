# API Reference: `reasoning`

*`HypothesisTrail`, the `DKSEngine` + CCS confidence gate, the disposable `InnerGraph`, and the `deliberate` driver — the plan-formation organ that runs strictly *before* the compiler.*

The `reasoning` tier is where `concursus` **forms** a plan by bounded deliberation, then **lowers** the converged conclusion into an immutable `AgentDAG`. Everything here happens **strictly before** [`OrchestrationAssembler.assemble`](assemble.md) freezes a plan; nothing here is ever wired into `Supervisor.run` (see [`execute`](execute.md)). This is the practical expression of the load-bearing invariant:

> **Concursus is a compiler, not a runtime governor.** Deliberation is a generative/mutating step that happens *before* `assemble`. Once a plan is frozen, a run is a single forward pass; a debate can never re-open or mutate a running plan — re-opening `.3` is a *new* formation episode, not a mid-flight re-plan.

Four modules stack bottom-up (all **pure stdlib** — they import and run with **neither LangGraph nor any LLM** installed; the LangGraph backend and every LLM/agent worker enter through **injected seams** with deterministic-stub defaults):

| Module | Source | Owns |
|---|---|---|
| `reasoning.trailstore` | [`../../src/concursus/reasoning/trailstore.py`](../../src/concursus/reasoning/trailstore.py) | The `HypothesisTrail` — the durable, replayable `.3` reasoning-branch store (a tree of hypotheses closed by verdicts) plus a Dung grounded-semantics argumentation layer. |
| `reasoning.dks_engine` | [`../../src/concursus/reasoning/dks_engine.py`](../../src/concursus/reasoning/dks_engine.py) | The `DKSEngine` — the bounded 8-node deliberation state machine, the CCS confidence-coherence score, and the routing gate. |
| `reasoning.inner_graph` | [`../../src/concursus/reasoning/inner_graph.py`](../../src/concursus/reasoning/inner_graph.py) | The `InnerGraph` — a fresh, disposable per-round parallel-dispatch projection of the open frontier, with `DIGEST` write-back to the `.2` worker-log lane. |
| `reasoning.deliberate` | [`../../src/concursus/reasoning/deliberate.py`](../../src/concursus/reasoning/deliberate.py) | The top-level driver: `seed` a `.3` root, `form_plan` by bounded deliberation, and `lower_to_dag` the converged debate into a frozen `AgentDAG`. |

For the narrative walkthrough of the whole SEED → form → LOWER flow see [Guide: The Reasoning Tier](../guides/reasoning.md). For the *runtime* governance loop — a strictly-**outer** organ, never something this tier grows into — see [Guide: The Governor](../guides/governor.md) and the [`governor`](governor.md) reference.

> The `reasoning` subpackage `__init__.py` is intentionally bare — import each symbol from its submodule (e.g. `from concursus.reasoning.trailstore import HypothesisTrail`). Where a source docstring cross-references a top-level `concursus.trailstore` (or `concursus.dks_engine`, `concursus.inner_graph`), read it as `concursus.reasoning.trailstore` — the shipping path is under `reasoning`.

---

## `reasoning.trailstore`

Source: [`../../src/concursus/reasoning/trailstore.py`](../../src/concursus/reasoning/trailstore.py)

The durable, replayable `.3` reasoning-branch store. Records a deliberation — a tree of hypotheses fanned out under a run's `.3` branch, each closed by a verdict — into an append-only JSONL log, and computes Dung grounded-semantics labels over an attack graph. Pure plan-formation: no method dispatches an agent, and none is ever wired into `Supervisor.run`.

| Symbol | Kind | Summary |
|---|---|---|
| [`LABEL_IN` / `LABEL_OUT` / `LABEL_UNDEC`](#grounded-label-constants) | constants | The three Dung grounded labels (`"in"` / `"out"` / `"undec"`). |
| [`Candidate`](#candidate) | type alias | A hypothesis candidate: bare text or a `{"text"/"statement", "confidence"}` dict. |
| [`TrailStoreError`](#trailstoreerror) | exception | Any invalid trailstore operation. Subclasses `ValueError`. |
| [`ThreadNotResolved`](#threadnotresolved) | exception | A deliberation has not converged (open frontier non-empty). Subclasses `TrailStoreError`. |
| [`Hypothesis`](#hypothesis) | dataclass | One node in the `.3` deliberation tree. |
| [`HypothesisTrail`](#hypothesistrail) | class | The durable, replayable deliberation over a run's `.3` branch. |
| [`require_resolved`](#require_resolved) | function | Assert convergence — the guard a LOWER step must call before distilling `.3` into an `AgentDAG`. |
| [`drive_deliberation`](#drive_deliberation) | function | A bounded, pure-Python deliberation driver over an injected `investigator` seam. |

### Grounded-label constants

```python
LABEL_IN = "in"        # accepted: unattacked, or all attackers are `out`
LABEL_OUT = "out"      # rejected: attacked by some `in` argument (dead-end)
LABEL_UNDEC = "undec"  # undecided: never settles to in or out
```

The three labels of the Dung grounded extension. They are re-exported and consumed by `dks_engine`, and `LABEL_OUT` drives which hypotheses are dropped at LOWER.

### `Candidate`

```python
Candidate = Union[str, Dict[str, object]]
```

A hypothesis candidate is either bare text (normalized to confidence `0.0`) or a dict `{"text": ..., "confidence": ...}` (the key `"statement"` is accepted as an alias for `"text"`). A malformed candidate (neither `str` nor `dict`) raises `TrailStoreError`.

### `TrailStoreError`

```python
class TrailStoreError(ValueError)
```

Raised on any invalid trailstore operation — an unknown hypothesis/parent/root id, a bad verdict string, a self-attack, or a malformed candidate. Subclasses `ValueError`, so callers may catch either.

### `ThreadNotResolved`

```python
class ThreadNotResolved(TrailStoreError)
```

Raised by [`require_resolved`](#require_resolved) when a deliberation has **not** converged — its [`open_frontier`](#hypothesistrailopen_frontier) is non-empty. The message names the count and the list of still-open hypotheses. A LOWER step must guard on this before distilling the debate into an `AgentDAG`: you may only lower a *converged* debate, never a live one.

### `Hypothesis`

```python
@dataclass
class Hypothesis:
    id: str
    parent: Optional[str]
    text: str
    confidence: float = 0.0
    depth: int = 0
    goal: Optional[str] = None
    resolved: bool = False
    verdict: Optional[str] = None
    evidence: Optional[dict] = None
    children: List[str] = field(default_factory=list)
    attacks: List[str] = field(default_factory=list)
    verdict_id: Optional[str] = None
```

One node in the `.3` deliberation tree. Verdicts live as **attributes** on the hypothesis (`resolved` / `verdict` / `evidence` / `verdict_id`), not as separate nodes in the read model.

| Field | Meaning |
|---|---|
| `id` | The materialized-path address (`.3/h1`, `.3/h1/c4`, …); the parent is the address with its last segment stripped. |
| `parent` | The parent hypothesis id, or `None` for a root seeded directly under `.3`. |
| `text` | The hypothesis statement. |
| `confidence` | A `[0, 1]` self-confidence. At or above `confidence_floor` a leaf is treated as closed by [`open_frontier`](#hypothesistrailopen_frontier). |
| `depth` | Distance from the root (roots are `0`). |
| `goal` | The seeding goal — set on roots only; `None` for children. |
| `resolved` / `verdict` / `evidence` / `verdict_id` | Populated once a verdict closes the hypothesis. |
| `children` / `attacks` | Child hypothesis ids fanned under this node / ids this node contradicts (outgoing Dung edges). |

> `Hypothesis` is a **plain, non-frozen** dataclass rebuilt on every read. Do not hold a reference expecting it to reflect later mutations — re-read via [`hypotheses()`](#hypothesistrailhypotheses).

### `HypothesisTrail`

```python
class HypothesisTrail:
    def __init__(self, run_dir: Union[str, Path], *, branch: str = ".3") -> None
```

A durable, replayable deliberation over a run's `.3` reasoning branch. Every mutation persists as an append-only JSONL record and rewrites `<run_dir>/.3/trail.jsonl` **atomically** (temp file + `os.replace`); a fresh trail over an existing branch reloads by replay, so a deliberation survives process exit. Thread-safe — all public methods run inside an `RLock`, and the trail re-reads from disk when a peer rewrote the log (mtime changed).

The `branch` argument is keyword-only and defaults to `".3"`. Passing a custom branch changes both the on-disk directory and the root id prefix.

| Member | Kind | Summary |
|---|---|---|
| [`from_config`](#hypothesistrailfrom_config) | classmethod | Bind to the same run dir a `FileVaultStateStore` would use. |
| [`branch_dir`](#hypothesistrailbranch_dir) | property | The on-disk `.3` directory (`run_dir/branch`). |
| [`fanout_root_hypotheses`](#hypothesistrailfanout_root_hypotheses) | method | Seed one root hypothesis per candidate (SEED). |
| [`fanout_hypotheses`](#hypothesistrailfanout_hypotheses) | method | Fan sharper child hypotheses under a parent. |
| [`open_frontier`](#hypothesistrailopen_frontier) | method | The sorted un-resolved leaves within the caps; `[]` means converged. |
| [`write_verdict`](#hypothesistrailwrite_verdict) | method | Close a hypothesis — verdict + resolved marker in one atomic swap. |
| [`hypotheses`](#hypothesistrailhypotheses) | method | Read the tree (or a subtree) as `{id: Hypothesis}`. |
| [`attack`](#hypothesistrailattack) / `contradicts` | method | Add a directed attacks/contradicts edge. |
| [`compute_grounded_extension`](#hypothesistrailcompute_grounded_extension) | method | The Dung grounded labels over a root's subtree. |
| [`arg_label`](#hypothesistrailarg_label) | method | The grounded label of a single hypothesis. |

#### `HypothesisTrail.from_config`

```python
@classmethod
def from_config(cls, *, vault_path: Union[str, Path], session_id: str,
                branch: str = ".3") -> "HypothesisTrail"
```

Bind a deliberation to the **same run dir** a [`FileVaultStateStore`](../../src/concursus/state/filevault.py) would use, so a run's `.1`/`.2` state notes and its `.3` reasoning branch live under one directory (`<vault>/runs/<slug(session_id)>/`). All arguments are keyword-only.

```python
from concursus.reasoning.trailstore import HypothesisTrail

trail = HypothesisTrail.from_config(vault_path="/vault", session_id="ticket-42")
```

#### `HypothesisTrail.branch_dir`

```python
@property
def branch_dir(self) -> Path
```

The on-disk `.3` reasoning-branch directory (`run_dir/branch`) for this run. (Its parent, `branch_dir.parent`, is the run directory an [`InnerGraphDigest`](#innergraphdigest) writes its `.2` lane under.)

#### `HypothesisTrail.fanout_root_hypotheses`

```python
def fanout_root_hypotheses(self, goal: str, candidates: Sequence[Candidate]) -> List[str]
```

Seed root hypotheses (one per candidate) under the `.3` branch and return their ids in candidate order. Roots are depth `0` with `parent=None`; the `goal` is recorded on each root. Root ids are `.3/h<seq>`. This is a **SEED** — a new-goal/ticket action only, never a retrieval query.

- **Raises** `TrailStoreError` if a candidate is neither `str` nor `dict`.

#### `HypothesisTrail.fanout_hypotheses`

```python
def fanout_hypotheses(self, parent_id: str, children: Sequence[Candidate]) -> List[str]
```

Fan sharper child hypotheses under `parent_id` (materialized at `parent/c<seq>`, `depth = parent.depth + 1`) and return their ids in order. Children carry `goal=None`. Bounded expansion — callers cap breadth; `open_frontier`'s `depth_cap` caps depth.

- **Raises** `TrailStoreError` on an unknown `parent_id` or a bad candidate type.

#### `HypothesisTrail.open_frontier`

```python
def open_frontier(self, root: str, *, depth_cap: int = 5,
                  confidence_floor: float = 0.6) -> List[str]
```

The **sorted** list of un-resolved *leaf* hypotheses in `root`'s subtree that are still open. Returning `[]` means the debate has **converged** — this is the termination guard. A hypothesis is **closed / excluded** if it is `resolved`, has children (not a leaf), has `depth > depth_cap`, **or** has `confidence >= confidence_floor`.

- **Raises** `TrailStoreError` on an unknown `root`.

> A leaf whose `confidence >= confidence_floor` is closed **even without a verdict**. High-confidence seeding (as in [`seed`](#seed)'s precedent reuse) empties the frontier immediately, intentionally skipping re-investigation.

#### `HypothesisTrail.write_verdict`

```python
def write_verdict(self, id: str, verdict: str, evidence: Optional[dict] = None) -> str
```

Close a hypothesis: append a `VERDICT` child **and** flip its `RESOLVED` marker in **one** atomic critical section / one file replace, so a concurrent scan never sees a verdict without its resolved marker. Returns the addressable verdict-child id (`<id>/v<seq>`). `verdict` is **upper-cased** before validation and must be one of `ACCEPT` | `REJECT` | `UNDEC`.

- **Raises** `TrailStoreError` if the verdict is not in `ACCEPT`/`REJECT`/`UNDEC`, or on an unknown `id`.

> Because the verdict is upper-cased first, `"accept"` is accepted but `"maybe"` raises `TrailStoreError`.

#### `HypothesisTrail.hypotheses`

```python
def hypotheses(self, root: Optional[str] = None) -> Dict[str, Hypothesis]
```

Read the current deliberation tree as `{id: Hypothesis}`. With `root=None` returns every hypothesis; with a `root` id returns that node's subtree (inclusive). The read model is rebuilt by replay on each call. Verdicts appear as attributes on their hypothesis, not as separate nodes.

- **Raises** `TrailStoreError` on an unknown `root` (when `root` is not `None`).

#### `HypothesisTrail.attack`

```python
def attack(self, attacker_id: str, target_id: str) -> None
contradicts = attack
```

Add a directed contradicts/attacks edge `attacker_id -> target_id`. The Dung attack graph is **independent** of the parent/child tree — an attack can cross subtrees. Idempotent (a duplicate edge is a no-op) and persisted so the edge survives replay. `contradicts` is a class-level alias with identical, still-directed semantics (attacker contradicts target).

- **Raises** `TrailStoreError` if either id is unknown, or on a self-attack (`attacker_id == target_id`).

#### `HypothesisTrail.compute_grounded_extension`

```python
def compute_grounded_extension(self, root: str) -> Dict[str, str]
```

The Dung **grounded extension** labels over `root`'s subtree: `{id -> "in" | "out" | "undec"}`. The least fixed point of the characteristic function — label `in` anything whose attackers are all `out` (vacuous for the unattacked), `out` anything attacked by an `in`, and the remainder `undec`. Restricts the attack graph to the subtree's node set. Pure computation over the `.3` trail.

- **Raises** `TrailStoreError` on an unknown `root`.

#### `HypothesisTrail.arg_label`

```python
def arg_label(self, id: str) -> str
```

The grounded label (`"in"` | `"out"` | `"undec"`) of a single hypothesis, computed over the argumentation framework of its root's subtree (it walks parent links to the root, computes the full grounded extension, then indexes the id).

- **Raises** `TrailStoreError` on an unknown `id`.

### `require_resolved`

```python
def require_resolved(trail: HypothesisTrail, root: str, *, depth_cap: int = 5,
                     confidence_floor: float = 0.6) -> None
```

Assert a deliberation has **converged** — raise [`ThreadNotResolved`](#threadnotresolved) (with the count and list of open hypotheses) if the open frontier is non-empty; otherwise return `None`. This is the termination guard a LOWER step must call before distilling `.3` into an `AgentDAG`. Pure structure over the trail; it imports nothing from the LOWER module (no cycle) and delegates to `trail.open_frontier`.

- **Raises** `ThreadNotResolved` (open frontier non-empty); `TrailStoreError` (unknown `root`, via `open_frontier`).

### `drive_deliberation`

```python
def drive_deliberation(trail: HypothesisTrail, root: str,
                       investigator: Callable[[Hypothesis], object], *,
                       max_rounds: int = 8, depth_cap: int = 5,
                       confidence_floor: float = 0.6) -> int
```

A bounded, pure-Python deliberation driver over an injected `investigator` seam — a leaner sibling of the [`DKSEngine`](#dksengine) loop. It repeatedly resolves each open-frontier hypothesis until the frontier empties **or** `max_rounds` is spent, then returns the round count. For each frontier hypothesis, `investigator(h)` returns:

- a **verdict spec** `{"verdict": "ACCEPT|REJECT|UNDEC", "evidence": {...}}` → closes it via `write_verdict`;
- a **truthy list of child candidates** → fans sharper children via `fanout_hypotheses`;
- **falsy / nothing** → closes it `UNDEC` (with a reason) so the loop always progresses.

Needs no LLM/LangGraph — a stub callable drives it in tests.

- **Raises** `TrailStoreError` (unknown `root`, via `open_frontier`).

```python
from concursus.reasoning.trailstore import HypothesisTrail, require_resolved

trail = HypothesisTrail.from_config(vault_path="/vault", session_id="ticket-42")
roots = trail.fanout_root_hypotheses(
    "fix outage", [{"text": "rollback", "confidence": 0.3}, "scale up"]
)
kids = trail.fanout_hypotheses(roots[0], ["revert deploy", "flush cache"])
trail.attack(kids[0], kids[1])                       # kids[0] contradicts kids[1]
vid = trail.write_verdict(kids[0], "ACCEPT", {"log": "reverted cleanly"})
labels = trail.compute_grounded_extension(roots[0])  # {".3/h1/c1": "in", ...}
require_resolved(trail, roots[0])                    # raises ThreadNotResolved if any leaf still open
```

---

## `reasoning.dks_engine`

Source: [`../../src/concursus/reasoning/dks_engine.py`](../../src/concursus/reasoning/dks_engine.py)

The DKS engine — the deliberation state machine. Drives a bounded deliberation over one `HypothesisTrail` through the cyclic `observe → name → structure → operationalize → test → challenge → improve → compile → re-observe` cycle with a confidence-gated loop-back, carrying a compact MDP-ish `DKSState`. It writes `.3` verdicts (via `trail.write_verdict`) and attack edges but never dispatches an agent and is never wired into `Supervisor.run`. LangGraph is an **optional, lazily-imported** backend that falls back to pure Python.

| Symbol | Kind | Summary |
|---|---|---|
| [`BAND_AUTO_ACCEPT` / `BAND_ARGUE_COUNTER` / `BAND_ESCALATE`](#routing-band-constants) | constants | The three confidence-routing bands. |
| [`DKS_NODES`](#dks_nodes) | constant | The eight-step deliberation chain. |
| [`DKSEngineError`](#dksengineerror) | exception | Invalid engine config or an unknown routing band. Subclasses `ValueError`. |
| [`CCSWeights`](#ccsweights) | dataclass (frozen) | The convex weights of the CCS. |
| [`compute_ccs`](#compute_ccs) | function | The Confidence-Coherence Score. |
| [`RoutePolicy`](#routepolicy) | type alias | The injected learned-policy seam over `(score, state)`. |
| [`route_by_confidence`](#route_by_confidence) | function | Route a CCS score to a band (heuristic + optional policy). |
| [`DKSState`](#dksstate) | dataclass | The MDP-ish deliberation-state pointer `s_t = (n, r, c, f)`. |
| [`DKSResult`](#dksresult) | dataclass | The outcome of a bounded `DKSEngine.run`. |
| [`Investigator`](#investigator--counterargumentfn) | type alias | The per-node work seam. |
| [`CounterArgumentFn`](#investigator--counterargumentfn) | type alias | The counter-argument seam on CHALLENGE. |
| [`DKSEngine`](#dksengine) | class | The cyclic deliberation state machine. |

### Routing-band constants

```python
BAND_AUTO_ACCEPT = "auto_accept"      # CCS >= 0.85 — single-agent auto-accept
BAND_ARGUE_COUNTER = "argue_counter"  # 0.50 <= CCS < 0.85 — two-agent argue + counter
BAND_ESCALATE = "escalate"            # CCS < 0.50 — human-escalation
```

The three bands [`route_by_confidence`](#route_by_confidence) may return. `BAND_ARGUE_COUNTER` is the only band that triggers the `counter_argument_fn` seam on CHALLENGE.

### `DKS_NODES`

```python
DKS_NODES = ("observe", "name", "structure", "operationalize",
             "test", "challenge", "improve", "compile")
```

The eight-step deliberation chain. The ninth step, `re-observe`, is the conditional loop-back edge from `compile` to `observe`, not a distinct node. Both backends walk this chain in order; `compile` routes via the confidence-gated edge.

### `DKSEngineError`

```python
class DKSEngineError(ValueError)
```

Raised on invalid engine configuration (a bad `backend` name, `max_rounds < 1`, or `backend="langgraph"` requested but not installed) or when an injected routing policy returns an unknown band. Subclasses `ValueError`.

### `CCSWeights`

```python
@dataclass(frozen=True)
class CCSWeights:
    alpha: float = 0.5   # weight on the LLM/self confidence
    beta: float = 0.25   # weight on homophily (agreement with same-label neighbours)
    gamma: float = 0.25  # weight on coherence (how few UNDEC labels remain)
```

The convex weights of the Confidence-Coherence Score. Defaults weight the model's own confidence most heavily; homophily and coherence are tie-breakers. Frozen (immutable). Weights are **not** clamped or re-normalized — caller-supplied weights are used verbatim.

### `compute_ccs`

```python
def compute_ccs(llm_conf: float, homophily: float, coherence: float,
                weights: CCSWeights = CCSWeights()) -> float
```

The Confidence-Coherence Score `CCS = alpha*llm_conf + beta*homophily + gamma*coherence`. A pure, planning-time function; the three inputs are each clamped to `[0, 1]` (a non-numeric input clamps to `0.0`), so with default weights the result is well-behaved in `[0, 1]`.

```python
from concursus.reasoning.dks_engine import compute_ccs, route_by_confidence

score = compute_ccs(0.9, 0.5, 0.8)   # weighted by CCSWeights() defaults
band = route_by_confidence(score)    # -> "auto_accept"
```

### `RoutePolicy`

```python
RoutePolicy = Callable[[float, Optional["DKSState"]], str]
```

The learned-policy seam: a routing policy over `(score, state)` returning a band string. Injected via `route_by_confidence(policy=...)` or `DKSEngine(policy=...)`. Its return **must** be one of the three valid bands, or `DKSEngineError` is raised.

### `route_by_confidence`

```python
def route_by_confidence(score: float, *, state: Optional["DKSState"] = None,
                        policy: Optional[RoutePolicy] = None) -> str
```

Route a CCS `score` to a band — the fixed-heuristic gate with an optional policy seam. Heuristic: `>= 0.85` → `BAND_AUTO_ACCEPT`, `>= 0.50` → `BAND_ARGUE_COUNTER`, else `BAND_ESCALATE`. Pure and planning-time only — it never re-routes a committed plan. An injected `policy` **fully overrides** the heuristic, but its returned band is validated against the three known bands; `state` is passed through to the policy only.

- **Raises** `DKSEngineError` if an injected policy returns an unknown band.

### `DKSState`

```python
@dataclass
class DKSState:
    node_count: int = 0
    label_fractions: Dict[str, float] = field(
        default_factory=lambda: {LABEL_IN: 0.0, LABEL_OUT: 0.0, LABEL_UNDEC: 0.0})
    calibration: float = 0.0
    rule_quality: Dict[str, float] = field(
        default_factory=lambda: {"ACCEPT": 0.0, "REJECT": 0.0, "UNDEC": 0.0})
    round: int = 0
    frontier_size: int = 0
    last_node: str = ""
```

The MDP-ish deliberation state `s_t = (n, r, c, f)` — a small (~1 KB), serializable snapshot a routing/RL policy can observe. The *durable* deliberation lives in the `HypothesisTrail`; this is just a pointer.

| Field | Symbol | Meaning |
|---|---|---|
| `node_count` | `n` | Hypotheses in the deliberation subtree. |
| `label_fractions` | `r` | Dung `in`/`out`/`undec` fractions (sum ~1). |
| `calibration` | `c` | Mean agreement between self-confidence and the verdict target over resolved hypotheses (`1.0` = perfectly calibrated; `0.0` when there are no resolved hypotheses). |
| `rule_quality` | `f` | Resolved fractions per verdict kind `ACCEPT`/`REJECT`/`UNDEC`. |
| `round` / `frontier_size` / `last_node` | — | Cycle bookkeeping. |

```python
def to_dict(self) -> dict
```

A JSON-friendly view keyed `{n, r, c, f, round, frontier_size, last_node}` (the two dict fields are copied defensively into fresh dicts).

### `DKSResult`

```python
@dataclass
class DKSResult:
    root: str
    rounds: int
    converged: bool
    frontier: List[str]
    state: DKSState
    trace: List[str]
    backend: str   # "langgraph" or "python"

    @property
    def resolved(self) -> bool  # alias for `converged`
```

The outcome of a bounded [`DKSEngine.run`](#dksenginerun): the `root` debated, the number of `rounds` (cycles) executed, whether it `converged` (frontier empty at end), the remaining open `frontier` ids, the final `DKSState`, the ordered `trace` of node names executed, and which `backend` ran. The `resolved` property is a read-only alias for `converged`.

### `Investigator` / `CounterArgumentFn`

```python
Investigator = Callable[[Hypothesis], object]
CounterArgumentFn = Callable[[Hypothesis, HypothesisTrail], Optional[Sequence[object]]]
```

`Investigator` is the per-node work seam (the **same** contract used by [`inner_graph`](#reasoninginner_graph) and [`deliberate`](#reasoningdeliberate)): given a hypothesis, return a verdict spec `{"verdict": ..., "evidence": {...}}` (closes it) or a list of child candidates (fans children). It defaults to a deterministic stub that closes every hypothesis `UNDEC`.

`CounterArgumentFn` is the counter-argument seam on the CHALLENGE step: given a hypothesis and the trail, return a list of counter-hypothesis candidates to fan (and attack the target), or `None`/`[]` for a no-op. It defaults to a no-op and is **only** invoked for a `BAND_ARGUE_COUNTER` decision.

### `DKSEngine`

```python
class DKSEngine:
    def __init__(self, trail: HypothesisTrail, *,
                 investigator: Optional[Investigator] = None,
                 policy: Optional[RoutePolicy] = None,
                 counter_argument_fn: Optional[CounterArgumentFn] = None,
                 weights: CCSWeights = CCSWeights(),
                 max_rounds: int = 8, depth_cap: int = 5,
                 confidence_floor: float = 0.6, backend: str = "auto") -> None
```

The cyclic deliberation state machine over a `HypothesisTrail`. Runs the 8-node cycle with a confidence-gated loop-back, carrying a compact `DKSState`. Bounded (`max_rounds` / `depth_cap` / `confidence_floor`) and terminates when the trail's open frontier empties or the round budget is spent. Planning-time only. All heavy work is injected — an unsupplied `investigator` / `counter_argument_fn` defaults to the deterministic stub / no-op, and `policy` defaults to `None` (the heuristic gate) — so constructing and running the engine needs neither LangGraph nor an LLM.

- **Raises** `DKSEngineError` if `backend` is not `"auto"` / `"python"` / `"langgraph"`, or if `max_rounds < 1`.

> The default investigator closes everything `UNDEC`, so a stock `DKSEngine.run` converges in a single round — **real reasoning requires injecting an `investigator`.**

#### `DKSEngine.run`

```python
def run(self, root: str) -> DKSResult
```

Drive the bounded deliberation to termination and return a `DKSResult`. Validates the root exists (via `trail.hypotheses(root)`), seeds the initial context, runs the LangGraph backend if available/requested else the pure-Python driver, and packages the final context. Backend selection:

| `backend` | Behavior |
|---|---|
| `"auto"` (default) | Try LangGraph; **silently fall back** to pure Python if it is not importable (or fails at invoke). |
| `"langgraph"` | Strict — **raises** `DKSEngineError` if LangGraph is not installed. |
| `"python"` | Force the pure-Python fallback driver. |

Both backends run the **same** node functions and routing. On return the trail is left in whatever converged / round-capped state was reached; callers may then assert convergence with [`require_resolved`](#require_resolved) / [`lower_guard`](#dksenginelower_guard).

- **Raises** `TrailStoreError` (unknown `root`, via `trail.hypotheses`); `DKSEngineError` (`backend="langgraph"` but LangGraph not installed).

```python
from concursus.reasoning.dks_engine import DKSEngine

engine = DKSEngine(trail, investigator=my_llm_fn, max_rounds=8, backend="python")
result = engine.run(root)          # DKSResult(converged=..., backend="python", trace=[...])
engine.lower_guard(root)           # raises ThreadNotResolved if not converged
```

#### `DKSEngine.lower_guard`

```python
def lower_guard(self, root: str) -> None
```

The hand-off guard: assert the deliberation has **converged** before lowering. A thin delegation to [`require_resolved`](#require_resolved) using the engine's own `depth_cap` / `confidence_floor`. The engine never imports the LOWER module (the dependency is one-directional).

- **Raises** `ThreadNotResolved` (open frontier); `TrailStoreError` (unknown `root`, via `open_frontier`).

---

## `reasoning.inner_graph`

Source: [`../../src/concursus/reasoning/inner_graph.py`](../../src/concursus/reasoning/inner_graph.py)

The inner graph — parallel hypothesis-investigator dispatch plus `DIGEST` write-back to the `.2` worker-log lane. A **fresh, disposable** per-round projection of the open frontier that fans one injected investigator per hypothesis through a bounded thread pool and merges results **order-insensitively**, then digests each result to `.2` as an append-only ACTION marker plus a slipbox-card RESULT (with the raw payload offloaded to a `log_ref` file). Confined to `.2`; it **never** writes a `.3` verdict — that is the engine's job.

| Symbol | Kind | Summary |
|---|---|---|
| [`MAX_FANOUT_CAP`](#max_fanout_cap) | constant | *(opt-in, default-off.)* The hard, preference-independent fan-out cap (`64`); a soft ceiling can only tighten below it. |
| [`_cpu_capacity`](#_cpu_capacity) | function | *(opt-in, default-off.)* The host's usable fan-out capacity — `max(1, min(os.cpu_count(), MAX_FANOUT_CAP))` (module-private). |
| [`resolve_ceiling`](#resolve_ceiling) | function | *(opt-in, default-off.)* Clamp a preferred fan-out by a capacity — `max(1, min(pref, cap))`. |
| [`InnerGraphError`](#innergrapherror) | exception | Invalid inner-graph operation. Subclasses `ValueError`. |
| [`Investigator`](#investigator-inner-graph) | type alias | The per-hypothesis work seam (same contract as the engine). |
| [`InvestigationResult`](#investigationresult) | dataclass | One investigator's result over a single hypothesis. |
| [`InnerGraph`](#innergraph) | dataclass | A disposable per-round projection of the open frontier. |
| [`partition_frontier`](#partition_frontier) | function | Split a frontier into a bounded fan-out. |
| [`compile_inner_graph`](#compile_inner_graph) | function | Snapshot the current open frontier into a fresh `InnerGraph`. |
| [`dispatch_frontier`](#dispatch_frontier) | function | Run one investigator per open hypothesis, bounded + order-insensitive. |
| [`InnerGraphDigest`](#innergraphdigest) | class | DIGEST a result to the `.2` lane (idempotent, restart-safe). |

### `InnerGraphError`

```python
class InnerGraphError(ValueError)
```

Raised on an invalid inner-graph operation — a non-positive concurrency ceiling. (Its docstring also mentions a missing hypothesis, but in practice it is currently only raised by [`partition_frontier`](#partition_frontier) for `ceiling < 1`; a stale/missing id is silently skipped at dispatch.) Subclasses `ValueError`.

### `MAX_FANOUT_CAP`

```python
MAX_FANOUT_CAP = 64
```

*(Opt-in, default-off.)* The **hard, preference-independent** upper bound on concurrent investigators. A caller's soft `concurrency_ceiling` config can only **tighten** the effective fan-out below this cap — it can never raise the fan-out above it — and the CPU-derived capacity is bounded by it too, so neither a runaway ceiling nor a many-core host can explode the fan-out. It is kept well above the default ceiling `_DEFAULT_CEILING = 4`, so the **default dispatch path is byte-for-byte unchanged** — this cap only ever engages when a caller opts into a large `concurrency_ceiling` (or runs on a host with more than 64 usable cores). It bounds the fan-out at *compile* time (a partition width), not at runtime: `dispatch_frontier` still runs a single static pass — see the [compiler-not-governor invariant](#reasoninginner_graph).

### `_cpu_capacity`

```python
def _cpu_capacity() -> int
```

*(Opt-in, default-off; module-private.)* The host's usable fan-out capacity: `max(1, min(os.cpu_count() or 1, MAX_FANOUT_CAP))`. Always `>= 1` (a `None` `os.cpu_count()` degrades to `1`), and never exceeds [`MAX_FANOUT_CAP`](#max_fanout_cap). Because it is `>= 4` on any host with `>= 4` usable cores, the default ceiling of `4` is preserved unchanged there. Used only inside [`compile_inner_graph`](#compile_inner_graph) as the `cap` argument to [`resolve_ceiling`](#resolve_ceiling); it is a private helper (leading underscore), not part of the public surface.

### `resolve_ceiling`

```python
def resolve_ceiling(pref: int, cap: int) -> int
```

*(Opt-in, default-off.)* Clamp a caller's preferred fan-out `pref` by a capacity `cap` — `max(1, min(pref, cap))`. **Pure and preference-independent on the upper side:** the soft `pref` (a caller's `concurrency_ceiling`) can only **tighten** the effective fan-out below `cap`; it can never raise it above. The `max(1, …)` floor keeps the fan-out bounded and making progress even for a degenerate `pref` / `cap`. This clamp shape now floors **every** fan-out ceiling: [`compile_inner_graph`](#compile_inner_graph) computes `ceiling = resolve_ceiling(concurrency_ceiling, _cpu_capacity())`, so the partition width is always `min(pref, capacity)` and never exceeds the hard cap. **Defaults reproduce today exactly:** with the default ceiling `4` and any `cap >= 4` this returns `4`, so the default dispatch path is byte-for-byte unchanged — the soft config is opt-in and default-off in the sense that leaving `concurrency_ceiling=4` on a normal host changes nothing.

```python
from concursus.reasoning.inner_graph import resolve_ceiling, MAX_FANOUT_CAP

resolve_ceiling(4, 8)               # 4   — default ceiling on an 8-core cap, unchanged
resolve_ceiling(100, 8)             # 8   — a soft ask of 100 is TIGHTENED to the 8-core cap
resolve_ceiling(100, MAX_FANOUT_CAP)  # 64  — never exceeds the hard cap
resolve_ceiling(0, 8)               # 1   — the max(1, …) floor keeps fan-out making progress
```

### `Investigator` (inner graph)

```python
Investigator = Callable[[Hypothesis], object]
```

The per-hypothesis work seam — the **same** contract the DKS engine uses. Returns a verdict spec or a list of child candidates, and defaults to a deterministic stub. The inner graph never **applies** the outcome to `.3`; it only investigates and digests to `.2`.

### `InvestigationResult`

```python
@dataclass
class InvestigationResult:
    hypothesis_id: str
    ok: bool = True
    outcome: Optional[object] = None
    action: str = "investigate"
    error: Optional[str] = None
    worker: int = 0
    dedup_key: str = ""
    log_ref: Optional[str] = None
    card_ref: Optional[str] = None
    digested: bool = False

    def key(self) -> str
```

One investigator's result over a single open hypothesis. A **failure is a first-class result** with `ok=False` and an `error` string (`"<ExcType>: <msg>"`) — never a raised exception, so one worker's crash never aborts the fan-out or merge. `outcome` is the investigator's return on success (a verdict spec or child list), `None` on failure. `worker` is the `.2/<worker>` lane index assigned at dispatch; `log_ref` / `card_ref` / `digested` are set by the digest. `key()` returns the idempotency key: the explicit `dedup_key` if set, else the default `"<hypothesis_id>:<action>"`.

### `InnerGraph`

```python
@dataclass
class InnerGraph:
    root: str
    batches: List[List[str]]
    ceiling: int
    projection: Dict[str, Hypothesis] = field(default_factory=dict)

    @property
    def frontier(self) -> List[str]   # all batches concatenated, in partition order
    def __len__(self) -> int          # total hypotheses across all batches
```

A fresh, disposable per-round projection of the **open** frontier — **not** a cyclic executor. Rebuilt each round from the pre-commit mutable hypothesis set and thrown away after dispatch. It carries only a read snapshot (`projection`, an `{id: Hypothesis}` snapshot at compile time) plus the bounded fan-out `batches` (each `<= ceiling`); it holds **no** reference to the durable trail or committed plan, so it can never ossify into a cyclic executor. `root` is the `.3` root the projection was compiled from; `ceiling` is the fan-out clamp. `len(graph)` is the frontier size.

### `partition_frontier`

```python
def partition_frontier(frontier: Sequence[str], ceiling: int) -> List[List[str]]
```

Split an open `frontier` into a **bounded** fan-out — a list of batches each `<= ceiling` — so a later dispatch never runs more than `ceiling` investigators at once. Deterministic and order-preserving; an empty frontier yields `[]`.

- **Raises** `InnerGraphError` if `ceiling < 1`.

```python
from concursus.reasoning.inner_graph import partition_frontier

partition_frontier([".3/h1", ".3/h2", ".3/h3"], 2)  # [[".3/h1", ".3/h2"], [".3/h3"]]
```

### `compile_inner_graph`

```python
def compile_inner_graph(trail: HypothesisTrail, root: str, *,
                        concurrency_ceiling: int = 4, depth_cap: int = 5,
                        confidence_floor: float = 0.6) -> InnerGraph
```

Snapshot the **current** open frontier into a fresh, disposable `InnerGraph`. Reads `trail.open_frontier` (within the caps), captures a read-only `{id: Hypothesis}` projection (only frontier ids still in the model), and partitions the frontier into a bounded fan-out via [`partition_frontier`](#partition_frontier). Meant to be rebuilt every round and discarded; holds no reference to the trail or committed plan. Mutates nothing. (The default `concurrency_ceiling` is `4`.)

> *(Opt-in, default-off.)* The caller's soft `concurrency_ceiling` is clamped by the host CPU capacity via [`resolve_ceiling`](#resolve_ceiling) — the partition width is `ceiling = resolve_ceiling(concurrency_ceiling, _cpu_capacity())`, i.e. `max(1, min(concurrency_ceiling, capacity))` where `capacity` is itself capped at [`MAX_FANOUT_CAP`](#max_fanout_cap) (`64`). So soft config can only **tighten** the fan-out below the host's capacity, never explode it past the hard cap. This is a *compile-time* clamp on the partition width, not a runtime governor. The default ceiling of `4` is preserved unchanged on any host with `>= 4` usable cores (`cap >= 4`) — **the default path is byte-for-byte unchanged**; the clamp only bites when a caller opts into a larger `concurrency_ceiling`.

- **Raises** `InnerGraphError` (`concurrency_ceiling < 1`); `TrailStoreError` (unknown `root`, via `open_frontier`).

### `dispatch_frontier`

```python
def dispatch_frontier(graph: InnerGraph, investigator: Optional[Investigator] = None, *,
                      digest: Optional["InnerGraphDigest"] = None
                      ) -> Dict[str, InvestigationResult]
```

Run **one** investigator per open hypothesis, clamped to `graph.ceiling` via a per-batch `ThreadPoolExecutor` (sized `min(ceiling, len(batch))`), merged **order-insensitively** (keyed by hypothesis id, so completion order is irrelevant). A worker failure arrives as an `ok=False` result rather than a raised exception. When a `digest` is supplied, each result is written back to the `.2` lane **before** it is merged. `investigator` defaults to the deterministic stub. Returns `{id: result}`. The inner graph never applies an outcome to `.3`.

```python
from concursus.reasoning.inner_graph import (
    compile_inner_graph, dispatch_frontier, InnerGraphDigest,
)

graph = compile_inner_graph(trail, root, concurrency_ceiling=4)
digest = InnerGraphDigest(trail.branch_dir.parent)        # writes under run_dir/.2
results = dispatch_frontier(graph, my_investigator, digest=digest)  # {id: InvestigationResult}
```

> `write_back` returns the **same** result object mutated in place (`log_ref` / `card_ref` / `digested` set). On a dedup no-op `digested` stays `False` — check `digested` to know whether a write actually happened.

### `InnerGraphDigest`

```python
class InnerGraphDigest:
    def __init__(self, run_dir, *, lane: str = ".2") -> None
```

DIGEST an investigator result to the `.2` worker-log lane (capture-validate-fix). Per result it appends (1) an append-only ACTION marker to `.2/log_<k>.jsonl` (deduped on `node.id:action`) and (2) a slipbox-card RESULT note whose raw payload is **offloaded** to a `log_ref` file (never inlined). A same-`dedup_key` retry is an idempotent no-op that survives process restart. Confined to `.2`; never writes a `.3` verdict. Pure stdlib, atomic writes, thread-safe under an `RLock`.

> `run_dir` is the **run** directory — the digest appends `/lane` itself. Pass `trail.branch_dir.parent` (the run dir), not an already-`.2` path, or the lane double-nests.

| Member | Kind | Summary |
|---|---|---|
| [`lane_dir`](#innergraphdigestlane_dir) | property | The on-disk `.2` lane directory (`run_dir/lane`). |
| [`write_back`](#innergraphdigestwrite_back) | method | Digest one result (idempotent). |
| [`markers`](#innergraphdigestmarkers) | method | Every ACTION marker across all worker-log lanes. |
| [`seen_keys`](#innergraphdigestseen_keys) | method | The dedup keys already digested, sorted. |

#### `InnerGraphDigest.lane_dir`

```python
@property
def lane_dir(self) -> Path
```

The on-disk `.2` worker-log lane directory this digest writes to (`run_dir/lane`).

#### `InnerGraphDigest.write_back`

```python
def write_back(self, result: InvestigationResult) -> InvestigationResult
```

Digest **one** result to `.2`; a same-`dedup_key` retry is an idempotent no-op. On a new key it offloads the raw payload to `.2/raw/<slug>.json`, writes the slipbox-card RESULT to `.2/cards/<slug>.md` referencing it, and appends the ACTION marker to `.2/log_<worker>.jsonl` — all atomically — then sets `log_ref` / `card_ref` / `digested` on the returned result. On a dedup hit it returns the result untouched with `digested` still `False`. Idempotency is checked against an in-memory `_seen` set reloaded from the lane logs on first use, so it holds across process restarts. Never touches `.3`.

#### `InnerGraphDigest.markers`

```python
def markers(self) -> List[dict]
```

Every ACTION marker across all worker-log lanes, in `(worker, seq)` order (reads every `.2/log_*.jsonl` line). For inspection/assertions. Skips torn/partial JSON lines; returns `[]` if the lane dir does not exist.

#### `InnerGraphDigest.seen_keys`

```python
def seen_keys(self) -> List[str]
```

The dedup keys already digested (the idempotency set), sorted. Loads the dedup set from the lane logs first.

---

## `reasoning.deliberate`

Source: [`../../src/concursus/reasoning/deliberate.py`](../../src/concursus/reasoning/deliberate.py)

The top-of-tier plan-formation phase **in front of** the compiler. It ties the `.3` hypothesis trail, the bounded DKS engine, the disposable per-round inner graph, and a compile-time [precedent retriever](../../src/concursus/state/precedent.py) into one loop that **forms** a plan by deliberation and then **lowers** the converged conclusion into an immutable `AgentDAG`. All model/agent work enters through injected seams with deterministic-stub defaults; it never touches `Supervisor.run`.

The public API is `__all__ = ["seed", "lower_to_dag", "unroll_static_fanout", "form_plan", "Investigator"]` (with `Investigator = Callable[[Hypothesis], object]`, the same seam contract as the engine and inner graph). Defaults: `max_rounds=8`, `depth_cap=5`, `confidence_floor=0.6`, `reuse_threshold=0.6`.

### `seed`

```python
def seed(trail: HypothesisTrail, goal: str, *, retriever: Optional[object] = None,
         limit: int = 3, reuse_threshold: float = 0.6,
         confidence_floor: float = 0.6) -> List[str]
```

Seed the `.3` root for a `goal`, **reusing** a strong retrieved precedent (prune-not-append) instead of appending it. Returns the seeded root ids. A **new plan-formation episode**, triggered by a goal/ticket only — the `retriever` is a *priming read*, never itself the write trigger. Two modes:

- **Reuse (warm start):** when the retriever returns a precedent scoring `>= reuse_threshold` that carries a decomposition, seed a single goal root then fan the prior's steps as already-**confident** children (`confidence = max(confidence_floor, 0.6)`), so `open_frontier` immediately excludes them.
- **Cold / weak-precedent:** with no retriever or no qualifying precedent, seed a single `{"text": "Approach: <goal>", "confidence": 0.0}` root (byte-for-byte the pre-existing behavior).

- **Raises** `ValueError` on an empty/whitespace `goal`.

> The reuse path fans children at `confidence = max(confidence_floor, 0.6)` — passing a `confidence_floor` below `0.6` does **not** lower the reuse confidence; it floors at `0.6`.

```python
from concursus.reasoning.deliberate import seed

roots = seed(trail, "fix outage")                          # cold: single "Approach: ..." root at 0.0
roots = seed(trail, "fix outage", retriever=precedent_idx) # warm: reuses a >=0.6-scoring precedent
```

### `lower_to_dag`

```python
def lower_to_dag(trail: HypothesisTrail, root: str, *,
                 require_resolved_first: bool = True, depth_cap: int = 5,
                 confidence_floor: float = 0.6) -> AgentDAG
```

Lower a **converged** `.3` debate into an immutable [`AgentDAG`](core.md#agentdag) — a pure, no-LLM deterministic fold. The surviving **IN**-labelled hypotheses (from the Dung grounded extension) become the task decomposition (one node per accepted hypothesis, edged parent → child along the accepted sub-tree); **OUT** hypotheses are dropped as dead-ends. Accepted ids are sorted (materialized paths sort parents before children) for deterministic order, and a parent → child edge is added only when **both** endpoints are accepted and their node names differ. A degenerate debate that accepted nothing still yields a valid empty DAG. The result is `dag.validate()`-d (acyclic). When `require_resolved_first` is `True` (default) it calls [`require_resolved`](#require_resolved) first.

- **Raises** `ThreadNotResolved` (open frontier, when `require_resolved_first=True`); `TrailStoreError` (unknown `root`).

> With `require_resolved_first=False` this can silently produce an empty or partial DAG from a live debate — the guard is the only thing enforcing convergence. An IN child under an OUT (dropped) parent becomes a disconnected node.

### `unroll_static_fanout`

```python
def unroll_static_fanout(dag: AgentDAG,
                         unroll: Optional[Mapping[str, int]] = None) -> AgentDAG
```

*(Opt-in and default-off — one of the flexibility & robustness additions completed in v0.6.0.)* A **compile-time** virtualization pass that unrolls a **statically-bounded** fan-out into frozen parallel branches, entirely **before** [`OrchestrationAssembler.assemble`](assemble.md) freezes the plan. Given `unroll = {base_node: N}` (a **declared, data-independent** count `N`), each named `base` node is expanded — in this **one** compile pass — into `N` frozen parallel branches: the sub-node is cloned under namespaced ids `f"{base}__fe{i}"` (`i` in `0..N-1`), plus

- a **scatter** — every upstream producer of `base` fans its (shared) input to all `N` clones (a static shared-input scatter, *not* a runtime split); and
- a **gather** — a synthetic join node `f"{base}__gather"` that collects the `N` clone outputs, onto which every original downstream consumer of `base` is re-pointed.

The result is a **new** frozen `AgentDAG` whose [`validate()`](core.md#agentdagvalidate) passes, so the static [`Supervisor`](execute.md) runs the `N` branches + the gather in **one** pass over the frozen `plan.order` — **no runtime graph mutation, no dynamic split.** All dynamism is spent here, at compile time, before the plan is frozen.

Gating (the default path is byte-for-byte unchanged):

- `unroll` absent / empty → the input `dag` is returned **unchanged (same object)**, so a caller that never opts in gets a byte-identical plan.
- Only `N >= 2` unrolls; `N == 1` is a degenerate no-op (the base node is left in place).
- A `base` id not present in `dag`, or a non-`int` / `N < 1` count, raises [`DAGError`](core.md#dagerror) — a spec error caught **at compile**, never a silent mis-compile. Unbounded / data-dependent fan-out is out of scope; `N` must be a declared static bound.

This is the DAG-topology sibling of the inner-graph concurrency cap: [`MAX_FANOUT_CAP`](#max_fanout_cap) bounds how many investigators a *deliberation round* dispatches at once, while `unroll_static_fanout` bounds how wide a *frozen plan's* declared fan-out can be — both are static, opt-in bounds that keep the compiler-not-governor invariant intact. The same `f"{base}__fe{i}"` / `f"{base}__gather"` namespacing is echoed in the [`assemble`](assemble.md) docs.

- **Raises** `DAGError` (unknown base id, or a non-int / `N < 1` count).

```python
from concursus.reasoning.deliberate import lower_to_dag, unroll_static_fanout

dag = lower_to_dag(trail, root)
dag = unroll_static_fanout(dag)                 # no spec -> same object, byte-for-byte unchanged
wide = unroll_static_fanout(dag, {"probe": 3})  # 3 frozen branches probe__fe0..2 + probe__gather
```

### `form_plan`

```python
def form_plan(trail: HypothesisTrail, goal: str, *, retriever: Optional[object] = None,
              engine: Optional[DKSEngine] = None, investigator: Optional[Investigator] = None,
              max_rounds: int = 8, depth_cap: int = 5, confidence_floor: float = 0.6,
              concurrency_ceiling: int = 4, digest: Optional[InnerGraphDigest] = None
              ) -> AgentDAG
```

Form a plan by **bounded** deliberation, then **lower** it into a frozen `AgentDAG`. For each seeded root it runs the `SEED → READ FRONTIER → DISPATCH (inner graph) → DIGEST → VERDICT (DKS engine) → RE-READ` loop until the frontier empties or `max_rounds` is spent, then folds every converged root into one DAG. If no `engine` is supplied, one is built over `trail` with the given `investigator` and caps. Each root's outer merge-dispatch loop is itself capped at `max_rounds` passes so a pathological engine can never hang the driver. `dispatch_frontier` writes results to the `.2` lane (when a `digest` is supplied); `engine.run` writes the `.3` verdicts. Runs end-to-end with neither LangGraph nor an LLM (deterministic stubs).

- **Raises** `ValueError` (empty goal, via `seed`); `ThreadNotResolved` (a root failed to converge within `max_rounds`, via LOWER).

> If you inject your **own** `engine`, the `investigator` / `max_rounds` / `depth_cap` / `confidence_floor` args no longer drive the verdict phase (the engine's own config does) — though they still drive `seed` / `open_frontier` / the inner-graph fan-out.

```python
from concursus.reasoning.deliberate import form_plan, lower_to_dag

# Bounded deliberation -> frozen AgentDAG, ready for OrchestrationAssembler.assemble.
dag = form_plan(trail, "resolve ticket COMET-42", investigator=my_llm_fn)

# Or drive the phases yourself:
dag = lower_to_dag(trail, root)  # raises ThreadNotResolved if the debate is still open
```

---

## Invariants at a glance

- **Everything is plan-formation, strictly before `assemble`.** No symbol here dispatches an agent, runs an SOP, or is wired into `Supervisor.run`. Re-opening `.3` is a *new* episode, never a mid-flight mutation of a frozen plan.
- **Every loop is bounded and terminates.** `open_frontier` returning `[]` is the convergence check; `drive_deliberation`, `DKSEngine`, and `form_plan` each enforce a `max_rounds` budget (the pure-Python driver adds a hard structural step cap).
- **Fan-out is statically bounded, opt-in, and default-off** (the flexibility & robustness layer completed in v0.6.0). The per-round dispatch width is clamped by [`resolve_ceiling(pref, cap)`](#resolve_ceiling) = `max(1, min(pref, cap))` under the hard [`MAX_FANOUT_CAP`](#max_fanout_cap) (`64`): a soft `concurrency_ceiling` can only *tighten*, never exceed the cap. A frozen plan's declared fan-out widens only via the opt-in [`unroll_static_fanout`](#unroll_static_fanout) compile pass. Both are compile-time bounds — the default (ceiling `4`, no unroll spec) is **byte-for-byte unchanged**, and neither turns the compiler into a runtime governor.
- **Verdicts commit atomically.** `write_verdict` writes the verdict child and the resolved marker in one `os.replace`, so a scan never sees one without the other.
- **The `.3` and `.2` lanes are strictly separated.** Only the engine writes `.3` verdicts (via `trail.write_verdict`); the inner graph is confined to the `.2` worker-log lane and never writes a `.3` verdict.
- **Lowering is guarded.** You may only `lower_to_dag` a *converged* debate — `require_resolved` raises `ThreadNotResolved` otherwise.
- **Pure stdlib.** All four modules import and run end-to-end with neither LangGraph nor any LLM installed; the LangGraph backend (`DKSEngine`) is lazily imported and falls back to pure Python, and every model/agent worker is an injected seam.

## See also

- [Guide: The Reasoning Tier (DKS Deliberation)](../guides/reasoning.md) — the narrative SEED → form → LOWER walkthrough.
- [Guide: The Governor](../guides/governor.md) and the [`governor`](governor.md) reference — the strictly-**outer** runtime loop this tier is *not*.
- [`core`](core.md) — the [`AgentDAG`](core.md#agentdag) that `lower_to_dag` produces.
- [`assemble`](assemble.md) — `OrchestrationAssembler.assemble`, which freezes the lowered DAG into a plan.
- [`execute`](execute.md) — the `Supervisor` forward pass this tier is never wired into.
- [`state`](state.md) — the `FileVaultStateStore` addressing `HypothesisTrail.from_config` reuses and the precedent retriever `seed` primes from.
- [Compiling & Running a Team](../guides/compiling-and-running.md) and the [documentation index](../README.md).
