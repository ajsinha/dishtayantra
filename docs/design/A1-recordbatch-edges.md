# A1 — Arrow `RecordBatch` on Edges (Transport) — Design

Status: **implemented in v5.1.0.** This was the last A1 increment: remove
the per-stage copy of batch data so the measured ~11.8× vectorization kernel
speedup actually reaches the end-to-end pipeline. It is the most invasive A1
change because it touches the shared value handling in `core/dag/graph_elements.py`,
so the bulk of this document is about keeping it **backward compatible**.

**Result (measured, `perftest/run_arrow_transport_example.py`, 20k trades):** the
per-trade output is byte-for-byte identical to the dict path (0 mismatches, 0.0
error), and throughput is **~2.29× the v4.5.0 dict-envelope path** by removing the
per-stage deep-copies. The remaining gap to the ~11.8× kernel ceiling is per-tick
node-loop overhead, not data copying. Delivered as `core/dag/edge_value.py`, the
minimal `graph_elements.py` substitution below, the `ArrowCalculator` RecordBatch
fast-path, the opt-in `ArrowBatchingSubscriptionNode` / `ArrowFlatteningPublicationNode`
nodes, and `core/transformer/arrow_transformer.py` (§5.1). See
`docs/TUTORIAL_arrow.md` for hands-on usage.

---

## 1. Where the cost is today (grounded in the code)

In `core/dag/graph_elements.py` every value that flows between nodes is copied,
and the copies dominate once a "value" is a 500-row batch:

- `Edge.get_data()` (line 206) calls `self.from_node.output()`, and `Node.output()`
  (line 40) returns `copy.deepcopy(self._output)`. **One deep-copy per outgoing
  edge read, every tick.**
- `Node.compute()` deep-copies the input (line 128, `self._input = copy.deepcopy(...)`)
  and the output (line 144, `self._output = copy.deepcopy(...)`).
- `consolidate_edge_inputs()` merges with `dict.update` (line 102) — assumes dicts.
- The equality gate uses `==` / `!=` (lines 124, 143).

With the `{"batch":[...]}` envelope, each of those deep-copies clones every row
dict in the batch. That is why source-batching (v4.5.0) only reached ~1.06–1.8×
even though the kernel is ~11.8×: the engine copies the batch 3–4 times per stage.

## 2. The enabling insight

An Arrow `RecordBatch` is **immutable** (verified: no in-place mutation API, not
hashable; columns are immutable Arrow arrays). The *only reason* the engine
deep-copies dict values is that dicts are mutable — a downstream calculator could
mutate a shared input and corrupt upstream state. That risk **does not exist** for
a `RecordBatch`: it cannot be mutated, and Arrow calculators are functional
(`append_columns` etc. return *new* batches). Therefore a `RecordBatch` can be
shared by reference across edges with **zero copies** and remain correct.

So the plan is: carry the data as a `RecordBatch` on the edges between Arrow
stages, convert dict→batch **once** at ingress and batch→dict **once** at egress,
and skip the deep-copies in between.

## 3. The value-type abstraction (new module `core/dag/edge_value.py`)

The engine performs exactly three operations on the values it moves: **copy**,
**compare** (the equality gate), and **consolidate** (merge incoming edges). We
introduce type-dispatched helpers and have the engine call those instead of the
hard-coded `copy.deepcopy` / `==` / `dict.update`:

```python
# core/dag/edge_value.py  (pyarrow imported lazily; core stays pyarrow-optional)
def is_batch(v):           # True only for a pyarrow.RecordBatch
def ev_copy(v):            # RecordBatch -> v (immutable, no copy); else copy.deepcopy(v)
def ev_equals(a, b):       # both batch -> a.equals(b); mixed types -> False; else a == b
def ev_consolidate(pairs): # list of (pname, edge_data) -> merged value
def ev_describe(v):        # JSON-safe summary for details()/UI (batch -> {rows, schema})
```

Crucially, **for any non-batch value these reduce to the current behaviour
exactly**: `ev_copy(d) == copy.deepcopy(d)`, `ev_equals(a,b) == (a==b)`, and
`ev_consolidate` runs the existing `dict.update` logic. If pyarrow is not
installed, `is_batch` is always `False` and every path is the dict path.

## 4. Exact engine changes (`graph_elements.py`)

Minimal, mechanical substitutions — the diff is meant to be trivially reviewable:

| line | today | becomes |
|------|-------|---------|
| 38/43 | `copy.deepcopy(self._input/_output)` | `ev_copy(self._input/_output)` |
| 102 | `merged_input.update(edge_data)` | handled by `ev_consolidate` |
| 124 | `if transformed_input == self._input:` | `if ev_equals(transformed_input, self._input):` |
| 128 | `self._input = copy.deepcopy(transformed_input)` | `self._input = ev_copy(transformed_input)` |
| 143 | `if transformed_output != self._output:` | `if not ev_equals(transformed_output, self._output):` |
| 144 | `self._output = copy.deepcopy(transformed_output)` | `self._output = ev_copy(transformed_output)` |
| 178/179 | `'input': self._input, 'output': self._output` | `ev_describe(...)` (so the stats UI can JSON-serialize a batch) |

`consolidate_edge_inputs()` becomes batch-aware: a single incoming edge carrying a
`RecordBatch` returns that batch as-is; dict edges merge exactly as today. It uses
`edge_data is not None` rather than truthiness (a `RecordBatch` has no well-defined
boolean and `dict.update(batch)` would fail).

## 5. New opt-in nodes / calculator path (additive — existing classes untouched)

The `RecordBatch` only ever appears on an edge if a node *emits* one, and only the
new opt-in pieces do:

- **`ArrowBatchingSubscriptionNode(BatchingSubscriptionNode)`** — drains messages
  (as today) but builds a `pa.RecordBatch` via `from_pylist` and sets that as
  `_output`, instead of the `{"batch":[...]}` dict envelope. Selected by node
  `type`, or by a `{"transport": "arrow"}` config on the batching node.
- **`ArrowCalculator.calculate()`** gains one branch: if the input *is* a
  `RecordBatch`, call `calculate_batch` directly and return a `RecordBatch` — no
  dict↔Arrow conversion at all. The existing single-dict and `{"batch":[...]}`
  envelope branches are unchanged.
- **`ArrowFlatteningPublicationNode(FlatteningPublicationNode)`** — converts the
  `RecordBatch` back to per-row dicts (`to_pylist`) and publishes each, preserving
  the per-message external contract.

Data flow: **dict at ingress → `from_pylist` once → RecordBatch through every
Arrow stage (no copies, no conversions) → `to_pylist` once at egress → dicts out.**
The conversion happens twice total, not per stage — that is the whole win.

## 5.1 Edge transformers — the telescopic view, in Arrow

A defining strength of the edge is its per-edge `data_transformer`:
`Edge.get_data()` returns `self.data_transformer.transform(self.from_node.output())`,
so each downstream consumer sees the source node's state through *its own* lens —
a projection, rename, filter, namespacing (`pname`), or derived view. The same
source output is adapted per-consumer, and the source knows nothing about its
consumers. This telescopic-view property is **preserved and made cheaper** under
Arrow, not lost:

- A view over a `RecordBatch` is a columnar projection, and the focusing
  operations are **zero-copy** (verified): `batch.select([...])` (pick columns),
  `batch.slice(off, len)` (row window), and `batch.rename_columns(...)` all share
  the source's buffers, so the projected batch carries no data copy.
  `batch.filter(mask)` and vectorized derivations add focused/derived views
  cheaply. The telescopic lens becomes a zero-copy columnar lens.
- We add an **Arrow-aware transformer** contract — `transform_batch(RecordBatch)
  -> RecordBatch`, mirroring `calculate_batch`. On a batch edge, if the
  transformer implements it, `Edge.get_data` calls it and returns a buffer-sharing
  projected batch.
- **Existing row transformers are untouched** (the dict path). On a batch edge a
  row transformer is bridged by a `RowTransformerBatchAdapter` (per-row,
  correctness over speed — the same pattern as `RowCalculatorBatchAdapter`), or
  the segment opts into `transform_batch` for the zero-copy path.
- The edge transformer is also the natural **boundary adapter**: if one consumer
  wants the Arrow batch and another wants a row/dict view of the *same* source,
  the second edge's transformer performs the `to_pylist`/aggregate conversion for
  that edge only. So the per-edge lens doubles as the per-consumer batch↔row
  bridge.

`pname` namespacing and multi-input consolidation of *batches* (fan-in) remain the
fan-in case scoped out of v1 (§8, fail-fast); the single-edge telescopic
projection — the common case — is fully supported, and `ev_consolidate` keeps the
existing dict `pname` behaviour unchanged.

## 6. The equality-gate invariant is preserved (not bypassed)

The unconditional equality gate stays unconditional. `ev_equals` implements *true
value equality* for batches via `RecordBatch.equals()`, so a node still
short-circuits when its input is unchanged. We are not adding a streaming/bypass
mode; we are only teaching the existing comparison how to compare a new value
type. (Cost: `equals()` is O(rows) — but it replaces 2–3 O(rows) deep-copies, so
it is still a large net reduction, and only on the opt-in path.)

## 7. Backward compatibility — the guarantees

1. **The dict path is behaviourally identical.** Every `ev_*` helper is defined to
   perform the exact current operation for non-batch values, so the engine
   substitution is a no-op for every existing DAG.
2. **No `RecordBatch` appears unless opted in.** Existing DAGs use existing node
   types, which only ever set dict outputs, so only the dict branch of each helper
   runs. The new behaviour is reachable only by choosing the new node types /
   `transport: "arrow"`.
3. **The equality-gate invariant is honoured**, not circumvented (§6).
4. **pyarrow remains optional for the core.** `edge_value` imports it lazily; with
   pyarrow absent, `is_batch` is always `False` and the engine is pure-dict.
5. **Fail-fast, no silent defaulting** for topologies the first version does not
   support: fan-in of multiple `RecordBatch` edges into one node, and row-style
   transformers on a batch edge, raise a clear error rather than mis-handling.
6. **Proven by:** the full existing suite (166 passed / 1 skipped after this work;
   the dict DAGs were green before and stayed green); the `graph_elements.py` diff is the minimal
   mechanical substitution above; the new node/calculator code is *appended*
   (existing classes byte-identical, as in v4.5.0); and an equivalence run of an
   all-dict DAG against a prior release zip must match byte-for-byte.

## 8. Scope of the first version

In scope: linear Arrow segments (batching source → Arrow calculators → flattening
sink), zero-copy `RecordBatch` transport between them, native `calculate_batch`
with no per-stage conversion.

Out of scope (v1, fail-fast): `RecordBatch` fan-in/fan-out joins, row transformers
on batch edges, and zero-copy into the C++/Rust/Java calculators (that is the
follow-on — the Arrow C Data Interface handoff, which this transport sets up).

## 9. Risks & mitigations

- **`details()` / stats UI** can't JSON-serialize a `RecordBatch` → `ev_describe`
  returns `{rows, schema, "__arrow_batch__": true}` for display.
- **Equality O(rows)** per compare → acceptable (replaces larger copy cost; opt-in
  only); can later add a cheap content hash/sequence id if needed.
- **Truthiness** of a batch is undefined → consolidate uses `is not None`.
- **Most invasive change so far** → kept to ~6 mechanical lines behind the helpers,
  default-off, full-suite-gated.

## 10. Expected payoff

Removing the per-stage copies should move the auto-batch path from ~1.06–1.8×
toward the ~11.8× kernel ceiling (the remaining gap is the per-tick node-loop
overhead, not data copying). It also lays the zero-copy `RecordBatch` foundation
for the native polyglot handoff (Arrow C Data Interface) and for A2 (DataFusion /
streaming SQL), both of which want Arrow already on the edges.

Related: `docs/design/A1-arrow-data-plane.md`, `docs/ROADMAP.md`,
`docs/TUTORIAL_arrow.md`.
