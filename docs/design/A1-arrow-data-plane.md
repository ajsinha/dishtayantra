# RFC: A1 — Arrow Columnar Data Plane

*Roadmap Phase 1, step A1 (the keystone). See `docs/ROADMAP.md`.*
Status: **Draft for review** · Owner: TBD · Depends on: Phase 0 (benchmark harness) · Enables: A2 (embed DataFusion), C1 (incremental views), C2 (WASM calculators)

---

## 1. Summary

Make Apache Arrow `RecordBatch` the in-memory data format that flows through the
DAG, and process data **column-at-a-time in micro-batches** instead of
**row-at-a-time per message**. Keep today's row-based calculators working
unchanged; add an opt-in batch contract for hot paths.

**Why now:** a controlled spike on this codebase's own trade-ETL numerics
(`benchmarks/arrow_vectorization_spike.py`) shows the same computation runs
**~11.8× faster** as vectorized Arrow kernels than as the current row loop,
with **bit-identical output** (max relative error `0.00` over 20,000 rows):

| path | throughput |
|------|-----------|
| row-at-a-time (today's model) | ~1.54 M rows/s |
| Arrow vectorized (A1) | ~18.1 M rows/s |

This is the single change that makes "single node + large data + real-time"
credible, and it is the foundation A2/C1/C2 build on.

---

## 2. Problem

Today the engine processes one Python `dict` per message. The calculator
contract is row-shaped (`core/calculator/core_calculator.py`):

```python
class DataCalculator(ABC):
    def __init__(self, name, config): ...
    def calculate(self, data) -> dict: ...   # one message at a time
```

and `CalculationNode.compute()` invokes it per message
(`core/dag/node_implementations.py:260`, also `core/dag/graph_elements.py:133`):

```python
self._output = self._calculator.calculate(self._input)   # self._input is one dict
```

Consequences:
- **Interpreter overhead dominates.** Per-row Python pays attribute lookups,
  boxing, and dict copies on every field of every message. Vectorized kernels
  amortize that across a whole column.
- **Polyglot calculators pay a marshaling tax.** Java (Py4J), C++ (pybind11),
  and Rust calculators must serialize/convert each message across the language
  boundary. Arrow's columnar buffers can be shared **zero-copy** via the Arrow
  C Data Interface instead.
- **No columnar substrate** means A2 (DataFusion/streaming SQL), C1 (incremental
  views), and C2 (WASM calculators) have nothing efficient to build on.

## 3. Goals / non-goals

**Goals**
- Arrow `RecordBatch` as the edge format on opt-in DAG paths.
- An `ArrowCalculator` batch contract that coexists with `DataCalculator`.
- Zero-copy handoff to C++/Rust/Java calculators via the Arrow C Data Interface.
- Measured throughput uplift on a real DAG path, latency held within budget.

**Non-goals (for A1)**
- Rewriting every calculator (row calculators keep working via a shim).
- Changing delivery semantics, HA, or scheduling.
- Distributed execution (that is D1, and it builds on B2, not A1).
- Streaming SQL (that is A2, which consumes A1's output).

## 4. Proposed design

### 4.1 New opt-in contract (additive)
Add alongside the existing row contract — nothing existing changes:

```python
import pyarrow as pa

class ArrowCalculator(DataCalculator):
    """Process a columnar micro-batch. Engine calls this when the node is
    in Arrow mode; otherwise the row `calculate()` path is used."""
    def calculate_batch(self, batch: "pa.RecordBatch") -> "pa.RecordBatch": ...
```

### 4.2 Micro-batching at the node boundary
`CalculationNode` (Arrow mode) accumulates incoming messages into a
`RecordBatch` and flushes when **either** bound trips:
- `arrow.max_batch_size` (e.g. 10k rows), or
- `arrow.max_linger_ms` (e.g. 5 ms) — the latency cap.

This is the classic throughput/latency knob (like a producer's batch+linger).
The linger cap is what keeps the real-time character: a partially-filled batch
flushes on a timer so a quiet stream never stalls.

### 4.3 Zero-copy polyglot handoff
For C++/Rust/Java `ArrowCalculator`s, pass the batch across the language
boundary using the **Arrow C Data Interface** (no serialization, no copy). This
turns the existing `core/cpp`, `core/rust`, `core/jvm` bridges from
"marshal each message" into "share the same buffers."

### 4.4 Backward compatibility (critical)
- **Row calculators are untouched.** A node stays in row mode by default.
- **Shim both directions** so mixed DAGs work:
  - *row calculator on an Arrow edge:* a `RowAdapter` iterates the batch and
    calls `calculate()` per row (correct, not fast — a migration stepping stone).
  - *Arrow calculator on a row edge:* wrap single messages as a 1-row batch.
- **Edge format negotiation:** each edge is `row` or `arrow`; the builder inserts
  adapters where formats differ, so a DAG can be migrated one node at a time.

### 4.5 The broker boundary
Brokers (and `InMemoryPubSub`) currently carry JSON strings. Two honest options,
decided during the vertical slice:
1. **Node-boundary accumulation (start here):** keep the wire format as-is;
   build the `RecordBatch` *inside* the node from decoded messages. Lowest risk;
   captures the compute speedup; defers transport changes.
   **(IMPLEMENTED)** via the opt-in `BatchingSubscriptionNode` (drains messages
   into one envelope) and `FlatteningPublicationNode` (republishes per record);
   `SubscriptionNode`/`PublicationNode` are unchanged. See
   `perftest/run_autobatch_example.py`. The remaining cap is the dataflow's
   per-stage envelope deep-copy — addressed by option 2.
2. **Arrow IPC transport (later):** carry Arrow IPC/Flight between nodes/processes
   for end-to-end zero-copy. Bigger change; do it once option 1 proves out.

## 5. Vertical slice (what to build first)

One DAG path, behind a feature flag, measured with the Phase-0 harness:

1. Add `ArrowCalculator` + the `RowAdapter` shim.
2. Re-express **two numeric stages** of the linear trade-ETL workload
   (`benchmarks/workloads.py`) — e.g. FX-convert and notional — as
   `ArrowCalculator`s using `pyarrow.compute` (the spike already contains the
   exact kernels).
3. Add `arrow: {enabled, max_batch_size, max_linger_ms}` to the node config
   schema; default off.
4. Run the **same** DAG in row mode and Arrow mode through
   `benchmarks/run_benchmark.py`; assert identical sink output and record the
   throughput/latency delta.

**Acceptance:** identical outputs row-vs-Arrow; measured throughput uplift on the
path; p99 latency within the linger budget.

## 6. Evidence & honest caveats

The spike (§1) isolates *only* the row-vs-columnar compute variable and shows
~11.8× with identical results. Caveats to set expectations:
- It excludes broker decode, batch accumulation, and the linger wait. **End-to-end
  uplift on the live engine will be lower** and must be measured by the vertical
  slice, not extrapolated from the microbenchmark.
- Vectorization helps **numeric/columnar** work most. String-heavy
  validation/normalization benefits less; those stages may stay row-based
  initially.
- Tiny batches erase the win (kernel setup cost) — hence `max_batch_size`.

## 7. Engine touch points

| Area | File(s) | Change |
|------|---------|--------|
| Calculator contract | `core/calculator/core_calculator.py` | add `ArrowCalculator` |
| Node hot path | `core/dag/node_implementations.py`, `core/dag/graph_elements.py` | batch accumulate + flush; call `calculate_batch` in Arrow mode |
| Graph build | `core/dag/compute_graph_builders.py` | edge format negotiation, insert adapters |
| Polyglot bridges | `core/cpp`, `core/rust`, `core/jvm` | Arrow C Data Interface handoff |
| Config schema | node config | `arrow.{enabled,max_batch_size,max_linger_ms}` |
| Transport (phase 2) | `core/pubsub/*` | optional Arrow IPC/Flight |

## 8. Risks & mitigations

| Risk | Mitigation |
|------|-----------|
| Batching adds latency | `max_linger_ms` cap; flush on idle; default small |
| Memory growth from large batches | bound batch size; reuse buffers; backpressure (D2) |
| Heterogeneous/loosely-typed messages need a schema | declare a schema per Arrow edge; fall back to row mode for un-schematizable nodes |
| Mixed row/Arrow DAG correctness | adapters + a row-vs-Arrow equivalence test in CI (the spike already does this) |
| pyarrow + free-threading (A3) interplay | covered by `benchmarks/freethreading_spike.py` extension inventory; verify cp314t wheels |
| Scope creep into A2/transport | A1 stops at node-boundary accumulation + compute; transport and SQL are separate steps |

## 9. Plan within A1 & success metrics

1. Contract + shim + config flag (+ keep CI green).
2. Two Arrow stages on the trade-ETL path; row-vs-Arrow equivalence test.
3. Measure with the harness; tune batch/linger; document the real uplift.
4. Zero-copy C++/Rust handoff for one native calculator.

**Success:** measurable end-to-end throughput uplift on the slice with identical
outputs and p99 latency within the linger budget; a documented, repeatable
methodology for migrating further nodes.

## 10. Open questions

- Default `max_batch_size` / `max_linger_ms` per workload class?
- Per-edge declared schema vs. inferred — how strict at build time?
- Do we expose Arrow mode in the DAG designer UI now, or keep it config-only
  until the slice is proven?
