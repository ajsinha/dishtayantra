# Tutorial: Extremely High-Performance DAGs with Arrow RecordBatch

This is a **deep-dive** tutorial on getting the absolute most throughput out of a
DishtaYantra pipeline using Apache Arrow and zero-copy `RecordBatch` transport. It
covers the *theory* (why the fast path is fast), the *design* (how DishtaYantra
makes it safe), the *calculators* and *DAG* you actually write, and a closing
*discussion* of when this pays off, how to tune it, and where the ceiling is.

It is written so a newcomer can follow it end to end — every concept is explained
in plain language before it is used — while still giving an experienced engineer
the design detail they want. If you have never written an Arrow calculator at all,
skim `docs/TUTORIAL_arrow_dag.md` first (the gentle how-to); this tutorial is the
performance-focused companion to it.

Runnable proof of everything here: `python -m perftest.run_arrow_transport_example`.

---

## Part 0 — The big picture, in plain English

Suppose you must process **millions of trades** every evening: value each one,
compute fees, score its risk. A naive program loops over the trades one at a time.
That works, but at high volume the *overhead around each record* — not the actual
math — becomes the bottleneck. Going "extremely high performance" means
**removing that per-record overhead** so the machine spends its time on the
arithmetic you actually care about.

There are three independent ideas that stack together to do this, and the rest of
the tutorial is really just these three explained properly:

1. **Work in bulk (batching).** Handle 500 trades as one unit instead of 500
   separate visits through the machinery. Fewer trips = less overhead.
2. **Do the math column-at-a-time (vectorization with Arrow).** Instead of looping
   "multiply price × quantity" 500 times, multiply the entire *price column* by the
   entire *quantity column* in one hardware-accelerated sweep.
3. **Stop copying the data between steps (zero-copy transport).** Move the batch
   from one pipeline step to the next *by reference* — handing over a pointer, not
   photocopying the whole stack of papers each time.

Each idea is a multiplier. Together they take a pipeline from "fine" to "a single
machine doing work that people assume needs a cluster." Let's understand each one.

---

## Part 1 — Theory: where does the time actually go?

To make something fast, first find where the time goes. In a naive row-at-a-time
pipeline with several stages, the cost per record breaks down roughly as:

- **the useful math** (multiply, add, compare) — usually tiny per record;
- **per-record bookkeeping** — function calls, dictionary lookups, the engine
  waking a step up, checking whether anything changed, and so on;
- **copying** — and this is the silent giant.

### 1.1 Why copying dominates

DishtaYantra keeps each step isolated: a step gets its *own* copy of its input so
that one step can never accidentally corrupt another step's data. Historically the
value flowing between steps was a Python dictionary, and dictionaries are
**mutable** (editable). To stay safe, the engine made a **deep copy** (a complete,
recursive duplicate) of the data on every edge read and every computation. For a
single small dict that is cheap. But once a "value" is a batch of 500 trade dicts,
each deep copy clones all 500 — and the engine does this several times per stage.
In a multi-stage pipeline you can copy the same data a dozen times before it ever
gets used. **That copying, not the math, is what caps throughput.**

This is the key realization: at high volume, *the enemy is data movement, not
computation.* Every one of the three ideas below attacks data movement.

### 1.2 Why columns beat rows (vectorization)

Computers are fastest when they do the *same operation* to a *contiguous run* of
numbers. Modern CPUs have SIMD instructions ("Single Instruction, Multiple Data")
that multiply, say, eight numbers in one step, and CPU caches that reward reading
memory in straight lines.

- **Row layout** stores trade 1's fields, then trade 2's fields, ... So the prices
  are scattered through memory, interleaved with everything else. To multiply all
  prices you hop around memory and loop in Python — slow.
- **Column layout** stores *all the prices together*, then *all the quantities
  together*. Now "multiply every price by its quantity" is one tight, cache-friendly,
  SIMD-accelerated pass over two contiguous arrays.

That is **vectorization**: one operation over a whole column. The classroom
analogy: telling thirty students "multiply your two numbers, go" finishes far
sooner than walking desk to desk doing each one yourself — even though the total
arithmetic is identical.

### 1.3 What Apache Arrow gives us

**Apache Arrow** is an open standard for laying out tables *by column* in memory,
designed precisely for this kind of fast, vectorized processing, and shared across
many tools (pandas, Spark, DuckDB, DataFusion, ...). Its Python library is
`pyarrow`, and `pyarrow.compute` provides the vectorized column operations.

A **RecordBatch** is Arrow's unit of "a chunk of a columnar table" — picture a few
hundred trades held column-by-column, ready to crunch. Two properties of a
RecordBatch matter enormously for us:

- it is **columnar**, so vectorized math is fast; and
- it is **immutable** — once built it cannot be changed in place. Operations like
  "add a column" return a *new* batch; they never edit the original.

That immutability is the hinge the whole high-performance design turns on, as we'll
see next.

---

## Part 2 — Design: how DishtaYantra makes the fast path safe

Speed is easy if you abandon safety. The interesting part of the design is keeping
DishtaYantra's guarantees while removing the cost.

### 2.1 The immutability insight (why zero-copy is safe)

Recall from §1.1 that the engine deep-copies values *only because dicts are
mutable* — a downstream step might edit shared data and corrupt an upstream step.
Now ask: what if the value is a **RecordBatch**, which *cannot* be mutated?

Then the reason for copying evaporates. Two steps can share the exact same
RecordBatch by reference with **zero copies**, and neither can corrupt it because
neither can change it. This is the single most important design idea in this
tutorial:

> An immutable value can be shared, not copied. Removing the per-stage deep-copy is
> not a risky optimization — it is *correct by construction* once the value is
> immutable.

### 2.2 Teaching the engine to handle two value types

DishtaYantra does exactly three things to values as they flow: it **copies** them
(for isolation), **compares** them (more on that next), and **consolidates** them
(merges inputs from multiple incoming edges). Rather than hard-code these for
dicts, the engine routes them through small type-aware helpers
(`core/dag/edge_value.py`):

- **copy** — a dict is deep-copied (exactly as before); a RecordBatch is returned
  as-is (shared by reference, zero copy).
- **compare** — two dicts compare with `==` (as before); two RecordBatches compare
  by content with Arrow's `.equals()`.
- **consolidate** — dict edges merge as before; a single RecordBatch edge passes
  through untouched.

The beautiful consequence: **for an ordinary dict pipeline, every helper behaves
exactly as it always did** — byte-for-byte identical, fully backward compatible.
The fast path is purely additive. (If `pyarrow` isn't even installed, the
RecordBatch branch is never taken and the engine is pure-dict.)

### 2.3 Preserving the equality gate (correctness, not bypass)

DishtaYantra has a core rule: a step only recomputes when its input actually
**changes** (the "equality gate"). This is what makes it a dependency-driven engine
rather than a busy-loop. A tempting but *wrong* way to go fast would be to switch
this off ("streaming mode") — that would silently change the engine's semantics.

The high-performance path does **not** do that. It keeps the gate, and simply
teaches the comparison how to compare the new value type: two batches are compared
with `RecordBatch.equals()`. The gate stays unconditional; we only widened what it
understands. (Cost note: comparing N rows is O(N) — but it *replaces* two or three
O(N) deep-copies, so it is still a large net win, and only on the opt-in path.)

### 2.4 The layered speedups (how the pieces stack)

It helps to see the progression, slowest to fastest:

1. **Row-at-a-time** — one dict per step. Maximum overhead. The baseline.
2. **Batch envelope** — gather many rows into one `{"batch": [...]}` message so
   Arrow calculators can vectorize. The math gets fast, *but* the engine still
   deep-copies the (now large) envelope at every stage, so the copy cost grows.
3. **Zero-copy RecordBatch transport** — carry the immutable batch itself on the
   edges. The per-stage copies vanish. Convert dict→batch once at the start and
   batch→dict once at the end, never in between.
4. **(Future) Polyglot zero-copy** — hand the same Arrow buffers to C++/Rust/Java
   calculators with no serialization at all (the Arrow C Data Interface). This
   transport step is what sets that up.

Each layer removes a different chunk of data movement. This tutorial gets you
through layer 3, which is where a single machine starts to feel impossibly fast.

---

## Part 3 — The calculators

A high-performance calculator subclasses `ArrowCalculator` and implements
`calculate_batch`, which receives a whole `RecordBatch` and returns a new one with
extra columns. The rules of the game for speed:

- **Operate on columns, never loop rows.** Use `pyarrow.compute` (`pc`) kernels.
- **Append columns; don't rebuild.** Use the `append_columns` helper so the input
  columns survive and you only add new ones.
- **Never call `to_pylist()` mid-pipeline.** That converts the batch back to row
  dicts — the exact copy you are trying to avoid. Stay columnar end to end.
- **Match row rounding for exact parity.** Use the provided `py_round_array` so the
  vectorized result is byte-identical to the row calculator's.

Here are two stages of a trade-valuation pipeline (the shipped versions are in
`perftest/arrow_etl_calculators.py`):

```python
import pyarrow as pa
import pyarrow.compute as pc
from core.calculator.arrow_calculator import ArrowCalculator, append_columns, py_round_array

class ArrowFxConvertCalculator(ArrowCalculator):
    """Convert each trade's price to USD using a currency -> rate lookup.
    Vectorized: works on the whole 'price' and 'currency' columns at once."""
    def calculate_batch(self, batch):
        price = pc.cast(batch.column("price"), pa.float64())
        # look up an FX rate per row by matching the 'currency' column to a table
        idx  = pc.index_in(batch.column("currency"), value_set=pa.array(["USD","EUR","GBP"]))
        rate = pc.fill_null(pa.array([1.0,1.08,1.27], pa.float64()).take(idx), 1.0)
        price_usd = py_round_array(pc.multiply(price, rate), 6)   # all rows at once
        return append_columns(batch, {"fx_rate_to_usd": rate, "price_usd": price_usd})

class ArrowNotionalCalculator(ArrowCalculator):
    """Value = quantity * price_usd, computed for the entire column in one sweep."""
    def calculate_batch(self, batch):
        qty = pc.cast(batch.column("quantity"), pa.float64())
        px  = pc.cast(batch.column("price_usd"), pa.float64())
        notional = pc.multiply(qty, px)
        return append_columns(batch, {"notional_usd": py_round_array(notional, 2)})
```

Notice there is **no Python loop over trades** anywhere — every line operates on a
full column. That is the whole source of the speed.

Two compatibility notes that make adoption painless:

- The same class also works on the slow paths. `ArrowCalculator.calculate` handles a
  single dict (as a 1-row batch) and a `{"batch": [...]}` envelope too, so your
  calculator is a drop-in even where the fast transport isn't used.
- If you have a *legacy row calculator* you can't rewrite, wrap it with
  `RowCalculatorBatchAdapter` to run it (per row) inside the batched path — correct,
  though without the vectorization win.

---

## Part 4 — The DAG: a complete maximum-throughput pipeline

You assemble the fast path by choosing two opt-in node types at the ends of an
Arrow segment; everything between them is ordinary `CalculationNode`s running your
Arrow calculators:

- `ArrowBatchingSubscriptionNode` — drains incoming per-message trades and emits one
  **RecordBatch** (dict → batch, *once*, at ingress).
- `ArrowFlatteningPublicationNode` — converts the final RecordBatch back to per-row
  messages (batch → dict, *once*, at egress), so the outside world still sees one
  message per trade.

Here is the complete configuration (the shipped file is
`perftest/perftest_arrow_transport.json`):

```json
{
  "name": "high_perf_trade_etl",
  "subscribers": [{"name": "in",  "config": {"source": "mem://queue/in",  "max_depth": 2000000}}],
  "publishers":  [{"name": "out", "config": {"destination": "mem://queue/out"}}],

  "calculators": [
    {"name": "fx",       "type": "perftest.arrow_etl_calculators.ArrowFxConvertCalculator", "config": {}},
    {"name": "notional", "type": "perftest.arrow_etl_calculators.ArrowNotionalCalculator",  "config": {}},
    {"name": "fee",      "type": "perftest.arrow_etl_calculators.ArrowFeeCalculator",       "config": {}}
  ],

  "nodes": [
    {"name": "ingest",        "type": "ArrowBatchingSubscriptionNode",  "config": {"batch": {"max_size": 500}}, "subscriber": "in"},
    {"name": "fx_node",       "type": "CalculationNode",                "config": {}, "calculator": "fx"},
    {"name": "notional_node", "type": "CalculationNode",                "config": {}, "calculator": "notional"},
    {"name": "fee_node",      "type": "CalculationNode",                "config": {}, "calculator": "fee"},
    {"name": "sink",          "type": "ArrowFlatteningPublicationNode", "config": {}, "publishers": ["out"]}
  ],

  "edges": [
    {"from_node": "ingest",        "to_node": "fx_node"},
    {"from_node": "fx_node",       "to_node": "notional_node"},
    {"from_node": "notional_node", "to_node": "fee_node"},
    {"from_node": "fee_node",      "to_node": "sink"}
  ]
}
```

What happens at run time: trades arrive one by one; `ingest` gathers up to 500 into
a single RecordBatch; that batch flows **by reference** through `fx_node →
notional_node → fee_node` (each appends columns, no copies, no conversions); `sink`
splits the final batch back into individual result messages. The dict↔Arrow
conversion happens exactly **twice total** — once in, once out — instead of at
every hop.

### Tuning the one important dial: batch size

`max_size` (the batch size) is the knob that trades latency for throughput:

- **Bigger batches** amortize the fixed costs (conversion, per-tick overhead) over
  more rows, pushing throughput toward the vectorization ceiling.
- **Smaller batches** lower latency (a row spends less time waiting for batch-mates)
  but leave speed on the table.
- **A batch of 1** is the worst of both worlds for Arrow — you pay the columnar
  setup cost for a single row and get none of the vectorization benefit, so it is
  actually *slower* than a plain row calculator.

The source is **load-adaptive**: under a backlog it fills large batches; when the
stream is quiet it flushes immediately so latency stays low. A starting value of
500–1000 is reasonable; measure and adjust.

---

## Part 5 — Measure it (don't take performance on faith)

Performance claims must be measured, never assumed. The shipped example runs the
*same* pipeline two ways — the dict-envelope transport and the zero-copy RecordBatch
transport — and compares both correctness and speed:

```bash
python -m perftest.run_arrow_transport_example --trades 20000
```

Representative output:

```
trades: 20,000
  delivered: envelope=20,000 transport=20,000
  identical per-trade: 0 mismatches, max abs error 0.00e+00  ->  IDENTICAL
  throughput: envelope 5,035/s | transport 11,533/s
  transport speedup vs envelope: 2.29x
```

Read it carefully, because it tells the honest story:

- **`IDENTICAL` / 0 mismatches.** The fast path produces byte-for-byte the same
  results as the slow path. Speed changed; answers did not. Always verify this when
  you optimize.
- **~2.29× over the envelope path.** That entire gain came from *removing the
  per-stage copies* — not from changing the math. It is the §1.1 "copying is the
  giant" point, paid back.
- The **kernel** speedup (the pure vectorized math, measured by
  `python -m benchmarks.arrow_vectorization_spike`) is far larger (~11.8× on these
  numerics). The gap between 2.29× and 11.8× is the subject of the discussion next.

---

## Part 6 — Discussion: limits, trade-offs, and when to use this

### Why not the full 11.8×? (the remaining ceiling)

Removing the copies got us a large jump, but not all the way to the raw kernel
speed. What's left is **per-tick engine overhead**: even with zero copies, the
reactive engine still wakes each node, runs the equality gate, and steps the batch
through the loop once per stage. That overhead is now the dominant remaining cost —
*data movement is no longer the bottleneck; orchestration is*. Future work (free-
threaded parallel execution, embedding a vectorized SQL engine, and the zero-copy
polyglot handoff) chips away at that orchestration cost. The point of this layer was
to make **copying** stop being the ceiling, and it did.

### When this pays off (and when it doesn't)

**Reach for the high-performance Arrow path when the work is:**

- **high-volume** — thousands to millions of records; the overhead you're removing
  only matters at scale;
- **numeric / columnar** — math over regular fields (prices, quantities, rates,
  scores), the sweet spot for vectorization;
- **schema-regular** — every record has the same fields/types, so it forms a clean
  column;
- **latency-tolerant** — you can afford to gather a batch before processing;
- **a linear segment** — a straight chain of stages (the v1 transport supports
  linear Arrow segments).

**Stay on plain row calculators when the work is:**

- **low-volume or ultra-low-latency per event** — the batching/conversion overhead
  isn't worth it, and a batch of 1 is slower;
- **branchy, string-heavy, or irregular** — lots of per-record `if`s or
  heterogeneous shapes don't vectorize cleanly;
- **I/O-bound** — if you're waiting on a database or network per record, the CPU
  math was never your bottleneck.

### Known limits of the first transport version (fail-fast, not silent)

- **Fan-in of batches** (joining two RecordBatch edges into one node) is not yet
  supported and **fails fast** with a clear error rather than mis-merging. Keep the
  fast path linear; do joins on the row/dict side or with an explicit aggregation
  node.
- **Row transformers on a batch edge** also fail fast unless wrapped in
  `RowTransformerBatchAdapter`. (Per-consumer "telescopic views" of a batch — picking
  or renaming columns for one downstream consumer — are supported and *zero-copy* via
  `ProjectionBatchTransformer`, since `select`/`rename` share the underlying buffers.)
- **Schema regularity is on you.** Arrow needs a consistent schema per batch; mixed
  or sparse shapes belong on row calculators.

### A mental checklist before you optimize

1. Is this path actually high-volume and numeric? If not, stop — row is fine.
2. Did I batch (so vectorization can pay off) *and* use the zero-copy transport (so
   copying stops dominating)?
3. Did my calculators avoid `to_pylist()` and stay on `pyarrow.compute` columns?
4. Did I **measure**, and is the output **identical** to the row version?
5. Is the remaining cost now orchestration rather than copying? Then you've hit this
   layer's ceiling — and that's the right place to stop for now.

---

## Where to go next

- **Gentle how-to (write your first Arrow calculator):** `docs/TUTORIAL_arrow_dag.md`
- **The design RFCs (full detail):** `docs/design/A1-arrow-data-plane.md`,
  `docs/design/A1-recordbatch-edges.md`
- **Coexistence + decision tree:** `docs/design/A1-worked-example-and-coexistence.md`
- **Roadmap (what comes after this layer):** `docs/ROADMAP.md`
- **In the app:** Help Center → *Tutorials* → *High-Performance Arrow*
