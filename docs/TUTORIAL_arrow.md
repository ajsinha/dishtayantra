# Arrow Columnar Data Plane — Tutorial

This tutorial combines the hands-on Arrow DAG walkthrough with the high-performance / heterogeneous-source guidance. Part A is the step-by-step build; Part B is the performance model and when (not) to use Arrow.

---

## Part A — Building an Arrow DAG

# Tutorial: Building DAGs with Arrow (Columnar) Calculators

A hands-on walkthrough of the Arrow columnar data plane (roadmap Phase 1 / A1):
writing an Arrow calculator, using it as a drop-in, vectorizing batches, mixing
old-style (row) and new-style (Arrow) calculators in one graph, bridging legacy
calculators onto a batched path, and measuring the result.

**Prerequisites:** `pyarrow` (pinned in `requirements.txt`). Run commands from
the repo root. Everything here uses the in-memory broker, so no Kafka is needed.

**Background in 30 seconds.** A normal calculator (`DataCalculator`) processes
one dict at a time via `calculate(data)`. An `ArrowCalculator` adds
`calculate_batch(record_batch)` to process a whole columnar batch with vectorized
`pyarrow.compute` kernels. Because `ArrowCalculator` *is* a `DataCalculator`, it
drops into the unchanged engine and coexists with row calculators. See
`docs/design/A1-arrow-data-plane.md` and
`docs/design/A1-worked-example-and-coexistence.md` for the design.

---

## Part 0 — The ideas, in plain English (read this if the words above were jargon)

If "columnar", "vectorized", and "Arrow" are unfamiliar, here is the whole idea in
everyday terms. Skip to Part 1 if you are already comfortable.

**Row-by-row vs. column-at-a-time.** Imagine a spreadsheet of trades: each *row* is
one trade, each *column* is a field (price, quantity, currency...). To compute the
dollar value of every trade you could either:

- walk down the rows, one trade at a time, doing the arithmetic for each — this is
  how an ordinary "row" calculator works; or
- grab the entire *price* column and the entire *quantity* column and multiply them
  in one sweep — this is the **columnar** approach.

The second way is dramatically faster for large amounts of numeric data, for the
same reason it is faster to tell a class "everyone multiply your two numbers" than
to visit each student's desk in turn. Doing one operation across a whole column at
once is called **vectorization**.

**What is Arrow?** Apache **Arrow** is a widely-used standard for laying out a table
*by column* in memory so these whole-column operations are fast. `pyarrow` is its
Python library, and `pyarrow.compute` provides the fast column operations (multiply,
add, compare, and so on). A chunk of such a columnar table is called a
**RecordBatch** — picture a few hundred trades stored column-by-column, ready to be
crunched together.

**What is a "batch"?** Just a group of records handled together — e.g. 500 trades
at once instead of one at a time. Bigger groups mean less per-record overhead and
let the column math pay off.

**The one catch (and the honest trade-off).** Converting your data into Arrow's
columnar form costs a little time. If you only have *one* record, that setup cost is
pure waste and the row calculator is actually faster. The columnar approach wins
only when you process records in **batches** that are large enough to pay back the
setup cost — and the bigger the batch, the bigger the win. We measure exactly this
in Part 7, so you can see the trade-off rather than take it on faith.

**Will the answers change?** No. A columnar calculator and its row twin are written
to produce **identical** output — vectorizing changes the *speed*, never the
*result*. We verify this in CI and in the worked examples.

With those five ideas — rows vs columns, vectorization, Arrow/RecordBatch, batches,
and the setup-cost trade-off — the rest of this tutorial will read naturally.

---

## Part 1 — Write your first Arrow calculator

Subclass `ArrowCalculator` and implement `calculate_batch`. Use `append_columns`
to add output columns while preserving the input ones.

```python
# myproject/calculators.py
from core.calculator.arrow_calculator import ArrowCalculator, append_columns
import pyarrow.compute as pc

class CelsiusToFahrenheitCalculator(ArrowCalculator):
    def calculate_batch(self, batch):
        f = pc.add(pc.multiply(pc.cast(batch.column("celsius"), "double"), 1.8), 32.0)
        return append_columns(batch, {"fahrenheit": f})
```

That single class now works **two ways** with no extra code:

```python
c = CelsiusToFahrenheitCalculator("c2f", {})

# (a) Drop-in row mode — a single dict (processed as a 1-row batch):
c.calculate({"sensor": "A", "celsius": 100.0})
# -> {"sensor": "A", "celsius": 100.0, "fahrenheit": 212.0}

# (b) Vectorized — a batch-envelope message:
c.calculate({"batch": [{"celsius": 0.0}, {"celsius": 37.0}, {"celsius": 100.0}]})
# -> {"batch": [{"celsius": 0.0, "fahrenheit": 32.0}, ...]}
```

**Tip — exact parity.** If you are replacing a row calculator that rounds, use
`py_round_array(arr, ndigits)` (from `core.calculator.arrow_calculator`) for the
rounded columns. It rounds with Python's correctly-rounded `round()` so results
are bit-identical to the scalar version, while the heavy math stays vectorized.

---

## Part 2 — Use it in a DAG

Reference any calculator (row or Arrow) by dotted path in the DAG config. Here is
a minimal in-memory DAG using one of the shipped Arrow calculators, driven
directly:

```python
import json, time
from core.dag.compute_graph import ComputeGraph
from core.pubsub.inmemorypubsub import InMemoryPubSub

config = {
    "name": "demo", "start_time": None, "end_time": None,
    "subscribers": [{"name": "sub", "config": {"source": "mem://queue/demo_in", "max_depth": 100000}}],
    "publishers":  [{"name": "pub", "config": {"destination": "mem://queue/demo_out"}}],
    "calculators": [
        {"name": "fx", "type": "perftest.arrow_etl_calculators.ArrowFxConvertCalculator", "config": {}}
    ],
    "transformers": [],
    "nodes": [
        {"name": "ingest", "type": "SubscriptionNode", "config": {}, "subscriber": "sub"},
        {"name": "fx_node", "type": "CalculationNode", "config": {}, "calculator": "fx"},
        {"name": "sink", "type": "PublicationNode", "config": {}, "publishers": ["pub"]},
    ],
    "edges": [
        {"from_node": "ingest", "to_node": "fx_node"},
        {"from_node": "fx_node", "to_node": "sink"},
    ],
}

g = ComputeGraph(config); g.start(); time.sleep(0.5)
ps = InMemoryPubSub()
ps.publish_to_queue("demo_in", json.dumps({"trade_id": 1, "price": 100.0, "currency": "EUR"}))
time.sleep(0.3)
print(ps.consume_from_queue("demo_out"))   # has fx_rate_to_usd + price_usd
g.stop()
```

The same calculator works whether you feed it single dicts (as above) or
`{"batch": [...]}` envelopes (for vectorization).

---

## Part 3 — Mix old-style and new-style calculators in one graph

A single graph can contain both. The shipped example
`perftest/perftest_arrow_mixed.json` does exactly this:

```
ingest → validate[row] → normalize[row] → fx[Arrow] → notional[Arrow]
       → fee[Arrow] → risk[row] → classify[row] → sink
```

Run the demonstration:

```bash
python -m perftest.run_arrow_example --trades 20000
```

You should see **Q1 CONFIRMED** (an all-row graph and this mixed graph both run
in one process) and **Q2 IDENTICAL/CONFIRMED** (the mixed graph's output is
bit-identical to an all-row pipeline). On single-dict flow the Arrow stages are
exact drop-ins — correctness first.

---

## Part 4 — Put a legacy row calculator on a batched path

When you batch for speed, Arrow calculators vectorize, but a *row* calculator
would choke on a `{"batch": [...]}` envelope. Wrap it with
`RowCalculatorBatchAdapter`, which applies the row calculator per record:

```python
from core.calculator.arrow_calculator import RowCalculatorBatchAdapter
from perftest.etl_calculators import RiskScoreCalculator

adapter = RowCalculatorBatchAdapter("risk", {}, wrapped=RiskScoreCalculator("risk", {}))
adapter.calculate({"batch": [rec1, rec2, ...]})   # row calc applied to each record
```

In a DAG config you can express the same via a wrapped config:

```json
{
  "name": "risk",
  "type": "core.calculator.arrow_calculator.RowCalculatorBatchAdapter",
  "config": {"wrapped": {"type": "perftest.etl_calculators.RiskScoreCalculator", "config": {}}}
}
```

So a batched path can freely mix vectorized Arrow stages and adapter-wrapped
legacy stages.

---

## Part 5 — Automatic batching (no envelopes in your app)

Building `{"batch": [...]}` envelopes by hand is fine, but you usually want your
producers and consumers to keep sending and receiving **ordinary per-message**
data while the DAG batches internally. Two opt-in node types do that:

- `BatchingSubscriptionNode` — drains the subscriber into one envelope per cycle
  (load-adaptive: fills under backlog, flushes immediately when idle).
- `FlatteningPublicationNode` — republishes each record, so output stays
  per-message.

`SubscriptionNode` / `PublicationNode` are unchanged; you opt in per node:

```json
{"name": "ingest", "type": "BatchingSubscriptionNode",
 "config": {"batch": {"max_size": 500}}, "subscriber": "in_sub"}
...
{"name": "sink", "type": "FlatteningPublicationNode",
 "config": {}, "publishers": ["out_pub"]}
```

The shipped example `perftest/perftest_arrow_autobatch.json` wires
`BatchingSubscriptionNode → fx[Arrow] → notional[Arrow] → fee[Arrow] →
FlatteningPublicationNode`. Run it:

```bash
python -m perftest.run_autobatch_example --trades 20000
```

You send 20,000 ordinary trades and get 20,000 ordinary results back; internally
the source batched them (e.g. ~488 messages per envelope) and the Arrow stages
vectorized. Output is identical to the all-row pipeline. The throughput gain from
*this* (envelope) form is modest because the dataflow deep-copies each envelope
per stage — **Part 6 removes that copy** by carrying the `RecordBatch` itself on
the edges. Mix in a legacy row stage on this batched path with
`RowCalculatorBatchAdapter` (Part 4).

## Part 6 — Zero-copy transport: a RecordBatch on the edges (v5.1.0)

The envelope path still pays a per-stage cost: because a dict is mutable, the
engine deep-copies the value on every edge read and every compute, so a 500-row
envelope is cloned 3–4 times per stage. An Arrow `RecordBatch` is **immutable**,
so it can be shared *by reference* with no copy at all. Two opt-in node types make
the batch the on-edge value:

- `ArrowBatchingSubscriptionNode` — drains messages and emits one `RecordBatch`
  (instead of a dict envelope).
- `ArrowFlatteningPublicationNode` — converts the `RecordBatch` back to per-row
  dicts once, at egress.

Between them the batch flows by reference through the Arrow stages — the dict↔Arrow
conversion happens **once in, once out**, not per stage. `ArrowCalculator` already
recognises a `RecordBatch` input and stays columnar, so your calculators need no
change. The shipped example `perftest/perftest_arrow_transport.json` is the same
pipeline as Part 5 with the two transport node types swapped in:

```json
{"name": "ingest", "type": "ArrowBatchingSubscriptionNode",
 "config": {"batch": {"max_size": 500}}, "subscriber": "in_sub"}
...
{"name": "sink", "type": "ArrowFlatteningPublicationNode",
 "config": {}, "publishers": ["out_pub"]}
```

```bash
python -m perftest.run_arrow_transport_example --trades 20000
```

It runs the envelope path and the transport path and compares them: the per-trade
output is **identical** (0 mismatches), and the transport path runs **~2.29×** the
envelope path's throughput purely by removing the per-stage copies. The
equality-gate invariant is preserved — the gate now compares batches with
`RecordBatch.equals` rather than `==`, it is not bypassed.

Per-consumer **telescopic views** still work and get cheaper: an edge transformer
that implements `transform_batch` projects columns with zero copy
(`ProjectionBatchTransformer` does `select`/`rename_columns`), and a legacy row
transformer can be bridged onto a batch edge with `RowTransformerBatchAdapter`
(`core/transformer/arrow_transformer.py`). A bare row transformer on a batch edge
fails fast rather than silently mis-handling the batch.

This first version supports **linear** Arrow segments; `RecordBatch` fan-in joins
and zero-copy handoff into the C++/Rust/Java calculators are the next A1 steps
(the Arrow C Data Interface, which this transport sets up).

## Part 7 — Measure it

```bash
# End-to-end through the real engine: row vs Arrow batch, with a correctness check
python -m benchmarks.a1_vertical --trades 20000 --batch 500

# The pure vectorization ceiling (kernel only) + correctness
python -m benchmarks.arrow_vectorization_spike --rows 200000 --batch 10000

# A reproducible latency/throughput baseline for any pipeline
python -m benchmarks.run_benchmark --messages 5000 --stages 6
```

Interpreting the numbers: the **kernel** speedup is large (~11.8× on the trade
numerics). With the **dict-envelope** transport the end-to-end speedup is smaller
(~1.8× at batch 500) because every stage still deep-copies the envelope. With the
**zero-copy `RecordBatch` transport** (Part 6) those per-stage copies are gone, so
the same pipeline runs ~2.29× the envelope path while producing identical output;
the remaining gap to the kernel ceiling is per-tick node-loop overhead, not data
copying. On **single-dict** flow (batch of 1), Arrow is *slower* than row — the
conversion is pure overhead. Batch size is the dial: bigger batches amortize the
conversion and approach the kernel speedup.

---

## Part 8 — When to use Arrow (and when not)

Use Arrow (and batch) when the path is **high-volume, numeric/columnar,
schema-regular (or normalizable to a stable schema), and latency-tolerant**
(e.g. trade/tick ETL, FX/notional/risk, aggregation, telemetry) — and especially
with native C++/Rust/Java calculators (zero-copy). Heterogeneous *sources* are
fine too if you normalize them to a stable schema at ingress (typed core columns
plus a JSON extras column — see the "Heterogeneous sources" section of the
high-performance Arrow tutorial). Stay on row calculators for **low-volume,
ultra-low-latency per-event, branchy paths, work that must compute per-record on
the variable fields, or I/O-bound** paths.

Full decision tree and the coexistence rules:
`docs/design/A1-worked-example-and-coexistence.md`.

**Gotchas**
- Single-dict flow gives correctness but no speedup (overhead) — batch for speed.
- Arrow needs a schema, so batches should be schema-regular; mixed/odd shapes are
  better left on row calculators.
- For exact parity with a row calculator's rounding, use `py_round_array`.
- Batching is cleanest on linear segments; fan-in/fan-out needs row alignment.

---

## Where to go next

- **Deep dive — extremely high performance:** `docs/TUTORIAL_arrow.md`
  (the theory, design, a full max-throughput pipeline, and a discussion of limits and tuning)
- Design RFC: `docs/design/A1-arrow-data-plane.md`
- Worked example + decision tree: `docs/design/A1-worked-example-and-coexistence.md`
- Roadmap: `docs/ROADMAP.md`
- Benchmarks: `benchmarks/README.md`


---

## Part B — High-performance Arrow & heterogeneous sources

# Tutorial: Extremely High-Performance DAGs with Arrow RecordBatch

This is a **deep-dive** tutorial on getting the absolute most throughput out of a
DishtaYantra pipeline using Apache Arrow and zero-copy `RecordBatch` transport. It
covers the *theory* (why the fast path is fast), the *design* (how DishtaYantra
makes it safe), the *calculators* and *DAG* you actually write, and a closing
*discussion* of when this pays off, how to tune it, and where the ceiling is.

It is written so a newcomer can follow it end to end — every concept is explained
in plain language before it is used — while still giving an experienced engineer
the design detail they want. If you have never written an Arrow calculator at all,
skim `docs/TUTORIAL_arrow.md` first (the gentle how-to); this tutorial is the
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
- **schema-regular, or normalizable to a stable schema** — every record forms a
  clean column. Heterogeneous sources qualify too, as long as you normalize them
  to a stable schema at ingress (see "Heterogeneous sources" below);
- **latency-tolerant** — you can afford to gather a batch before processing;
- **a linear segment** — a straight chain of stages (the v1 transport supports
  linear Arrow segments).

**Stay on plain row calculators when the work is:**

- **low-volume or ultra-low-latency per event** — the batching/conversion overhead
  isn't worth it, and a batch of 1 is slower;
- **branchy, or computing on the *variable* fields** — lots of per-record `if`s,
  or logic that must read each record's variable/optional attributes rather than
  the regular columns, doesn't vectorize cleanly. (Heterogeneous *input* is **not**
  itself a blocker — normalize it to a stable schema at ingress; see below.);
- **I/O-bound** — if you're waiting on a database or network per record, the CPU
  math was never your bottleneck.

### Heterogeneous sources (different attributes per record)

A common worry is that a feed where records carry *different* attribute sets
can't go through Arrow. It can — you just normalize at ingress instead of feeding
ragged dicts straight into `records_to_batch` (which infers the schema from the
first row only, so it silently drops attributes missing from row 0 and raises on
cross-row type conflicts).

The pattern, shown end-to-end in `perftest/perftest_trade_etl_arrow.json` and
implemented by `perftest/arrow_trade_nodes.py`'s
`NormalizingArrowBatchingSubscriptionNode`:

- A small set of **core attributes** that are always present becomes typed
  columns.
- **Everything else** — including nested dicts and lists — is preserved losslessly
  in a single JSON **`extras_json`** column, so nothing is dropped and the schema
  is identical for every batch regardless of which optional attributes appear.

#### The stable schema

The node drains up to `batch.max_size` raw trade dicts and builds one RecordBatch
with this fixed schema (the default `DEFAULT_CORE_FIELDS`):

| Column | Arrow type | Default if missing | Coercion |
|--------|-----------|--------------------|----------|
| `trade_id` | string | `""` | to string |
| `seq` | int64 | `null` | to int (or null) |
| `symbol` | string | `""` | upper-cased, trimmed |
| `side` | string | `""` | upper-cased, trimmed |
| `quantity` | float64 | `0.0` | to float |
| `price` | float64 | `0.0` | to float |
| `currency` | string | `"USD"` | upper-cased, trimmed |
| `extras_json` | string | `"{}"` | JSON of all non-core keys |

So normalization (upper-casing symbol/side/currency) is folded into ingest, and
every batch has exactly these 8 columns no matter how ragged the input is.

#### Worked example

Three trades with different shapes go in:

```json
{"trade_id":"T1","symbol":"aapl","side":"buy","quantity":100,"price":150.0,"currency":"usd","trader":"alice","meta":{"src":"algo"}}
{"trade_id":"T2","symbol":"vod","side":"sell","quantity":2000,"price":0.8,"currency":"GBP","strategy":"twap","legs":[{"id":1},{"id":2}]}
{"trade_id":"T3","symbol":"7203","side":"BUY","quantity":5000,"price":2500,"currency":"JPY"}
```

Out comes one RecordBatch with identical columns for every row. The core columns
are typed and normalized; the variable attributes (different per row, including
the nested `meta` dict and the `legs` list) are JSON-packed into `extras_json`:

| trade_id | symbol | side | quantity | price | currency | extras_json |
|----------|--------|------|----------|-------|----------|-------------|
| T1 | AAPL | BUY | 100.0 | 150.0 | USD | `{"trader":"alice","meta":{"src":"algo"}}` |
| T2 | VOD | SELL | 2000.0 | 0.8 | GBP | `{"strategy":"twap","legs":[{"id":1},{"id":2}]}` |
| T3 | 7203 | BUY | 5000.0 | 2500.0 | JPY | `{}` |

#### Robustness: never drops, never raises

- A **missing core field** falls back to its default (e.g. T3 had no extras → `{}`;
  a trade with no `currency` becomes `"USD"`).
- A **bad/garbage value** is coerced rather than crashing (a non-numeric
  `quantity` becomes `0.0`); the coercion helpers never raise.
- **Cross-row type conflicts** in optional attributes (e.g. `meta` is a dict in
  one trade and a string in another) are a non-issue, because optional attributes
  are JSON-stringified into `extras_json` rather than typed into a column. This is
  exactly the case that crashes a naive `records_to_batch`.

#### Configuring it

The node is selected by dotted-path `type` and is fully config-driven:

```json
{
  "name": "trade_ingest",
  "type": "perftest.arrow_trade_nodes.NormalizingArrowBatchingSubscriptionNode",
  "subscriber": "trade_kafka",
  "calculator": "validate",
  "config": {
    "batch": { "max_size": 5000 },
    "extras_key": "extras_json",
    "core_fields": [
      {"name": "trade_id", "type": "string",  "coerce": "str",   "default": ""},
      {"name": "quantity", "type": "float64", "coerce": "float", "default": 0.0}
    ]
  }
}
```

- `batch.max_size` — how many trades to drain into one RecordBatch per cycle.
- `extras_key` — rename the JSON catch-all column (default `extras_json`).
- `core_fields` — override the default schema for a non-trade domain. Each entry
  is `{name, type (string|float64|int64|bool), coerce (str|upper|float|int),
  default}`. Omit it to use the trade defaults above.

#### The round trip

Downstream, the vectorized calculators operate on the typed core columns; the
`extras_json` column rides through untouched. At the sink,
`ArrowFlatteningPublicationNode` converts the batch back to one dict per row
(`to_pylist`), so each published message carries the computed columns **plus**
`extras_json` — the original variable attributes, intact and lossless. The
external/broker contract stays one-message-in / one-message-out; batching is
purely internal.

#### Performance note

Highest throughput comes from promoting **every field you compute on** into a
typed core column and leaving only pure pass-through data in `extras_json`. If a
calculator has to `json.loads(extras_json)` per row to read a variable attribute,
that stage is back to row speed — so if an optional attribute becomes something
you compute on, add it as a (nullable) core column instead.

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

- **Gentle how-to (write your first Arrow calculator):** `docs/TUTORIAL_arrow.md`
- **The design RFCs (full detail):** `docs/design/A1-arrow-data-plane.md`,
  `docs/design/A1-recordbatch-edges.md`
- **Coexistence + decision tree:** `docs/design/A1-worked-example-and-coexistence.md`
- **Roadmap (what comes after this layer):** `docs/ROADMAP.md`
- **In the app:** Help Center → *Tutorials* → *High-Performance Arrow*
