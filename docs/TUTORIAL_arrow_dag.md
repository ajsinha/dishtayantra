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
vectorized. Output is identical to the all-row pipeline. The throughput gain is
currently modest because the dataflow deep-copies each envelope per stage —
carrying Arrow `RecordBatch`es on edges (to remove that copy) is the next A1
increment. Mix in a legacy row stage on this batched path with
`RowCalculatorBatchAdapter` (Part 4).

## Part 6 — Measure it

```bash
# End-to-end through the real engine: row vs Arrow batch, with a correctness check
python -m benchmarks.a1_vertical --trades 20000 --batch 500

# The pure vectorization ceiling (kernel only) + correctness
python -m benchmarks.arrow_vectorization_spike --rows 200000 --batch 10000

# A reproducible latency/throughput baseline for any pipeline
python -m benchmarks.run_benchmark --messages 5000 --stages 6
```

Interpreting the numbers: the **kernel** speedup is large (~11.8× on the trade
numerics). The **end-to-end** speedup is smaller (~1.8× at batch 500) because the
node boundary still converts dicts ↔ Arrow. And on **single-dict** flow (batch of
1), Arrow is *slower* than row — the conversion is pure overhead. Batch size is
the dial: bigger batches amortize the conversion and approach the kernel speedup.

---

## Part 7 — When to use Arrow (and when not)

Use Arrow (and batch) when the path is **high-volume, numeric/columnar,
schema-regular, and latency-tolerant** (e.g. trade/tick ETL, FX/notional/risk,
aggregation, telemetry) — and especially with native C++/Rust/Java calculators
(zero-copy). Stay on row calculators for **low-volume, ultra-low-latency
per-event, branchy/string-heavy, heterogeneous, or I/O-bound** paths.

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

- Design RFC: `docs/design/A1-arrow-data-plane.md`
- Worked example + decision tree: `docs/design/A1-worked-example-and-coexistence.md`
- Roadmap: `docs/ROADMAP.md`
- Benchmarks: `benchmarks/README.md`
