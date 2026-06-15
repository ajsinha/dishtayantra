# Processing Interrelated Batch Files (e.g. End-of-Day Feeds)

> *"I'm delivered large, interrelated batch files — an end-of-day trade feed, an
> EOD FX-rate feed, an EOD client-limits feed — and I want a DAG that processes
> them at very high speed. The data isn't arriving as a stream. Can I use
> DishtaYantra, and do I really have to process one record at a time?"*

Short answer: **yes, this is a great fit**, and **usually you do *not* need
one-record-at-a-time** — that only applies to one specific kind of
interrelation. This page explains the pattern and points to a runnable example.

A runnable demo of everything below: `python -m perftest.run_eod_example`.

---

## 1. A batch file is just a bounded stream

DishtaYantra's DAG engine processes messages; it does not care whether they
arrive live or are *replayed* from a file. To process a batch file you simply
read it and publish its records onto the DAG's input queue (or point a
file/object-store subscriber at it). A 10-million-row trade file becomes a finite
stream of 10 million messages. Nothing about "it's a file, not a stream" stops
you.

So the only real design question is the *interrelation* between your feeds.

## 2. Two kinds of "interrelated" — and only one needs one-at-a-time

**(a) Fact × Dimension (the common case — NOT one-at-a-time).**
Your trade feed is the large **fact** feed. The FX-rate feed and client-limits
feed are small **dimension** feeds. A trade record needs *its* FX rate (by
currency) and *its* client limit (by client) — but trades do **not** depend on
each other. So you:

1. Load the two small dimension feeds **once** into in-memory lookup tables.
2. Stream the large trade feed through, enriching each trade by *looking up* its
   rate and limit.

Each trade is independent given the loaded tables, so this runs fast, in
parallel, and can be vectorized. **No ordering or one-at-a-time needed.** The
feeling that you "must process one record at a time" usually comes from this
case — but the cross-feed link is fact→dimension, not trade→trade, so preloading
the dimensions removes the constraint.

**(b) Record × Record sequential (the genuine one-at-a-time case).**
If a record depends on the records *before* it — e.g. a client's **running**
exposure accumulates trade by trade and must be checked incrementally in order —
then records must be processed sequentially, in order, with state carried
forward. This is the only case that truly needs one-at-a-time, and DishtaYantra
handles it with a **stateful calculator** on an **ordered** stream.

Most EOD enrichment/validation is case (a). Running balances, position keeping,
and sequence-sensitive checks are case (b). You can mix them: enrich in parallel
(a), then run the sequential step (b) on the ordered result.

## 3. Architecture for the EOD example

```
            ┌─ EOD FX-rate feed ──────┐  loaded once into
            ├─ EOD client-limits feed ┘  in-memory lookup tables
            │
EOD trade   ▼
  file  →  [ replay onto queue ] → DAG: enrich + value + limit-check → output
 (fact)        (bounded stream)        (FX lookup, notional, breach flag)
```

- **trades** = fact feed (large) → replayed through the DAG.
- **fx_rates**, **client_limits** = dimension feeds (small) → loaded once.

## 4. Step by step

### 4.1 Load the dimension feeds once
A calculator can load reference tables in its constructor — inline or from a file
path:

```json
{
  "name": "enrich",
  "type": "perftest.eod_enrichment_calculators.ReferenceEnrichCalculator",
  "config": {
    "fx_file": "/feeds/eod/fx_rates.json",
    "limits_file": "/feeds/eod/client_limits.json",
    "default_rate": 1.0
  }
}
```

(For very large or shared reference data, load it into the LMDB store or the
Redis-clone instead of a per-calculator dict; the lookup pattern is the same.)

### 4.2 Replay the trade file onto the queue
Read the trade file and publish each record (the example does this in-memory):

```python
import json
from core.pubsub.inmemorypubsub import InMemoryPubSub
ps = InMemoryPubSub()
with open("/feeds/eod/trades.jsonl") as fh:
    for line in fh:
        ps.publish_to_queue("eod_trades_in", line.strip())
```

In production you'd point a file/Kafka/object-store subscriber at the feed
instead; the DAG is identical.

### 4.3 Enrich + value + check (the DAG)
`ingest → enrich → sink`. The enricher looks up FX and limit, computes notional,
and flags breaches. See `ReferenceEnrichCalculator` — output adds `fx_rate`,
`notional_usd`, `client_limit`, `limit_breach`.

## 5. Making it fast

Because case (a) is independent per trade, you have every lever:

- **Dimensions in memory** → O(1) lookups, no per-record I/O.
- **Auto-batching + Arrow** → put a `BatchingSubscriptionNode` at the source and
  use Arrow (vectorized) calculators, or wrap a row enricher with
  `RowCalculatorBatchAdapter`, and finish with a `FlatteningPublicationNode` so
  output stays per-record. See `docs/TUTORIAL_arrow_dag.md`.
- **Parallelism** → partition the trade file (e.g. by symbol or client) and run
  multiple DAG instances / the worker pool; each partition shares the same loaded
  dimension tables.

The demo runs the same enrichment one-at-a-time and auto-batched and shows
identical results at higher batched throughput.

## 6. The sequential case (when you really need one-at-a-time)

For running exposure, process the ordered trade stream through a **stateful**
calculator (`RunningExposureCalculator`): it keeps a per-client cumulative
notional and flags the trade that pushes a client over its limit.

```python
# config: {"client_limits": {...}}   # ordered input required
# output adds: running_exposure_usd, running_breach
```

Keys to correctness here:
- **Order matters** — feed the records in the intended order (don't shuffle).
- **Keep the dependency on one partition** — if exposure is per client, you may
  shard by client across workers (each client's trades stay ordered within its
  shard), but you cannot vectorize across the running dependency itself.
- **Enrich first, then accumulate** — do the parallel enrichment (case a) up
  front, then the sequential step (case b) on the result.

## 7. Runnable example

```bash
python -m perftest.run_eod_example --trades 20000
```

Files:
- `perftest/eod_enrichment_calculators.py` — `ReferenceEnrichCalculator`
  (fact×dimension) and `RunningExposureCalculator` (sequential).
- `perftest/run_eod_example.py` — loads FX + limits, replays the trade file,
  runs enrichment one-at-a-time and auto-batched, reports breaches + throughput.

## 8. Summary

- Batch files are bounded streams — replay them through the DAG.
- Load small dimension feeds once; stream the large fact feed and enrich by
  lookup. This is fast and parallel and does **not** need one-at-a-time.
- Only genuine record-to-record dependencies (running totals, positions) need
  ordered, stateful, one-at-a-time processing — supported via a stateful
  calculator on an ordered stream.
- Combine both: parallel enrichment, then the sequential step.

Related: `docs/TUTORIAL_arrow_dag.md`, `docs/ARCHITECTURE.md`,
`docs/design/A1-worked-example-and-coexistence.md`.
