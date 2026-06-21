# Tutorial: End-of-Day Batch Processing (a gentle, complete walk-through)

This tutorial teaches you how to run a real **end-of-day (EOD)** job on
DishtaYantra: take a big file of the day's trades, look up some reference data
(currency exchange rates and per-client risk limits), calculate the value of each
trade in US dollars, and flag any trade that pushes a client over their limit.

It is written for someone who has **never used DishtaYantra** and may be new to
data pipelines in general. We explain every term the first time it appears, and
every step says *what* you are doing and *why*. The runnable version of everything
here is one command: `python -m perftest.run_eod_example`.

If you want the short, reference-style version instead, see
`docs/BATCH_FILE_PROCESSING.md`.

---

## Part 0 — The ideas, in plain English

Before any code, here are the handful of ideas you need. Read this once; the rest
of the tutorial will make sense.

**What is DishtaYantra?** It is an engine that runs a *pipeline*: data comes in,
flows through a series of processing steps, and comes out transformed. You
describe the pipeline once (as a small configuration file) and the engine runs it.

**What is a DAG?** DAG stands for "directed acyclic graph" — a fancy name for a
flowchart with no loops. Each **box** in the flowchart is a step, and the
**arrows** show which step feeds the next. In DishtaYantra:

- a **node** is a box (a step),
- an **edge** is an arrow (data flowing from one step to the next),
- a **calculator** is the actual logic that runs inside a step (the thing that
  takes data in and gives transformed data out).

So a pipeline that reads trades, enriches them, and writes the results is three
nodes connected by two edges: `read → enrich → write`.

**What does "reactive" mean?** The engine is event-driven: when new data arrives
at a step, that step "wakes up", does its work, and passes the result on. Nothing
runs unless there is something to do. (One useful consequence: a step only re-runs
when its input actually *changes* — this is the engine's "only recompute on
change" rule, and it is why your calculators should return a *new* result rather
than quietly modifying the data they were handed.)

**What is a "batch file"?** Just a file with many records in it — for example, one
line per trade for the whole day. The key insight of this tutorial: **a file is
simply a stream of data that happens to end.** You feed the file's records into the
pipeline one after another, and the job is "finished" when the last record has
been processed. Because you know how many records the file has, you always know
when you are done.

**The one decision that matters for EOD jobs.** When you process a file against
other files, ask: *does each record stand on its own, or does it depend on the
records before it?*

- If each trade can be valued **on its own** (just look up today's exchange rate
  and multiply), the records are **independent**. You can process them in any
  order, in parallel, very fast. This is the common case.
- If a record's result **depends on the running total of everything before it**
  (for example, "has this client's cumulative position crossed their limit *yet*
  today?"), the records are **sequential**. These must be processed in order, one
  at a time.

Most EOD enrichment is the first kind. We will build that first, then show the
sequential case at the end.

---

## Part 1 — The three feeds, and why two of them are tiny

A typical EOD enrichment job has three inputs:

1. **The trade file** — large. One record per trade, maybe hundreds of thousands
   of them. In data jargon this is the **fact** feed: the things that happened.
2. **The FX (foreign-exchange) rates** — tiny. A short table like
   `{"USD": 1.0, "EUR": 1.08, "GBP": 1.27}` saying how many dollars one unit of
   each currency is worth.
3. **The client limits** — tiny. A short table like `{"C1": 1000000, "C2": 50000}`
   saying the maximum dollar exposure each client is allowed.

Feeds 2 and 3 are called **dimension** feeds: small, slowly-changing lookup tables
you check the big fact feed against. The whole job is: *for each trade (fact), look
things up in the dimensions, and compute new fields.*

Because the dimensions are small, you load them **once** into memory and reuse them
for every trade. You never re-read them per record — that would be like
re-opening the dictionary for every single word you look up.

---

## Part 2 — Writing the enrichment calculator

A **calculator** is a small Python class with one important method, `calculate`,
which receives one record (a Python dictionary) and returns a new dictionary. Here
is the enrichment calculator, annotated. (The shipped version is
`perftest/eod_enrichment_calculators.py::ReferenceEnrichCalculator`.)

```python
from core.calculator.core_calculator import DataCalculator

class ReferenceEnrichCalculator(DataCalculator):
    def __init__(self, name, config):
        super().__init__(name, config)
        cfg = config or {}
        # Load the small dimension tables ONCE, when the calculator is created.
        # You can pass them inline (fx_rates / client_limits) or as file paths
        # (fx_file / limits_file) that are read once here.
        self.fx_rates      = cfg.get("fx_rates")      or self._load(cfg.get("fx_file"))
        self.client_limits = cfg.get("client_limits") or self._load(cfg.get("limits_file"))

    def calculate(self, trade):
        # 'trade' is one record, e.g.
        #   {"trade_id": 7, "client_id": "C2", "currency": "EUR",
        #    "quantity": 1000, "price": 42.5}

        ccy  = trade.get("currency", "USD")
        rate = self.fx_rates.get(ccy, 1.0)        # look up today's FX rate

        # value the trade in US dollars
        notional_usd = round(trade["quantity"] * trade["price"] * rate, 2)

        limit = self.client_limits.get(trade["client_id"])   # look up the limit

        # Return a NEW dictionary: the original trade plus the new fields.
        # We do not modify 'trade' in place — the engine relies on that.
        return {
            **trade,
            "fx_rate": rate,
            "notional_usd": notional_usd,
            "client_limit": limit,
            "limit_breach": limit is not None and notional_usd > limit,
        }
```

Two habits to notice, because they matter throughout DishtaYantra:

- **Load reference data once** (in `__init__`), not per record. This is the single
  biggest speed difference between a fast EOD job and a slow one.
- **Return a new dictionary; never mutate the input.** The engine's "recompute only
  on change" rule compares the new result against the previous one. If you secretly
  edit the input, that comparison breaks. Treat inputs as read-only.

A quick worked number, so the formula is concrete: a trade of `1000` units at a
price of `42.5` in EUR, with an FX rate of `1.08`, is worth
`1000 × 42.5 × 1.08 = 45,900` USD. If client `C2`'s limit is `50,000`, this trade
is *under* the limit, so `limit_breach` is `False`. One more euro-heavy trade and
they would cross it.

---

## Part 3 — Describing the pipeline (the DAG configuration)

You do not write code to wire the pipeline together; you describe it in a small
JSON file. Here is the whole thing, with comments on each part:

```json
{
  "name": "eod_enrichment",

  "subscribers": [
    {"name": "in", "config": {"source": "mem://queue/eod_in", "max_depth": 2000000}}
  ],
  "publishers": [
    {"name": "out", "config": {"destination": "mem://queue/eod_out"}}
  ],

  "calculators": [
    {"name": "enrich",
     "type": "perftest.eod_enrichment_calculators.ReferenceEnrichCalculator",
     "config": {"fx_rates":      {"USD": 1.0, "EUR": 1.08, "GBP": 1.27},
                "client_limits": {"C1": 1000000, "C2": 50000}}}
  ],

  "nodes": [
    {"name": "ingest",      "type": "SubscriptionNode", "config": {}, "subscriber": "in"},
    {"name": "enrich_node", "type": "CalculationNode",  "config": {}, "calculator": "enrich"},
    {"name": "sink",        "type": "PublicationNode",  "config": {}, "publishers": ["out"]}
  ],

  "edges": [
    {"from_node": "ingest",      "to_node": "enrich_node"},
    {"from_node": "enrich_node", "to_node": "sink"}
  ]
}
```

Reading it top to bottom:

- **`subscribers`** are the *inputs* — where data comes *in*. Here, an in-memory
  queue named `eod_in`. (`mem://queue/...` means "an in-process queue"; in
  production this could be `kafka://...`, `rabbitmq://...`, and so on — the
  pipeline does not change, only this address does.) `max_depth` is just how many
  messages the queue can hold.
- **`publishers`** are the *outputs* — where results go *out*. Here, a queue named
  `eod_out`.
- **`calculators`** lists the logic blocks. We declare one, `enrich`, point it at
  our Python class by its full path, and hand it the two dimension tables as
  config. This is where the FX rates and limits get loaded.
- **`nodes`** are the three boxes: `ingest` reads from the input
  (a `SubscriptionNode`), `enrich_node` runs our calculator (a `CalculationNode`),
  and `sink` writes to the output (a `PublicationNode`).
- **`edges`** are the two arrows: `ingest → enrich_node → sink`.

That is the entire pipeline. Notice you wrote *no* loop, no threading, no queue
handling — the engine does all of that. You described *what* should happen, and the
engine handles *how*.

---

## Part 4 — Running it and reading the output

Now we replay the trade file into the pipeline and collect the enriched results.

```python
import json, time
from core.dag.compute_graph import ComputeGraph
from core.pubsub.inmemorypubsub import InMemoryPubSub

# 1. Build and start the pipeline from the JSON above.
graph = ComputeGraph(json.load(open("eod_enrichment.json")))
graph.start()

# 2. Get a handle to the in-memory messaging system.
ps = InMemoryPubSub()

# 3. Read the trade file and feed each trade into the input queue.
#    THIS is "replaying the file as a stream".
trades = [json.loads(line) for line in open("trades.jsonl")]
for t in trades:
    ps.publish_to_queue("eod_in", json.dumps(t))

# 4. Collect the results from the output queue. Because we know how many trades
#    we sent, we know we are done when we have collected that many results.
out = {}
while len(out) < len(trades):
    msg = ps.consume_from_queue("eod_out", block=False)   # see note below
    if msg is None:
        time.sleep(0.002)      # nothing ready yet; wait a blink and retry
        continue
    rec = json.loads(msg)
    out[rec["trade_id"]] = rec

# 5. Use the results.
breaches = sum(1 for r in out.values() if r["limit_breach"])
print(f"enriched {len(out)} trades, {breaches} of them breached a client limit")

graph.stop()
```

**Why `block=False`?** When you ask an empty queue for a message, the default
behaviour is to *wait* until one arrives. That is great inside the engine, but here
it would make our loop hang forever the moment we have collected everything.
`block=False` says "if there is nothing right now, return `None` immediately" so we
can finish cleanly. (This is a real gotcha — a blocking drain loop on an empty queue
is the classic way to make a script appear to freeze.)

If you ran this against 20,000 trades you would see something like
`enriched 20000 trades, 5380 of them breached a client limit`.

---

## Part 5 — Making it fast (because the trades are independent)

Recall the key decision from Part 0: in enrichment, each trade is **independent**
(it only needs the lookup tables, not its neighbours). That independence is what
lets us go fast, in two complementary ways.

**(a) Process many trades as one "batch".** Instead of handing the calculator one
trade at a time, the engine can gather, say, 500 trades and process them together.
Fewer trips through the machinery means more throughput. You turn this on by
swapping two node types — no change to your calculator:

- `SubscriptionNode` → `BatchingSubscriptionNode` (gathers incoming trades into
  batches automatically),
- `PublicationNode` → `FlatteningPublicationNode` (splits the batch back into
  individual result messages, so the outside world still sees one message per
  trade).

**(b) Do the math on whole columns at once (Arrow).** A "columnar" calculator
computes `notional_usd` for all 500 trades in one vectorized operation instead of
looping. For the absolute fastest path, the
`ArrowBatchingSubscriptionNode` / `ArrowFlatteningPublicationNode` pair carries the
batch between steps without copying it. The full story (with a hands-on build) is
in `docs/TUTORIAL_arrow.md`. The important promise: **the answers are
byte-for-byte identical** — batching and vectorizing change only the speed, never
the result.

The shipped demo proves exactly this:

```bash
python -m perftest.run_eod_example --trades 20000
```

It runs the same 20,000 trades **one-at-a-time** and **auto-batched**, and reports
that both find the same breaches (≈5,380 on the seeded data) with identical
per-trade output — the batched run is simply faster.

---

## Part 6 — The sequential case (when order really matters)

Sometimes a record's result genuinely depends on the ones before it. The classic
example: a **running exposure** per client — a cumulative total that grows trade by
trade, where a breach happens the moment the total crosses the limit. You cannot
compute that in parallel or out of order, because "the total so far" only means
something in sequence.

This calculator keeps state between records:

```python
class RunningExposureCalculator(DataCalculator):
    def __init__(self, name, config):
        super().__init__(name, config)
        self.limits = (config or {}).get("client_limits", {})
        self._cum = {}    # remembers each client's cumulative exposure so far

    def calculate(self, trade):
        c = trade["client_id"]
        # add this trade's value to the client's running total
        self._cum[c] = self._cum.get(c, 0.0) + trade["notional_usd"]
        limit = self.limits.get(c)
        return {
            **trade,
            "running_exposure_usd": round(self._cum[c], 2),
            "running_breach": limit is not None and self._cum[c] > limit,
        }
```

The difference from Part 2 is the `self._cum` dictionary — it **remembers**
information across records. That makes this calculator **stateful**, and stateful
means three rules:

- feed it the trades **in order**,
- do **not** batch or parallelize it (parallel workers would each keep their own
  partial total and none would be correct),
- do **not** replay it twice without resetting (the totals would double).

That is the whole distinction in one sentence: **enrichment is order-free and can
be made fast; running aggregates are order-bound and must stay sequential.**
Recognising which one you have is the most important skill for EOD work.

---

## Part 7 — Running it for real (headless)

In production you would not drive an EOD job from a Python session you babysit. You
run it **headless** — the engine starts, processes the file to completion, writes a
summary, and exits with a status code your scheduler (Autosys, Control-M, Airflow,
cron) can check:

```bash
python -m core.dag.headless_runner \
    --config eod_enrichment.json \
    --replay trades.jsonl \
    --expect 20000 \
    --summary eod_summary.json
```

It exits `0` if it processed everything and `1` if it could not — so the next job
in your overnight batch only runs if this one truly succeeded. Running EOD jobs
headless, and having one controller launch many of them, is the subject of the
next tutorial.

---

## Where to go next

- **Reference (short version):** `docs/BATCH_FILE_PROCESSING.md`
- **Make enrichment fast (Arrow / columnar):** `docs/TUTORIAL_arrow.md`
- **Run-once + orchestrating many jobs:** `docs/TUTORIAL_headless.md`
- **In the app:** Help Center → *Batch File Processing*, and → *Calculators*
