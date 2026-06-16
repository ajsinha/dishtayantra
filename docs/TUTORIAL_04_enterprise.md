# Tutorial 4: An Enterprise Data Pipeline

A large, layered pipeline that ingests from many sources,
converges them through complex multi-input merge nodes, processes in stages,
and fans out to multiple destinations. This is the shape of a real
production graph — and it shows how the model scales to dozens of nodes.

## New here? The idea in plain English

"Enterprise" just means *production-grade*: the same kind of pipeline as before, but wired up the way you would actually deploy it for a business that depends on it. The data-flow ideas are unchanged — nodes, calculators, edges, queues — what is new is the surrounding **operational** concerns: reading from and writing to durable broker queues, organising several processing stages cleanly, and producing output other systems can consume reliably.

If Tutorial 3 was "make the business logic work," this one is "make it something you could hand to an operations team." Read it as a worked example of shaping a serious pipeline, not as new engine concepts.

**In one sentence:** the same assembly-line idea, assembled the way a real deployment would be — durable inputs and outputs, clear stages, dependable results.

---

## What you will build

A complete, runnable pipeline that follows the classic enterprise shape: a
wide set of inputs at the top, a **diamond** convergence through
heavy multi-input merge nodes in the middle, and a wide fan-out to outputs at
the bottom. The example below is built at a readable scale (about a dozen
nodes) that exercises *every* pattern; the final section explains
exactly how to grow it to 50+ nodes without changing the model.

```
  transactions (kafka) ─┐
  customers   (kafka) ─┤
  products    (kafka) ─┼─► [ingest_*] ─► [validate_*] ─┐
  reference   (file)  ─┤                                ├─► [central_merge] ◄── 5 inputs ★
  heartbeat   (timer) ─┘                                │        │
                                                        │        ▼
                                              [enrich] ─► [risk_assessment] ◄── (merge) ★
                                                                 │
                                                                 ▼
                                                        [final_check] ─┬─► kafka://processed
                                                                       ├─► kafka://alerts
                                                                       └─► file://audit
```

> **Concepts combined:** everything from Tutorials 1–3, plus
> **heterogeneous inputs** (Kafka + file + metronome in one DAG),
> **complex merge nodes** that take five or more incoming edges,
> a multi-layer topology, and a wide multi-destination fan-out — all
> inside a single time-windowed DAG.

> **In plain English.** This is what a real production line looks
> like once it grows up. Picture many in-trays at the top (some fed by brokers,
> one by a file, one by a timer), several columns of stations in the middle that
> *merge* many belts together and process in layers, and many out-trays at
> the bottom. The "**diamond**" shape just means: lots of inputs
> funnel inward to a few central decision points, which then spread back out to
> many outputs. A **merge node** is simply a station with several
> belts feeding it — it waits for its inputs and combines them into one
> record. Nothing here is a new *kind* of thing; it's the same five building
> blocks from Tutorial 1, just more of them wired into layers. That's the real
> lesson: you scale up by adding more of the same simple pieces, not by learning
> new machinery.

---

## Step 1 — The calculators

Three small calculators cover the pipeline's logic. They follow the same
`DataCalculator` contract as the earlier tutorials.

**Tagging calculator** — stamps each record with its source
so the merge node can tell inputs apart.

**core/calculator/tutorial/tag\_source\_calculator.py**

```python
"""Tutorial 4: tag a record with the source it arrived from."""
from datetime import datetime, timezone
from core.calculator.core_calculator import DataCalculator

class TagSourceCalculator(DataCalculator):
    """Adds a `source` label (from config) and an ingest timestamp.

    Config keys:
        source_name : str - label to stamp onto each record (required)
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        self._source_name = self.config.get("source_name", name)

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now(timezone.utc).isoformat()
        record = dict(data) if isinstance(data, dict) else {"value": data}
        record["source"] = self._source_name
        record["ingested_at"] = datetime.now(timezone.utc).isoformat()
        return record
```

**Merge calculator** — the heart of a complex node. It runs
on a node with many incoming edges and combines the per-source records into a
single consolidated record. Because the engine packages multi-edge input as a
dict keyed by edge `pname`, the calculator can address each source
by name.

**core/calculator/tutorial/merge\_calculator.py**

```python
"""Tutorial 4: consolidate multiple upstream sources into one record.

When a node has several incoming edges that carry a `pname`, the engine
hands this calculator a dict shaped like:

    { "transactions": {...}, "customers": {...}, "reference": {...}, ... }

We flatten the pieces we care about into a single consolidated record and
record which sources were present on this compute sweep.
"""
from datetime import datetime, timezone
from core.calculator.core_calculator import DataCalculator

class MergeCalculator(DataCalculator):
    """Merge named upstream inputs into one record.

    Config keys:
        keep_sources : list[str] - source pnames to copy through
                        (default: copy everything received)
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        self._keep = self.config.get("keep_sources")

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now(timezone.utc).isoformat()

        merged = {"merged_at": datetime.now(timezone.utc).isoformat(),
                  "present_sources": []}

        if isinstance(data, dict):
            for pname, payload in data.items():
                if self._keep and pname not in self._keep:
                    continue
                merged["present_sources"].append(pname)
                # Namespace each source's fields to avoid collisions.
                if isinstance(payload, dict):
                    for k, v in payload.items():
                        merged[f"{pname}.{k}"] = v
                else:
                    merged[pname] = payload
        else:
            merged["value"] = data

        merged["source_count"] = len(merged["present_sources"])
        return merged
```

**Risk / gate calculator** — a decision node that scores the
consolidated record and sets a routing flag used downstream.

**core/calculator/tutorial/gate\_calculator.py**

```python
"""Tutorial 4: score a consolidated record and set a routing decision."""
from datetime import datetime, timezone
from core.calculator.core_calculator import DataCalculator

class GateCalculator(DataCalculator):
    """Sets `decision` (PASS / ALERT) from a completeness threshold.

    Config keys:
        min_sources : int - minimum distinct sources required to PASS (default 3)
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        self._min_sources = int(self.config.get("min_sources", 3))

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now(timezone.utc).isoformat()
        record = dict(data) if isinstance(data, dict) else {}
        present = int(record.get("source_count", 0))
        record["decision"] = "PASS" if present >= self._min_sources else "ALERT"
        record["evaluated_at"] = datetime.now(timezone.utc).isoformat()
        return record
```

## Step 2 — The pipeline DAG

The DAG wires heterogeneous inputs through tagging and validation into a
**central merge node with five incoming edges** (each labelled with a
`pname` so the merge calculator can address them), then through a
gate, and out to three destinations. It runs only during business hours.

**config/dags/tutorial\_4\_enterprise.json**

```json
{
  "name": "tutorial_4_enterprise",

  "start_time": "0600",
  "duration": "17h",
  "schedule": {
    "days_of_week": ["mon", "tue", "wed", "thu", "fri"],
    "holiday_calendars": ["USA"]
  },

  "subscribers": [
    { "name": "transactions", "config": { "source": "kafka://topic/transactions", "resilient": true } },
    { "name": "customers",    "config": { "source": "kafka://topic/customers",    "resilient": true } },
    { "name": "products",     "config": { "source": "kafka://topic/products",     "resilient": true } },
    { "name": "reference",    "config": { "source": "file:///data/reference/master_data.json", "watch_seconds": 300 } },
    { "name": "heartbeat",    "config": { "source": "metronome", "interval": 30 } }
  ],

  "publishers": [
    { "name": "processed_out", "config": { "destination": "kafka://topic/processed_data", "resilient": true } },
    { "name": "alerts_out",    "config": { "destination": "kafka://topic/alerts",         "resilient": true } },
    { "name": "audit_out",     "config": { "destination": "file:///logs/audit.jsonl" } }
  ],

  "calculators": [
    { "name": "tag_tx",   "type": "core.calculator.tutorial.tag_source_calculator.TagSourceCalculator", "config": { "source_name": "transactions" } },
    { "name": "tag_cust", "type": "core.calculator.tutorial.tag_source_calculator.TagSourceCalculator", "config": { "source_name": "customers" } },
    { "name": "tag_prod", "type": "core.calculator.tutorial.tag_source_calculator.TagSourceCalculator", "config": { "source_name": "products" } },
    { "name": "tag_ref",  "type": "core.calculator.tutorial.tag_source_calculator.TagSourceCalculator", "config": { "source_name": "reference" } },
    { "name": "tag_hb",   "type": "core.calculator.tutorial.tag_source_calculator.TagSourceCalculator", "config": { "source_name": "heartbeat" } },
    { "name": "merge",    "type": "core.calculator.tutorial.merge_calculator.MergeCalculator", "config": {} },
    { "name": "gate",     "type": "core.calculator.tutorial.gate_calculator.GateCalculator", "config": { "min_sources": 3 } }
  ],

  "transformers": [],

  "nodes": [
    { "name": "in_tx",   "type": "SubscriptionNode", "subscriber": "transactions" },
    { "name": "in_cust", "type": "SubscriptionNode", "subscriber": "customers" },
    { "name": "in_prod", "type": "SubscriptionNode", "subscriber": "products" },
    { "name": "in_ref",  "type": "SubscriptionNode", "subscriber": "reference" },
    { "name": "in_hb",   "type": "SubscriptionNode", "subscriber": "heartbeat" },

    { "name": "tag_tx",   "type": "CalculationNode", "calculator": "tag_tx" },
    { "name": "tag_cust", "type": "CalculationNode", "calculator": "tag_cust" },
    { "name": "tag_prod", "type": "CalculationNode", "calculator": "tag_prod" },
    { "name": "tag_ref",  "type": "CalculationNode", "calculator": "tag_ref" },
    { "name": "tag_hb",   "type": "CalculationNode", "calculator": "tag_hb" },

    { "name": "central_merge", "type": "CalculationNode", "calculator": "merge" },
    { "name": "gate",          "type": "CalculationNode", "calculator": "gate" },

    { "name": "publish", "type": "PublisherSinkNode",
      "config": { "publishers": ["processed_out", "alerts_out", "audit_out"] } }
  ],

  "edges": [
    { "from_node": "in_tx",   "to_node": "tag_tx" },
    { "from_node": "in_cust", "to_node": "tag_cust" },
    { "from_node": "in_prod", "to_node": "tag_prod" },
    { "from_node": "in_ref",  "to_node": "tag_ref" },
    { "from_node": "in_hb",   "to_node": "tag_hb" },

    { "from_node": "tag_tx",   "to_node": "central_merge", "pname": "transactions" },
    { "from_node": "tag_cust", "to_node": "central_merge", "pname": "customers" },
    { "from_node": "tag_prod", "to_node": "central_merge", "pname": "products" },
    { "from_node": "tag_ref",  "to_node": "central_merge", "pname": "reference" },
    { "from_node": "tag_hb",   "to_node": "central_merge", "pname": "heartbeat" },

    { "from_node": "central_merge", "to_node": "gate" },
    { "from_node": "gate",          "to_node": "publish" }
  ]
}
```

> **The complex node.** `central_merge` has five
> incoming edges, each with a distinct `pname`. That is exactly
> how DishtaYantra models a node that must consolidate many upstream sources:
> give every inbound edge a `pname` and let the merge calculator
> address each source by name. Nodes like this are the convergence points of
> the “diamond” — the spec this tutorial is based on had six
> such nodes (the heaviest with seven inputs).

## Step 3 — The layered topology

The engine computes a topological order and runs nodes layer by layer. This
pipeline has four logical layers:

| Layer | Nodes | Role |
| --- | --- | --- |
| 1 - Ingest | `in_tx`, `in_cust`, `in_prod`, `in_ref`, `in_hb` | Pull from 5 heterogeneous sources (Kafka, file, timer) |
| 2 - Tag | `tag_*` | Stamp each record with its source |
| 3 - Merge | `central_merge` | Consolidate 5 inputs into one record (complex node) |
| 4 - Decide & publish | `gate`, `publish` | Score, then fan out to 3 destinations |

Open the DAG's **Details** page and the new **Graph View**
renders this exact structure — the five inputs on the left converging on
the merge node, then flowing out to the publisher.

## Step 4 — Run it

```bash
# Package layout for the custom code:
#   core/calculator/tutorial/__init__.py
#   core/calculator/tutorial/tag_source_calculator.py
#   core/calculator/tutorial/merge_calculator.py
#   core/calculator/tutorial/gate_calculator.py
#   config/dags/tutorial_4_enterprise.json

# Kafka + file inputs are configured above; resilient connectors need the
# broker reachable (or buffer until it is). Then:
export SECRET_KEY="change-me"
python run_server.py
# Dashboard -> Start "tutorial_4_enterprise"  (overrides the 06:00-23:00
# window for manual testing; left alone it runs only in business hours)
```

## Step 5 — Scaling to 50+ nodes

The pattern above is the whole story; a 50-node enterprise pipeline is just
*more of the same*. To grow it:

- **Add more inputs** — declare additional subscribers
  (more Kafka topics, more files, more metronome timers) and an ingest +
  tag node for each.
- **Add intermediate layers** — insert
  validation/enrichment/calculation nodes between tag and merge, or
  between merge and gate. Each is a `CalculationNode` wired by
  edges, so the engine slots it into the topological order automatically.
- **Add more complex merge nodes** — a realistic graph
  has several convergence points (e.g. a central merge with seven inputs,
  an analytics aggregation with six, a comprehensive validation with six).
  Each is built exactly like `central_merge`: many incoming
  edges, each with a `pname`, feeding a merge-style calculator.
- **Widen the output fan-out** — add publishers and route
  to them from one or more `PublisherSinkNode`s (e.g. four Kafka
  topics + five file outputs).
- **Encapsulate sub-pipelines** — when a region of the
  graph becomes a unit (say, a risk-scoring cluster), wrap it in a
  [subgraph](/help/subgraph) and reference it as
  a single `SubgraphNode` in the parent. This keeps a 50-node
  graph readable.

> **Warning —**
>
> **Practical limits.** A single DAG runs on one compute thread,
> so a very large graph is still sequential per sweep. For CPU-heavy
> 50-node pipelines, assign the DAG to the
> [worker pool](/help/worker-pool), or split the
> graph into several DAGs connected by broker topics. The
> **Graph View** on the Details page is the fastest way to sanity-check
> a large topology — the complex merge nodes stand out immediately as the
> points where many edges converge.

## You have completed the tutorials

From a single timer node (Tutorial 1) to a layered, multi-source enterprise
pipeline (this one), you have now seen the whole model. For depth on any
feature used here, see [Scheduling](/help/scheduling),
[Subgraphs](/help/subgraph),
[Worker Pool](/help/worker-pool), and the per-broker
setup guides under [User Guides](/help/userguides).
