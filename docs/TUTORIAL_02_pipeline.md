# Tutorial 2: A Multi-Node Broker Pipeline

Read trade messages from a broker, validate and price them across
several nodes wired by edges, reshape the data on an edge with a transformer,
and publish the result to an output topic.

## New here? The idea in plain English

In Tutorial 1 we had a single station. A **pipeline** is what you get when you line several stations up and connect them with conveyor belts. In DishtaYantra terms: several **nodes** (stations), each running a **calculator** (a worker), joined by **edges** (the belts that carry a record from one node to the next).

The other new idea here is a **message broker**. Think of it as a post office between programs: one program drops messages into a named queue and another picks them up, even if they run on different machines. Our pipeline reads its input from a broker queue instead of an internal timer, which is how real systems feed data in. (We start with an in-memory queue so you need nothing installed, then show the one-line change to point at a real broker like Kafka.)

**In one sentence:** data arrives on a queue, flows through two connected stations (price it, then size it), and the finished records are published out — your first true multi-step assembly line.

---

## What you will build

A four-node pipeline that ingests raw trades, filters out invalid ones,
computes a notional value, reshapes the record for the wire, and publishes it:

```
    [ingest] → [validate] → [price] —(transformer)→ [publish]
```

Unlike Tutorial 1, logic is split across **multiple nodes connected by
edges**. The engine computes a topological order from the edges and runs
the nodes in dependency order each sweep, passing each node's output to the next.

> **In plain English.** Tutorial 1 had a single station that did
> everything. Real pipelines split the work into several stations, each doing one
> clear job, connected by conveyor belts. That's what we build here: data comes in
> from an *in-tray* (the broker subscriber), passes through a
> *validate* station, then a *price* station, gets *reshaped*
> on the belt into just the fields we want to send out, and lands in the
> *out-tray* (the broker publisher). "Topological order" is just a fancy way
> of saying the engine always runs a station only after the stations feeding it
> have run — so data never arrives at a station before it's ready. You don't
> arrange that by hand; you just declare which station connects to which, and the
> engine figures out the order. Splitting work this way keeps each piece simple to
> write, test, and reuse — the same reason a real factory uses specialised
> stations instead of one worker doing everything.

> **What's a "broker"?** A message broker (Kafka, RabbitMQ, ActiveMQ,
> and others) is a post office for data: producers drop messages into named
> mailboxes (called *topics* or *queues*) and consumers pick them up,
> so the two sides never have to talk directly or be running at the same time.
> To keep things runnable with nothing to install, we first use the built-in
> *in-memory* broker (a mailbox that lives inside the server), then show the
> same DAG against real Kafka — only the connection address changes.

> **Concepts introduced:** `SubscriptionNode` and
> `PublisherSinkNode`; multiple `CalculationNode`s wired
> by `edges`; an edge `data_transformer`; a broker
> subscriber/publisher (shown with the in-memory broker so it runs with no
> external services, then with Kafka).

---

## Step 1 — The pricing calculator

This calculator reads `price` and `quantity` from the
record and writes a `notional` field. It demonstrates reading config
(a rounding precision) and defensive handling of bad input.

**core/calculator/tutorial/pricing\_calculator.py**

```python
"""Tutorial 2 calculator: compute notional = price * quantity."""
from datetime import datetime, timezone
from core.calculator.core_calculator import DataCalculator

class PricingCalculator(DataCalculator):
    """Adds a `notional` field computed from `price` and `quantity`.

    Config keys:
        precision : int - decimal places to round notional to (default 2)
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        self._precision = int(self.config.get("precision", 2))

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now(timezone.utc).isoformat()

        record = dict(data) if isinstance(data, dict) else {}
        try:
            price = float(record.get("price", 0))
            qty = float(record.get("quantity", 0))
            record["notional"] = round(price * qty, self._precision)
        except (TypeError, ValueError):
            # Never raise into the engine loop for one bad record; flag instead.
            record["notional"] = None
            record["pricing_error"] = True

        return record
```

## Step 2 — The validation calculator

A second calculator that marks records valid/invalid. Invalid records are
passed through with a flag so a downstream filter could drop them; here we keep
it simple and just annotate.

**core/calculator/tutorial/validation\_calculator.py**

```python
"""Tutorial 2 calculator: validate required fields on a trade record."""
from datetime import datetime, timezone
from core.calculator.core_calculator import DataCalculator

class ValidationCalculator(DataCalculator):
    """Sets `valid` (bool) based on presence of required fields.

    Config keys:
        required : list[str] - field names that must be present and non-null
                               (default ["symbol", "price", "quantity"])
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        self._required = self.config.get(
            "required", ["symbol", "price", "quantity"])

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now(timezone.utc).isoformat()

        record = dict(data) if isinstance(data, dict) else {}
        missing = [f for f in self._required
                   if f not in record or record[f] is None]
        record["valid"] = (len(missing) == 0)
        if missing:
            record["missing_fields"] = missing
        return record
```

## Step 3 — An edge transformer

A transformer reshapes data as it crosses an edge — here, projecting the
internal record down to just the fields we want on the wire. Transformers
subclass `DataTransformer` and implement `transform(self, data)`.

**core/transformer/tutorial/wire\_projection.py**

```python
"""Tutorial 2 transformer: keep only the public wire fields."""
from datetime import datetime, timezone
from core.transformer.core_transformer import DataTransformer

class WireProjectionTransformer(DataTransformer):
    """Projects a trade record down to a fixed set of output fields.

    Config keys:
        fields : list[str] - field names to keep
                 (default ["symbol", "notional", "valid"])
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        self._fields = self.config.get(
            "fields", ["symbol", "notional", "valid"])

    def transform(self, data):
        self._transform_count += 1
        self._last_transform = datetime.now(timezone.utc).isoformat()

        record = dict(data) if isinstance(data, dict) else {}
        return {k: record.get(k) for k in self._fields}
```

## Step 4 — The DAG

Now wire it together. Four nodes connected by three edges. The first node is
a `SubscriptionNode` bound to a subscriber; the last is a
`PublisherSinkNode` that publishes whatever arrives on its incoming
edge. The edge from `price` to `publish` carries the
`data_transformer`.

**config/dags/tutorial\_2\_trades.json**

```json
{
  "name": "tutorial_2_trades",

  "subscribers": [
    {
      "name": "raw_trades",
      "config": { "source": "mem://topic/raw_trades" }
    }
  ],

  "publishers": [
    {
      "name": "priced_trades",
      "config": { "destination": "mem://topic/priced_trades" }
    },
    {
      "name": "audit_log",
      "config": { "destination": "file:///tmp/dishtayantra/tutorial2.jsonl" }
    }
  ],

  "calculators": [
    { "name": "validate",
      "type": "core.calculator.tutorial.validation_calculator.ValidationCalculator",
      "config": { "required": ["symbol", "price", "quantity"] } },
    { "name": "price",
      "type": "core.calculator.tutorial.pricing_calculator.PricingCalculator",
      "config": { "precision": 2 } }
  ],

  "transformers": [
    { "name": "to_wire",
      "type": "core.transformer.tutorial.wire_projection.WireProjectionTransformer",
      "config": { "fields": ["symbol", "notional", "valid"] } }
  ],

  "nodes": [
    { "name": "ingest",   "type": "SubscriptionNode",  "subscriber": "raw_trades" },
    { "name": "validate", "type": "CalculationNode",   "calculator": "validate" },
    { "name": "price",    "type": "CalculationNode",   "calculator": "price" },
    { "name": "publish",  "type": "PublisherSinkNode",
      "config": { "publishers": ["priced_trades", "audit_log"] } }
  ],

  "edges": [
    { "from_node": "ingest",   "to_node": "validate" },
    { "from_node": "validate", "to_node": "price" },
    { "from_node": "price",    "to_node": "publish", "data_transformer": "to_wire", "pname": "wire" }
  ]
}
```

How the data moves each compute sweep:

- **ingest** pulls one message from the `raw_trades`
  subscriber and outputs it.
- **validate** runs `ValidationCalculator`, adding a
  `valid` flag.
- **price** runs `PricingCalculator`, adding
  `notional`.
- the edge **price → publish** applies `to_wire`,
  projecting the record to `symbol, notional, valid`.
- **publish** sends the projected record to both the
  `priced_trades` topic and the `audit_log` file.

## Step 5 — Run it and feed it data

The in-memory broker (`mem://`) keeps everything in-process, so no
external services are needed. Start the DAG from the dashboard, then publish a
few test trades onto the input topic. The simplest way is the
**Publish Message** action in the UI, or a tiny script:

```python
"""Feed sample trades into tutorial_2_trades via the in-memory broker."""
from core.pubsub.pubsubfactory import create_publisher

pub = create_publisher("feeder", {"destination": "mem://topic/raw_trades"})
pub.publish({"symbol": "ACME", "price": 12.50, "quantity": 100})
pub.publish({"symbol": "GLOBEX", "price": 3.10, "quantity": 250})
pub.publish({"symbol": "INITECH", "quantity": 50})   # missing price -> valid:false
```

The audit log then shows the projected output:

```json
{"symbol": "ACME", "notional": 1250.0, "valid": true}
{"symbol": "GLOBEX", "notional": 775.0, "valid": true}
{"symbol": "INITECH", "notional": 0.0, "valid": false}
```

## Switching to a real broker

To run against Kafka instead of the in-memory broker, change only the URIs
— no code changes:

```json
"subscribers": [
  { "name": "raw_trades",
    "config": { "source": "kafka://topic/raw_trades", "resilient": true } }
],
"publishers": [
  { "name": "priced_trades",
    "config": { "destination": "kafka://topic/priced_trades", "resilient": true } }
]
```

`"resilient": true` wraps the connector with retry, outage-buffering,
and automatic reconnect. The same swap works for `rabbitmq://`,
`activemq://`, `sqs://`, `servicebus://`, and the
other schemes in [Pub/Sub](/help/pubsub).

> **Key idea.** Calculators hold business logic, transformers
> handle plumbing (reshaping), nodes are the graph vertices, and edges define
> both data flow and execution order. Because everything is referenced by name,
> the same calculator or publisher can be reused by several nodes.

## Next steps

[Tutorial 3](/help/userguides/TUTORIAL_03_full_feature.md) combines almost every
feature: fan-in from multiple sources, a subgraph, multiple calculators in several
languages, market-aware scheduling, an output to a cloud queue, and resilient
connectors.
