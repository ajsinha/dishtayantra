# Tutorial 3: A Full-Feature Trading Pipeline

A realistic market-data pipeline that exercises nearly every
capability: fan-in from multiple feeds, a chain of calculators (including a
risk subgraph), market-aware scheduling, resilient broker input, and a
cloud-queue output.

## New here? The idea in plain English

This tutorial builds a realistic **trading pipeline**: the kind of thing a bank actually runs at end of day. A raw trade comes in, and a series of stations each add something — convert the price to US dollars, work out the trade’s total value, add fees, score its risk. By the end you have a fully enriched record ready for reporting.

Nothing here is conceptually new beyond Tutorial 2 — it is the same "stations connected by belts" idea — but there are *more* stations and the calculators do realistic work. The goal is to show how naturally a real, multi-stage business process maps onto a DAG: one station per logical step, wired in the order the work must happen.

**In one sentence:** a raw trade flows through several enrichment stations (FX → valuation → fees → risk) and comes out as a complete, decision-ready record.

---

## Architecture

```
  feed_a (kafka) --\
  feed_b (kafka) ---+--> [ingest] --> [normalize] --> [enrich] --> [risk:subgraph] --(to_wire)--> [publish]
  feed_c (kafka) --/                                                                                |
                                                                              priced_topic + sqs + audit_log
```

Three Kafka feeds are merged with `fanin://` into a single
subscriber. The pipeline then normalizes symbols, enriches with reference data,
runs a risk calculation encapsulated as a **subgraph**, projects the
record for the wire, and fans the result out to a Kafka topic, an AWS SQS queue,
and a local audit file. The whole DAG only runs during US market hours on
non-holiday weekdays.

> **Concepts combined:** everything from Tutorials 1–2 plus
> `fanin://` multi-source merge, a `SubgraphNode`,
> market-aware scheduling (time window + days + holiday calendars), resilient
> connectors, and multi-destination publishing across broker + cloud + file.

> **The new ideas, in plain English.**
>
> - **Fan-in** — instead of one in-tray, three feeds pour
>   into the same first station. Think of three conveyor belts merging into one.
>   The `fanin://` address is how you say "combine these sources."
> - **Subgraph** — a mini assembly line packaged up so it
>   looks like a single station. Here the risk calculation is several steps, but
>   we wrap them as one reusable "risk" box and drop it into the line. It keeps a
>   big pipeline readable and lets you reuse the box elsewhere.
> - **Market-aware scheduling** — the line only switches on
>   during US stock-market hours on non-holiday weekdays. You give it a time
>   window, the days, and a holiday calendar, and it starts and stops itself.
> - **Fan-out** — the opposite of fan-in: one finished
>   record is sent to several out-trays at once (a Kafka topic, an AWS SQS queue,
>   and an audit file), so different downstream systems each get a copy.
> - **Resilient connector** — an in-tray/out-tray that
>   automatically reconnects and retries if the broker hiccups, so a brief
>   outage doesn't lose data or stop the line.

---

## Step 1 — The calculators

**Normalizer** — uppercases the symbol and coerces numeric
fields, so downstream stages can assume clean types.

**core/calculator/tutorial/normalize\_calculator.py**

```python
"""Tutorial 3: normalize incoming market records."""
from datetime import datetime, timezone
from core.calculator.core_calculator import DataCalculator

class NormalizeCalculator(DataCalculator):
    """Uppercases `symbol`, coerces `price`/`quantity` to float.

    Config keys:
        symbol_field : str - field holding the instrument symbol (default "symbol")
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        self._symbol_field = self.config.get("symbol_field", "symbol")

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now(timezone.utc).isoformat()

        record = dict(data) if isinstance(data, dict) else {}
        sym = record.get(self._symbol_field)
        if isinstance(sym, str):
            record[self._symbol_field] = sym.strip().upper()
        for f in ("price", "quantity"):
            if f in record:
                try:
                    record[f] = float(record[f])
                except (TypeError, ValueError):
                    record[f] = None
        return record
```

**Enricher** — joins static reference data (sector, currency)
from the calculator's own config, demonstrating config-driven lookup tables.

**core/calculator/tutorial/enrich\_calculator.py**

```python
"""Tutorial 3: enrich a record with reference data from config."""
from datetime import datetime, timezone
from core.calculator.core_calculator import DataCalculator

class EnrichCalculator(DataCalculator):
    """Looks up reference attributes for the record's symbol.

    Config keys:
        reference : dict[str, dict] - symbol -> {sector, currency, ...}
        default_currency : str - fallback currency (default "USD")
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        self._reference = self.config.get("reference", {})
        self._default_ccy = self.config.get("default_currency", "USD")

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now(timezone.utc).isoformat()

        record = dict(data) if isinstance(data, dict) else {}
        ref = self._reference.get(record.get("symbol"), {})
        record["sector"] = ref.get("sector", "UNKNOWN")
        record["currency"] = ref.get("currency", self._default_ccy)
        return record
```

**Risk scorer** — lives inside the subgraph (step 3). It
computes notional and a simple risk band.

**core/calculator/tutorial/risk\_calculator.py**

```python
"""Tutorial 3: compute notional and a risk band (used inside the subgraph)."""
from datetime import datetime, timezone
from core.calculator.core_calculator import DataCalculator

class RiskCalculator(DataCalculator):
    """Adds `notional` and a `risk_band` of LOW / MEDIUM / HIGH.

    Config keys:
        medium_threshold : float - notional above which risk is MEDIUM (default 100000)
        high_threshold   : float - notional above which risk is HIGH   (default 1000000)
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        self._medium = float(self.config.get("medium_threshold", 100_000))
        self._high = float(self.config.get("high_threshold", 1_000_000))

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now(timezone.utc).isoformat()

        record = dict(data) if isinstance(data, dict) else {}
        price = record.get("price") or 0.0
        qty = record.get("quantity") or 0.0
        notional = round(float(price) * float(qty), 2)
        record["notional"] = notional

        if notional >= self._high:
            record["risk_band"] = "HIGH"
        elif notional >= self._medium:
            record["risk_band"] = "MEDIUM"
        else:
            record["risk_band"] = "LOW"
        return record
```

## Step 2 — The wire transformer

Same idea as Tutorial 2 — project to the public field set as data leaves
the pipeline.

**core/transformer/tutorial/trade\_wire.py**

```python
"""Tutorial 3: project the enriched/scored record to the output schema."""
from datetime import datetime, timezone
from core.transformer.core_transformer import DataTransformer

class TradeWireTransformer(DataTransformer):
    """Keeps the public output fields and stamps a publish time."""

    DEFAULT_FIELDS = ["symbol", "sector", "currency", "notional", "risk_band"]

    def __init__(self, name, config):
        super().__init__(name, config)
        self._fields = self.config.get("fields", self.DEFAULT_FIELDS)

    def transform(self, data):
        self._transform_count += 1
        self._last_transform = datetime.now(timezone.utc).isoformat()

        record = dict(data) if isinstance(data, dict) else {}
        out = {k: record.get(k) for k in self._fields}
        out["published_at"] = datetime.now(timezone.utc).isoformat()
        return out
```

## Step 3 — The risk subgraph

A subgraph is a complete DAG embedded as a single node in the parent. Here it
wraps the risk calculation so it can be developed, tested, suspended, and reused
independently. Define it as its own file:

**config/dags/subgraphs/risk\_subgraph.json**

```json
{
  "name": "risk_subgraph",
  "subscribers": [],
  "publishers": [],
  "calculators": [
    { "name": "risk",
      "type": "core.calculator.tutorial.risk_calculator.RiskCalculator",
      "config": { "medium_threshold": 100000, "high_threshold": 1000000 } }
  ],
  "transformers": [],
  "nodes": [
    { "name": "score", "type": "CalculationNode", "calculator": "risk" }
  ],
  "edges": []
}
```

The parent references it with a `SubgraphNode` that points at the
file. `entry_connection` / `exit_connections` name the
internal nodes that receive input and produce output. See
[Subgraphs](/help/subgraph) for the full model
(including inline definitions and dynamic light up/down).

## Step 4 — The full DAG

Note the top-level `start_time` / `duration` /
`schedule` block: this DAG is active only 09:30–16:00 on
weekdays that are not USA/CANADA market holidays.

**config/dags/tutorial\_3\_trading.json**

```json
{
  "name": "tutorial_3_trading",

  "start_time": "0930",
  "duration": "6h30m",
  "schedule": {
    "days_of_week": ["mon", "tue", "wed", "thu", "fri"],
    "holiday_calendars": ["USA", "CANADA"]
  },

  "subscribers": [
    { "name": "feed_a", "config": { "source": "kafka://topic/feed_a", "resilient": true } },
    { "name": "feed_b", "config": { "source": "kafka://topic/feed_b", "resilient": true } },
    { "name": "feed_c", "config": { "source": "kafka://topic/feed_c", "resilient": true } },
    { "name": "merged_feed", "config": { "source": "fanin://feed_a,feed_b,feed_c" } }
  ],

  "publishers": [
    { "name": "priced_topic",
      "config": { "destination": "kafka://topic/priced_trades", "resilient": true } },
    { "name": "risk_queue",
      "config": { "destination": "sqs://trade-risk", "region": "us-east-1", "resilient": true } },
    { "name": "audit_log",
      "config": { "destination": "file:///tmp/dishtayantra/tutorial3.jsonl" } }
  ],

  "calculators": [
    { "name": "normalize",
      "type": "core.calculator.tutorial.normalize_calculator.NormalizeCalculator",
      "config": { "symbol_field": "symbol" } },
    { "name": "enrich",
      "type": "core.calculator.tutorial.enrich_calculator.EnrichCalculator",
      "config": {
        "default_currency": "USD",
        "reference": {
          "ACME":    { "sector": "Industrials", "currency": "USD" },
          "GLOBEX":  { "sector": "Technology",  "currency": "USD" },
          "INITECH": { "sector": "Technology",  "currency": "CAD" }
        }
      } }
  ],

  "transformers": [
    { "name": "to_wire",
      "type": "core.transformer.tutorial.trade_wire.TradeWireTransformer",
      "config": { "fields": ["symbol", "sector", "currency", "notional", "risk_band"] } }
  ],

  "nodes": [
    { "name": "ingest",    "type": "SubscriptionNode", "subscriber": "merged_feed" },
    { "name": "normalize", "type": "CalculationNode",   "calculator": "normalize" },
    { "name": "enrich",    "type": "CalculationNode",   "calculator": "enrich" },

    { "name": "risk", "type": "SubgraphNode",
      "config": {
        "subgraph_file": "subgraphs/risk_subgraph.json",
        "entry_connection": "score",
        "exit_connections": ["score"]
      } },

    { "name": "publish", "type": "PublisherSinkNode",
      "config": { "publishers": ["priced_topic", "risk_queue", "audit_log"] } }
  ],

  "edges": [
    { "from_node": "ingest",    "to_node": "normalize" },
    { "from_node": "normalize", "to_node": "enrich" },
    { "from_node": "enrich",    "to_node": "risk" },
    { "from_node": "risk",      "to_node": "publish", "data_transformer": "to_wire", "pname": "wire" }
  ]
}
```

## Step 5 — Walkthrough of one record

Suppose `{"symbol": "globex", "price": "4200.0", "quantity": "300"}`
arrives on `feed_b`:

1. **fan-in** delivers it through `merged_feed` to
   `ingest`.
2. **normalize** → `symbol: "GLOBEX"`,
   `price: 4200.0`, `quantity: 300.0`.
3. **enrich** → adds `sector: "Technology"`,
   `currency: "USD"`.
4. **risk subgraph** → `notional: 1260000.0`,
   `risk_band: "HIGH"`.
5. the **to\_wire** edge transformer projects to the output schema
   and stamps `published_at`.
6. **publish** sends the result to the Kafka topic, the SQS
   queue, and the audit file simultaneously.

```json
{"symbol": "GLOBEX", "sector": "Technology", "currency": "USD",
 "notional": 1260000.0, "risk_band": "HIGH",
 "published_at": "2026-06-12T14:31:07.512+00:00"}
```

## Step 6 — Run, schedule, and scale

```bash
# Package layout for the custom code:
#   core/calculator/tutorial/__init__.py
#   core/calculator/tutorial/normalize_calculator.py
#   core/calculator/tutorial/enrich_calculator.py
#   core/calculator/tutorial/risk_calculator.py
#   core/transformer/tutorial/__init__.py
#   core/transformer/tutorial/trade_wire.py
#   config/dags/subgraphs/risk_subgraph.json
#   config/dags/tutorial_3_trading.json

# SQS output needs boto3 + AWS credentials in the environment:
pip install boto3
export AWS_REGION=us-east-1   # plus your standard AWS credential chain

export SECRET_KEY="change-me"
python run_server.py
# Dashboard -> Start "tutorial_3_trading"
```

> **Warning —**
>
> **Outside market hours?** If you start the DAG when the schedule
> is not active, the dashboard shows *Outside Window*. A UI-initiated
> Start overrides scheduling so you can test any time; left to itself, the
> engine only runs the DAG 09:30–16:00 on non-holiday weekdays and
> re-reads schedule edits within five minutes.

> **Scaling out.** Assign this DAG to the worker pool
> ([Worker Pool](/help/worker-pool)) for true
> multi-core execution, or **Clone** it from the dashboard to run a
> second instance over different feeds — the clone inherits the day/holiday
> schedule automatically and lets you set a new time window.

## Where to go next

- [Calculators](/help/calculators) — built-ins and the C++/Rust/Java options for hot paths.
- [Subgraphs](/help/subgraph) — inline definitions and dynamic light up/down.
- [Scheduling](/help/scheduling) & [Cloud Pub/Sub](/help/cloud-pubsub) — the full options for the features used here.
