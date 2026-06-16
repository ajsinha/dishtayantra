# Tutorial 1: Your First DAG

Build a self-contained pipeline that emits a tick every few seconds,
stamps it with a custom calculator, and writes the result to a log file.
No brokers or external services required.

## New here? The idea in plain English

Imagine a small **factory assembly line**. Items arrive at one end,
each station does one job to them as they pass, and finished items come out the
other end. DishtaYantra is that assembly line for *data*: a piece of data
(a "message" — just a record, like one trade, one sensor reading, or one
order) arrives, flows through stations that transform or enrich it, and the result
is sent somewhere useful.

The whole assembly line is called a **DAG**. The letters stand for
"Directed Acyclic Graph," but you can ignore the jargon — it just means
"a set of stations wired together with one-way connections, and no loops." You
describe a DAG in a small text file (JSON), and the server runs it for you. Here
are the only five building blocks you need to know, in everyday terms:

- **Subscriber** — the *in-tray*. Where data comes
  *from* (a queue, a file, a message broker like Kafka). In this first
  tutorial we don't use one — a built-in timer creates the data instead.
- **Publisher** — the *out-tray*. Where finished data
  is sent *to* (a file, a queue, a broker). Ours writes to a file.
- **Calculator** — a *worker at a station*. A small
  piece of logic that takes one record in and returns the changed record. You
  write these in Python; this tutorial writes a tiny one.
- **Node** — a *station on the line*. A node usually
  holds one calculator and is connected to the stations before and after it.
- **Edge** — a *conveyor belt* between two stations,
  carrying data from one node to the next. (We have no edges yet because this
  tutorial has only one station; edges appear in Tutorial 2.)

One more idea that makes DishtaYantra special: a station only does work when its
input *actually changes*. If the same data arrives again unchanged, the
station stays idle instead of redoing identical work. You don't have to manage
that — it happens automatically — but it's why the system is efficient.

> **So this tutorial, in one sentence:** a built-in timer produces a
> "tick" every few seconds, a worker you write stamps each tick with a time and a
> counter, and the result is written to a file you can watch. That's a complete,
> if tiny, data pipeline.

## What you will build

A single-node DAG driven by a `MetronomeNode` (an internal timer).
On every tick it runs a small Python calculator that adds a timestamp and a
sequence number, then publishes the enriched record to a file sink. This is
the simplest useful shape: **source → calculator → sink**,
all inside one node.

> **Concepts introduced:** the five DAG element types, the
> `MetronomeNode` timer source, writing a custom Python calculator
> by subclassing `DataCalculator`, and the `file://`
> publisher.

---

## Step 1 — Write the calculator

Create a Python file for a custom calculator. A calculator subclasses
`DataCalculator` and implements one method, `calculate(self, data)`,
which receives the incoming record (a dict) and returns the processed record.
The base class gives you `self.name` and `self.config`
(the `config` block from the DAG JSON).

**core/calculator/tutorial/heartbeat\_calculator.py**

```python
"""
Tutorial 1 calculator: stamp each heartbeat with a timestamp and a
monotonically increasing sequence number.
"""
from datetime import datetime, timezone
from core.calculator.core_calculator import DataCalculator

class HeartbeatCalculator(DataCalculator):
    """Adds `seq`, `emitted_at`, and a configurable `source_label`.

    Config keys (all optional):
        source_label : str  - label written into every record (default "heartbeat")
        start_seq    : int  - first sequence number to use (default 1)
    """

    def __init__(self, name, config):
        # Always call the base constructor - it sets self.name / self.config.
        super().__init__(name, config)
        self._seq = int(self.config.get("start_seq", 1))
        self._label = self.config.get("source_label", "heartbeat")

    def calculate(self, data):
        # Book-keeping the base class exposes via details() / the UI.
        self._calculation_count += 1
        self._last_calculation = datetime.now(timezone.utc).isoformat()

        # `data` may be None or a dict depending on the upstream node; be safe.
        record = dict(data) if isinstance(data, dict) else {}

        record["source_label"] = self._label
        record["seq"] = self._seq
        record["emitted_at"] = datetime.now(timezone.utc).isoformat()

        self._seq += 1
        return record
```

> **Why this signature?** When the DAG references a calculator by
> dotted path, the engine imports the module and constructs the class as
> `HeartbeatCalculator(name=<name>, config=<config>)`.
> Matching `__init__(self, name, config)` is therefore required.
> Make sure an `__init__.py` exists in
> `core/calculator/tutorial/` so the package is importable.

## Step 2 — Write the DAG

A DAG is a JSON file in `config/dags/`. It declares the named
building blocks (subscribers, publishers, calculators, transformers) and then
wires them together with nodes and edges. Here we have no subscriber (the
metronome is the source), one calculator, and one file publisher.

**config/dags/tutorial\_1\_heartbeat.json**

```json
{
  "name": "tutorial_1_heartbeat",

  "subscribers": [],

  "publishers": [
    {
      "name": "heartbeat_log",
      "config": { "destination": "file:///tmp/dishtayantra/tutorial1.jsonl" }
    }
  ],

  "calculators": [
    {
      "name": "stamp",
      "type": "core.calculator.tutorial.heartbeat_calculator.HeartbeatCalculator",
      "config": { "source_label": "tutorial-1", "start_seq": 1 }
    }
  ],

  "transformers": [],

  "nodes": [
    {
      "name": "tick",
      "type": "MetronomeNode",
      "config": { "interval": 5 },
      "calculator": "stamp",
      "publishers": ["heartbeat_log"]
    }
  ],

  "edges": []
}
```

Field by field:

- `name` — unique DAG name, also its handle in the UI.
- `publishers[].config.destination` — a `file://`
  URI. Each published record is appended as one JSON line (`.jsonl`).
- `calculators[].type` — the dotted import path to the class
  from step 1. (Built-ins like `ApplyDefaultsCalculator` are
  referenced by bare name; anything with a dot is imported as a custom class.)
- `nodes[]` — one `MetronomeNode` firing every
  `interval` seconds. `calculator` and
  `publishers` reference the named blocks above.
- `edges` — empty here, because everything happens inside the
  single node. Edges only matter once you have more than one node (Tutorial 2).

## Step 3 — Run it

```bash
# 1. Make the tutorial package importable (once):
mkdir -p core/calculator/tutorial
touch core/calculator/tutorial/__init__.py
# (place heartbeat_calculator.py inside that folder)

# 2. Drop tutorial_1_heartbeat.json into config/dags/

# 3. Start the server and open the dashboard
export SECRET_KEY="change-me"
python run_server.py
# visit http://localhost:5002/dashboard  ->  Start "tutorial_1_heartbeat"

# 4. Watch the output
tail -f /tmp/dishtayantra/tutorial1.jsonl
```

Every five seconds a new line appears:

```json
{"source_label": "tutorial-1", "seq": 1, "emitted_at": "2026-06-12T14:03:21.004+00:00"}
{"source_label": "tutorial-1", "seq": 2, "emitted_at": "2026-06-12T14:03:26.010+00:00"}
```

> **What just happened.** The DAG's single compute thread ran its
> loop: the metronome node marked itself dirty on each interval, the engine
> called `compute()`, which ran your `calculate()` and
> handed the result to the file publisher. You can **Suspend**,
> **Resume**, or **Stop** it from the dashboard.

## Next steps

You have the basic shape. In [Tutorial 2](/help/userguides/TUTORIAL_02_pipeline.md)
you will split logic across multiple nodes, connect them with edges, and read
from / write to a real message broker.
