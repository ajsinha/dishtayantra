# DishtaYantra — Quick Start & User Guide

A high-performance, thread-safe DAG (Directed Acyclic Graph) compute engine
for real-time data pipelines: polyglot calculators wired to twenty-plus
message brokers and cloud services, supervised with high availability and
market-aware schedule management.

This single guide covers everything you need to install, configure, run,
and operate DishtaYantra. For deeper architecture detail see
`docs/ARCHITECTURE.md`; for the full configuration and cloud-messaging
reference see `docs/CONFIG_AND_CLOUD.md`; for decoupling publication from the
compute thread see the *Async Egress (WAL-Backed Publication) Guide* and its two
tutorials (basic, and single-process vs multiprocess workers); the in-app
**Help** pages (`/help`) document every subsystem interactively.

---

## Table of Contents

1. [Install](#1-install)
2. [Configure](#2-configure)
3. [Run](#3-run)
4. [The Web Console](#4-the-web-console)
5. [The Compute Model (Nodes, Edges, Engine)](#5-the-compute-model)
6. [Calculators](#6-calculators)
7. [Pub/Sub: Brokers, Cloud & Data Sources](#7-pubsub-brokers-cloud--data-sources)
8. [Storage Providers](#8-storage-providers)
9. [High Availability](#9-high-availability)
10. [Market-Aware Scheduling](#10-market-aware-scheduling)
11. [Cloning DAGs](#11-cloning-dags)
12. [Users, Roles & API Keys](#12-users-roles--api-keys)
13. [Monitoring & Operations](#13-monitoring--operations)
14. [Tests](#14-tests)
15. [Troubleshooting](#15-troubleshooting)

---

## 1. Install

```bash
pip install -r requirements.txt
```

Front-end assets (Bootstrap, jQuery, Prism, fonts) are **vendored locally**
under `web/static/vendor/`, so the UI renders fully with no CDN access -
suitable for air-gapped deployments. No extra install step is needed for
the web console.

Optional dependencies - install only what you actually configure:

```bash
# Cloud storage / messaging
pip install boto3                  # S3 storage, s3:// sqs:// kinesis:// sns:// pub-sub, S3 HA
pip install azure-storage-blob     # Azure Blob storage, azureblob:// pub-sub
pip install azure-servicebus       # servicebus:// pub-sub
pip install azure-eventhub         # eventhubs:// pub-sub
pip install google-cloud-storage   # GCS storage, gcs:// pub-sub

# High availability backends
pip install kazoo                  # Zookeeper HA
pip install redis                  # Redis HA

# Brokers (as needed)
pip install kafka-python           # kafka://
pip install pika                   # rabbitmq://
pip install stomp.py               # activemq://

# Native calculators (as needed)
pip install py4j                   # Java calculators (JVM gateway)
# C++ (pybind11) and Rust (PyO3) modules are built from source - see /help
```

---

## 2. Configure

Configuration is **format-agnostic**: provide either
`config/application.yaml` (recommended) or the classic
`config/application.properties`. The loader auto-detects the format by file
extension and prefers YAML when both exist. Both ship with the project and
resolve to identical values.

The defaults run a standalone server with local storage and a SQLite user
database - no external services required:

```yaml
server:
  host: 0.0.0.0
  port: 5002

app:
  # Session secret - REQUIRED (or export SECRET_KEY)
  secret_key: "${SECRET_KEY:default_secret_key_change_in_production}"

storage:
  provider: filesystem        # filesystem | s3 | azureblob | gcs
  filesystem:
    root: ./
  dags:
    prefix: config/dags        # where DAG JSON configs are read from

db:
  engine: sqlite               # sqlite | postgresql
  sqlite:
    path: data/dishtayantra.db

ha:
  provider: none               # none | zookeeper | redis | s3 | socket

holidays:
  prefix: config/holidays
  reload_seconds: 180
schedule:
  refresh_seconds: 240
```

### Variable substitution (both formats)

`${...}` substitution works identically in YAML and `.properties`:

- `${other.key}` - reference another config value (nested refs resolved)
- `${ENV_VAR}` - reference an environment variable
- `${ENV_VAR:default}` - use the env var, or fall back to `default`

Precedence, highest first: command-line `--key=value`, environment
variables, then config-file values. YAML values are read as strings and
coerced on access, so `port: 5002` and `port: "${PORT:5002}"` behave
identically.

> **Fail-fast philosophy:** if a required property for the configured
> provider is missing, the server refuses to start and the log names the
> exact property. Nothing is silently defaulted.

---

## 3. Run

```bash
export SECRET_KEY="change-me-in-production"
python run_server.py
```

Open <http://localhost:5002>. Anonymous visitors see the landing page;
click **Sign in** to reach the console.

### First login

- If you upgraded from v1.x with a `config/users.json`, those accounts are
  migrated into the database on first boot (passwords hashed) and the file
  is renamed `users.json.migrated`.
- On a fresh install the bootstrap account is **admin / admin123** - change
  the password immediately under **Users**.

---

## 4. The Web Console

Once logged in, the navbar gives access to every area, and shows this
instance's **HA role (PRIMARY / SECONDARY)** on every page.

| Area | Path | Purpose |
| --- | --- | --- |
| Dashboard | `/dashboard` | List, start/suspend/stop, clone, and inspect DAGs; see each DAG's schedule |
| DAG Designer | `/dag-designer` | Build/edit DAG configurations visually |
| Manage &gt; Egress | `/egress` | Async-egress (WAL-backed publication) monitoring |
| Manage &gt; Flow Time Travel | `/flow` | Stream-replay the flow change-log for a chosen window (max 24h), grouped by compute cycle (whole start-to-finish waves) and capped to 5 concurrent replays; export the window as JSONL; recording status is shown **read-only** here |
| Manage &gt; Cache | `/cache` | Browse and manage the in-memory cache |
| Manage &gt; Live Logs | `/admin/logs/live` | Live SSE log tail — available to **any authenticated user** |
| Admin &gt; System Monitoring | `/admin/monitoring` | Live CPU / memory / disk / network; **flow recording enable/disable** (global or per-DAG); **SLO / staleness alerts** *(admin)* |
| Admin &gt; Worker Pool | `/admin/workers` | Monitor and control worker processes *(admin)* |
| Admin &gt; JVM / C++ / Rust | `/jvm`, `/cpp`, `/rust` | Native calculator module management *(admin)* |
| Admin &gt; System Logs | `/admin/logs` | View, filter, download log files *(admin)* |
| Admin &gt; Logging / Maintenance / API Keys / Audit | `/admin/logging`, `/admin/maintenance`, `/admin/api-keys`, `/admin/audit` | Runtime log-level control, drain/maintenance, key management, audit trail *(admin)* |
| Users | `/users` | Accounts, roles, API keys *(admin only)* |
| Help | `/help` | Interactive documentation for every subsystem |
| About &gt; About / Compare | `/about`, `/comparison` | Product overview; dimension-by-dimension comparison vs Spark / Flink / Airflow |

The navbar groups operational views under a **Manage** dropdown (Egress, Flow
Time Travel, Cache, Live Logs), admin-only tools under **Admin**, and **About**
is a dropdown with *About* and *Compare*. Live Logs is reachable by any
logged-in user; the other admin items require the `admin` role.

A light/dark theme toggle sits in the navbar.

---

## 5. The Compute Model

A DAG is a JSON file in `config/dags/`. Five element types make up every
DAG, and understanding how they fit together is the key to building
pipelines.

### The Compute Engine

Each running DAG owns one **compute thread**. On start, the engine builds
all components, validates that the graph is acyclic (a cycle is a fatal
configuration error), and computes a **topological order** of the nodes so
upstream nodes always run before their dependents. It then loops:

1. **pre-compute** every node (e.g. a subscription node marks itself
   "dirty" if its broker queue has data);
2. **compute** every node that is dirty, in topological order, passing
   results downstream along edges;
3. **post-compute** the nodes that acted and bump their compute counts.

The loop is dirty-driven: a node only does work when it has new input, so an
idle DAG is cheap. A DAG can run in the main process or be assigned to the
**worker pool** for true multi-core parallelism (section 13). Start, suspend
(pause without tearing down), resume, and stop are all available from the
Dashboard, and the engine respects the DAG's schedule (section 10).

### The Five Element Types

| Element | Key | Role |
| --- | --- | --- |
| **Subscribers** | `subscribers` | Inputs - pull data from a broker / source (section 7) |
| **Publishers** | `publishers` | Outputs - push data to a broker / sink (section 7) |
| **Calculators** | `calculators` | The logic applied at a node (section 6) |
| **Transformers** | `transformers` | Lightweight reshaping of data on a node's input/output or along an edge |
| **Nodes** | `nodes` | The graph vertices - each wires together a subscriber, calculator, transformers, and/or publishers |
| **Edges** | `edges` | Directed links that pass one node's output to the next node's input |

Subscribers, publishers, calculators, and transformers are declared once
(as named building blocks), then **referenced by name** from nodes and
edges. This lets several nodes share one publisher, one calculator, etc.

### Nodes

A node's `type` selects its behavior. Built-in node types:

| Node type | Purpose |
| --- | --- |
| `SubscriptionNode` | Pulls messages from a named `subscriber`; marks dirty when data is available |
| `CalculationNode` | Applies a named `calculator` to its input (the default type) |
| `MetronomeNode` | A timer source that fires every `interval` seconds - drives DAGs that have no external input |
| `PublisherSinkNode` | Terminal node that publishes its incoming edge data to one or more `publishers` |
| `SinkNode` | Terminal no-op (useful as a graph leaf) |
| `SubgraphNode` | Embeds another DAG as a single node (graph-as-a-node); supervised independently |

A node config may include any of: `subscriber` (name), `calculator`
(name), `publishers` (list of names), `input_transformers` /
`output_transformers` (lists of names), and a node-specific `config`
object. A custom node class can be supplied by dotted path
(`my.package.MyNode`) instead of a built-in type.

### Edges

An edge is a directed connection `from_node -> to_node`. The engine pushes
`from_node`'s output to `to_node`'s input each compute sweep. Edges support
two optional fields:

- `data_transformer` - a named transformer applied to the data **as it
  crosses the edge** (e.g. reshape one node's output to the next node's
  expected input);
- `pname` - an optional pseudo-name label for the edge, useful when a node
  needs to distinguish data arriving on different incoming edges.

```json
{ "from_node": "ingest", "to_node": "enrich" }
{ "from_node": "enrich", "to_node": "publish", "data_transformer": "to_wire_format", "pname": "enriched" }
```

Edges define the dependency order; the engine derives the topological sort
from them and **rejects any configuration that contains a cycle**.

### Transformers

Transformers are lightweight data-shaping steps (declared under
`transformers`, same `type` mechanism as calculators). They can be attached
to a node's input (`input_transformers`) or output (`output_transformers`),
or to an edge (`data_transformer`). Use a calculator for business logic and
a transformer for plumbing (renaming/wrapping/reshaping).

### Example A - Timer-driven heartbeat (no external input)

Emits a tick every 10 seconds, enriches it with defaults, and publishes to
an in-memory topic and a log file:

```json
{
  "name": "my_first_dag",
  "subscribers": [],
  "publishers": [
    { "name": "out_topic", "config": { "destination": "mem://topic/my_topic" } },
    { "name": "out_log",   "config": { "destination": "file:///tmp/log/my_dag.jsonl" } }
  ],
  "calculators": [
    { "name": "enrich", "type": "ApplyDefaultsCalculator",
      "config": { "defaults": { "dag_name": "my_first_dag", "status": "healthy" } } }
  ],
  "transformers": [],
  "nodes": [
    { "name": "tick", "type": "MetronomeNode", "config": { "interval": 10 },
      "publishers": ["out_topic", "out_log"], "calculator": "enrich" }
  ],
  "edges": []
}
```

### Example B - Broker ingest -> calculate -> publish (multi-node with edges)

Reads from a Kafka topic, applies a calculator, and publishes the result -
three nodes wired by two edges:

```json
{
  "name": "ingest_pipeline",
  "subscribers": [
    { "name": "ticks_in", "config": { "source": "kafka://topic/market.ticks", "resilient": true } }
  ],
  "publishers": [
    { "name": "signals_out", "config": { "destination": "kafka://topic/market.signals", "resilient": true } }
  ],
  "calculators": [
    { "name": "signal", "type": "core.calculator.random_calculator.RandomCalculator", "config": {} }
  ],
  "transformers": [],
  "nodes": [
    { "name": "ingest",  "type": "SubscriptionNode",   "subscriber": "ticks_in" },
    { "name": "compute", "type": "CalculationNode",    "calculator": "signal" },
    { "name": "publish", "type": "PublisherSinkNode",  "config": { "publishers": ["signals_out"] } }
  ],
  "edges": [
    { "from_node": "ingest",  "to_node": "compute" },
    { "from_node": "compute", "to_node": "publish" }
  ]
}
```

### Example C - Fan-in from several sources

`fanin://` merges several subscribers into one stream; the merged
subscriber then feeds a normal subscription node (see the shipped
`config/dags/` DAGs for a complete working version).

```json
"subscribers": [
  { "name": "s1", "config": { "source": "metronome", "interval": 120 } },
  { "name": "s2", "config": { "source": "metronome", "interval": 60 } },
  { "name": "merged", "config": { "source": "fanin://s1,s2" } }
]
```

Drop any of these files in `config/dags/`, then click **Start** on the
Dashboard. The two DAGs shipped in `config/dags/` are working references;
more examples live in `config/example/dags/` (not auto-loaded). The visual
**DAG Designer** (`/dag-designer`) builds the same JSON without hand-editing.

### Loading new files into a running server

The storage prefix is scanned **once at startup**, so a JSON file you add or
delete while the server is running is not picked up automatically. To
reconcile a running server with the files on disk, use the **Reload DAGs**
button on the Dashboard (admin-only, shown on the primary instance) or POST to
`/dags/reload`. Reloading:

- **adds** a DAG for every new JSON file in the prefix,
- **stops and removes** DAGs whose source file was deleted, and
- **leaves untouched** DAGs created in memory (DAG Designer / clones), which
  have no backing file.

Reloading does *not* rebuild a DAG whose file merely changed; for a structural
change, delete and re-add the DAG (intraday **schedule** edits are still
applied automatically within five minutes — see section 10).

## 6. Calculators

Calculators hold the logic applied at a `CalculationNode` (or any node with
a `calculator`). Reference a built-in by name, or any class by its dotted
import path.

**Built-in calculators:** `NullCalculator`, `PassthruCalculator`,
`ApplyDefaultsCalculator`, `AttributeFilterCalculator`,
`AttributeFilterAwayCalculator`, `AttributeNameChangeCalculator`,
`AdditionCalculator`, `MultiplicationCalculator`.

```json
{ "name": "my_calc", "type": "core.calculator.random_calculator.RandomCalculator", "config": {} }
```

A calculator's `config` object is passed to it at construction, so the same
class can be reused with different parameters. Write your own by
subclassing `DataCalculator` and registering it (built-in name) or
referencing it by dotted path.

**Polyglot calculators** let you write the hot path in a faster language:

| Language | Technology | Manage at |
| --- | --- | --- |
| Python | built-in / dotted path | n/a |
| Java | Py4J / JVM gateway | `/jvm`, `/help/py4j_integration` |
| C++ | pybind11 | `/cpp`, `/help/pybind11_integration` |
| Rust | PyO3 | `/rust`, `/help/rust_integration` |
| REST | HTTP/JSON service | `/help/rest_integration` |

Native modules are loaded/hot-reloaded from their management pages; the
help pages walk through building each one. For very large payloads, the
LMDB zero-copy transport (`lmdb://`) lets native calculators access data
by memory-map instead of serializing it.


---

## 7. Pub/Sub: Brokers, Cloud & Data Sources

Publishers and subscribers are chosen by a URI scheme in their `config`
(`destination` for publishers, `source` for subscribers). Add
`"resilient": true` to most schemes for retry / outage-buffering /
reconnect.

**Messaging brokers**

| Scheme | Service |
| --- | --- |
| `kafka://topic/<t>` | Apache Kafka |
| `rabbitmq://queue/<q>`, `rabbitmq://topic/<t>` | RabbitMQ |
| `activemq://queue/<q>`, `activemq://topic/<t>` | ActiveMQ |
| `tibcoems://queue/<q>`, `tibcoems://topic/<t>` | TIBCO EMS |
| `websphere://queue/<q>`, `websphere://topic/<t>` | IBM WebSphere MQ |
| `redis://`, `redischannel://` | Redis list / pub-sub |
| `mem://queue/<q>`, `mem://topic/<t>`, `inmemory://` | In-process |
| `lmdb://<channel>` | LMDB zero-copy cross-process |
| `grpc://` | gRPC stream |

**AWS & Azure managed messaging** (see also `docs/CONFIG_AND_CLOUD.md`)

| Scheme | Service | Notes |
| --- | --- | --- |
| `sqs://<queue>` | Amazon SQS | queue semantics, long-polling, FIFO |
| `kinesis://<stream>` | Amazon Kinesis | ordered sharded streams |
| `sns://<topic>` | Amazon SNS | publish-only fan-out (consume via SQS) |
| `servicebus://queue/<q>` or `topic/<t>/<sub>` | Azure Service Bus | queues + topics |
| `eventhubs://<hub>` | Azure Event Hubs | high-throughput streaming |

**Cloud object stores** (polling pub/sub)

| Scheme | Service |
| --- | --- |
| `s3://<bucket>/<prefix>` | Amazon S3 |
| `azureblob://<container>/<prefix>` | Azure Blob Storage |
| `gcs://<bucket>/<prefix>` | Google Cloud Storage |

**Other sources / sinks:** `file://`, `sql://`, `aerospike://`, `rest://`,
`custom://`, plus `fanin://a,b,c` (merge several subscribers) and
`composite://` for subscribers.

```jsonc
// AWS example
{"destination": "sqs://orders", "region": "us-east-1", "resilient": true}
{"source": "kinesis://ticks", "region": "us-east-1", "iterator_type": "LATEST"}

// Azure example
{"source": "servicebus://topic/ticks/risk",
 "connection_string": "${AZURE_SERVICEBUS_CONNECTION_STRING}"}
```

---

## 8. Storage Providers

DAG configs, holiday calendars, and other stored objects are read/written
through a pluggable storage provider, selected by `storage.provider`:

- `filesystem` (default) - `storage.filesystem.root`
- `s3` - bucket/prefix/region (`boto3`)
- `azureblob` - connection string + container (`azure-storage-blob`)
- `gcs` - bucket/prefix + credentials (`google-cloud-storage`)

See `/help/storage` for the per-provider properties. Switching providers is
purely a config change.

---

## 9. High Availability

Set `ha.provider` to elect a single PRIMARY across instances, with
automatic failover:

- `none` - single instance, always primary
- `zookeeper` - ZNode leader election (`kazoo`)
- `redis` - lease key with TTL (`redis`)
- `s3` - S3-object lease (`boto3`)
- `socket` - exclusive-socket election (no extra dependency)

On demotion a DAG server suspends its DAGs; on election it resumes. The
current role is shown in the navbar and on the dashboard. See `/help/ha`.

---

## 10. Market-Aware Scheduling

Add an optional time window and `schedule` block to any DAG. A DAG runs
only when the time window matches, the weekday is allowed, and the date is
not a holiday in **any** followed calendar:

```json
"start_time": "0930",
"duration": "6h30m",
"schedule": {
  "days_of_week": ["mon", "tue", "wed", "thu", "fri"],
  "exclude_days_of_week": [],
  "holiday_calendars": ["USA", "CANADA"],
  "timezone": "America/New_York"
}
```

- **Time window**: `start_time` (HHMM) plus `duration` (e.g. `6h30m`,
  `30m`, `7h`); windows that wrap past midnight are handled correctly.
- **Days**: `days_of_week` is an allow-list; `exclude_days_of_week` is a
  deny-list.
- **Timezone**: `timezone` is an IANA name (e.g. `America/New_York`,
  `Europe/London`, `UTC`) and defaults to **`America/New_York`**. The entire
  schedule — the time window *and* the day-of-week / holiday rules — is
  evaluated in this zone, **not** the server's local time. This matters
  because servers commonly run in UTC: a `0930-1600` market window means
  09:30-16:00 Eastern, so it stays correct whether the host clock is UTC or
  anything else. DST transitions are handled automatically.
- **Holidays**: NYSE and TSX calendars for 2026-2040 ship in
  `config/holidays/` (regenerate with
  `scripts/generate_holiday_calendars.py`).
- **Intraday edits** to calendar files or a DAG's schedule are honored by
  the running system within five minutes.

The two default DAGs in `config/dags/` ship with this US-equities-session
schedule. The dashboard's **Time Window** column shows the window plus
day-of-week and holiday-calendar badges. See `/help/scheduling` and
`/help/time_windows`.

---

## 11. Cloning DAGs

From the Dashboard, **Clone** copies a DAG under a new timestamped name.
The clone form lets you set a new time window (pre-filled from the
original). The parent's **day-of-week and holiday schedule is inherited**
by the clone automatically; only the time window is editable at clone time.
Cloned DAGs do not start automatically and never inherit autoclone.

**AutoClone** can duplicate a DAG on a configurable interval - see
`/help/autoclone`.

---

## 12. Users, Roles & API Keys

Users, roles, and API keys live in a relational database (SQLite by
default; PostgreSQL via `db.engine=postgresql` and
`config/schema/schema_postgres.sql`). Passwords are PBKDF2-SHA256 hashed.

- Manage accounts under **Users** (`/users`, admin only).
- The `admin` role unlocks administration (users, clone, native module
  management); other accounts get read/operate access.
- API keys authenticate programmatic `/api/` calls.

See `/help/database`.

---

## 13. Monitoring & Operations

- **Health**: `curl http://localhost:5002/health`
- **Prometheus metrics**: `curl http://localhost:5002/metrics`
- **System monitoring**: `/admin/monitoring` (live CPU/memory/disk/network)
- **Logs**: `/admin/logs` (view/filter/search/download, admin) and
  `/admin/logs/live` (SSE streaming, any authenticated user)
- **Worker pool**: `/admin/workers` to monitor/control worker processes

DAGs can run in the main process or be assigned to the worker pool for true
CPU parallelism (bypassing the GIL); see `/help/worker_pool`.

---

## 14. Tests

```bash
python -m pytest tests/ -q
```

The suite (108 passing, 1 skipped) covers the storage abstraction, the
database DAO layer, the user registry (including the one-time JSON
migration), HA election and socket failover, the resilient pub/sub
wrappers, the format-agnostic configuration system and `${VAR:default}`
resolution, the AWS/Azure pub/sub factory, market-aware scheduling and
time-window math, and the full web auth flow against the live FastAPI app.

---

## 15. Troubleshooting

| Symptom | Likely cause / fix |
| --- | --- |
| Server refuses to start, log names a property | A required property for the configured provider is missing - set it (fail-fast by design). |
| `SECRET_KEY` error on startup | Export `SECRET_KEY` or set `app.secret_key` in config. |
| A `pip install` is requested at runtime | An optional provider/broker SDK is needed for a scheme you used - install only that one (section 1). |
| DAG shows "Outside Window" and won't run | Current time/day/holiday is outside its schedule (section 10) - expected behavior. |
| Native (C++/Rust/Java) calculator unavailable | The module isn't built/loaded or its runtime is missing - see the `/jvm`, `/cpp`, `/rust` pages. |
| UI looks unstyled | Front-end assets are vendored under `web/static/vendor/`; ensure `/static` is being served. |

Log files are written under `logs/`. For anything else, the in-app
**Help** (`/help`) documents each subsystem in depth.

---

**DishtaYantra** | (c) 2025-2030 Ashutosh Sinha | Proprietary & Confidential
