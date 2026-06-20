# DishtaYantra Compute Server

## © 2025-2030 Ashutosh Sinha

> *"The performance of C++, the safety of Rust, the ecosystem of Python, and the simplicity of execution."*

A high-performance, multi-threaded, and thread-safe DAG (Directed Acyclic Graph) compute server with support for multiple message brokers, data sources, **multi-language calculator integrations**, **LMDB zero-copy data exchange**, and **comprehensive research documentation**.

[![Version](https://img.shields.io/badge/version-5.16.2-blue.svg)](https://github.com/ajsinha/dishtayantra)
[![Python](https://img.shields.io/badge/python-3.8%2B-green.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-Proprietary-red.svg)](LICENSE)

---

## Highlights

DishtaYantra is an enterprise DAG compute engine for real-time data
pipelines: polyglot calculators wired to twenty-plus message brokers and
cloud services, supervised with high availability and market-aware
schedule management. Recent capabilities include:

- **Format-agnostic configuration** - YAML (recommended) or classic
  `.properties`, with `${VAR:default}` resolution, nested references, and
  environment / command-line overrides
- **AWS & Azure managed messaging** - SQS, Kinesis, SNS, Azure Service Bus,
  and Event Hubs, alongside the existing Kafka/RabbitMQ/ActiveMQ/TIBCO/MQ
  and cloud object-store transports
- **Market-aware scheduling** - time windows with duration syntax
  (e.g. `09:30` + `6h30m`), day-of-week allow/deny lists, and USA/Canada
  market-holiday calendars, with intraday edits honored within five minutes
- **FastAPI web layer** with full light/dark theming and self-hosted
  front-end assets (renders fully in air-gapped deployments)
- **Pluggable storage** (filesystem, S3, Azure Blob, GCS), database-backed
  users with roles and API keys, and a configurable high-availability
  manager (Zookeeper, Redis, S3-lease, or socket)

## Features

### Core Capabilities
- **Multi-threaded DAG Execution**: Efficient parallel processing with topologically sorted node execution
- **Worker Pool**: Multiprocessing with DAG affinity for true CPU parallelism (bypasses GIL)
- **Arrow columnar data plane (opt-in, additive)**: `ArrowCalculator` calculators vectorize batched messages with Apache Arrow / `pyarrow.compute`, output-identical to their row counterparts. They are drop-in `DataCalculator`s, so the engine is unchanged and old-style (row) and new-style (Arrow) nodes/calculators **coexist in the same instance and even in the same graph** (see `docs/design/A1-worked-example-and-coexistence.md` and `perftest/run_arrow_example.py`)
- **Async egress (opt-in, WAL-backed)**: publication can be decoupled from the compute thread — `publish()` becomes a non-blocking append to a per-destination write-ahead log drained by a bounded worker pool (default 4), with per-destination FIFO ordering, durable at-least-once resume, portable stdlib WAL backends (filelog/sqlite), per-publisher opt-in (in-memory destinations stay inline), and massively parallel egress in worker mode (see the *Async Egress* guide + tutorials)
- **Backpressure (opt-in)**: credit-based flow control between publisher and subscriber (block or drop)
- **Subgraph Support**: Modular graph-as-a-node pattern with dynamic light up/down control
- **Multiple Message Brokers**: Kafka, ActiveMQ, RabbitMQ, Redis, TIBCO EMS, WebSphere MQ, In-Memory, LMDB
- **AWS & Azure Messaging**: SQS, Kinesis, SNS, Azure Service Bus, Event Hubs
- **Cloud Object-Store Pub/Sub**: Amazon S3, Azure Blob Storage, Google Cloud Storage
- **Various Data Sources**: File, SQL databases, Aerospike, Redis, REST, gRPC, custom implementations
- **Web UI**: Professional FastAPI-based dashboard with role-based access control and light/dark theming
- **High Availability**: Configurable primary election and failover (Zookeeper, Redis, S3-lease, or socket)
- **Market-Aware Scheduling**: Time windows with duration syntax, day-of-week allow/deny lists, and USA/Canada market-holiday calendars; intraday edits honored within five minutes
- **Format-Agnostic Configuration**: YAML or `.properties`, with `${VAR:default}` resolution and env/command-line overrides
- **Dynamic Cloning**: Clone DAGs with different time windows at runtime (day/holiday schedule inherited)
- **AutoClone**: Automatic DAG duplication with configurable intervals

### Multi-Language Calculators

| Language | Technology | Performance | Use Case |
|----------|------------|-------------|----------|
| Python | Built-in | Baseline | Rapid development, prototyping |
| Java | Py4J | 10-100x faster | JVM ecosystem, enterprise libraries |
| C++ | pybind11 | 50-100x faster | Ultra-low latency, SIMD, numeric |
| Rust | PyO3 | 50-100x faster | Memory safety, thread safety |
| REST | HTTP/JSON | Network dependent | Microservices, third-party APIs |

### Arrow Columnar Data Plane (opt-in)

`ArrowCalculator` (in `core/calculator/arrow_calculator.py`) is an **additive**,
opt-in calculator contract that processes a columnar batch with
`pyarrow.compute` kernels. Because it subclasses `DataCalculator`, every Arrow
calculator is a **drop-in** for a row calculator: the engine is not modified, so
row (old-style) and Arrow (new-style) calculators **coexist in the same instance
and in the same graph**. Vectorized stages are output-identical to their row
counterparts (validated in CI) and deliver their speedup on *batched* message
flow (~11.8x at the kernel level).

- Worked example & decision tree: `docs/design/A1-worked-example-and-coexistence.md`
- Tutorial (hands-on): `docs/TUTORIAL_arrow_dag.md`
- Runnable demos: `python -m perftest.run_arrow_example` (mixed graph), `python -m perftest.run_autobatch_example` (automatic source-batching), and `python -m perftest.run_arrow_transport_example` (zero-copy transport)

Two **opt-in** node types make batching automatic without changing producers or
consumers: `BatchingSubscriptionNode` drains incoming messages into columnar
batches, and `FlatteningPublicationNode` unbatches on the way out so the external
per-message contract is preserved. The existing `SubscriptionNode` /
`PublicationNode` are unchanged, so existing DAGs are unaffected.

For maximum throughput, the **zero-copy transport** path carries an immutable
`pyarrow.RecordBatch` *on the edges* (shared by reference, no per-stage copy) via
`ArrowBatchingSubscriptionNode` / `ArrowFlatteningPublicationNode` — ~2.29x the
dict-envelope path with byte-identical output, while the equality-gate invariant is
preserved (`core/dag/edge_value.py`). The dict path is behaviourally unchanged.
- Design RFCs: `docs/design/A1-arrow-data-plane.md`, `docs/design/A1-recordbatch-edges.md`

### Headless Execution & Orchestration (opt-in)

Run a DAG to completion without the web UI (`python -m core.dag.headless_runner`),
and have a long-running control-plane instance dispatch ephemeral, isolated
headless workers for heavy run-to-completion ETL (`JobDispatchCalculator`:
idempotent, async, a bounded worker pool, reactive completion events). Additive —
the web app / engine paths are untouched.
- Tutorial (hands-on): `docs/TUTORIAL_headless.md`
- Reference: `docs/HEADLESS_AND_ORCHESTRATION.md`
- Runnable demo: `python -m perftest.run_orchestration_example`

### End-of-Day Batch Processing
Process large interrelated batch files (trade facts enriched against FX/limit
dimensions), with clear guidance on when records can be vectorized in parallel and
when a true cross-record dependency needs one-at-a-time ordering.
- Tutorial (hands-on): `docs/TUTORIAL_eod_batch.md`
- Reference: `docs/BATCH_FILE_PROCESSING.md`
- Runnable demo: `python -m perftest.run_eod_example`

### LMDB Zero-Copy Data Exchange
- **Memory-Mapped Transport**: 100-1000x faster than serialization for large payloads
- **Zero-Copy I/O**: Native calculators access data directly via memory-mapped files
- **Multi-Language Support**: Works with Java (lmdbjava), C++ (liblmdb), Rust (lmdb-rs)
- **Automatic Threshold Detection**: Seamlessly switches based on payload size
- **Configurable**: Path and settings via application.yaml (or .properties)

### Admin Features
- **System Monitoring Dashboard**: Real-time CPU, memory, disk, and network monitoring
- **Live Logs**: Real-time log streaming with SSE
- **System Logs Viewer**: View, filter, search, and download application logs
- **Worker Pool Management**: Monitor and control worker processes
- **JVM Management**: Java gateway status and control
- **C++ Management**: pybind11 module administration
- **Rust Management**: PyO3 module administration
- **Service Health Checks**: Monitor DAG server and integration status

### Documentation & Help
- **20+ page help system** with comprehensive guides
- **Interactive DAG Designer** for visual workflow creation
- **50+ term glossary** for reference
- **Sample DAG configurations** with examples
- **API reference documentation**
- **Research paper** with architecture details

---

## Project Structure

```
dishtayantra/
├── core/
│   ├── calculator/
│   │   ├── core_calculator.py      # Calculator factory & implementations
│   │   ├── java_calculator.py      # Py4J Java integration
│   │   ├── cpp_calculator.py       # pybind11 C++ integration
│   │   ├── rust_calculator.py      # PyO3 Rust integration
│   │   └── rest_calculator.py      # REST API integration
│   ├── transformer/
│   │   └── core_transformer.py     # Data transformers
│   ├── pubsub/
│   │   ├── datapubsub.py           # Abstract base classes
│   │   ├── pubsubfactory.py        # Factory methods
│   │   ├── kafka_datapubsub.py     # Kafka pub/sub
│   │   ├── rabbitmq_datapubsub.py  # RabbitMQ pub/sub
│   │   ├── redis_datapubsub.py     # Redis pub/sub
│   │   ├── lmdb_datapubsub.py      # LMDB pub/sub (cross-process)
│   │   └── ...                     # Other pub/sub implementations
│   ├── dag/
│   │   ├── graph_elements.py       # Node and Edge classes
│   │   ├── node_implementations.py # Concrete node types
│   │   ├── compute_graph.py        # ComputeGraph class
│   │   ├── dag_server.py           # DAGComputeServer
│   │   └── time_window_utils.py    # Time window handling
│   └── worker/
│       └── worker_pool.py          # Multiprocessing worker pool
├── routes/
│   ├── auth_routes.py              # Authentication
│   ├── dashboard_routes.py         # Dashboard
│   ├── dag_routes.py               # DAG operations
│   ├── admin_routes.py             # System monitoring & live logs
│   ├── worker_routes.py            # Worker pool management
│   ├── jvm_routes.py               # JVM/Java management
│   ├── cpp_routes.py               # C++ management
│   ├── rust_routes.py              # Rust management
│   └── noauth_routes.py            # Public routes (help, about)
├── web/
│   ├── dishtayantra_webapp.py      # FastAPI application
│   ├── static/
│   │   └── research/               # Research paper files (PDF, LaTeX, MD)
│   └── templates/
│       ├── base.html               # Base template with navigation
│       ├── dashboard.html          # Main dashboard
│       ├── admin/
│       │   ├── system_monitoring.html
│       │   ├── system_logs.html
│       │   ├── live_logs.html      # Live log streaming (NEW)
│       │   └── workers.html
│       ├── dag/
│       │   └── designer.html       # Visual DAG designer
│       ├── jvm/                    # JVM management pages
│       ├── cpp/                    # C++ management pages
│       ├── rust/                   # Rust management pages
│       └── help/
│           ├── index.html          # Help center
│           ├── research.html       # Research paper viewer
│           └── ...                 # 20+ help pages
├── java/                           # Java calculator sources
│   └── src/com/dishtayantra/
├── cpp/                            # C++ calculator sources
│   └── src/
├── rust/                           # Rust calculator sources
│   └── src/
├── config/
│   ├── application.yaml             # Main config (or application.properties)
│   ├── users.json                  # Legacy users, migrated to DB on first boot
│   ├── worker_config.json          # Worker pool configuration
│   ├── cpp_config.json             # C++ module configuration
│   ├── rust_config.json            # Rust module configuration
│   └── dags/                       # DAG configurations
├── docs/
│   ├── research/                   # Research paper (PDF, LaTeX, MD)
│   ├── requirements/               # Requirements documents
│   └── userguides/                 # User guides
├── logs/                           # Application logs
├── requirements.txt
├── QUICKSTART.md
└── README.md
```

---

## Installation

### Prerequisites
- Python 3.8+ (3.13+ recommended for free-threading)
- pip package manager

### Basic Installation

```bash
# Clone repository
git clone https://github.com/ajsinha/dishtayantra.git
cd dishtayantra

# Install Python dependencies
pip install -r requirements.txt

# Create directories
python setup_directories.py

# Start the server
python run_server.py
```

### Access the Web UI
Open http://localhost:5000 in your browser.

**Default Credentials:**
- Username: `admin`
- Password: `admin123`

---

## Quick Start

> For the complete, comprehensive walkthrough - install, configuration,
> the web console, building DAGs, all calculators and brokers, storage, HA,
> scheduling, cloning, users, monitoring, and troubleshooting - see
> **[QUICKSTART.md](QUICKSTART.md)**. The example below is a brief taste.

### 1. Create a Simple DAG

```json
{
    "name": "simple_pipeline",
    "start_time": "0000",
    "duration": "24h",
    "nodes": [
        {
            "name": "source",
            "type": "SubscriptionNode",
            "subscribers": [{"uri": "kafka://localhost:9092/input_topic"}]
        },
        {
            "name": "processor",
            "type": "CalculationNode",
            "calculator": {"type": "python", "name": "SimpleCalculator"}
        },
        {
            "name": "sink",
            "type": "PublicationNode",
            "publishers": [{"uri": "kafka://localhost:9092/output_topic"}]
        }
    ],
    "edges": [
        {"from": "source", "to": "processor"},
        {"from": "processor", "to": "sink"}
    ]
}
```

### 2. Using C++ Calculator (High Performance)

```json
{
    "name": "processor",
    "type": "CalculationNode",
    "calculator": {
        "type": "cpp",
        "name": "MathCalculator"
    }
}
```

### 3. Using Rust Calculator (Memory Safe)

```json
{
    "name": "processor",
    "type": "CalculationNode",
    "calculator": {
        "type": "rust",
        "name": "StatisticsCalculator"
    }
}
```

### 4. Using LMDB Zero-Copy for Large Payloads

```json
{
    "name": "processor",
    "type": "CalculationNode",
    "calculator": {
        "type": "cpp",
        "name": "TradePricingCalculator",
        "lmdb_exchange": {
            "enabled": true,
            "mode": "both"
        }
    }
}
```

---

## Performance

### Calculator Performance Comparison

| Calculator Type | Latency | Throughput | Best For |
|-----------------|---------|------------|----------|
| Python | ~100μs | ~10K/s | Prototyping |
| Java (Py4J) | ~1ms | ~1K/s | JVM libraries |
| C++ (pybind11) | ~100ns | ~1M/s | Ultra-low latency |
| Rust (PyO3) | ~100ns | ~1M/s | Safe high performance |
| REST | 10-1000ms | ~100/s | External services |

### LMDB Zero-Copy Performance

| Payload Size | JSON Serialization | LMDB Zero-Copy | Improvement |
|--------------|-------------------|----------------|-------------|
| 1 KB | 50 μs | 5 μs | 10x |
| 100 KB | 5 ms | 50 μs | 100x |
| 1 MB | 50 ms | 200 μs | 250x |
| 10 MB | 500 ms | 2 ms | 250x |

### System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 2 cores | 8+ cores |
| RAM | 2 GB | 8+ GB |
| Disk | 1 GB | 10+ GB |
| Python | 3.8 | 3.13+ (free-threading) |

---

## API Reference

### DAGComputeServer Methods

```python
# DAG Management
dag_name = server.add_dag(config_dict, filename)
server.start(dag_name)
server.stop(dag_name)
server.suspend(dag_name)
server.resume(dag_name)
server.delete(dag_name)
new_name = server.clone_dag(dag_name, start_time, end_time)

# Information
details = server.details(dag_name)
dags = server.list_dags()
status = server.get_server_status()
```

### REST Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Dashboard |
| GET | `/dag/<n>` | DAG details |
| POST | `/dag/<n>/start` | Start DAG |
| POST | `/dag/<n>/stop` | Stop DAG |
| POST | `/dag/<n>/suspend` | Suspend DAG |
| POST | `/dag/<n>/resume` | Resume DAG |
| DELETE | `/dag/<n>` | Delete DAG |
| GET | `/admin/monitoring` | System monitoring |
| GET | `/admin/logs` | System logs |
| GET | `/admin/logs/live` | Live log streaming |
| GET | `/admin/workers` | Worker pool management |
| GET | `/jvm/management` | JVM/Java management |
| GET | `/cpp/management` | C++ management |
| GET | `/rust/management` | Rust management |
| GET | `/help` | Help center |
| GET | `/help/research` | Research paper |

---

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| DAG won't start | Check if PRIMARY, verify JSON syntax |
| Messages not flowing | Verify subscriber connections, check queue depths |
| High memory | Reduce max_depth, check for accumulation |
| Java calc fails | Ensure gateway is running, check port 25333 |
| C++ module not found | Verify compilation, check `cpp_config.json` |
| Rust module not found | Run `maturin develop`, check `rust_config.json` |
| REST timeout | Increase timeout, verify endpoint |
| Live logs disconnected | Check SSE support, refresh page |

### Log Locations
- Application: `logs/dagserver.log`
- Errors: `logs/error.log`
- Web access: uvicorn / FastAPI console output

---

## Version & History

Current version: **5.16.2** (the authoritative version is always
`core/version.py::VERSION`, which every module, template, and banner imports —
nothing hard-codes a version string). DishtaYantra is developed as a continuously
evolving system; rather than a release-by-release changelog, the current
capabilities are described in the **Highlights** and **Features** sections
above, and per-version highlights live in `core/version.py`. For details on
specific subsystems see:

- `docs/CONFIG_AND_CLOUD.md` - YAML configuration and AWS/Azure messaging
- `docs/ARCHITECTURE.md` - system architecture
- The in-app **Help** and **About** pages

## Security Considerations

⚠️ **For Production Use**:

1. **Hash passwords**: Use bcrypt/argon2 instead of cleartext
2. **Enable HTTPS**: Use SSL/TLS certificates
3. **Secure credentials**: Use environment variables for API keys
4. **Network security**: Configure firewalls, use VPN
5. **Audit logging**: Enable comprehensive logging
6. **Regular updates**: Keep dependencies current

---

## Contact & Support

- **Author**: Ashutosh Sinha
- **Email**: ajsinha@gmail.com
- **GitHub**: https://github.com/ajsinha/dishtayantra

---

## Legal Information

### Copyright Notice

© 2025-2030 Ashutosh Sinha. All rights reserved.

DishtaYantra, including all source code, documentation, algorithms, designs, and associated intellectual property, is the exclusive property of Ashutosh Sinha.

### License

This software is proprietary and confidential. Unauthorized copying, modification, distribution, or use of this software, in whole or in part, is strictly prohibited without the express written permission of the copyright holder.

### Trademarks

DishtaYantra™ is a trademark of Ashutosh Sinha. All other trademarks mentioned herein are the property of their respective owners.

### Disclaimer

THIS SOFTWARE IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY ARISING FROM THE USE OF THIS SOFTWARE.

---

**DishtaYantra** | © 2025-2030 Ashutosh Sinha
