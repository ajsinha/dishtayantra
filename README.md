# DishtaYantra Compute Server

## © 2025-2030 Ashutosh Sinha

> *"The performance of C++, the safety of Rust, the ecosystem of Python, and the simplicity of execution."*

A high-performance, multi-threaded, and thread-safe DAG (Directed Acyclic Graph) compute server with support for multiple message brokers, data sources, **multi-language calculator integrations**, **LMDB zero-copy data exchange**, and **comprehensive research documentation**.

[![Version](https://img.shields.io/badge/version-1.7.6-blue.svg)](https://github.com/ajsinha/dishtayantra)
[![Python](https://img.shields.io/badge/python-3.8%2B-green.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-Proprietary-red.svg)](LICENSE)

---

## What's New in Version 1.7.6

### Kafka Connection Resilience (NEW)
- **Connection Retry with Backoff**: Configurable retry attempts (default: 5) with delay between attempts
- **Automatic Reconnection**: Broken connections are detected and recovered automatically
- **Both Libraries Enhanced**: kafka-python and confluent-kafka wrappers with full retry/recovery support
- **Configurable via Properties**: `kafka.connection.max_retries`, `kafka.connection.retry_delay`, `kafka.connection.auto_reconnect`

### Template Properties Injection (NEW)
- **Centralized Configuration**: Author email, copyright, GitHub repo now in `application.properties`
- **Dynamic Templates**: All HTML pages pull values from configuration
- **Easy Customization**: Change branding in one place, reflected everywhere

### Smart Message Deserialization
- **Universal Smart Deserializer**: Auto-packages non-JSON messages into dict format
- **Works Across All Brokers**: Kafka, ActiveMQ, RabbitMQ, TIBCO EMS, WebSphere MQ, Redis, gRPC
- **No Configuration Required**: Automatic handling of plain text, JSON, arrays, and binary data
- **Consistent Output**: DAG calculators always receive dict format regardless of source message format

### Enhanced Logging Policy
- **Message Tracking**: Every publish and receive is logged with source, type, and preview
- **Full Stack Traces**: All exceptions now include complete stack traces for easier debugging
- **Consistent Format**: Same logging pattern across all pubsub implementations

### Kafka Consumer Improvements
- **Poll-based Retrieval**: More reliable message consumption with external producers
- **Default `auto.offset.reset=earliest`**: New consumer groups read all messages from beginning
- **Both Libraries Updated**: kafka-python and confluent-kafka implementations enhanced

### Live Logs Streaming
- **Real-Time Log Viewer**: Server-Sent Events (SSE) for live log streaming
- **Auto-Reconnect**: Automatic reconnection on connection drops
- **Level & Source Filtering**: Filter by DEBUG, INFO, WARNING, ERROR, CRITICAL and source
- **Search**: Real-time search through log messages
- **Pause/Resume**: Control the stream without losing connection
- **Statistics Bar**: Total count, errors, warnings, log rate, buffer size
- **Connection Status**: Visual indicators for connection state

### Research Documentation
- **Comprehensive Research Paper**: 22-page technical paper with architecture diagrams
- **Multiple Formats**: Available in PDF, LaTeX, and Markdown
- **Web Integration**: Research section in help system with runtime rendering
- **Download Support**: Direct download links for all formats

### Version 1.7.0/1.7.1 Features

#### CPP Manager & pybind11 Integration (MAJOR)
- **Centralized C++ Management**: Web UI for C++ module administration
- **Pre-configured Calculators**: MathCalculator, StatisticsCalculator, TradePricingCalculator, RiskMetricsCalculator
- **Hot Module Loading**: Load/unload C++ modules without restart
- **Calculator Discovery**: Automatic discovery of available calculators
- **Health Monitoring**: Real-time status of C++ modules

#### Rust Manager & PyO3 Integration (MAJOR)
- **Centralized Rust Management**: Web UI for Rust module administration
- **Pre-configured Calculators**: MathCalculator, StatisticsCalculator, TradePricingCalculator
- **Hot Module Loading**: Dynamic Rust module management
- **LMDB Integration**: Zero-copy transport for Rust calculators
- **Thread-Safe Execution**: Leverages Rust's memory safety guarantees

---

## Features

### Core Capabilities
- **Multi-threaded DAG Execution**: Efficient parallel processing with topologically sorted node execution
- **Worker Pool**: Multiprocessing with DAG affinity for true CPU parallelism (bypasses GIL)
- **Subgraph Support**: Modular graph-as-a-node pattern with dynamic light up/down control
- **Multiple Message Brokers**: Kafka, ActiveMQ, RabbitMQ, Redis, TIBCO EMS, WebSphere MQ, In-Memory, LMDB
- **Various Data Sources**: File, SQL databases, Aerospike, Redis, REST, gRPC, custom implementations
- **Web UI**: Professional Flask-based dashboard with role-based access control
- **High Availability**: Zookeeper-based leader election for primary/failover setup
- **Time-windowed Execution**: Configure DAGs to run only during specific time windows
- **Dynamic Cloning**: Clone DAGs with different configurations at runtime
- **AutoClone**: Automatic DAG duplication with configurable intervals

### Multi-Language Calculators

| Language | Technology | Performance | Use Case |
|----------|------------|-------------|----------|
| Python | Built-in | Baseline | Rapid development, prototyping |
| Java | Py4J | 10-100x faster | JVM ecosystem, enterprise libraries |
| C++ | pybind11 | 50-100x faster | Ultra-low latency, SIMD, numeric |
| Rust | PyO3 | 50-100x faster | Memory safety, thread safety |
| REST | HTTP/JSON | Network dependent | Microservices, third-party APIs |

### LMDB Zero-Copy Data Exchange
- **Memory-Mapped Transport**: 100-1000x faster than serialization for large payloads
- **Zero-Copy I/O**: Native calculators access data directly via memory-mapped files
- **Multi-Language Support**: Works with Java (lmdbjava), C++ (liblmdb), Rust (lmdb-rs)
- **Automatic Threshold Detection**: Seamlessly switches based on payload size
- **Configurable**: Path and settings via application.properties

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
│   ├── dishtyantra_webapp.py       # Flask application
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
│   ├── users.json                  # User credentials
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
├── QUICKSTART_v1.7.0.md
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
- Web access: Flask console output

---

## Changelog

### Version 1.7.6 (Current)
- **Kafka Connection Resilience**: Retry with configurable attempts and automatic reconnection
- **Template Properties Injection**: Author, copyright, GitHub from application.properties
- **Smart Message Deserialization**: Auto-packages non-JSON messages into dict format
- **Universal Broker Support**: Works across Kafka, ActiveMQ, RabbitMQ, TIBCO EMS, IBM MQ, Redis, gRPC
- **Enhanced Logging Policy**: Every message logged on publish/receive with full stack traces
- **Kafka Improvements**: Poll-based retrieval, default `auto.offset.reset=earliest`
- Live Logs streaming with Server-Sent Events (SSE)
- Real-time log viewer with filtering and search
- Auto-reconnect and connection status indicators
- Research documentation web integration
- Version synchronization across all documentation

### Version 1.7.1
- Research paper expansion to 22 pages
- Comprehensive legal disclaimer
- ASCII architecture diagrams
- Workload descriptions (W1, W2)
- PDF and LaTeX generation

### Version 1.7.0
- CPP Manager for C++ module administration
- Rust Manager for Rust module administration
- Pre-configured calculators for C++ and Rust
- Web UI for language integration management
- Hot module loading support

### Version 1.6.0
- Worker Pool with multiprocessing
- DAG affinity scheduling
- LMDB cross-process pub/sub
- Health monitoring and auto-restart

### Version 1.5.x
- Subgraph feature (graph-as-a-node)
- Light up/down dynamic control
- Extended in-memory URI support

### Version 1.1.x
- LMDB zero-copy data exchange
- System monitoring dashboard
- Multi-language calculator support
- Python 3.13+ free-threading

### Version 1.0.0
- Initial release
- Multi-broker support
- Flask web UI
- Zookeeper HA

---

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

**DishtaYantra v1.7.6** | © 2025-2030 Ashutosh Sinha
