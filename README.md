# DishtaYantra Compute Server

## © 2025-2030 Ashutosh Sinha

> *"The performance of C++, the safety of Rust, the ecosystem of Python, and the simplicity of execution."*

A high-performance, multi-threaded, and thread-safe DAG (Directed Acyclic Graph) compute server with support for multiple message brokers, data sources, **multi-language calculator integrations**, and **LMDB zero-copy data exchange**.

[![Version](https://img.shields.io/badge/version-1.5.0-blue.svg)](https://github.com/ajsinha/dishtayantra)
[![Python](https://img.shields.io/badge/python-3.8%2B-green.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-Proprietary-red.svg)](LICENSE)

---

## What's New in Version 1.5.0

### PFE Research Module (NEW)
- **Comprehensive PFE Framework**: Real-time Potential Future Exposure calculations with Monte Carlo simulation
- **Realistic Data Generation**: Synthetic trades with full cashflow schedules, market data, counterparties, CSAs
- **SA-CCR Calculator**: Full Basel III/IV Standardized Approach implementation
- **CVA/DVA Calculator**: Credit Valuation Adjustment with spread sensitivity
- **Kafka Integration**: All data streams through Kafka for DAG consumption
- **Performance**: 31-56x speedup with C++ calculators vs pure Python
- **Research Paper**: Comprehensive research paper with theoretical foundations, 35+ references

### Subgraph Feature (Graph-as-a-Node)
- **Modular Composition**: Encapsulate complex pipelines as single nodes in parent graph
- **Light Up / Light Down**: Dynamically activate or suspend subgraphs at runtime
- **Node Borrowing**: Subgraph nodes borrow calculators from parent - no duplication
- **Hierarchical Nesting**: Support for up to 3 levels of nested subgraphs
- **Hybrid Loading**: Define inline or load from external JSON files
- **Supervisor Control**: Bulk operations to manage all subgraphs at once
- **Dirty Propagation Control**: Block dirty signals when subgraph is suspended

### LMDB Zero-Copy Data Exchange (v1.1.2 - Patent Pending)
- **Memory-Mapped Transport**: 100-1000x faster than serialization for large payloads
- **Zero-Copy I/O**: Native calculators access data directly via memory-mapped files
- **Multi-Language Support**: Works with Java (lmdbjava), C++ (liblmdb), Rust (lmdb-rs)
- **Automatic Threshold Detection**: Seamlessly switches based on payload size
- **Configurable**: Path and settings via application.properties

### Admin & System Monitoring (v1.1.1)
- **System Monitoring Dashboard**: Real-time CPU, memory, disk, and network monitoring
- **Admin Dropdown Menu**: Consolidated admin features in navigation bar
- **System Logs Viewer**: View, filter, search, and download application logs
- **Service Health Checks**: Monitor DAG server and integration status

### Multi-Language Calculator Support (v1.1.0)
- **Java (Py4J)**: 10-100x faster, JVM ecosystem access, gateway pooling
- **C++ (pybind11)**: 50-100x faster, ~100ns overhead, SIMD optimization, zero-copy
- **Rust (PyO3)**: C++ equivalent performance, memory safety, thread safety with rayon
- **REST API**: HTTP POST endpoints with API Key, Basic, Bearer authentication

### Python 3.13+ Free-Threading Support
- Optional GIL-free execution for true parallelism
- Automatic detection and configuration

---

## Features

### Core Capabilities
- **Multi-threaded DAG Execution**: Efficient parallel processing with topologically sorted node execution
- **Subgraph Support**: Modular graph-as-a-node pattern with dynamic light up/down control
- **Multiple Message Brokers**: Kafka, ActiveMQ, RabbitMQ, Redis, TIBCO EMS, WebSphere MQ, In-Memory
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

### Admin Features (v1.1.1)
- System monitoring with real-time metrics
- Process and resource tracking
- Log viewer with filtering and search
- Calculator integration status
- Service health checks

### Documentation & Help
- 12-page comprehensive help system
- Interactive DAG Designer
- 40+ term glossary
- Sample DAG configurations
- API reference documentation

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
│   │   ├── inmemory_redisclone.py  # In-memory Redis clone
│   │   └── ...                     # Other pub/sub implementations
│   ├── dag/
│   │   ├── graph_elements.py       # Node and Edge classes
│   │   ├── node_implementations.py # Concrete node types
│   │   ├── compute_graph.py        # ComputeGraph class
│   │   ├── dag_server.py           # DAGComputeServer
│   │   └── time_window_utils.py    # Time window handling
│   └── db/
│       ├── db_connection_pool.py   # Database connection pooling
│       └── db_connection_pool_manager.py
├── routes/
│   ├── auth_routes.py              # Authentication
│   ├── dashboard_routes.py         # Dashboard
│   ├── dag_routes.py               # DAG operations
│   ├── admin_routes.py             # System monitoring (NEW)
│   └── ...
├── web/
│   ├── dishtyantra_webapp.py       # Flask application
│   └── templates/
│       ├── base.html               # Base template with admin dropdown
│       ├── dashboard.html          # Main dashboard
│       ├── dag_designer.html       # Visual DAG designer
│       ├── admin/
│       │   ├── system_monitoring.html  # System metrics (NEW)
│       │   └── system_logs.html        # Log viewer (NEW)
│       └── help/
│           ├── index.html          # Help center
│           ├── py4j_integration.html
│           ├── pybind11_integration.html
│           ├── rust_integration.html
│           ├── rest_integration.html
│           └── ...
├── java/                           # Java calculator sources
│   └── src/com/dishtayantra/
├── cpp/                            # C++ calculator sources
│   └── src/
├── rust/                           # Rust calculator sources
│   └── src/
├── config/
│   ├── users.json                  # User credentials
│   └── dags/                       # DAG configurations
├── docs/
│   ├── requirements/               # Requirements documents
│   └── userguides/                 # User guides
├── requirements.txt
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

# Start server
python run_server.py
```

### Optional: Multi-Language Calculator Setup

#### Java (Py4J)
```bash
# Install Py4J
pip install py4j

# Compile Java classes
cd java
javac -cp .:py4j.jar src/com/dishtayantra/**/*.java

# Start Java gateway
java -cp .:py4j.jar:src com.dishtayantra.gateway.DishtaYantraGateway
```

#### C++ (pybind11)
```bash
# Install pybind11
pip install pybind11

# Build C++ module
cd cpp
mkdir build && cd build
cmake ..
make
cp dishtayantra_cpp*.so ../../
```

#### Rust (PyO3)
```bash
# Install maturin
pip install maturin

# Build Rust module
cd rust
maturin develop --release
```

---

## Configuration

### Users Configuration

Edit `config/users.json`:
```json
{
  "admin": {
    "password": "admin123",
    "roles": ["admin", "user"],
    "full_name": "System Administrator"
  },
  "operator": {
    "password": "operator123",
    "roles": ["user"],
    "full_name": "DAG Operator"
  }
}
```

### DAG Configuration

Create JSON files in `config/dags/` directory:

```json
{
  "name": "my_pipeline",
  "start_time": "0900",
  "duration": 480,
  "subscribers": [
    {
      "name": "input_sub",
      "config": { "source": "kafka://topic/input" }
    }
  ],
  "publishers": [
    {
      "name": "output_pub",
      "config": { "destination": "redis://output_key" }
    }
  ],
  "calculators": [
    {
      "name": "java_calc",
      "type": "com.example.MyCalculator",
      "config": { "calculator": "java" }
    }
  ],
  "nodes": [
    { "name": "input", "type": "SubscriptionNode", "subscriber": "input_sub" },
    { "name": "process", "type": "CalculationNode", "calculator": "java_calc" },
    { "name": "output", "type": "PublicationNode", "publisher": "output_pub" }
  ],
  "edges": [
    { "from": "input", "to": "process" },
    { "from": "process", "to": "output" }
  ]
}
```

### Calculator Configuration Examples

#### Java Calculator
```json
{
  "name": "risk_calc",
  "type": "com.company.RiskCalculator",
  "config": {
    "calculator": "java",
    "gateway_port": 25333,
    "pool_size": 4
  }
}
```

#### C++ Calculator
```json
{
  "name": "simd_calc",
  "type": "SimdVectorCalculator",
  "config": {
    "calculator": "cpp",
    "use_simd": true
  }
}
```

#### Rust Calculator
```json
{
  "name": "safe_calc",
  "type": "ThreadSafeCalculator",
  "config": {
    "calculator": "rust",
    "thread_count": 8
  }
}
```

#### REST Calculator
```json
{
  "name": "api_calc",
  "config": {
    "calculator": "rest",
    "endpoint": "https://api.example.com/calculate",
    "auth_type": "api_key",
    "api_key": "${API_KEY}",
    "timeout": 10,
    "retries": 3
  }
}
```

---

## Running the Application

### Start Server
```bash
python run_server.py
```

### Environment Variables
| Variable | Description | Default |
|----------|-------------|---------|
| `DAG_CONFIG_FOLDER` | Path to DAG configurations | `./config/dags` |
| `ZOOKEEPER_HOSTS` | Zookeeper connection string | `localhost:2181` |
| `USERS_FILE` | Path to users file | `./config/users.json` |
| `SECRET_KEY` | Flask session secret | (generated) |
| `FLASK_PORT` | Web server port | `5000` |

### Access Web Interface
Navigate to `http://localhost:5000` and login with credentials from `users.json`.

---

## Web Interface Guide

### Dashboard
- View all DAGs with status indicators
- Start/Stop/Suspend/Resume DAGs
- Clone DAGs with new time windows
- Archive/Delete DAGs
- Real-time status updates

### DAG Designer
- Visual drag-and-drop DAG creation
- Node palette with all node types
- Edge connection interface
- JSON configuration export

### Admin Features (Admin Role Required)
Access via **Admin** dropdown in navigation bar:

#### System Monitoring
- **CPU**: Usage percentage, core count, load average
- **Memory**: Usage, total, available
- **Disk**: Usage, partitions, free space
- **Network**: Bytes sent/received, connections, errors
- **DAG Server**: Running/stopped/error DAG counts
- **Process Info**: PID, memory, threads, open files
- **Calculator Status**: Java, C++, Rust, REST availability
- **Health Checks**: Service status indicators
- **Auto-refresh**: 5-second automatic updates

#### System Logs
- View application logs (dagserver.log, application.log, error.log)
- Filter by log level (Info, Warning, Error)
- Search log content
- Download logs

#### User Management
- Create/Edit/Delete users
- Assign roles (admin, user)
- Password management

### Help Center
Comprehensive documentation including:
- Getting Started guide
- DAG Configuration reference
- Calculator types and configuration
- Pub/Sub setup guides
- Multi-language integration guides
- API reference
- Glossary (40+ terms)

---

## Components Reference

### Publishers and Subscribers

| Protocol | URI Format | Example |
|----------|------------|---------|
| In-Memory | `mem://queue/name` | `mem://queue/input` |
| In-Memory Topic | `mem://topic/name` | `mem://topic/broadcast` |
| Kafka | `kafka://topic/name` | `kafka://topic/events` |
| Redis | `redis://key` | `redis://output_data` |
| Redis Channel | `redischannel://name` | `redischannel://updates` |
| RabbitMQ | `rabbitmq://queue/name` | `rabbitmq://queue/tasks` |
| ActiveMQ | `activemq://queue/name` | `activemq://queue/jobs` |
| File | `file:///path` | `file:///data/output.json` |
| SQL | `sql://source` | `sql://trades_db` |
| REST | `rest://endpoint` | `rest://api/data` |
| Aerospike | `aerospike://ns/set` | `aerospike://test/users` |

### Built-in Calculators

| Calculator | Description |
|------------|-------------|
| `NullCalculator` | Returns deep copy of input |
| `PassthruCalculator` | Returns input unchanged |
| `AttributeFilterCalculator` | Keeps specified attributes |
| `AttributeFilterAwayCalculator` | Removes specified attributes |
| `ApplyDefaultsCalculator` | Applies default values |
| `AdditionCalculator` | Adds numeric attributes |
| `MultiplicationCalculator` | Multiplies numeric attributes |
| `AttributeNameChangeCalculator` | Renames attributes |
| `RandomCalculator` | Generates random values |

### Transformers

| Transformer | Description |
|-------------|-------------|
| `NullDataTransformer` | Returns deep copy |
| `PassthruDataTransformer` | Returns unchanged |
| `AttributeFilterDataTransformer` | Keeps attributes |
| `AttributeFilterAwayDataTransformer` | Removes attributes |
| `ApplyDefaultsDataTransformer` | Applies defaults |

### Node Types

| Node Type | Description |
|-----------|-------------|
| `SubscriptionNode` | Ingests data from subscriber |
| `PublicationNode` | Publishes to publisher(s) |
| `CalculationNode` | Performs calculations |
| `TransformationNode` | Transforms data |
| `MetronomeNode` | Periodic execution |
| `SinkNode` | Consumes without output |

---

## Performance Characteristics

### Calculator Performance Comparison

| Type | Overhead | Throughput | Best For |
|------|----------|------------|----------|
| Python | Baseline | ~10K ops/s | Prototyping, simple logic |
| Java (Py4J) | 100-500μs | ~50K ops/s | JVM libraries, enterprise |
| C++ (pybind11) | ~100ns | ~1M ops/s | Ultra-low latency, SIMD |
| Rust (PyO3) | ~100ns | ~1M ops/s | Safety-critical, parallel |
| REST | 10-1000ms | ~100 ops/s | External services, APIs |

### System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 2 cores | 8+ cores |
| RAM | 2 GB | 8+ GB |
| Disk | 1 GB | 10+ GB |
| Python | 3.8 | 3.13+ (free-threading) |

---

## High Availability

The system uses Zookeeper for leader election:

1. **Primary Instance**: Actively runs DAGs, processes messages
2. **Standby Instance**: Monitors primary, ready for failover

When primary fails, standby automatically:
- Becomes primary
- Resumes all suspended DAGs
- Continues processing

```bash
# Start with HA
export ZOOKEEPER_HOSTS=zk1:2181,zk2:2181,zk3:2181
python run_server.py
```

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
| GET | `/help` | Help center |

---

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| DAG won't start | Check if PRIMARY, verify JSON syntax |
| Messages not flowing | Verify subscriber connections, check queue depths |
| High memory | Reduce max_depth, check for accumulation |
| Java calc fails | Ensure gateway is running, check port |
| C++/Rust not found | Verify module compilation, check Python path |
| REST timeout | Increase timeout, verify endpoint |

### Log Locations
- Application: `logs/dagserver.log`
- Errors: `logs/error.log`
- Web access: Flask console output

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

## Changelog

### Version 1.5.0 (Current)
- Subgraph feature (graph-as-a-node pattern)
- Light up / light down dynamic control
- Supervisor bulk operations
- Node borrowing from parent graph
- Hybrid loading (inline + external file)
- Dirty propagation control when suspended
- REST API for subgraph control

### Version 1.1.2
- LMDB zero-copy data exchange (Patent Pending)
- Memory-mapped transport for native calculators
- 100-1000x faster for large payloads
- Multi-language LMDB support (Java, C++, Rust)

### Version 1.1.1
- System Monitoring dashboard with real-time metrics
- Admin dropdown menu in navigation
- System logs viewer with filtering
- Service health checks
- Calculator integration status display
- Auto-refresh monitoring

### Version 1.1.0
- Java calculator integration (Py4J)
- C++ calculator integration (pybind11)
- Rust calculator integration (PyO3)
- REST API calculator integration
- Python 3.13+ free-threading support
- Comprehensive help documentation
- DAG Designer improvements

### Version 1.0.0
- Initial release
- Multi-broker support
- Flask web UI
- Zookeeper HA
- Time-windowed execution
- DAG cloning

---

## Contact & Support

- **Author**: Ashutosh Sinha
- **Email**: ajsinha@gmail.com
- **GitHub**: https://github.com/ajsinha/dishtayantra

---

## Legal Information

### Patent Notice

**PATENT PENDING**: The following technologies implemented in DishtaYantra are the subject of one or more pending patent applications:

#### 1. LMDB Zero-Copy Data Exchange System
- Automatic payload size detection for LMDB routing decisions
- Unified reference protocol for heterogeneous language calculator integration
- Transaction-based zero-copy data exchange between Python and native code (Java, C++, Rust)
- Memory-mapped file transport with automatic TTL-based cleanup
- Format-agnostic serialization layer for cross-language data exchange

#### 2. Multi-Language Calculator Framework
- Hot-swappable calculator integration architecture
- Unified calculator interface across Python, Java, C++, and Rust
- Gateway pooling system for Java/JVM integration (Py4J)
- Native binding abstraction layer for C++ (pybind11) and Rust (PyO3)
- Automatic language detection and routing
- Zero-copy data passing between language boundaries

#### 3. DAG Execution Engine
- Real-time topological sort with dynamic node insertion
- Time-windowed execution scheduling
- AutoClone feature for automatic DAG replication
- Multi-broker message routing architecture

#### 4. Free-Threading Support System
- Automatic GIL-free execution detection for Python 3.13+
- Thread-safe calculator invocation framework
- Parallel DAG node execution without GIL constraints

Unauthorized use, reproduction, or implementation of these technologies may constitute patent infringement.

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

**DishtaYantra v1.5.0** | Patent Pending | © 2025-2030 Ashutosh Sinha
