# DishtaYantra v1.7.6 - Quick Start Guide

## 🚀 Getting Started

### 1. Extract the Archive

```bash
# For zip
unzip dishtayantra_v1_7_6_complete.zip

cd dishtayantra_enhanced
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the Server

```bash
python run_server.py
```

Access the web interface at: **http://localhost:5000**

Default credentials:
- Username: `admin`
- Password: `admin123`

---

## 🆕 New in v1.7.6: Kafka Resilience & Smart Deserialization

### Kafka Connection Resilience

Kafka connections now automatically retry on failure:

```properties
# config/application.properties
kafka.connection.max_retries=5
kafka.connection.retry_delay=3
kafka.connection.auto_reconnect=true
```

**Features:**
- Automatic retry on startup failures
- Configurable retry attempts and delay
- Automatic reconnection on broken connections
- Works with both kafka-python and confluent-kafka

### Template Properties Configuration

Author, copyright, and GitHub info now come from `application.properties`:

```properties
app.author.name=Ashutosh Sinha
app.author.email=ajsinha@gmail.com
app.copyright.years=2025-2030
app.github.repo=https://github.com/ajsinha/dishtayantra
```

### Smart Message Deserialization

All message brokers now automatically handle **both JSON and non-JSON messages**:

```bash
# Plain text now works - no JSON required!
echo "hello world" | kafka-console-producer --topic my_topic --bootstrap-server localhost:9092
```

**Transformation Examples:**
| Input Message | Output (to Calculator) |
|---------------|------------------------|
| `{"key": "value"}` | `{"key": "value"}` |
| `hello world` | `{"_raw_data": "hello world", "_auto_packaged": true}` |
| `[1, 2, 3]` | `{"_raw_data": [1,2,3], "_raw_type": "list", "_auto_packaged": true}` |

**Supported Brokers:** Kafka, ActiveMQ, RabbitMQ, TIBCO EMS, IBM MQ, Redis, gRPC

### Enhanced Logging Policy

- **Every message logged** on publish and receive
- **Full stack traces** for all exceptions
- Consistent logging format across all brokers

### Live Logs Streaming

Access real-time log streaming at **Admin → Live Logs**:

- **Server-Sent Events (SSE)** for live streaming
- **Level filtering**: DEBUG, INFO, WARNING, ERROR, CRITICAL
- **Source filtering**: DAG Server, Workers, Calculators, Pub/Sub
- **Search**: Real-time search through log messages
- **Pause/Resume**: Control stream without losing connection
- **Auto-scroll**: Follow new entries automatically
- **Statistics**: Total count, errors, warnings, log rate

### Research Documentation

Access the research paper at **Help → Research**:

- **22-page technical paper** with architecture diagrams
- **Multiple formats**: PDF, LaTeX, Markdown
- **Download support** for offline reading

---

## 🆕 New in v1.7.0: C++ & Rust Manager

### C++ Management (pybind11)

Access C++ management at **Admin → C++ Management**:

- Pre-configured calculators:
  - `MathCalculator`, `StatisticsCalculator`
  - `TradePricingCalculator`, `RiskMetricsCalculator`
  
- Use in DAG:
```json
{
    "calculator": {
        "type": "cpp",
        "name": "MathCalculator"
    }
}
```

### Rust Management (PyO3)

Access Rust management at **Admin → Rust Management**:

- Pre-configured calculators:
  - `MathCalculator`, `StatisticsCalculator`, `TradePricingCalculator`

- Use in DAG:
```json
{
    "calculator": {
        "type": "rust",
        "name": "StatisticsCalculator"
    }
}
```

---

## 🔧 Worker Pool & Multiprocessing

The Worker Pool enables **true CPU parallelism** by running DAGs across multiple worker processes:

### Enable Worker Pool

Edit `config/worker_config.json`:

```json
{
    "worker_pool": {
        "enabled": true,
        "num_workers": 4
    }
}
```

### Configure DAG Worker Affinity

```json
{
    "name": "my_dag",
    "worker_affinity": {
        "pinned_worker": 0
    }
}
```

### Cross-Worker Communication with LMDB

Publisher (DAG on Worker 0):
```json
"destination": "lmdb://shared_channel"
```

Subscriber (DAG on Worker 1):
```json
"source": "lmdb://shared_channel"
```

### Worker Pool UI

Access **Admin → Worker Pool** to:
- View all workers and their status
- Monitor CPU/memory usage per worker
- Migrate DAGs between workers
- Manually restart workers

---

## ⚡ LMDB Zero-Copy Data Exchange

For large payloads (100KB+), enable LMDB zero-copy:

```json
{
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

**Performance gains**: 100-250x faster than JSON serialization for large payloads.

---

## 📁 Key Files

| File | Description |
|------|-------------|
| `run_server.py` | Main entry point |
| `config/worker_config.json` | Worker pool settings |
| `config/cpp_config.json` | C++ calculator definitions |
| `config/rust_config.json` | Rust calculator definitions |
| `docs/research/` | Research paper (PDF, LaTeX, MD) |

---

## 📖 Documentation

| URL | Description |
|-----|-------------|
| `/help` | Help Center |
| `/help/research` | Research Paper |
| `/admin/logs/live` | Live Logs |
| `/admin/monitoring` | System Monitoring |
| `/admin/workers` | Worker Pool |
| `/cpp/management` | C++ Management |
| `/rust/management` | Rust Management |

---

## 📋 What's Included

- **Core DAG Engine** with subgraph support
- **Multi-Language Calculators**: Python, Java, C++, Rust, REST
- **10+ Message Brokers**: Kafka, RabbitMQ, ActiveMQ, Redis, TIBCO, IBM MQ, LMDB
- **LMDB Zero-Copy Transport**: Ultra-low latency data exchange
- **Web UI**: Dashboard, DAG Designer, Live Logs, State Viewer
- **Research Documentation**: 22-page technical paper
- **High Availability**: Zookeeper-based leader election

---

**DishtaYantra v1.7.6**
**Copyright © 2025-2030 Ashutosh Sinha. All Rights Reserved.**
