# DishtaYantra v1.5.2 - Quick Start Guide

## 🚀 Getting Started

### 1. Extract the Archive

```bash
# For tar.gz
tar -xzvf dishtayantra_v1.5.2_complete.tar.gz

# For zip
unzip dishtayantra_v1.5.2_complete.zip

cd dishtayantra_pkg
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the Server

```bash
python run_server.py
```

Access the web interface at: **http://localhost:5002**

Default credentials:
- Username: `admin`
- Password: `admin`

---

## 🆕 New in v1.5.2: Multiprocessing Worker Pool

### What is the Worker Pool?

The **Worker Pool** enables true CPU parallelism by distributing DAGs across multiple worker processes. This bypasses Python's GIL limitation for CPU-bound calculations.

### Key Features

| Feature | Description |
|---------|-------------|
| **DAG Affinity** | DAGs stay on assigned workers for cache locality |
| **True Parallelism** | Multiple CPU cores used simultaneously |
| **Auto-Restart** | Crashed workers automatically restart |
| **LMDB Pub/Sub** | Cross-process communication via LMDB |
| **Smart Scheduling** | Load-balanced DAG assignment |

### Quick Configuration

Edit `config/worker_config.json`:

```json
{
    "worker_pool": {
        "enabled": true,
        "num_workers": 4,
        "auto_restart_on_crash": true
    },
    "affinity": {
        "default_strategy": "weight_based"
    }
}
```

### Pin a DAG to a Specific Worker

Add `worker_affinity` to your DAG configuration:

```json
{
    "name": "critical_trading_dag",
    "worker_affinity": {
        "pinned_worker": 0,
        "exclusive": false
    },
    "nodes": [...]
}
```

### LMDB Pub/Sub for Cross-Worker Communication

```json
{
    "subscribers": [{
        "name": "from_other_worker",
        "config": { "source": "lmdb://shared_channel" }
    }],
    "publishers": [{
        "name": "to_other_worker",
        "config": { "destination": "lmdb://shared_channel" }
    }]
}
```

### Access Worker Pool UI

Navigate to **Admin → Worker Pool** in the web interface to:
- View worker status and health
- Monitor CPU/memory usage
- Migrate DAGs between workers
- Restart workers manually

---

## 📦 Previous Features

### Subgraph Feature (v1.5.0)

Subgraphs are **modular, reusable computation units** that act as a single node:

- **Graph-as-a-Node**: Encapsulate complex pipelines
- **Light Up / Light Down**: Dynamic activation
- **Node Borrowing**: Share calculators with parent

### Run the Subgraph Demo

```bash
python example/subgraph_demo.py
```

---

## 📄 Documentation

Find comprehensive user guides at: **Help → User Guides**

Key guides:
- Worker Pool and DAG Affinity Guide
- Subgraph Feature Guide
- Kafka Integration Guide
- AutoClone Pattern Guide
- LMDB Store Guide

---

## 🔧 Supported Pub/Sub Protocols

| Protocol | Publisher | Subscriber |
|----------|-----------|------------|
| `mem://` | ✓ | ✓ |
| `lmdb://` | ✓ | ✓ |
| `kafka://` | ✓ | ✓ |
| `rabbitmq://` | ✓ | ✓ |
| `activemq://` | ✓ | ✓ |
| `redis://` | ✓ | ✓ |
| `tibcoems://` | ✓ | ✓ |
| `websphere://` | ✓ | ✓ |
| `file://` | ✓ | ✓ |
| `rest://` | ✓ | ✓ |

---

## 📊 Run Tests

```bash
# Run worker pool tests
python tests/test_worker_pool.py

# Run all tests
python -m pytest tests/
```

---

## 📁 Key Files

| File | Description |
|------|-------------|
| `config/worker_config.json` | Worker pool configuration |
| `config/application.properties` | Application settings |
| `config/dags/` | DAG configuration files |
| `docs/userguides/` | User documentation |

---

## ⚠️ Important Notes

1. **Worker Pool Requires**: Python 3.8+, psutil, lmdb
2. **Default Port**: 5002 (configurable in application.properties)
3. **SSL Support**: Configure cert/key in application.properties

---

**Copyright © 2025 Ashutosh Sinha. All rights reserved.**
