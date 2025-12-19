# DishtaYantra v1.2.0 - Quick Start Guide

## 🚀 Getting Started

### 1. Extract the Archive

```bash
# For tar.gz
tar -xzvf dishtayantra_v1.2.0_subgraph.tar.gz

# For zip
unzip dishtayantra_v1.2.0_subgraph.zip

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

Access the web interface at: **http://localhost:5000**

Default credentials:
- Username: `admin`
- Password: `admin`

---

## 🆕 New in v1.2.0: Subgraph Feature

### What are Subgraphs?

Subgraphs are **modular, reusable computation units** that act as a single node within a parent graph. Key capabilities:

- **Graph-as-a-Node**: Encapsulate complex pipelines as single nodes
- **Light Up / Light Down**: Dynamically activate or suspend subgraphs at runtime
- **Node Borrowing**: Subgraph nodes borrow calculators from parent - no duplication
- **Hierarchical Composition**: Nest subgraphs up to 3 levels deep

### Run the Subgraph Demo

```bash
python example/subgraph_demo.py
```

This demonstrates:
- ✓ Inline subgraph definition
- ✓ External file subgraph loading
- ✓ Light up / light down control
- ✓ Supervisor bulk control
- ✓ Dirty propagation behavior
- ✓ Subgraph metrics
- ✓ Node borrowing from parent graph

### Example Subgraph Configuration

**Inline Definition:**
```json
{
  "name": "validation_subgraph",
  "type": "SubgraphNode",
  "config": {
    "execution_mode": "synchronous"
  },
  "subgraph": {
    "name": "validation_pipeline",
    "entry_node": "sg_validate",
    "exit_node": "sg_filter",
    "nodes": [
      {"name": "sg_validate", "borrows": "validator"},
      {"name": "sg_filter", "borrows": "filter"}
    ],
    "edges": [
      {"from": "sg_validate", "to": "sg_filter"}
    ]
  }
}
```

**External File Reference:**
```json
{
  "name": "risk_subgraph",
  "type": "SubgraphNode",
  "config": {
    "subgraph_file": "subgraphs/risk_calculation.json"
  }
}
```

### REST API for Subgraph Control

```bash
# Light up a subgraph
curl -X POST http://localhost:5000/dag/my_dag/subgraph/control \
  -H "Content-Type: application/json" \
  -d '{"command": "light_up", "subgraph": "risk_subgraph", "reason": "Market opened"}'

# Light down all subgraphs
curl -X POST http://localhost:5000/dag/my_dag/subgraph/control \
  -H "Content-Type: application/json" \
  -d '{"command": "light_down_all", "reason": "Maintenance"}'

# Get status
curl http://localhost:5000/dag/my_dag/subgraph/status
```

---

## 📁 Key Files

| File | Description |
|------|-------------|
| `run_server.py` | Main entry point |
| `core/dag/subgraph.py` | Subgraph implementation |
| `example/subgraph_demo.py` | Demonstration script |
| `config/subgraphs/` | External subgraph definitions |
| `config/example/dags/trading_pipeline_with_subgraphs.json` | Complete example DAG |

---

## 📖 Documentation

- **Help Center**: http://localhost:5000/help
- **Architecture**: http://localhost:5000/help/architecture
- **Subgraph Guide**: http://localhost:5000/help/subgraph

---

## 📋 What's Included

- **Core DAG Engine** with subgraph support
- **Multi-Language Calculators**: Python, Java, C++, Rust, REST
- **10+ Message Brokers**: Kafka, RabbitMQ, ActiveMQ, Redis, TIBCO, IBM MQ
- **LMDB Zero-Copy Transport**: Ultra-low latency data exchange
- **Web UI**: Dashboard, DAG Designer, State Viewer
- **High Availability**: Zookeeper-based leader election

---

**Patent Pending - DishtaYantra Framework**
**Copyright © 2025-2030 Ashutosh Sinha. All Rights Reserved.**
