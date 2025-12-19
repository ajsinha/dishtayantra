# Subgraph Feature - Complete Documentation

## Overview

The Subgraph feature introduces a powerful abstraction for modular, reusable computation units within DishtaYantra DAGs. A subgraph is essentially a "graph-as-a-node" pattern, enabling hierarchical composition of processing pipelines.

**Patent Pending - DishtaYantra Framework**
**Copyright © 2025-2030 Ashutosh Sinha. All Rights Reserved.**

---

## Key Concepts

### What is a Subgraph?

A subgraph is a self-contained computation graph that acts as a single node within a parent graph. Key characteristics:

- **Cannot subscribe to external data publishers** - receives data only from parent graph
- **Connects to exactly one parent node** (entry connection)
- **Can output to one or more parent nodes** (exit connections)
- **Borrows calculators/transformers** from parent graph (no duplication)
- **Can be "lit up" or "lit down"** by supervisor
- **Supports synchronous or asynchronous execution**
- **Maximum nesting depth: 3 levels**

### Visual Representation

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           PARENT GRAPH                                          │
│                                                                                 │
│   ┌──────────┐      ┌──────────┐      ╔═══════════════════════════════════╗    │
│   │  Node A  │ ──▶  │  Node B  │ ──▶  ║         SUBGRAPH (as Node)        ║    │
│   │(Subscriber)     │(Calculator)     ║  ┌─────────────────────────────┐  ║    │
│   └──────────┘      └──────────┘      ║  │  ┌─────┐   ┌─────┐   ┌─────┐│  ║    │
│                                       ║  │  │ SG1 │ → │ SG2 │ → │ SG3 ││  ║    │
│                                       ║  │  │Entry│   │     │   │Exit ││  ║    │
│                                       ║  │  └─────┘   └─────┘   └─────┘│  ║    │
│                                       ║  └─────────────────────────────┘  ║    │
│                                       ╚════════════════╤══════════════════╝    │
│                                                        │                        │
│                                                        ▼                        │
│                                               ┌──────────┐      ┌──────────┐   │
│                                               │  Node C  │ ──▶  │  Node D  │   │
│                                               │(Calculator)     │(Publisher)   │
│                                               └──────────┘      └──────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Configuration

### Hybrid Loading (Inline + External File)

DishtaYantra supports two ways to define subgraphs:

#### 1. Inline Definition (in parent DAG JSON)

```json
{
  "name": "validation_subgraph",
  "type": "SubgraphNode",
  "config": {
    "entry_connection": "trade_input",
    "exit_connections": ["risk_subgraph"],
    "execution_mode": "synchronous"
  },
  "subgraph": {
    "name": "validation_pipeline",
    "description": "Inline subgraph for trade validation",
    "entry_node": "sg_validate",
    "exit_node": "sg_filter",
    "execution_mode": "synchronous",
    "dark_output_mode": "passthrough",
    
    "nodes": [
      {"name": "sg_validate", "borrows": "trade_validator", "role": "entry"},
      {"name": "sg_enrich", "borrows": "trade_enricher"},
      {"name": "sg_filter", "borrows": "trade_filter", "role": "exit"}
    ],
    
    "edges": [
      {"from": "sg_validate", "to": "sg_enrich"},
      {"from": "sg_enrich", "to": "sg_filter"}
    ]
  }
}
```

#### 2. External File Reference

**In parent DAG:**
```json
{
  "name": "risk_subgraph",
  "type": "SubgraphNode",
  "config": {
    "subgraph_file": "subgraphs/risk_calculation.json",
    "entry_connection": "validation_subgraph",
    "exit_connections": ["limit_check", "risk_output"],
    "execution_mode": "synchronous"
  }
}
```

**In `config/subgraphs/risk_calculation.json`:**
```json
{
  "name": "risk_calculation_subgraph",
  "type": "subgraph",
  "version": "1.0.0",
  "description": "Risk calculation pipeline",
  
  "entry_node": "sg_scenario_gen",
  "exit_node": "sg_pfe_output",
  
  "execution_mode": "synchronous",
  "dark_output_mode": "cached",
  "error_policy": "use_cached",
  
  "nodes": [
    {"name": "sg_scenario_gen", "borrows": "scenario_generator", "role": "entry"},
    {"name": "sg_pricer", "borrows": "trade_pricer"},
    {"name": "sg_netting", "borrows": "netting_calculator"},
    {"name": "sg_pfe_output", "borrows": "pfe_calculator", "role": "exit"}
  ],
  
  "edges": [
    {"from": "sg_scenario_gen", "to": "sg_pricer"},
    {"from": "sg_pricer", "to": "sg_netting"},
    {"from": "sg_netting", "to": "sg_pfe_output"}
  ]
}
```

### Configuration Options

| Option | Values | Description |
|--------|--------|-------------|
| `entry_node` | Node name | First node in subgraph (receives input from parent) |
| `exit_node` | Node name | Last node in subgraph (outputs to parent) |
| `execution_mode` | `synchronous`, `asynchronous` | How subgraph executes within parent |
| `dark_output_mode` | `cached`, `passthrough`, `block` | What to output when subgraph is suspended |
| `error_policy` | `propagate`, `use_cached`, `auto_suspend` | How to handle execution errors |

### Node Borrowing

Subgraph nodes **borrow** calculators from parent graph nodes:

```json
{
  "name": "sg_pricer",
  "borrows": "trade_pricer"
}
```

This means:
- `sg_pricer` uses the calculator defined in parent node `trade_pricer`
- No calculator duplication
- Parent must define all borrowed nodes

---

## Supervisor Control

### Light Up / Light Down

The supervisor can control subgraph states:

```python
# Activate a subgraph
dag.light_up_subgraph("risk_subgraph", reason="Market opened")

# Suspend a subgraph
dag.light_down_subgraph("risk_subgraph", reason="Market closed")

# Bulk operations
dag.light_up_all_subgraphs(reason="System startup")
dag.light_down_all_subgraphs(reason="Maintenance window")
```

### Subgraph States

```
┌─────────────┐
│   CREATED   │
└──────┬──────┘
       │ load()
       ▼
┌─────────────┐
│   LOADED    │
└──────┬──────┘
       │ bind_to_parent()
       ▼
┌─────────────┐       light_down()       ┌─────────────┐
│   ACTIVE    │ ◀────────────────────▶  │  SUSPENDED  │
│  (Lit Up)   │       light_up()        │ (Lit Down)  │
└─────────────┘                          └─────────────┘
```

### Behavior When Suspended (Dark)

| Dark Output Mode | Behavior |
|------------------|----------|
| `cached` | Returns last computed output |
| `passthrough` | Passes input directly to output |
| `block` | Returns `None`, blocking downstream |

---

## Dirty Propagation

### Active Subgraph

When a subgraph is **active**, dirty signals propagate through:

```
Node B (dirty=True)
    │
    ▼
╔═══════════════════╗
║ Subgraph (ACTIVE) ║
║   🔴→🔴→🔴        ║  ← Dirty propagates through
╚═════════╤═════════╝
          │
          ▼
Node C (dirty=True)  ← Marked dirty
```

### Suspended Subgraph

When a subgraph is **suspended**, dirty signals STOP:

```
Node B (dirty=True)
    │
    ▼
╔═══════════════════╗
║ Subgraph (DARK)   ║
║   ⬜→⬜→⬜        ║  ← NOT propagated
║  Using: CACHE     ║
╚═════════╤═════════╝
          │
          ▼
Node C (clean)  ← Uses cached output, NOT marked dirty
```

---

## API Reference

### Subgraph Class

```python
class Subgraph:
    """A self-contained computation graph that acts as a node."""
    
    # Properties
    is_active: bool      # True if subgraph is lit up
    is_suspended: bool   # True if subgraph is lit down
    entry_node: SubgraphNodeReference
    exit_node: SubgraphNodeReference
    node_count: int
    
    # Methods
    def activate(self, reason: str = None)
    def suspend(self, reason: str = None, resume_time: datetime = None)
    def execute(self, input_data: Any) -> Any
    def propagate_dirty(self)
    def get_internal_metrics(self) -> Dict[str, Dict]
    def to_dict(self) -> Dict[str, Any]
```

### SubgraphSupervisor Class

```python
class SubgraphSupervisor:
    """Supervisory controller for managing subgraph states."""
    
    def register_subgraph(self, subgraph_node: SubgraphNode)
    def unregister_subgraph(self, name: str)
    def light_up(self, subgraph_name: str, reason: str = None)
    def light_down(self, subgraph_name: str, reason: str = None, resume_time: datetime = None)
    def light_up_all(self, reason: str = None)
    def light_down_all(self, reason: str = None)
    def get_status(self) -> Dict[str, Dict[str, Any]]
    def process_control_message(self, message: Dict[str, Any])
```

### ComputeGraph Extensions

```python
class ComputeGraph:
    # New properties
    subgraph_nodes: Dict[str, SubgraphNode]
    subgraph_supervisor: SubgraphSupervisor
    
    # New methods
    def light_up_subgraph(self, name: str, reason: str = None)
    def light_down_subgraph(self, name: str, reason: str = None)
    def light_up_all_subgraphs(self, reason: str = None)
    def light_down_all_subgraphs(self, reason: str = None)
    def get_subgraph_status(self) -> Dict
```

---

## UI Integration

### DAG Details Page

The DAG Details page shows subgraph cards with:
- State indicator (🟢 Active / 🔴 Suspended)
- Node count and execution mode
- Entry/exit node names
- Metrics (execution count, latency, cache hits)
- Light Up / Light Down buttons
- Expandable internal node list

### Supervisor Controls

- **Light Up All** - Activate all subgraphs
- **Light Down All** - Suspend all subgraphs
- Per-subgraph controls with reason prompts

---

## REST API Endpoints

### Control Subgraph

```
POST /dag/<dag_name>/subgraph/control

Body:
{
  "command": "light_up" | "light_down" | "light_up_all" | "light_down_all",
  "subgraph": "subgraph_name",  // Required for single commands
  "reason": "optional reason"
}

Response:
{
  "success": true,
  "message": "Subgraph activated"
}
```

### Get Subgraph Status

```
GET /dag/<dag_name>/subgraph/status

Response:
{
  "dag_name": "trading_pipeline",
  "subgraph_count": 2,
  "subgraphs": {
    "validation_subgraph": {
      "state": "active",
      "is_active": true,
      "execution_mode": "synchronous",
      "node_count": 3,
      "metrics": {...}
    },
    "risk_subgraph": {...}
  }
}
```

---

## Best Practices

### When to Use Subgraphs

| Use Case | Recommendation |
|----------|----------------|
| Simple, one-off pipeline (2-3 nodes) | Inline definition |
| Reusable computation module | External file |
| Shared across teams | External file with versioning |
| Quick prototyping | Inline definition |
| Production deployment | External file (auditable) |

### Design Guidelines

1. **Keep subgraphs focused** - One responsibility per subgraph
2. **Use meaningful names** - `risk_calculation`, not `subgraph_1`
3. **Document borrowing** - Note which parent nodes are borrowed
4. **Plan for suspension** - Choose appropriate `dark_output_mode`
5. **Monitor metrics** - Track execution counts and latencies
6. **Limit nesting** - Max 3 levels of subgraphs

### Error Handling

```json
{
  "error_policy": "use_cached"
}
```

| Policy | Behavior |
|--------|----------|
| `propagate` | Raise error to parent graph |
| `use_cached` | Log error, return cached output |
| `auto_suspend` | Suspend subgraph, return cached output |

---

## Example: Complete Trading Pipeline

See `config/example/dags/trading_pipeline_with_subgraphs.json` for a complete example with:
- Trade ingestion
- Validation subgraph (inline)
- Risk calculation subgraph (references external file)
- Limit checking
- Alert generation

Run the demo script:
```bash
python example/subgraph_demo.py
```

---

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| "Cannot borrow from 'X' - not found" | Ensure parent graph defines the borrowed node |
| "Subgraph nesting depth exceeds maximum" | Reduce nesting (max 3 levels) |
| "Entry node should not have incoming edges" | Entry node should only receive from parent |
| "Exit node should not have outgoing edges" | Exit node should only output to parent |

### Debug Logging

Enable debug logging:
```python
import logging
logging.getLogger('core.dag.subgraph').setLevel(logging.DEBUG)
```

---

## Changelog

### Version 1.0.0

- Initial subgraph implementation
- Inline and external file loading
- Light up / light down control
- Supervisor bulk operations
- Dirty propagation control
- UI integration with DAG details page
- REST API for subgraph control
