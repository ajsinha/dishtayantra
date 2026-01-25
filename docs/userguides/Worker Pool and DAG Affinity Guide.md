# Worker Pool and DAG Affinity Guide

**DishtaYantra v1.5.2**

---

## Overview

DishtaYantra v1.5.2 introduces a **multiprocessing worker pool** that enables true CPU parallelism by distributing DAGs across multiple worker processes. Each worker process runs in its own Python interpreter, bypassing the GIL (Global Interpreter Lock) limitation.

### Key Benefits

| Feature | Benefit |
|---------|---------|
| **True Parallelism** | Multiple CPU cores used simultaneously |
| **DAG Affinity** | DAGs stay on assigned workers for cache locality |
| **Fault Isolation** | Worker crash doesn't affect other workers |
| **Auto-Restart** | Crashed workers automatically restart with their DAGs |
| **Smart Scheduling** | Load-balanced DAG assignment |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      ORCHESTRATOR (Main Process)                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ Web Server   │  │ DAG Manager  │  │ Affinity Manager     │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
│                            │                                     │
│            ┌───────────────┼───────────────┐                    │
│            ▼               ▼               ▼                    │
│      [Control Q]     [Control Q]     [Control Q]                │
└─────────────────────────────────────────────────────────────────┘
             │               │               │
             ▼               ▼               ▼
      ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
      │  Worker 0   │ │  Worker 1   │ │  Worker 2   │
      │             │ │             │ │             │
      │  DAG: A     │ │  DAG: B     │ │  DAG: C     │
      │  DAG: D     │ │  DAG: E     │ │  DAG: F     │
      │             │ │             │ │             │
      │  CPU Core 0 │ │  CPU Core 1 │ │  CPU Core 2 │
      └─────────────┘ └─────────────┘ └─────────────┘
```

---

## Configuration

### Worker Pool Configuration

All worker pool configuration is managed via `config/worker_config.json`:

```json
{
    "worker_pool": {
        "enabled": true,
        "num_workers": 4,
        "min_workers": 2,
        "max_workers": 32,
        "auto_restart_on_crash": true,
        "max_restart_attempts": 3,
        "restart_backoff_seconds": 5,
        "health_check_interval_seconds": 5,
        "worker_startup_timeout_seconds": 30,
        "worker_shutdown_timeout_seconds": 10,
        "status_report_interval_seconds": 2
    },
    
    "affinity": {
        "default_strategy": "weight_based",
        "rebalance_enabled": true,
        "rebalance_threshold": 0.3,
        "rebalance_interval_seconds": 60,
        "allow_dag_pinning": true,
        "allow_exclusive_workers": true
    },
    
    "communication": {
        "control_queue_maxsize": 1000,
        "status_queue_maxsize": 5000,
        "use_lmdb_for_cross_worker": true,
        "lmdb_path": "data/worker_lmdb",
        "lmdb_map_size_mb": 1024
    },
    
    "monitoring": {
        "collect_cpu_metrics": true,
        "collect_memory_metrics": true,
        "collect_dag_metrics": true,
        "metrics_history_size": 1000
    },
    
    "defaults": {
        "dag_cpu_weight": 0.2,
        "dag_priority": 5
    }
}
```

### Configuration Options

#### Worker Pool Section

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `true` | Enable/disable worker pool |
| `num_workers` | int/string | `4` | Number of workers (use `"auto"` for CPU count) |
| `min_workers` | int | `2` | Minimum workers when using "auto" |
| `max_workers` | int | `32` | Maximum workers when using "auto" |
| `auto_restart_on_crash` | bool | `true` | Auto-restart crashed workers |
| `max_restart_attempts` | int | `3` | Max consecutive restart attempts |
| `restart_backoff_seconds` | int | `5` | Initial backoff before restart (exponential) |
| `health_check_interval_seconds` | int | `5` | Health check frequency |
| `worker_startup_timeout_seconds` | int | `30` | Timeout waiting for worker to start |
| `worker_shutdown_timeout_seconds` | int | `10` | Timeout waiting for worker to stop |
| `status_report_interval_seconds` | int | `2` | How often workers send status |

#### Affinity Section

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `default_strategy` | string | `"weight_based"` | DAG assignment strategy |
| `rebalance_enabled` | bool | `true` | Enable automatic rebalancing |
| `rebalance_threshold` | float | `0.3` | Load difference to trigger rebalance |
| `rebalance_interval_seconds` | int | `60` | Rebalance check interval |
| `allow_dag_pinning` | bool | `true` | Allow DAGs to pin to workers |
| `allow_exclusive_workers` | bool | `true` | Allow exclusive worker assignment |

#### Communication Section

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `control_queue_maxsize` | int | `1000` | Max control messages per worker |
| `status_queue_maxsize` | int | `5000` | Max status messages in queue |
| `use_lmdb_for_cross_worker` | bool | `true` | Enable LMDB pub/sub |
| `lmdb_path` | string | `"data/worker_lmdb"` | LMDB database path |
| `lmdb_map_size_mb` | int | `1024` | LMDB max size in MB |

### Assignment Strategies

| Strategy | Description |
|----------|-------------|
| `round_robin` | Simple rotation through workers |
| `weight_based` | Balance based on estimated CPU weight |
| `least_loaded` | Assign to worker with lowest CPU usage |
| `random` | Random assignment |

---

## DAG Worker Affinity Configuration

You can control which worker a DAG is assigned to by adding `worker_affinity` to the DAG configuration:

### Pin to Specific Worker

```json
{
    "name": "critical_trading_dag",
    "worker_affinity": {
        "pinned_worker": 0
    },
    "nodes": [...]
}
```

### Exclusive Worker

```json
{
    "name": "high_frequency_dag",
    "worker_affinity": {
        "exclusive": true
    },
    "nodes": [...]
}
```

### Preferred Workers

```json
{
    "name": "analytics_dag",
    "worker_affinity": {
        "preferred_workers": [1, 2]
    },
    "nodes": [...]
}
```

### Full Example

```json
{
    "name": "production_pricing",
    "priority": 10,
    
    "worker_affinity": {
        "pinned_worker": 0,
        "exclusive": false,
        "preferred_workers": [0, 1]
    },
    
    "subscribers": [
        {
            "name": "price_feed",
            "config": {
                "source": "kafka://topic/prices",
                "bootstrap_servers": ["localhost:9092"]
            }
        }
    ],
    
    "nodes": [...]
}
```

---

## Cross-Worker Communication with LMDB

Use LMDB-based pub/sub for efficient cross-worker data sharing:

### LMDB Publisher Configuration

```json
{
    "name": "cross_worker_publisher",
    "config": {
        "destination": "lmdb://shared_channel",
        "lmdb_path": "data/worker_lmdb",
        "map_size_mb": 1024
    }
}
```

### LMDB Subscriber Configuration

```json
{
    "name": "cross_worker_subscriber",
    "config": {
        "source": "lmdb://shared_channel",
        "lmdb_path": "data/worker_lmdb",
        "poll_interval_ms": 10,
        "auto_package_non_dict": true
    }
}
```

### Example: Cross-Worker Pipeline

**DAG A (Worker 0)** → LMDB → **DAG B (Worker 1)**

DAG A (producer):
```json
{
    "name": "dag_a",
    "worker_affinity": { "pinned_worker": 0 },
    "publishers": [
        {
            "name": "to_dag_b",
            "config": { "destination": "lmdb://a_to_b_channel" }
        }
    ]
}
```

DAG B (consumer):
```json
{
    "name": "dag_b", 
    "worker_affinity": { "pinned_worker": 1 },
    "subscribers": [
        {
            "name": "from_dag_a",
            "config": { "source": "lmdb://a_to_b_channel" }
        }
    ]
}
```

---

## Web UI

Access the Worker Pool Management page at:

```
http://localhost:5000/admin/workers
```

### Features

1. **Pool Status Overview**
   - Total workers
   - Healthy/unhealthy count
   - Total DAGs loaded

2. **Worker Cards**
   - CPU usage with visual bar
   - Memory usage
   - Loaded DAGs list
   - Restart count
   - Manual restart button

3. **DAG Assignments Table**
   - View all DAG-to-worker mappings
   - Migrate DAGs between workers

4. **Pool Controls**
   - Start/Stop pool
   - Manual refresh

---

## API Endpoints

### Get Pool Status

```bash
GET /api/workers/status
```

Response:
```json
{
    "running": true,
    "num_workers": 4,
    "healthy_workers": 4,
    "total_dags": 10,
    "workers": {...},
    "dag_assignments": {...}
}
```

### Get Worker Details

```bash
GET /api/workers/{worker_id}
```

### Restart Worker

```bash
POST /api/workers/{worker_id}/restart
```

### Migrate DAG

```bash
POST /api/workers/dag/{dag_name}/migrate
Content-Type: application/json

{"to_worker": 2}
```

### Start/Stop Pool

```bash
POST /api/workers/pool/start
POST /api/workers/pool/stop
```

---

## Health Monitoring and Auto-Restart

### How It Works

1. **Heartbeat Monitoring**: Workers send heartbeats every 2 seconds
2. **Unhealthy Detection**: No heartbeat for 3 intervals = unhealthy
3. **Crash Detection**: Process not alive = crashed
4. **Auto-Restart**: Crashed workers restart with exponential backoff
5. **DAG Restoration**: DAGs are automatically reloaded on restarted worker

### Restart Backoff

| Attempt | Backoff |
|---------|---------|
| 1 | 5 seconds |
| 2 | 10 seconds |
| 3 | 20 seconds |
| 4+ | Max 60 seconds |

### Manual Restart

Via UI or API:
```bash
POST /api/workers/0/restart
```

---

## Best Practices

### 1. Co-locate Related DAGs

Put DAGs that share data or calculators on the same worker:

```json
{
    "name": "pricing_dag",
    "worker_affinity": { "pinned_worker": 0 }
}
{
    "name": "risk_dag",
    "worker_affinity": { "pinned_worker": 0 }
}
```

### 2. Use Exclusive Workers for Critical DAGs

```json
{
    "name": "trading_engine",
    "worker_affinity": { "exclusive": true }
}
```

### 3. Use LMDB for Cross-Worker Communication

LMDB is memory-mapped and supports multiple readers across processes.

### 4. Monitor Worker Health

- Check the UI regularly
- Set up alerts for unhealthy workers
- Review restart counts

### 5. Size Workers Appropriately

- Start with `4` workers (default), or use `"auto"` for CPU count
- Reduce if memory constrained
- Increase for I/O heavy workloads

---

## Troubleshooting

### Worker Won't Start

1. Check logs: `logs/worker_{id}.log`
2. Verify LMDB path is writable
3. Check for port conflicts

### High Restart Count

1. Check DAG for errors
2. Review memory usage
3. Look for resource leaks

### DAGs Not Loading

1. Verify DAG config is valid
2. Check calculator availability
3. Review worker logs

### Cross-Worker Data Not Flowing

1. Verify LMDB path is same for all
2. Check channel names match
3. Ensure subscriber is polling

---

## Files Added in v1.5.2

| File | Description |
|------|-------------|
| `config/worker_config.json` | Worker pool configuration |
| `core/workers/__init__.py` | Workers module init |
| `core/workers/worker_pool.py` | Pool manager |
| `core/workers/worker_process.py` | Worker process class |
| `core/workers/dag_affinity.py` | Affinity manager |
| `core/workers/worker_monitor.py` | Health monitoring |
| `core/pubsub/lmdbpubsub.py` | LMDB pub/sub |
| `routes/worker_routes.py` | API routes |
| `web/templates/admin/workers.html` | UI template |

---

## Copyright

**© 2025 Ashutosh Sinha. All rights reserved.**
