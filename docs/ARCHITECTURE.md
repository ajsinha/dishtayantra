# DishtaYantra Architecture Document

## Version 2.2

© 2025-2030 Ashutosh Sinha

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture Layers](#architecture-layers)
3. [Core Components](#core-components)
4. [Worker Pool Architecture](#worker-pool-architecture)
5. [JVM Manager Architecture](#jvm-manager-architecture)
6. [Multi-Language Calculator Architecture](#multi-language-calculator-architecture)
7. [LMDB Zero-Copy Data Exchange](#lmdb-zero-copy-data-exchange)
8. [Pub/Sub Framework](#pubsub-framework)
9. [DAG Execution Engine](#dag-execution-engine)
10. [Web Application Architecture](#web-application-architecture)
11. [Admin & Monitoring System](#admin--monitoring-system)
12. [Prometheus Metrics Integration](#prometheus-metrics-integration)
13. [High Availability](#high-availability)
14. [Market-Aware Scheduling](#market-aware-scheduling)
15. [Security Architecture](#security-architecture)
16. [Performance Considerations](#performance-considerations)
17. [Deployment Architecture](#deployment-architecture)

---

## System Overview

DishtaYantra is a high-performance, multi-threaded DAG (Directed Acyclic Graph) compute server designed for real-time data processing pipelines. The system supports multiple message brokers, data sources, multi-language calculator integrations, LMDB zero-copy data exchange, multiprocessing worker pool with DAG affinity, and comprehensive Prometheus monitoring.

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DishtaYantra v1.6.0                            │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐ │
│  │   Web UI     │  │  REST API    │  │   Admin      │  │   Worker    │ │
│  │  Dashboard   │  │  Endpoints   │  │  Monitoring  │  │   Pool UI   │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬──────┘ │
│         │                 │                 │                  │        │
│  ┌──────┴─────────────────┴─────────────────┴──────────────────┴──────┐ │
│  │                      FastAPI Application Layer                        │ │
│  │            (Routes, Authentication, Session Management)             │ │
│  └────────────────────────────────┬────────────────────────────────────┘ │
│                                   │                                      │
│  ┌────────────────────────────────┴────────────────────────────────────┐ │
│  │                     Worker Pool Manager                     │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────────┐  │ │
│  │  │  Affinity   │  │   Health    │  │    LMDB     │  │   Auto     │  │ │
│  │  │  Manager    │  │   Monitor   │  │   Pub/Sub   │  │  Restart   │  │ │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └────────────┘  │ │
│  └────────────────────────────────┬────────────────────────────────────┘ │
│                                   │                                      │
│  ┌────────────────────────────────┴────────────────────────────────────┐ │
│  │                        DAG Compute Server                            │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────────┐  │ │
│  │  │ ComputeGraph│  │   Node      │  │   Time      │  │  Zookeeper │  │ │
│  │  │   Manager   │  │  Executor   │  │   Windows   │  │     HA     │  │ │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └────────────┘  │ │
│  └────────────────────────────────┬────────────────────────────────────┘ │
│                                   │                                      │
│  ┌────────────────────────────────┴────────────────────────────────────┐ │
│  │                    Core Processing Components                        │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐ │ │
│  │  │                   Calculator Framework                           │ │ │
│  │  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐        │ │ │
│  │  │  │ Python │ │  Java  │ │  C++   │ │  Rust  │ │  REST  │        │ │ │
│  │  │  │Built-in│ │ (Py4J) │ │pybind11│ │ (PyO3) │ │  API   │        │ │ │
│  │  │  └────────┘ └────────┘ └────────┘ └────────┘ └────────┘        │ │ │
│  │  └─────────────────────────────────────────────────────────────────┘ │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐ │ │
│  │  │              LMDB Zero-Copy Transport (v1.1.2)                   │ │ │
│  │  │  ┌─────────────────────────────────────────────────────────────┐ │ │ │
│  │  │  │ Memory-Mapped Files │ 100-1000x Faster │ High Performance    │ │ │ │
│  │  │  └─────────────────────────────────────────────────────────────┘ │ │ │
│  │  └─────────────────────────────────────────────────────────────────┘ │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐ │ │
│  │  │                  Pub/Sub Framework                               │ │ │
│  │  │  ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐   │ │ │
│  │  │  │ Kafka │ │Rabbit │ │ Redis │ │Active │ │ File  │ │  LMDB │   │ │ │
│  │  │  │       │ │  MQ   │ │       │ │  MQ   │ │       │ │       │   │ │ │
│  │  │  └───────┘ └───────┘ └───────┘ └───────┘ └───────┘ └───────┘   │ │ │
│  │  └─────────────────────────────────────────────────────────────────┘ │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐ │ │
│  │  │                  Transformer Framework                           │ │ │
│  │  └─────────────────────────────────────────────────────────────────┘ │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐ │ │
│  │  │               Prometheus Metrics & Monitoring                    │ │ │
│  │  │  ┌─────────────────────────────────────────────────────────────┐ │ │ │
│  │  │  │ /metrics │ /health │ Grafana Dashboard │ Alert Rules       │ │ │ │
│  │  │  └─────────────────────────────────────────────────────────────┘ │ │ │
│  │  └─────────────────────────────────────────────────────────────────┘ │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Architecture Layers

### Layer 1: Presentation Layer
- **Web UI**: FastAPI + Jinja2 templates with Bootstrap 5 (vendored locally), light/dark theming
- **REST API**: JSON endpoints for programmatic access
- **Admin Interface**: System monitoring, logs, user management
- **Help Center**: Documentation and guides

### Layer 2: Application Layer
- **FastAPI Application**: Request routing, session management, SSE streaming
- **Authentication**: Role-based access control (RBAC)
- **Route Handlers**: Modular route organization

### Layer 3: Business Logic Layer
- **DAG Compute Server**: DAG lifecycle management
- **Compute Graph**: Graph building and execution
- **Node Executor**: Parallel node processing
- **Time Windows**: Scheduled execution management

### Layer 4: Integration Layer
- **Calculator Framework**: Multi-language calculation support
- **Pub/Sub Framework**: Message broker integrations
- **Transformer Framework**: Data transformation pipeline

### Layer 5: Infrastructure Layer
- **Zookeeper**: Leader election, distributed coordination
- **Database Connections**: Connection pooling
- **Caching**: In-memory data storage

---

## Core Components

### DAG Compute Server

The central orchestrator managing all DAG operations.

```
┌────────────────────────────────────────────────────────────────┐
│                     DAGComputeServer                            │
├────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌────────────────┐  │
│  │  DAG Registry   │  │  Config Loader  │  │  HA Manager    │  │
│  │  (Thread-safe)  │  │                 │  │  (Zookeeper)   │  │
│  └────────┬────────┘  └────────┬────────┘  └───────┬────────┘  │
│           │                    │                    │           │
│  ┌────────┴────────────────────┴────────────────────┴────────┐  │
│  │                    DAG Operations                          │  │
│  │   add_dag(), start(), stop(), suspend(), resume(),        │  │
│  │   delete(), clone_dag(), list_dags(), details()           │  │
│  └───────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

### Compute Graph

Represents a single DAG with all its nodes and edges.

```
┌────────────────────────────────────────────────────────────────┐
│                      ComputeGraph                               │
├────────────────────────────────────────────────────────────────┤
│  Configuration:                                                 │
│  ├── name: string                                               │
│  ├── start_time: HHMM                                           │
│  ├── duration: minutes                                          │
│  └── auto_clone: boolean                                        │
│                                                                 │
│  Components:                                                    │
│  ├── nodes: Dict[name, Node]                                    │
│  ├── edges: List[Edge]                                          │
│  ├── subscribers: Dict[name, DataSubscriber]                    │
│  ├── publishers: Dict[name, DataPublisher]                      │
│  ├── calculators: Dict[name, DataCalculator]                    │
│  └── transformers: Dict[name, DataTransformer]                  │
│                                                                 │
│  Execution:                                                     │
│  ├── topological_sort() → List[Node]                            │
│  ├── executor_thread: Thread                                    │
│  └── status: running | stopped | suspended | error              │
└────────────────────────────────────────────────────────────────┘
```

### Node Types

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ SubscriptionNode│     │ CalculationNode │     │ PublicationNode │
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│ - subscriber    │ ──▶ │ - calculator    │ ──▶ │ - publishers[]  │
│ - input_queue   │     │ - input_queue   │     │ - input_queue   │
│ - output_data   │     │ - output_data   │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘

┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│TransformNode    │     │  MetronomeNode  │     │    SinkNode     │
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│ - transformer   │     │ - interval_ms   │     │ - discard input │
│ - input_queue   │     │ - output_data   │     │ - no output     │
│ - output_data   │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

---

## Worker Pool Architecture

### Overview

The Worker Pool provides true CPU parallelism by distributing DAG execution across multiple worker processes, bypassing Python's Global Interpreter Lock (GIL).

```
┌─────────────────────────────────────────────────────────────────┐
│                      MAIN PROCESS (Orchestrator)                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ Web Server  │  │ DAG Manager │  │ Affinity Manager        │  │
│  │             │  │             │  │ (weight_based strategy) │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│         │              │                      │                  │
│  ┌──────┴──────────────┴──────────────────────┴───────────────┐ │
│  │                 Worker Pool Manager                         │ │
│  │  ┌─────────────────┐  ┌────────────────────────────────┐   │ │
│  │  │ Health Monitor  │  │ Status Processing Thread       │   │ │
│  │  │ (heartbeats)    │  │ (worker status aggregation)    │   │ │
│  │  └─────────────────┘  └────────────────────────────────┘   │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
              │ Control Queues            │ Status Queue
              ▼                           ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│   Worker 0      │ │   Worker 1      │ │   Worker 2      │
│ ┌─────────────┐ │ │ ┌─────────────┐ │ │ ┌─────────────┐ │
│ │   DAG_A     │ │ │ │   DAG_B     │ │ │ │   DAG_C     │ │
│ │   DAG_D     │ │ │ │   DAG_E     │ │ │ │   DAG_F     │ │
│ └─────────────┘ │ │ └─────────────┘ │ │ └─────────────┘ │
│   (Process)     │ │   (Process)     │ │   (Process)     │
└─────────────────┘ └─────────────────┘ └─────────────────┘
         │                  │                   │
         └──────────────────┴───────────────────┘
                           │
                    LMDB (shared data)
```

### Components

| Component | Description |
|-----------|-------------|
| **WorkerPoolManager** | Orchestrates worker lifecycle, DAG assignments, and health monitoring |
| **DAGWorkerProcess** | Individual worker process that runs assigned DAGs |
| **DAGAffinityManager** | Determines optimal DAG-to-worker assignments |
| **WorkerHealthMonitor** | Tracks worker health via heartbeats, triggers auto-restart |
| **LMDBDataPublisher/Subscriber** | Cross-process pub/sub for inter-worker communication |

### DAG Affinity Strategies

1. **weight_based** (default): Assigns DAGs to the worker with lowest estimated load
2. **round_robin**: Distributes DAGs evenly across workers
3. **least_loaded**: Uses actual CPU metrics for assignment
4. **random**: Random assignment for testing

### Worker Affinity Configuration

```json
{
    "name": "critical_dag",
    "worker_affinity": {
        "pinned_worker": 0,      // Force to Worker 0
        "exclusive": true,       // Get dedicated worker
        "preferred_workers": [0, 1]  // Preference list
    }
}
```

### Health Monitoring & Auto-Restart

```
                    ┌───────────────────┐
                    │  WorkerHealthMonitor │
                    └─────────┬─────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
     ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
     │ Heartbeat   │  │ Process     │  │ Auto        │
     │ Tracking    │  │ Liveness    │  │ Restart     │
     │ (2s interval)│  │ Check       │  │ (backoff)   │
     └─────────────┘  └─────────────┘  └─────────────┘
```

- **Heartbeat Interval**: 2 seconds
- **Unhealthy Threshold**: 3 missed heartbeats (15 seconds)
- **Restart Backoff**: Exponential (5s, 10s, 20s, max 60s)
- **Max Restart Attempts**: Configurable (default: 3)

### Cross-Worker Communication (LMDB Pub/Sub)

```
┌─────────────────┐                    ┌─────────────────┐
│   Worker 0      │                    │   Worker 1      │
│ ┌─────────────┐ │                    │ ┌─────────────┐ │
│ │ Publisher   │ │                    │ │ Subscriber  │ │
│ │ lmdb://chan │─┼──────────────────▶│ │ lmdb://chan │ │
│ └─────────────┘ │     LMDB File      │ └─────────────┘ │
└─────────────────┘    (memory-mapped) └─────────────────┘
```

Benefits:
- Zero-copy reads across processes
- Multiple readers simultaneously
- ACID compliant data integrity
- Persistent across worker restarts

---

## JVM Manager Architecture

### Overview

The JVM Manager provides centralized management of Java Virtual Machine instances and Py4J gateway connections for Java calculator integration.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        JVM Manager (v1.6.0)                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────┐    ┌───────────────────────────────────────────┐  │
│  │  jvm_config.json │───▶│              JVMManager                   │  │
│  │                  │    │  - Gateway configs                        │  │
│  │  - gateways[]    │    │  - Calculator definitions                 │  │
│  │  - calculators   │    │  - Health monitoring thread               │  │
│  │  - jvm options   │    │  - Config change detection                │  │
│  └──────────────────┘    └───────────────────┬───────────────────────┘  │
│                                              │                           │
│                    ┌─────────────────────────┼─────────────────────────┐ │
│                    │                         │                         │ │
│              ┌─────┴─────┐             ┌─────┴─────┐             ┌─────┴─────┐
│              │ Gateway 1 │             │ Gateway 2 │             │ Gateway N │
│              │ "primary" │             │"secondary"│             │  "custom" │
│              │           │             │           │             │           │
│              │ Py4J Pool │             │ Py4J Pool │             │ Py4J Pool │
│              │ (4 conn)  │             │ (2 conn)  │             │ (N conn)  │
│              └─────┬─────┘             └─────┬─────┘             └─────┬─────┘
│                    │                         │                         │
│              ┌─────┴─────┐             ┌─────┴─────┐             ┌─────┴─────┐
│              │   JVM 1   │             │   JVM 2   │             │   JVM N   │
│              │           │             │           │             │           │
│              │ Heap: 2GB │             │ Heap: 1GB │             │ Heap:Conf │
│              │ G1GC      │             │ Defaults  │             │ Options   │
│              └───────────┘             └───────────┘             └───────────┘
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Configuration File (jvm_config.json)

```json
{
    "jvm_manager": {
        "enabled": true,
        "auto_start_on_load": true,
        "health_check_interval_seconds": 30,
        "config_check_interval_seconds": 600,
        "shutdown_timeout_seconds": 30
    },
    "gateways": [
        {
            "name": "primary",
            "enabled": true,
            "host": "localhost",
            "base_port": 25333,
            "pool_size": 4,
            "jvm": {
                "auto_start": true,
                "heap_size_mb": 512,
                "max_heap_size_mb": 2048,
                "jvm_options": ["-XX:+UseG1GC"],
                "classpath": ["java/lib/*"]
            }
        }
    ],
    "calculators": {
        "definitions": {
            "MathCalculator": {
                "java_class": "com.dishtayantra.calculators.examples...",
                "gateway": "primary",
                "default_config": {}
            }
        }
    }
}
```

### Health Monitoring

The JVM Manager includes automatic health monitoring:

1. **Health Check Thread**: Runs every 30 seconds (configurable)
   - Checks JVM process status
   - Verifies gateway connections
   - Auto-restarts failed JVMs if `auto_start` is enabled

2. **Config Change Detection**: Monitors every 10 minutes (configurable)
   - Detects changes to jvm_config.json
   - Automatically restarts affected JVMs with new settings

3. **Graceful Shutdown**: On application shutdown
   - Closes all gateway connections
   - Terminates JVM processes with configurable timeout
   - Force kills if timeout exceeded

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/jvm/status` | GET | Get JVM Manager status |
| `/api/jvm/gateways` | GET | List all gateways |
| `/api/jvm/gateways/{name}` | GET | Get gateway details |
| `/api/jvm/gateways/{name}/stop` | POST | Stop a JVM |
| `/api/jvm/gateways/{name}/restart` | POST | Restart a JVM |
| `/api/jvm/gateways/{name}/reconnect` | POST | Reconnect gateway |
| `/api/jvm/reload-config` | POST | Reload configuration |
| `/api/jvm/calculators` | GET | List Java calculators |

### Worker Process Integration

Worker processes automatically connect to existing JVM gateways:

```
Main Process                          Worker Process
┌─────────────────────┐               ┌─────────────────────┐
│  JVM Manager        │               │  JVM Manager        │
│  - Starts JVMs      │               │  - Connect only     │
│  - Creates gateways │               │  - No JVM start     │
│  - Health monitor   │               │  - Uses existing    │
└──────────┬──────────┘               └──────────┬──────────┘
           │                                     │
           ▼                                     ▼
┌─────────────────────────────────────────────────────────────┐
│                    Py4J Gateway Pool                         │
│              (localhost:25333, 25334, ...)                   │
└─────────────────────────────────────────────────────────────┘
           │                                     │
           ▼                                     ▼
┌─────────────────────────────────────────────────────────────┐
│                   JVM Process (shared)                       │
│            DishtaYantraGateway with Calculators             │
└─────────────────────────────────────────────────────────────┘
```

---

## CPP Manager Architecture

### Overview

The CPP Manager provides centralized management of C++ pybind11 modules and calculators for high-performance native computation.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CPP Manager (v1.7.0)                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Configuration: config/cpp_config.json                               │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │  {                                                              │ │
│  │    "cpp_manager": {                                             │ │
│  │      "enabled": true,                                           │ │
│  │      "auto_load_on_startup": true                               │ │
│  │    },                                                           │ │
│  │    "modules": [...],                                            │ │
│  │    "calculators": [...]                                         │ │
│  │  }                                                              │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
│  Module Management:                                                  │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │  - Automatic module loading on startup                          │ │
│  │  - Hot reload support                                           │ │
│  │  - Build automation via CMake                                   │ │
│  │  - Calculator instance caching                                  │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
│  Available Calculators:                                              │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │  • PassthroughCalculator    • MatrixCalculator                 │ │
│  │  • MathCalculator           • StringTransformCalculator        │ │
│  │  • StatisticsCalculator     • DataValidationCalculator         │ │
│  │  • TradePricingCalculator   • RiskMetricsCalculator            │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### C++ Module Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     C++ Module Integration                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Python Process                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │  CPP Manager                                                    │ │
│  │  ┌──────────────────┐    ┌──────────────────────────────────┐  │ │
│  │  │ module_configs   │    │ dishtayantra_cpp.so              │  │ │
│  │  │ loaded_modules   │───▶│                                  │  │ │
│  │  │ calculator_cache │    │ ┌────────────────────────────┐   │  │ │
│  │  └──────────────────┘    │ │ Calculator Classes:        │   │  │ │
│  │                          │ │ - MathCalculator           │   │  │ │
│  │  Performance:            │ │ - StatisticsCalculator     │   │  │ │
│  │  ~100ns call overhead    │ │ - TradePricingCalculator   │   │  │ │
│  │  Zero-copy data transfer │ │ - RiskMetricsCalculator    │   │  │ │
│  │                          │ └────────────────────────────┘   │  │ │
│  │                          └──────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### CPP Manager API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/cpp/status` | GET | Get CPP Manager status |
| `/api/cpp/modules` | GET | List all modules |
| `/api/cpp/modules/{name}/load` | POST | Load a module |
| `/api/cpp/modules/{name}/unload` | POST | Unload a module |
| `/api/cpp/modules/{name}/reload` | POST | Reload a module |
| `/api/cpp/modules/{name}/build` | POST | Build module via CMake |
| `/api/cpp/calculators` | GET | List calculator definitions |
| `/api/cpp/calculators/{name}/test` | POST | Test a calculator |
| `/api/cpp/config` | GET | Get configuration |
| `/api/cpp/reload-config` | POST | Reload configuration |

### Worker Process Integration

Similar to JVM Manager, CPP Manager works across main and worker processes:

```
┌──────────────────────────────────┐  ┌──────────────────────────────────┐
│         Main Process             │  │        Worker Process            │
│  ┌────────────────────────────┐  │  │  ┌────────────────────────────┐  │
│  │  CPP Manager               │  │  │  │  CPP Manager               │  │
│  │  (singleton)               │  │  │  │  (separate instance)       │  │
│  │                            │  │  │  │                            │  │
│  │  - Loads config            │  │  │  │  - Reads same config       │  │
│  │  - Manages modules         │  │  │  │  - Independent modules     │  │
│  │  - Caches calculators      │  │  │  │  - Own calculator cache    │  │
│  └────────────────────────────┘  │  │  └────────────────────────────┘  │
│            │                      │  │            │                     │
│            ▼                      │  │            ▼                     │
│  ┌────────────────────────────┐  │  │  ┌────────────────────────────┐  │
│  │  dishtayantra_cpp.so       │  │  │  │  dishtayantra_cpp.so       │  │
│  │  (loaded in-process)       │  │  │  │  (loaded in-process)       │  │
│  └────────────────────────────┘  │  │  └────────────────────────────┘  │
└──────────────────────────────────┘  └──────────────────────────────────┘
```

---

## Rust Manager Architecture

### Overview

The Rust Manager provides centralized management of Rust PyO3 modules and calculators with memory safety guarantees at compile time.

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        Rust Manager (v1.7.0)                              │
├──────────────────────────────────────────────────────────────────────────┤
│  config/rust_config.json                                                  │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  {                                                                   │ │
│  │    "rust_manager": { "enabled": true, "auto_load_on_startup": true },│ │
│  │    "modules": [                                                      │ │
│  │      { "name": "dishtayantra_rust", "enabled": true,                │ │
│  │        "lmdb": { "enabled": false, "min_payload_size": 10240 } }    │ │
│  │    ],                                                                │ │
│  │    "calculators": [                                                  │ │
│  │      { "name": "RustMathCalculator", "rust_class": "MathCalculator" }│ │
│  │    ],                                                                │ │
│  │    "performance": { "enable_simd": true, "rayon_threads": 0 }       │ │
│  │  }                                                                   │ │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

### Rust Module Management

- **Auto-load**: Modules loaded on application startup via `rust_config.json`
- **Hot-reload**: Config changes detected and applied without restart
- **maturin Build**: Built-in support for compiling with `maturin develop --release`
- **Calculator Caching**: Instances cached for reuse
- **LMDB Support**: Zero-copy data exchange for payloads > 10KB

### Available Rust Calculators

| Calculator | Description | Key Config |
|------------|-------------|------------|
| `RustPassthroughCalculator` | Returns input unchanged | - |
| `RustMathCalculator` | Math operations (sum, mean, variance) | `operation`, `precision` |
| `RustStatisticsCalculator` | Statistical analysis with percentiles | `compute_percentiles`, `percentile_values` |
| `RustStringCalculator` | String operations | `operation` (uppercase, lowercase, etc.) |
| `RustHashCalculator` | Hash functions | `algorithm` (sha256, xxhash) |
| `RustJsonCalculator` | JSON parsing/transformation | `operation`, `strict` |
| `RustDataValidationCalculator` | Data validation | `required_fields`, `strict_mode` |
| `RustTradePricingCalculator` | Trade pricing | `commission_rate`, `tax_rate` |
| `RustRiskMetricsCalculator` | VaR and risk metrics | `var_confidence`, `risk_free_rate` |
| `RustTimeSeriesCalculator` | Moving averages | `window_size`, `operation` (sma, ema, wma) |

### Rust Manager API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/rust/status` | GET | Get Rust Manager status |
| `/api/rust/modules` | GET | List all configured modules |
| `/api/rust/modules/<name>/load` | POST | Load a module |
| `/api/rust/modules/<name>/unload` | POST | Unload a module |
| `/api/rust/modules/<name>/reload` | POST | Reload a module |
| `/api/rust/modules/<name>/build` | POST | Build with maturin |
| `/api/rust/calculators` | GET | List all calculators |
| `/api/rust/calculators/<name>/test` | POST | Test a calculator |

### Worker Process Integration

```
┌──────────────────────────────────┐  ┌──────────────────────────────────┐
│        Main Process               │  │      Worker Process               │
│  ┌────────────────────────────┐  │  │  ┌────────────────────────────┐  │
│  │  Rust Manager              │  │  │  │  Rust Manager              │  │
│  │  (singleton)               │  │  │  │  (separate instance)       │  │
│  │                            │  │  │  │                            │  │
│  │  - Loads config            │  │  │  │  - Reads same config       │  │
│  │  - Manages modules         │  │  │  │  - Independent modules     │  │
│  │  - Caches calculators      │  │  │  │  - Own calculator cache    │  │
│  └────────────────────────────┘  │  │  └────────────────────────────┘  │
│            │                      │  │            │                     │
│            ▼                      │  │            ▼                     │
│  ┌────────────────────────────┐  │  │  ┌────────────────────────────┐  │
│  │  dishtayantra_rust.so      │  │  │  │  dishtayantra_rust.so      │  │
│  │  (loaded in-process)       │  │  │  │  (loaded in-process)       │  │
│  └────────────────────────────┘  │  │  └────────────────────────────┘  │
└──────────────────────────────────┘  └──────────────────────────────────┘
```

### Rust Memory Safety Benefits

- **Compile-time guarantees**: No null pointers, buffer overflows, or data races
- **Ownership system**: Memory automatically managed without garbage collection
- **Thread safety**: Fearless concurrency with Send/Sync traits
- **Zero-cost abstractions**: High-level APIs with C-level performance
- **GIL release**: Rayon parallel processing releases Python GIL

---

## Multi-Language Calculator Architecture

### Calculator Factory Pattern

```
┌────────────────────────────────────────────────────────────────┐
│                    CalculatorFactory                            │
├────────────────────────────────────────────────────────────────┤
│  create(name, type, config) → DataCalculator                    │
│                                                                 │
│  Decision Flow:                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  if config.calculator == "java"                          │   │
│  │      → JavaCalculator (Py4J)                             │   │
│  │  elif config.calculator == "cpp"                         │   │
│  │      → CppCalculator (pybind11)                          │   │
│  │  elif config.calculator == "rust"                        │   │
│  │      → RustCalculator (PyO3)                             │   │
│  │  elif config.calculator == "rest" or config.endpoint     │   │
│  │      → RestCalculator (HTTP)                             │   │
│  │  elif type contains "."                                  │   │
│  │      → CustomCalculator (dynamic import)                 │   │
│  │  else                                                    │   │
│  │      → Built-in calculator                               │   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────┘
```

### Java Calculator (Py4J)

```
┌─────────────────────────────────────────────────────────────────┐
│                     Java Integration                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Python Process              │  JVM Process                      │
│  ┌─────────────────┐         │  ┌─────────────────────────┐     │
│  │ JavaCalculator  │         │  │ DishtaYantraGateway     │     │
│  │                 │ ◀──────▶│  │                         │     │
│  │ - gateway_pool  │  Py4J   │  │ - Calculator registry   │     │
│  │ - calculate()   │  TCP    │  │ - calculate(Map data)   │     │
│  └─────────────────┘         │  └─────────────────────────┘     │
│                              │                                   │
│  Features:                   │  Features:                        │
│  - Connection pooling        │  - AbstractCalculator base        │
│  - Thread-safe gateway       │  - Custom implementations         │
│  - Automatic reconnection    │  - Type-safe interfaces           │
│                              │                                   │
└─────────────────────────────────────────────────────────────────┘
```

### C++ Calculator (pybind11)

```
┌─────────────────────────────────────────────────────────────────┐
│                     C++ Integration                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Python Process                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  import dishtayantra_cpp                                  │   │
│  │                                                           │   │
│  │  ┌─────────────────┐      ┌─────────────────────────┐    │   │
│  │  │ CppCalculator   │──────│ dishtayantra_cpp.so     │    │   │
│  │  │                 │      │                          │    │   │
│  │  │ - calculate()   │      │ - Native C++ code       │    │   │
│  │  │ - details()     │      │ - SIMD optimizations    │    │   │
│  │  └─────────────────┘      │ - Zero-copy arrays      │    │   │
│  │                           └─────────────────────────┘    │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  Performance: ~100ns overhead, direct memory access              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Rust Calculator (PyO3)

```
┌─────────────────────────────────────────────────────────────────┐
│                     Rust Integration                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Python Process                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  import dishtayantra_rust                                 │   │
│  │                                                           │   │
│  │  ┌─────────────────┐      ┌─────────────────────────┐    │   │
│  │  │ RustCalculator  │──────│ dishtayantra_rust.so    │    │   │
│  │  │                 │      │                          │    │   │
│  │  │ - calculate()   │      │ - Memory-safe code      │    │   │
│  │  │ - details()     │      │ - Rayon parallelism     │    │   │
│  │  └─────────────────┘      │ - Thread-safe by design │    │   │
│  │                           └─────────────────────────┘    │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  Benefits: Memory safety, thread safety, C++ performance         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### REST Calculator

```
┌─────────────────────────────────────────────────────────────────┐
│                     REST Integration                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  DishtaYantra                  │  External Service               │
│  ┌─────────────────────┐       │  ┌─────────────────────┐       │
│  │   RestCalculator    │       │  │   REST Endpoint     │       │
│  │                     │ HTTP  │  │                     │       │
│  │ - endpoint          │ POST  │  │ - /api/calculate    │       │
│  │ - auth_type         │ ─────▶│  │                     │       │
│  │ - retries           │ JSON  │  │ - Authentication    │       │
│  │ - timeout           │ ◀─────│  │ - Business logic    │       │
│  └─────────────────────┘       │  └─────────────────────┘       │
│                                │                                 │
│  Authentication:               │                                 │
│  - API Key (X-API-Key)         │                                 │
│  - Basic Auth (Base64)         │                                 │
│  - Bearer Token (JWT)          │                                 │
│  - Custom Headers              │                                 │
│                                │                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Arrow Columnar Data Plane (Opt-In)

The Arrow columnar data plane (roadmap Phase 1 / A1) is an **additive, opt-in**
extension to the calculator layer. It introduces `ArrowCalculator`
(`core/calculator/arrow_calculator.py`) — a calculator that processes a columnar
micro-batch with Apache Arrow / `pyarrow.compute` kernels instead of one Python
dict at a time.

**Additive by design.** `ArrowCalculator` subclasses `DataCalculator` and
implements the normal `calculate(data)` row method, so the reactive-dataflow
engine (`core/dag/graph_elements.py::Node.compute`) drives it with no changes.
Neither `core/dag/*` nor `core_calculator.py` is modified. A bare record dict is
processed as a 1-row batch; a batch-envelope message `{"batch": [...]}` is
processed vectorized.

**Coexistence.** Because node types and calculator types are each resolved
independently by the builder (`core/dag/compute_graph.py`,
`core/dag/compute_graph_builders.py`), a graph is never required to be
homogeneous:

- Old-style (row) and new-style (Arrow) DAGs run side by side in one instance.
- A single graph may contain both row and Arrow calculators. On single-dict flow
  Arrow calculators are exact drop-ins (output-identical to row); on batched flow
  any legacy row stage is wrapped by `RowCalculatorBatchAdapter`.

**Performance.** Vectorization helps numeric/columnar work on high-volume,
schema-regular, latency-tolerant streams; it adds overhead (no win) on
single-dict flow, low-volume, ultra-low-latency, branchy/string, or I/O-bound
paths. Measured: ~11.8x at the kernel level, ~1.8x end-to-end at batch 500.

See `docs/design/A1-arrow-data-plane.md` (RFC) and
`docs/design/A1-worked-example-and-coexistence.md` (worked example + decision
tree). Runnable: `python -m perftest.run_arrow_example`.

**Automatic source batching (opt-in node types).** Two additive node types make
batching internal so producers/consumers keep sending and receiving ordinary
per-message data: `BatchingSubscriptionNode` drains the subscriber into one
`{"batch": [...]}` envelope per cycle (load-adaptive — fills under backlog,
flushes when idle), and `FlatteningPublicationNode` republishes each record so
the external per-message contract is preserved. `SubscriptionNode` and
`PublicationNode` are unchanged; a DAG opts in by setting a node's `type`.
Runnable: `python -m perftest.run_autobatch_example`. (Current throughput gain is
modest because the dataflow deep-copies each envelope per stage; carrying Arrow
`RecordBatch`es on edges to remove that copy is the next A1 increment.)

---

## LMDB Zero-Copy Data Exchange

### Overview (v1.1.2 - High Performance)

DishtaYantra v1.1.2 introduces **LMDB-based zero-copy data exchange** for native calculators. This high-performance innovation enables 100-1000x faster data transfer for large payloads compared to traditional serialization methods.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Python DAG Engine                                │
│  ┌─────────────┐                              ┌─────────────────┐   │
│  │ Input Data  │──▶ LMDB Transport ──▶ Write │    LMDB File    │   │
│  │ (Dict/Array)│                              │ (Memory-Mapped) │   │
│  └─────────────┘                              └────────┬────────┘   │
│                                                        │            │
│                                     Zero-Copy Memory Map (mmap)     │
│                                                        │            │
│  ┌─────────────────────────────────────────────────────┼──────────┐ │
│  │                  Native Calculator                   ▼          │ │
│  │  ┌───────────┐      ┌───────────┐      ┌────────────────────┐ │ │
│  │  │   Java    │      │    C++    │      │       Rust         │ │ │
│  │  │ (lmdbjava)│      │ (liblmdb) │      │    (lmdb-rs)       │ │ │
│  │  └───────────┘      └───────────┘      └────────────────────┘ │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                     │
│  Output flows back via LMDB ◀──────────────────────────────────────│
└─────────────────────────────────────────────────────────────────────┘
```

### Performance Comparison

| Payload Size | JSON Serialization | MessagePack | LMDB Zero-Copy | Speedup |
|-------------|-------------------|-------------|----------------|---------|
| 1 KB | 50 μs | 20 μs | **5 μs** | 10x |
| 10 KB | 500 μs | 200 μs | **10 μs** | 50x |
| 100 KB | 5 ms | 2 ms | **50 μs** | 100x |
| 1 MB | 50 ms | 20 ms | **200 μs** | 250x |
| 10 MB | 500 ms | 200 ms | **2 ms** | 250x |

### Key Components

#### LMDBTransport (`core/lmdb/lmdb_transport.py`)

```python
class LMDBTransport:
    """
    Core transport layer for zero-copy data exchange.
    
    Features:
    - Memory-mapped file I/O
    - Multiple data formats (JSON, MsgPack, NumPy, Arrow)
    - Automatic compression for large payloads
    - TTL-based expiration and cleanup
    - ACID transaction guarantees
    - Checksum verification for data integrity
    """
```

#### LMDBDataExchange (`core/lmdb/lmdb_calculator.py`)

```python
class LMDBDataExchange:
    """
    Per-calculator exchange handler.
    
    Features:
    - Automatic payload size detection
    - Unified reference protocol for all languages
    - Transaction-based exchange with cleanup
    - Configurable thresholds
    """
```

### Configuration

#### application.yaml / application.properties

```properties
# LMDB Zero-Copy Data Exchange Configuration
lmdb.db.path=${LMDB_DB_PATH:/tmp/dishtayantra_lmdb}
lmdb.map.size=1073741824          # 1GB
lmdb.max.dbs=100
lmdb.ttl.seconds=300
lmdb.max.readers=126
lmdb.cleanup.interval=60
```

#### DAG Folders and Name Uniqueness (v3.1.0)

DAG JSON files are loaded from `config/dags` and, optionally, additional
folders listed in `storage.dags.prefixes` (comma-separated). `config/dags` is
always scanned. Each folder is scanned for its **direct `.json` children
only** — sub-folders are never auto-loaded.

DAG `name` is the server-wide identity (registry, URLs, pub/sub, metrics) and
**must be globally unique across all folders**. A name collision is treated as
a configuration error:

- **Startup:** fatal — the server collects all collisions, logs them, and
  refuses to boot.
- **Reload:** the incumbent (already-running) DAG wins; the colliding newcomer
  is rejected and surfaced on the dashboard as a persistent red banner with a
  delete-file action.

A DAG's source is tracked by its full object path, so identical filenames in
different folders are fine and removal reconciliation is exact. See the "DAG
Folders, Name Uniqueness, and External Libraries" user guide.

#### DAG Node Configuration

```json
{
  "name": "heavy_processor",
  "type": "com.example.HeavyProcessor",
  "calculator": "java",
  "lmdb_enabled": true,
  "lmdb_min_size": 10240,
  "lmdb_exchange_mode": "both",
  "lmdb_data_format": "msgpack"
}
```

### Exchange Modes

| Mode | Description |
|------|-------------|
| `input` | Data written to LMDB for native read, output returned directly |
| `output` | Data passed directly, native writes output to LMDB |
| `both` | Both input and output via LMDB (recommended for large payloads) |
| `reference` | Only key references passed, native handles all I/O |

### Data Formats

| Format | Best For |
|--------|----------|
| `msgpack` | General purpose (default) |
| `json` | Debugging, human-readable |
| `numpy` | Numerical arrays |
| `arrow` | Columnar/tabular data |
| `raw` | Custom binary formats |

### Native Language Integration

#### Java (lmdbjava)

```java
// Check for LMDB reference
if (data.get("_lmdb_ref")) {
    String inputKey = data.get("_lmdb_input_key");
    ByteBuffer buf = dbi.get(txn, keyBuf);  // Zero-copy read
    // Process...
    dbi.put(txn, outputKey, resultBuf);      // Zero-copy write
}
```

#### C++ (liblmdb)

```cpp
// Check for LMDB reference
if (data["_lmdb_ref"].cast<bool>()) {
    MDB_val mdb_key, mdb_data;
    mdb_get(txn, dbi, &mdb_key, &mdb_data);  // Zero-copy via mmap
    process((uint8_t*)mdb_data.mv_data);
}
```

#### Rust (lmdb-rs)

```rust
// Check for LMDB reference
if data.get_item("_lmdb_ref")?.is_true()? {
    let txn = env.begin_ro_txn()?;
    let bytes = txn.get(db, &input_key)?;    // Zero-copy via mmap
    let result = process(bytes)?;
}
```

### Innovation Summary

This architecture represents a **high-performance innovation** not found in any other DAG framework:

1. **Automatic payload size detection** for LMDB usage decision
2. **Unified reference protocol** across Java, C++, and Rust
3. **Transaction-based exchange** with automatic cleanup
4. **Format-agnostic transport** supporting multiple serialization formats
5. **Configurable thresholds** for optimal performance tuning
6. **ACID guarantees** for data integrity

---

## Pub/Sub Framework

### Publisher/Subscriber Factory

```
┌─────────────────────────────────────────────────────────────────┐
│                      PubSubFactory                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  URI Parsing:  protocol://type/name                              │
│                                                                  │
│  ┌─────────────┬──────────────────────────────────────────────┐ │
│  │ Protocol    │ Implementation                                │ │
│  ├─────────────┼──────────────────────────────────────────────┤ │
│  │ mem://      │ InMemoryDataPubSub                            │ │
│  │ memredis:// │ InMemoryRedisDataPubSub                       │ │
│  │ kafka://    │ KafkaDataPubSub                               │ │
│  │ rabbitmq:// │ RabbitMQDataPubSub                            │ │
│  │ redis://    │ RedisDataPubSub                               │ │
│  │ activemq:// │ ActiveMQDataPubSub                            │ │
│  │ tibcoems:// │ TibcoEMSDataPubSub                            │ │
│  │ websphere://│ WebSphereMQDataPubSub                         │ │
│  │ file://     │ FileDataPubSub                                │ │
│  │ sql://      │ SQLDataPubSub                                 │ │
│  │ rest://     │ RESTDataPubSub                                │ │
│  │ grpc://     │ GRPCDataPubSub                                │ │
│  │ aerospike://│ AerospikeDataPubSub                           │ │
│  │ custom://   │ CustomDataPubSub                              │ │
│  │ metronome   │ MetronomeDataPubSub                           │ │
│  └─────────────┴──────────────────────────────────────────────┘ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Resilient Connections

```
┌─────────────────────────────────────────────────────────────────┐
│                   Resilient Connection Pattern                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │               ResilientConnection                        │    │
│  │  ┌─────────────────────────────────────────────────┐    │    │
│  │  │  - Primary connection                            │    │    │
│  │  │  - Failover connections[]                        │    │    │
│  │  │  - Health check interval                         │    │    │
│  │  │  - Auto-reconnect logic                          │    │    │
│  │  │  - Circuit breaker                               │    │    │
│  │  └─────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Supported:                                                      │
│  - ResilientKafka                                                │
│  - ResilientRabbitMQ                                             │
│  - ResilientRedis                                                │
│  - ResilientActiveMQ                                             │
│  - ResilientTibcoEMS                                             │
│  - ResilientWebSphereMQ                                          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## DAG Execution Engine

### Execution Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    DAG Execution Flow                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Build Phase                                                  │
│     ┌─────────────────────────────────────────────────────┐     │
│     │  Load JSON → Create Nodes → Create Edges → Validate │     │
│     └─────────────────────────────────────────────────────┘     │
│                              │                                   │
│  2. Topological Sort         ▼                                   │
│     ┌─────────────────────────────────────────────────────┐     │
│     │  Kahn's Algorithm → Execution Order → Detect Cycles │     │
│     └─────────────────────────────────────────────────────┘     │
│                              │                                   │
│  3. Start Execution          ▼                                   │
│     ┌─────────────────────────────────────────────────────┐     │
│     │  Start Subscribers → Start Executor Thread          │     │
│     └─────────────────────────────────────────────────────┘     │
│                              │                                   │
│  4. Processing Loop          ▼                                   │
│     ┌─────────────────────────────────────────────────────┐     │
│     │  For each node in topological order:                │     │
│     │    - Check if dirty (has new input)                 │     │
│     │    - Process node (calculate/transform)             │     │
│     │    - Propagate output to downstream nodes           │     │
│     │    - Mark downstream nodes as dirty                 │     │
│     └─────────────────────────────────────────────────────┘     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Time Window Management

```
┌─────────────────────────────────────────────────────────────────┐
│                   Time Window Architecture                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Configuration:                                                  │
│  {                                                               │
│    "start_time": "0900",    // HHMM format                       │
│    "duration": 480,         // minutes (or "end_time")           │
│    "auto_clone": true,      // create next day's DAG             │
│    "clone_before": 30       // minutes before end                │
│  }                                                               │
│                                                                  │
│  Timeline:                                                       │
│  ─────────────────────────────────────────────────────────────  │
│  │        │         ACTIVE          │        │                  │
│  │ BEFORE │◀───────────────────────▶│ AFTER  │                  │
│  │        │                         │        │                  │
│  ─────────────────────────────────────────────────────────────  │
│         0900                      1700                           │
│                                                                  │
│  States:                                                         │
│  - BEFORE: DAG suspended, waiting for start_time                 │
│  - ACTIVE: DAG running normally                                  │
│  - AFTER: DAG stopped (or cloned for next day)                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Web Application Architecture

### FastAPI Application Structure

```
┌─────────────────────────────────────────────────────────────────┐
│                   DishtaYantraWebApp                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Singleton Pattern with Thread Safety                            │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │ FastAPI App  │  │  Components  │  │   Routes     │           │
│  ├──────────────┤  ├──────────────┤  ├──────────────┤           │
│  │ - Config     │  │ - DAGServer  │  │ - AuthRoutes │           │
│  │ - Sessions   │  │ - UserReg    │  │ - Dashboard  │           │
│  │ - Templates  │  │ - RedisCache │  │ - DAGRoutes  │           │
│  │              │  │              │  │ - AdminRoutes│           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Route Organization

```
┌─────────────────────────────────────────────────────────────────┐
│                     Route Handlers                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  AuthRoutes          │  NoAuthRoutes      │  AdminRoutes         │
│  ├── /login          │  ├── /about        │  ├── /admin/monitor  │
│  ├── /logout         │  ├── /help         │  ├── /admin/logs     │
│  └── @login_required │  └── /help/*       │  └── @admin_required │
│                      │                    │                      │
│  DashboardRoutes     │  DAGRoutes         │  CacheRoutes         │
│  ├── /dashboard      │  ├── /dag/<name>   │  ├── /cache          │
│  ├── /dag/designer   │  ├── /dag/start    │  ├── /cache/create   │
│  └── /dag/create     │  ├── /dag/stop     │  └── /cache/edit     │
│                      │  └── /dag/delete   │                      │
│                      │                    │                      │
│  UserRoutes          │  DAGDesignerRoutes │                      │
│  ├── /users          │  ├── /designer     │                      │
│  ├── /user/create    │  └── /designer/api │                      │
│  └── /user/edit      │                    │                      │
│                      │                    │                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## Admin & Monitoring System

### System Monitoring Architecture (v1.1.1)

```
┌─────────────────────────────────────────────────────────────────┐
│                   System Monitoring                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    AdminRoutes                            │   │
│  │  ┌─────────────────────────────────────────────────────┐ │   │
│  │  │  /admin/monitoring                                   │ │   │
│  │  │    ├── CPU metrics (psutil)                          │ │   │
│  │  │    ├── Memory metrics                                │ │   │
│  │  │    ├── Disk metrics                                  │ │   │
│  │  │    ├── Network metrics                               │ │   │
│  │  │    ├── DAG Server stats                              │ │   │
│  │  │    ├── Process info                                  │ │   │
│  │  │    ├── Health checks                                 │ │   │
│  │  │    └── Calculator availability                       │ │   │
│  │  └─────────────────────────────────────────────────────┘ │   │
│  │  ┌─────────────────────────────────────────────────────┐ │   │
│  │  │  /admin/monitoring/api (JSON refresh)                │ │   │
│  │  │    └── Auto-refresh every 5 seconds                  │ │   │
│  │  └─────────────────────────────────────────────────────┘ │   │
│  │  ┌─────────────────────────────────────────────────────┐ │   │
│  │  │  /admin/logs                                         │ │   │
│  │  │    ├── Log file selection                            │ │   │
│  │  │    ├── Level filtering                               │ │   │
│  │  │    ├── Search                                        │ │   │
│  │  │    └── Download                                      │ │   │
│  │  └─────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  Dependencies:                                                   │
│  - psutil: System metrics collection                             │
│  - threading: Active thread count                                │
│  - platform: System information                                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Metrics Collected

| Category | Metrics |
|----------|---------|
| CPU | Usage %, Core count, Load average (1/5/15 min) |
| Memory | Usage %, Total, Available |
| Disk | Usage %, Total, Free, Partitions |
| Network | Bytes sent/received, Connections, Errors |
| Process | PID, Memory, Threads, Open files |
| DAG Server | Total/Running/Stopped/Error DAGs |
| Calculators | Java/C++/Rust/REST availability |

---

## Prometheus Metrics Integration

DishtaYantra v1.5.0 includes comprehensive Prometheus metrics integration for real-time observability and monitoring.

### Monitoring Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Prometheus Monitoring Stack                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                      DishtaYantra                                │    │
│  │  ┌─────────────────────────────────────────────────────────────┐ │    │
│  │  │                  Metrics Module                              │ │    │
│  │  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐            │ │    │
│  │  │  │  Counters   │ │   Gauges    │ │ Histograms  │            │ │    │
│  │  │  │ executions  │ │ active_dags │ │  latency    │            │ │    │
│  │  │  │ messages    │ │ queue_depth │ │  msg_size   │            │ │    │
│  │  │  │ errors      │ │ cache_size  │ │  calc_time  │            │ │    │
│  │  │  └─────────────┘ └─────────────┘ └─────────────┘            │ │    │
│  │  └──────────────────────────┬──────────────────────────────────┘ │    │
│  │                             │                                    │    │
│  │  ┌──────────────────────────┴──────────────────────────────────┐ │    │
│  │  │              Endpoints                                       │ │    │
│  │  │  /metrics  │  /health  │  /health/live  │  /health/ready   │ │    │
│  │  └──────────────────────────┬──────────────────────────────────┘ │    │
│  └─────────────────────────────┼────────────────────────────────────┘    │
│                                │                                         │
│                                ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                      Prometheus                                  │    │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐                │    │
│  │  │   Scraper   │ │    TSDB     │ │   Rules     │                │    │
│  │  │  (15s int)  │ │  (Storage)  │ │  (Alerts)   │                │    │
│  │  └─────────────┘ └─────────────┘ └─────────────┘                │    │
│  └──────────────────────────┬──────────────────────────────────────┘    │
│                             │                                            │
│                             ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                       Grafana                                    │    │
│  │  ┌─────────────────────────────────────────────────────────────┐ │    │
│  │  │ Dashboard: DAG Metrics │ Calculator Perf │ System Health   │ │    │
│  │  └─────────────────────────────────────────────────────────────┘ │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Metrics Categories

| Category | Metrics | Purpose |
|----------|---------|---------|
| DAG | executions, duration, active_count | Track DAG execution and performance |
| Node | executions, duration, errors | Monitor individual node processing |
| Calculator | calls, duration, errors | Measure calculator performance |
| Messaging | received, published, lag, queue_depth | Monitor message throughput |
| Cache | hits, misses, size, evictions | Track cache efficiency |
| Database | connections, query_duration | Monitor DB pool health |
| System | cpu, memory, threads, uptime | System resource monitoring |
| Health | component_status | Component health checks |

### Metrics Endpoints

| Endpoint | Purpose | Auth Required |
|----------|---------|---------------|
| `/metrics` | Prometheus text format metrics | No |
| `/metrics/json` | JSON format (debugging) | No |
| `/health` | Overall health status | No |
| `/health/live` | Kubernetes liveness probe | No |
| `/health/ready` | Kubernetes readiness probe | No |

### Key Metrics Reference

```
# DAG Execution Metrics
dishtayantra_dag_executions_total{dag_name="...", status="success|error"}
dishtayantra_dag_execution_duration_seconds_bucket{dag_name="...", le="..."}
dishtayantra_active_dags

# Calculator Metrics
dishtayantra_calculator_calls_total{calculator_name="...", calculator_type="...", status="..."}
dishtayantra_calculator_duration_seconds_bucket{calculator_name="...", le="..."}

# Messaging Metrics
dishtayantra_messages_received_total{transport="kafka|redis|...", topic="..."}
dishtayantra_messages_published_total{transport="...", topic="..."}
dishtayantra_kafka_consumer_lag{consumer_group="...", topic="...", partition="..."}

# Cache Metrics
dishtayantra_cache_hits_total{cache_name="..."}
dishtayantra_cache_misses_total{cache_name="..."}

# System Metrics
dishtayantra_system_cpu_usage
dishtayantra_system_memory_percent
dishtayantra_uptime_seconds
```

### Alert Rules

Pre-configured alerts are provided in `docker/alert_rules.yml`:

| Alert | Condition | Severity |
|-------|-----------|----------|
| DishtaYantraDown | Application unreachable for 1m | Critical |
| DAGExecutionErrors | Error rate > 10% for 5m | Warning |
| DAGExecutionSlow | p95 latency > 30s for 5m | Warning |
| HighCPUUsage | CPU > 80% for 5m | Warning |
| CriticalMemoryUsage | Memory > 95% for 2m | Critical |
| KafkaConsumerLag | Lag > 10000 for 5m | Warning |
| DBPoolExhausted | All connections used for 5m | Critical |

### Integration with Decorators

```python
from core.metrics import metrics, track_execution_time, count_calls

class MyCalculator:
    @track_execution_time(
        metrics.calculator_duration,
        {'calculator_name': 'my_calc', 'calculator_type': 'python'}
    )
    @count_calls(
        metrics.calculator_calls,
        {'calculator_name': 'my_calc', 'calculator_type': 'python'}
    )
    def calculate(self, data):
        return result
```

---

## High Availability

### Leader Election Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    High Availability                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│        ┌─────────────┐                    ┌─────────────┐       │
│        │  Instance A │                    │  Instance B │       │
│        │  (PRIMARY)  │                    │  (STANDBY)  │       │
│        └──────┬──────┘                    └──────┬──────┘       │
│               │                                  │               │
│               │      ┌─────────────────┐        │               │
│               └──────│    Zookeeper    │────────┘               │
│                      │    Ensemble     │                        │
│                      │                 │                        │
│                      │ /dishtayantra/  │                        │
│                      │   leader        │                        │
│                      └─────────────────┘                        │
│                                                                  │
│  Failover Sequence:                                              │
│  1. PRIMARY fails (connection lost)                              │
│  2. Zookeeper detects failure                                    │
│  3. STANDBY acquires leader lock                                 │
│  4. STANDBY becomes PRIMARY                                      │
│  5. All DAGs resumed automatically                               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Market-Aware Scheduling

Each DAG may carry a schedule that gates when it runs. A schedule combines a
daily time window, day-of-week rules, holiday calendars, and a timezone, all
evaluated together on every monitor pass.

### Schedule model

- **Time window** — `start_time` (HHMM) plus a `duration` (e.g. `6h30m`);
  the end is computed as `start_time + duration`. Windows that wrap past
  midnight are handled. A legacy explicit `end_time` is still accepted.
- **Days** — `days_of_week` (allow-list) and `exclude_days_of_week`
  (deny-list).
- **Holiday calendars** — one or more named calendars (e.g. `USA`, `CANADA`)
  baked for 2026-2040; a DAG is inactive on any followed calendar's holiday.
- **Timezone** — an IANA name (default `America/New_York`). **The whole
  schedule is evaluated in this timezone, not the server's local clock.**
  Because servers commonly run in UTC, a naive comparison would shift a
  market window by several hours; resolving "now" in the schedule's zone
  (via `zoneinfo`, DST-aware) keeps `0930-1600` meaning Eastern market hours
  regardless of host timezone.

### Evaluation path

```
dag.is_within_schedule(now=None)
        │
        ▼
is_schedule_active(start_time, end_time, schedule, now)
        │   resolves "now" in schedule.timezone
        ├─► is_within_time_window(...)      (wrap-around aware)
        └─► schedule.is_active(...)          (day-of-week + holiday)
        ▼
   (active: bool, reason: str)   ← reason is surfaced on the dashboard
```

The DAG server's time-window monitor loop (PRIMARY only) calls
`is_within_schedule()` every cycle to auto-suspend/resume DAGs as they leave
and enter their window. Intraday edits to a DAG's schedule or to calendar
files are picked up within ~5 minutes without a restart. AutoClone ramp times
are evaluated in the same timezone for consistency.

---

## Security Architecture

### Authentication & Authorization

```
┌─────────────────────────────────────────────────────────────────┐
│                   Security Architecture                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    UserRegistry                          │    │
│  │  ┌─────────────────────────────────────────────────┐    │    │
│  │  │  Database (SQLite default / PostgreSQL)          │    │    │
│  │  │   tables: users, roles, api_keys                 │    │    │
│  │  │   passwords: PBKDF2-SHA256 hashes                │    │    │
│  │  │   (legacy users.json migrated once, then         │    │    │
│  │  │    renamed users.json.migrated)                  │    │    │
│  │  └─────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Role-Based Access Control:                                      │
│  ┌─────────────┬──────────────────────────────────────────┐     │
│  │ Role        │ Permissions                               │     │
│  ├─────────────┼──────────────────────────────────────────┤     │
│  │ admin       │ All operations, user mgmt, system monitor│     │
│  │ user        │ View DAGs, start/stop, clone             │     │
│  └─────────────┴──────────────────────────────────────────┘     │
│                                                                  │
│  Session Management:                                             │
│  - Starlette sessions with secure cookies                        │
│  - Configurable session timeout                                  │
│  - CSRF protection                                               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Performance Considerations

### Threading Model

```
┌─────────────────────────────────────────────────────────────────┐
│                    Threading Architecture                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Main Thread                                                     │
│  └── uvicorn ASGI Server                                         │
│                                                                  │
│  DAG Executor Threads (per DAG)                                  │
│  ├── DAG-1 Executor                                              │
│  ├── DAG-2 Executor                                              │
│  └── DAG-N Executor                                              │
│                                                                  │
│  Subscriber Threads (per subscriber)                             │
│  ├── Kafka Consumer Thread                                       │
│  ├── RabbitMQ Consumer Thread                                    │
│  └── ...                                                         │
│                                                                  │
│  Background Threads                                              │
│  ├── Zookeeper Session                                           │
│  ├── User Registry Reloader                                      │
│  └── Time Window Monitor                                         │
│                                                                  │
│  Python 3.13+ Free-Threading:                                    │
│  - Optional GIL-free mode                                        │
│  - True parallel execution                                       │
│  - Automatic detection                                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Performance Benchmarks

| Operation | Latency | Throughput |
|-----------|---------|------------|
| Node execution | < 1ms | 10K nodes/s |
| Python calculator | ~100μs | 10K ops/s |
| Java calculator (Py4J) | ~200μs | 5K ops/s |
| C++ calculator (pybind11) | ~100ns | 1M ops/s |
| Rust calculator (PyO3) | ~100ns | 1M ops/s |
| REST calculator | 10-1000ms | 100 ops/s |
| Kafka publish | ~5ms | 100K msg/s |
| In-memory queue | ~1μs | 1M msg/s |

---

## Deployment Architecture

### Single Instance Deployment

```
┌─────────────────────────────────────────────────────────────────┐
│                    Single Instance                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    Server Host                           │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │    │
│  │  │ DishtaYantra│  │   Kafka     │  │   Redis     │      │    │
│  │  │    :5000    │  │   :9092     │  │   :6379     │      │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### High Availability Deployment

```
┌─────────────────────────────────────────────────────────────────┐
│                    HA Deployment                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌───────────────┐    ┌───────────────┐    ┌───────────────┐   │
│  │  Instance 1   │    │  Instance 2   │    │  Instance 3   │   │
│  │  (PRIMARY)    │    │  (STANDBY)    │    │  (STANDBY)    │   │
│  └───────┬───────┘    └───────┬───────┘    └───────┬───────┘   │
│          │                    │                    │            │
│          └────────────────────┼────────────────────┘            │
│                               │                                  │
│                    ┌──────────┴──────────┐                      │
│                    │  Zookeeper Cluster  │                      │
│                    │    (3-5 nodes)      │                      │
│                    └─────────────────────┘                      │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Message Broker Cluster                      │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐    │   │
│  │  │ Kafka-1 │  │ Kafka-2 │  │ Kafka-3 │  │ Redis   │    │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Docker Deployment

```yaml
# docker-compose.yml
version: '3.8'
services:
  dishtayantra:
    build: .
    ports:
      - "5000:5000"
    environment:
      - ZOOKEEPER_HOSTS=zookeeper:2181
      - DAG_CONFIG_FOLDER=/app/config/dags
    depends_on:
      - zookeeper
      - kafka
      
  zookeeper:
    image: zookeeper:3.8
    ports:
      - "2181:2181"
      
  kafka:
    image: apache/kafka
    ports:
      - "9092:9092"
```

---

## Appendix

### Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.1.2 | Dec 2025 | LMDB zero-copy data exchange  |
| 1.1.1 | Dec 2025 | System monitoring, admin features, logs viewer |
| 1.1.0 | Dec 2025 | Java, C++, Rust, REST calculators, free-threading |
| 1.0.0 | Nov 2025 | Initial release |

### Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| FastAPI + uvicorn | current | Web framework (ASGI) |
| psutil | 6.1.0 | System monitoring |
| py4j | 0.10.9.7 | Java integration |
| requests | 2.32.3 | REST integration |
| kazoo | 2.10.0 | Zookeeper client |
| kafka-python | 2.2.15 | Kafka integration |
| lmdb | 1.4.1 | LMDB transport |

---

## Legal Information

### Copyright Notice

© 2025-2030 Ashutosh Sinha. All rights reserved.

### Trademark Notice

DishtaYantra™ is a trademark of Ashutosh Sinha.

### Confidentiality

This document contains proprietary and confidential information. Unauthorized copying, distribution, or disclosure is strictly prohibited.

---

**DishtaYantra v2.2** | © 2025-2030 Ashutosh Sinha
