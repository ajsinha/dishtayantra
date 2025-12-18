# DishtaYantra Architecture Document

## Version 1.1.1

© 2025-2030 Ashutosh Sinha

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture Layers](#architecture-layers)
3. [Core Components](#core-components)
4. [Multi-Language Calculator Architecture](#multi-language-calculator-architecture)
5. [Pub/Sub Framework](#pubsub-framework)
6. [DAG Execution Engine](#dag-execution-engine)
7. [Web Application Architecture](#web-application-architecture)
8. [Admin & Monitoring System](#admin--monitoring-system)
9. [High Availability](#high-availability)
10. [Security Architecture](#security-architecture)
11. [Performance Considerations](#performance-considerations)
12. [Deployment Architecture](#deployment-architecture)

---

## System Overview

DishtaYantra is a high-performance, multi-threaded DAG (Directed Acyclic Graph) compute server designed for real-time data processing pipelines. The system supports multiple message brokers, data sources, and multi-language calculator integrations.

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DishtaYantra v1.1.1                            │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐ │
│  │   Web UI     │  │  REST API    │  │   Admin      │  │    Help     │ │
│  │  Dashboard   │  │  Endpoints   │  │  Monitoring  │  │   Center    │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └─────────────┘ │
│         │                 │                 │                           │
│  ┌──────┴─────────────────┴─────────────────┴─────────────────────────┐ │
│  │                      Flask Application Layer                        │ │
│  │            (Routes, Authentication, Session Management)             │ │
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
│  │  │                  Pub/Sub Framework                               │ │ │
│  │  │  ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐   │ │ │
│  │  │  │ Kafka │ │Rabbit │ │ Redis │ │Active │ │ File  │ │  SQL  │   │ │ │
│  │  │  │       │ │  MQ   │ │       │ │  MQ   │ │       │ │       │   │ │ │
│  │  │  └───────┘ └───────┘ └───────┘ └───────┘ └───────┘ └───────┘   │ │ │
│  │  └─────────────────────────────────────────────────────────────────┘ │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐ │ │
│  │  │                  Transformer Framework                           │ │ │
│  │  └─────────────────────────────────────────────────────────────────┘ │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Architecture Layers

### Layer 1: Presentation Layer
- **Web UI**: Flask templates with Bootstrap 5
- **REST API**: JSON endpoints for programmatic access
- **Admin Interface**: System monitoring, logs, user management
- **Help Center**: Documentation and guides

### Layer 2: Application Layer
- **Flask Application**: Request routing, session management
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

### Flask Application Structure

```
┌─────────────────────────────────────────────────────────────────┐
│                   DishtaYantraWebApp                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Singleton Pattern with Thread Safety                            │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │  Flask App   │  │  Components  │  │   Routes     │           │
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
│  │  │  users.json                                      │    │    │
│  │  │  {                                               │    │    │
│  │  │    "admin": {                                    │    │    │
│  │  │      "password": "...",                          │    │    │
│  │  │      "roles": ["admin", "user"],                 │    │    │
│  │  │      "full_name": "..."                          │    │    │
│  │  │    }                                             │    │    │
│  │  │  }                                               │    │    │
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
│  - Flask sessions with secure cookies                            │
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
│  └── Flask WSGI Server                                           │
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
| 1.1.1 | Dec 2025 | System monitoring, admin features, logs viewer |
| 1.1.0 | Dec 2025 | Java, C++, Rust, REST calculators, free-threading |
| 1.0.0 | Nov 2025 | Initial release |

### Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| Flask | 3.1.2 | Web framework |
| psutil | 6.1.0 | System monitoring |
| py4j | 0.10.9.7 | Java integration |
| requests | 2.32.3 | REST integration |
| kazoo | 2.10.0 | Zookeeper client |
| kafka-python | 2.2.15 | Kafka integration |

---

© 2025-2030 Ashutosh Sinha. All rights reserved.
