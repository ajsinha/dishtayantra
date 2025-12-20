# Prometheus Monitoring Integration Guide

**DishtaYantra v1.5.0**

**Author:** Ashutosh Sinha  
**Email:** ajsinha@gmail.com  
**Date:** December 2025

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Quick Start](#quick-start)
4. [Metrics Endpoints](#metrics-endpoints)
5. [Available Metrics](#available-metrics)
6. [Prometheus Configuration](#prometheus-configuration)
7. [Grafana Dashboard](#grafana-dashboard)
8. [Alert Rules](#alert-rules)
9. [Kubernetes Deployment](#kubernetes-deployment)
10. [Troubleshooting](#troubleshooting)

---

## Overview

DishtaYantra provides comprehensive Prometheus metrics for monitoring DAG execution, calculator performance, messaging throughput, cache efficiency, and system health. This integration enables real-time observability of your data processing pipelines.

### Key Features

- **Zero-configuration metrics** - Metrics are automatically collected
- **Pull-based scraping** - Standard Prometheus `/metrics` endpoint
- **Push gateway support** - For batch jobs and short-lived processes
- **Health endpoints** - Kubernetes-compatible liveness and readiness probes
- **Pre-built dashboards** - Grafana dashboard included
- **Alert rules** - Production-ready alerting configuration

---

## Prerequisites

### Required

- DishtaYantra v1.5.0 or later
- Python 3.8+

### Optional (for full monitoring stack)

- Prometheus 2.x
- Grafana 9.x
- Alertmanager (for alerts)

### Python Dependencies

```bash
pip install prometheus-client psutil
```

---

## Quick Start

### 1. Start DishtaYantra

```bash
python run_server.py
```

### 2. Verify Metrics Endpoint

```bash
curl http://localhost:5002/metrics
```

### 3. Check Health

```bash
curl http://localhost:5002/health
```

### 4. Start Prometheus

```bash
prometheus --config.file=docker/prometheus.yml
```

### 5. Access Prometheus UI

Open http://localhost:9090 in your browser.

---

## Metrics Endpoints

DishtaYantra exposes the following endpoints:

| Endpoint | Description | Authentication |
|----------|-------------|----------------|
| `/metrics` | Prometheus metrics in text format | None |
| `/metrics/json` | Metrics in JSON format (debugging) | None |
| `/health` | Overall health status | None |
| `/health/live` | Kubernetes liveness probe | None |
| `/health/ready` | Kubernetes readiness probe | None |

### Example: Metrics Response

```
# HELP dishtayantra_dag_executions_total Total number of DAG executions
# TYPE dishtayantra_dag_executions_total counter
dishtayantra_dag_executions_total{dag_name="trade_processor",status="success"} 1542.0
dishtayantra_dag_executions_total{dag_name="trade_processor",status="error"} 3.0

# HELP dishtayantra_dag_execution_duration_seconds DAG execution duration
# TYPE dishtayantra_dag_execution_duration_seconds histogram
dishtayantra_dag_execution_duration_seconds_bucket{dag_name="trade_processor",le="0.1"} 1200.0
dishtayantra_dag_execution_duration_seconds_bucket{dag_name="trade_processor",le="1.0"} 1500.0
```

### Example: Health Response

```json
{
  "status": "healthy",
  "timestamp": "2025-12-19T10:30:00Z",
  "version": "1.5.0",
  "uptime_seconds": 3600.5,
  "components": {
    "dag_server": {
      "status": "healthy",
      "dags_loaded": 5
    },
    "cache": {
      "status": "healthy",
      "type": "inmemory",
      "keys": 1250
    }
  }
}
```

---

## Available Metrics

### Application Info

| Metric | Type | Description |
|--------|------|-------------|
| `dishtayantra_app_info` | Info | Application version and metadata |

### DAG Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `dishtayantra_dag_executions_total` | Counter | dag_name, status | Total DAG executions |
| `dishtayantra_dag_execution_duration_seconds` | Histogram | dag_name | DAG execution latency |
| `dishtayantra_active_dags` | Gauge | - | Currently running DAGs |
| `dishtayantra_dag_nodes_total` | Gauge | dag_name | Nodes per DAG |
| `dishtayantra_dag_edges_total` | Gauge | dag_name | Edges per DAG |

### Node Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `dishtayantra_node_executions_total` | Counter | dag_name, node_name, node_type, status | Node executions |
| `dishtayantra_node_execution_duration_seconds` | Histogram | dag_name, node_name, node_type | Node latency |
| `dishtayantra_node_errors_total` | Counter | dag_name, node_name, node_type, error_type | Node errors |

### Calculator Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `dishtayantra_calculator_calls_total` | Counter | calculator_name, calculator_type, status | Calculator invocations |
| `dishtayantra_calculator_duration_seconds` | Histogram | calculator_name, calculator_type | Calculator latency |
| `dishtayantra_calculator_errors_total` | Counter | calculator_name, calculator_type, error_type | Calculator errors |

### Messaging Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `dishtayantra_messages_received_total` | Counter | transport, topic, dag_name | Messages received |
| `dishtayantra_messages_published_total` | Counter | transport, topic, dag_name | Messages published |
| `dishtayantra_message_processing_duration_seconds` | Histogram | transport, topic | Processing latency |
| `dishtayantra_message_size_bytes` | Histogram | transport, direction | Message sizes |
| `dishtayantra_message_queue_depth` | Gauge | transport, queue_name | Queue depth |
| `dishtayantra_kafka_consumer_lag` | Gauge | consumer_group, topic, partition | Kafka lag |

### Cache Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `dishtayantra_cache_hits_total` | Counter | cache_name | Cache hits |
| `dishtayantra_cache_misses_total` | Counter | cache_name | Cache misses |
| `dishtayantra_cache_size` | Gauge | cache_name | Number of cached items |
| `dishtayantra_cache_size_bytes` | Gauge | cache_name | Cache size in bytes |
| `dishtayantra_cache_evictions_total` | Counter | cache_name | Cache evictions |

### Database Pool Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `dishtayantra_db_pool_connections_active` | Gauge | database | Active connections |
| `dishtayantra_db_pool_connections_idle` | Gauge | database | Idle connections |
| `dishtayantra_db_pool_connections_max` | Gauge | database | Max pool size |
| `dishtayantra_db_query_duration_seconds` | Histogram | database, query_type | Query latency |

### System Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `dishtayantra_system_cpu_usage` | Gauge | CPU usage percentage |
| `dishtayantra_system_memory_usage` | Gauge | Memory usage in bytes |
| `dishtayantra_system_memory_percent` | Gauge | Memory usage percentage |
| `dishtayantra_process_threads` | Gauge | Active thread count |
| `dishtayantra_uptime_seconds` | Gauge | Application uptime |
| `dishtayantra_health_check_status` | Gauge | Component health (1=healthy) |

---

## Prometheus Configuration

### Basic Configuration

Create `prometheus.yml`:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'dishtayantra'
    static_configs:
      - targets: ['localhost:5002']
    metrics_path: '/metrics'
```

### Multiple Instances

```yaml
scrape_configs:
  - job_name: 'dishtayantra'
    static_configs:
      - targets:
          - 'server1:5002'
          - 'server2:5002'
          - 'server3:5002'
        labels:
          environment: 'production'
```

### With Authentication (if enabled)

```yaml
scrape_configs:
  - job_name: 'dishtayantra'
    static_configs:
      - targets: ['localhost:5002']
    basic_auth:
      username: 'prometheus'
      password: 'secret'
```

---

## Grafana Dashboard

### Import Dashboard

1. Open Grafana
2. Go to Dashboards → Import
3. Upload `docker/grafana_dashboard.json`
4. Select Prometheus data source
5. Click Import

### Dashboard Panels

The pre-built dashboard includes:

- **Active DAGs** - Current running DAG count
- **DAG Executions/sec** - Throughput rate
- **CPU/Memory Usage** - System resources
- **DAG Execution Latency** - p50, p95, p99 percentiles
- **DAG Executions by Status** - Success vs Error breakdown
- **Message Throughput** - Received/Published rates
- **Calculator Latency** - Per-calculator performance
- **Cache Hit/Miss Rate** - Cache efficiency
- **Error Rate by Type** - Error breakdown

---

## Alert Rules

### Enable Alerts

1. Copy alert rules to Prometheus:
   ```bash
   cp docker/alert_rules.yml /etc/prometheus/
   ```

2. Update `prometheus.yml`:
   ```yaml
   rule_files:
     - "alert_rules.yml"
   ```

3. Reload Prometheus:
   ```bash
   kill -HUP $(pgrep prometheus)
   ```

### Key Alerts

| Alert | Condition | Severity |
|-------|-----------|----------|
| DishtaYantraDown | Application unreachable | Critical |
| DAGExecutionErrors | Error rate > 10% | Warning |
| DAGExecutionSlow | p95 latency > 30s | Warning |
| HighCPUUsage | CPU > 80% | Warning |
| HighMemoryUsage | Memory > 85% | Warning |
| KafkaConsumerLag | Lag > 10000 | Warning |
| DBPoolExhausted | All connections used | Critical |

---

## Kubernetes Deployment

### Pod Annotations

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: dishtayantra
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "5002"
    prometheus.io/path: "/metrics"
spec:
  containers:
    - name: dishtayantra
      image: dishtayantra:1.5.0
      ports:
        - containerPort: 5002
      livenessProbe:
        httpGet:
          path: /health/live
          port: 5002
        initialDelaySeconds: 10
        periodSeconds: 10
      readinessProbe:
        httpGet:
          path: /health/ready
          port: 5002
        initialDelaySeconds: 5
        periodSeconds: 5
```

### ServiceMonitor (Prometheus Operator)

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: dishtayantra
  labels:
    app: dishtayantra
spec:
  selector:
    matchLabels:
      app: dishtayantra
  endpoints:
    - port: http
      path: /metrics
      interval: 15s
```

---

## Troubleshooting

### Metrics Not Available

1. Check if prometheus-client is installed:
   ```bash
   pip install prometheus-client
   ```

2. Check application logs for errors:
   ```bash
   tail -f logs/dagserver.log | grep -i metric
   ```

3. Test endpoint directly:
   ```bash
   curl -v http://localhost:5002/metrics
   ```

### High Cardinality Issues

Avoid using high-cardinality labels like trade IDs or timestamps. Use aggregated labels instead.

### Memory Usage

If metrics consume too much memory, reduce histogram buckets or disable unused metrics in configuration.

### Prometheus Scrape Errors

Check Prometheus targets page: http://localhost:9090/targets

Common issues:
- Firewall blocking port 5002
- Incorrect target address
- SSL/TLS misconfiguration

---

## Using Metrics in Code

### Instrumenting Custom Calculators

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
        # Your calculation logic
        return result
```

### Recording Custom Metrics

```python
from core.metrics import metrics

# Increment counter
metrics.dag_executions.labels(dag_name="my_dag", status="success").inc()

# Record histogram observation
metrics.dag_execution_duration.labels(dag_name="my_dag").observe(0.5)

# Set gauge value
metrics.active_dags.set(3)

# Time a block of code
with metrics.calculator_duration.labels(
    calculator_name="my_calc",
    calculator_type="python"
).time():
    perform_calculation()
```

### Push to Pushgateway

For batch jobs:

```python
from core.metrics import metrics

# After batch job completes
metrics.push_to_gateway(
    gateway="localhost:9091",
    job="batch_processor",
    grouping_key={"instance": "batch-1"}
)
```

---

## Best Practices

1. **Use meaningful labels** - Include dag_name, node_type, calculator_name
2. **Avoid high cardinality** - Don't use unique IDs as labels
3. **Set appropriate scrape intervals** - 15s is good for most cases
4. **Configure alerts early** - Don't wait for production issues
5. **Monitor the monitors** - Ensure Prometheus itself is healthy
6. **Use recording rules** - Pre-compute expensive queries
7. **Retain data appropriately** - Balance storage vs history needs

---

## Support

For issues with Prometheus integration:

1. Check the [Troubleshooting](#troubleshooting) section
2. Review application logs
3. Consult Prometheus documentation
4. Open an issue on the project repository
