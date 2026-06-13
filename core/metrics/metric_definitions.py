"""
Metric Definitions Mixin (v2.2 module split)
============================================

The _create_metrics catalogue (every Counter/Gauge/Histogram the server
exposes), extracted verbatim from prometheus_metrics.py to respect the
500-line architecture limit. All state lives on DishtaYantraMetrics.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

"""
Prometheus Metrics Module for DishtaYantra
==========================================

Provides comprehensive metrics instrumentation for monitoring
DAG execution, calculator performance, messaging, and system health.

Metrics are exposed via a /metrics endpoint for Prometheus scraping.

Usage:
    from core.metrics import metrics
    
    # Increment counters
    metrics.dag_executions.labels(dag_name="my_dag", status="success").inc()
    
    # Record histograms
    with metrics.dag_execution_duration.labels(dag_name="my_dag").time():
        execute_dag()
    
    # Set gauges
    metrics.active_dags.set(5)

Author: Ashutosh Sinha
Email: ajsinha@gmail.com
Version: 1.5.0
Date: December 2025
"""

import time
import functools
import logging
from typing import Optional, Callable, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Try to import prometheus_client
try:
    from prometheus_client import (
        Counter, Gauge, Histogram, Summary, Info,
        CollectorRegistry, REGISTRY,
        generate_latest, CONTENT_TYPE_LATEST,
        push_to_gateway, delete_from_gateway
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logger.warning("prometheus_client not installed. Metrics will be disabled. "
                   "Install with: pip install prometheus-client")


# Default histogram buckets for latency metrics (in seconds)
LATENCY_BUCKETS = (
    0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75,
    1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, 75.0, 100.0, float("inf")
)

# Buckets for message size (in bytes)
SIZE_BUCKETS = (
    100, 500, 1000, 5000, 10000, 50000, 100000, 500000,
    1000000, 5000000, 10000000, float("inf")
)




class MetricDefinitionsMixin:
    """Defines the full DishtaYantra metric catalogue on the registry."""

    def _create_metrics(self):
        """Create all Prometheus metrics."""
        
        # =====================================================================
        # Application Info
        # =====================================================================
        self.app_info = Info(
            'dishtayantra_app',
            'DishtaYantra application information',
            registry=self._registry
        )
        self.app_info.info({
            'version': '1.5.2',
            'name': 'DishtaYantra',
            'description': 'Real-Time DAG Processing Engine'
        })
        
        # =====================================================================
        # DAG Metrics
        # =====================================================================
        self.dag_executions = Counter(
            'dishtayantra_dag_executions_total',
            'Total number of DAG executions',
            ['dag_name', 'status'],
            registry=self._registry
        )
        
        self.dag_execution_duration = Histogram(
            'dishtayantra_dag_execution_duration_seconds',
            'DAG execution duration in seconds',
            ['dag_name'],
            buckets=LATENCY_BUCKETS,
            registry=self._registry
        )
        
        self.active_dags = Gauge(
            'dishtayantra_active_dags',
            'Number of currently active DAGs',
            registry=self._registry
        )
        
        self.dag_nodes_total = Gauge(
            'dishtayantra_dag_nodes_total',
            'Total number of nodes in DAG',
            ['dag_name'],
            registry=self._registry
        )
        
        self.dag_edges_total = Gauge(
            'dishtayantra_dag_edges_total',
            'Total number of edges in DAG',
            ['dag_name'],
            registry=self._registry
        )
        
        self.dag_last_execution_timestamp = Gauge(
            'dishtayantra_dag_last_execution_timestamp',
            'Timestamp of last DAG execution',
            ['dag_name'],
            registry=self._registry
        )
        
        # =====================================================================
        # Node Metrics
        # =====================================================================
        self.node_executions = Counter(
            'dishtayantra_node_executions_total',
            'Total number of node executions',
            ['dag_name', 'node_name', 'node_type', 'status'],
            registry=self._registry
        )
        
        self.node_execution_duration = Histogram(
            'dishtayantra_node_execution_duration_seconds',
            'Node execution duration in seconds',
            ['dag_name', 'node_name', 'node_type'],
            buckets=LATENCY_BUCKETS,
            registry=self._registry
        )
        
        self.node_errors = Counter(
            'dishtayantra_node_errors_total',
            'Total number of node execution errors',
            ['dag_name', 'node_name', 'node_type', 'error_type'],
            registry=self._registry
        )
        
        # =====================================================================
        # Calculator Metrics
        # =====================================================================
        self.calculator_calls = Counter(
            'dishtayantra_calculator_calls_total',
            'Total number of calculator invocations',
            ['calculator_name', 'calculator_type', 'status'],
            registry=self._registry
        )
        
        self.calculator_duration = Histogram(
            'dishtayantra_calculator_duration_seconds',
            'Calculator execution duration in seconds',
            ['calculator_name', 'calculator_type'],
            buckets=LATENCY_BUCKETS,
            registry=self._registry
        )
        
        self.calculator_errors = Counter(
            'dishtayantra_calculator_errors_total',
            'Total number of calculator errors',
            ['calculator_name', 'calculator_type', 'error_type'],
            registry=self._registry
        )
        
        # =====================================================================
        # Messaging Metrics (Kafka, Redis, etc.)
        # =====================================================================
        self.messages_received = Counter(
            'dishtayantra_messages_received_total',
            'Total messages received',
            ['transport', 'topic', 'dag_name'],
            registry=self._registry
        )
        
        self.messages_published = Counter(
            'dishtayantra_messages_published_total',
            'Total messages published',
            ['transport', 'topic', 'dag_name'],
            registry=self._registry
        )
        
        self.message_processing_duration = Histogram(
            'dishtayantra_message_processing_duration_seconds',
            'Message processing duration',
            ['transport', 'topic'],
            buckets=LATENCY_BUCKETS,
            registry=self._registry
        )
        
        self.message_size_bytes = Histogram(
            'dishtayantra_message_size_bytes',
            'Size of messages in bytes',
            ['transport', 'direction'],
            buckets=SIZE_BUCKETS,
            registry=self._registry
        )
        
        self.message_queue_depth = Gauge(
            'dishtayantra_message_queue_depth',
            'Current message queue depth',
            ['transport', 'queue_name'],
            registry=self._registry
        )
        
        self.kafka_consumer_lag = Gauge(
            'dishtayantra_kafka_consumer_lag',
            'Kafka consumer lag (messages behind)',
            ['topic', 'partition', 'consumer_group'],
            registry=self._registry
        )
        
        # =====================================================================
        # Cache Metrics
        # =====================================================================
        self.cache_hits = Counter(
            'dishtayantra_cache_hits_total',
            'Total cache hits',
            ['cache_name'],
            registry=self._registry
        )
        
        self.cache_misses = Counter(
            'dishtayantra_cache_misses_total',
            'Total cache misses',
            ['cache_name'],
            registry=self._registry
        )
        
        self.cache_size = Gauge(
            'dishtayantra_cache_size',
            'Current cache size (number of items)',
            ['cache_name'],
            registry=self._registry
        )
        
        self.cache_size_bytes = Gauge(
            'dishtayantra_cache_size_bytes',
            'Current cache size in bytes',
            ['cache_name'],
            registry=self._registry
        )
        
        self.cache_evictions = Counter(
            'dishtayantra_cache_evictions_total',
            'Total cache evictions',
            ['cache_name', 'reason'],
            registry=self._registry
        )
        
        # =====================================================================
        # Database Connection Pool Metrics
        # =====================================================================
        self.db_pool_connections_active = Gauge(
            'dishtayantra_db_pool_connections_active',
            'Active database connections',
            ['pool_name'],
            registry=self._registry
        )
        
        self.db_pool_connections_idle = Gauge(
            'dishtayantra_db_pool_connections_idle',
            'Idle database connections',
            ['pool_name'],
            registry=self._registry
        )
        
        self.db_pool_connections_max = Gauge(
            'dishtayantra_db_pool_connections_max',
            'Maximum database connections',
            ['pool_name'],
            registry=self._registry
        )
        
        self.db_query_duration = Histogram(
            'dishtayantra_db_query_duration_seconds',
            'Database query duration',
            ['pool_name', 'query_type'],
            buckets=LATENCY_BUCKETS,
            registry=self._registry
        )
        
        # =====================================================================
        # Subgraph Metrics
        # =====================================================================
        self.subgraph_executions = Counter(
            'dishtayantra_subgraph_executions_total',
            'Total subgraph executions',
            ['dag_name', 'subgraph_name', 'status'],
            registry=self._registry
        )
        
        self.subgraph_duration = Histogram(
            'dishtayantra_subgraph_duration_seconds',
            'Subgraph execution duration',
            ['dag_name', 'subgraph_name'],
            buckets=LATENCY_BUCKETS,
            registry=self._registry
        )
        
        # =====================================================================
        # Transformer Metrics
        # =====================================================================
        self.transformer_executions = Counter(
            'dishtayantra_transformer_executions_total',
            'Total transformer executions',
            ['transformer_name', 'status'],
            registry=self._registry
        )
        
        self.transformer_duration = Histogram(
            'dishtayantra_transformer_duration_seconds',
            'Transformer execution duration',
            ['transformer_name'],
            buckets=LATENCY_BUCKETS,
            registry=self._registry
        )
        
        # =====================================================================
        # System/Resource Metrics
        # =====================================================================
        self.system_cpu_usage = Gauge(
            'dishtayantra_system_cpu_usage_percent',
            'System CPU usage percentage',
            registry=self._registry
        )
        
        self.system_memory_usage = Gauge(
            'dishtayantra_system_memory_usage_bytes',
            'System memory usage in bytes',
            registry=self._registry
        )
        
        self.system_memory_percent = Gauge(
            'dishtayantra_system_memory_usage_percent',
            'System memory usage percentage',
            registry=self._registry
        )
        
        self.process_threads = Gauge(
            'dishtayantra_process_threads',
            'Number of threads in process',
            registry=self._registry
        )
        
        self.uptime_seconds = Gauge(
            'dishtayantra_uptime_seconds',
            'Application uptime in seconds',
            registry=self._registry
        )
        
        # =====================================================================
        # Error/Health Metrics
        # =====================================================================
        self.errors_total = Counter(
            'dishtayantra_errors_total',
            'Total errors by type',
            ['error_type', 'component'],
            registry=self._registry
        )
        
        self.health_check_status = Gauge(
            'dishtayantra_health_check_status',
            'Health check status (1=healthy, 0=unhealthy)',
            ['component'],
            registry=self._registry
        )
        
        logger.info("Prometheus metrics initialized successfully")
    
