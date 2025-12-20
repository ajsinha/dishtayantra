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


class DishtaYantraMetrics:
    """
    Central metrics registry for DishtaYantra.
    
    Provides all Prometheus metrics used throughout the system.
    Metrics are organized by category:
    - DAG metrics
    - Node metrics
    - Calculator metrics
    - Messaging metrics (Kafka, Redis, etc.)
    - Cache metrics
    - System metrics
    """
    
    def __init__(self, registry: Optional[Any] = None):
        """
        Initialize metrics.
        
        Args:
            registry: Optional custom CollectorRegistry
        """
        self._enabled = PROMETHEUS_AVAILABLE
        self._registry = registry or (REGISTRY if PROMETHEUS_AVAILABLE else None)
        
        if not self._enabled:
            self._create_dummy_metrics()
            return
        
        self._create_metrics()
    
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
            'version': '1.5.0',
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
    
    def _create_dummy_metrics(self):
        """Create dummy metrics when prometheus_client is not available."""
        
        class DummyMetric:
            """Dummy metric that does nothing."""
            def labels(self, *args, **kwargs):
                return self
            def inc(self, *args, **kwargs):
                pass
            def dec(self, *args, **kwargs):
                pass
            def set(self, *args, **kwargs):
                pass
            def observe(self, *args, **kwargs):
                pass
            def info(self, *args, **kwargs):
                pass
            @contextmanager
            def time(self):
                yield
        
        dummy = DummyMetric()
        
        # Set all metrics to dummy
        self.app_info = dummy
        self.dag_executions = dummy
        self.dag_execution_duration = dummy
        self.active_dags = dummy
        self.dag_nodes_total = dummy
        self.dag_edges_total = dummy
        self.dag_last_execution_timestamp = dummy
        self.node_executions = dummy
        self.node_execution_duration = dummy
        self.node_errors = dummy
        self.calculator_calls = dummy
        self.calculator_duration = dummy
        self.calculator_errors = dummy
        self.messages_received = dummy
        self.messages_published = dummy
        self.message_processing_duration = dummy
        self.message_size_bytes = dummy
        self.message_queue_depth = dummy
        self.kafka_consumer_lag = dummy
        self.cache_hits = dummy
        self.cache_misses = dummy
        self.cache_size = dummy
        self.cache_size_bytes = dummy
        self.cache_evictions = dummy
        self.db_pool_connections_active = dummy
        self.db_pool_connections_idle = dummy
        self.db_pool_connections_max = dummy
        self.db_query_duration = dummy
        self.subgraph_executions = dummy
        self.subgraph_duration = dummy
        self.transformer_executions = dummy
        self.transformer_duration = dummy
        self.system_cpu_usage = dummy
        self.system_memory_usage = dummy
        self.system_memory_percent = dummy
        self.process_threads = dummy
        self.uptime_seconds = dummy
        self.errors_total = dummy
        self.health_check_status = dummy
        
        logger.warning("Using dummy metrics (prometheus_client not available)")
    
    def is_enabled(self) -> bool:
        """Check if metrics are enabled."""
        return self._enabled
    
    def generate_latest(self) -> bytes:
        """Generate latest metrics in Prometheus format."""
        if not self._enabled:
            return b"# Prometheus metrics disabled\n"
        return generate_latest(self._registry)
    
    def get_content_type(self) -> str:
        """Get content type for metrics response."""
        if not self._enabled:
            return "text/plain"
        return CONTENT_TYPE_LATEST
    
    def push_to_gateway(
        self,
        gateway: str,
        job: str,
        grouping_key: Optional[dict] = None
    ) -> bool:
        """
        Push metrics to Prometheus Pushgateway.
        
        Args:
            gateway: Pushgateway address (e.g., "localhost:9091")
            job: Job name
            grouping_key: Optional grouping key dict
            
        Returns:
            True if successful
        """
        if not self._enabled:
            return False
        
        try:
            push_to_gateway(gateway, job=job, registry=self._registry,
                           grouping_key=grouping_key or {})
            return True
        except Exception as e:
            logger.error(f"Failed to push metrics to gateway: {e}")
            return False
    
    def delete_from_gateway(
        self,
        gateway: str,
        job: str,
        grouping_key: Optional[dict] = None
    ) -> bool:
        """Delete metrics from Pushgateway."""
        if not self._enabled:
            return False
        
        try:
            delete_from_gateway(gateway, job=job,
                               grouping_key=grouping_key or {})
            return True
        except Exception as e:
            logger.error(f"Failed to delete metrics from gateway: {e}")
            return False


# Singleton metrics instance
metrics = DishtaYantraMetrics()


# =============================================================================
# Decorator Helpers
# =============================================================================

def track_execution_time(
    histogram_metric,
    labels: Optional[dict] = None
) -> Callable:
    """
    Decorator to track function execution time.
    
    Usage:
        @track_execution_time(metrics.calculator_duration, 
                              {'calculator_name': 'my_calc', 'calculator_type': 'python'})
        def my_function():
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if labels:
                with histogram_metric.labels(**labels).time():
                    return func(*args, **kwargs)
            else:
                with histogram_metric.time():
                    return func(*args, **kwargs)
        return wrapper
    return decorator


def count_calls(
    counter_metric,
    labels: Optional[dict] = None,
    success_label: str = 'status'
) -> Callable:
    """
    Decorator to count function calls with success/failure tracking.
    
    Usage:
        @count_calls(metrics.calculator_calls,
                     {'calculator_name': 'my_calc', 'calculator_type': 'python'})
        def my_function():
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            metric_labels = dict(labels) if labels else {}
            try:
                result = func(*args, **kwargs)
                metric_labels[success_label] = 'success'
                counter_metric.labels(**metric_labels).inc()
                return result
            except Exception as e:
                metric_labels[success_label] = 'error'
                counter_metric.labels(**metric_labels).inc()
                raise
        return wrapper
    return decorator


@contextmanager
def time_block(histogram_metric, labels: Optional[dict] = None):
    """
    Context manager to time a block of code.
    
    Usage:
        with time_block(metrics.dag_execution_duration, {'dag_name': 'my_dag'}):
            execute_dag()
    """
    start = time.time()
    try:
        yield
    finally:
        duration = time.time() - start
        if labels:
            histogram_metric.labels(**labels).observe(duration)
        else:
            histogram_metric.observe(duration)


def update_system_metrics():
    """Update system resource metrics."""
    if not metrics.is_enabled():
        return
    
    try:
        import psutil
        
        # CPU
        metrics.system_cpu_usage.set(psutil.cpu_percent())
        
        # Memory
        mem = psutil.virtual_memory()
        metrics.system_memory_usage.set(mem.used)
        metrics.system_memory_percent.set(mem.percent)
        
        # Threads
        import threading
        metrics.process_threads.set(threading.active_count())
        
    except ImportError:
        pass
    except Exception as e:
        logger.debug(f"Failed to update system metrics: {e}")
