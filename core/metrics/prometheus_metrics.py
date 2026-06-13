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



# v2.2 module split: the metric catalogue lives in metric_definitions.py.
from core.metrics.metric_definitions import MetricDefinitionsMixin  # noqa: F401


class DishtaYantraMetrics(MetricDefinitionsMixin):
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

# v2.2 module split: decorators + helpers live in metrics_decorators.py
# (imported AFTER the metrics singleton above, which they bind to).
from core.metrics.metrics_decorators import (  # noqa: E402,F401
    count_calls,
    time_block,
    track_execution_time,
    update_system_metrics,
)
