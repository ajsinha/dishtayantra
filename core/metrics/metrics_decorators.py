"""
Metrics Decorators & Helpers (v2.2 module split)
================================================

track_execution_time / count_calls / time_block decorators and
update_system_metrics, extracted verbatim from prometheus_metrics.py to
respect the 500-line architecture limit. Re-exported from
core.metrics.prometheus_metrics.

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



from core.metrics.prometheus_metrics import metrics

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
