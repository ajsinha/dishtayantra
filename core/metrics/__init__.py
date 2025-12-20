"""
Metrics Module for DishtaYantra
===============================

Provides Prometheus metrics instrumentation for monitoring
DAG execution, calculator performance, messaging, and system health.

Usage:
    from core.metrics import metrics
    
    # Increment counters
    metrics.dag_executions.labels(dag_name="my_dag", status="success").inc()
    
    # Record histograms
    with metrics.dag_execution_duration.labels(dag_name="my_dag").time():
        execute_dag()

Author: Ashutosh Sinha
Email: ajsinha@gmail.com
Version: 1.5.0
Date: December 2025
"""

from .prometheus_metrics import (
    metrics,
    DishtaYantraMetrics,
    PROMETHEUS_AVAILABLE,
    track_execution_time,
    count_calls,
    time_block,
    update_system_metrics,
)

__all__ = [
    'metrics',
    'DishtaYantraMetrics',
    'PROMETHEUS_AVAILABLE',
    'track_execution_time',
    'count_calls',
    'time_block',
    'update_system_metrics',
]

__version__ = '1.5.0'
