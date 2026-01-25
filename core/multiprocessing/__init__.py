"""
DishtaYantra Multiprocessing Module
Version: 1.5.2

Re-exports worker pool components from core.workers module.

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

# Re-export from workers module
from core.workers import (
    WorkerPoolManager,
    DAGWorkerProcess,
    DAGAffinityManager as AffinityManager,
    AffinityStrategy,
    WorkerHealthMonitor as HealthMonitor
)

from core.workers.worker_monitor import WorkerState

__all__ = [
    'WorkerPoolManager',
    'DAGWorkerProcess',
    'AffinityManager',
    'AffinityStrategy',
    'HealthMonitor',
    'WorkerState'
]

__version__ = '1.5.2'
