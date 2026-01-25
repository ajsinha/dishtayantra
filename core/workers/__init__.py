"""
DishtaYantra Worker Pool Module
Version: 1.5.2

Provides multiprocessing-based worker pool for DAG execution with:
- DAG affinity (DAGs pinned to specific workers)
- Smart load balancing
- Health monitoring and auto-restart
- Cross-worker communication via LMDB

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

from .worker_pool import WorkerPoolManager
from .worker_process import DAGWorkerProcess
from .dag_affinity import DAGAffinityManager, AffinityStrategy
from .worker_monitor import WorkerHealthMonitor

__all__ = [
    'WorkerPoolManager',
    'DAGWorkerProcess', 
    'DAGAffinityManager',
    'AffinityStrategy',
    'WorkerHealthMonitor'
]

__version__ = '1.5.2'
