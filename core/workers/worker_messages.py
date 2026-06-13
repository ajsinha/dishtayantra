"""
Worker IPC Messages (v2.0.0 module split)
=========================================

Control/status message types, dataclasses, and worker logging setup shared
between the worker pool and the worker process, extracted verbatim from
worker_process.py so each module stays within the 500-line architecture
limit. All names are re-exported from core.workers.worker_process.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import os
import sys
import time
import json
import queue
import signal
import logging
import traceback
import threading
import warnings
from multiprocessing import Process, Queue, Event
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum

# Suppress LMDB GIL warnings (Python 3.13+ free-threading mode)
warnings.filterwarnings("ignore", message=".*GIL.*lmdb.*", category=RuntimeWarning)

# Configure logging for worker process
def setup_worker_logging(worker_id: int):
    """Setup logging for worker process"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Create handler for this worker
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        f'%(asctime)s [Worker-{worker_id}] %(levelname)s %(name)s: %(message)s'
    ))
    logger.handlers = [handler]
    
    return logging.getLogger(__name__)




class ControlMessageType(Enum):
    """Types of control messages from orchestrator"""
    LOAD_DAG = "load_dag"
    UNLOAD_DAG = "unload_dag"
    RELOAD_DAG = "reload_dag"
    PAUSE_DAG = "pause_dag"
    RESUME_DAG = "resume_dag"
    SHUTDOWN = "shutdown"
    PING = "ping"
    GET_STATUS = "get_status"
    GET_DAG_STATE = "get_dag_state"  # v1.5.2: Request full DAG state for UI


class StatusMessageType(Enum):
    """Types of status messages to orchestrator"""
    HEARTBEAT = "heartbeat"
    DAG_LOADED = "dag_loaded"
    DAG_UNLOADED = "dag_unloaded"
    DAG_ERROR = "dag_error"
    WORKER_READY = "worker_ready"
    WORKER_STOPPING = "worker_stopping"
    PONG = "pong"
    DAG_STATE = "dag_state"  # v1.5.2: Full DAG state response for UI


@dataclass
class ControlMessage:
    """Message from orchestrator to worker"""
    type: str
    dag_name: Optional[str] = None
    dag_config: Optional[dict] = None
    timestamp: float = field(default_factory=time.time)
    message_id: str = ""


@dataclass
class StatusMessage:
    """Message from worker to orchestrator"""
    type: str
    worker_id: int
    timestamp: float = field(default_factory=time.time)
    dag_name: Optional[str] = None
    data: Optional[dict] = None
    error: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            'type': self.type,
            'worker_id': self.worker_id,
            'timestamp': self.timestamp,
            'dag_name': self.dag_name,
            'data': self.data,
            'error': self.error
        }


@dataclass
class DAGStats:
    """Runtime statistics for a DAG"""
    name: str
    status: str = "running"
    nodes_executed: int = 0
    messages_processed: int = 0
    errors: int = 0
    last_execution: Optional[float] = None
    avg_cycle_time_ms: float = 0.0
    cpu_percent: float = 0.0
    
    def to_dict(self) -> dict:
        return asdict(self)


