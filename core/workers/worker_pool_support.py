"""
Worker Pool Support Mixins (v2.2 module split)
==============================================

Status-message processing / worker restart handling, and the status-query
API for WorkerPoolManager, extracted verbatim from worker_pool.py to
respect the 500-line architecture limit. All state lives on the manager.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

"""
Worker Pool Manager
Version: 1.5.2

Manages a pool of worker processes for DAG execution.

Features:
- Dynamic worker spawning based on configuration
- DAG affinity management
- Health monitoring and auto-restart
- Cross-worker communication via LMDB

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

import os
import json
import queue
import logging
import threading
import time
from multiprocessing import Process, Queue, Event
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime

from .worker_process import DAGWorkerProcess, ControlMessageType, StatusMessageType
from .dag_affinity import DAGAffinityManager, AffinityStrategy
from .worker_monitor import WorkerHealthMonitor, WorkerState

logger = logging.getLogger(__name__)


@dataclass


class WorkerHealthMixin:
    """Status-message pump, worker restart and unhealthy-worker handling."""

    def _process_status_messages(self):
        """Process status messages from workers"""
        logger.info("Status message processor started")
        
        while self._running or not self.status_queue.empty():
            try:
                msg = self.status_queue.get(timeout=0.5)
                self._handle_status_message(msg)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error processing status message: {e}")
        
        logger.info("Status message processor stopped")
    
    def _handle_status_message(self, msg: dict):
        """Handle a status message from a worker"""
        msg_type = msg.get('type')
        worker_id = msg.get('worker_id')
        
        if msg_type == StatusMessageType.HEARTBEAT.value:
            data = msg.get('data', {})
            self.health_monitor.update_heartbeat(worker_id, data)
            
            # Update worker info
            if worker_id in self.workers:
                worker_info = self.workers[worker_id]
                worker_info.cpu_percent = data.get('cpu_percent', 0)
                worker_info.memory_mb = data.get('memory_mb', 0)
                worker_info.dag_count = data.get('dag_count', 0)
                worker_info.loaded_dags = data.get('loaded_dags', [])
                worker_info.pid = data.get('pid')
            
            # Update affinity manager with worker metrics
            self.affinity_manager.update_worker_status(
                worker_id,
                data.get('cpu_percent', 0),
                data.get('memory_mb', 0),
                data.get('dag_stats')
            )
        
        elif msg_type == StatusMessageType.WORKER_READY.value:
            pid = msg.get('data', {}).get('pid')
            self.health_monitor.mark_worker_ready(worker_id, pid)
            if worker_id in self.workers:
                self.workers[worker_id].pid = pid
        
        elif msg_type == StatusMessageType.DAG_LOADED.value:
            dag_name = msg.get('dag_name')
            logger.info(f"DAG '{dag_name}' loaded on worker {worker_id}")
        
        elif msg_type == StatusMessageType.DAG_UNLOADED.value:
            dag_name = msg.get('dag_name')
            logger.info(f"DAG '{dag_name}' unloaded from worker {worker_id}")
        
        elif msg_type == StatusMessageType.DAG_ERROR.value:
            dag_name = msg.get('dag_name')
            error = msg.get('error')
            logger.error(f"DAG '{dag_name}' error on worker {worker_id}: {error}")
        
        elif msg_type == StatusMessageType.WORKER_STOPPING.value:
            self.health_monitor.mark_worker_stopping(worker_id)
        
        elif msg_type == StatusMessageType.PONG.value:
            logger.debug(f"Received pong from worker {worker_id}")
        
        elif msg_type == StatusMessageType.DAG_STATE.value:
            # v1.5.2: Handle DAG state response for UI
            dag_name = msg.get('dag_name')
            if dag_name in self._pending_state_requests:
                request = self._pending_state_requests[dag_name]
                request['data'] = msg.get('data')
                request['error'] = msg.get('error')
                request['event'].set()
                logger.debug(f"Received DAG state for {dag_name} from worker {worker_id}")
    
    def _handle_worker_restart(self, worker_id: int, health_status):
        """Handle worker restart request from health monitor"""
        logger.info(f"Handling restart for worker {worker_id}")
        
        with self._lock:
            # Get DAGs that were on this worker
            dags_to_restore = self.affinity_manager.get_dags_on_worker(worker_id)
            
            # Clean up old worker
            if worker_id in self.workers:
                old_worker = self.workers[worker_id]
                if old_worker.process and old_worker.process.is_alive():
                    old_worker.process.terminate()
                    old_worker.process.join(timeout=2)
            
            # Spawn new worker
            self._spawn_worker(worker_id)
            
            # Wait for worker to be ready
            time.sleep(1)  # Brief wait for process to start
            
            # Restore DAGs
            for dag_name in dags_to_restore:
                if dag_name in self.dag_configs:
                    logger.info(f"Restoring DAG '{dag_name}' on worker {worker_id}")
                    self._send_control(worker_id, {
                        'type': ControlMessageType.LOAD_DAG.value,
                        'dag_name': dag_name,
                        'dag_config': self.dag_configs[dag_name]
                    })
    
    def _handle_worker_unhealthy(self, worker_id: int, health_status):
        """Handle worker becoming unhealthy"""
        logger.warning(f"Worker {worker_id} is unhealthy")
        
        # Mark worker as unhealthy in affinity manager
        self.affinity_manager.mark_worker_unhealthy(worker_id)
    
    # ===================
    # Public API
    # ===================
    


class WorkerStatusMixin:
    """Status / assignment / load-summary query API."""

    def get_worker_status(self, worker_id: int) -> Optional[dict]:
        """Get status of a specific worker"""
        if worker_id not in self.workers:
            return None
        
        worker_info = self.workers[worker_id]
        health = self.health_monitor.get_health_status(worker_id)
        
        return {
            'worker_id': worker_id,
            'pid': worker_info.pid,
            'is_alive': worker_info.process.is_alive() if worker_info.process else False,
            'state': health.state.value if health else 'unknown',
            'cpu_percent': worker_info.cpu_percent,
            'memory_mb': worker_info.memory_mb,
            'dag_count': worker_info.dag_count,
            'loaded_dags': worker_info.loaded_dags,
            'start_time': worker_info.start_time.isoformat() if worker_info.start_time else None,
            'restart_count': health.restart_count if health else 0
        }
    
    def get_all_workers_status(self) -> Dict[int, dict]:
        """Get status of all workers"""
        return {
            worker_id: self.get_worker_status(worker_id)
            for worker_id in range(self.num_workers)
        }
    
    def get_dag_assignment(self, dag_name: str) -> Optional[int]:
        """Get the worker ID a DAG is assigned to"""
        return self.affinity_manager.get_worker_assignment(dag_name)
    
    def get_dag_state(self, dag_name: str, timeout: float = 5.0) -> Optional[dict]:
        """
        Get full DAG state from worker for UI display (v1.5.2)
        
        Sends a GET_DAG_STATE request to the worker running the DAG and
        waits for the response containing node states.
        
        Args:
            dag_name: Name of the DAG
            timeout: Timeout in seconds to wait for response
            
        Returns:
            dict with node_states, subscriber_states, etc. or None if failed
        """
        # Find which worker has this DAG
        worker_id = self.get_dag_assignment(dag_name)
        if worker_id is None:
            logger.warning(f"Cannot get state for {dag_name}: not assigned to any worker")
            return None
        
        if worker_id not in self.workers:
            logger.warning(f"Cannot get state for {dag_name}: worker {worker_id} not found")
            return None
        
        # Create request tracking
        request_event = threading.Event()
        self._pending_state_requests[dag_name] = {
            'event': request_event,
            'data': None,
            'error': None
        }
        
        try:
            # Import here to avoid circular imports
            from core.workers.worker_process import ControlMessageType
            
            # Send request to worker
            self._send_control(worker_id, {
                'type': ControlMessageType.GET_DAG_STATE.value,
                'dag_name': dag_name
            })
            
            # Wait for response
            if request_event.wait(timeout=timeout):
                request = self._pending_state_requests.get(dag_name)
                if request:
                    if request.get('error'):
                        logger.error(f"Error getting DAG state: {request['error']}")
                        return None
                    return request.get('data')
            else:
                logger.warning(f"Timeout waiting for DAG state from worker {worker_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error requesting DAG state for {dag_name}: {e}")
            return None
        finally:
            # Cleanup
            self._pending_state_requests.pop(dag_name, None)
    
    def get_all_dag_assignments(self) -> Dict[str, int]:
        """Get all DAG-to-worker assignments"""
        return self.affinity_manager.get_all_assignments()
    
    def get_load_summary(self) -> dict:
        """Get summary of worker loads"""
        return {
            'num_workers': self.num_workers,
            'workers': self.affinity_manager.get_load_summary(),
            'total_dags': len(self.dag_configs),
            'strategy': self.affinity_manager.strategy.value
        }
    
    def get_pool_status(self) -> dict:
        """Get comprehensive pool status"""
        healthy_workers = self.health_monitor.get_healthy_workers()
        unhealthy_workers = self.health_monitor.get_unhealthy_workers()
        
        return {
            'running': self._running,
            'num_workers': self.num_workers,
            'healthy_workers': len(healthy_workers),
            'unhealthy_workers': len(unhealthy_workers),
            'total_dags': len(self.dag_configs),
            'workers': self.get_all_workers_status(),
            'dag_assignments': self.get_all_dag_assignments(),
            'health_statuses': self.health_monitor.get_all_health_statuses()
        }
    
    def is_running(self) -> bool:
        """Check if the worker pool is running"""
        return self._running
