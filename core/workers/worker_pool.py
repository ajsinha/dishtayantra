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
class WorkerInfo:
    """Information about a worker process"""
    worker_id: int
    process: Optional[Process] = None
    control_queue: Optional[Queue] = None
    pid: Optional[int] = None
    start_time: Optional[datetime] = None
    
    # Latest status
    cpu_percent: float = 0.0
    memory_mb: float = 0.0
    dag_count: int = 0
    loaded_dags: List[str] = None
    
    def __post_init__(self):
        if self.loaded_dags is None:
            self.loaded_dags = []


class WorkerPoolManager:
    """
    Orchestrates a pool of worker processes for DAG execution.
    
    This is the main interface for:
    - Starting/stopping the worker pool
    - Assigning DAGs to workers
    - Monitoring worker health
    - Handling worker crashes and restarts
    """
    
    def __init__(self, config_path: str = None, config: dict = None):
        """
        Initialize the worker pool manager.
        
        Args:
            config_path: Path to worker_config.json
            config: Direct configuration dict (overrides config_path)
        """
        self.config = self._load_config(config_path, config)
        
        # Determine number of workers (v1.5.2: default is 4, configured via worker_config.json)
        pool_config = self.config.get('worker_pool', {})
        num_workers_config = pool_config.get('num_workers', 4)
        
        if num_workers_config == 'auto':
            self.num_workers = os.cpu_count() or 4
        else:
            self.num_workers = int(num_workers_config)
        
        # Apply min/max limits
        min_workers = pool_config.get('min_workers', 1)
        max_workers = pool_config.get('max_workers', 32)
        self.num_workers = max(min_workers, min(self.num_workers, max_workers))
        
        logger.info(f"WorkerPoolManager initializing with {self.num_workers} workers")
        
        # Initialize components
        self.affinity_manager = DAGAffinityManager(
            self.num_workers,
            self.config.get('affinity', {})
        )
        
        self.health_monitor = WorkerHealthMonitor(pool_config)
        
        # Worker tracking
        self.workers: Dict[int, WorkerInfo] = {}
        self.status_queue = Queue(maxsize=pool_config.get('status_queue_maxsize', 5000))
        self.shutdown_event = Event()
        
        # DAG config storage (for restarts)
        self.dag_configs: Dict[str, dict] = {}
        
        # Status processing thread
        self._status_thread: Optional[threading.Thread] = None
        self._running = False
        
        # Register health monitor callbacks
        self.health_monitor.on_restart(self._handle_worker_restart)
        self.health_monitor.on_unhealthy(self._handle_worker_unhealthy)
        
        self._lock = threading.RLock()
        
        # v1.5.2: Pending DAG state requests (for UI state page)
        self._pending_state_requests: Dict[str, dict] = {}  # dag_name -> {'event': Event, 'data': None, 'error': None}
    
    def _load_config(self, config_path: str, config: dict) -> dict:
        """
        Load configuration from file or use provided dict.
        
        Configuration is loaded exclusively from worker_config.json (v1.5.2).
        """
        if config:
            return config
        
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
        
        # Try default path
        if os.path.exists('config/worker_config.json'):
            with open('config/worker_config.json', 'r') as f:
                return json.load(f)
        
        # Return defaults
        logger.warning("worker_config.json not found, using defaults")
        return {
            'worker_pool': {
                'enabled': True,
                'num_workers': 4,
                'auto_restart_on_crash': True,
                'min_workers': 2,
                'max_workers': 32,
                'health_check_interval_seconds': 5,
                'max_restart_attempts': 3
            },
            'affinity': {
                'default_strategy': 'weight_based',
                'allow_dag_pinning': True,
                'allow_exclusive_workers': True
            },
            'communication': {
                'use_lmdb_for_cross_worker': True,
                'lmdb_path': 'data/worker_lmdb'
            }
        }
    
    def start(self):
        """Start the worker pool"""
        if self._running:
            logger.warning("Worker pool already running")
            return
        
        logger.info(f"Starting worker pool with {self.num_workers} workers")
        
        self._running = True
        self.shutdown_event.clear()
        
        # Start status processing thread
        self._status_thread = threading.Thread(
            target=self._process_status_messages,
            name="WorkerPoolStatusProcessor",
            daemon=True
        )
        self._status_thread.start()
        
        # Start health monitoring
        self.health_monitor.start_monitoring()
        
        # Spawn workers
        for worker_id in range(self.num_workers):
            self._spawn_worker(worker_id)
        
        # Wait for workers to be ready
        self._wait_for_workers_ready()
        
        logger.info("Worker pool started successfully")
    
    def stop(self, timeout: float = 10.0):
        """Stop the worker pool gracefully"""
        if not self._running:
            return
        
        logger.info("Stopping worker pool...")
        
        self._running = False
        
        # Signal shutdown to all workers
        for worker_id, worker_info in self.workers.items():
            self._send_control(worker_id, {'type': ControlMessageType.SHUTDOWN.value})
        
        # Set shutdown event
        self.shutdown_event.set()
        
        # Wait for workers to stop
        stop_deadline = time.time() + timeout
        for worker_id, worker_info in list(self.workers.items()):
            if worker_info.process and worker_info.process.is_alive():
                remaining = max(0, stop_deadline - time.time())
                worker_info.process.join(timeout=remaining)
                
                if worker_info.process.is_alive():
                    logger.warning(f"Force terminating worker {worker_id}")
                    worker_info.process.terminate()
                    worker_info.process.join(timeout=1)
        
        # Stop health monitoring
        self.health_monitor.stop_monitoring()
        
        # Stop status thread
        if self._status_thread:
            self._status_thread.join(timeout=2)
        
        # Clear state
        self.workers.clear()
        
        logger.info("Worker pool stopped")
    
    def _spawn_worker(self, worker_id: int):
        """Spawn a new worker process"""
        logger.info(f"Spawning worker {worker_id}")
        
        # Create control queue for this worker
        control_queue = Queue(
            maxsize=self.config.get('communication', {}).get('control_queue_maxsize', 1000)
        )
        
        # Create worker process
        worker_config = {
            **self.config.get('communication', {}),
            **self.config.get('worker_pool', {}),
            'worker_id': worker_id
        }
        
        process = DAGWorkerProcess(
            worker_id=worker_id,
            control_queue=control_queue,
            status_queue=self.status_queue,
            shutdown_event=self.shutdown_event,
            config=worker_config
        )
        
        # Store worker info
        self.workers[worker_id] = WorkerInfo(
            worker_id=worker_id,
            process=process,
            control_queue=control_queue,
            start_time=datetime.now()
        )
        
        # Register with health monitor
        self.health_monitor.register_worker(worker_id)
        
        # Start the process
        process.start()
        
        logger.info(f"Worker {worker_id} spawned (PID: {process.pid})")
    
    def _wait_for_workers_ready(self, timeout: float = 30.0):
        """Wait for all workers to report ready"""
        logger.info("Waiting for workers to be ready...")
        
        deadline = time.time() + timeout
        ready_workers = set()
        
        while time.time() < deadline and len(ready_workers) < self.num_workers:
            # Check which workers have reported ready
            for worker_id in range(self.num_workers):
                health = self.health_monitor.get_health_status(worker_id)
                if health and health.state == WorkerState.RUNNING:
                    ready_workers.add(worker_id)
            
            if len(ready_workers) >= self.num_workers:
                break
            
            time.sleep(0.1)
        
        if len(ready_workers) < self.num_workers:
            not_ready = set(range(self.num_workers)) - ready_workers
            logger.warning(f"Workers not ready after {timeout}s: {not_ready}")
        else:
            logger.info(f"All {self.num_workers} workers ready")
    
    def _send_control(self, worker_id: int, message: dict):
        """Send control message to a worker"""
        if worker_id not in self.workers:
            logger.warning(f"Cannot send to non-existent worker {worker_id}")
            return False
        
        worker_info = self.workers[worker_id]
        if not worker_info.control_queue:
            logger.warning(f"Worker {worker_id} has no control queue")
            return False
        
        try:
            worker_info.control_queue.put_nowait(message)
            return True
        except queue.Full:
            logger.error(f"Control queue full for worker {worker_id}")
            return False
    
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
    
    def load_dag(self, dag_config: dict) -> int:
        """
        Load a DAG onto a worker.
        
        Args:
            dag_config: DAG configuration dictionary
            
        Returns:
            worker_id: The worker the DAG was assigned to
        """
        dag_name = dag_config.get('name')
        if not dag_name:
            raise ValueError("DAG config must have 'name' field")
        
        with self._lock:
            # Store config for potential restart
            self.dag_configs[dag_name] = dag_config
            
            # Assign to worker
            worker_id = self.affinity_manager.assign_dag(dag_config)
            
            # Send load command
            success = self._send_control(worker_id, {
                'type': ControlMessageType.LOAD_DAG.value,
                'dag_name': dag_name,
                'dag_config': dag_config
            })
            
            if not success:
                raise RuntimeError(f"Failed to send load command to worker {worker_id}")
            
            logger.info(f"DAG '{dag_name}' assigned to worker {worker_id}")
            return worker_id
    
    def unload_dag(self, dag_name: str):
        """
        Unload a DAG from its worker.
        
        Args:
            dag_name: Name of the DAG to unload
        """
        with self._lock:
            worker_id = self.affinity_manager.get_worker_assignment(dag_name)
            
            if worker_id is None:
                logger.warning(f"DAG '{dag_name}' not found in any worker")
                return
            
            # Send unload command
            self._send_control(worker_id, {
                'type': ControlMessageType.UNLOAD_DAG.value,
                'dag_name': dag_name
            })
            
            # Update affinity manager
            self.affinity_manager.unassign_dag(dag_name)
            
            # Remove stored config
            self.dag_configs.pop(dag_name, None)
            
            logger.info(f"DAG '{dag_name}' unloaded from worker {worker_id}")
    
    def reload_dag(self, dag_name: str, dag_config: dict = None):
        """
        Reload a DAG (optionally with new configuration).
        
        Args:
            dag_name: Name of the DAG to reload
            dag_config: New configuration (uses existing if None)
        """
        with self._lock:
            if dag_config is None:
                dag_config = self.dag_configs.get(dag_name)
                if not dag_config:
                    raise ValueError(f"No config found for DAG '{dag_name}'")
            
            worker_id = self.affinity_manager.get_worker_assignment(dag_name)
            
            if worker_id is None:
                # DAG not loaded, just load it
                self.load_dag(dag_config)
                return
            
            # Store new config
            self.dag_configs[dag_name] = dag_config
            
            # Send reload command
            self._send_control(worker_id, {
                'type': ControlMessageType.RELOAD_DAG.value,
                'dag_name': dag_name,
                'dag_config': dag_config
            })
    
    def pause_dag(self, dag_name: str):
        """Pause a DAG"""
        worker_id = self.affinity_manager.get_worker_assignment(dag_name)
        if worker_id is not None:
            self._send_control(worker_id, {
                'type': ControlMessageType.PAUSE_DAG.value,
                'dag_name': dag_name
            })
    
    def resume_dag(self, dag_name: str):
        """Resume a paused DAG"""
        worker_id = self.affinity_manager.get_worker_assignment(dag_name)
        if worker_id is not None:
            self._send_control(worker_id, {
                'type': ControlMessageType.RESUME_DAG.value,
                'dag_name': dag_name
            })
    
    def migrate_dag(self, dag_name: str, to_worker: int) -> bool:
        """
        Migrate a DAG to a different worker.
        
        Args:
            dag_name: Name of DAG to migrate
            to_worker: Target worker ID
            
        Returns:
            True if migration was successful
        """
        with self._lock:
            from_worker = self.affinity_manager.get_worker_assignment(dag_name)
            
            if from_worker is None:
                logger.error(f"DAG '{dag_name}' not found")
                return False
            
            if from_worker == to_worker:
                logger.warning(f"DAG '{dag_name}' already on worker {to_worker}")
                return True
            
            dag_config = self.dag_configs.get(dag_name)
            if not dag_config:
                logger.error(f"No config found for DAG '{dag_name}'")
                return False
            
            logger.info(f"Migrating DAG '{dag_name}' from worker {from_worker} "
                       f"to worker {to_worker}")
            
            # Unload from current worker
            self._send_control(from_worker, {
                'type': ControlMessageType.UNLOAD_DAG.value,
                'dag_name': dag_name
            })
            
            # Brief wait for unload
            time.sleep(0.5)
            
            # Update affinity
            self.affinity_manager.execute_migration(dag_name, to_worker)
            
            # Load on new worker
            self._send_control(to_worker, {
                'type': ControlMessageType.LOAD_DAG.value,
                'dag_name': dag_name,
                'dag_config': dag_config
            })
            
            return True
    
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
