"""
Worker Health Monitor
Version: 1.5.2

Monitors worker processes for:
- Heartbeat timeouts
- Process crashes
- Resource exhaustion
- Automatic restart on failure

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

import logging
import threading
import time
from typing import Dict, Optional, Callable, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class WorkerState(Enum):
    """Worker process states"""
    STARTING = "starting"
    RUNNING = "running"
    UNHEALTHY = "unhealthy"
    STOPPING = "stopping"
    STOPPED = "stopped"
    CRASHED = "crashed"
    RESTARTING = "restarting"


@dataclass
class WorkerHealthStatus:
    """Health status for a worker"""
    worker_id: int
    state: WorkerState = WorkerState.STOPPED
    pid: Optional[int] = None
    
    # Heartbeat tracking
    last_heartbeat: Optional[datetime] = None
    missed_heartbeats: int = 0
    
    # Resource metrics
    cpu_percent: float = 0.0
    memory_mb: float = 0.0
    
    # DAG info
    dag_count: int = 0
    loaded_dags: List[str] = field(default_factory=list)
    
    # Restart tracking
    restart_count: int = 0
    last_restart: Optional[datetime] = None
    consecutive_failures: int = 0
    
    # Timestamps
    start_time: Optional[datetime] = None
    uptime_seconds: float = 0.0


class WorkerHealthMonitor:
    """
    Monitors health of worker processes and handles failures.
    
    Features:
    - Heartbeat monitoring
    - Automatic crash detection
    - Restart management with backoff
    - Resource monitoring
    """
    
    def __init__(self, config: dict = None):
        self.config = config or {}
        
        # Configuration
        self.check_interval = self.config.get('health_check_interval_seconds', 5)
        self.heartbeat_timeout = self.check_interval * 3  # Miss 3 heartbeats = unhealthy
        self.auto_restart = self.config.get('auto_restart_on_crash', True)
        self.max_restarts = self.config.get('max_restart_attempts', 3)
        self.restart_backoff = self.config.get('restart_backoff_seconds', 5)
        
        # State
        self.health_statuses: Dict[int, WorkerHealthStatus] = {}
        self.restart_callbacks: List[Callable] = []
        self.unhealthy_callbacks: List[Callable] = []
        
        # Monitoring thread
        self._monitor_thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.RLock()
        
        logger.info(f"WorkerHealthMonitor initialized (check_interval={self.check_interval}s, "
                   f"auto_restart={self.auto_restart})")
    
    def register_worker(self, worker_id: int, pid: int = None):
        """Register a new worker for monitoring"""
        with self._lock:
            self.health_statuses[worker_id] = WorkerHealthStatus(
                worker_id=worker_id,
                state=WorkerState.STARTING,
                pid=pid,
                start_time=datetime.now()
            )
            logger.info(f"Registered worker {worker_id} for health monitoring (PID: {pid})")
    
    def unregister_worker(self, worker_id: int):
        """Remove a worker from monitoring"""
        with self._lock:
            if worker_id in self.health_statuses:
                del self.health_statuses[worker_id]
                logger.info(f"Unregistered worker {worker_id} from health monitoring")
    
    def update_heartbeat(self, worker_id: int, status_data: dict):
        """
        Update worker status from heartbeat message.
        
        Args:
            worker_id: Worker ID
            status_data: Data from heartbeat message
        """
        with self._lock:
            if worker_id not in self.health_statuses:
                self.register_worker(worker_id)
            
            health = self.health_statuses[worker_id]
            
            # Update timestamps
            health.last_heartbeat = datetime.now()
            health.missed_heartbeats = 0
            
            # Update state
            if health.state in [WorkerState.STARTING, WorkerState.UNHEALTHY, WorkerState.RESTARTING]:
                health.state = WorkerState.RUNNING
                logger.info(f"Worker {worker_id} now healthy")
            
            # Update metrics from status data
            health.pid = status_data.get('pid', health.pid)
            health.cpu_percent = status_data.get('cpu_percent', 0)
            health.memory_mb = status_data.get('memory_mb', 0)
            health.uptime_seconds = status_data.get('uptime_seconds', 0)
            health.dag_count = status_data.get('dag_count', 0)
            health.loaded_dags = status_data.get('loaded_dags', [])
            
            # Reset consecutive failures on successful heartbeat
            health.consecutive_failures = 0
    
    def mark_worker_ready(self, worker_id: int, pid: int):
        """Mark worker as ready after successful startup"""
        with self._lock:
            if worker_id not in self.health_statuses:
                self.register_worker(worker_id, pid)
            
            health = self.health_statuses[worker_id]
            health.state = WorkerState.RUNNING
            health.pid = pid
            health.last_heartbeat = datetime.now()
            health.start_time = datetime.now()
            
            logger.info(f"Worker {worker_id} marked ready (PID: {pid})")
    
    def mark_worker_stopping(self, worker_id: int):
        """Mark worker as stopping (graceful shutdown)"""
        with self._lock:
            if worker_id in self.health_statuses:
                self.health_statuses[worker_id].state = WorkerState.STOPPING
    
    def mark_worker_stopped(self, worker_id: int):
        """Mark worker as stopped"""
        with self._lock:
            if worker_id in self.health_statuses:
                self.health_statuses[worker_id].state = WorkerState.STOPPED
                self.health_statuses[worker_id].pid = None
    
    def mark_worker_crashed(self, worker_id: int):
        """Mark worker as crashed"""
        with self._lock:
            if worker_id in self.health_statuses:
                health = self.health_statuses[worker_id]
                health.state = WorkerState.CRASHED
                health.consecutive_failures += 1
                health.pid = None
                
                logger.error(f"Worker {worker_id} marked as crashed "
                           f"(consecutive_failures={health.consecutive_failures})")
    
    def mark_worker_restarting(self, worker_id: int):
        """Mark worker as restarting"""
        with self._lock:
            if worker_id in self.health_statuses:
                health = self.health_statuses[worker_id]
                health.state = WorkerState.RESTARTING
                health.restart_count += 1
                health.last_restart = datetime.now()
    
    def on_restart(self, callback: Callable):
        """Register callback for worker restart events"""
        self.restart_callbacks.append(callback)
    
    def on_unhealthy(self, callback: Callable):
        """Register callback for worker unhealthy events"""
        self.unhealthy_callbacks.append(callback)
    
    def start_monitoring(self):
        """Start the health monitoring thread"""
        if self._running:
            return
        
        self._running = True
        self._monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            name="WorkerHealthMonitor",
            daemon=True
        )
        self._monitor_thread.start()
        logger.info("Worker health monitoring started")
    
    def stop_monitoring(self):
        """Stop the health monitoring thread"""
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
            self._monitor_thread = None
        logger.info("Worker health monitoring stopped")
    
    def _monitoring_loop(self):
        """Main monitoring loop"""
        while self._running:
            try:
                self._check_workers()
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in health monitoring loop: {e}")
                time.sleep(1)
    
    def _check_workers(self):
        """Check health of all workers"""
        with self._lock:
            current_time = datetime.now()
            
            for worker_id, health in list(self.health_statuses.items()):
                # Skip workers that are stopping or stopped
                if health.state in [WorkerState.STOPPING, WorkerState.STOPPED]:
                    continue
                
                # Check for missed heartbeats
                if health.last_heartbeat:
                    time_since_heartbeat = (current_time - health.last_heartbeat).total_seconds()
                    
                    if time_since_heartbeat > self.heartbeat_timeout:
                        health.missed_heartbeats += 1
                        
                        if health.state == WorkerState.RUNNING:
                            health.state = WorkerState.UNHEALTHY
                            logger.warning(f"Worker {worker_id} unhealthy - "
                                         f"no heartbeat for {time_since_heartbeat:.1f}s")
                            
                            # Trigger unhealthy callbacks
                            for callback in self.unhealthy_callbacks:
                                try:
                                    callback(worker_id, health)
                                except Exception as e:
                                    logger.error(f"Error in unhealthy callback: {e}")
                
                # Check if process is actually alive
                if health.pid and health.state not in [WorkerState.STOPPED, WorkerState.STOPPING]:
                    if not self._is_process_alive(health.pid):
                        logger.error(f"Worker {worker_id} process {health.pid} not alive")
                        self.mark_worker_crashed(worker_id)
                        
                        # Trigger restart if enabled
                        if self.auto_restart:
                            self._request_restart(worker_id, health)
    
    def _is_process_alive(self, pid: int) -> bool:
        """Check if a process is alive"""
        try:
            import psutil
            return psutil.pid_exists(pid)
        except:
            # Fallback: try sending signal 0
            import os
            try:
                os.kill(pid, 0)
                return True
            except OSError:
                return False
    
    def _request_restart(self, worker_id: int, health: WorkerHealthStatus):
        """Request worker restart"""
        if health.consecutive_failures >= self.max_restarts:
            logger.error(f"Worker {worker_id} exceeded max restart attempts "
                        f"({self.max_restarts}), not restarting")
            return
        
        # Calculate backoff
        backoff = self.restart_backoff * (2 ** (health.consecutive_failures - 1))
        backoff = min(backoff, 60)  # Cap at 60 seconds
        
        logger.info(f"Requesting restart for worker {worker_id} "
                   f"(attempt {health.consecutive_failures + 1}/{self.max_restarts}, "
                   f"backoff={backoff}s)")
        
        self.mark_worker_restarting(worker_id)
        
        # Trigger restart callbacks after backoff
        def delayed_restart():
            time.sleep(backoff)
            for callback in self.restart_callbacks:
                try:
                    callback(worker_id, health)
                except Exception as e:
                    logger.error(f"Error in restart callback: {e}")
        
        threading.Thread(target=delayed_restart, daemon=True).start()
    
    def get_health_status(self, worker_id: int) -> Optional[WorkerHealthStatus]:
        """Get health status for a worker"""
        return self.health_statuses.get(worker_id)
    
    def get_all_health_statuses(self) -> Dict[int, dict]:
        """Get health status for all workers"""
        with self._lock:
            return {
                worker_id: {
                    'worker_id': health.worker_id,
                    'state': health.state.value,
                    'pid': health.pid,
                    'last_heartbeat': health.last_heartbeat.isoformat() if health.last_heartbeat else None,
                    'missed_heartbeats': health.missed_heartbeats,
                    'cpu_percent': health.cpu_percent,
                    'memory_mb': health.memory_mb,
                    'dag_count': health.dag_count,
                    'loaded_dags': health.loaded_dags,
                    'restart_count': health.restart_count,
                    'consecutive_failures': health.consecutive_failures,
                    'uptime_seconds': health.uptime_seconds,
                    'start_time': health.start_time.isoformat() if health.start_time else None
                }
                for worker_id, health in self.health_statuses.items()
            }
    
    def get_healthy_workers(self) -> List[int]:
        """Get list of healthy worker IDs"""
        with self._lock:
            return [
                worker_id for worker_id, health in self.health_statuses.items()
                if health.state == WorkerState.RUNNING
            ]
    
    def get_unhealthy_workers(self) -> List[int]:
        """Get list of unhealthy/crashed worker IDs"""
        with self._lock:
            return [
                worker_id for worker_id, health in self.health_statuses.items()
                if health.state in [WorkerState.UNHEALTHY, WorkerState.CRASHED]
            ]
    
    def is_worker_healthy(self, worker_id: int) -> bool:
        """Check if a specific worker is healthy"""
        health = self.health_statuses.get(worker_id)
        return health is not None and health.state == WorkerState.RUNNING
