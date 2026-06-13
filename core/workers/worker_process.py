"""
DAG Worker Process
Version: 1.7.0

Worker process that owns and executes multiple DAGs.
Runs as a separate process for true CPU parallelism.

v1.7.0: Added CPP Manager integration for C++ calculator support.
v1.6.0: Added JVM Manager integration for Java calculator support.

Copyright © 2025 Ashutosh Sinha. All rights reserved.
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

# v2.0.0 module split: IPC message types + worker logging live in
# worker_messages.py; the native-manager/status mixin in
# worker_process_runtime.py. Re-exported so existing imports keep working.
from core.workers.worker_messages import (  # noqa: F401
    ControlMessage,
    ControlMessageType,
    DAGStats,
    StatusMessage,
    StatusMessageType,
    setup_worker_logging,
)
from core.workers.worker_process_runtime import WorkerRuntimeMixin

logger = logging.getLogger(__name__)


class DAGWorkerProcess(WorkerRuntimeMixin, Process):
    """
    Worker process that owns and runs multiple DAGs.
    
    Each worker runs as a separate OS process for true parallelism.
    DAGs assigned to a worker share:
    - Calculator registry (loaded once)
    - LMDB connections
    - Local caches
    
    Communication with orchestrator via multiprocessing Queues.
    """
    
    def __init__(self, worker_id: int, control_queue: Queue, status_queue: Queue,
                 shutdown_event: Event, config: dict = None):
        super().__init__(name=f"DAGWorker-{worker_id}")
        
        self.worker_id = worker_id
        self.control_queue = control_queue
        self.status_queue = status_queue
        self.shutdown_event = shutdown_event
        self.config = config or {}
        
        # Will be initialized in run()
        self.logger = None
        self.dags: Dict[str, Any] = {}  # dag_name -> DAG instance
        self.dag_stats: Dict[str, DAGStats] = {}
        self.dag_configs: Dict[str, dict] = {}
        self.paused_dags: set = set()
        
        self.calculator_registry = None
        self.running = True
        
        self.status_interval = self.config.get('status_report_interval_seconds', 2)
        self.last_status_time = 0
        
        # Performance tracking
        self.start_time = None
        self.total_cycles = 0
    
    def run(self):
        """Main worker process entry point"""
        try:
            # Setup logging for this process
            self.logger = setup_worker_logging(self.worker_id)
            self.logger.info(f"Worker {self.worker_id} starting (PID: {os.getpid()})")
            
            # Setup signal handlers
            signal.signal(signal.SIGTERM, self._handle_signal)
            signal.signal(signal.SIGINT, self._handle_signal)
            
            # Initialize components
            self._initialize()
            
            # Signal ready
            self._send_status(StatusMessageType.WORKER_READY, data={
                'pid': os.getpid(),
                'start_time': time.time()
            })
            
            self.start_time = time.time()
            
            # Main loop
            self._main_loop()
            
        except Exception as e:
            self.logger.error(f"Worker {self.worker_id} fatal error: {e}")
            self.logger.error(traceback.format_exc())
            self._send_status(StatusMessageType.WORKER_STOPPING, error=str(e))
        finally:
            self._cleanup()
    
    def _initialize(self):
        """Initialize worker components"""
        self.logger.info("Initializing calculator registry...")
        
        try:
            # Import here to avoid issues with multiprocessing
            from core.calculator.calculator_factory import CalculatorFactory
            self.calculator_registry = CalculatorFactory()
            self.logger.info("Calculator registry initialized")
        except Exception as e:
            self.logger.warning(f"Could not initialize calculator registry: {e}")
            self.calculator_registry = None
        
        # v1.6.0: Initialize JVM Manager for Java calculator support
        self._init_jvm_manager()
        
        # v1.7.0: Initialize CPP Manager for C++ calculator support
        self._init_cpp_manager()
        
        # v1.7.0: Initialize Rust Manager for Rust calculator support
        self._init_rust_manager()
        
        # Initialize LMDB connection for cross-worker communication if configured
        if self.config.get('use_lmdb_for_cross_worker', False):
            self._init_lmdb_connection()
    
    def _main_loop(self):
        """Main worker loop"""
        self.logger.info(f"Worker {self.worker_id} entering main loop")
        
        while self.running and not self.shutdown_event.is_set():
            try:
                # Process control messages
                self._process_control_messages()
                
                # Run DAG execution cycles
                self._run_dag_cycles()
                
                # Send periodic status
                self._send_periodic_status()
                
                # Small sleep to prevent CPU spinning
                time.sleep(0.001)
                
            except Exception as e:
                self.logger.error(f"Error in main loop: {e}")
                self.logger.error(traceback.format_exc())
                time.sleep(0.1)
        
        self.logger.info(f"Worker {self.worker_id} exiting main loop")
    
    def _process_control_messages(self):
        """Process control messages from orchestrator"""
        try:
            while True:
                try:
                    msg_dict = self.control_queue.get_nowait()
                    msg = ControlMessage(**msg_dict) if isinstance(msg_dict, dict) else msg_dict
                    
                    self._handle_control_message(msg)
                    
                except queue.Empty:
                    break
                    
        except Exception as e:
            self.logger.error(f"Error processing control messages: {e}")
    
    def _handle_control_message(self, msg: ControlMessage):
        """Handle a single control message"""
        msg_type = msg.type if isinstance(msg.type, str) else msg.type.value
        
        self.logger.debug(f"Received control message: {msg_type}")
        
        if msg_type == ControlMessageType.LOAD_DAG.value:
            self._load_dag(msg.dag_name, msg.dag_config)
        
        elif msg_type == ControlMessageType.UNLOAD_DAG.value:
            self._unload_dag(msg.dag_name)
        
        elif msg_type == ControlMessageType.RELOAD_DAG.value:
            self._reload_dag(msg.dag_name, msg.dag_config)
        
        elif msg_type == ControlMessageType.PAUSE_DAG.value:
            self._pause_dag(msg.dag_name)
        
        elif msg_type == ControlMessageType.RESUME_DAG.value:
            self._resume_dag(msg.dag_name)
        
        elif msg_type == ControlMessageType.SHUTDOWN.value:
            self.logger.info("Received shutdown command")
            self.running = False
        
        elif msg_type == ControlMessageType.PING.value:
            self._send_status(StatusMessageType.PONG)
        
        elif msg_type == ControlMessageType.GET_STATUS.value:
            self._send_full_status()
        
        elif msg_type == ControlMessageType.GET_DAG_STATE.value:
            self._send_dag_state(msg.dag_name)
    
    def _load_dag(self, dag_name: str, dag_config: dict):
        """Load and start a DAG"""
        try:
            self.logger.info(f"Loading DAG: {dag_name}")
            
            if dag_name in self.dags:
                self.logger.warning(f"DAG {dag_name} already loaded, unloading first")
                self._unload_dag(dag_name)
            
            # Import ComputeGraph class
            from core.dag.compute_graph import ComputeGraph
            
            # Create DAG instance
            dag = ComputeGraph(dag_config)
            dag.start()
            
            self.dags[dag_name] = dag
            self.dag_configs[dag_name] = dag_config
            self.dag_stats[dag_name] = DAGStats(name=dag_name)
            
            self._send_status(StatusMessageType.DAG_LOADED, dag_name=dag_name, data={
                'node_count': len(dag_config.get('nodes', [])),
                'calculator_count': len(dag_config.get('calculators', []))
            })
            
            self.logger.info(f"DAG {dag_name} loaded successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to load DAG {dag_name}: {e}")
            self.logger.error(traceback.format_exc())
            self._send_status(StatusMessageType.DAG_ERROR, dag_name=dag_name, 
                            error=str(e))
    
    def _unload_dag(self, dag_name: str):
        """Stop and remove a DAG"""
        try:
            if dag_name not in self.dags:
                self.logger.warning(f"DAG {dag_name} not found for unload")
                return
            
            self.logger.info(f"Unloading DAG: {dag_name}")
            
            dag = self.dags[dag_name]
            
            # Stop DAG
            try:
                dag.stop()
            except Exception as e:
                self.logger.error(f"Error stopping DAG {dag_name}: {e}")
            
            # Cleanup
            try:
                if hasattr(dag, 'cleanup'):
                    dag.cleanup()
            except Exception as e:
                self.logger.error(f"Error cleaning up DAG {dag_name}: {e}")
            
            # Remove from tracking
            del self.dags[dag_name]
            self.dag_configs.pop(dag_name, None)
            self.dag_stats.pop(dag_name, None)
            self.paused_dags.discard(dag_name)
            
            self._send_status(StatusMessageType.DAG_UNLOADED, dag_name=dag_name)
            
            self.logger.info(f"DAG {dag_name} unloaded")
            
        except Exception as e:
            self.logger.error(f"Error unloading DAG {dag_name}: {e}")
            self._send_status(StatusMessageType.DAG_ERROR, dag_name=dag_name,
                            error=str(e))
    
    def _reload_dag(self, dag_name: str, dag_config: dict):
        """Reload a DAG with new configuration"""
        self.logger.info(f"Reloading DAG: {dag_name}")
        self._unload_dag(dag_name)
        self._load_dag(dag_name, dag_config)
    
    def _pause_dag(self, dag_name: str):
        """Pause a DAG (stop processing but keep loaded)"""
        if dag_name in self.dags:
            self.paused_dags.add(dag_name)
            self.dag_stats[dag_name].status = "paused"
            self.logger.info(f"DAG {dag_name} paused")
    
    def _resume_dag(self, dag_name: str):
        """Resume a paused DAG"""
        if dag_name in self.dags:
            self.paused_dags.discard(dag_name)
            self.dag_stats[dag_name].status = "running"
            self.logger.info(f"DAG {dag_name} resumed")
    
    def _run_dag_cycles(self):
        """Run execution cycles for all DAGs"""
        for dag_name, dag in list(self.dags.items()):
            if dag_name in self.paused_dags:
                continue
            
            try:
                start = time.time()
                
                # Run one cycle of DAG execution
                if hasattr(dag, 'run_cycle'):
                    dag.run_cycle()
                elif hasattr(dag, 'execute'):
                    dag.execute()
                
                cycle_time = (time.time() - start) * 1000  # ms
                
                # Update stats
                stats = self.dag_stats.get(dag_name)
                if stats:
                    stats.nodes_executed += 1
                    stats.last_execution = time.time()
                    # Exponential moving average for cycle time
                    stats.avg_cycle_time_ms = (stats.avg_cycle_time_ms * 0.9 + 
                                               cycle_time * 0.1)
                
                self.total_cycles += 1
                
            except Exception as e:
                self.logger.error(f"Error in DAG {dag_name} cycle: {e}")
                if dag_name in self.dag_stats:
                    self.dag_stats[dag_name].errors += 1
                    self.dag_stats[dag_name].status = "error"
    
    def _handle_signal(self, signum, frame):
        """Handle termination signals"""
        if self.logger:
            self.logger.info(f"Received signal {signum}, shutting down")
        self.running = False
    
    def _cleanup(self):
        """Cleanup before exit"""
        if self.logger:
            self.logger.info(f"Worker {self.worker_id} cleaning up")
        
        # Stop all DAGs
        for dag_name in list(self.dags.keys()):
            try:
                self._unload_dag(dag_name)
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Error unloading DAG {dag_name} during cleanup: {e}")
        
        # Close LMDB
        if hasattr(self, 'lmdb_env') and self.lmdb_env:
            try:
                self.lmdb_env.close()
            except:
                pass
        
        # Send final status
        self._send_status(StatusMessageType.WORKER_STOPPING)
        
        if self.logger:
            self.logger.info(f"Worker {self.worker_id} cleanup complete")


def worker_process_entry(worker_id: int, control_queue: Queue, status_queue: Queue,
                        shutdown_event: Event, config: dict = None):
    """
    Entry point for spawning worker process.
    
    This function is used when using multiprocessing.Process with target=
    instead of subclassing Process.
    """
    worker = DAGWorkerProcess(
        worker_id=worker_id,
        control_queue=control_queue,
        status_queue=status_queue,
        shutdown_event=shutdown_event,
        config=config
    )
    worker.run()
