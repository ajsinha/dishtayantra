"""
DAG Worker Process
Version: 1.6.0

Worker process that owns and executes multiple DAGs.
Runs as a separate process for true CPU parallelism.

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


class DAGWorkerProcess(Process):
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
        
        # Initialize LMDB connection for cross-worker communication if configured
        if self.config.get('use_lmdb_for_cross_worker', False):
            self._init_lmdb_connection()
    
    def _init_jvm_manager(self):
        """Initialize JVM Manager connection for Java calculators"""
        try:
            from core.jvm import get_jvm_manager, is_jvm_available
            
            jvm_manager = get_jvm_manager()
            
            # Check if already initialized (by main process or another worker)
            if not jvm_manager.is_initialized():
                # Try to connect to existing gateways (don't start new JVMs from workers)
                jvm_config_path = self.config.get('jvm_config_path', 'config/jvm_config.json')
                if os.path.exists(jvm_config_path):
                    jvm_manager.load_config(jvm_config_path)
                    # Connect only, don't auto-start JVMs from worker processes
                    # JVMs should be started by main process only
                    jvm_manager.initialize(auto_connect=True)
            
            if jvm_manager.is_gateway_available("primary"):
                self.logger.info("JVM Manager connected - Java calculators available")
            else:
                self.logger.info("JVM Manager initialized but gateway not available")
                
        except ImportError:
            self.logger.debug("JVM Manager not available (Py4J not installed)")
        except Exception as e:
            self.logger.warning(f"Could not initialize JVM Manager: {e}")
    
    def _init_lmdb_connection(self):
        """Initialize LMDB connection for cross-worker data sharing"""
        try:
            lmdb_path = self.config.get('lmdb_path', 'data/worker_lmdb')
            os.makedirs(lmdb_path, exist_ok=True)
            
            import lmdb
            map_size = self.config.get('lmdb_map_size_mb', 1024) * 1024 * 1024
            
            self.lmdb_env = lmdb.open(
                lmdb_path,
                map_size=map_size,
                max_dbs=100,
                max_readers=self.config.get('max_workers', 32) * 4
            )
            self.logger.info(f"LMDB connection initialized: {lmdb_path}")
        except Exception as e:
            self.logger.warning(f"Could not initialize LMDB: {e}")
            self.lmdb_env = None
    
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
    
    def _send_status(self, status_type: StatusMessageType, dag_name: str = None,
                    data: dict = None, error: str = None):
        """Send status message to orchestrator"""
        try:
            msg = StatusMessage(
                type=status_type.value,
                worker_id=self.worker_id,
                dag_name=dag_name,
                data=data,
                error=error
            )
            self.status_queue.put_nowait(msg.to_dict())
        except queue.Full:
            if self.logger:
                self.logger.warning("Status queue full, dropping message")
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error sending status: {e}")
    
    def _send_periodic_status(self):
        """Send periodic heartbeat with worker status"""
        current_time = time.time()
        
        if current_time - self.last_status_time < self.status_interval:
            return
        
        self.last_status_time = current_time
        
        try:
            import psutil
            process = psutil.Process()
            cpu_percent = process.cpu_percent()
            memory_mb = process.memory_info().rss / 1024 / 1024
        except:
            cpu_percent = 0
            memory_mb = 0
        
        # Collect DAG stats
        dag_stats_dict = {
            name: stats.to_dict() 
            for name, stats in self.dag_stats.items()
        }
        
        data = {
            'pid': os.getpid(),
            'uptime_seconds': current_time - self.start_time if self.start_time else 0,
            'cpu_percent': round(cpu_percent, 1),
            'memory_mb': round(memory_mb, 1),
            'dag_count': len(self.dags),
            'total_cycles': self.total_cycles,
            'dag_stats': dag_stats_dict
        }
        
        self._send_status(StatusMessageType.HEARTBEAT, data=data)
    
    def _send_full_status(self):
        """Send complete status on request"""
        try:
            import psutil
            process = psutil.Process()
            cpu_percent = process.cpu_percent()
            memory_info = process.memory_info()
        except:
            cpu_percent = 0
            memory_info = None
        
        data = {
            'pid': os.getpid(),
            'worker_id': self.worker_id,
            'uptime_seconds': time.time() - self.start_time if self.start_time else 0,
            'cpu_percent': round(cpu_percent, 1),
            'memory_mb': round(memory_info.rss / 1024 / 1024, 1) if memory_info else 0,
            'memory_vms_mb': round(memory_info.vms / 1024 / 1024, 1) if memory_info else 0,
            'dag_count': len(self.dags),
            'paused_dag_count': len(self.paused_dags),
            'total_cycles': self.total_cycles,
            'loaded_dags': list(self.dags.keys()),
            'paused_dags': list(self.paused_dags),
            'dag_stats': {name: stats.to_dict() for name, stats in self.dag_stats.items()},
            'timestamp': time.time()
        }
        
        self._send_status(StatusMessageType.HEARTBEAT, data=data)
    
    def _send_dag_state(self, dag_name: str):
        """
        Send full DAG state for UI display (v1.5.2)
        
        Collects detailed node state including input/output values,
        calculation counts, and errors for display in the state page.
        """
        try:
            if dag_name not in self.dags:
                self._send_status(StatusMessageType.DAG_STATE, dag_name=dag_name,
                                error=f"DAG {dag_name} not found in worker")
                return
            
            dag = self.dags[dag_name]
            
            # Get topological order of nodes
            try:
                sorted_nodes = dag.topological_sort()
            except Exception as e:
                self.logger.error(f"Error getting topological sort for {dag_name}: {e}")
                sorted_nodes = list(dag.nodes.values())
            
            # Collect state for each node
            node_states = []
            for node in sorted_nodes:
                # Get calculation count
                calculation_count = None
                if hasattr(node, '_compute_count'):
                    calculation_count = node._compute_count
                elif hasattr(node, 'compute_count'):
                    calculation_count = node.compute_count
                
                # Get last calculation time
                last_calculation = None
                if hasattr(node, '_last_compute'):
                    last_calculation = str(node._last_compute) if node._last_compute else None
                elif hasattr(node, 'last_compute'):
                    last_calculation = str(node.last_compute) if node.last_compute else None
                
                # Serialize input/output (handle non-serializable objects)
                try:
                    import json
                    input_data = node._input
                    if input_data is not None:
                        json.dumps(input_data)  # Test if serializable
                except (TypeError, ValueError):
                    input_data = str(node._input) if node._input else None
                
                try:
                    output_data = node._output
                    if output_data is not None:
                        json.dumps(output_data)  # Test if serializable
                except (TypeError, ValueError):
                    output_data = str(node._output) if node._output else None
                
                # Serialize errors
                errors = []
                for err in node._errors:
                    if isinstance(err, dict):
                        errors.append(err)
                    else:
                        errors.append({'error': str(err), 'time': 'Unknown'})
                
                node_states.append({
                    'name': node.name,
                    'type': type(node).__name__,
                    'input': input_data,
                    'output': output_data,
                    'isdirty': node._isdirty,
                    'errors': errors,
                    'calculation_count': calculation_count,
                    'last_calculation': last_calculation
                })
            
            # Get subscriber queue depths
            subscriber_states = {}
            for sub_name, subscriber in dag.subscribers.items():
                try:
                    queue_depth = subscriber.queue_depth() if hasattr(subscriber, 'queue_depth') else 0
                    subscriber_states[sub_name] = {
                        'name': sub_name,
                        'queue_depth': queue_depth,
                        'source': getattr(subscriber, 'source', 'N/A')
                    }
                except Exception as e:
                    subscriber_states[sub_name] = {
                        'name': sub_name,
                        'queue_depth': 0,
                        'error': str(e)
                    }
            
            # Build response data
            data = {
                'dag_name': dag_name,
                'is_running': dag._compute_thread and dag._compute_thread.is_alive() if hasattr(dag, '_compute_thread') else False,
                'is_suspended': not dag._suspend_event.is_set() if hasattr(dag, '_suspend_event') else False,
                'node_states': node_states,
                'subscriber_states': subscriber_states,
                'timestamp': time.time()
            }
            
            self._send_status(StatusMessageType.DAG_STATE, dag_name=dag_name, data=data)
            self.logger.debug(f"Sent DAG state for {dag_name}: {len(node_states)} nodes")
            
        except Exception as e:
            self.logger.error(f"Error collecting DAG state for {dag_name}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            self._send_status(StatusMessageType.DAG_STATE, dag_name=dag_name,
                            error=str(e))
    
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
