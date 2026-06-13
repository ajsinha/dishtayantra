"""
Worker Process Runtime Mixin (v2.0.0 module split)
==================================================

Carries two cohesive method groups for
:class:`core.workers.worker_process.DAGWorkerProcess`, extracted verbatim
so each module stays within the 500-line architecture limit. All state
lives on DAGWorkerProcess.

    Native-manager initialization - per-process JVM (Py4J), CPP (pybind11),
        Rust (PyO3) managers and the LMDB zero-copy connection.
    Status reporting - heartbeat, periodic, full, and per-DAG state
        messages sent back to the pool over the status queue.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import os
import queue
import threading
import time
import traceback
from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.workers.worker_messages import (
    ControlMessage,
    ControlMessageType,
    DAGStats,
    StatusMessage,
    StatusMessageType,
)

logger = logging.getLogger(__name__)


class WorkerRuntimeMixin:
    """Native-manager initialization + status reporting for the worker."""

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
    
    def _init_cpp_manager(self):
        """Initialize CPP Manager for C++ pybind11 calculators (v1.7.0)"""
        try:
            from core.cpp import get_cpp_manager, is_cpp_available
            
            cpp_manager = get_cpp_manager()
            
            # Check if already initialized
            if not cpp_manager._initialized:
                cpp_config_path = self.config.get('cpp_config_path', 'config/cpp_config.json')
                if os.path.exists(cpp_config_path):
                    # Initialize with config - modules will load automatically
                    cpp_manager.initialize(cpp_config_path)
            
            if is_cpp_available():
                status = cpp_manager.get_status()
                modules_loaded = status.get('modules_loaded', 0)
                self.logger.info(f"CPP Manager initialized - {modules_loaded} module(s) loaded")
            else:
                self.logger.info("CPP Manager initialized but no modules loaded")
                
        except ImportError:
            self.logger.debug("CPP Manager not available")
        except Exception as e:
            self.logger.warning(f"Could not initialize CPP Manager: {e}")
    
    def _init_rust_manager(self):
        """Initialize Rust Manager for PyO3 Rust calculators (v1.7.0)"""
        try:
            from core.rust import get_rust_manager, is_rust_available
            
            rust_manager = get_rust_manager()
            
            # Check if already initialized
            if not rust_manager._initialized:
                rust_config_path = self.config.get('rust_config_path', 'config/rust_config.json')
                if os.path.exists(rust_config_path):
                    # Initialize with config - modules will load automatically
                    rust_manager.initialize(rust_config_path)
            
            if is_rust_available():
                status = rust_manager.get_status()
                modules_loaded = status.get('modules_loaded', 0)
                self.logger.info(f"Rust Manager initialized - {modules_loaded} module(s) loaded")
            else:
                self.logger.info("Rust Manager initialized but no modules loaded")
                
        except ImportError:
            self.logger.debug("Rust Manager not available")
        except Exception as e:
            self.logger.warning(f"Could not initialize Rust Manager: {e}")
    
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
    
