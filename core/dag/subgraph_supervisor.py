"""
SubgraphSupervisor (v2.0.0 module split)
========================================

Supervisor that lights subgraphs up/down based on external control inputs,
extracted verbatim from subgraph.py. Re-exported from core.dag.subgraph,
so existing imports keep working.

Patent Pending - DishtaYantra Framework
Copyright (c) 2025-2030 Ashutosh Sinha. All Rights Reserved.
"""

from __future__ import annotations

import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, Callable
from collections import deque

logger = logging.getLogger(__name__)

from core.dag.subgraph_model import SubgraphState

class SubgraphSupervisor:
    """
    Supervisory controller for managing subgraph states.
    
    The supervisor has authority to light up or light down subgraphs,
    and can respond to control messages from external sources.
    """
    
    def __init__(self, parent_graph: Any, config: Dict[str, Any] = None):
        """
        Initialize the supervisor.
        
        Args:
            parent_graph: The parent graph containing subgraphs
            config: Supervisor configuration
        """
        self.parent_graph = parent_graph
        self.config = config or {}
        
        # Managed subgraphs (name -> SubgraphNode)
        self.managed_subgraphs: Dict[str, SubgraphNode] = {}
        
        # Control source (e.g., Kafka topic)
        self.control_source = self.config.get('control_source')
        
        # State tracking
        self._state_history: List[Dict[str, Any]] = []
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Event callbacks
        self._on_state_change: List[Callable] = []
        
        logger.info("SubgraphSupervisor initialized")
    
    def register_subgraph(self, subgraph_node: SubgraphNode):
        """Register a subgraph for supervision."""
        with self._lock:
            self.managed_subgraphs[subgraph_node.name] = subgraph_node
            logger.debug(f"Registered subgraph '{subgraph_node.name}' for supervision")
    
    def unregister_subgraph(self, name: str):
        """Unregister a subgraph from supervision."""
        with self._lock:
            if name in self.managed_subgraphs:
                del self.managed_subgraphs[name]
                logger.debug(f"Unregistered subgraph '{name}' from supervision")
    
    def light_up(self, subgraph_name: str, reason: str = None):
        """
        Activate a subgraph.
        
        Args:
            subgraph_name: Name of subgraph to activate
            reason: Optional reason for activation
        """
        with self._lock:
            subgraph_node = self.managed_subgraphs.get(subgraph_name)
            if not subgraph_node:
                logger.warning(f"Subgraph '{subgraph_name}' not found")
                return
            
            old_state = subgraph_node.subgraph.state if subgraph_node.subgraph else None
            subgraph_node.light_up(reason)
            
            self._record_state_change(subgraph_name, old_state, SubgraphState.ACTIVE, reason)
            self._emit_state_change(subgraph_name, SubgraphState.ACTIVE, reason)
    
    def light_down(self, subgraph_name: str, reason: str = None, 
                   resume_time: datetime = None):
        """
        Suspend a subgraph.
        
        Args:
            subgraph_name: Name of subgraph to suspend
            reason: Optional reason for suspension
            resume_time: Optional time to auto-resume
        """
        with self._lock:
            subgraph_node = self.managed_subgraphs.get(subgraph_name)
            if not subgraph_node:
                logger.warning(f"Subgraph '{subgraph_name}' not found")
                return
            
            old_state = subgraph_node.subgraph.state if subgraph_node.subgraph else None
            subgraph_node.light_down(reason, resume_time)
            
            self._record_state_change(subgraph_name, old_state, SubgraphState.SUSPENDED, reason)
            self._emit_state_change(subgraph_name, SubgraphState.SUSPENDED, reason)
    
    def light_up_all(self, reason: str = None):
        """Activate all managed subgraphs."""
        with self._lock:
            for name in self.managed_subgraphs:
                self.light_up(name, reason)
    
    def light_down_all(self, reason: str = None):
        """Suspend all managed subgraphs."""
        with self._lock:
            for name in self.managed_subgraphs:
                self.light_down(name, reason)
    
    def get_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all managed subgraphs."""
        with self._lock:
            status = {}
            for name, node in self.managed_subgraphs.items():
                if node.subgraph:
                    sg = node.subgraph
                    status[name] = {
                        'state': sg.state.value,
                        'is_active': sg.is_active,
                        'execution_mode': sg.execution_mode.value,
                        'node_count': sg.node_count,
                        'suspend_reason': sg.suspend_reason,
                        'resume_time': sg.resume_time.isoformat() if sg.resume_time else None,
                        'metrics': {
                            'execution_count': sg.metrics.execution_count,
                            'avg_latency_ms': sg.metrics.avg_execution_time_ms,
                            'last_latency_ms': sg.metrics.last_execution_time_ms,
                            'cache_hits': sg.metrics.cache_hit_count,
                            'errors': sg.metrics.error_count
                        }
                    }
            return status
    
    def process_control_message(self, message: Dict[str, Any]):
        """
        Process a control message.
        
        Expected format:
        {
            "command": "light_up" | "light_down" | "light_up_all" | "light_down_all",
            "target": "subgraph_name",  # For single commands
            "reason": "optional reason",
            "resume_time": "ISO datetime"  # For light_down
        }
        """
        command = message.get('command')
        target = message.get('target')
        reason = message.get('reason')
        resume_time_str = message.get('resume_time')
        
        resume_time = None
        if resume_time_str:
            resume_time = datetime.fromisoformat(resume_time_str)
        
        if command == 'light_up':
            if target:
                self.light_up(target, reason)
            else:
                logger.warning("light_up command requires 'target'")
        elif command == 'light_down':
            if target:
                self.light_down(target, reason, resume_time)
            else:
                logger.warning("light_down command requires 'target'")
        elif command == 'light_up_all':
            self.light_up_all(reason)
        elif command == 'light_down_all':
            self.light_down_all(reason)
        else:
            logger.warning(f"Unknown supervisor command: {command}")
    
    def on_state_change(self, callback: Callable):
        """Register a callback for state changes."""
        self._on_state_change.append(callback)
    
    def _record_state_change(self, subgraph_name: str, old_state: SubgraphState,
                              new_state: SubgraphState, reason: str):
        """Record state change in history."""
        self._state_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'subgraph': subgraph_name,
            'old_state': old_state.value if old_state else None,
            'new_state': new_state.value,
            'reason': reason
        })
        
        # Keep only last 100 entries
        if len(self._state_history) > 100:
            self._state_history = self._state_history[-100:]
    
    def _emit_state_change(self, subgraph_name: str, new_state: SubgraphState, 
                           reason: str):
        """Emit state change event to callbacks."""
        for callback in self._on_state_change:
            try:
                callback(subgraph_name, new_state, reason)
            except Exception as e:
                logger.error(f"State change callback error: {e}")
    
    def get_state_history(self) -> List[Dict[str, Any]]:
        """Get state change history."""
        return self._state_history.copy()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert supervisor state to dictionary."""
        return {
            'managed_subgraphs': list(self.managed_subgraphs.keys()),
            'status': self.get_status(),
            'control_source': self.control_source,
            'state_history_count': len(self._state_history)
        }


