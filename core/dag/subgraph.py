# core/dag/subgraph.py

"""
Subgraph Implementation for DishtaYantra DAG Framework.

A subgraph is a graph-as-a-node pattern enabling hierarchical composition
of processing pipelines. Key characteristics:
- Cannot subscribe to external data publishers
- Connects to exactly one parent node (entry)
- Can output to one or more parent nodes (exit)
- Borrows calculators/transformers from parent graph
- Can be "lit up" or "lit down" by supervisor
- Supports synchronous or asynchronous execution

Patent Pending - DishtaYantra Framework
Copyright © 2025-2030 Ashutosh Sinha. All Rights Reserved.
"""

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



# v2.0.0 module split: model types, the node wrapper, and the supervisor
# now live in sibling modules. Everything is re-exported here so existing
# `from core.dag.subgraph import ...` statements keep working unchanged.
from core.dag.subgraph_model import (  # noqa: F401
    SubgraphStructureMixin,
    DarkOutputMode,
    ErrorPolicy,
    ExecutionMode,
    SubgraphConfigError,
    SubgraphError,
    SubgraphExecutionError,
    SubgraphMetrics,
    SubgraphNodeReference,
    SubgraphEdge,
    SubgraphState,
)

class Subgraph(SubgraphStructureMixin):
    """
    A self-contained computation graph that acts as a single node in a parent graph.
    
    Subgraphs enable modular, reusable computation units with:
    - Internal topological ordering
    - Dirty propagation within boundaries
    - Light up/down capability
    - Sync or async execution
    """
    
    MAX_NESTING_DEPTH = 3
    
    def __init__(self, name: str, config: Dict[str, Any], parent_graph: Any = None,
                 depth: int = 0):
        """
        Initialize a subgraph.
        
        Args:
            name: Unique name for this subgraph
            config: Configuration dictionary
            parent_graph: Reference to parent graph for borrowing nodes
            depth: Nesting depth (max 3)
        """
        self.name = name
        self.config = config
        self.parent_graph = parent_graph
        self.depth = depth
        
        if depth > self.MAX_NESTING_DEPTH:
            raise SubgraphConfigError(
                f"Subgraph nesting depth {depth} exceeds maximum {self.MAX_NESTING_DEPTH}"
            )
        
        # Core properties
        self.description = config.get('description', '')
        self.version = config.get('version', '1.0.0')
        
        # Entry/exit configuration
        self.entry_node_name: str = config.get('entry_node', '')
        self.exit_node_name: str = config.get('exit_node', '')
        
        # Execution configuration
        self.execution_mode = ExecutionMode(
            config.get('execution_mode', 'synchronous')
        )
        self.dark_output_mode = DarkOutputMode(
            config.get('dark_output_mode', 'cached')
        )
        self.error_policy = ErrorPolicy(
            config.get('error_policy', 'propagate')
        )
        
        # Nodes and edges
        self.nodes: Dict[str, SubgraphNodeReference] = {}
        self.edges: List[SubgraphEdge] = []
        
        # Computed graph structure
        self._adjacency: Dict[str, List[str]] = {}  # Forward edges
        self._reverse_adjacency: Dict[str, List[str]] = {}  # Backward edges
        self._topological_order: List[str] = []
        
        # State
        self.state = SubgraphState.CREATED
        self.cached_output: Any = None
        self.suspend_reason: Optional[str] = None
        self.resume_time: Optional[datetime] = None
        
        # Metrics
        self.metrics = SubgraphMetrics()
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Async execution
        self._pending_future: Optional[Future] = None
        
        logger.info(f"Subgraph '{name}' created at depth {depth}")
    
    @property
    def is_active(self) -> bool:
        """Check if subgraph is active (lit up)."""
        return self.state == SubgraphState.ACTIVE
    
    @property
    def is_suspended(self) -> bool:
        """Check if subgraph is suspended (lit down)."""
        return self.state == SubgraphState.SUSPENDED
    
    @property
    def entry_node(self) -> Optional[SubgraphNodeReference]:
        """Get the entry node of this subgraph."""
        return self.nodes.get(self.entry_node_name)
    
    @property
    def exit_node(self) -> Optional[SubgraphNodeReference]:
        """Get the exit node of this subgraph."""
        return self.nodes.get(self.exit_node_name)
    
    @property
    def node_count(self) -> int:
        """Get number of nodes in subgraph."""
        return len(self.nodes)
    
    def load(self, node_definitions: List[Dict], edge_definitions: List[Dict]):
        """
        Load subgraph structure from configuration.
        
        Args:
            node_definitions: List of node configs with 'name' and 'borrows' keys
            edge_definitions: List of edge configs with 'from' and 'to' keys
        """
        with self._lock:
            # Create node references
            for node_def in node_definitions:
                name = node_def['name']
                borrows = node_def.get('borrows', node_def.get('borrows_from', ''))
                role = node_def.get('role', 'internal')
                
                # Determine role from entry/exit config
                if name == self.entry_node_name:
                    role = 'entry'
                elif name == self.exit_node_name:
                    role = 'exit'
                
                node_ref = SubgraphNodeReference(
                    name=name,
                    borrows_from=borrows,
                    role=role
                )
                self.nodes[name] = node_ref
                self._adjacency[name] = []
                self._reverse_adjacency[name] = []
            
            # Create edges
            for edge_def in edge_definitions:
                from_node = edge_def['from']
                to_node = edge_def['to']
                
                if from_node not in self.nodes:
                    raise SubgraphConfigError(f"Edge references unknown node: {from_node}")
                if to_node not in self.nodes:
                    raise SubgraphConfigError(f"Edge references unknown node: {to_node}")
                
                edge = SubgraphEdge(from_node=from_node, to_node=to_node)
                self.edges.append(edge)
                self._adjacency[from_node].append(to_node)
                self._reverse_adjacency[to_node].append(from_node)
            
            # Compute topological order
            self._compute_topological_order()
            
            # Validate structure
            self._validate_structure()
            
            self.state = SubgraphState.LOADED
            logger.info(f"Subgraph '{self.name}' loaded with {len(self.nodes)} nodes")
    
    def bind_to_parent(self, parent_graph: Any):
        """
        Bind subgraph nodes to parent graph nodes (borrow calculators).
        
        Args:
            parent_graph: The parent graph to borrow nodes from
        """
        self.parent_graph = parent_graph
        
        for node_ref in self.nodes.values():
            parent_node_name = node_ref.borrows_from
            
            # First try to find a node in the parent graph
            parent_node = parent_graph.get_node(parent_node_name) if hasattr(parent_graph, 'get_node') else None
            
            if parent_node is not None:
                node_ref.parent_node = parent_node
                
                # Borrow calculator or transformer from the node
                if hasattr(parent_node, 'calculator') and parent_node.calculator:
                    node_ref.calculator = parent_node.calculator
                if hasattr(parent_node, 'transformer') and parent_node.transformer:
                    node_ref.transformer = parent_node.transformer
                if hasattr(parent_node, '_calculator') and parent_node._calculator:
                    node_ref.calculator = parent_node._calculator
            else:
                # Try to find a calculator directly in parent graph
                if hasattr(parent_graph, 'calculators') and parent_node_name in parent_graph.calculators:
                    node_ref.calculator = parent_graph.calculators[parent_node_name]
                    logger.debug(f"Subgraph node '{node_ref.name}' borrowed calculator '{parent_node_name}'")
                # Try to find a transformer directly in parent graph
                elif hasattr(parent_graph, 'transformers') and parent_node_name in parent_graph.transformers:
                    node_ref.transformer = parent_graph.transformers[parent_node_name]
                    logger.debug(f"Subgraph node '{node_ref.name}' borrowed transformer '{parent_node_name}'")
                else:
                    raise SubgraphConfigError(
                        f"Subgraph '{self.name}' node '{node_ref.name}' cannot borrow from "
                        f"'{parent_node_name}' - not found in parent graph (nodes, calculators, or transformers)"
                    )
            
            logger.debug(f"Subgraph node '{node_ref.name}' bound to parent '{parent_node_name}'")
        
        self.state = SubgraphState.ACTIVE
        self.metrics.last_state_change = datetime.utcnow()
        logger.info(f"Subgraph '{self.name}' bound to parent and activated")
    
    def activate(self, reason: str = None):
        """
        Activate (light up) the subgraph.
        
        When active, the subgraph participates in calculation cycles.
        """
        with self._lock:
            if self.state == SubgraphState.ACTIVE:
                return
            
            old_state = self.state
            self.state = SubgraphState.ACTIVE
            self.suspend_reason = None
            self.resume_time = None
            
            # Update metrics
            now = datetime.utcnow()
            if self.metrics.last_state_change and old_state == SubgraphState.SUSPENDED:
                delta = (now - self.metrics.last_state_change).total_seconds()
                self.metrics.suspended_time_seconds += delta
            self.metrics.last_state_change = now
            
            # Mark entry node dirty to trigger recalculation
            if self.entry_node:
                self.entry_node.mark_dirty()
            
            logger.info(f"Subgraph '{self.name}' activated (lit up). Reason: {reason}")
    
    def suspend(self, reason: str = None, resume_time: datetime = None):
        """
        Suspend (light down) the subgraph.
        
        When suspended, the subgraph is skipped and cached output is used.
        """
        with self._lock:
            if self.state == SubgraphState.SUSPENDED:
                return
            
            old_state = self.state
            self.state = SubgraphState.SUSPENDED
            self.suspend_reason = reason
            self.resume_time = resume_time
            
            # Update metrics
            now = datetime.utcnow()
            if self.metrics.last_state_change and old_state == SubgraphState.ACTIVE:
                delta = (now - self.metrics.last_state_change).total_seconds()
                self.metrics.active_time_seconds += delta
            self.metrics.last_state_change = now
            
            logger.info(f"Subgraph '{self.name}' suspended (lit down). Reason: {reason}")
    
    def execute(self, input_data: Any) -> Any:
        """
        Execute the subgraph with given input data.
        
        Args:
            input_data: Data to feed to entry node
            
        Returns:
            Output from exit node
        """
        import time
        
        with self._lock:
            if not self.is_active:
                # Subgraph is dark - return based on dark_output_mode
                self.metrics.cache_hit_count += 1
                
                if self.dark_output_mode == DarkOutputMode.CACHED:
                    logger.debug(f"Subgraph '{self.name}' is dark, returning cached output")
                    return self.cached_output
                elif self.dark_output_mode == DarkOutputMode.PASSTHROUGH:
                    logger.debug(f"Subgraph '{self.name}' is dark, passing through input")
                    return input_data
                else:  # BLOCK
                    logger.debug(f"Subgraph '{self.name}' is dark, blocking output")
                    return None
            
            start_time = time.perf_counter()
            
            try:
                self.state = SubgraphState.RUNNING
                
                # Feed input to entry node
                self.entry_node.input_data = input_data
                self.entry_node.mark_dirty()
                
                # Execute nodes in topological order
                current_data = input_data
                
                for node_name in self._topological_order:
                    node = self.nodes[node_name]
                    
                    if not node.is_dirty:
                        continue
                    
                    # Get input from upstream nodes
                    upstream_nodes = self._reverse_adjacency.get(node_name, [])
                    
                    if not upstream_nodes:
                        # Entry node - use provided input
                        node_input = input_data
                    elif len(upstream_nodes) == 1:
                        # Single upstream - use its output
                        node_input = self.nodes[upstream_nodes[0]].output_data
                    else:
                        # Multiple upstream - merge outputs
                        node_input = {
                            up: self.nodes[up].output_data 
                            for up in upstream_nodes
                        }
                    
                    # Execute node
                    node.execute(node_input)
                    
                    # Propagate dirty to downstream
                    for downstream in self._adjacency.get(node_name, []):
                        self.nodes[downstream].mark_dirty()
                
                # Get output from exit node
                output = self.exit_node.output_data
                self.cached_output = output
                
                # Update metrics
                execution_time = (time.perf_counter() - start_time) * 1000
                self.metrics.execution_count += 1
                self.metrics.total_execution_time_ms += execution_time
                self.metrics.last_execution_time_ms = execution_time
                self.metrics.last_execution_timestamp = datetime.utcnow()
                
                self.state = SubgraphState.ACTIVE
                
                logger.debug(f"Subgraph '{self.name}' executed in {execution_time:.2f}ms")
                
                return output
                
            except Exception as e:
                self.metrics.error_count += 1
                self.state = SubgraphState.ACTIVE  # Reset state
                
                if self.error_policy == ErrorPolicy.PROPAGATE:
                    raise SubgraphExecutionError(self.name, e) from e
                elif self.error_policy == ErrorPolicy.USE_CACHED:
                    logger.error(f"Subgraph '{self.name}' error: {e}, using cached output")
                    return self.cached_output
                else:  # AUTO_SUSPEND
                    logger.error(f"Subgraph '{self.name}' error: {e}, auto-suspending")
                    self.suspend(reason=str(e))
                    return self.cached_output
    
    def propagate_dirty(self):
        """Propagate dirty flag through internal nodes."""
        if not self.is_active:
            # Don't propagate dirty through dark subgraph
            return
        
        for node in self.nodes.values():
            node.mark_dirty()
    
    def get_internal_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get metrics for each internal node."""
        return {
            name: {
                'is_dirty': node.is_dirty,
                'last_execution_time_ms': node.last_execution_time_ms,
                'borrows_from': node.borrows_from
            }
            for name, node in self.nodes.items()
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert subgraph to dictionary for serialization."""
        return {
            'name': self.name,
            'description': self.description,
            'version': self.version,
            'entry_node': self.entry_node_name,
            'exit_node': self.exit_node_name,
            'execution_mode': self.execution_mode.value,
            'dark_output_mode': self.dark_output_mode.value,
            'error_policy': self.error_policy.value,
            'state': self.state.value,
            'suspend_reason': self.suspend_reason,
            'depth': self.depth,
            'nodes': [node.to_dict() for node in self.nodes.values()],
            'edges': [edge.to_dict() for edge in self.edges],
            'metrics': {
                'execution_count': self.metrics.execution_count,
                'avg_execution_time_ms': self.metrics.avg_execution_time_ms,
                'last_execution_time_ms': self.metrics.last_execution_time_ms,
                'cache_hit_count': self.metrics.cache_hit_count,
                'error_count': self.metrics.error_count,
                # UI-facing aliases (the details template reads these names):
                'avg_latency_ms': self.metrics.avg_execution_time_ms,
                'last_latency_ms': self.metrics.last_execution_time_ms,
                'cache_hits': self.metrics.cache_hit_count,
                'errors': self.metrics.error_count
            }
        }




# v2.0.0 module split (imported AFTER Subgraph is defined because
# SubgraphNode instantiates Subgraph at runtime).
from core.dag.subgraph_node import SubgraphNode  # noqa: E402,F401
from core.dag.subgraph_supervisor import SubgraphSupervisor  # noqa: E402,F401

# v2.0.0 module split: load_subgraph_from_config lives with SubgraphNode.
from core.dag.subgraph_node import load_subgraph_from_config  # noqa: E402,F401
