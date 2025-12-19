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


class SubgraphState(Enum):
    """Lifecycle states for a subgraph."""
    CREATED = "created"
    LOADED = "loaded"
    ACTIVE = "active"          # "Lit up" - participates in calculations
    SUSPENDED = "suspended"    # "Lit down" - skipped, uses cached output
    RUNNING = "running"        # Currently executing (async mode)
    FAILED = "failed"
    UNLOADED = "unloaded"


class ExecutionMode(Enum):
    """Execution mode for subgraph."""
    SYNCHRONOUS = "synchronous"    # Blocks parent graph execution
    ASYNCHRONOUS = "asynchronous"  # Runs in separate thread


class DarkOutputMode(Enum):
    """What to output when subgraph is dark (suspended)."""
    CACHED = "cached"          # Use last computed output
    PASSTHROUGH = "passthrough"  # Pass input directly to output
    BLOCK = "block"            # Block downstream execution


class ErrorPolicy(Enum):
    """How to handle errors in subgraph execution."""
    PROPAGATE = "propagate"      # Raise error to parent
    USE_CACHED = "use_cached"    # Return cached output, log error
    AUTO_SUSPEND = "auto_suspend"  # Suspend subgraph and notify


@dataclass
class SubgraphMetrics:
    """Metrics for subgraph execution."""
    execution_count: int = 0
    total_execution_time_ms: float = 0.0
    last_execution_time_ms: float = 0.0
    last_execution_timestamp: Optional[datetime] = None
    cache_hit_count: int = 0
    error_count: int = 0
    active_time_seconds: float = 0.0
    suspended_time_seconds: float = 0.0
    last_state_change: Optional[datetime] = None
    
    @property
    def avg_execution_time_ms(self) -> float:
        if self.execution_count == 0:
            return 0.0
        return self.total_execution_time_ms / self.execution_count


@dataclass
class SubgraphNodeReference:
    """
    A virtual node in a subgraph that borrows implementation from parent.
    
    This node doesn't have its own calculator - it references a node
    defined in the parent graph and uses its calculator.
    """
    name: str
    borrows_from: str  # Name of parent node to borrow from
    role: str = "internal"  # "entry", "exit", or "internal"
    
    # Runtime state (set during initialization)
    parent_node: Any = None  # Reference to actual parent node
    calculator: Any = None   # Borrowed calculator
    transformer: Any = None  # Borrowed transformer
    
    # Execution state
    is_dirty: bool = True
    input_data: Any = None
    output_data: Any = None
    last_execution_time_ms: float = 0.0
    
    def mark_dirty(self):
        """Mark this node as needing recalculation."""
        self.is_dirty = True
    
    def mark_clean(self):
        """Mark this node as up-to-date."""
        self.is_dirty = False
    
    def execute(self, input_data: Any) -> Any:
        """Execute borrowed calculator on input data."""
        import time
        start = time.perf_counter()
        
        self.input_data = input_data
        
        if self.calculator:
            self.output_data = self.calculator.calculate(input_data)
        elif self.transformer:
            self.output_data = self.transformer.transform(input_data)
        else:
            # Pass through if no calculator/transformer
            self.output_data = input_data
        
        self.last_execution_time_ms = (time.perf_counter() - start) * 1000
        self.mark_clean()
        
        return self.output_data
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'name': self.name,
            'borrows_from': self.borrows_from,
            'role': self.role,
            'is_dirty': self.is_dirty,
            'last_execution_time_ms': self.last_execution_time_ms
        }


@dataclass
class SubgraphEdge:
    """Edge connecting nodes within a subgraph."""
    from_node: str
    to_node: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {'from': self.from_node, 'to': self.to_node}


class Subgraph:
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
    
    def _compute_topological_order(self):
        """Compute topological ordering of nodes using Kahn's algorithm."""
        in_degree = {node: 0 for node in self.nodes}
        
        for edges in self._adjacency.values():
            for to_node in edges:
                in_degree[to_node] += 1
        
        queue = deque([node for node, degree in in_degree.items() if degree == 0])
        order = []
        
        while queue:
            node = queue.popleft()
            order.append(node)
            
            for neighbor in self._adjacency.get(node, []):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        if len(order) != len(self.nodes):
            raise SubgraphConfigError(f"Subgraph '{self.name}' contains a cycle")
        
        self._topological_order = order
    
    def _validate_structure(self):
        """Validate subgraph structure."""
        # Must have entry node
        if not self.entry_node_name or self.entry_node_name not in self.nodes:
            raise SubgraphConfigError(
                f"Subgraph '{self.name}' must have a valid entry_node"
            )
        
        # Must have exit node
        if not self.exit_node_name or self.exit_node_name not in self.nodes:
            raise SubgraphConfigError(
                f"Subgraph '{self.name}' must have a valid exit_node"
            )
        
        # Entry node should have no incoming edges from within subgraph
        if self._reverse_adjacency.get(self.entry_node_name):
            raise SubgraphConfigError(
                f"Entry node '{self.entry_node_name}' should not have incoming edges "
                f"from within the subgraph"
            )
        
        # Exit node should have no outgoing edges within subgraph
        if self._adjacency.get(self.exit_node_name):
            raise SubgraphConfigError(
                f"Exit node '{self.exit_node_name}' should not have outgoing edges "
                f"within the subgraph"
            )
        
        logger.debug(f"Subgraph '{self.name}' structure validated")
    
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
                'error_count': self.metrics.error_count
            }
        }


class SubgraphNode:
    """
    A node in the parent graph that represents an entire subgraph.
    
    This is the "composite node" that appears in the parent graph and
    encapsulates a complete subgraph.
    """
    
    def __init__(self, name: str, config: Dict[str, Any], parent_graph: Any = None):
        """
        Initialize a SubgraphNode.
        
        Args:
            name: Node name in parent graph
            config: Configuration including subgraph definition or file reference
            parent_graph: Reference to parent graph
        """
        self.name = name
        self.config = config
        self.parent_graph = parent_graph
        
        # Connection configuration
        self.entry_connection: str = config.get('entry_connection', '')
        self.exit_connections: List[str] = config.get('exit_connections', [])
        
        # The actual subgraph
        self.subgraph: Optional[Subgraph] = None
        
        # Node state (for parent graph compatibility)
        self.is_dirty = True
        self.input_data: Any = None
        self.output_data: Any = None
        
        # Async execution
        self._executor: Optional[ThreadPoolExecutor] = None
        self._pending_future: Optional[Future] = None
        
        logger.debug(f"SubgraphNode '{name}' created")
    
    @property
    def is_active(self) -> bool:
        """Check if contained subgraph is active."""
        return self.subgraph.is_active if self.subgraph else False
    
    @property
    def is_suspended(self) -> bool:
        """Check if contained subgraph is suspended."""
        return self.subgraph.is_suspended if self.subgraph else True
    
    @property
    def execution_mode(self) -> ExecutionMode:
        """Get execution mode of contained subgraph."""
        if self.subgraph:
            return self.subgraph.execution_mode
        return ExecutionMode.SYNCHRONOUS
    
    def load_subgraph(self, subgraph_config: Dict[str, Any] = None, 
                      subgraph_file: str = None,
                      base_path: str = None,
                      depth: int = 0):
        """
        Load the subgraph from inline config or external file.
        
        Args:
            subgraph_config: Inline subgraph configuration
            subgraph_file: Path to external subgraph JSON file
            base_path: Base path for resolving relative file paths
            depth: Current nesting depth
        """
        # Determine source of subgraph definition
        if subgraph_file:
            # Load from external file
            subgraph_config = self._load_subgraph_file(subgraph_file, base_path)
        elif not subgraph_config:
            raise SubgraphConfigError(
                f"SubgraphNode '{self.name}' requires either 'subgraph' (inline) "
                f"or 'subgraph_file' configuration"
            )
        
        # Create subgraph
        subgraph_name = subgraph_config.get('name', f"{self.name}_subgraph")
        self.subgraph = Subgraph(
            name=subgraph_name,
            config=subgraph_config,
            parent_graph=self.parent_graph,
            depth=depth
        )
        
        # Load structure
        nodes = subgraph_config.get('nodes', [])
        edges = subgraph_config.get('edges', [])
        self.subgraph.load(nodes, edges)
        
        logger.info(f"SubgraphNode '{self.name}' loaded subgraph '{subgraph_name}'")
    
    def _load_subgraph_file(self, file_path: str, base_path: str = None) -> Dict[str, Any]:
        """Load subgraph configuration from external file."""
        path = Path(file_path)
        
        # Resolve relative paths
        if not path.is_absolute():
            if base_path:
                path = Path(base_path) / path
            else:
                # Try common locations
                for search_path in ['config/subgraphs', 'subgraphs', '.']:
                    candidate = Path(search_path) / file_path
                    if candidate.exists():
                        path = candidate
                        break
        
        if not path.exists():
            raise SubgraphConfigError(f"Subgraph file not found: {path}")
        
        with open(path, 'r') as f:
            config = json.load(f)
        
        logger.debug(f"Loaded subgraph from file: {path}")
        return config
    
    def bind_to_parent(self, parent_graph: Any):
        """Bind this node and its subgraph to the parent graph."""
        self.parent_graph = parent_graph
        if self.subgraph:
            self.subgraph.bind_to_parent(parent_graph)
    
    def mark_dirty(self):
        """Mark this node and subgraph as dirty."""
        self.is_dirty = True
        if self.subgraph and self.subgraph.is_active:
            self.subgraph.propagate_dirty()
    
    def mark_clean(self):
        """Mark this node as clean."""
        self.is_dirty = False
    
    def execute(self, input_data: Any) -> Any:
        """
        Execute the subgraph.
        
        Args:
            input_data: Data from upstream node
            
        Returns:
            Output from subgraph exit node
        """
        self.input_data = input_data
        
        if not self.subgraph:
            logger.warning(f"SubgraphNode '{self.name}' has no loaded subgraph")
            self.output_data = input_data
            return input_data
        
        if self.execution_mode == ExecutionMode.ASYNCHRONOUS:
            return self._execute_async(input_data)
        else:
            return self._execute_sync(input_data)
    
    def _execute_sync(self, input_data: Any) -> Any:
        """Execute subgraph synchronously."""
        self.output_data = self.subgraph.execute(input_data)
        self.mark_clean()
        return self.output_data
    
    def _execute_async(self, input_data: Any) -> Any:
        """Execute subgraph asynchronously."""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=1, 
                                                 thread_name_prefix=f"subgraph_{self.name}")
        
        def run_subgraph():
            return self.subgraph.execute(input_data)
        
        self._pending_future = self._executor.submit(run_subgraph)
        
        # Return cached output immediately
        # Caller should check for completion later
        return self.subgraph.cached_output
    
    def wait_for_completion(self, timeout: float = None) -> Any:
        """Wait for async execution to complete."""
        if self._pending_future:
            try:
                self.output_data = self._pending_future.result(timeout=timeout)
                self._pending_future = None
                self.mark_clean()
                return self.output_data
            except Exception as e:
                logger.error(f"Async subgraph execution failed: {e}")
                return self.subgraph.cached_output
        return self.output_data
    
    def is_async_complete(self) -> bool:
        """Check if async execution is complete."""
        if self._pending_future:
            return self._pending_future.done()
        return True
    
    def light_up(self, reason: str = None):
        """Activate the contained subgraph."""
        if self.subgraph:
            self.subgraph.activate(reason)
            self.mark_dirty()  # Trigger recalculation
    
    def light_down(self, reason: str = None, resume_time: datetime = None):
        """Suspend the contained subgraph."""
        if self.subgraph:
            self.subgraph.suspend(reason, resume_time)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            'name': self.name,
            'type': 'SubgraphNode',
            'entry_connection': self.entry_connection,
            'exit_connections': self.exit_connections,
            'is_dirty': self.is_dirty,
            'is_active': self.is_active
        }
        
        if self.subgraph:
            result['subgraph'] = self.subgraph.to_dict()
        
        return result


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


# Custom Exceptions

class SubgraphError(Exception):
    """Base exception for subgraph errors."""
    pass


class SubgraphConfigError(SubgraphError):
    """Configuration error in subgraph definition."""
    pass


class SubgraphExecutionError(SubgraphError):
    """Error during subgraph execution."""
    
    def __init__(self, subgraph_name: str, original_error: Exception):
        self.subgraph_name = subgraph_name
        self.original_error = original_error
        super().__init__(f"Subgraph '{subgraph_name}' execution failed: {original_error}")


# Factory function for loading subgraphs

def load_subgraph_from_config(node_config: Dict[str, Any], 
                               parent_graph: Any,
                               base_path: str = None,
                               depth: int = 0) -> SubgraphNode:
    """
    Factory function to create and load a SubgraphNode from configuration.
    
    Supports hybrid loading (inline or external file).
    
    Args:
        node_config: Node configuration from DAG JSON
        parent_graph: Parent graph reference
        base_path: Base path for resolving file references
        depth: Current nesting depth
        
    Returns:
        Configured SubgraphNode
    """
    name = node_config['name']
    config = node_config.get('config', {})
    
    # Create SubgraphNode
    subgraph_node = SubgraphNode(name=name, config=config, parent_graph=parent_graph)
    
    # Determine subgraph source
    subgraph_file = config.get('subgraph_file')
    inline_subgraph = node_config.get('subgraph')
    
    # Load subgraph
    subgraph_node.load_subgraph(
        subgraph_config=inline_subgraph,
        subgraph_file=subgraph_file,
        base_path=base_path,
        depth=depth
    )
    
    return subgraph_node
