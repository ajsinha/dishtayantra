"""
Subgraph Model Types (v2.0.0 module split)
==========================================

Enums, dataclasses, and exceptions for the subgraph framework, extracted
verbatim from subgraph.py so each module stays within the 500-line
architecture limit. All names are re-exported from core.dag.subgraph, so
existing imports keep working.

Patent Pending - DishtaYantra Framework
Copyright (c) 2025-2030 Ashutosh Sinha. All Rights Reserved.
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




# Custom Exceptions

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


class SubgraphStructureMixin:
    """Topological ordering and structural validation for Subgraph
    (v2.0.0 module split - all state lives on Subgraph)."""

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
    
