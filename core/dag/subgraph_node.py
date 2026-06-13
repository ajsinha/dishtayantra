"""
SubgraphNode (v2.0.0 module split)
==================================

The graph-as-a-node wrapper that embeds a Subgraph inside a parent
ComputeGraph, extracted verbatim from subgraph.py. Re-exported from
core.dag.subgraph, so existing imports keep working.

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

from core.dag.subgraph_model import (
    DarkOutputMode,
    ErrorPolicy,
    ExecutionMode,
    SubgraphConfigError,
    SubgraphError,
    SubgraphExecutionError,
    SubgraphState,
)

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
        # Lazy import: core.dag.subgraph re-exports this module, so the
        # Subgraph class must be resolved at call time, not import time.
        from core.dag.subgraph import Subgraph
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


# Factory function for loading subgraphs
# (Subgraph is imported lazily inside to avoid a circular import with
#  core.dag.subgraph, which re-exports this function.)

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
