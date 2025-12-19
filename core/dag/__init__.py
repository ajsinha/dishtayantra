# core/dag/__init__.py
"""
DAG module for compute graph execution.

Includes:
- ComputeGraph: Main DAG execution engine
- Node implementations: SubscriptionNode, CalculationNode, MetronomeNode, etc.
- Subgraph support: Subgraph, SubgraphNode, SubgraphSupervisor

Patent Pending - DishtaYantra Framework
Copyright © 2025-2030 Ashutosh Sinha. All Rights Reserved.
"""

from core.dag.compute_graph import ComputeGraph
from core.dag.graph_elements import Node, Edge
from core.dag.node_implementations import (
    SinkNode, PublisherSinkNode, SubscriptionNode, 
    MetronomeNode, CalculationNode, SubgraphWrapperNode
)
from core.dag.subgraph import (
    Subgraph, SubgraphNode, SubgraphNodeReference, SubgraphSupervisor,
    SubgraphState, ExecutionMode, DarkOutputMode, ErrorPolicy,
    SubgraphError, SubgraphConfigError, SubgraphExecutionError,
    load_subgraph_from_config
)

__all__ = [
    # Core DAG
    'ComputeGraph',
    'Node',
    'Edge',
    
    # Node implementations
    'SinkNode',
    'PublisherSinkNode', 
    'SubscriptionNode',
    'MetronomeNode',
    'CalculationNode',
    'SubgraphWrapperNode',
    
    # Subgraph
    'Subgraph',
    'SubgraphNode',
    'SubgraphNodeReference',
    'SubgraphSupervisor',
    'SubgraphState',
    'ExecutionMode',
    'DarkOutputMode',
    'ErrorPolicy',
    'SubgraphError',
    'SubgraphConfigError',
    'SubgraphExecutionError',
    'load_subgraph_from_config',
]
