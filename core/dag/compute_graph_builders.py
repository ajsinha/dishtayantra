"""
ComputeGraph Component Builder Mixin (v2.0.0 module split)
==========================================================

Construction of pub/sub components, calculators, transformers, and embedded
subgraph nodes for :class:`core.dag.compute_graph.ComputeGraph`, extracted
verbatim so each module stays within the 500-line architecture limit. All
state lives on ComputeGraph.

Patent Pending - DishtaYantra Framework
Copyright (c) 2025-2030 Ashutosh Sinha. All Rights Reserved.
"""

from __future__ import annotations

import logging
import traceback

from core.core_utils import instantiate_module
# The builder resolves built-in calculator/transformer types by looking the
# type name up in this module's globals() (legacy mechanism, preserved
# verbatim) - so the full built-in namespaces must be star-imported here.
from core.calculator.core_calculator import *  # noqa: F401,F403
from core.transformer.core_transformer import *  # noqa: F401,F403
from core.dag.subgraph import (
    SubgraphConfigError,
    SubgraphNode,
    load_subgraph_from_config,
)
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
# SubgraphWrapperNode is constructed at runtime by _build_subgraph_node.
from core.dag.node_implementations import SubgraphWrapperNode

logger = logging.getLogger(__name__)


class ComponentBuilderMixin:
    """Builds subscribers, publishers, calculators, transformers, and
    embedded subgraph nodes from the DAG configuration."""

    def _build_components(self):
        """Build all components from config"""
        # Build subscribers
        for sub_config in self.config.get('subscribers', []):
            name = sub_config['name']
            config = sub_config['config']
            self.subscribers[name] = create_subscriber(name, config)
            logger.info(f"Created subscriber: {name}")

        # Now look for composite subscribers and adjust it.
        for sub_name in self.subscribers.keys():
            sub_object = self.subscribers.get(sub_name)
            if sub_object.is_composite():
                component_names = [x.strip() for x in sub_object.source.split(',')]
                for component_name in component_names:
                    component_sub_object = self.subscribers[component_name]
                    sub_object.add_data_subscriber(component_sub_object)
        #
        # Build publishers
        for pub_config in self.config.get('publishers', []):
            name = pub_config['name']
            config = pub_config['config']
            self.publishers[name] = create_publisher(name, config)
            logger.info(f"Created publisher: {name}")

        # Build calculators
        for calc_config in self.config.get('calculators', []):
            name = calc_config['name']
            calc_type = calc_config.get('type', 'NullCalculator')
            config = calc_config.get('config', {})

            # Check if it's a known calculator
            if calc_type in globals():
                self.calculators[name] = globals()[calc_type](name, config)
            else:
                # Custom calculator
                parts = calc_type.rsplit('.', 1)
                module_path = parts[0]
                class_name = parts[1] if len(parts) > 1 else calc_type
                self.calculators[name]: DataCalculatorLike = instantiate_module(module_path, class_name, {'name': name, 'config': config})

            logger.info(f"Created calculator: {name}")

        # Build transformers
        for trans_config in self.config.get('transformers', []):
            name = trans_config['name']
            trans_type = trans_config.get('type', 'NullDataTransformer')
            config = trans_config.get('config', {})

            # Check if it's a known transformer
            if trans_type in globals():
                self.transformers[name] = globals()[trans_type](name, config)
            else:
                # Custom transformer
                parts = trans_type.rsplit('.', 1)
                module_path = parts[0]
                class_name = parts[1] if len(parts) > 1 else trans_type
                self.transformers[name] = instantiate_module(module_path, class_name, {'name': name, 'config': config})

            logger.info(f"Created transformer: {name}")


    def _build_subgraph_node(self, node_config: dict) -> SubgraphNode:
        """
        Build a SubgraphNode from configuration.
        
        Supports hybrid loading (inline or external file).
        """
        name = node_config['name']
        config = node_config.get('config', {})
        
        # Create SubgraphNode
        subgraph_node = SubgraphNode(name=name, config=config, parent_graph=self)
        
        # Determine subgraph source (hybrid approach)
        subgraph_file = config.get('subgraph_file')
        inline_subgraph = node_config.get('subgraph')
        
        # Load subgraph
        subgraph_node.load_subgraph(
            subgraph_config=inline_subgraph,
            subgraph_file=subgraph_file,
            base_path=self._config_base_path,
            depth=1  # Parent graph is depth 0
        )
        
        # Bind to parent graph
        subgraph_node.bind_to_parent(self)
        
        # Track in subgraph_nodes dict
        self.subgraph_nodes[name] = subgraph_node
        
        # Create a wrapper node for graph compatibility
        wrapper = SubgraphWrapperNode(name, config, subgraph_node)
        wrapper.set_graph(self)
        
        logger.info(f"Created SubgraphNode: {name} with {subgraph_node.subgraph.node_count} internal nodes")
        
        return wrapper

