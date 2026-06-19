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

from core.core_utils import instantiate_module
# Built-in calculator/transformer types are resolved by type name from these two
# module namespaces (see _resolve_builtin_type). This replaces a legacy
# `from ... import *` that populated globals() and defeated static analysis.
from core.calculator import core_calculator as _calc_builtins
from core.transformer import core_transformer as _transformer_builtins
from core.calculator.core_calculator import DataCalculatorLike, CalculatorFactory
from core.dag.subgraph import SubgraphNode
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
from core.egress.async_publisher import maybe_wrap_publisher
# SubgraphWrapperNode is constructed at runtime by _build_subgraph_node.
from core.dag.node_implementations import SubgraphWrapperNode

logger = logging.getLogger(__name__)


def _resolve_builtin_type(type_name):
    """Resolve a built-in calculator/transformer class by its type name, or None.

    Looks in the core_calculator and core_transformer module namespaces - the same
    set of names the previous `globals()` star-import lookup exposed, but explicit
    and statically analyzable.
    """
    for namespace in (_calc_builtins, _transformer_builtins):
        cls = getattr(namespace, type_name, None)
        if isinstance(cls, type):
            return cls
    return None


# Implicit built-in transformers: usable by these short names in a node's
# input_transformers / output_transformers WITHOUT an explicit entry in the DAG's
# "transformers" section. An explicit transformer of the same name overrides them.
# These are identity-style helpers callers reach for constantly, so requiring a
# boilerplate definition was pure friction. Kept here so both the builder and the
# DAG Designer validator can import one source of truth.
IMPLICIT_TRANSFORMERS = {
    "passthru": "PassthruDataTransformer",
    "null": "NullDataTransformer",
}


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
            self.publishers[name] = maybe_wrap_publisher(
                create_publisher(name, config),
                getattr(self, 'prop_conf', None), name=name,
                scope=getattr(self, 'name', None), publisher_config=config)
            logger.info(f"Created publisher: {name}")

        # Build calculators
        for calc_config in self.config.get('calculators', []):
            name = calc_config['name']
            calc_type = calc_config.get('type', 'NullCalculator')
            config = calc_config.get('config', {})

            # Bridge calculators (C++ via pybind11, Java via Py4J, Rust via PyO3) are
            # created through CalculatorFactory, which knows how to load the native
            # backend. Detected by an explicit type of cpp/java/rust, or by the presence
            # of a *_class key in config. The factory's own config key is 'calculator',
            # so we map the schema's `type` onto it (without clobbering an explicit one).
            is_bridge = (isinstance(calc_type, str)
                         and calc_type.lower() in ('cpp', 'java', 'rust')) \
                or any(k in config for k in
                       ('cpp_class', 'java_class', 'rust_class'))
            if is_bridge:
                factory_config = dict(config)
                factory_config.setdefault('calculator', calc_type)
                self.calculators[name] = CalculatorFactory.create(name, factory_config)
                logger.info(f"Created calculator: {name} (bridge: {calc_type})")
                continue

            # Check if it's a known built-in calculator
            builtin = _resolve_builtin_type(calc_type)
            if builtin is not None:
                self.calculators[name] = builtin(name, config)
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

            # Check if it's a known built-in transformer
            builtin = _resolve_builtin_type(trans_type)
            if builtin is not None:
                self.transformers[name] = builtin(name, config)
            else:
                # Custom transformer
                parts = trans_type.rsplit('.', 1)
                module_path = parts[0]
                class_name = parts[1] if len(parts) > 1 else trans_type
                self.transformers[name] = instantiate_module(module_path, class_name, {'name': name, 'config': config})

            logger.info(f"Created transformer: {name}")

        # Seed implicit built-in transformers (passthru, null) for any name not
        # explicitly defined above, so nodes may reference them without boilerplate.
        # An explicit definition of the same name (built above) takes precedence.
        for impl_name, cls_name in IMPLICIT_TRANSFORMERS.items():
            if impl_name not in self.transformers:
                cls = _resolve_builtin_type(cls_name)
                if cls is not None:
                    self.transformers[impl_name] = cls(impl_name, {})


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

