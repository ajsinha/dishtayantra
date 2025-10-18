import json
import logging
import threading
import time
from datetime import datetime
from collections import deque, defaultdict
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
from core.calculator.core_calculator import *
from core.transformer.core_transformer import *
from core.dag.graph_elements import Node, Edge
from core.dag.node_implementations import *
from core.core_utils import instantiate_module

logger = logging.getLogger(__name__)


class ComputeGraph:
    """Compute graph for DAG execution"""

    def __init__(self, config):
        # Load config if it's a file path
        if isinstance(config, str):
            with open(config, 'r') as f:
                config = json.load(f)

        self.config = config
        self.name = config.get('name')
        self.start_time = config.get('start_time')
        self.end_time = config.get('end_time')

        self.subscribers = {}
        self.publishers = {}
        self.calculators = {}
        self.transformers = {}
        self.nodes = {}
        self.edges = []

        self._compute_thread = None
        self._stop_event = threading.Event()
        self._suspend_event = threading.Event()
        self._suspend_event.set()  # Start in running state

        self._time_check_thread = None

        self._build_components()
        self.build_dag()

        logger.info(f"ComputeGraph {self.name} initialized")

    def _build_components(self):
        """Build all components from config"""
        # Build subscribers
        for sub_config in self.config.get('subscribers', []):
            name = sub_config['name']
            config = sub_config['config']
            self.subscribers[name] = create_subscriber(name, config)
            logger.info(f"Created subscriber: {name}")

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
                self.calculators[name] = instantiate_module(module_path, class_name, {'name': name, 'config': config})

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

    def subscriber_by_name(self, sub_name):
        return self.subscribers[sub_name]
    
    def publisher_by_name(self, pub_name):
        return self.subscribers[pub_name]

    def build_dag(self):
        """Build the DAG from config"""
        # Build nodes
        for node_config in self.config.get('nodes', []):
            name = node_config['name']
            node_type = node_config.get('type', 'CalculationNode')
            config = node_config.get('config', {})

            # Create node
            if node_type in globals():
                node = globals()[node_type](name, config)
            else:
                # Custom node
                parts = node_type.rsplit('.', 1)
                module_path = parts[0]
                class_name = parts[1] if len(parts) > 1 else node_type
                node = instantiate_module(module_path, class_name, {'name': name, 'config': config})

            # Set subscriber if specified
            if 'subscriber' in node_config:
                sub_name = node_config['subscriber']
                if sub_name in self.subscribers:
                    node.set_subscriber(self.subscribers[sub_name])

            # Add publishers if specified
            if 'publishers' in node_config:
                for pub_name in node_config['publishers']:
                    if pub_name in self.publishers:
                        node.add_publisher(self.publishers[pub_name])

            # Set calculator if specified
            if 'calculator' in node_config:
                calc_name = node_config['calculator']
                if calc_name in self.calculators:
                    node.set_calculator(self.calculators[calc_name])

            # Add input transformers if specified
            if 'input_transformers' in node_config:
                for trans_name in node_config['input_transformers']:
                    if trans_name in self.transformers:
                        node.add_input_transformer(self.transformers[trans_name])

            # Add output transformers if specified
            if 'output_transformers' in node_config:
                for trans_name in node_config['output_transformers']:
                    if trans_name in self.transformers:
                        node.add_output_transformer(self.transformers[trans_name])

            node.set_graph(self)
            self.nodes[name] = node
            logger.info(f"Created node: {name}")

        # Build edges
        for edge_config in self.config.get('edges', []):
            from_node_name = edge_config['from_node']
            to_node_name = edge_config['to_node']
            transformer_name = edge_config.get('data_transformer')

            from_node = self.nodes[from_node_name]
            to_node = self.nodes[to_node_name]
            transformer = self.transformers.get(transformer_name) if transformer_name else None

            edge = Edge(from_node, to_node, transformer)
            self.edges.append(edge)
            logger.info(f"Created edge: {edge.name}")

        # Check for cycles
        if self._has_cycle():
            cycle_info = self._find_cycle()
            error_msg = f"Cycle detected in graph: {' -> '.join(cycle_info)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info(f"DAG built successfully with {len(self.nodes)} nodes and {len(self.edges)} edges")

    def _has_cycle(self):
        """Check if graph has a cycle using DFS"""
        visited = set()
        rec_stack = set()

        def dfs(node):
            visited.add(node.name)
            rec_stack.add(node.name)

            for edge in node._outgoing_edges:
                child = edge.to_node
                if child.name not in visited:
                    if dfs(child):
                        return True
                elif child.name in rec_stack:
                    return True

            rec_stack.remove(node.name)
            return False

        for node in self.nodes.values():
            if node.name not in visited:
                if dfs(node):
                    return True

        return False

    def _find_cycle(self):
        """Find and return cycle path"""
        visited = set()
        rec_stack = []

        def dfs(node):
            visited.add(node.name)
            rec_stack.append(node.name)

            for edge in node._outgoing_edges:
                child = edge.to_node
                if child.name not in visited:
                    result = dfs(child)
                    if result:
                        return result
                elif child.name in rec_stack:
                    cycle_start = rec_stack.index(child.name)
                    return rec_stack[cycle_start:] + [child.name]

            rec_stack.pop()
            return None

        for node in self.nodes.values():
            if node.name not in visited:
                result = dfs(node)
                if result:
                    return result

        return []

    def topological_sort(self):
        """Return nodes in topologically sorted order"""
        in_degree = {name: 0 for name in self.nodes}

        for node in self.nodes.values():
            for edge in node._outgoing_edges:
                in_degree[edge.to_node.name] += 1

        queue = deque([name for name, degree in in_degree.items() if degree == 0])
        sorted_nodes = []

        while queue:
            node_name = queue.popleft()
            sorted_nodes.append(self.nodes[node_name])

            for edge in self.nodes[node_name]._outgoing_edges:
                child_name = edge.to_node.name
                in_degree[child_name] -= 1
                if in_degree[child_name] == 0:
                    queue.append(child_name)

        return sorted_nodes

    def start(self):
        """Start the compute graph"""
        # Start all subscribers
        for subscriber in self.subscribers.values():
            subscriber.start()

        # Start metronome nodes
        for node in self.nodes.values():
            if isinstance(node, MetronomeNode):
                node.start_metronome()

        # Start compute thread
        if not self._compute_thread or not self._compute_thread.is_alive():
            self._stop_event.clear()
            self._compute_thread = threading.Thread(target=self.do_compute, daemon=True)
            self._compute_thread.start()

        # Start time window check thread if time window is configured
        if self.start_time and self.end_time:
            if not self._time_check_thread or not self._time_check_thread.is_alive():
                self._time_check_thread = threading.Thread(target=self._time_window_check, daemon=True)
                self._time_check_thread.start()
                logger.info(f"DAG {self.name}: Time window monitor started ({self.start_time}-{self.end_time})")

        logger.info(f"ComputeGraph {self.name} started")

    def _time_window_check(self):
        """Check if current time is within configured window"""
        logger.info(f"DAG {self.name}: Time window checker started")

        while not self._stop_event.is_set():
            if not self.start_time or not self.end_time:
                # No time window configured, skip
                time.sleep(60)
                continue

            now = datetime.now()
            current_time = now.strftime('%H%M')
            current_time_int = int(current_time)
            start_time_int = int(self.start_time)
            end_time_int = int(self.end_time)

            in_window = start_time_int <= current_time_int <= end_time_int

            logger.debug(
                f"DAG {self.name} time check: current={current_time_int}, start={start_time_int}, end={end_time_int}, in_window={in_window}")

            if in_window:
                # Within time window - should be running
                if not self._suspend_event.is_set():
                    logger.info(f"DAG {self.name}: Entering time window, resuming")
                    self.resume()
            else:
                # Outside time window - should be suspended
                if self._suspend_event.is_set():
                    logger.info(f"DAG {self.name}: Leaving time window, suspending")
                    self.suspend()

            time.sleep(60)  # Check every minute

        logger.info(f"DAG {self.name}: Time window checker stopped")

    def is_in_time_window(self):
        """Check if current time is within the configured window"""
        if not self.start_time or not self.end_time:
            return True  # Always active if no window configured

        now = datetime.now()
        current_time_int = int(now.strftime('%H%M'))
        start_time_int = int(self.start_time)
        end_time_int = int(self.end_time)

        return start_time_int <= current_time_int <= end_time_int

    def suspend(self):
        """Suspend the compute graph"""
        self._suspend_event.clear()

        for subscriber in self.subscribers.values():
            subscriber.suspend()

        logger.info(f"ComputeGraph {self.name} suspended")

    def resume(self):
        """Resume the compute graph"""
        self._suspend_event.set()

        for subscriber in self.subscribers.values():
            subscriber.resume()

        logger.info(f"ComputeGraph {self.name} resumed")

    def stop(self):
        """Stop the compute graph"""
        self._stop_event.set()
        self._suspend_event.set()  # Unblock if suspended

        # Stop subscribers
        for subscriber in self.subscribers.values():
            subscriber.stop()

        # Stop publishers
        for publisher in self.publishers.values():
            publisher.stop()

        # Stop metronome nodes
        for node in self.nodes.values():
            if isinstance(node, MetronomeNode):
                node.stop_metronome()

        # Wait for compute thread
        if self._compute_thread:
            self._compute_thread.join(timeout=5)

        logger.info(f"ComputeGraph {self.name} stopped")

    def do_compute(self):
        """Main compute loop"""
        sorted_nodes = self.topological_sort()

        while not self._stop_event.is_set():
            self._suspend_event.wait()  # Block if suspended

            if self._stop_event.is_set():
                break

            # Pre-compute phase
            for node in sorted_nodes:
                node.pre_compute()

            # Compute phase
            for node in sorted_nodes:
                if node.isdirty():
                    node.compute()
                    node.post_compute()

            time.sleep(0.01)  # Small delay to prevent CPU spinning

    def clone(self, start_time=None, end_time=None):
        """Clone the compute graph with optional time window"""
        cloned_config = self.config.copy()
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        cloned_config['name'] = f"{self.name}_{timestamp}"

        if start_time:
            cloned_config['start_time'] = start_time
        if end_time:
            cloned_config['end_time'] = end_time

        return ComputeGraph(cloned_config)

    def show_json(self):
        """Return configuration as JSON string"""
        return json.dumps(self.config, indent=2)

    def details(self):
        """Return details of the compute graph"""
        return {
            'name': self.name,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'is_running': self._compute_thread and self._compute_thread.is_alive(),
            'is_suspended': not self._suspend_event.is_set(),
            'nodes': {name: node.details() for name, node in self.nodes.items()},
            'edges': [edge.details() for edge in self.edges],
            'subscribers': {name: sub.details() for name, sub in self.subscribers.items()},
            'publishers': {name: pub.details() for name, pub in self.publishers.items()},
            'calculators': {name: calc.details() for name, calc in self.calculators.items()},
            'transformers': {name: trans.details() for name, trans in self.transformers.items()}
        }