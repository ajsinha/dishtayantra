import logging
import threading
import time
from datetime import datetime
from core.dag.graph_elements import Node

logger = logging.getLogger(__name__)

class SinkNode(Node):
    def __init__(self, name, config):
        super().__init__(name, config)

    def compute(self):
        pass

class PublisherSinkNode(Node):
    def __init__(self, name, config):
        super().__init__(name, config)
        self.publishers = config.get("publishers", [])
        self._edge_tracker = {}

    def compute(self) -> bool:
        """Compute node output based on inputs"""
        if not self.isdirty():
            return False


        # Gather inputs from incoming edges
        edge_data_collection = []
        for edge in self._incoming_edges:
            edge_data = edge.get_data()
            edge_name = edge.name
            if edge_name not in self._edge_tracker.keys():
                self._edge_tracker[edge_name] = edge_data
                edge_data_collection.append(edge_data)
            else:
                known = self._edge_tracker[edge_name]
                if known == edge_data:
                    pass
                else:
                    self._edge_tracker[edge_name] = edge_data
                    edge_data_collection.append(edge_data)

        if len(edge_data_collection) >0:
            self_graph = self._graph
            for data in edge_data_collection:
                for publisher_name in self.publishers:
                    local_publisher = self_graph.get_publiisher_by_name(publisher_name)
                    local_publisher.publish(data)
            #self.increment_compute_count()
            return True
        else:
            return False


class SubscriptionNode(Node):
    """Node that pulls data from a DataSubscriber"""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.subscriber = None

    def set_subscriber(self, subscriber):
        """Set the data subscriber"""
        self.subscriber = subscriber

    def pre_compute(self):
        """Check if subscriber has data and mark dirty if so"""
        if self.subscriber and self.subscriber.get_queue_size() > 0:
            self.set_dirty()

    def compute(self) -> bool:
        """Pull data from subscriber and compute"""
        if not self.isdirty() or not self.subscriber:
            return False

        try:
            # Get data from subscriber
            data = self.subscriber.get_data(block_time=0)

            if data is None:
                return False

            # Merge with current input
            self._input = data

            # Apply input transformers
            transformed_input = self._input
            for transformer in self._input_transformers:
                transformed_input = transformer.transform(transformed_input)

            # Calculate if calculator is available
            if self._calculator:
                calculated_output = self._calculator.calculate(transformed_input)
            else:
                calculated_output = transformed_input

            # Apply output transformers
            transformed_output = calculated_output
            for transformer in self._output_transformers:
                transformed_output = transformer.transform(transformed_output)

            # Update output if changed
            if transformed_output != self._output:
                self._output = transformed_output

                # Mark children as dirty
                for edge in self._outgoing_edges:
                    edge.to_node.set_dirty()

            #self.increment_compute_count()
            self.set_clean()
            return True

        except Exception as e:
            error_info = {
                'time': datetime.now().isoformat(),
                'error': str(e)
            }
            self._errors.append(error_info)
            logger.error(f"Error in subscription node {self.name}: {str(e)}")
            return False


class PublicationNode(Node):
    """Node that publishes output to DataPublishers"""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.publishers = []
        self._last_published_output = None

    def add_publisher(self, publisher):
        """Add a data publisher"""
        self.publishers.append(publisher)

    def compute(self) -> bool:
        """Compute and publish if output changed"""
        if not self.isdirty():
            return False

        try:
            # Run parent compute logic
            super().compute()

            # Publish if output changed
            if self._output != self._last_published_output:
                for publisher in self.publishers:
                    try:
                        publisher.publish(self._output)
                    except Exception as e:
                        logger.error(f"Error publishing from node {self.name}: {str(e)}")

                self._last_published_output = self._output.copy() if self._output else None
            #self.increment_compute_count()
            return True
        except Exception as e:
            error_info = {
                'time': datetime.now().isoformat(),
                'error': str(e)
            }
            self._errors.append(error_info)
            logger.error(f"Error in publication node {self.name}: {str(e)}")
            return False


class MetronomeNode(Node):
    """Node that executes at regular intervals"""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.interval = config.get('interval', 1)
        self.subscriber = None
        self.publishers = []
        self._last_execution = 0
        self._metronome_thread = None
        self._stop_event = threading.Event()

    def set_subscriber(self, subscriber):
        """Set the data subscriber"""
        self.subscriber = subscriber

    def add_publisher(self, publisher):
        """Add a data publisher"""
        self.publishers.append(publisher)

    def start_metronome(self):
        """Start metronome thread"""
        if not self._metronome_thread or not self._metronome_thread.is_alive():
            self._stop_event.clear()
            self._metronome_thread = threading.Thread(target=self._metronome_loop, daemon=True)
            self._metronome_thread.start()
            logger.info(f"Metronome node {self.name} started with interval {self.interval}s")

    def _metronome_loop(self):
        """Metronome execution loop"""
        while not self._stop_event.is_set():
            current_time = time.time()
            elapsed = current_time - self._last_execution

            if elapsed >= self.interval:
                # Time to tick - mark node as dirty
                self.set_dirty()
                self._last_execution = current_time
                logger.debug(f"Metronome node {self.name} tick at {current_time}")
                # Small sleep before next check
                time.sleep(0.1)
            else:
                # Sleep until next interval (or check every 0.1s, whichever is smaller)
                time_to_sleep = min(self.interval - elapsed, 0.1)
                self.set_clean()
                time.sleep(time_to_sleep)

    def pre_compute(self):
        """Check subscriber if available"""
        '''
        if self.subscriber and self.subscriber.get_queue_size() > 0:
            self.set_dirty()
        '''
        pass

    def compute(self) -> bool:
        """Execute calculation and publish"""
        if not self.isdirty():
            return False

        try:
            # If subscriber is available, get data from it
            if self.subscriber:
                data = self.subscriber.get_data(block_time=0)
                if data:
                    self._input = data

            # Add tick counter to ensure output changes
            if not self._input:
                self._input = {}
            self._input['metronome_tick'] = self._compute_count  # Add this line

            # Run calculation
            if self._calculator:
                self._output = self._calculator.calculate(self._input)
            else:
                self._output = self._input


            # Publish if publishers are available
            for publisher in self.publishers:
                try:
                    publisher.publish(self._output)
                except Exception as e:
                    logger.error(f"Error publishing from metronome node {self.name}: {str(e)}")

            # Mark children as dirty
            for edge in self._outgoing_edges:
                edge.to_node.set_dirty()
                logger.info(f'node: {self.name} setting child {edge.to_node.name} as dirty')

            # Mark this node as clean (not dirty) - it will be marked dirty again by metronome
            self.set_clean()
            #self.increment_compute_count()
            return True
        except Exception as e:
            error_info = {
                'time': datetime.now().isoformat(),
                'error': str(e)
            }
            self._errors.append(error_info)
            logger.error(f"Error in metronome node {self.name}: {str(e)}")
            return False

    def stop_metronome(self):
        """Stop metronome thread"""
        self._stop_event.set()
        if self._metronome_thread:
            self._metronome_thread.join(timeout=2)
        logger.info(f"Metronome node {self.name} stopped")


class CalculationNode(Node):
    """Node that performs calculations on input"""

    def __init__(self, name, config):
        super().__init__(name, config)

    # Uses default Node.compute() implementation


class SubgraphWrapperNode(Node):
    """
    Node that wraps a SubgraphNode for integration with the parent graph.
    
    This node acts as a bridge between the parent graph's node system and 
    the subgraph's internal execution. It handles:
    - Dirty propagation across subgraph boundaries
    - Input/output data marshaling
    - Subgraph state management (active/suspended)
    
    Patent Pending - DishtaYantra Framework
    """
    
    def __init__(self, name, config, subgraph_node):
        """
        Initialize the wrapper node.
        
        Args:
            name: Node name in parent graph
            config: Node configuration
            subgraph_node: The SubgraphNode to wrap
        """
        super().__init__(name, config)
        self.subgraph_node = subgraph_node
        self._node_type = 'SubgraphNode'
        
    @property
    def is_subgraph(self) -> bool:
        """Indicate this is a subgraph node."""
        return True
    
    @property
    def subgraph(self):
        """Get the underlying subgraph."""
        return self.subgraph_node.subgraph if self.subgraph_node else None
    
    @property
    def is_active(self) -> bool:
        """Check if subgraph is active."""
        return self.subgraph_node.is_active if self.subgraph_node else False
    
    @property
    def is_suspended(self) -> bool:
        """Check if subgraph is suspended."""
        return self.subgraph_node.is_suspended if self.subgraph_node else True
    
    def set_dirty(self):
        """Set this node as dirty and propagate to subgraph if active."""
        super().set_dirty()
        if self.subgraph_node and self.is_active:
            self.subgraph_node.mark_dirty()
    
    def pre_compute(self):
        """Pre-compute phase - check if any incoming edges have new data."""
        # Check incoming edges for data
        for edge in self._incoming_edges:
            if edge.from_node.isdirty() or self._input != edge.get_data():
                self.set_dirty()
                break
    
    def compute(self) -> bool:
        """
        Execute the subgraph and propagate output.
        
        If subgraph is suspended, cached output is used and downstream
        nodes are NOT marked dirty (preventing unnecessary recalculation).
        """
        if not self.isdirty():
            return False
        
        try:
            # Gather inputs from incoming edges
            inputs = {}
            for edge in self._incoming_edges:
                edge_data = edge.get_data()
                if edge_data is not None:
                    if edge.pname:
                        inputs[edge.pname] = edge_data
                    else:
                        inputs[edge.from_node.name] = edge_data
            
            # Merge into single input if only one source
            if len(inputs) == 1:
                input_data = list(inputs.values())[0]
            else:
                input_data = inputs
            
            self._input = input_data
            
            # Execute subgraph
            if self.subgraph_node:
                self._output = self.subgraph_node.execute(input_data)
            else:
                self._output = input_data
            
            # Propagate to outgoing edges
            for edge in self._outgoing_edges:
                edge.set_data(self._output)
                # Only mark downstream dirty if subgraph is active
                # When suspended, we use cached output but don't propagate dirty
                if self.is_active:
                    edge.to_node.set_dirty()
            
            self.set_clean()
            return True
            
        except Exception as e:
            error_info = {
                'time': datetime.now().isoformat(),
                'error': str(e),
                'subgraph': self.name
            }
            self._errors.append(error_info)
            logger.error(f"Error in subgraph node {self.name}: {str(e)}")
            return False
    
    def light_up(self, reason: str = None):
        """Activate the subgraph."""
        if self.subgraph_node:
            self.subgraph_node.light_up(reason)
            self.set_dirty()  # Trigger recalculation
    
    def light_down(self, reason: str = None):
        """Suspend the subgraph."""
        if self.subgraph_node:
            self.subgraph_node.light_down(reason)
    
    def details(self) -> dict:
        """Return detailed information about this node."""
        base_details = super().details()
        
        # Add subgraph-specific details
        base_details['node_type'] = 'SubgraphNode'
        base_details['is_subgraph'] = True
        
        if self.subgraph_node and self.subgraph:
            sg = self.subgraph
            base_details['subgraph'] = {
                'name': sg.name,
                'state': sg.state.value,
                'is_active': sg.is_active,
                'execution_mode': sg.execution_mode.value,
                'node_count': sg.node_count,
                'entry_node': sg.entry_node_name,
                'exit_node': sg.exit_node_name,
                'suspend_reason': sg.suspend_reason,
                'metrics': {
                    'execution_count': sg.metrics.execution_count,
                    'avg_latency_ms': sg.metrics.avg_execution_time_ms,
                    'last_latency_ms': sg.metrics.last_execution_time_ms,
                    'cache_hits': sg.metrics.cache_hit_count,
                    'errors': sg.metrics.error_count
                },
                'internal_nodes': [
                    {
                        'name': node.name,
                        'borrows_from': node.borrows_from,
                        'role': node.role,
                        'is_dirty': node.is_dirty,
                        'last_execution_ms': node.last_execution_time_ms
                    }
                    for node in sg.nodes.values()
                ]
            }
        
        return base_details