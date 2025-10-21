import copy
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from collections import deque

logger = logging.getLogger(__name__)


class Node(ABC):
    """Abstract base class for nodes in compute graph"""

    def __init__(self, name, config):
        self.name = name
        self.config = config
        self._input = {}
        self._output = {}
        self._isdirty = True
        self._calculator = None
        self._input_transformers = []
        self._output_transformers = []
        self._last_compute = None
        self._compute_count = 0
        self._errors = deque(maxlen=10)
        self._outgoing_edges = []
        self._incoming_edges = []
        self._graph = None

    def input(self):
        """Return copy of input data"""
        return copy.deepcopy(self._input)

    def output(self):
        """Return copy of output data"""
        return copy.deepcopy(self._output)

    def isdirty(self):
        """Return dirty status"""
        return self._isdirty

    def set_graph(self, g):
        self._graph = g


    def set_dirty(self):
        """Mark node as dirty"""
        self._isdirty = True

    def set_calculator(self, calculator):
        """Set the calculator for this node"""
        self._calculator = calculator

    def add_input_transformer(self, transformer):
        """Add input transformer"""
        self._input_transformers.append(transformer)

    def add_output_transformer(self, transformer):
        """Add output transformer"""
        self._output_transformers.append(transformer)

    def add_outgoing_edge(self, edge):
        """Add outgoing edge"""
        self._outgoing_edges.append(edge)

    def add_incoming_edge(self, edge):
        """Add incoming edge"""
        self._incoming_edges.append(edge)

    def pre_compute(self):
        """Pre-compute hook - can be overridden by subclasses"""
        pass

    def consolidate_edge_inputs(self):
        # Gather inputs from incoming edges
        merged_input = {}
        for edge in self._incoming_edges:
            edge_data = edge.get_data()
            edge_pname = edge.pname
            if edge_pname is None:
                edge_pname = ''
            edge_pname = edge_pname.strip()
            if edge_data:
                if len(edge_pname) == 0:
                    merged_input.update(edge_data)
                else:
                    if edge_pname not in merged_input.keys():
                        merged_input[edge_pname] = {}
                    merged_input[edge_pname].update(edge_data)
        return merged_input

    def compute(self):
        """Compute node output based on inputs"""
        if not self._isdirty:
            return

        try:
            # Gather inputs from incoming edges
            merged_input = self.consolidate_edge_inputs()

            # Apply input transformers
            transformed_input = merged_input
            for transformer in self._input_transformers:
                transformed_input = transformer.transform(transformed_input)

            # Check if input has changed
            if transformed_input == self._input:
                self._isdirty = False
                return

            self._input = copy.deepcopy(transformed_input)

            # Calculate if calculator is available
            if self._calculator:
                calculated_output = self._calculator.calculate(self._input)
            else:
                calculated_output = self._input

            # Apply output transformers
            transformed_output = calculated_output
            for transformer in self._output_transformers:
                transformed_output = transformer.transform(transformed_output)

            # Check if output has changed
            if transformed_output != self._output:
                self._output = copy.deepcopy(transformed_output)

                # Mark children as dirty
                for edge in self._outgoing_edges:
                    edge.to_node.set_dirty()

            self._isdirty = False
            self._last_compute = datetime.now().isoformat()
            self._compute_count += 1

        except Exception as e:
            error_info = {
                'time': datetime.now().isoformat(),
                'error': str(e)
            }
            self._errors.append(error_info)
            logger.error(f"Error in node {self.name}: {str(e)}")

    def post_compute(self):
        """Post-compute hook - can be overridden by subclasses"""
        pass

    def details(self):
        """Return node details in JSON format"""
        return {
            'name': self.name,
            'type': self.__class__.__name__,
            'isdirty': self._isdirty,
            'last_compute': self._last_compute,
            'compute_count': self._compute_count,
            'input': self._input,
            'output': self._output,
            'errors': list(self._errors),
            'incoming_edges': [e.name for e in self._incoming_edges],
            'outgoing_edges': [e.name for e in self._outgoing_edges]
        }


class Edge:
    """Edge connecting two nodes in compute graph"""

    def __init__(self, from_node, to_node, data_transformer=None, pname =None):
        self.from_node = from_node
        self.to_node = to_node
        self.data_transformer = data_transformer

        self.name = f"{from_node.name}_to_{to_node.name}"
        self.pname = ""
        if pname is not None:
            self.pname = pname.strip()  ##pseudoname associated with an edge


        # Register edge with nodes
        from_node.add_outgoing_edge(self)
        to_node.add_incoming_edge(self)

        logger.debug(f"Created edge: {self.name}")

    def get_data(self):
        """Get data from source node through transformer"""
        output = self.from_node.output()

        if self.data_transformer:
            return self.data_transformer.transform(output)

        return output

    def details(self):
        """Return edge details in JSON format"""
        return {
            'name': self.name,
            'from_node': self.from_node.name,
            'to_node': self.to_node.name,
            'transformer': self.data_transformer.name if self.data_transformer else None
        }