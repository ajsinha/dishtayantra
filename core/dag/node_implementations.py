import logging
import threading
import time
from datetime import datetime
from core.dag.graph_elements import Node

logger = logging.getLogger(__name__)


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

    def compute(self):
        """Pull data from subscriber and compute"""
        if not self._isdirty or not self.subscriber:
            return

        try:
            # Get data from subscriber
            data = self.subscriber.get_data(block_time=0)

            if data is None:
                return

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

            self._isdirty = False
            self._last_compute = datetime.now().isoformat()
            self._compute_count += 1

        except Exception as e:
            error_info = {
                'time': datetime.now().isoformat(),
                'error': str(e)
            }
            self._errors.append(error_info)
            logger.error(f"Error in subscription node {self.name}: {str(e)}")


class PublicationNode(Node):
    """Node that publishes output to DataPublishers"""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.publishers = []
        self._last_published_output = None

    def add_publisher(self, publisher):
        """Add a data publisher"""
        self.publishers.append(publisher)

    def compute(self):
        """Compute and publish if output changed"""
        if not self._isdirty:
            return

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

        except Exception as e:
            error_info = {
                'time': datetime.now().isoformat(),
                'error': str(e)
            }
            self._errors.append(error_info)
            logger.error(f"Error in publication node {self.name}: {str(e)}")


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

    def _metronome_loop(self):
        """Metronome execution loop"""
        while not self._stop_event.is_set():
            current_time = time.time()

            if current_time - self._last_execution >= self.interval:
                self.set_dirty()
                self._last_execution = current_time

            time.sleep(0.1)

    def pre_compute(self):
        """Check subscriber if available"""
        if self.subscriber and self.subscriber.get_queue_size() > 0:
            self.set_dirty()

    def compute(self):
        """Execute calculation and publish"""
        if not self._isdirty:
            return

        try:
            # If subscriber is available, get data from it
            if self.subscriber:
                data = self.subscriber.get_data(block_time=0)
                if data:
                    self._input = data

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

            self._isdirty = False
            self._last_compute = datetime.now().isoformat()
            self._compute_count += 1

        except Exception as e:
            error_info = {
                'time': datetime.now().isoformat(),
                'error': str(e)
            }
            self._errors.append(error_info)
            logger.error(f"Error in metronome node {self.name}: {str(e)}")

    def stop_metronome(self):
        """Stop metronome thread"""
        self._stop_event.set()
        if self._metronome_thread:
            self._metronome_thread.join(timeout=2)


class CalculationNode(Node):
    """Node that performs calculations on input"""

    def __init__(self, name, config):
        super().__init__(name, config)

    # Uses default Node.compute() implementation