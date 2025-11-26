import json
from abc import ABC, abstractmethod
import threading
import queue
import time
import logging
from datetime import datetime
from typing import Any,Protocol, runtime_checkable

from core import instantiate_from_full_name

logger = logging.getLogger(__name__)

class PriorityExtractorLike(Protocol):
    def resolve(self, data: Any) -> int:
        ... # The '...' indicates an abstract/required method

class PriorityExtractor(ABC):

    def __init__(self):
        self.default_priority = 5
        self.priority_key='_dag_priority'

    @abstractmethod
    def resolve(self, data: Any) -> int:
        pass

class DefaultPriorityExtractor(PriorityExtractor):

    def __init__(self, priority_key='_dag_priority'):
        PriorityExtractor.__init__(self)
        self.priority_key = priority_key


    def resolve(self, data):
        """
        Extract priority from data.

        Rules:
        1. Only priorities 1, 2, 3, 4, 5 are allowed
        2. Default priority is 5
        3. Priority extraction order:
           a. If data is DataAwarePayload instance, use its dag_priority attribute
           b. If data is a dictionary and has the priority key, use that value
           c. Otherwise default to 5
        4. Validation rules (apply to all sources):
           - If value is <= 0 (zero or negative), default to 5
           - If value is > 5, cap to 5
           - If conversion fails, default to 5

        Args:
            data: The data to extract priority from (DataAwarePayload, dict, or other)
            priority_key: Name of the key to look for priority in dictionaries (default: '_dag_priority')

        Returns:
            int: Priority value between 1-5 (lower number = higher priority)
                 1 = highest priority, 5 = lowest priority
        """


        # Check if data is DataAwarePayload instance
        if isinstance(data, DataAwarePayload):
            try:
                priority = int(data.dag_priority)

                # Zero or negative values default to 5
                if priority <= 0:
                    logger.warning(
                        f"DataAwarePayload priority {priority} is zero or negative, using default: {self.default_priority}")
                    return self.default_priority

                # Values greater than 5 are capped to 5
                if priority > 5:
                    logger.warning(f"DataAwarePayload priority {priority} exceeds maximum, capping to: 5")
                    return 5

                # Valid priority (1-5)
                return priority

            except (ValueError, TypeError, AttributeError):
                logger.warning(f"Failed to extract priority from DataAwarePayload, using default: {self.default_priority}")
                return self.default_priority

        # Check if data is dictionary
        if not isinstance(data, dict):
            return self.default_priority

        if self.priority_key not in data:
            return self.default_priority

        try:
            priority = int(data[self.priority_key])

            # Zero or negative values default to 5
            if priority <= 0:
                logger.warning(f"Priority {priority} is zero or negative, using default: {self.default_priority}")
                return self.default_priority

            # Values greater than 5 are capped to 5
            if priority > 5:
                logger.warning(f"Priority {priority} exceeds maximum, capping to: 5")
                return 5

            # Valid priority (1-5)
            return priority

        except (ValueError, TypeError):
            logger.warning(f"Failed to convert {self.priority_key} to int, using default: {self.default_priority}")
            return self.default_priority


class DataAwarePayload:
    """Class that carries a payload - typically a dictionary - along with destination i.e. queue or topic."""

    def __init__(self, destination, cde, payload, dag_priority=5):
        self.destination = destination
        self.cde = cde
        self.payload = payload
        self.dag_priority = dag_priority  # Default priority is 5

    def add_to_cde(self, k, v):
        self.cde[k] = v

    def to_dict(self):
        return {"destination": self.destination, 'cde': self.cde, 'payload': self.payload,
                'dag_priority': self.dag_priority}

    def get_data_for_publication(self):
        if self.destination is None or len(self.destination) == 0:
            if self.cde is None or len(self.cde) == 0:
                return '', self.payload
            else:
                return '', {'cde': self.cde, 'payload': self.payload}
        else:
            if self.cde is None or len(self.cde) == 0:
                return self.destination, self.payload
            else:
                return self.destination, {'cde': self.cde, 'payload': self.payload}

    def __str__(self):
        local_dict = {'destination': self.destination, 'cde': self.cde, 'payload': self.payload,
                      'dag_priority': self.dag_priority}
        return json.dumps(local_dict)

class AbstractDataPubSub(ABC):

    def __init__(self, name, config: dict):
        self.name = name
        self.config = config

        # Get priority key name from config (defaults to '_dag_priority')
        self.queue_type = config.get('queue_type', 'fifo').lower()
        self.priority_key = config.get('dag_priority', '_dag_priority')
        priority_extractor_module = config.get('priority_extractor', 'core.pubsub.datapubsub.DefaultPriorityExtractor')
        self.priority_extractor: PriorityExtractorLike = instantiate_from_full_name(priority_extractor_module)
        if 'dag_priority_key' in self.config.keys():
            self.priority_key = config.get('dag_priority_key')
            self.priority_extractor.priority_key = self.priority_key
        else:
            self.priority_key = self.priority_extractor.priority_key

    def is_priority_queue(self):
        return self.queue_type == 'priority'

    def set_priority_extractor(self, extractor: PriorityExtractorLike):
            self.priority_extractor = extractor


class DataPublisher(AbstractDataPubSub):
    """Abstract base class for data publishers"""

    def __init__(self, name, destination, config):
        AbstractDataPubSub.__init__(self, name, config)

        self.destination = destination
        self.publish_interval = config.get('publish_interval', 0)
        self.batch_size = config.get('batch_size', None)


        # Use PriorityQueue instead of regular Queue
        if self.publish_interval > 0:
            if self.is_priority_queue():
                self._publish_queue = queue.PriorityQueue()
            else:
                self._publish_queue = queue.Queue()
        else:
            self._publish_queue = None


        self._max_queue_depth = config.get('max_queue_depth', 100000)

        self._publish_thread = None
        self._stop_event = threading.Event()
        self._last_publish = None
        self._publish_count = 0
        self._lock = threading.Lock()
        self._sequence_counter = 0  # For maintaining insertion order within same priority
        self._is_priority_queue = isinstance(self._publish_queue, queue.PriorityQueue)


        if self.publish_interval > 0:
            self._start_periodic_publisher()

        logger.info(
            f"DataPublisher {name} initialized for destination {destination} (Priority: {self._is_priority_queue}, Key: {self.priority_key})")

    def is_composite(self):
        return False

    def set_publish_queue(self, given_queue):
        """
        Set or replace the publish queue.
        Automatically detects if it's a PriorityQueue or regular Queue.

        Args:
            given_queue: Either queue.Queue or queue.PriorityQueue instance
        """
        with self._lock:
            self._publish_queue = given_queue
            self._is_priority_queue = isinstance(given_queue, queue.PriorityQueue)
            logger.info(f"DataPublisher {self.name} queue updated (Priority: {self._is_priority_queue})")

    def _check_and_update_queue_type(self):
        """
        Defensive check: Detect if queue type has changed externally.
        Updates internal flag accordingly.

        Returns:
            bool: True if queue type changed, False otherwise
        """
        if self._publish_queue is not None:
            current_type = isinstance(self._publish_queue, queue.PriorityQueue)
            if current_type != self._is_priority_queue:
                with self._lock:
                    self._is_priority_queue = current_type
                    logger.warning(
                        f"DataPublisher {self.name} queue type changed externally to {'Priority' if current_type else 'Regular'}")
                return True
        return False

    def _start_periodic_publisher(self):
        """Start periodic publisher thread"""
        self._publish_thread = threading.Thread(target=self._periodic_publish_loop, daemon=True)
        self._publish_thread.start()

    def _periodic_publish_loop(self):
        """Periodic publishing loop"""
        while not self._stop_event.is_set():
            time.sleep(self.publish_interval)
            self._flush_queue()

    def _flush_queue(self):
        """Flush accumulated messages - handles both PriorityQueue and regular Queue"""
        # Defensive check: verify queue type
        self._check_and_update_queue_type()

        messages = []
        while not self._publish_queue.empty():
            try:
                item = self._publish_queue.get_nowait()

                # If priority queue, item is (priority, sequence, data) tuple
                # If regular queue, item is just data
                if self._is_priority_queue and isinstance(item, tuple) and len(item) == 3:
                    _, _, msg = item  # Extract data from priority tuple
                else:
                    msg = item  # Regular queue or non-tuple item

                messages.append(msg)
            except queue.Empty:
                break

        for msg in messages:
            try:
                self._do_publish(msg)
            except Exception as e:
                logger.error(f"Error publishing message in {self.name}: {str(e)}")

    def publish(self, data):
        """
        Publish data to destination with priority support.

        Automatically adapts to queue type:
        - PriorityQueue: Extracts priority and uses priority-based ordering
        - Regular Queue: Uses FIFO ordering (classic behavior)

        Priority is extracted from data if it's a dictionary with the configured priority key.
        The priority key defaults to '_dag_priority' but can be configured via config['dag_priority'].
        Priority defaults to 5 if not specified.
        """
        if self.publish_interval > 0:
            # Defensive check: verify queue type hasn't changed
            self._check_and_update_queue_type()

            # Check if queue is full (respect max_depth)
            if self._publish_queue.qsize() >= self._max_queue_depth:
                logger.warning(f"Publish queue full for {self.name}, blocking until space available")
                # Block until space is available
                while self._publish_queue.qsize() >= self._max_queue_depth and not self._stop_event.is_set():
                    time.sleep(0.1)

            if self._is_priority_queue:
                # Priority queue behavior: extract priority and sequence
                priority = self.priority_extractor.resolve(data)

                # Use sequence counter to maintain insertion order within same priority
                # Lower values processed first, so we want FIFO within same priority
                with self._lock:
                    sequence = self._sequence_counter
                    self._sequence_counter += 1

                # Add to priority queue: (priority, sequence, data)
                # Python's PriorityQueue uses tuples for comparison
                self._publish_queue.put((priority, sequence, data))
            else:
                # Regular queue behavior: classic FIFO
                self._publish_queue.put(data)

            if self.batch_size and self._publish_queue.qsize() >= self.batch_size:
                self._flush_queue()
        else:
            self._do_publish(data)

    @abstractmethod
    def _do_publish(self, data):
        """Actual publish implementation"""
        pass

    def details(self):
        """Return details in JSON format"""
        with self._lock:
            return {
                'name': self.name,
                'destination': self.destination,
                'publish_interval': self.publish_interval,
                'batch_size': self.batch_size,
                'max_queue_depth': self._max_queue_depth,
                'priority_queue_enabled': self._is_priority_queue,
                'priority_key': self.priority_key,
                'last_publish': self._last_publish,
                'publish_count': self._publish_count,
                'queue_depth': self._publish_queue.qsize() if self._publish_queue else 0
            }

    def stop(self):
        """Stop the publisher"""
        if self._publish_thread:
            self._stop_event.set()
            self._flush_queue()
            self._publish_thread.join(timeout=5)



class DataSubscriber(AbstractDataPubSub):
    """Abstract base class for data subscribers"""

    def __init__(self, name, source, config, given_internal_queue: queue.Queue = None):
        AbstractDataPubSub.__init__(self, name, config)

        self.source = source
        self.max_depth = config.get('max_depth', 100000)

        # Use PriorityQueue if not provided, otherwise use given queue
        # Note: If given_internal_queue is provided, it should be a PriorityQueue for priority support

        if given_internal_queue is None:
            if self.is_priority_queue():
                self._internal_queue =queue.PriorityQueue(maxsize=self.max_depth)
            else:
                self._internal_queue = queue.Queue(maxsize=self.max_depth)
        else:
            self._internal_queue = given_internal_queue

        self._subscriber_thread = None
        self._stop_event = threading.Event()
        self._suspend_event = threading.Event()
        self._suspend_event.set()  # Start in running state
        self._last_receive = None
        self._receive_count = 0
        self._lock = threading.Lock()
        self._sequence_counter = 0  # For maintaining insertion order within same priority
        self._is_priority_queue = isinstance(self._internal_queue, queue.PriorityQueue)

        logger.info(
            f"DataSubscriber {name} initialized for source {source} (Priority: {self._is_priority_queue}, Key: {self.priority_key})")


    def set_internal_queue(self, given_queue):
        """
        Set internal queue - automatically detects PriorityQueue vs regular Queue.

        Args:
            given_queue: Either queue.Queue or queue.PriorityQueue instance
        """
        with self._lock:
            self._internal_queue = given_queue
            self._is_priority_queue = isinstance(given_queue, queue.PriorityQueue)
            logger.info(f"DataSubscriber {self.name} queue updated (Priority: {self._is_priority_queue})")

    def _check_and_update_queue_type(self):
        """
        Defensive check: Detect if queue type has changed externally.
        Updates internal flag accordingly.

        Returns:
            bool: True if queue type changed, False otherwise
        """
        if self._internal_queue is not None:
            current_type = isinstance(self._internal_queue, queue.PriorityQueue)
            if current_type != self._is_priority_queue:
                with self._lock:
                    self._is_priority_queue = current_type
                    logger.warning(
                        f"DataSubscriber {self.name} queue type changed externally to {'Priority' if current_type else 'Regular'}")
                return True
        return False

    def is_composite(self):
        return False

    def is_message_router(self):
        return False

    def start(self):
        """Start the subscriber"""
        if not self._subscriber_thread or not self._subscriber_thread.is_alive():
            self._stop_event.clear()
            self._subscriber_thread = threading.Thread(target=self._subscription_loop, daemon=True)
            self._subscriber_thread.start()
            logger.info(f"Subscriber {self.name} started")

    def _subscription_loop(self):
        """Main subscription loop - handles both PriorityQueue and regular Queue"""
        while not self._stop_event.is_set():
            self._suspend_event.wait()  # Block if suspended

            if self._stop_event.is_set():
                break

            try:
                # Defensive check: verify queue type hasn't changed
                self._check_and_update_queue_type()

                data = self._do_subscribe()
                if data is not None:
                    if self._is_priority_queue:
                        # Priority queue behavior: extract priority and add with sequence for FIFO within priority
                        priority = self.priority_extractor.resolve(data)
                        with self._lock:
                            sequence = self._sequence_counter
                            self._sequence_counter += 1

                        # Put with timeout to respect max_depth blocking behavior
                        self._internal_queue.put((priority, sequence, data), timeout=1)
                    else:
                        # Regular queue behavior: classic FIFO (backward compatibility)
                        self._internal_queue.put(data, timeout=1)

                    with self._lock:
                        self._last_receive = datetime.now().isoformat()
                        self._receive_count += 1
                else:
                    time.sleep(0.1)
            except queue.Full:
                logger.warning(f"Internal queue full for subscriber {self.name}")
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"Error in subscription loop for {self.name}: {str(e)}")
                time.sleep(1)

    @abstractmethod
    def _do_subscribe(self):
        """Actual subscribe implementation"""
        pass

    def get_data(self, block_time=None):
        """
        Get data from internal queue.

        Automatically adapts to queue type:
        - PriorityQueue: Unwraps (priority, sequence, data) tuple and returns data
        - Regular Queue: Returns data directly

        Returns just the data (not the priority tuple) to maintain backward compatibility.
        """
        # Defensive check: verify queue type hasn't changed
        self._check_and_update_queue_type()

        try:
            if block_time is None:
                item = self._internal_queue.get_nowait()
            elif block_time == -1:
                item = self._internal_queue.get(block=True)
            else:
                item = self._internal_queue.get(block=True, timeout=block_time)

            # If priority queue, extract data from tuple
            if self._is_priority_queue and isinstance(item, tuple) and len(item) == 3:
                _, _, data = item
                return data
            else:
                # Regular queue or non-tuple item
                return item

        except queue.Empty:
            return None

    def suspend(self):
        """Suspend subscription"""
        self._suspend_event.clear()
        logger.info(f"Subscriber {self.name} suspended")

    def resume(self):
        """Resume subscription"""
        self._suspend_event.set()
        logger.info(f"Subscriber {self.name} resumed")

    def get_queue_size(self):
        """Get current queue size"""
        return self._internal_queue.qsize()

    def details(self):
        """Return details in JSON format"""
        with self._lock:
            return {
                'name': self.name,
                'source': self.source,
                'max_depth': self.max_depth,
                'current_depth': self.get_queue_size(),
                'last_receive': self._last_receive,
                'receive_count': self._receive_count,
                'suspended': not self._suspend_event.is_set(),
                'priority_queue_enabled': self._is_priority_queue,
                'priority_key': self.priority_key
            }

    def stop(self):
        """Stop the subscriber"""
        self._stop_event.set()
        self._suspend_event.set()  # Unblock if suspended
        if self._subscriber_thread:
            self._subscriber_thread.join(timeout=5)
        logger.info(f"Subscriber {self.name} stopped")