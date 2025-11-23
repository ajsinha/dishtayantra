import json
from abc import ABC, abstractmethod
import threading
import queue
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DataAwarePayload():
    """Class that carries a payload - typically a dictionary - along with destination i.e. queue or topic."""
    def __init__(self, destination, cde, payload):
        self.destination = destination
        self.cde = cde
        self.payload = payload

    def add_to_cde(self, k, v):
        self.cde[k] = v

    def to_dict(self):
        return {"destination": self.destination, 'cde': self.cde, 'payload': self.payload}

    def get_data_for_publication(self):
        if self.destination is None or len(self.destination) ==0:
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
        local_dict = {'destination': self.destination,'cde': self.cde, 'payload': self.payload}
        return json.dumps(local_dict)

class DataPublisher(ABC):
    """Abstract base class for data publishers"""

    def __init__(self, name, destination, config):
        self.name = name
        self.destination = destination
        self.config = config
        self.publish_interval = config.get('publish_interval', 0)
        self.batch_size = config.get('batch_size', None)
        self._publish_queue = queue.Queue() if self.publish_interval > 0 else None
        self._publish_thread = None
        self._stop_event = threading.Event()
        self._last_publish = None
        self._publish_count = 0
        self._lock = threading.Lock()

        if self.publish_interval > 0:
            self._start_periodic_publisher()

        logger.info(f"DataPublisher {name} initialized for destination {destination}")

    def is_composite(self):
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
        """Flush accumulated messages"""
        messages = []
        while not self._publish_queue.empty():
            try:
                messages.append(self._publish_queue.get_nowait())
            except queue.Empty:
                break

        for msg in messages:
            try:
                self._do_publish(msg)
            except Exception as e:
                logger.error(f"Error publishing message in {self.name}: {str(e)}")

    def publish(self, data):
        """Publish data to destination"""
        if self.publish_interval > 0:
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


class DataSubscriber(ABC):
    """Abstract base class for data subscribers"""

    def __init__(self, name, source, config, given_internal_queue: queue.Queue = None):
        self.name = name
        self.source = source
        self.config = config
        self.max_depth = config.get('max_depth', 100000)
        self._internal_queue = given_internal_queue
        if given_internal_queue is None:
            self._internal_queue = queue.Queue(maxsize=self.max_depth)

        self._subscriber_thread = None
        self._stop_event = threading.Event()
        self._suspend_event = threading.Event()
        self._suspend_event.set()  # Start in running state
        self._last_receive = None
        self._receive_count = 0
        self._lock = threading.Lock()

        logger.info(f"DataSubscriber {name} initialized for source {source}")

    def set_internal_queue(self, given_queue):
        self._internal_queue = given_queue

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
        """Main subscription loop"""
        while not self._stop_event.is_set():
            self._suspend_event.wait()  # Block if suspended

            if self._stop_event.is_set():
                break

            try:
                data = self._do_subscribe()
                if data is not None:
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
        """Get data from internal queue"""
        if block_time is None:
            try:
                return self._internal_queue.get_nowait()
            except queue.Empty:
                return None
        elif block_time == -1:
            return self._internal_queue.get(block=True)
        else:
            try:
                return self._internal_queue.get(block=True, timeout=block_time)
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
                'suspended': not self._suspend_event.is_set()
            }

    def stop(self):
        """Stop the subscriber"""
        self._stop_event.set()
        self._suspend_event.set()  # Unblock if suspended
        if self._subscriber_thread:
            self._subscriber_thread.join(timeout=5)
        logger.info(f"Subscriber {self.name} stopped")
