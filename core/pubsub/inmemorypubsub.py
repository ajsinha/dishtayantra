import threading
import queue
import logging
import json
from datetime import datetime
from typing import Dict, List

logger = logging.getLogger(__name__)


class InMemoryPubSub:
    """Singleton in-memory pub/sub implementation for queues and topics"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(InMemoryPubSub, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._queues = {}
        self._topics = {}
        self._queue_lock = threading.Lock()
        self._topic_lock = threading.Lock()
        self._queue_stats = {}
        self._topic_stats = {}
        self._initialized = True
        logger.info("InMemoryPubSub initialized")

    def create_queue(self, queue_name, max_size=100000):
        """Create a queue with specified name and max size"""
        with self._queue_lock:
            if queue_name not in self._queues:
                self._queues[queue_name] = queue.Queue(maxsize=max_size)
                self._queue_stats[queue_name] = {
                    'max_depth': max_size,
                    'created_at': datetime.now().isoformat(),
                    'last_publish': None,
                    'last_consume': None
                }
                logger.info(f"Created queue: {queue_name}")

    def create_topic(self, topic_name):
        """Create a topic with specified name"""
        with self._topic_lock:
            if topic_name not in self._topics:
                self._topics[topic_name] = []
                self._topic_stats[topic_name] = {
                    'created_at': datetime.now().isoformat(),
                    'last_publish': None,
                    'subscriber_count': 0
                }
                logger.info(f"Created topic: {topic_name}")

    def publish_to_queue(self, queue_name, message, block=True, timeout=None):
        """Publish message to a queue"""
        if queue_name not in self._queues:
            self.create_queue(queue_name)

        try:
            self._queues[queue_name].put(message, block=block, timeout=timeout)
            with self._queue_lock:
                self._queue_stats[queue_name]['last_publish'] = datetime.now().isoformat()
            logger.debug(f"Published to queue {queue_name}")
        except queue.Full:
            logger.warning(f"Queue {queue_name} is full")
            raise

    def consume_from_queue(self, queue_name, block=True, timeout=None):
        """Consume message from a queue"""
        if queue_name not in self._queues:
            return None

        try:
            message = self._queues[queue_name].get(block=block, timeout=timeout)
            with self._queue_lock:
                self._queue_stats[queue_name]['last_consume'] = datetime.now().isoformat()
            logger.debug(f"Consumed from queue {queue_name}")
            return message
        except queue.Empty:
            return None

    def get_queue_size(self, queue_name):
        """Get current size of queue"""
        if queue_name in self._queues:
            return self._queues[queue_name].qsize()
        return 0

    def publish_to_topic(self, topic_name, message):
        """Publish message to a topic (all subscribers)"""
        if topic_name not in self._topics:
            self.create_topic(topic_name)

        with self._topic_lock:
            subscribers = self._topics[topic_name].copy()
            self._topic_stats[topic_name]['last_publish'] = datetime.now().isoformat()

        for subscriber_queue in subscribers:
            try:
                subscriber_queue.put(message, block=False)
            except queue.Full:
                logger.warning(f"Subscriber queue for topic {topic_name} is full")

    def subscribe_to_topic(self, topic_name, max_size=100000):
        """Subscribe to a topic and return a queue for receiving messages"""
        if topic_name not in self._topics:
            self.create_topic(topic_name)

        subscriber_queue = queue.Queue(maxsize=max_size)
        with self._topic_lock:
            self._topics[topic_name].append(subscriber_queue)
            self._topic_stats[topic_name]['subscriber_count'] = len(self._topics[topic_name])

        logger.info(f"New subscriber to topic {topic_name}")
        return subscriber_queue

    def get_queue_details(self, queue_name):
        """Get details of a queue"""
        if queue_name not in self._queues:
            return None

        stats = self._queue_stats[queue_name].copy()
        stats['current_depth'] = self.get_queue_size(queue_name)
        return stats

    def get_topic_details(self, topic_name):
        """Get details of a topic"""
        if topic_name not in self._topics:
            return None

        return self._topic_stats[topic_name].copy()