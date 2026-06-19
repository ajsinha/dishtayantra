"""
DataPublisher and Pub/Sub Facade (v2.0.0 module split)
======================================================

The threaded publisher base class plus re-exports of the whole pub/sub
foundation. The base types live in pubsub_base.py and DataSubscriber in
datasubscriber.py - everything is re-exported here so the dozens of
existing `from core.pubsub.datapubsub import ...` statements keep working
unchanged.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
from abc import ABC, abstractmethod
import threading
import queue
import time
import logging
import traceback
from datetime import datetime
from typing import Any, Protocol, runtime_checkable, Union

from core import instantiate_from_full_name

logger = logging.getLogger(__name__)


from core.pubsub.pubsub_base import (  # noqa: F401
    AbstractDataPubSub,
    DataAwarePayload,
    DefaultPriorityExtractor,
    PriorityExtractor,
    PriorityExtractorLike,
    smart_deserialize,
)

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
        
        v1.7.2 Policy: Every publish is logged, every exception prints full stack trace.
        """
        try:
            # v1.7.2 Policy: Log every publish attempt
            self._log_publish_attempt(data)
            
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
        except Exception as e:
            # v1.7.2 Policy: Full stack trace for all exceptions
            logger.error(f"Error publishing to {self.name}: {str(e)}")
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            raise

    def _log_publish_attempt(self, data):
        """
        v1.7.2 Policy: Log every publish attempt by any publisher.
        
        This provides consistent visibility across all pubsub implementations.
        """
        # v5.14.0: per-message tracing is DEBUG-only (see DataSubscriber). At INFO
        # this returns immediately - no preview build, no log calls.
        if not logger.isEnabledFor(logging.DEBUG):
            return
        try:
            # Get a preview of the message (truncate large messages)
            if isinstance(data, dict):
                preview = json.dumps(data)[:500]
            elif isinstance(data, (str, bytes)):
                preview = str(data)[:500]
            else:
                preview = repr(data)[:500]
            
            with self._lock:
                count = self._publish_count + 1
            
            logger.debug(f"PUBLISH [{self.name}]:")
            logger.debug(f"  Destination: {self.destination}")
            logger.debug(f"  Type: {type(data).__name__}")
            logger.debug(f"  Count: {count}")
            logger.debug(f"  Preview: {preview}")
        except Exception as e:
            logger.warning(f"Could not log publish for {self.name}: {e}")

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





# v2.0.0 module split (imported AFTER DataPublisher so subclasses that
# import both names from this module resolve in one place).
from core.pubsub.datasubscriber import DataSubscriber  # noqa: E402,F401
