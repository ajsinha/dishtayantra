"""
DataSubscriber (v2.0.0 module split)
====================================

The threaded subscriber base class (poll loop, internal queue, priority
handling, lifecycle), extracted verbatim from datapubsub.py. Re-exported
from core.pubsub.datapubsub, so existing imports keep working.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
from core.version import VERSION
from abc import ABC, abstractmethod
import threading
import queue
import time
import logging
from core.metrics.rate_meter import RateMeter
import traceback
from datetime import datetime
from typing import Any, Protocol, runtime_checkable, Union

from core import instantiate_from_full_name

logger = logging.getLogger(__name__)


from core.pubsub.pubsub_base import (
    AbstractDataPubSub,
    DataAwarePayload,
    DefaultPriorityExtractor,
    PriorityExtractor,
    smart_deserialize,
)

class DataSubscriber(AbstractDataPubSub):
    """
    Abstract base class for data subscribers.
    
    Enhanced in v1.5.2 with automatic packaging of non-dictionary messages.
    This ensures consistent data format for downstream calculators regardless
    of the original message format from external sources.
    
    Configuration Options:
        auto_package_non_dict (bool): Enable automatic packaging of non-dict messages (default: True)
        package_wrapper_key (str): Key name for original data in packaged dict (default: 'payload')
        add_package_metadata (bool): Add subscriber metadata to packaged messages (default: True)
        preserve_raw_on_error (bool): If packaging fails, preserve raw data (default: True)
    
    Example packaged message:
        {
            'payload': <original_data>,
            '_packaged': True,
            '_original_type': 'str',
            '_dag_priority': 5,
            '_metadata': {
                'subscriber_name': 'kafka_sub',
                'source': 'trades_topic',
                'received_at': '2025-01-13T10:30:00'
            }
        }
    """

    def __init__(self, name, source, config, given_internal_queue: queue.Queue = None):
        AbstractDataPubSub.__init__(self, name, config)

        self.source = source
        self.max_depth = config.get('max_depth', 100000)
        # v3.0.0: idle poll interval for the subscription loop. The old fixed
        # 0.1s sleep dominated ingress latency (~50-100ms). A smaller default
        # cuts that to single-digit ms; tune per source. Set higher to reduce
        # idle CPU on low-rate sources, lower for latency-critical feeds.
        self.idle_poll_interval = float(config.get('idle_poll_interval', 0.005))

        # v1.5.2: Non-dictionary message packaging configuration
        self.auto_package_non_dict = config.get('auto_package_non_dict', True)
        self.package_wrapper_key = config.get('package_wrapper_key', 'payload')
        self.add_package_metadata = config.get('add_package_metadata', True)
        self.preserve_raw_on_error = config.get('preserve_raw_on_error', True)
        self._packaged_count = 0  # Track how many messages were auto-packaged

        # Use PriorityQueue if not provided, otherwise use given queue
        # Note: If given_internal_queue is provided, it should be a PriorityQueue for priority support

        if given_internal_queue is None:
            if self.is_priority_queue():
                self._internal_queue = queue.PriorityQueue(maxsize=self.max_depth)
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
        # v3.1.0: live messages/minute throughput (EWMA, decays when idle).
        self._rate_meter = RateMeter()
        self._lock = threading.Lock()
        self._sequence_counter = 0  # For maintaining insertion order within same priority
        self._is_priority_queue = isinstance(self._internal_queue, queue.PriorityQueue)

        # v3.0.0: optional event-driven wakeup. A consumer (e.g. the owning
        # ComputeGraph compute loop) can register a zero-arg callback that is
        # invoked immediately after a message is enqueued, so the consumer can
        # wake without polling. Defaults to None => no behavioural change.
        self._notify_callback = None

        logger.info(
            f"DataSubscriber {name} initialized for source {source} "
            f"(Priority: {self._is_priority_queue}, Key: {self.priority_key}, "
            f"AutoPackage: {self.auto_package_non_dict})")


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

    def _package_message(self, data: Any) -> dict:
        """
        Package non-dictionary messages into a standardized dictionary format.
        
        This ensures:
        1. Consistent format for downstream calculators
        2. Priority extraction works with a known structure
        3. Metadata can be attached for debugging/tracing
        4. Original data type is preserved for proper parsing
        
        Args:
            data: Any type of incoming message (str, int, list, bytes, etc.)
            
        Returns:
            dict: Standardized message dictionary
            
        Examples:
            Input: "TRADE_001,BUY,100,AAPL"
            Output: {
                'payload': "TRADE_001,BUY,100,AAPL",
                '_packaged': True,
                '_original_type': 'str',
                '_dag_priority': 5,
                '_metadata': {...}
            }
            
            Input: [1, 2, 3, 4, 5]
            Output: {
                'payload': [1, 2, 3, 4, 5],
                '_packaged': True,
                '_original_type': 'list',
                '_dag_priority': 5,
                '_metadata': {...}
            }
        """
        # If already a dict, return as-is (backward compatible)
        if isinstance(data, dict):
            return data
        
        # If it's a DataAwarePayload, convert to dict
        if isinstance(data, DataAwarePayload):
            return data.to_dict()
        
        try:
            # Package non-dict into standardized format
            packaged = {
                self.package_wrapper_key: data,  # Original data under configured key
                '_packaged': True,               # Flag indicating auto-packaged
                '_original_type': type(data).__name__,  # Original type for parsing
                '_dag_priority': self.priority_extractor.default_priority,  # Default priority
            }
            
            # Add metadata if enabled
            if self.add_package_metadata:
                packaged['_metadata'] = {
                    'subscriber_name': self.name,
                    'source': self.source,
                    'received_at': datetime.now().isoformat(),
                    'packaging_version': VERSION
                }
            
            # Track packaging statistics
            with self._lock:
                self._packaged_count += 1
            
            logger.debug(f"Packaged {type(data).__name__} message in subscriber {self.name}")
            return packaged
            
        except Exception as e:
            logger.warning(f"Failed to package message in {self.name}: {e}")
            if self.preserve_raw_on_error:
                # Return minimal wrapper on error
                return {
                    self.package_wrapper_key: data,
                    '_packaged': True,
                    '_package_error': str(e)
                }
            raise

    def start(self):
        """Start the subscriber"""
        if not self._subscriber_thread or not self._subscriber_thread.is_alive():
            self._stop_event.clear()
            self._subscriber_thread = threading.Thread(target=self._subscription_loop, daemon=True)
            self._subscriber_thread.start()
            logger.info(f"Subscriber {self.name} started")

    def _subscription_loop(self):
        """
        Main subscription loop - handles both PriorityQueue and regular Queue.
        
        Enhanced in v1.5.2 to automatically package non-dictionary messages
        for consistent downstream processing.
        
        v1.7.2 Policy: Every message received is logged, every exception prints full stack trace.
        """
        while not self._stop_event.is_set():
            self._suspend_event.wait()  # Block if suspended

            if self._stop_event.is_set():
                break

            try:
                # Defensive check: verify queue type hasn't changed
                self._check_and_update_queue_type()

                data = self._do_subscribe()
                if data is not None:
                    # v1.7.2 Policy: Log every message received
                    self._log_message_received(data)
                    
                    # v1.5.2: Auto-package non-dictionary messages
                    if self.auto_package_non_dict:
                        data = self._package_message(data)
                    
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
                    self._rate_meter.mark()
                    # v3.0.0: wake any event-driven consumer immediately rather
                    # than making it poll. Best-effort: a failing callback must
                    # never break the subscription loop.
                    if self._notify_callback is not None:
                        try:
                            self._notify_callback()
                        except Exception as e:
                            logger.warning(
                                f"notify_callback failed for {self.name}: {e}")
                else:
                    time.sleep(self.idle_poll_interval)
            except queue.Full:
                # v3.0.0 ZERO-LOSS: never discard a message. The internal queue
                # is full (consumer not keeping up); block-retry the SAME
                # message until space frees up instead of dropping it. We hold
                # the already-deserialized `data` and re-attempt enqueue.
                logger.warning(f"Internal queue full for subscriber {self.name}; "
                               f"applying backpressure (will retry, not drop)")
                requeued = False
                while not self._stop_event.is_set() and not requeued:
                    try:
                        if self._is_priority_queue:
                            with self._lock:
                                sequence = self._sequence_counter
                                self._sequence_counter += 1
                            self._internal_queue.put((priority, sequence, data),
                                                     timeout=1)
                        else:
                            self._internal_queue.put(data, timeout=1)
                        requeued = True
                        with self._lock:
                            self._last_receive = datetime.now().isoformat()
                            self._receive_count += 1
                        self._rate_meter.mark()
                        if self._notify_callback is not None:
                            try:
                                self._notify_callback()
                            except Exception:
                                pass
                    except queue.Full:
                        continue  # keep applying backpressure until space frees
            except Exception as e:
                # v1.7.2 Policy: Full stack trace for all exceptions
                logger.error(f"Error in subscription loop for {self.name}: {str(e)}")
                logger.error(f"Full stack trace:\n{traceback.format_exc()}")
                time.sleep(1)

    def set_notify_callback(self, callback):
        """v3.0.0: register a zero-arg callback fired after each enqueue.

        Used for event-driven wakeup so a consumer does not have to poll.
        Pass None to clear.
        """
        self._notify_callback = callback

    def _log_message_received(self, data):
        """
        v1.7.2 Policy: Log every message received by any subscriber.
        
        This provides consistent visibility across all pubsub implementations
        (Kafka, ActiveMQ, RabbitMQ, TIBCO, Redis, etc.)
        """
        try:
            # Get a preview of the message (truncate large messages)
            if isinstance(data, dict):
                preview = json.dumps(data)[:500]
            elif isinstance(data, (str, bytes)):
                preview = str(data)[:500]
            else:
                preview = repr(data)[:500]
            
            logger.info(f"MESSAGE RECEIVED [{self.name}]:")
            logger.info(f"  Source: {self.source}")
            logger.info(f"  Type: {type(data).__name__}")
            logger.info(f"  Count: {self._receive_count + 1}")
            logger.info(f"  Preview: {preview}")
        except Exception as e:
            logger.warning(f"Could not log message for {self.name}: {e}")

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
        """Return details in JSON format including packaging statistics"""
        with self._lock:
            return {
                'name': self.name,
                'source': self.source,
                'max_depth': self.max_depth,
                'current_depth': self.get_queue_size(),
                'last_receive': self._last_receive,
                'receive_count': self._receive_count,
                'rate_per_minute': self._rate_meter.rate_per_minute(),
                'peak_per_minute': self._rate_meter.peak_per_minute(),
                'suspended': not self._suspend_event.is_set(),
                'priority_queue_enabled': self._is_priority_queue,
                'priority_key': self.priority_key,
                # v1.5.2: Packaging statistics
                'auto_package_enabled': self.auto_package_non_dict,
                'packaged_count': self._packaged_count,
                'package_wrapper_key': self.package_wrapper_key
            }

    def stop(self):
        """Stop the subscriber"""
        self._stop_event.set()
        self._suspend_event.set()  # Unblock if suspended
        if self._subscriber_thread:
            self._subscriber_thread.join(timeout=5)
        logger.info(f"Subscriber {self.name} stopped")