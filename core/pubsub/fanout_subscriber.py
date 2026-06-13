"""
FanoutDataSubscriber (v2.2 module split)
========================================

The routing subscriber - one source subscriber fanned out to child
subscribers by routing key - extracted verbatim from fanout_datapubsub.py
to respect the 500-line architecture limit. Re-exported from
core.pubsub.fanout_datapubsub.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import queue
import importlib
from copy import deepcopy
from datetime import datetime
from typing import Dict, Any, Protocol, runtime_checkable

from core.pubsub.datapubsub import DataSubscriber, DataPublisher, DataAwarePayload

logger = logging.getLogger(__name__)


from core.pubsub.fanout_datapubsub import RoutingKeyResolverLike
from core.pubsub.fanout_subscriber_stats import FanoutSubscriberStatsMixin

class FanoutDataSubscriber(FanoutSubscriberStatsMixin, DataSubscriber):
    """
    Routes messages from a source subscriber to multiple child subscribers based on a routing strategy.

    This implements a fan-out pattern with intelligent routing:
    - Receives messages from a single source subscriber
    - Uses a pluggable resolver to determine routing destination
    - Pushes messages to appropriate child subscriber's internal queue
    - Logs unrouted messages to a file for later analysis

    The resolver class must implement a resolve(message_dict) -> str method that returns
    the routing key to identify which child subscriber should receive the message.
    """

    def __init__(self, name, config):
        """
        Initialize FanoutDataSubscriber

        Args:
            name: Name of the router subscriber

            config: Configuration dictionary containing:
                - 'child_subscribers': Dict mapping routing keys to DataSubscriber instances
                - 'resolver_class': Full module path to resolver class (e.g., 'my.module.MyResolver')
                - 'unrouted_file': Path to file for unrouted messages (optional, default: 'unrouted_messages.jsonl')
                - 'max_depth': Maximum queue depth for router's own queue (default: 100000)
                - 'auto_start_children': Whether to auto-start child subscribers (default: True)
                - Any other config params

        Raises:
            ValueError: If resolver_class is not specified in config
        """

        # Initialize parent class with a dummy source since we're using source_subscriber
        super().__init__(name, f"router://{name}", config)
        self.source_subscriber_name = config.get('source_subscriber')
        self.source_subscriber = None
        self.child_subscriber_configs = config.get('child_subscribers', {}) # this map will be key -> name
        self.child_subscribers = {}
        self.unrouted_file = config.get('unrouted_file', f'unrouted_messages_{name}.jsonl')
        self.auto_start_children = config.get('auto_start_children', True)

        # Load and instantiate the resolver
        resolver_class_path = config.get('resolver_class')
        if not resolver_class_path:
            raise ValueError("resolver_class must be specified in config")

        self.resolver = self._load_resolver(resolver_class_path)

        # Statistics tracking
        self._routed_count = {}  # key -> count
        self._unrouted_count = 0
        self._error_count = 0

    def is_message_router(self):
        return True

    def _get_source_subscriber_name(self):
        return self.source_subscriber_name

    def _get_child_subscription_config(self):
        return deepcopy(self.child_subscriber_configs)

    def _set_subscriptions(self, source_subscriber: DataSubscriber) -> Dict[str, DataSubscriber]:
        from core.pubsub.holder_datapubsub import create_holder_subscriber
        child_subscription_objects: Dict[str, DataSubscriber] = {}
        if not isinstance(source_subscriber, DataSubscriber):
            raise TypeError(f"Expected DataSubscriber instance, got {type(source_subscriber)}")

        self.source_subscriber = source_subscriber

        # Validate child subscribers
        for key, child_name in self.child_subscriber_configs.items():
            child = create_holder_subscriber(child_name)
            child_subscription_objects[child_name] = child
            if not isinstance(child, DataSubscriber):
                raise TypeError(f"Child subscriber '{key}' is not a DataSubscriber instance")
            self.add_child_subscriber(key, child)

        logger.info(
            f"FanoutDataSubscriber '{self.name}' initialized with {len(self.child_subscribers)} child subscribers")
        return child_subscription_objects

    def _load_resolver(self, class_path):
        """
        Dynamically load and instantiate the resolver class

        Args:
            class_path: Full module path to resolver class (e.g., 'my.module.MyResolver')

        Returns:
            Instance of the resolver class

        Raises:
            ImportError: If module cannot be imported
            AttributeError: If class doesn't exist in module
        """
        try:
            module_path, class_name = class_path.rsplit('.', 1)
            module = importlib.import_module(module_path)
            resolver_class = getattr(module, class_name)
            resolver_instance = resolver_class()

            # Validate that resolver has the required resolve method
            if not hasattr(resolver_instance, 'resolve'):
                raise AttributeError(f"Resolver class {class_name} must have a 'resolve' method")

            logger.info(f"Loaded resolver: {class_path}")
            return resolver_instance

        except Exception as e:
            logger.error(f"Error loading resolver class '{class_path}': {str(e)}")
            raise

    def add_child_subscriber(self, routing_key, subscriber):
        """
        Add a child subscriber for a specific routing key

        Args:
            routing_key: The key that will be used to route messages to this subscriber
            subscriber: DataSubscriber instance

        Raises:
            TypeError: If subscriber is not a DataSubscriber instance
        """
        if not isinstance(subscriber, DataSubscriber):
            raise TypeError(f"Expected DataSubscriber instance, got {type(subscriber)}")

        self.child_subscribers[routing_key] = subscriber
        self._routed_count[routing_key] = 0  # Initialize counter

        logger.info(
            f"Added child subscriber '{subscriber.name}' with routing key '{routing_key}' to router '{self.name}'")

    def remove_child_subscriber(self, routing_key):
        """
        Remove a child subscriber by routing key

        Args:
            routing_key: The routing key of the subscriber to remove

        Returns:
            The removed subscriber instance, or None if not found
        """
        subscriber = self.child_subscribers.pop(routing_key, None)
        if subscriber:
            logger.info(f"Removed child subscriber with routing key '{routing_key}' from router '{self.name}'")
        else:
            logger.warning(f"No child subscriber found for routing key '{routing_key}' in router '{self.name}'")
        return subscriber

    def get_child_subscriber(self, routing_key):
        """
        Get a child subscriber by routing key

        Args:
            routing_key: The routing key

        Returns:
            DataSubscriber instance or None if not found
        """
        return self.child_subscribers.get(routing_key)

    def list_routing_keys(self):
        """
        Get list of all configured routing keys

        Returns:
            List of routing keys
        """
        return list(self.child_subscribers.keys())

    def start(self):
        """
        Start the router and optionally all child subscribers

        This will:
        1. Start the source subscriber if not already running
        2. Start the router's own subscription loop
        3. Optionally start all child subscribers (if auto_start_children is True)
        """
        # Start the source subscriber
        try:
            self.source_subscriber.start()
            logger.info(f"Started source subscriber for router '{self.name}'")
        except Exception as e:
            logger.error(f"Error starting source subscriber: {str(e)}")

        # Start the router's own subscription loop
        super().start()

        # Optionally start child subscribers
        if self.auto_start_children and self.child_subscribers:
            started_count = 0
            for routing_key, subscriber in self.child_subscribers.items():
                try:
                    subscriber.start()
                    started_count += 1
                except Exception as e:
                    logger.error(f"Error starting child subscriber '{routing_key}': {str(e)}")

            logger.info(f"Router '{self.name}' started {started_count}/{len(self.child_subscribers)} child subscribers")

    def _subscription_loop(self):
        """
        Main subscription loop that routes messages from source to children

        This overrides the parent's subscription loop to implement routing logic:
        1. Get message from source subscriber
        2. Resolve routing key using the resolver
        3. Route to appropriate child subscriber or log as unrouted
        """
        while not self._stop_event.is_set():
            self._suspend_event.wait()  # Block if suspended

            if self._stop_event.is_set():
                break

            try:
                # Get message from source subscriber
                data = self.source_subscriber.get_data(block_time=1)

                if data is not None:
                    # Route the message
                    self._route_message(data)

                    # Update router's own statistics
                    with self._lock:
                        self._last_receive = datetime.now().isoformat()
                        self._receive_count += 1
                else:
                    # No data available, short sleep
                    import time
                    time.sleep(0.1)
                    continue

            except Exception as e:
                logger.error(f"Error in routing loop for '{self.name}': {str(e)}")
                with self._lock:
                    self._error_count += 1
                # Sleep to avoid tight loop on errors
                import time
                time.sleep(1)

    def _route_message(self, data):
        """
        Route message to appropriate child subscriber based on resolver

        Args:
            data: The message data to route (can be DataAwarePayload, dict, or other)
        """
        try:
            # Convert data to dict for resolver
            message_dict = self._convert_to_dict(data)

            # Resolve the routing key
            routing_key = self.resolver.resolve(message_dict)

            if routing_key is None or routing_key == '':
                logger.warning(f"Resolver returned None or empty routing key for message in '{self.name}'")
                self._write_unrouted_message('null_key', data)
                return

            # Find child subscriber for this key
            if routing_key in self.child_subscribers:
                child = self.child_subscribers[routing_key]

                # v3.0.0 ZERO-LOSS: never drop on a full child queue. Block-retry
                # until the child has space (backpressures the upstream feed).
                enqueued = False
                while not enqueued and not self._stop_event.is_set():
                    try:
                        child._internal_queue.put(data, timeout=1)
                        enqueued = True
                    except queue.Full:
                        logger.warning(
                            f"Child subscriber '{routing_key}' queue full in "
                            f"router '{self.name}'; applying backpressure "
                            f"(will retry, not drop)")
                        continue

                if enqueued:
                    # Update routing statistics
                    with self._lock:
                        self._routed_count[routing_key] = self._routed_count.get(routing_key, 0) + 1
                    # Wake the child's event-driven consumer if present.
                    cb = getattr(child, '_notify_callback', None)
                    if cb is not None:
                        try:
                            cb()
                        except Exception:
                            pass
                    logger.debug(f"Routed message to '{routing_key}' in router '{self.name}'")

            else:
                # No child subscriber for this key - write to unrouted file
                logger.debug(f"No child subscriber for routing key '{routing_key}', logging to file")
                self._write_unrouted_message(routing_key, data)

        except Exception as e:
            logger.error(f"Error routing message in '{self.name}': {str(e)}")
            with self._lock:
                self._error_count += 1
            # Try to save the message even if routing failed
            self._write_unrouted_message('error', data)

    def _convert_to_dict(self, data):
        """
        Convert data to dictionary format for resolver

        Args:
            data: The data to convert (DataAwarePayload, dict, or other)

        Returns:
            Dictionary representation of the data
        """
        if isinstance(data, DataAwarePayload):
            return data.to_dict()
        elif isinstance(data, dict):
            return data
        else:
            # Wrap non-dict data
            return {'data': data}

    def _write_unrouted_message(self, routing_key, data):
        """
        Write unrouted message to file for later analysis

        Args:
            routing_key: The routing key that was resolved (or 'error'/'null_key')
            data: The message data that couldn't be routed
        """
        try:
            unrouted_entry = {
                'timestamp': datetime.now().isoformat(),
                'router_name': self.name,
                'routing_key': routing_key,
                'available_keys': list(self.child_subscribers.keys()),
                'data': self._convert_to_dict(data)
            }

            # Append to JSONL file (one JSON object per line)
            with open(self.unrouted_file, 'a') as f:
                f.write(json.dumps(unrouted_entry) + '\n')

            with self._lock:
                self._unrouted_count += 1

            logger.debug(f"Unrouted message with key '{routing_key}' written to {self.unrouted_file}")

        except Exception as e:
            logger.error(f"Error writing unrouted message to file: {str(e)}")

    def _do_subscribe(self):
        """
        Not used in router pattern as _subscription_loop is overridden

        This method is required by the abstract base class but is not used
        in the router pattern. The router's subscription loop directly gets
        data from the source subscriber.

        Returns:
            None
        """
        return None

    def suspend(self):
        """
        Suspend the router and all child subscribers
        """
        super().suspend()

        suspended_count = 0
        for routing_key, subscriber in self.child_subscribers.items():
            try:
                subscriber.suspend()
                suspended_count += 1
            except Exception as e:
                logger.error(f"Error suspending child subscriber '{routing_key}': {str(e)}")

        logger.info(
            f"Router '{self.name}' suspended (including {suspended_count}/{len(self.child_subscribers)} children)")

    def resume(self):
        """
        Resume the router and all child subscribers
        """
        super().resume()

        resumed_count = 0
        for routing_key, subscriber in self.child_subscribers.items():
            try:
                subscriber.resume()
                resumed_count += 1
            except Exception as e:
                logger.error(f"Error resuming child subscriber '{routing_key}': {str(e)}")

        logger.info(f"Router '{self.name}' resumed (including {resumed_count}/{len(self.child_subscribers)} children)")

    def stop(self):
        """
        Stop the router, source subscriber, and all child subscribers
        """
        # Stop the router's own subscription loop
        super().stop()

        # Stop source subscriber
        try:
            self.source_subscriber.stop()
            logger.info(f"Stopped source subscriber for router '{self.name}'")
        except Exception as e:
            logger.error(f"Error stopping source subscriber: {str(e)}")

        # Stop child subscribers
        stopped_count = 0
        for routing_key, subscriber in self.child_subscribers.items():
            try:
                subscriber.stop()
                stopped_count += 1
            except Exception as e:
                logger.error(f"Error stopping child subscriber '{routing_key}': {str(e)}")

        logger.info(f"Router '{self.name}' stopped (including {stopped_count}/{len(self.child_subscribers)} children)")

