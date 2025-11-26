import json
import logging
import queue
import importlib
from copy import deepcopy
from datetime import datetime
from typing import Dict

from core.pubsub.datapubsub import DataSubscriber, DataPublisher, DataAwarePayload

logger = logging.getLogger(__name__)


class FanoutDataSubscriber(DataSubscriber):
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
        Initialize MessageRouterDataSubscriber

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
            f"MessageRouterDataSubscriber '{self.name}' initialized with {len(self.child_subscribers)} child subscribers")
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

                try:
                    # Push message to child's internal queue
                    child._internal_queue.put(data, timeout=1)

                    # Update routing statistics
                    with self._lock:
                        self._routed_count[routing_key] = self._routed_count.get(routing_key, 0) + 1

                    logger.debug(f"Routed message to '{routing_key}' in router '{self.name}'")

                except queue.Full:
                    logger.error(f"Child subscriber '{routing_key}' queue is full, dropping message")
                    with self._lock:
                        self._error_count += 1

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

    def details(self):
        """
        Return detailed information about the router and all child subscribers

        Returns:
            Dictionary containing router details and all child subscriber details
        """
        with self._lock:
            base_details = {
                'name': self.name,
                'type': 'message_router',
                'source': self.source.replace('router://', ''),
                'source_subscriber': self.source_subscriber.name,
                'resolver_class': f"{self.resolver.__class__.__module__}.{self.resolver.__class__.__name__}",
                'max_depth': self.max_depth,
                'current_depth': self.get_queue_size(),
                'last_receive': self._last_receive,
                'receive_count': self._receive_count,
                'routed_count': dict(self._routed_count),
                'unrouted_count': self._unrouted_count,
                'error_count': self._error_count,
                'unrouted_file': self.unrouted_file,
                'suspended': not self._suspend_event.is_set(),
                'child_count': len(self.child_subscribers),
                'routing_keys': list(self.child_subscribers.keys()),
                'children': {}
            }

            # Add details from each child subscriber
            for routing_key, subscriber in self.child_subscribers.items():
                try:
                    base_details['children'][routing_key] = subscriber.details()
                except Exception as e:
                    logger.error(f"Error getting details for child subscriber '{routing_key}': {str(e)}")
                    base_details['children'][routing_key] = {'error': str(e)}

            return base_details

    def get_routing_statistics(self):
        """
        Get statistics about message routing

        Returns:
            Dictionary with routing statistics
        """
        with self._lock:
            total_routed = sum(self._routed_count.values())

            stats = {
                'total_received': self._receive_count,
                'total_routed': total_routed,
                'total_unrouted': self._unrouted_count,
                'total_errors': self._error_count,
                'routing_efficiency': (total_routed / self._receive_count * 100) if self._receive_count > 0 else 0,
                'routes': {}
            }

            # Per-route statistics
            for routing_key, count in self._routed_count.items():
                stats['routes'][routing_key] = {
                    'count': count,
                    'percentage': (count / total_routed * 100) if total_routed > 0 else 0
                }

            return stats

    def get_child_summary(self):
        """
        Get summary of all child subscribers

        Returns:
            Dictionary with child subscriber summary
        """
        summary = {
            'total_children': len(self.child_subscribers),
            'children': {}
        }

        for routing_key, subscriber in self.child_subscribers.items():
            try:
                details = subscriber.details()
                summary['children'][routing_key] = {
                    'name': details.get('name'),
                    'source': details.get('source'),
                    'queue_depth': details.get('current_depth', 0),
                    'receive_count': details.get('receive_count', 0),
                    'suspended': details.get('suspended', False)
                }
            except Exception as e:
                logger.error(f"Error getting summary for child '{routing_key}': {str(e)}")
                summary['children'][routing_key] = {'error': str(e)}

        return summary

    def get_total_child_receive_count(self):
        """
        Get total receive count across all child subscribers

        Returns:
            Total number of messages received by all child subscribers
        """
        total = 0
        for subscriber in self.child_subscribers.values():
            try:
                details = subscriber.details()
                total += details.get('receive_count', 0)
            except Exception as e:
                logger.error(f"Error getting receive count: {str(e)}")
        return total

    def clear_unrouted_file(self):
        """
        Clear the unrouted messages file

        This can be useful for maintenance or after analyzing unrouted messages

        Returns:
            Number of lines that were in the file before clearing
        """
        try:
            with open(self.unrouted_file, 'r') as f:
                line_count = sum(1 for _ in f)

            # Clear the file
            open(self.unrouted_file, 'w').close()

            logger.info(f"Cleared {line_count} unrouted messages from {self.unrouted_file}")
            return line_count

        except FileNotFoundError:
            logger.info(f"Unrouted file {self.unrouted_file} does not exist")
            return 0
        except Exception as e:
            logger.error(f"Error clearing unrouted file: {str(e)}")
            return -1


class FanoutDataPublisher(DataPublisher):
    """
    Routes outgoing messages to different child publishers based on a routing strategy.

    This implements a fan-out pattern for publishing with intelligent routing:
    - Receives publish requests
    - Uses a pluggable resolver to determine routing destination
    - Publishes to appropriate child publisher
    - Logs unrouted messages to a file for later analysis

    The resolver class must implement a resolve(message_dict) -> str method that returns
    the routing key to identify which child publisher should handle the message.
    """

    def __init__(self, name, destination, config):
        """
        Initialize MessageRouterDataPublisher

        Args:
            name: Name of the router publisher
            destination: Destination identifier (typically 'router://{name}')
            config: Configuration dictionary containing:
                - 'child_publishers': Dict mapping routing keys to DataPublisher instances
                - 'resolver_class': Full module path to resolver class (e.g., 'my.module.MyResolver')
                - 'unrouted_file': Path to file for unrouted messages (optional, default: 'unrouted_pub_{name}.jsonl')
                - 'publish_interval': Interval for periodic publishing (optional, inherited from parent)
                - 'batch_size': Batch size for publishing (optional, inherited from parent)
                - Any other config params

        Raises:
            ValueError: If resolver_class is not specified in config
        """
        # Initialize parent class
        super().__init__(name, destination, config)

        self.child_publishers = config.get('child_publishers', {})
        self.unrouted_file = config.get('unrouted_file', f'unrouted_pub_{name}.jsonl')

        # Load and instantiate the resolver
        resolver_class_path = config.get('resolver_class')
        if not resolver_class_path:
            raise ValueError("resolver_class must be specified in config")

        self.resolver = self._load_resolver(resolver_class_path)

        # Statistics tracking
        self._routed_count = {}  # key -> count
        self._unrouted_count = 0
        self._error_count = 0

        # Validate child publishers
        for key, child in self.child_publishers.items():
            if not isinstance(child, DataPublisher):
                raise TypeError(f"Child publisher '{key}' is not a DataPublisher instance")

        logger.info(
            f"MessageRouterDataPublisher '{name}' initialized with {len(self.child_publishers)} child publishers")

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

    def is_message_router(self):
        """Indicate this is a composite-like publisher"""
        return True

    def add_child_publisher(self, routing_key, publisher):
        """
        Add a child publisher for a specific routing key

        Args:
            routing_key: The key that will be used to route messages to this publisher
            publisher: DataPublisher instance

        Raises:
            TypeError: If publisher is not a DataPublisher instance
        """
        if not isinstance(publisher, DataPublisher):
            raise TypeError(f"Expected DataPublisher instance, got {type(publisher)}")

        self.child_publishers[routing_key] = publisher
        self._routed_count[routing_key] = 0  # Initialize counter

        logger.info(
            f"Added child publisher '{publisher.name}' with routing key '{routing_key}' to router '{self.name}'")

    def remove_child_publisher(self, routing_key):
        """
        Remove a child publisher by routing key

        Args:
            routing_key: The routing key of the publisher to remove

        Returns:
            The removed publisher instance, or None if not found
        """
        publisher = self.child_publishers.pop(routing_key, None)
        if publisher:
            logger.info(f"Removed child publisher with routing key '{routing_key}' from router '{self.name}'")
        else:
            logger.warning(f"No child publisher found for routing key '{routing_key}' in router '{self.name}'")
        return publisher

    def get_child_publisher(self, routing_key):
        """
        Get a child publisher by routing key

        Args:
            routing_key: The routing key

        Returns:
            DataPublisher instance or None if not found
        """
        return self.child_publishers.get(routing_key)

    def list_routing_keys(self):
        """
        Get list of all configured routing keys

        Returns:
            List of routing keys
        """
        return list(self.child_publishers.keys())

    def _do_publish(self, data):
        """
        Route and publish message to appropriate child publisher

        This implements the core routing logic:
        1. Convert data to dict for resolver
        2. Resolve routing key using the resolver
        3. Publish to appropriate child publisher or log as unrouted

        Args:
            data: The message data to route and publish
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

            # Find child publisher for this key
            if routing_key in self.child_publishers:
                child = self.child_publishers[routing_key]

                try:
                    # Publish to child
                    child.publish(data)

                    # Update routing statistics
                    with self._lock:
                        self._routed_count[routing_key] = self._routed_count.get(routing_key, 0) + 1
                        self._last_publish = datetime.now().isoformat()

                    logger.debug(f"Routed message to '{routing_key}' in router '{self.name}'")

                except Exception as e:
                    logger.error(f"Error publishing to child publisher '{routing_key}': {str(e)}")
                    with self._lock:
                        self._error_count += 1
                    # Still write to unrouted file as the message wasn't successfully published
                    self._write_unrouted_message(f'{routing_key}_error', data)

            else:
                # No child publisher for this key - write to unrouted file
                logger.debug(f"No child publisher for routing key '{routing_key}', logging to file")
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
                'available_keys': list(self.child_publishers.keys()),
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

    def stop(self):
        """
        Stop the router and all child publishers
        """
        # Stop parent (flushes router's own queue if periodic publishing is enabled)
        super().stop()

        # Stop child publishers
        stopped_count = 0
        for routing_key, publisher in self.child_publishers.items():
            try:
                publisher.stop()
                stopped_count += 1
            except Exception as e:
                logger.error(f"Error stopping child publisher '{routing_key}': {str(e)}")

        logger.info(f"Router '{self.name}' stopped (including {stopped_count}/{len(self.child_publishers)} children)")

    def flush_all(self):
        """
        Flush all child publishers immediately

        This is useful when you want to ensure all pending messages
        are published before continuing.
        """
        flushed_count = 0
        for routing_key, publisher in self.child_publishers.items():
            try:
                # Check if publisher has a flush queue method
                if hasattr(publisher, '_flush_queue'):
                    publisher._flush_queue()
                    flushed_count += 1
            except Exception as e:
                logger.error(f"Error flushing child publisher '{routing_key}': {str(e)}")

        # Flush router's own queue if periodic publishing is enabled
        if self._publish_queue:
            self._flush_queue()

        logger.info(f"Flushed {flushed_count}/{len(self.child_publishers)} child publishers")

    def details(self):
        """
        Return detailed information about the router and all child publishers

        Returns:
            Dictionary containing router details and all child publisher details
        """
        with self._lock:
            base_details = {
                'name': self.name,
                'type': 'message_router_publisher',
                'destination': self.destination.replace('router://', ''),
                'resolver_class': f"{self.resolver.__class__.__module__}.{self.resolver.__class__.__name__}",
                'publish_interval': self.publish_interval,
                'batch_size': self.batch_size,
                'last_publish': self._last_publish,
                'publish_count': self._publish_count,
                'queue_depth': self._publish_queue.qsize() if self._publish_queue else 0,
                'routed_count': dict(self._routed_count),
                'unrouted_count': self._unrouted_count,
                'error_count': self._error_count,
                'unrouted_file': self.unrouted_file,
                'child_count': len(self.child_publishers),
                'routing_keys': list(self.child_publishers.keys()),
                'children': {}
            }

            # Add details from each child publisher
            for routing_key, publisher in self.child_publishers.items():
                try:
                    base_details['children'][routing_key] = publisher.details()
                except Exception as e:
                    logger.error(f"Error getting details for child publisher '{routing_key}': {str(e)}")
                    base_details['children'][routing_key] = {'error': str(e)}

            return base_details

    def get_routing_statistics(self):
        """
        Get statistics about message routing

        Returns:
            Dictionary with routing statistics
        """
        with self._lock:
            total_routed = sum(self._routed_count.values())
            total_attempted = total_routed + self._unrouted_count

            stats = {
                'total_published': self._publish_count,
                'total_routed': total_routed,
                'total_unrouted': self._unrouted_count,
                'total_errors': self._error_count,
                'routing_efficiency': (total_routed / total_attempted * 100) if total_attempted > 0 else 0,
                'routes': {}
            }

            # Per-route statistics
            for routing_key, count in self._routed_count.items():
                stats['routes'][routing_key] = {
                    'count': count,
                    'percentage': (count / total_routed * 100) if total_routed > 0 else 0
                }

            return stats

    def get_child_summary(self):
        """
        Get summary of all child publishers

        Returns:
            Dictionary with child publisher summary
        """
        summary = {
            'total_children': len(self.child_publishers),
            'children': {}
        }

        for routing_key, publisher in self.child_publishers.items():
            try:
                details = publisher.details()
                summary['children'][routing_key] = {
                    'name': details.get('name'),
                    'destination': details.get('destination'),
                    'queue_depth': details.get('queue_depth', 0),
                    'publish_count': details.get('publish_count', 0),
                    'last_publish': details.get('last_publish')
                }
            except Exception as e:
                logger.error(f"Error getting summary for child '{routing_key}': {str(e)}")
                summary['children'][routing_key] = {'error': str(e)}

        return summary

    def get_total_child_publish_count(self):
        """
        Get total publish count across all child publishers

        Returns:
            Total number of messages published by all child publishers
        """
        total = 0
        for publisher in self.child_publishers.values():
            try:
                details = publisher.details()
                total += details.get('publish_count', 0)
            except Exception as e:
                logger.error(f"Error getting publish count: {str(e)}")
        return total

    def get_total_queue_depth(self):
        """
        Get total queue depth across all child publishers

        Returns:
            Total queue depth of all child publishers
        """
        total = 0
        for publisher in self.child_publishers.values():
            try:
                details = publisher.details()
                total += details.get('queue_depth', 0)
            except Exception as e:
                logger.error(f"Error getting queue depth: {str(e)}")
        return total

    def clear_unrouted_file(self):
        """
        Clear the unrouted messages file

        This can be useful for maintenance or after analyzing unrouted messages

        Returns:
            Number of lines that were in the file before clearing
        """
        try:
            with open(self.unrouted_file, 'r') as f:
                line_count = sum(1 for _ in f)

            # Clear the file
            open(self.unrouted_file, 'w').close()

            logger.info(f"Cleared {line_count} unrouted messages from {self.unrouted_file}")
            return line_count

        except FileNotFoundError:
            logger.info(f"Unrouted file {self.unrouted_file} does not exist")
            return 0
        except Exception as e:
            logger.error(f"Error clearing unrouted file: {str(e)}")
            return -1