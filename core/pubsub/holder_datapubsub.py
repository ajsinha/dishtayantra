"""
HolderDataSubscriber - A passive DataSubscriber implementation

This subscriber does not actively subscribe to any external data source.
Instead, it provides a way for external code to directly populate its
internal queue. This is useful for:

1. Testing - inject test data without external dependencies
2. Manual data feeding - programmatically add data
3. Buffering - act as a holding area between components
4. Mock implementations - simulate subscribers in tests

Copyright © 2025 Abhikarta. All rights reserved.
"""

import logging
import queue
import threading
from datetime import datetime
from core.pubsub.datapubsub import DataSubscriber, DataPublisher

logger = logging.getLogger(__name__)


class HolderDataSubscriber(DataSubscriber):
    """
    A passive DataSubscriber that does not subscribe to any external source.

    This subscriber extends DataSubscriber but does not actively poll or receive
    data from any external system (Kafka, database, file, etc.). Instead, it
    provides methods for external code to directly inject data into its internal
    queue.

    Key Characteristics:
    - No external data source (Kafka, DB, file, etc.)
    - Data is added programmatically via add_data() methods
    - Still provides all DataSubscriber functionality (get_data, suspend, resume, etc.)
    - Useful for testing, manual data injection, or buffering
    - Can be started/stopped (though start() doesn't subscribe to anything)

    Example Usage:
        # Create holder
        holder = HolderDataSubscriber('data_holder', max_depth=1000)

        # Add data programmatically
        holder.add_data({'type': 'order', 'id': 123})
        holder.add_data_batch([msg1, msg2, msg3])

        # Consumer can retrieve data normally
        data = holder.get_data(block_time=1)

        # Or start subscription loop (though it won't receive external data)
        holder.start()
        # ... consume via get_data() in another thread
    """

    def __init__(self, name, source='holder://passive', config=None):
        """
        Initialize HolderDataSubscriber

        Args:
            name: Name of the subscriber
            source: Source identifier (default: 'holder://passive')
            config: Optional configuration dictionary containing:
                - 'max_depth': Maximum queue depth (default: 100000)
                - Any other DataSubscriber config params

        Example:
            holder = HolderDataSubscriber('my_holder')
            holder = HolderDataSubscriber('my_holder', max_depth=5000)
            holder = HolderDataSubscriber('my_holder', config={'max_depth': 5000})
        """
        # Handle both config dict and direct max_depth parameter
        if config is None:
            config = {}

        # Set source if not in config
        if 'source' not in config:
            config['source'] = source

        # Initialize parent class
        super().__init__(name, source, config)

        # Track statistics for data added programmatically
        self._data_added_count = 0
        self._data_added_failed = 0

        logger.info(f"HolderDataSubscriber '{name}' initialized (passive mode)")

    def _do_subscribe(self):
        """
        Required implementation of abstract method from DataSubscriber

        Since HolderDataSubscriber does not subscribe to any external source,
        this method always returns None. The subscription loop will continue
        to run if started, but it won't receive any data through this method.

        Data should be added via add_data() or add_data_batch() methods.

        Returns:
            None (no external data source)
        """
        # No external source to poll
        # This is intentional - data comes from add_data() methods
        return None

    def add_data(self, data, block=True, timeout=None):
        """
        Add data directly to the internal queue

        This is the primary way to inject data into the HolderDataSubscriber.
        The data will be available for consumers via get_data().

        Args:
            data: The data to add to the queue (any type)
            block: If True, block if queue is full (default: True)
            timeout: Timeout in seconds when blocking (None = wait forever)

        Returns:
            bool: True if data was added successfully, False otherwise

        Raises:
            queue.Full: If block=False and queue is full

        Example:
            holder.add_data({'type': 'order', 'id': 123})
            holder.add_data(my_message, block=False)  # Don't wait if full
            holder.add_data(data, timeout=5)  # Wait max 5 seconds
        """
        try:
            if block:
                if timeout is not None:
                    self._internal_queue.put(data, block=True, timeout=timeout)
                else:
                    self._internal_queue.put(data, block=True)
            else:
                self._internal_queue.put_nowait(data)

            # Update statistics
            with self._lock:
                self._data_added_count += 1
                self._last_receive = datetime.now().isoformat()
                self._receive_count += 1

            logger.debug(f"Data added to HolderDataSubscriber '{self.name}'")
            return True

        except queue.Full:
            with self._lock:
                self._data_added_failed += 1
            logger.warning(f"Failed to add data to HolderDataSubscriber '{self.name}': queue full")
            if not block:
                raise
            return False
        except Exception as e:
            with self._lock:
                self._data_added_failed += 1
            logger.error(f"Error adding data to HolderDataSubscriber '{self.name}': {str(e)}")
            return False

    def add_data_batch(self, data_list, block=True, timeout=None, stop_on_full=False):
        """
        Add multiple data items to the internal queue

        Args:
            data_list: List of data items to add
            block: If True, block if queue is full (default: True)
            timeout: Timeout in seconds when blocking (None = wait forever)
            stop_on_full: If True, stop adding when queue is full (default: False)

        Returns:
            tuple: (success_count, failed_count)

        Example:
            messages = [msg1, msg2, msg3, msg4]
            success, failed = holder.add_data_batch(messages)
            print(f"Added {success}, failed {failed}")

            # Stop immediately if queue fills up
            success, failed = holder.add_data_batch(messages, stop_on_full=True)
        """
        success_count = 0
        failed_count = 0

        for data in data_list:
            try:
                if self.add_data(data, block=block, timeout=timeout):
                    success_count += 1
                else:
                    failed_count += 1
                    if stop_on_full:
                        # Stop adding more if queue is full
                        failed_count += len(data_list) - success_count - failed_count
                        break
            except queue.Full:
                failed_count += 1
                if stop_on_full:
                    # Count remaining items as failed
                    failed_count += len(data_list) - success_count - failed_count
                    break

        logger.info(f"Batch add to HolderDataSubscriber '{self.name}': "
                    f"{success_count} succeeded, {failed_count} failed")
        return success_count, failed_count

    def try_add_data(self, data):
        """
        Try to add data without blocking (convenience method)

        This is equivalent to add_data(data, block=False) but with
        simpler error handling.

        Args:
            data: The data to add

        Returns:
            bool: True if added successfully, False if queue is full

        Example:
            if holder.try_add_data(my_data):
                print("Added successfully")
            else:
                print("Queue is full")
        """
        try:
            return self.add_data(data, block=False)
        except queue.Full:
            return False

    def clear(self):
        """
        Clear all data from the internal queue

        This removes all pending data that hasn't been consumed yet.
        Useful for resetting state in tests or clearing backlog.

        Returns:
            int: Number of items that were cleared

        Example:
            cleared = holder.clear()
            print(f"Cleared {cleared} items")
        """
        count = 0
        while not self._internal_queue.empty():
            try:
                self._internal_queue.get_nowait()
                count += 1
            except queue.Empty:
                break

        if count > 0:
            logger.info(f"Cleared {count} items from HolderDataSubscriber '{self.name}'")

        return count

    def peek(self):
        """
        Look at the next item without removing it from the queue

        Note: This is NOT thread-safe with other consumers. It's intended
        for debugging or single-threaded scenarios.

        Returns:
            The next data item, or None if queue is empty

        Example:
            next_item = holder.peek()
            if next_item:
                print(f"Next item: {next_item}")
        """
        if self._internal_queue.empty():
            return None

        # Get and put back (not atomic, not thread-safe with other consumers)
        try:
            data = self._internal_queue.get_nowait()
            self._internal_queue.put_nowait(data)
            return data
        except (queue.Empty, queue.Full):
            return None

    def get_statistics(self):
        """
        Get statistics about the holder subscriber

        Returns:
            dict: Statistics including data added, queue depth, etc.

        Example:
            stats = holder.get_statistics()
            print(f"Added: {stats['data_added_count']}")
            print(f"Queue depth: {stats['current_depth']}")
        """
        with self._lock:
            return {
                'name': self.name,
                'source': self.source,
                'type': 'holder',
                'current_depth': self.get_queue_size(),
                'max_depth': self.max_depth,
                'data_added_count': self._data_added_count,
                'data_added_failed': self._data_added_failed,
                'data_retrieved_count': self._receive_count,
                'last_receive': self._last_receive,
                'suspended': not self._suspend_event.is_set()
            }

    def details(self):
        """
        Return detailed information (overrides parent to add holder-specific info)

        Returns:
            dict: Complete details about the holder subscriber
        """
        base_details = super().details()

        # Add holder-specific information
        with self._lock:
            base_details['type'] = 'holder'
            base_details['data_added_count'] = self._data_added_count
            base_details['data_added_failed'] = self._data_added_failed

        return base_details

    def is_composite(self):
        """Indicate this is not a composite subscriber"""
        return False

    def start(self):
        """
        Start the subscription loop

        Note: For HolderDataSubscriber, the subscription loop won't receive
        any data from _do_subscribe() since there's no external source.
        However, starting the subscriber still enables the full threading
        infrastructure if needed for consistency with other subscribers.

        Typically, you don't need to start() a HolderDataSubscriber since
        data is added programmatically and retrieved via get_data().
        """
        logger.info(f"Starting HolderDataSubscriber '{self.name}' "
                    f"(passive mode - data must be added via add_data())")
        super().start()

    def __repr__(self):
        """String representation"""
        return (f"HolderDataSubscriber(name='{self.name}', "
                f"source='{self.source}', "
                f"queue_depth={self.get_queue_size()}/{self.max_depth})")

    def __len__(self):
        """Return current queue size (enables len(holder))"""
        return self.get_queue_size()


def create_holder_subscriber(name, max_depth=100000, source='holder://passive'):
    """
    Factory function to create a HolderDataSubscriber

    Args:
        name: Name of the subscriber
        max_depth: Maximum queue depth (default: 100000)
        source: Source identifier (default: 'holder://passive')

    Returns:
        HolderDataSubscriber instance

    Example:
        holder = create_holder_subscriber('my_holder', max_depth=5000)
    """
    config = {
        'source': source,
        'max_depth': max_depth
    }
    return HolderDataSubscriber(name, source, config)


class HolderDataPublisher(DataPublisher):
    """
    A passive DataPublisher that does not publish to any external destination.

    This publisher extends DataPublisher but does not actually send data to
    any external system (Kafka, database, file, etc.). Instead, it stores
    published messages internally so they can be retrieved and verified.

    This is useful for:
    - Testing - verify what was published without external dependencies
    - Mock implementations - simulate publishers in tests
    - Debugging - inspect published messages
    - Development - work without real infrastructure

    Key Characteristics:
    - No external destination (Kafka, DB, file, etc.)
    - Published messages are stored in an internal list
    - Provides methods to retrieve and inspect published messages
    - Still provides all DataPublisher functionality (batching, periodic publishing, etc.)

    Example Usage:
        # Create holder publisher
        holder = HolderDataPublisher('test_pub')

        # Publish data
        holder.publish({'type': 'order', 'id': 123})
        holder.publish({'type': 'log', 'level': 'INFO'})

        # Retrieve published messages for verification
        messages = holder.get_published_messages()
        assert len(messages) == 2
        assert messages[0]['type'] == 'order'
    """

    def __init__(self, name, destination='holder://passive', config=None):
        """
        Initialize HolderDataPublisher

        Args:
            name: Name of the publisher
            destination: Destination identifier (default: 'holder://passive')
            config: Optional configuration dictionary containing:
                - 'publish_interval': Interval for periodic publishing (default: 0)
                - 'batch_size': Batch size for publishing (default: None)
                - Any other DataPublisher config params

        Example:
            holder = HolderDataPublisher('my_pub')
            holder = HolderDataPublisher('my_pub', config={'publish_interval': 1})
        """
        # Handle both config dict and direct parameters
        if config is None:
            config = {}

        # Set destination if not in config
        if 'destination' not in config:
            config['destination'] = destination

        # Initialize parent class
        super().__init__(name, destination, config)

        # Internal storage for published messages
        self._published_messages = []
        self._messages_lock = threading.Lock()

        logger.info(f"HolderDataPublisher '{name}' initialized (passive mode)")

    def _do_publish(self, data):
        """
        Required implementation of abstract method from DataPublisher

        Since HolderDataPublisher does not publish to any external destination,
        this method just stores the data in an internal list for later retrieval.

        Args:
            data: The data to "publish" (store internally)
        """
        with self._messages_lock:
            self._published_messages.append(data)

        # Update parent statistics
        with self._lock:
            self._publish_count += 1
            self._last_publish = datetime.now().isoformat()

        logger.debug(f"Data stored in HolderDataPublisher '{self.name}' "
                     f"(total: {len(self._published_messages)})")

    def get_published_messages(self):
        """
        Get all messages that have been published

        Returns:
            list: Copy of all published messages

        Example:
            messages = holder.get_published_messages()
            for msg in messages:
                print(msg)
        """
        with self._messages_lock:
            return self._published_messages.copy()

    def get_published_count(self):
        """
        Get count of published messages

        Returns:
            int: Number of messages published

        Example:
            count = holder.get_published_count()
            print(f"Published {count} messages")
        """
        with self._messages_lock:
            return len(self._published_messages)

    def get_last_published(self, n=1):
        """
        Get the last N published messages

        Args:
            n: Number of messages to retrieve (default: 1)

        Returns:
            list: Last N published messages (or fewer if not enough published)

        Example:
            # Get last message
            last = holder.get_last_published()

            # Get last 5 messages
            recent = holder.get_last_published(5)
        """
        with self._messages_lock:
            if n == 1 and self._published_messages:
                return [self._published_messages[-1]]
            return self._published_messages[-n:] if self._published_messages else []

    def clear_published_messages(self):
        """
        Clear all stored published messages

        Useful for resetting state between tests or clearing memory.

        Returns:
            int: Number of messages that were cleared

        Example:
            cleared = holder.clear_published_messages()
            print(f"Cleared {cleared} messages")
        """
        with self._messages_lock:
            count = len(self._published_messages)
            self._published_messages.clear()

        if count > 0:
            logger.info(f"Cleared {count} messages from HolderDataPublisher '{self.name}'")

        return count

    def find_messages(self, predicate):
        """
        Find messages matching a predicate function

        Args:
            predicate: Function that takes a message and returns True/False

        Returns:
            list: All messages for which predicate returns True

        Example:
            # Find all order messages
            orders = holder.find_messages(lambda m: m.get('type') == 'order')

            # Find all high-value orders
            high_value = holder.find_messages(
                lambda m: m.get('type') == 'order' and m.get('amount', 0) > 1000
            )
        """
        with self._messages_lock:
            return [msg for msg in self._published_messages if predicate(msg)]

    def contains_message(self, predicate):
        """
        Check if any message matches the predicate

        Args:
            predicate: Function that takes a message and returns True/False

        Returns:
            bool: True if at least one message matches

        Example:
            has_orders = holder.contains_message(lambda m: m.get('type') == 'order')
        """
        with self._messages_lock:
            return any(predicate(msg) for msg in self._published_messages)

    def get_statistics(self):
        """
        Get statistics about the holder publisher

        Returns:
            dict: Statistics including messages published, queue depth, etc.

        Example:
            stats = holder.get_statistics()
            print(f"Published: {stats['messages_stored']}")
        """
        with self._lock:
            with self._messages_lock:
                return {
                    'name': self.name,
                    'destination': self.destination,
                    'type': 'holder',
                    'messages_stored': len(self._published_messages),
                    'publish_count': self._publish_count,
                    'last_publish': self._last_publish,
                    'queue_depth': self._publish_queue.qsize() if self._publish_queue else 0,
                    'publish_interval': self.publish_interval,
                    'batch_size': self.batch_size
                }

    def details(self):
        """
        Return detailed information (overrides parent to add holder-specific info)

        Returns:
            dict: Complete details about the holder publisher
        """
        base_details = super().details()

        # Add holder-specific information
        with self._messages_lock:
            base_details['type'] = 'holder'
            base_details['messages_stored'] = len(self._published_messages)

        return base_details

    def is_composite(self):
        """Indicate this is not a composite publisher"""
        return False

    def __repr__(self):
        """String representation"""
        return (f"HolderDataPublisher(name='{self.name}', "
                f"destination='{self.destination}', "
                f"messages_stored={self.get_published_count()})")

    def __len__(self):
        """Return number of stored messages (enables len(holder))"""
        return self.get_published_count()


def create_holder_publisher(name, destination='holder://passive',
                            publish_interval=0, batch_size=None):
    """
    Factory function to create a HolderDataPublisher

    Args:
        name: Name of the publisher
        destination: Destination identifier (default: 'holder://passive')
        publish_interval: Interval for periodic publishing (default: 0)
        batch_size: Batch size for publishing (default: None)

    Returns:
        HolderDataPublisher instance

    Example:
        holder = create_holder_publisher('my_pub')
        holder = create_holder_publisher('my_pub', publish_interval=1, batch_size=10)
    """
    config = {
        'destination': destination,
        'publish_interval': publish_interval,
        'batch_size': batch_size
    }
    return HolderDataPublisher(name, destination, config)