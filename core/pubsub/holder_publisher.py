"""
HolderDataPublisher (v2.2 module split)
=======================================

Extracted verbatim from holder_datapubsub.py to respect the 500-line
architecture limit. Re-exported from core.pubsub.holder_datapubsub.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

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