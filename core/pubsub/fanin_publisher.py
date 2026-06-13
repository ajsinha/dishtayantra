"""
FaninDataPublisher (v2.2 module split)
======================================

Extracted verbatim from fanin_datapubsub.py to respect the 500-line
architecture limit. Re-exported from core.pubsub.fanin_datapubsub, so
existing imports keep working.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import queue
from core.pubsub.datapubsub import DataSubscriber, DataPublisher

logger = logging.getLogger(__name__)




class FaninDataPublisher(DataPublisher):
    """
    Composite publisher that broadcasts data to multiple child publishers.
    When publish() is called, the data is sent to all child publishers,
    implementing a fan-out/broadcast pattern.
    """

    def __init__(self, name, destination, config):
        """
        Initialize CompositeDataPublisher

        Args:
            name: Name of the composite publisher
            destination: Destination identifier (typically 'fanin://')
            config: Configuration dictionary containing:
                - publish_interval: Interval for periodic publishing (default: 0)
                - batch_size: Batch size for publishing (default: None)
                - Any other config params for the composite
        """
        # Initialize parent class with config
        super().__init__(name, destination, config)

        # Initialize empty map for child publishers (name -> DataPublisher)
        self._publishers_map = {}

        logger.info(f"CompositeDataPublisher {name} initialized")

    def is_composite(self):
        return True

    def add_data_publisher(self, publisher):
        """
        Add a data publisher to the composite

        Args:
            publisher: DataPublisher instance to add

        The publisher will receive all data published to the composite,
        implementing a broadcast/fan-out pattern.
        """
        if not isinstance(publisher, DataPublisher):
            raise TypeError(f"Expected DataPublisher instance, got {type(publisher)}")

        # Add to map using publisher's name as key
        self._publishers_map[publisher.name] = publisher

        logger.info(f"Added publisher '{publisher.name}' to composite '{self.name}'")

    def remove_data_publisher(self, publisher_name):
        """
        Remove a data publisher from the composite

        Args:
            publisher_name: Name of the publisher to remove

        Returns:
            The removed publisher instance, or None if not found
        """
        publisher = self._publishers_map.pop(publisher_name, None)
        if publisher:
            logger.info(f"Removed publisher '{publisher_name}' from composite '{self.name}'")
        else:
            logger.warning(f"Publisher '{publisher_name}' not found in composite '{self.name}'")
        return publisher

    def get_publisher(self, publisher_name):
        """
        Get a publisher by name

        Args:
            publisher_name: Name of the publisher

        Returns:
            DataPublisher instance or None if not found
        """
        return self._publishers_map.get(publisher_name)

    def list_publishers(self):
        """
        Get list of all publisher names

        Returns:
            List of publisher names
        """
        return list(self._publishers_map.keys())

    def _do_publish(self, data):
        """
        Publish data to all child publishers (broadcast pattern)

        Args:
            data: Data to publish to all children
        """
        if not self._publishers_map:
            logger.warning(f"CompositeDataPublisher '{self.name}' has no child publishers")
            return

        published_count = 0
        failed_count = 0

        for publisher in self._publishers_map.values():
            try:
                publisher.publish(data)
                published_count += 1
            except Exception as e:
                failed_count += 1
                logger.error(f"Error publishing to '{publisher.name}': {str(e)}")

        # Update composite statistics
        with self._lock:
            self._publish_count += 1
            from datetime import datetime
            self._last_publish = datetime.now().isoformat()

        logger.debug(f"CompositeDataPublisher '{self.name}' published to "
                     f"{published_count}/{len(self._publishers_map)} publishers "
                     f"({failed_count} failed)")

    def stop(self):
        """
        Stop all child publishers and the composite
        """
        stopped_count = 0
        for publisher in self._publishers_map.values():
            try:
                publisher.stop()
                stopped_count += 1
            except Exception as e:
                logger.error(f"Error stopping publisher '{publisher.name}': {str(e)}")

        # Stop the parent (flushes composite queue if periodic publishing is enabled)
        super().stop()

        logger.info(f"CompositeDataPublisher '{self.name}' stopped "
                    f"{stopped_count}/{len(self._publishers_map)} child publishers")

    def flush_all(self):
        """
        Flush all child publishers immediately

        This is useful when you want to ensure all pending messages
        are published before continuing.
        """
        flushed_count = 0
        for publisher in self._publishers_map.values():
            try:
                # Check if publisher has a flush queue method
                if hasattr(publisher, '_flush_queue'):
                    publisher._flush_queue()
                    flushed_count += 1
            except Exception as e:
                logger.error(f"Error flushing publisher '{publisher.name}': {str(e)}")

        # Flush composite's own queue if periodic publishing is enabled
        if self._publish_queue:
            self._flush_queue()

        logger.info(f"Flushed {flushed_count}/{len(self._publishers_map)} child publishers")

    def details(self):
        """
        Return detailed information about the composite and all child publishers

        Returns:
            Dictionary containing composite details and all child publisher details
        """
        with self._lock:
            base_details = {
                'name': self.name,
                'destination': self.destination,
                'publish_interval': self.publish_interval,
                'batch_size': self.batch_size,
                'last_publish': self._last_publish,
                'publish_count': self._publish_count,
                'queue_depth': self._publish_queue.qsize() if self._publish_queue else 0,
                'publisher_count': len(self._publishers_map),
                'publishers': {}
            }

            # Add details from each child publisher
            for name, publisher in self._publishers_map.items():
                try:
                    base_details['publishers'][name] = publisher.details()
                except Exception as e:
                    logger.error(f"Error getting details for publisher '{name}': {str(e)}")
                    base_details['publishers'][name] = {'error': str(e)}

            return base_details

    def get_total_publish_count(self):
        """
        Get total publish count across all child publishers

        Returns:
            Total number of messages published by all child publishers
        """
        total = 0
        for publisher in self._publishers_map.values():
            try:
                details = publisher.details()
                total += details.get('publish_count', 0)
            except Exception as e:
                logger.error(f"Error getting publish count for publisher '{publisher.name}': {str(e)}")
        return total

    def get_total_queue_depth(self):
        """
        Get total queue depth across all child publishers

        Returns:
            Total queue depth of all child publishers
        """
        total = 0
        for publisher in self._publishers_map.values():
            try:
                details = publisher.details()
                total += details.get('queue_depth', 0)
            except Exception as e:
                logger.error(f"Error getting queue depth for publisher '{publisher.name}': {str(e)}")
        return total

    def get_publisher_summary(self):
        """
        Get summary statistics of all child publishers

        Returns:
            Dictionary with aggregate statistics
        """
        summary = {
            'total_publishers': len(self._publishers_map),
            'total_publishes': self.get_total_publish_count(),
            'total_queue_depth': self.get_total_queue_depth(),
            'composite_publishes': self._publish_count,
            'publishers': {}
        }

        for name, publisher in self._publishers_map.items():
            try:
                details = publisher.details()
                summary['publishers'][name] = {
                    'destination': details.get('destination'),
                    'publish_count': details.get('publish_count', 0),
                    'queue_depth': details.get('queue_depth', 0),
                    'last_publish': details.get('last_publish')
                }
            except Exception as e:
                logger.error(f"Error getting summary for publisher '{name}': {str(e)}")

        return summary