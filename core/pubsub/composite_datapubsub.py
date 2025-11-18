import logging
import queue
from core.pubsub.datapubsub import DataSubscriber, DataPublisher

logger = logging.getLogger(__name__)


class CompositeDataSubscriber(DataSubscriber):
    """
    Composite subscriber that aggregates data from multiple child subscribers.
    All child subscribers share the same internal queue, allowing data from
    multiple sources to be consumed from a single point.
    """

    def __init__(self, name, source, config, given_internal_queue: queue.Queue = None):
        """
        Initialize CompositeDataSubscriber

        Args:
            name: Name of the composite subscriber
            source: Source identifier (typically 'composite://')
            config: Configuration dictionary containing:
                - max_depth: Maximum queue depth (default: 100000)
                - Any other config params for the composite
            given_internal_queue: Optional external queue to use instead of creating new one
        """
        # Initialize parent class with config
        # This will create the internal queue based on config max_depth
        super().__init__(name, source, config, given_internal_queue)

        # Initialize empty map for child subscribers (name -> DataSubscriber)
        self._subscribers_map = {}

        logger.info(f"CompositeDataSubscriber {name} initialized")

    def is_composite(self):
        return True

    def add_data_subscriber(self, subscriber):
        """
        Add a data subscriber to the composite

        Args:
            subscriber: DataSubscriber instance to add

        The subscriber's internal queue will be set to the composite's queue,
        so all data flows into a single aggregation point.
        """
        if not isinstance(subscriber, DataSubscriber):
            raise TypeError(f"Expected DataSubscriber instance, got {type(subscriber)}")

        # Set the child subscriber's internal queue to the composite's queue
        # This ensures all data from child subscribers flows to one place
        subscriber.set_internal_queue(self._internal_queue)

        # Add to map using subscriber's name as key
        self._subscribers_map[subscriber.name] = subscriber

        logger.info(f"Added subscriber '{subscriber.name}' to composite '{self.name}'")

    def remove_data_subscriber(self, subscriber_name):
        """
        Remove a data subscriber from the composite

        Args:
            subscriber_name: Name of the subscriber to remove

        Returns:
            The removed subscriber instance, or None if not found
        """
        subscriber = self._subscribers_map.pop(subscriber_name, None)
        if subscriber:
            logger.info(f"Removed subscriber '{subscriber_name}' from composite '{self.name}'")
        else:
            logger.warning(f"Subscriber '{subscriber_name}' not found in composite '{self.name}'")
        return subscriber

    def get_subscriber(self, subscriber_name):
        """
        Get a subscriber by name

        Args:
            subscriber_name: Name of the subscriber

        Returns:
            DataSubscriber instance or None if not found
        """
        return self._subscribers_map.get(subscriber_name)

    def list_subscribers(self):
        """
        Get list of all subscriber names

        Returns:
            List of subscriber names
        """
        return list(self._subscribers_map.keys())

    def start(self):
        """
        Start all child subscribers

        Note: This does NOT call super().start() because the composite itself
        doesn't subscribe to anything - it only manages child subscribers.
        """
        if not self._subscribers_map:
            logger.warning(f"CompositeDataSubscriber '{self.name}' has no child subscribers to start")
            return

        started_count = 0
        for subscriber in self._subscribers_map.values():
            try:
                subscriber.start()
                started_count += 1
            except Exception as e:
                logger.error(f"Error starting subscriber '{subscriber.name}': {str(e)}")

        logger.info(
            f"CompositeDataSubscriber '{self.name}' started {started_count}/{len(self._subscribers_map)} child subscribers")

    def stop(self):
        """
        Stop all child subscribers
        """
        stopped_count = 0
        for subscriber in self._subscribers_map.values():
            try:
                subscriber.stop()
                stopped_count += 1
            except Exception as e:
                logger.error(f"Error stopping subscriber '{subscriber.name}': {str(e)}")

        # Set stop event to ensure parent is also stopped
        self._stop_event.set()

        logger.info(
            f"CompositeDataSubscriber '{self.name}' stopped {stopped_count}/{len(self._subscribers_map)} child subscribers")

    def suspend(self):
        """
        Suspend all child subscribers
        """
        suspended_count = 0
        for subscriber in self._subscribers_map.values():
            try:
                subscriber.suspend()
                suspended_count += 1
            except Exception as e:
                logger.error(f"Error suspending subscriber '{subscriber.name}': {str(e)}")

        # Also suspend the parent
        super().suspend()

        logger.info(
            f"CompositeDataSubscriber '{self.name}' suspended {suspended_count}/{len(self._subscribers_map)} child subscribers")

    def resume(self):
        """
        Resume all child subscribers
        """
        resumed_count = 0
        for subscriber in self._subscribers_map.values():
            try:
                subscriber.resume()
                resumed_count += 1
            except Exception as e:
                logger.error(f"Error resuming subscriber '{subscriber.name}': {str(e)}")

        # Also resume the parent
        super().resume()

        logger.info(
            f"CompositeDataSubscriber '{self.name}' resumed {resumed_count}/{len(self._subscribers_map)} child subscribers")

    def _do_subscribe(self):
        """
        Not used in composite pattern as children handle subscription

        This method is required by the abstract base class but is not used
        in the composite pattern. The child subscribers handle all actual
        subscription logic.

        Returns:
            None
        """
        return None

    def details(self):
        """
        Return detailed information about the composite and all child subscribers

        Returns:
            Dictionary containing composite details and all child subscriber details
        """
        with self._lock:
            base_details = {
                'name': self.name,
                'source': self.source,
                'max_depth': self.max_depth,
                'current_depth': self.get_queue_size(),
                'last_receive': self._last_receive,
                'receive_count': self._receive_count,
                'suspended': not self._suspend_event.is_set(),
                'subscriber_count': len(self._subscribers_map),
                'subscribers': {}
            }

            # Add details from each child subscriber
            for name, subscriber in self._subscribers_map.items():
                try:
                    base_details['subscribers'][name] = subscriber.details()
                except Exception as e:
                    logger.error(f"Error getting details for subscriber '{name}': {str(e)}")
                    base_details['subscribers'][name] = {'error': str(e)}

            return base_details

    def get_total_receive_count(self):
        """
        Get total receive count across all child subscribers

        Returns:
            Total number of messages received by all child subscribers
        """
        total = 0
        for subscriber in self._subscribers_map.values():
            try:
                details = subscriber.details()
                total += details.get('receive_count', 0)
            except Exception as e:
                logger.error(f"Error getting receive count for subscriber '{subscriber.name}': {str(e)}")
        return total

    def get_subscriber_status(self):
        """
        Get status summary of all child subscribers

        Returns:
            Dictionary with counts of subscribers in each state
        """
        status = {
            'total': len(self._subscribers_map),
            'suspended': 0,
            'active': 0
        }

        for subscriber in self._subscribers_map.values():
            try:
                details = subscriber.details()
                if details.get('suspended', False):
                    status['suspended'] += 1
                else:
                    status['active'] += 1
            except Exception as e:
                logger.error(f"Error getting status for subscriber '{subscriber.name}': {str(e)}")

        return status


class CompositeDataPublisher(DataPublisher):
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
            destination: Destination identifier (typically 'composite://')
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