import logging
import queue
from core.pubsub.datapubsub import DataSubscriber, DataPublisher

logger = logging.getLogger(__name__)


class FaninDataSubscriber(DataSubscriber):
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
            source: Source identifier (typically 'fanin://')
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




# v2.2 module split: the publisher lives in fanin_publisher.py.
from core.pubsub.fanin_publisher import FaninDataPublisher  # noqa: E402,F401
