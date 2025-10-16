import json
import logging
from datetime import datetime
from core.pubsub.datapubsub import DataPublisher, DataSubscriber
from core.pubsub.inmemorypubsub import InMemoryPubSub

logger = logging.getLogger(__name__)


class InMemoryDataPublisher(DataPublisher):
    """Publisher for in-memory queues and topics"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)
        self.pubsub = InMemoryPubSub()
        self.is_queue = 'queue' in destination
        self.target_name = destination.split('/')[-1]

        if self.is_queue:
            self.pubsub.create_queue(self.target_name, config.get('max_size', 100000))
        else:
            self.pubsub.create_topic(self.target_name)

        logger.info(f"InMemoryDataPublisher created for {destination}")

    def _do_publish(self, data):
        """Publish to in-memory queue or topic"""
        json_data = json.dumps(data)

        if self.is_queue:
            self.pubsub.publish_to_queue(self.target_name, json_data, block=False)
        else:
            self.pubsub.publish_to_topic(self.target_name, json_data)

        with self._lock:
            self._last_publish = datetime.now().isoformat()
            self._publish_count += 1

        logger.debug(f"Published to {self.destination}")


class InMemoryDataSubscriber(DataSubscriber):
    """Subscriber for in-memory queues and topics"""

    def __init__(self, name, source, config):
        super().__init__(name, source, config)
        self.pubsub = InMemoryPubSub()
        self.is_queue = 'queue' in source
        self.target_name = source.split('/')[-1]

        if self.is_queue:
            self.pubsub.create_queue(self.target_name, config.get('max_size', 100000))
        else:
            self._subscription_queue = self.pubsub.subscribe_to_topic(self.target_name, self.max_depth)

        logger.info(f"InMemoryDataSubscriber created for {source}")

    def _do_subscribe(self):
        """Subscribe from in-memory queue or topic"""
        if self.is_queue:
            json_data = self.pubsub.consume_from_queue(self.target_name, block=False)
        else:
            try:
                json_data = self._subscription_queue.get(block=False)
            except:
                json_data = None

        if json_data:
            return json.loads(json_data)
        return None