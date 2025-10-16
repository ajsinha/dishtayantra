import json
import logging
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from core.pubsub.datapubsub import DataPublisher, DataSubscriber

logger = logging.getLogger(__name__)


class KafkaDataPublisher(DataPublisher):
    """Publisher for Kafka topics"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)
        self.topic = destination.split('/')[-1]
        self.bootstrap_servers = config.get('bootstrap_servers', ['localhost:9092'])

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            **config.get('producer_config', {})
        )
        logger.info(f"Kafka producer created for topic {self.topic}")

    def _do_publish(self, data):
        """Publish to Kafka topic"""
        self.producer.send(self.topic, value=data)
        self.producer.flush()

        with self._lock:
            self._last_publish = datetime.now().isoformat()
            self._publish_count += 1

        logger.debug(f"Published to Kafka topic {self.topic}")

    def stop(self):
        """Stop the publisher"""
        super().stop()
        if self.producer:
            self.producer.close()


class KafkaDataSubscriber(DataSubscriber):
    """Subscriber for Kafka topics"""

    def __init__(self, name, source, config):
        super().__init__(name, source, config)
        self.topic = source.split('/')[-1]
        self.bootstrap_servers = config.get('bootstrap_servers', ['localhost:9092'])
        self.group_id = config.get('group_id', f'{name}_group')

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=100,
            **config.get('consumer_config', {})
        )
        logger.info(f"Kafka consumer created for topic {self.topic}")

    def _do_subscribe(self):
        """Subscribe from Kafka topic"""
        for message in self.consumer:
            return message.value
        return None

    def stop(self):
        """Stop the subscriber"""
        super().stop()
        if self.consumer:
            self.consumer.close()