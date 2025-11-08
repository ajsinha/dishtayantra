import json
import logging
import time
from datetime import datetime
import redis
from core.pubsub.datapubsub import DataPublisher, DataSubscriber
import queue
logger = logging.getLogger(__name__)


class RedisDataPublisher(DataPublisher):
    """Publisher that sets data in Redis"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 6379)
        self.db = config.get('db', 0)

        self.redis_client = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            decode_responses=True
        )

        logger.info(f"Redis publisher created for {self.host}:{self.port}")

    def _do_publish(self, data):
        """Set data in Redis using __dagserver_key"""
        try:
            key = data.get('__dagserver_key')
            if not key:
                logger.error("No __dagserver_key found in data")
                return

            # Remove the key from data before storing
            data_copy = data.copy()
            data_copy.pop('__dagserver_key', None)

            self.redis_client.set(key, json.dumps(data_copy))

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to Redis key {key}")
        except Exception as e:
            logger.error(f"Error publishing to Redis: {str(e)}")
            raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        if self.redis_client:
            self.redis_client.close()


class RedisChannelDataPublisher(DataPublisher):
    """Publisher that publishes to Redis channel"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        self.channel = destination.replace('redischannel://', '')
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 6379)
        self.db = config.get('db', 0)

        self.redis_client = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            decode_responses=True
        )

        logger.info(f"Redis channel publisher created for channel {self.channel}")

    def _do_publish(self, data):
        """Publish data to Redis channel"""
        try:
            self.redis_client.publish(self.channel, json.dumps(data))

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to Redis channel {self.channel}")
        except Exception as e:
            logger.error(f"Error publishing to Redis channel: {str(e)}")
            raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        if self.redis_client:
            self.redis_client.close()


class RedisChannelDataSubscriber(DataSubscriber):
    """Subscriber that subscribes to Redis channel"""

    def __init__(self, name, source, config, given_queue: queue.Queue = None):
        super().__init__(name, source, config, given_queue)

        self.channel = source.replace('redischannel://', '')
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 6379)
        self.db = config.get('db', 0)

        self.redis_client = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            decode_responses=True
        )

        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe(self.channel)

        logger.info(f"Redis channel subscriber created for channel {self.channel}")

    def _do_subscribe(self):
        """Subscribe from Redis channel"""
        try:
            message = self.pubsub.get_message(timeout=0.1)

            if message and message['type'] == 'message':
                return json.loads(message['data'])

            return None
        except Exception as e:
            logger.error(f"Error subscribing from Redis channel: {str(e)}")
            time.sleep(0.5)
            return None

    def stop(self):
        """Stop the subscriber"""
        super().stop()
        if self.pubsub:
            self.pubsub.unsubscribe()
            self.pubsub.close()
        if self.redis_client:
            self.redis_client.close()