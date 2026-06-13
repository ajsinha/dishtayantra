"""
InMemoryRedis DataPublisher and DataSubscriber
Publishers and subscribers for the InMemoryRedisClone using the datapubsub pattern.

Usage:
    from inmemoryredis_datapubsub import InMemoryRedisDataPublisher, InMemoryRedisChannelDataSubscriber
    from inmemory_redisclone import InMemoryRedisClone

    # Shared Redis instance
    redis = InMemoryRedisClone()

    # Create publisher
    publisher = InMemoryRedisDataPublisher(
        name="my_publisher",
        destination="inmemoryredis://",
        config={'redis_instance': redis}
    )

    # Publish data
    publisher.publish({'__dagserver_key': 'mykey', 'value': 'data'})
"""

import json
import logging
import time
import threading
from datetime import datetime
from typing import Optional

# Import the base classes from datapubsub
# Note: In actual usage, this would be: from core.pubsub.datapubsub import DataPublisher, DataSubscriber
# For this implementation, we'll define stub base classes if needed

logger = logging.getLogger(__name__)


# Base classes (these would normally be imported from datapubsub.py)
class DataPublisher:
    """Abstract base class for data publishers"""

    def __init__(self, name, destination, config):
        self.name = name
        self.destination = destination
        self.config = config
        self.publish_interval = config.get('publish_interval', 0)
        self.batch_size = config.get('batch_size', None)
        self._last_publish = None
        self._publish_count = 0
        self._lock = threading.Lock()
        logger.info(f"DataPublisher {name} initialized for destination {destination}")

    def publish(self, data):
        """Publish data to destination"""
        self._do_publish(data)

    def _do_publish(self, data):
        """Actual publish implementation - to be overridden"""
        pass

    def details(self):
        """Return details in JSON format"""
        with self._lock:
            return {
                'name': self.name,
                'destination': self.destination,
                'last_publish': self._last_publish,
                'publish_count': self._publish_count
            }

    def stop(self):
        """Stop the publisher"""
        pass


class DataSubscriber:
    """Abstract base class for data subscribers"""

    def __init__(self, name, source, config):
        self.name = name
        self.source = source
        self.config = config
        self._last_receive = None
        self._receive_count = 0
        self._lock = threading.Lock()
        logger.info(f"DataSubscriber {name} initialized for source {source}")

    def _do_subscribe(self):
        """Actual subscribe implementation - to be overridden"""
        pass

    def details(self):
        """Return details in JSON format"""
        with self._lock:
            return {
                'name': self.name,
                'source': self.source,
                'last_receive': self._last_receive,
                'receive_count': self._receive_count
            }

    def stop(self):
        """Stop the subscriber"""
        pass


class InMemoryRedisDataPublisher(DataPublisher):
    """Publisher that sets data in InMemoryRedisClone"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        # Get the shared Redis instance from config
        self.redis_instance = config.get('redis_instance')
        if not self.redis_instance:
            # Create a new instance if not provided
            from inmemory_redisclone import InMemoryRedisClone
            self.redis_instance = InMemoryRedisClone()
            logger.warning(f"No redis_instance provided in config, created new instance")

        self.key_prefix = config.get('key_prefix', '')
        self.default_ttl = config.get('ttl_seconds', None)

        logger.info(f"InMemoryRedis publisher created for {self.name}")

    def _do_publish(self, data):
        """Set data in InMemoryRedisClone using __dagserver_key"""
        try:
            key = data.get('__dagserver_key')
            if not key:
                logger.error("No __dagserver_key found in data")
                return

            # Add prefix if configured
            if self.key_prefix:
                key = f"{self.key_prefix}{key}"

            # Remove the key and TTL from data before storing
            data_copy = data.copy()
            data_copy.pop('__dagserver_key', None)
            ttl_seconds = data_copy.pop('__ttl_seconds', None)

            # Use provided TTL or default
            ttl = ttl_seconds or self.default_ttl

            # Set data in Redis
            self.redis_instance.set(key, json.dumps(data_copy))

            # Set expiration if TTL provided
            if ttl and ttl > 0:
                self.redis_instance.expire(key, ttl)

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to InMemoryRedis key '{key}'" +
                         (f" with TTL {ttl}s" if ttl else ""))

        except Exception as e:
            logger.error(f"Error publishing to InMemoryRedis: {str(e)}")
            raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        logger.info(f"InMemoryRedis publisher {self.name} stopped")


class InMemoryRedisChannelDataPublisher(DataPublisher):
    """Publisher that publishes to InMemoryRedisClone channel using pub/sub"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        # Parse channel from destination URL
        self.channel = destination.replace('inmemoryredischannel://', '')
        if not self.channel:
            self.channel = config.get('channel', 'default_channel')

        # Get the shared Redis instance from config
        self.redis_instance = config.get('redis_instance')
        if not self.redis_instance:
            # Create a new instance if not provided
            from inmemory_redisclone import InMemoryRedisClone
            self.redis_instance = InMemoryRedisClone()
            logger.warning(f"No redis_instance provided in config, created new instance")

        logger.info(f"InMemoryRedis channel publisher created for channel '{self.channel}'")

    def _do_publish(self, data):
        """Publish data to InMemoryRedisClone channel using pub/sub"""
        try:
            # Publish message to channel
            subscriber_count = self.redis_instance.publish(self.channel, json.dumps(data))

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to InMemoryRedis channel '{self.channel}' " +
                         f"({subscriber_count} subscriber(s))")

        except Exception as e:
            logger.error(f"Error publishing to InMemoryRedis channel: {str(e)}")
            raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        logger.info(f"InMemoryRedis channel publisher {self.name} stopped")




# v2.2 module split: subscribers live in inmemoryredis_subscribers.py.
from core.pubsub.inmemoryredis_subscribers import (  # noqa: E402,F401
    InMemoryRedisChannelDataSubscriber,
    InMemoryRedisDataSubscriber,
)
