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


class InMemoryRedisChannelDataSubscriber(DataSubscriber):
    """Subscriber that subscribes to InMemoryRedisClone channel using pub/sub"""

    def __init__(self, name, source, config):
        super().__init__(name, source, config)

        # Parse channel from source URL
        self.channel = source.replace('inmemoryredischannel://', '')
        if not self.channel:
            self.channel = config.get('channel', 'default_channel')

        # Get the shared Redis instance from config
        self.redis_instance = config.get('redis_instance')
        if not self.redis_instance:
            # Create a new instance if not provided
            from inmemory_redisclone import InMemoryRedisClone
            self.redis_instance = InMemoryRedisClone()
            logger.warning(f"No redis_instance provided in config, created new instance")

        # Queue to store incoming messages
        self._message_queue = []
        self._queue_lock = threading.Lock()

        # Subscribe to the channel with callback
        self.redis_instance.subscribe(self.channel, self._on_message)

        logger.info(f"InMemoryRedis channel subscriber created for channel '{self.channel}'")

    def _on_message(self, channel, message):
        """Callback for incoming messages"""
        with self._queue_lock:
            self._message_queue.append(message)
            logger.debug(f"Received message on channel '{channel}'")

    def _do_subscribe(self):
        """Subscribe from InMemoryRedisClone channel"""
        try:
            # Check if there are any messages in the queue
            with self._queue_lock:
                if self._message_queue:
                    message_data = self._message_queue.pop(0)

                    with self._lock:
                        self._last_receive = datetime.now().isoformat()
                        self._receive_count += 1

                    return json.loads(message_data)

            # No messages available
            time.sleep(0.01)  # Small sleep to avoid busy waiting
            return None

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from channel: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error subscribing from InMemoryRedis channel: {str(e)}")
            time.sleep(0.1)
            return None

    def get_queue_size(self):
        """Get current queue size"""
        with self._queue_lock:
            return len(self._message_queue)

    def stop(self):
        """Stop the subscriber"""
        super().stop()
        if self.redis_instance:
            self.redis_instance.unsubscribe(self.channel, self._on_message)
        logger.info(f"InMemoryRedis channel subscriber {self.name} stopped")


class InMemoryRedisDataSubscriber(DataSubscriber):
    """Subscriber that polls keys from InMemoryRedisClone"""

    def __init__(self, name, source, config):
        super().__init__(name, source, config)

        # Get the shared Redis instance from config
        self.redis_instance = config.get('redis_instance')
        if not self.redis_instance:
            # Create a new instance if not provided
            from inmemory_redisclone import InMemoryRedisClone
            self.redis_instance = InMemoryRedisClone()
            logger.warning(f"No redis_instance provided in config, created new instance")

        # Key pattern to watch
        self.key_pattern = config.get('key_pattern', '*')
        self.key_prefix = config.get('key_prefix', '')
        self.delete_on_read = config.get('delete_on_read', False)
        self.poll_interval = config.get('poll_interval', 0.1)

        # Track already seen keys to avoid duplicate reads
        self._seen_keys = set()
        self._seen_keys_lock = threading.Lock()

        logger.info(f"InMemoryRedis subscriber created for pattern '{self.key_pattern}'")

    def _do_subscribe(self):
        """Poll keys from InMemoryRedisClone"""
        try:
            # Get all keys matching pattern
            pattern = f"{self.key_prefix}{self.key_pattern}"
            keys = self.redis_instance.keys(pattern)

            # Find new keys we haven't seen
            with self._seen_keys_lock:
                new_keys = [k for k in keys if k not in self._seen_keys]

                if new_keys:
                    # Process first new key
                    key = new_keys[0]
                    self._seen_keys.add(key)

                    # Get data
                    value = self.redis_instance.get(key)
                    if value:
                        # Delete if configured
                        if self.delete_on_read:
                            self.redis_instance.delete(key)
                            self._seen_keys.discard(key)

                        with self._lock:
                            self._last_receive = datetime.now().isoformat()
                            self._receive_count += 1

                        # Try to parse as JSON
                        try:
                            data = json.loads(value)
                            # Add the key to the data
                            data['__dagserver_key'] = key
                            return data
                        except json.JSONDecodeError:
                            # Return as raw string
                            return {'__dagserver_key': key, 'value': value}

            # No new messages available
            time.sleep(self.poll_interval)
            return None

        except Exception as e:
            logger.error(f"Error subscribing from InMemoryRedis: {str(e)}")
            time.sleep(0.5)
            return None

    def reset_seen_keys(self):
        """Reset the set of seen keys"""
        with self._seen_keys_lock:
            self._seen_keys.clear()
        logger.info(f"Subscriber {self.name} reset seen keys")

    def stop(self):
        """Stop the subscriber"""
        super().stop()
        logger.info(f"InMemoryRedis subscriber {self.name} stopped")


# Example usage and testing
if __name__ == '__main__':
    import sys
    import time
    from inmemory_redisclone import InMemoryRedisClone

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("=" * 70)
    print("InMemoryRedis DataPublisher/DataSubscriber Demo")
    print("=" * 70)

    # Create shared Redis instance
    redis = InMemoryRedisClone()

    # Demo 1: Key-based publisher and subscriber
    print("\n--- Demo 1: Key-based Pub/Sub ---")

    # Create publisher
    publisher = InMemoryRedisDataPublisher(
        name="test_publisher",
        destination="inmemoryredis://",
        config={
            'redis_instance': redis,
            'key_prefix': 'demo:',
            'ttl_seconds': 300
        }
    )

    # Create subscriber
    subscriber = InMemoryRedisDataSubscriber(
        name="test_subscriber",
        source="inmemoryredis://",
        config={
            'redis_instance': redis,
            'key_pattern': 'demo:*',
            'delete_on_read': True,
            'poll_interval': 0.1
        }
    )

    # Publish some data
    print("\nPublishing data...")
    for i in range(3):
        data = {
            '__dagserver_key': f'message_{i}',
            'index': i,
            'content': f'This is message {i}',
            'timestamp': datetime.now().isoformat()
        }
        publisher.publish(data)
        print(f"  Published: message_{i}")

    # Subscribe and receive data
    print("\nReceiving data...")
    time.sleep(0.2)  # Give a moment for processing

    for _ in range(3):
        data = subscriber._do_subscribe()
        if data:
            print(f"  Received: {data.get('__dagserver_key')} - {data.get('content')}")
        time.sleep(0.1)

    print(f"\nPublisher stats: {publisher.details()}")
    print(f"Subscriber stats: {subscriber.details()}")

    # Demo 2: Channel-based publisher and subscriber
    print("\n--- Demo 2: Channel-based Pub/Sub ---")

    # Create channel publisher
    channel_publisher = InMemoryRedisChannelDataPublisher(
        name="channel_publisher",
        destination="inmemoryredischannel://notifications",
        config={'redis_instance': redis}
    )

    # Create channel subscriber
    channel_subscriber = InMemoryRedisChannelDataSubscriber(
        name="channel_subscriber",
        source="inmemoryredischannel://notifications",
        config={'redis_instance': redis}
    )

    # Publish to channel
    print("\nPublishing to channel...")
    for i in range(3):
        data = {
            'event': 'notification',
            'message': f'Alert {i}',
            'timestamp': datetime.now().isoformat()
        }
        channel_publisher.publish(data)
        print(f"  Published to channel: Alert {i}")
        time.sleep(0.05)  # Small delay to ensure delivery

    # Receive from channel
    print("\nReceiving from channel...")
    time.sleep(0.2)  # Give a moment for processing

    for _ in range(3):
        data = channel_subscriber._do_subscribe()
        if data:
            print(f"  Received from channel: {data.get('message')}")
        time.sleep(0.1)

    print(f"\nChannel publisher stats: {channel_publisher.details()}")
    print(f"Channel subscriber stats: {channel_subscriber.details()}")

    # Demo 3: Multiple subscribers on same channel
    print("\n--- Demo 3: Multiple Subscribers ---")

    subscriber2 = InMemoryRedisChannelDataSubscriber(
        name="channel_subscriber_2",
        source="inmemoryredischannel://notifications",
        config={'redis_instance': redis}
    )

    # Publish one message
    print("\nPublishing to channel with 2 subscribers...")
    channel_publisher.publish({
        'event': 'broadcast',
        'message': 'Message for all subscribers',
        'timestamp': datetime.now().isoformat()
    })

    time.sleep(0.2)

    # Both subscribers should receive it
    print("Subscriber 1 received:", channel_subscriber._do_subscribe())
    print("Subscriber 2 received:", subscriber2._do_subscribe())

    # Cleanup
    publisher.stop()
    subscriber.stop()
    channel_publisher.stop()
    channel_subscriber.stop()
    subscriber2.stop()

    print("\n" + "=" * 70)
    print("Demo Complete!")
    print("=" * 70)