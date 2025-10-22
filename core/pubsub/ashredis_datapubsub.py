import json
import logging
import time
from datetime import datetime
from ashredis_client import AshRedisClient
from core.pubsub.datapubsub import DataPublisher, DataSubscriber

logger = logging.getLogger(__name__)


class AshRedisDataPublisher(DataPublisher):
    """Publisher that sets data in AshRedis"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 6379)
        self.region = config.get('region', None)

        self.ashredis_client = AshRedisClient(
            host=self.host,
            port=self.port,
            default_region=self.region
        )
        self.ashredis_client.connect()

        logger.info(f"AshRedis publisher created for {self.host}:{self.port}" + 
                   (f" region={self.region}" if self.region else ""))

    def _do_publish(self, data):
        """Set data in AshRedis using __dagserver_key"""
        try:
            key = data.get('__dagserver_key')
            if not key:
                logger.error("No __dagserver_key found in data")
                return

            # Remove the key from data before storing
            data_copy = data.copy()
            data_copy.pop('__dagserver_key', None)

            # Get optional TTL from data or config
            ttl_seconds = data_copy.pop('__ttl_seconds', None) or self.config.get('ttl_seconds')

            # Set data in AshRedis
            self.ashredis_client.set(
                key=key,
                value=json.dumps(data_copy),
                region=self.region,
                ttl_seconds=ttl_seconds
            )

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to AshRedis key {key}" + 
                        (f" in region {self.region}" if self.region else ""))
        except Exception as e:
            logger.error(f"Error publishing to AshRedis: {str(e)}")
            raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        if self.ashredis_client:
            self.ashredis_client.close()


class AshRedisChannelDataPublisher(DataPublisher):
    """Publisher that publishes to AshRedis channel using native subscription"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        # Parse channel from destination URL
        self.channel = destination.replace('ashredischannel://', '')
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 6379)
        self.region = config.get('region', None)

        # Create client for SET operations (to publish to channel)
        self.ashredis_client = AshRedisClient(
            host=self.host,
            port=self.port,
            default_region=self.region
        )
        self.ashredis_client.connect()

        logger.info(f"AshRedis channel publisher created for channel {self.channel}")

    def _do_publish(self, data):
        """Publish data to AshRedis channel by setting channel key"""
        try:
            # In AshRedis, publishing means setting the channel key with the data
            # The subscription mechanism will detect this change
            channel_key = f"channel:{self.channel}"
            
            self.ashredis_client.set(
                key=channel_key,
                value=json.dumps(data),
                region=self.region
            )

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to AshRedis channel {self.channel}")
        except Exception as e:
            logger.error(f"Error publishing to AshRedis channel: {str(e)}")
            raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        if self.ashredis_client:
            self.ashredis_client.close()


class AshRedisChannelDataSubscriber(DataSubscriber):
    """Subscriber that subscribes to AshRedis channel using native subscription"""

    def __init__(self, name, source, config):
        super().__init__(name, source, config)

        # Parse channel from source URL
        self.channel = source.replace('ashredischannel://', '')
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 6379)
        self.region = config.get('region', None)

        self.ashredis_client = AshRedisClient(
            host=self.host,
            port=self.port,
            default_region=self.region
        )
        self.ashredis_client.connect()

        # Queue to store incoming messages
        self._message_queue = []
        self._queue_lock = threading.Lock()

        # Subscribe to the channel with callback
        self.ashredis_client.subscribe(self.channel, self._on_message)

        logger.info(f"AshRedis channel subscriber created for channel {self.channel}")

    def _on_message(self, message):
        """Callback for incoming messages"""
        with self._queue_lock:
            self._message_queue.append(message)

    def _do_subscribe(self):
        """Subscribe from AshRedis channel"""
        try:
            # Check if there are any messages in the queue
            with self._queue_lock:
                if self._message_queue:
                    message_data = self._message_queue.pop(0)
                    return json.loads(message_data)

            # No messages available
            time.sleep(0.1)
            return None
        except Exception as e:
            logger.error(f"Error subscribing from AshRedis channel: {str(e)}")
            time.sleep(0.5)
            return None

    def stop(self):
        """Stop the subscriber"""
        super().stop()
        if self.ashredis_client:
            self.ashredis_client.unsubscribe(self.channel)
            self.ashredis_client.close()


# Import threading for queue lock
import threading
