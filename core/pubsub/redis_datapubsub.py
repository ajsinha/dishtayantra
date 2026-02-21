"""
Redis DataPublisher and DataSubscriber implementation

Supports both key-value storage and pub/sub channel messaging.
v1.7.6: Enhanced connection retry and auto-recovery support.
"""

import json
import logging
import time
import traceback
from datetime import datetime
import redis
from core.pubsub.datapubsub import DataPublisher, DataSubscriber, smart_deserialize
import queue

logger = logging.getLogger(__name__)


class RedisDataPublisher(DataPublisher):
    """Publisher that sets data in Redis with v1.7.6 connection resilience."""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 6379)
        self.db = config.get('db', 0)
        self.password = config.get('password', None)

        # v1.7.6: Connection retry configuration
        self._max_retries = config.get('max_retries', 5)
        self._retry_delay = config.get('retry_delay', 3)
        self._auto_reconnect = config.get('auto_reconnect', True)
        self._connected = False
        self.redis_client = None

        # v1.7.6: Connect with retry logic
        self._connect_with_retry()

        logger.info(f"Redis publisher created for {self.host}:{self.port}")

    def _connect_with_retry(self):
        """v1.7.6: Establish Redis connection with retry logic."""
        last_error = None

        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(f"Redis publisher connection attempt {attempt}/{self._max_retries} to {self.host}:{self.port}")

                self.redis_client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    password=self.password,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                # Test connection
                self.redis_client.ping()

                self._connected = True
                logger.info(f"Redis publisher connected successfully (attempt={attempt})")
                return

            except Exception as e:
                last_error = e
                logger.warning(f"Redis publisher connection attempt {attempt}/{self._max_retries} failed: {e}")

                if attempt < self._max_retries:
                    logger.info(f"Retrying in {self._retry_delay} seconds...")
                    time.sleep(self._retry_delay)

        logger.error(f"Failed to connect Redis publisher after {self._max_retries} attempts: {last_error}")
        raise ConnectionError(f"Could not connect to Redis {self.host}:{self.port}: {last_error}")

    def _reconnect_if_needed(self):
        """v1.7.6: Reconnect if connection is broken."""
        if not self._auto_reconnect:
            return

        try:
            if self._connected and self.redis_client:
                self.redis_client.ping()
                return  # Connection is healthy
        except:
            pass

        self._connected = False
        logger.info("Redis publisher connection lost, attempting reconnection...")

        try:
            if self.redis_client:
                try:
                    self.redis_client.close()
                except:
                    pass
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"Redis publisher reconnection failed: {e}")
            raise

    def _do_publish(self, data):
        """Set data in Redis using __dagserver_key with auto-reconnection."""
        try:
            # v1.7.6: Check and reconnect if needed
            self._reconnect_if_needed()

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
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            self._connected = False

            if self._auto_reconnect:
                try:
                    self._reconnect_if_needed()
                    # Retry
                    key = data.get('__dagserver_key')
                    if key:
                        data_copy = data.copy()
                        data_copy.pop('__dagserver_key', None)
                        self.redis_client.set(key, json.dumps(data_copy))
                        logger.info("Retry publish succeeded after reconnection")
                except Exception as retry_error:
                    logger.error(f"Retry publish failed: {retry_error}")
                    raise
            else:
                raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        if self.redis_client:
            try:
                self.redis_client.close()
            except:
                pass
        self._connected = False


class RedisChannelDataPublisher(DataPublisher):
    """Publisher that publishes to Redis channel with v1.7.6 connection resilience."""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        self.channel = destination.replace('redischannel://', '')
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 6379)
        self.db = config.get('db', 0)
        self.password = config.get('password', None)

        # v1.7.6: Connection retry configuration
        self._max_retries = config.get('max_retries', 5)
        self._retry_delay = config.get('retry_delay', 3)
        self._auto_reconnect = config.get('auto_reconnect', True)
        self._connected = False
        self.redis_client = None

        # v1.7.6: Connect with retry logic
        self._connect_with_retry()

        logger.info(f"Redis channel publisher created for channel {self.channel}")

    def _connect_with_retry(self):
        """v1.7.6: Establish Redis connection with retry logic."""
        last_error = None

        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(f"Redis channel publisher connection attempt {attempt}/{self._max_retries}")

                self.redis_client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    password=self.password,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                # Test connection
                self.redis_client.ping()

                self._connected = True
                logger.info(f"Redis channel publisher connected successfully (attempt={attempt})")
                return

            except Exception as e:
                last_error = e
                logger.warning(f"Redis channel publisher connection attempt {attempt}/{self._max_retries} failed: {e}")

                if attempt < self._max_retries:
                    logger.info(f"Retrying in {self._retry_delay} seconds...")
                    time.sleep(self._retry_delay)

        logger.error(f"Failed to connect Redis channel publisher after {self._max_retries} attempts: {last_error}")
        raise ConnectionError(f"Could not connect to Redis {self.host}:{self.port}: {last_error}")

    def _reconnect_if_needed(self):
        """v1.7.6: Reconnect if connection is broken."""
        if not self._auto_reconnect:
            return

        try:
            if self._connected and self.redis_client:
                self.redis_client.ping()
                return
        except:
            pass

        self._connected = False
        logger.info("Redis channel publisher connection lost, attempting reconnection...")

        try:
            if self.redis_client:
                try:
                    self.redis_client.close()
                except:
                    pass
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"Redis channel publisher reconnection failed: {e}")
            raise

    def _do_publish(self, data):
        """Publish data to Redis channel with auto-reconnection."""
        try:
            # v1.7.6: Check and reconnect if needed
            self._reconnect_if_needed()

            self.redis_client.publish(self.channel, json.dumps(data))

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to Redis channel {self.channel}")
        except Exception as e:
            logger.error(f"Error publishing to Redis channel: {str(e)}")
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            self._connected = False

            if self._auto_reconnect:
                try:
                    self._reconnect_if_needed()
                    self.redis_client.publish(self.channel, json.dumps(data))
                    logger.info("Retry publish succeeded after reconnection")
                except Exception as retry_error:
                    logger.error(f"Retry publish failed: {retry_error}")
                    raise
            else:
                raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        if self.redis_client:
            try:
                self.redis_client.close()
            except:
                pass
        self._connected = False


class RedisChannelDataSubscriber(DataSubscriber):
    """Subscriber that subscribes to Redis channel with v1.7.6 connection resilience."""

    def __init__(self, name, source, config, given_queue: queue.Queue = None):
        super().__init__(name, source, config, given_queue)

        self.channel = source.replace('redischannel://', '')
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 6379)
        self.db = config.get('db', 0)
        self.password = config.get('password', None)

        # v1.7.6: Connection retry configuration
        self._max_retries = config.get('max_retries', 5)
        self._retry_delay = config.get('retry_delay', 3)
        self._auto_reconnect = config.get('auto_reconnect', True)
        self._connected = False
        self._stopping = False
        self.redis_client = None
        self.pubsub = None

        # v1.7.6: Connect with retry logic
        self._connect_with_retry()

        logger.info(f"Redis channel subscriber created for channel {self.channel}")

    def _connect_with_retry(self):
        """v1.7.6: Establish Redis connection with retry logic."""
        last_error = None

        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(f"Redis channel subscriber connection attempt {attempt}/{self._max_retries}")

                self.redis_client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    password=self.password,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                # Test connection
                self.redis_client.ping()

                self.pubsub = self.redis_client.pubsub()
                self.pubsub.subscribe(self.channel)

                self._connected = True
                logger.info(f"Redis channel subscriber connected successfully (attempt={attempt})")
                return

            except Exception as e:
                last_error = e
                logger.warning(f"Redis channel subscriber connection attempt {attempt}/{self._max_retries} failed: {e}")

                if attempt < self._max_retries:
                    logger.info(f"Retrying in {self._retry_delay} seconds...")
                    time.sleep(self._retry_delay)

        logger.error(f"Failed to connect Redis channel subscriber after {self._max_retries} attempts: {last_error}")
        raise ConnectionError(f"Could not connect to Redis {self.host}:{self.port}: {last_error}")

    def _reconnect_if_needed(self):
        """v1.7.6: Reconnect if connection is broken."""
        if not self._auto_reconnect or self._stopping:
            return

        try:
            if self._connected and self.redis_client:
                self.redis_client.ping()
                return
        except:
            pass

        self._connected = False
        logger.info("Redis channel subscriber connection lost, attempting reconnection...")

        try:
            self._close_connections()
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"Redis channel subscriber reconnection failed: {e}")

    def _close_connections(self):
        """Safely close existing connections."""
        try:
            if self.pubsub:
                self.pubsub.unsubscribe()
                self.pubsub.close()
        except:
            pass
        try:
            if self.redis_client:
                self.redis_client.close()
        except:
            pass
        self.pubsub = None
        self.redis_client = None

    def _do_subscribe(self):
        """Subscribe from Redis channel with auto-reconnection."""
        try:
            # v1.7.6: Check and reconnect if needed
            self._reconnect_if_needed()

            message = self.pubsub.get_message(timeout=0.1)

            if message and message['type'] == 'message':
                # v1.7.2: Use smart deserializer for non-JSON message handling
                return smart_deserialize(message['data'], f"redis:{self.name}")

            return None
        except Exception as e:
            # v1.7.2 Policy: Full stack trace for all exceptions
            logger.error(f"Error subscribing from Redis channel: {str(e)}")
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            self._connected = False

            if self._auto_reconnect and not self._stopping:
                try:
                    self._reconnect_if_needed()
                except:
                    pass
            time.sleep(0.5)
            return None

    def stop(self):
        """Stop the subscriber"""
        self._stopping = True
        super().stop()
        self._close_connections()
        self._connected = False