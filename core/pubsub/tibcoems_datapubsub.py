"""
TIBCO EMS DataPublisher and DataSubscriber implementation

Supports both queue and topic messaging patterns.
v1.7.6: Enhanced connection retry and auto-recovery support.
"""

import json
import logging
import time
import traceback
import threading
from datetime import datetime
from core.pubsub.datapubsub import DataPublisher, DataSubscriber, smart_deserialize
import queue
try:
    import tibcoems
except ImportError:
    logging.warning("tibcoems library not found. Install TIBCO EMS Python client")
    tibcoems = None

logger = logging.getLogger(__name__)


class TibcoEMSDataPublisher(DataPublisher):
    """Publisher for TIBCO EMS queues and topics with v1.7.6 connection resilience."""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        if not tibcoems:
            raise ImportError("tibcoems library required for TIBCO EMS. Install TIBCO EMS Python client")

        # Parse destination: tibcoems://queue/queue_name or tibcoems://topic/topic_name
        parts = destination.replace('tibcoems://', '').split('/')
        self.dest_type = parts[0]  # 'queue' or 'topic'
        self.dest_name = parts[1] if len(parts) > 1 else 'default'

        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 7222)
        self.username = config.get('username', 'admin')
        self.password = config.get('password', 'admin')

        # Connection URL for TIBCO EMS
        self.server_url = config.get('server_url', f'tcp://{self.host}:{self.port}')

        # SSL settings
        self.use_ssl = config.get('use_ssl', False)
        if self.use_ssl:
            self.server_url = config.get('server_url', f'ssl://{self.host}:{self.port}')

        # v1.7.6: Connection retry configuration
        self._max_retries = config.get('max_retries', 5)
        self._retry_delay = config.get('retry_delay', 3)
        self._auto_reconnect = config.get('auto_reconnect', True)
        self._connected = False

        # Connect to TIBCO EMS
        self.connection = None
        self.session = None
        self.producer = None
        self.destination_obj = None
        
        # v1.7.6: Connect with retry logic
        self._connect_with_retry()

        logger.info(f"TIBCO EMS publisher connected to {self.server_url}, "
                    f"dest_type={self.dest_type}, dest_name={self.dest_name}")

    def _do_connect(self):
        """Establish connection to TIBCO EMS"""
        # Create connection factory
        factory = tibcoems.ConnectionFactory()
        factory.setServerUrl(self.server_url)

        # Create connection
        self.connection = factory.createConnection(self.username, self.password)

        # Create session
        self.session = self.connection.createSession(
            False,  # Not transacted
            tibcoems.Session.AUTO_ACKNOWLEDGE
        )

        # Create destination
        if self.dest_type == 'queue':
            self.destination_obj = self.session.createQueue(self.dest_name)
        else:  # topic
            self.destination_obj = self.session.createTopic(self.dest_name)

        # Create producer
        self.producer = self.session.createProducer(self.destination_obj)

        # Start connection
        self.connection.start()
        self._connected = True

    def _connect_with_retry(self):
        """v1.7.6: Establish connection to TIBCO EMS with retry logic"""
        last_error = None

        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(f"TIBCO EMS publisher connection attempt {attempt}/{self._max_retries} to {self.server_url}")
                self._do_connect()
                logger.info(f"TIBCO EMS publisher connected successfully (attempt={attempt})")
                return
            except Exception as e:
                last_error = e
                logger.warning(f"TIBCO EMS publisher connection attempt {attempt}/{self._max_retries} failed: {e}")
                if attempt < self._max_retries:
                    logger.info(f"Retrying in {self._retry_delay} seconds...")
                    time.sleep(self._retry_delay)

        logger.error(f"Failed to connect TIBCO EMS publisher after {self._max_retries} attempts: {last_error}")
        raise ConnectionError(f"Could not connect to TIBCO EMS {self.server_url}: {last_error}")

    # Keep old method name for compatibility
    def _connect(self):
        """Alias for _connect_with_retry"""
        self._connect_with_retry()

    def _reconnect_if_needed(self):
        """v1.7.6: Reconnect if connection is broken"""
        if not self._auto_reconnect:
            return
            
        if self._connected and self.connection and self.session and self.producer:
            return  # Assume connection is healthy
        
        self._connected = False
        logger.info("TIBCO EMS publisher connection lost, attempting reconnection...")
        
        try:
            self._close_connections()
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"TIBCO EMS publisher reconnection failed: {e}")
            raise

    def _close_connections(self):
        """Safely close existing connections"""
        try:
            if self.producer:
                self.producer.close()
        except:
            pass
        try:
            if self.session:
                self.session.close()
        except:
            pass
        try:
            if self.connection:
                self.connection.close()
        except:
            pass
        self.producer = None
        self.session = None
        self.connection = None

    def _do_publish(self, data):
        """Publish to TIBCO EMS queue or topic with auto-reconnection"""
        try:
            # v1.7.6: Check and reconnect if needed
            self._reconnect_if_needed()
            
            # Serialize data to JSON
            json_data = json.dumps(data)

            # Create text message
            message = self.session.createTextMessage(json_data)

            # Send message
            self.producer.send(message)

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to TIBCO EMS {self.dest_type}/{self.dest_name}")

        except Exception as e:
            logger.error(f"Error publishing to TIBCO EMS: {str(e)}")
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            self._connected = False
            
            if self._auto_reconnect:
                try:
                    self._reconnect_if_needed()
                    # Retry the publish
                    json_data = json.dumps(data)
                    message = self.session.createTextMessage(json_data)
                    self.producer.send(message)
                    logger.info("Retry publish succeeded after reconnection")
                except Exception as retry_error:
                    logger.error(f"Retry publish failed: {retry_error}")
                    raise
            else:
                raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        self._close_connections()
        self._connected = False
        logger.info(f"TIBCO EMS publisher {self.name} stopped")


class TibcoEMSDataSubscriber(DataSubscriber):
    """Subscriber for TIBCO EMS queues and topics with v1.7.6 connection resilience."""

    def __init__(self, name, source, config, given_queue: queue.Queue = None):
        super().__init__(name, source, config, given_queue)

        if not tibcoems:
            raise ImportError("tibcoems library required for TIBCO EMS. Install TIBCO EMS Python client")

        # Parse source: tibcoems://queue/queue_name or tibcoems://topic/topic_name
        parts = source.replace('tibcoems://', '').split('/')
        self.dest_type = parts[0]  # 'queue' or 'topic'
        self.dest_name = parts[1] if len(parts) > 1 else 'default'

        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 7222)
        self.username = config.get('username', 'admin')
        self.password = config.get('password', 'admin')

        # Connection URL for TIBCO EMS
        self.server_url = config.get('server_url', f'tcp://{self.host}:{self.port}')

        # SSL settings
        self.use_ssl = config.get('use_ssl', False)
        if self.use_ssl:
            self.server_url = config.get('server_url', f'ssl://{self.host}:{self.port}')

        # Message selector for filtering
        self.message_selector = config.get('message_selector', None)

        # Durable subscriber settings (for topics only)
        self.durable = config.get('durable', False)
        self.client_id = config.get('client_id', f'{name}_client')
        self.durable_name = config.get('durable_name', f'{name}_durable')

        # v1.7.6: Connection retry configuration
        self._max_retries = config.get('max_retries', 5)
        self._retry_delay = config.get('retry_delay', 3)
        self._auto_reconnect = config.get('auto_reconnect', True)
        self._connected = False
        self._stopping = False

        # Connect to TIBCO EMS
        self.connection = None
        self.session = None
        self.consumer = None
        self.destination_obj = None
        
        # v1.7.6: Connect with retry logic
        self._connect_with_retry()

        logger.info(f"TIBCO EMS subscriber connected to {self.server_url}, "
                    f"dest_type={self.dest_type}, dest_name={self.dest_name}")

    def _do_connect(self):
        """Establish connection to TIBCO EMS"""
        # Create connection factory
        factory = tibcoems.ConnectionFactory()
        factory.setServerUrl(self.server_url)

        # Create connection
        self.connection = factory.createConnection(self.username, self.password)

        # Set client ID if using durable subscriber
        if self.durable and self.dest_type == 'topic':
            self.connection.setClientID(self.client_id)

        # Create session
        self.session = self.connection.createSession(
            False,  # Not transacted
            tibcoems.Session.AUTO_ACKNOWLEDGE
        )

        # Create destination
        if self.dest_type == 'queue':
            self.destination_obj = self.session.createQueue(self.dest_name)
            # Create consumer for queue
            if self.message_selector:
                self.consumer = self.session.createConsumer(
                    self.destination_obj,
                    self.message_selector
                )
            else:
                self.consumer = self.session.createConsumer(self.destination_obj)
        else:  # topic
            self.destination_obj = self.session.createTopic(self.dest_name)
            # Create consumer for topic (durable or non-durable)
            if self.durable:
                if self.message_selector:
                    self.consumer = self.session.createDurableSubscriber(
                        self.destination_obj,
                        self.durable_name,
                        self.message_selector,
                        False  # noLocal
                    )
                else:
                    self.consumer = self.session.createDurableSubscriber(
                        self.destination_obj,
                        self.durable_name
                    )
            else:
                if self.message_selector:
                    self.consumer = self.session.createConsumer(
                        self.destination_obj,
                        self.message_selector
                    )
                else:
                    self.consumer = self.session.createConsumer(self.destination_obj)

        # Start connection
        self.connection.start()
        self._connected = True

    def _connect_with_retry(self):
        """v1.7.6: Establish connection to TIBCO EMS with retry logic"""
        last_error = None

        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(f"TIBCO EMS subscriber connection attempt {attempt}/{self._max_retries} to {self.server_url}")
                self._do_connect()
                logger.info(f"TIBCO EMS subscriber connected successfully (attempt={attempt})")
                return
            except Exception as e:
                last_error = e
                logger.warning(f"TIBCO EMS subscriber connection attempt {attempt}/{self._max_retries} failed: {e}")
                if attempt < self._max_retries:
                    logger.info(f"Retrying in {self._retry_delay} seconds...")
                    time.sleep(self._retry_delay)

        logger.error(f"Failed to connect TIBCO EMS subscriber after {self._max_retries} attempts: {last_error}")
        raise ConnectionError(f"Could not connect to TIBCO EMS {self.server_url}: {last_error}")

    # Keep old method name for compatibility
    def _connect(self):
        """Alias for _connect_with_retry"""
        self._connect_with_retry()

    def _reconnect_if_needed(self):
        """v1.7.6: Reconnect if connection is broken"""
        if not self._auto_reconnect or self._stopping:
            return
            
        if self._connected and self.connection and self.session and self.consumer:
            return  # Assume connection is healthy
        
        self._connected = False
        logger.info("TIBCO EMS subscriber connection lost, attempting reconnection...")
        
        try:
            self._close_connections()
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"TIBCO EMS subscriber reconnection failed: {e}")

    def _close_connections(self):
        """Safely close existing connections"""
        try:
            if self.consumer:
                self.consumer.close()
        except:
            pass
        try:
            if self.session:
                if self.durable and self.dest_type == 'topic':
                    try:
                        self.session.unsubscribe(self.durable_name)
                    except:
                        pass
                self.session.close()
        except:
            pass
        try:
            if self.connection:
                self.connection.close()
        except:
            pass
        self.consumer = None
        self.session = None
        self.connection = None

    def _do_subscribe(self):
        """Subscribe from TIBCO EMS with auto-reconnection"""
        try:
            # v1.7.6: Check and reconnect if needed
            self._reconnect_if_needed()

            # Receive message with timeout (100ms)
            message = self.consumer.receive(100)

            if message:
                # Check if it's a text message
                if isinstance(message, tibcoems.TextMessage):
                    text = message.getText()
                    # v1.7.2: Use smart deserializer for non-JSON message handling
                    data = smart_deserialize(text, f"tibcoems:{self.name}")
                    logger.debug(f"Received message from TIBCO EMS {self.dest_type}/{self.dest_name}")
                    return data
                else:
                    logger.warning(f"Received non-text message, skipping")
                    return None
            else:
                # No messages available
                return None

        except tibcoems.EMSException as e:
            logger.error(f"TIBCO EMS error: {str(e)}")
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            self._connected = False
            
            if self._auto_reconnect and not self._stopping:
                try:
                    self._reconnect_if_needed()
                except:
                    pass
            time.sleep(1)
            return None

        except Exception as e:
            # v1.7.2 Policy: Full stack trace for all exceptions
            logger.error(f"Error subscribing from TIBCO EMS: {str(e)}")
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            self._connected = False
            return None

    def stop(self):
        """Stop the subscriber"""
        self._stopping = True
        super().stop()
        self._close_connections()
        self._connected = False
        logger.info(f"TIBCO EMS subscriber {self.name} stopped")
