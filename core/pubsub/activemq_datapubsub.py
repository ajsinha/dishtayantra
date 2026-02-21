import json
import logging
import time
import traceback
import threading
from datetime import datetime
import stomp
from core.pubsub.datapubsub import DataPublisher, DataSubscriber, DataAwarePayload, smart_deserialize
import queue
logger = logging.getLogger(__name__)


class ActiveMQDataPublisher(DataPublisher):
    """Publisher for ActiveMQ queues and topics with retry and recovery support."""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        # Parse destination: activemq://queue/queue_name or activemq://topic/topic_name
        parts = destination.replace('activemq://', '').split('/')
        self.dest_type = parts[0]  # 'queue' or 'topic'
        self.dest_name = parts[1]

        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 61613)
        self._username = config.get('username', 'admin')
        self._password = config.get('password', 'admin')
        
        # v1.7.6: Connection retry configuration
        self._max_retries = config.get('max_retries', 5)
        self._retry_delay = config.get('retry_delay', 3)
        self._auto_reconnect = config.get('auto_reconnect', True)
        self._connected = False
        self.connection = None

        # v1.7.6: Connect with retry logic
        self._connect_with_retry()

    def _connect_with_retry(self):
        """v1.7.6: Establish ActiveMQ connection with retry logic."""
        last_error = None
        
        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(f"ActiveMQ publisher connection attempt {attempt}/{self._max_retries} to {self.host}:{self.port}")
                
                self.connection = stomp.Connection([(self.host, self.port)])
                self.connection.connect(self._username, self._password, wait=True)
                
                self._connected = True
                logger.info(f"ActiveMQ publisher connected successfully (attempt={attempt})")
                return
                
            except Exception as e:
                last_error = e
                logger.warning(f"ActiveMQ publisher connection attempt {attempt}/{self._max_retries} failed: {e}")
                
                if attempt < self._max_retries:
                    logger.info(f"Retrying in {self._retry_delay} seconds...")
                    time.sleep(self._retry_delay)
        
        logger.error(f"Failed to connect ActiveMQ publisher after {self._max_retries} attempts: {last_error}")
        raise ConnectionError(f"Could not connect to ActiveMQ {self.host}:{self.port}: {last_error}")

    def _reconnect_if_needed(self):
        """v1.7.6: Reconnect if connection is broken."""
        if not self._auto_reconnect:
            return
        
        try:
            if self.connection and self.connection.is_connected():
                return  # Connection is healthy
        except:
            pass
        
        self._connected = False
        logger.info("ActiveMQ publisher connection lost, attempting reconnection...")
        
        try:
            if self.connection:
                try:
                    self.connection.disconnect()
                except:
                    pass
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"ActiveMQ publisher reconnection failed: {e}")
            raise

    def _do_publish(self, data):
        """Publish to ActiveMQ queue or topic with auto-reconnection."""
        try:
            # v1.7.6: Check and reconnect if needed
            self._reconnect_if_needed()
            
            destination = f'/queue/{self.dest_name}' if self.dest_type == 'queue' else f'/topic/{self.dest_name}'

            local_destination = destination
            local_data = data
            if isinstance(data, DataAwarePayload):
                local_destination, local_data = data.get_data_for_publication()
                if local_destination is None or len(local_destination) == 0:
                    local_destination = destination

            self.connection.send(local_destination, json.dumps(local_data))

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to ActiveMQ {self.name}/{local_destination}")
        except Exception as e:
            logger.error(f"Error publishing to ActiveMQ: {str(e)}")
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            self._connected = False
            
            if self._auto_reconnect:
                try:
                    self._reconnect_if_needed()
                    # Retry the publish
                    self.connection.send(local_destination, json.dumps(local_data))
                except Exception as retry_error:
                    logger.error(f"Retry publish failed: {retry_error}")
                    raise
            else:
                raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        if self.connection:
            try:
                if self.connection.is_connected():
                    self.connection.disconnect()
            except:
                pass
        self._connected = False


class ActiveMQListener(stomp.ConnectionListener):
    """Listener for ActiveMQ messages with reconnection support."""

    def __init__(self, subscriber):
        self.subscriber = subscriber

    def on_message(self, frame):
        try:
            # v1.7.2: Use smart deserializer for non-JSON message handling
            data = smart_deserialize(frame.body, f"activemq:{self.subscriber.name}")
            
            # v1.7.2 Policy: Log every message received
            self._log_message(data)
            
            self.subscriber._internal_queue.put(data, timeout=1)
            with self.subscriber._lock:
                self.subscriber._last_receive = datetime.now().isoformat()
                self.subscriber._receive_count += 1
        except Exception as e:
            # v1.7.2 Policy: Full stack trace for all exceptions
            logger.error(f"Error processing ActiveMQ message: {str(e)}")
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")

    def _log_message(self, data):
        """v1.7.2 Policy: Log every message received"""
        try:
            if isinstance(data, dict):
                preview = json.dumps(data)[:500]
            else:
                preview = str(data)[:500]
            
            logger.info(f"MESSAGE RECEIVED [activemq:{self.subscriber.name}]:")
            logger.info(f"  Source: {self.subscriber.source}")
            logger.info(f"  Type: {type(data).__name__}")
            logger.info(f"  Count: {self.subscriber._receive_count + 1}")
            logger.info(f"  Preview: {preview}")
        except Exception as e:
            logger.warning(f"Could not log message: {e}")

    def on_error(self, frame):
        logger.error(f"ActiveMQ error: {frame.body}")
        logger.error(f"Full stack trace:\n{traceback.format_exc()}")

    def on_disconnected(self):
        """v1.7.6: Handle disconnection and trigger reconnection."""
        logger.warning(f"ActiveMQ subscriber {self.subscriber.name} disconnected")
        self.subscriber._connected = False
        
        if self.subscriber._auto_reconnect and not self.subscriber._stopping:
            logger.info("Scheduling reconnection...")
            # Use a thread to avoid blocking
            threading.Thread(target=self.subscriber._reconnect_if_needed, daemon=True).start()


class ActiveMQDataSubscriber(DataSubscriber):
    """Subscriber for ActiveMQ queues and topics with retry and recovery support."""

    def __init__(self, name, source, config, given_queue: queue.Queue=None):
        # Don't call super().__init__ yet as we need to override the subscription loop
        self.name = name
        self.source = source
        self.config = config
        self.max_depth = config.get('max_depth', 100000)

        self._internal_queue = given_queue
        if given_queue is None:
            self._internal_queue = queue.Queue(maxsize=self.max_depth)
        self._stop_event = None
        self._suspend_event = None
        self._last_receive = None
        self._receive_count = 0
        self._lock = threading.Lock()
        self._stopping = False

        # Parse source: activemq://queue/queue_name or activemq://topic/topic_name
        parts = source.replace('activemq://', '').split('/')
        self.dest_type = parts[0]
        self.dest_name = parts[1]

        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 61613)
        self._username = config.get('username', 'admin')
        self._password = config.get('password', 'admin')
        
        # v1.7.6: Connection retry configuration
        self._max_retries = config.get('max_retries', 5)
        self._retry_delay = config.get('retry_delay', 3)
        self._auto_reconnect = config.get('auto_reconnect', True)
        self._connected = False
        self.connection = None
        self._listener = None

        # v1.7.6: Connect with retry logic
        self._connect_with_retry()

    def _connect_with_retry(self):
        """v1.7.6: Establish ActiveMQ connection with retry logic."""
        last_error = None
        destination = f'/queue/{self.dest_name}' if self.dest_type == 'queue' else f'/topic/{self.dest_name}'
        
        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(f"ActiveMQ subscriber connection attempt {attempt}/{self._max_retries} to {self.host}:{self.port}")
                
                self.connection = stomp.Connection([(self.host, self.port)])
                self._listener = ActiveMQListener(self)
                self.connection.set_listener('', self._listener)
                self.connection.connect(self._username, self._password, wait=True)
                self.connection.subscribe(destination=destination, id=1, ack='auto')
                
                self._connected = True
                logger.info(f"ActiveMQ subscriber connected successfully to {destination} (attempt={attempt})")
                return
                
            except Exception as e:
                last_error = e
                logger.warning(f"ActiveMQ subscriber connection attempt {attempt}/{self._max_retries} failed: {e}")
                
                if attempt < self._max_retries:
                    logger.info(f"Retrying in {self._retry_delay} seconds...")
                    time.sleep(self._retry_delay)
        
        logger.error(f"Failed to connect ActiveMQ subscriber after {self._max_retries} attempts: {last_error}")
        raise ConnectionError(f"Could not connect to ActiveMQ {self.host}:{self.port}: {last_error}")

    def _reconnect_if_needed(self):
        """v1.7.6: Reconnect if connection is broken."""
        if not self._auto_reconnect or self._stopping:
            return
        
        if self._connected:
            try:
                if self.connection and self.connection.is_connected():
                    return  # Connection is healthy
            except:
                pass
        
        self._connected = False
        logger.info("ActiveMQ subscriber connection lost, attempting reconnection...")
        
        try:
            if self.connection:
                try:
                    self.connection.disconnect()
                except:
                    pass
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"ActiveMQ subscriber reconnection failed: {e}")

    def _do_subscribe(self):
        """Messages are received via listener, this is not used"""
        time.sleep(0.1)
        return None

    def start(self):
        """ActiveMQ uses push model, no need for polling thread"""
        logger.info(f"Subscriber {self.name} is active (push model)")

    def suspend(self):
        """Suspend subscription"""
        logger.info(f"Subscriber {self.name} suspended")

    def resume(self):
        """Resume subscription"""
        logger.info(f"Subscriber {self.name} resumed")

    def stop(self):
        """Stop the subscriber"""
        self._stopping = True
        if self.connection:
            try:
                if self.connection.is_connected():
                    self.connection.disconnect()
            except:
                pass
        self._connected = False
        logger.info(f"Subscriber {self.name} stopped")