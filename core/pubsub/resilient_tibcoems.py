import logging
import time
import threading
from queue import Queue, Empty
from typing import Dict, Optional, Any, List, Callable, Tuple
from dataclasses import dataclass
from enum import Enum

# TIBCO EMS Python client imports
# Note: The exact import may vary based on your TIBCO EMS Python installation
try:
    import TIBCO.EMS as tibems
except ImportError:
    try:
        import tibems
    except ImportError:
        # Fallback for different installation methods
        import pyems as tibems

logger = logging.getLogger(__name__)


class DeliveryMode(Enum):
    """JMS Delivery Modes."""
    NON_PERSISTENT = tibems.DeliveryMode.NON_PERSISTENT
    PERSISTENT = tibems.DeliveryMode.PERSISTENT


class AcknowledgeMode(Enum):
    """JMS Acknowledge Modes."""
    AUTO_ACKNOWLEDGE = tibems.Session.AUTO_ACKNOWLEDGE
    CLIENT_ACKNOWLEDGE = tibems.Session.CLIENT_ACKNOWLEDGE
    DUPS_OK_ACKNOWLEDGE = tibems.Session.DUPS_OK_ACKNOWLEDGE
    SESSION_TRANSACTED = tibems.Session.SESSION_TRANSACTED
    EXPLICIT_CLIENT_ACKNOWLEDGE = tibems.Session.EXPLICIT_CLIENT_ACKNOWLEDGE
    EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE = tibems.Session.EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE
    NO_ACKNOWLEDGE = tibems.Session.NO_ACKNOWLEDGE


@dataclass
class BufferedMessage:
    """Represents a buffered EMS message."""
    destination: str
    message_body: Any
    message_type: str  # 'text', 'bytes', 'map', 'object', 'stream'
    properties: Dict[str, Any]
    headers: Dict[str, Any]
    delivery_mode: int
    priority: int
    time_to_live: int
    correlation_id: Optional[str]
    reply_to: Optional[str]
    callback: Optional[Callable]


@dataclass
class Subscription:
    """Represents a subscription to a destination."""
    destination: str
    destination_type: str  # 'queue' or 'topic'
    message_listener: Optional[Callable]
    selector: Optional[str]
    durable: bool
    subscription_name: Optional[str]
    no_local: bool


class ResilientSession:
    """A resilient wrapper around EMS Session."""

    def __init__(self, connection, acknowledge_mode: AcknowledgeMode = AcknowledgeMode.AUTO_ACKNOWLEDGE):
        self.connection = connection
        self.acknowledge_mode = acknowledge_mode
        self._session = None
        self._consumers = {}  # destination -> consumer mapping
        self._producers = {}  # destination -> producer mapping
        self._subscriptions = {}  # subscription_id -> Subscription mapping
        self._transacted = (acknowledge_mode == AcknowledgeMode.SESSION_TRANSACTED)
        self._temp_queues = []  # Track temporary queues
        self._temp_topics = []  # Track temporary topics

    def _get_session(self):
        """Get or create the underlying session."""
        if self._session is None or not self._is_session_valid():
            self._session = self.connection._create_session(
                transacted=self._transacted,
                acknowledge_mode=self.acknowledge_mode.value
            )
            self._restore_session_state()
        return self._session

    def _is_session_valid(self):
        """Check if the session is still valid."""
        try:
            # Try to create a temporary destination to test session
            if self._session:
                temp = self._session.createTemporaryQueue()
                temp.delete()
                return True
        except:
            return False
        return False

    def _restore_session_state(self):
        """Restore session state after reconnection."""
        if not self._session:
            return

        # Restore temporary destinations
        for temp_queue_name in self._temp_queues:
            try:
                self._session.createTemporaryQueue()
                logger.debug(f"Restored temporary queue")
            except Exception as e:
                logger.error(f"Failed to restore temporary queue: {e}")

        for temp_topic_name in self._temp_topics:
            try:
                self._session.createTemporaryTopic()
                logger.debug(f"Restored temporary topic")
            except Exception as e:
                logger.error(f"Failed to restore temporary topic: {e}")

        # Restore consumers and subscriptions
        for sub_id, subscription in self._subscriptions.items():
            try:
                self._restore_subscription(subscription)
                logger.debug(f"Restored subscription to {subscription.destination}")
            except Exception as e:
                logger.error(f"Failed to restore subscription to {subscription.destination}: {e}")

    def _restore_subscription(self, subscription: Subscription):
        """Restore a single subscription."""
        if subscription.destination_type == 'queue':
            destination = self._session.createQueue(subscription.destination)
        else:
            destination = self._session.createTopic(subscription.destination)

        if subscription.durable and subscription.destination_type == 'topic':
            consumer = self._session.createDurableSubscriber(
                destination,
                subscription.subscription_name,
                subscription.selector,
                subscription.no_local
            )
        else:
            consumer = self._session.createConsumer(
                destination,
                messageSelector=subscription.selector
            )

        if subscription.message_listener:
            consumer.setMessageListener(subscription.message_listener)

        self._consumers[subscription.destination] = consumer

    def create_producer(self, destination: str, destination_type: str = 'queue'):
        """Create a message producer with resilience."""
        session = self._get_session()

        if destination_type == 'queue':
            dest_obj = session.createQueue(destination)
        else:
            dest_obj = session.createTopic(destination)

        producer = session.createProducer(dest_obj)
        self._producers[destination] = producer
        return producer

    def create_consumer(self, destination: str, destination_type: str = 'queue',
                        message_selector: Optional[str] = None,
                        message_listener: Optional[Callable] = None,
                        durable: bool = False, subscription_name: Optional[str] = None,
                        no_local: bool = False):
        """Create a message consumer with resilience."""
        session = self._get_session()

        # Create subscription object for tracking
        subscription = Subscription(
            destination=destination,
            destination_type=destination_type,
            message_listener=message_listener,
            selector=message_selector,
            durable=durable,
            subscription_name=subscription_name or f"sub_{destination}",
            no_local=no_local
        )

        sub_id = f"{destination_type}:{destination}"
        self._subscriptions[sub_id] = subscription

        # Create the actual consumer
        self._restore_subscription(subscription)

        return self._consumers[destination]

    def create_browser(self, queue_name: str, message_selector: Optional[str] = None):
        """Create a queue browser."""
        session = self._get_session()
        queue = session.createQueue(queue_name)
        return session.createBrowser(queue, message_selector)

    def create_temporary_queue(self):
        """Create a temporary queue."""
        session = self._get_session()
        temp_queue = session.createTemporaryQueue()
        self._temp_queues.append(temp_queue.getQueueName())
        return temp_queue

    def create_temporary_topic(self):
        """Create a temporary topic."""
        session = self._get_session()
        temp_topic = session.createTemporaryTopic()
        self._temp_topics.append(temp_topic.getTopicName())
        return temp_topic

    def unsubscribe(self, subscription_name: str):
        """Unsubscribe from a durable subscription."""
        session = self._get_session()
        session.unsubscribe(subscription_name)

        # Remove from tracked subscriptions
        self._subscriptions = {k: v for k, v in self._subscriptions.items()
                               if v.subscription_name != subscription_name}

    def send(self, destination: str, message: Any, destination_type: str = 'queue',
             delivery_mode: int = DeliveryMode.PERSISTENT.value,
             priority: int = tibems.Message.DEFAULT_PRIORITY,
             time_to_live: int = tibems.Message.DEFAULT_TIME_TO_LIVE,
             properties: Optional[Dict] = None, headers: Optional[Dict] = None,
             correlation_id: Optional[str] = None, reply_to: Optional[str] = None):
        """Send a message with automatic buffering on failure."""
        return self.connection._send_with_retry(
            self, destination, message, destination_type,
            delivery_mode, priority, time_to_live,
            properties, headers, correlation_id, reply_to
        )

    def receive(self, destination: str, destination_type: str = 'queue',
                timeout: int = 0, selector: Optional[str] = None):
        """Receive a message with timeout."""
        if destination not in self._consumers:
            self.create_consumer(destination, destination_type, message_selector=selector)

        consumer = self._consumers[destination]

        for attempt in range(1, self.connection.reconnect_tries + 1):
            try:
                if timeout == 0:
                    return consumer.receiveNoWait()
                elif timeout < 0:
                    return consumer.receive()
                else:
                    return consumer.receive(timeout)

            except Exception as e:
                logger.warning(f"Receive failed (attempt {attempt}/{self.connection.reconnect_tries}): {e}")
                if attempt < self.connection.reconnect_tries:
                    time.sleep(self.connection.reconnect_interval_seconds)
                    self.connection._ensure_connection()
                else:
                    raise

    def commit(self):
        """Commit the current transaction."""
        if self._transacted and self._session:
            for attempt in range(1, self.connection.reconnect_tries + 1):
                try:
                    self._session.commit()
                    return
                except Exception as e:
                    logger.warning(f"Commit failed (attempt {attempt}/{self.connection.reconnect_tries}): {e}")
                    if attempt < self.connection.reconnect_tries:
                        time.sleep(self.connection.reconnect_interval_seconds)
                        self.connection._ensure_connection()
                    else:
                        raise

    def rollback(self):
        """Rollback the current transaction."""
        if self._transacted and self._session:
            self._session.rollback()

    def recover(self):
        """Stop message delivery and restart with oldest unacknowledged message."""
        if self._session:
            self._session.recover()

    def close(self):
        """Close the session."""
        # Close all consumers
        for consumer in self._consumers.values():
            try:
                consumer.close()
            except:
                pass

        # Close all producers
        for producer in self._producers.values():
            try:
                producer.close()
            except:
                pass

        # Close session
        if self._session:
            try:
                self._session.close()
            except Exception as e:
                logger.debug(f"Error closing session: {e}")
            self._session = None


class ResilientTibcoEMSConnection:
    """
    A resilient TIBCO EMS connection wrapper that automatically handles reconnection
    on failures and buffers messages during reconnection to prevent message loss.

    This class provides automatic reconnection capabilities when connection failures
    occur during initialization, session operations, or message sending/receiving.
    Messages sent during reconnection attempts are buffered and retried once
    connection is restored.

    Args:
        server_url: TIBCO EMS server URL (e.g., 'tcp://localhost:7222')
        username: Username for authentication
        password: Password for authentication
        reconnect_tries: Maximum number of reconnection attempts (default: 10)
        reconnect_interval_seconds: Sleep interval between reconnection attempts (default: 60)
        buffer_max_messages: Maximum messages to buffer during reconnection (default: 10000)
        client_id: Optional client ID for durable subscriptions
        connection_factory_params: Additional ConnectionFactory parameters
    """

    def __init__(self,
                 server_url: str = 'tcp://localhost:7222',
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 reconnect_tries: int = 10,
                 reconnect_interval_seconds: int = 60,
                 buffer_max_messages: int = 10000,
                 client_id: Optional[str] = None,
                 connection_factory_params: Optional[Dict] = None):
        """Initialize the ResilientTibcoEMSConnection with reconnection and buffering."""

        # Connection parameters
        self.server_url = server_url
        self.username = username
        self.password = password
        self.client_id = client_id
        self.connection_factory_params = connection_factory_params or {}

        # Reconnection settings
        self.reconnect_tries = reconnect_tries
        self.reconnect_interval_seconds = reconnect_interval_seconds
        self.buffer_max_messages = buffer_max_messages

        # Connection state
        self._connection = None
        self._connection_factory = None
        self._current_retry = 0
        self._connected = False
        self._reconnecting = False
        self._lock = threading.Lock()
        self._exception_listener = None

        # Session management
        self._sessions = []

        # Message buffer for storing messages during reconnection
        self._message_buffer = Queue(maxsize=buffer_max_messages)
        self._failed_messages = []

        # Initialize connection with retry logic
        self._connect_with_retry()

        # Start background thread for processing buffered messages
        self._stop_buffer_processor = threading.Event()
        self._buffer_processor_thread = threading.Thread(
            target=self._process_buffered_messages,
            daemon=True
        )
        self._buffer_processor_thread.start()

    def _connect_with_retry(self):
        """Establish connection to TIBCO EMS with retry logic."""
        last_exception = None

        for attempt in range(1, self.reconnect_tries + 1):
            try:
                logger.info(f"Attempting to connect to TIBCO EMS (attempt {attempt}/{self.reconnect_tries})")

                # Create ConnectionFactory
                self._connection_factory = tibems.ConnectionFactory(self.server_url)

                # Apply any additional connection factory parameters
                for param, value in self.connection_factory_params.items():
                    if hasattr(self._connection_factory, f'set{param}'):
                        getattr(self._connection_factory, f'set{param}')(value)

                # Create Connection
                if self.username and self.password:
                    self._connection = self._connection_factory.createConnection(
                        self.username, self.password
                    )
                else:
                    self._connection = self._connection_factory.createConnection()

                # Set client ID if provided
                if self.client_id:
                    self._connection.setClientID(self.client_id)

                # Set exception listener
                self._connection.setExceptionListener(self._on_exception)

                # Start the connection
                self._connection.start()

                self._connected = True
                self._reconnecting = False
                self._current_retry = 0
                logger.info("Successfully connected to TIBCO EMS")

                # Restore sessions
                self._restore_sessions()

                return

            except Exception as e:
                last_exception = e
                logger.warning(f"Failed to connect to TIBCO EMS (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    logger.info(f"Retrying in {self.reconnect_interval_seconds} seconds...")
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    logger.error(f"Failed to connect to TIBCO EMS after {self.reconnect_tries} attempts")
                    raise last_exception

    def _on_exception(self, exception):
        """Handle connection exceptions."""
        logger.error(f"Connection exception: {exception}")
        if not self._reconnecting:
            threading.Thread(target=self._reconnect, daemon=True).start()

    def _ensure_connection(self):
        """Ensure EMS connection is active, reconnect if necessary."""
        try:
            # Check connection health by creating and closing a session
            if self._connection and self._connected:
                test_session = self._connection.createSession(
                    False, tibems.Session.AUTO_ACKNOWLEDGE
                )
                test_session.close()
                return True
        except:
            pass

        logger.warning("Connection lost, attempting to reconnect...")
        self._reconnect()
        return self._connected

    def _reconnect(self):
        """Attempt to reconnect to TIBCO EMS."""
        with self._lock:
            if self._reconnecting:
                return
            self._reconnecting = True

        logger.info("Starting reconnection process...")

        try:
            # Reset connection state
            self._connected = False

            # Close existing connection
            if self._connection:
                try:
                    self._connection.stop()
                    self._connection.close()
                except Exception as e:
                    logger.debug(f"Error closing existing connection: {e}")

            # Reconnect with retry logic
            self._connect_with_retry()

            # Flush buffered messages
            self._flush_buffer()

        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            raise
        finally:
            with self._lock:
                self._reconnecting = False

    def _restore_sessions(self):
        """Restore all sessions after reconnection."""
        logger.info(f"Restoring {len(self._sessions)} sessions")

        for session in self._sessions:
            try:
                # Force session recreation
                session._session = None
                session._get_session()
                logger.debug(f"Restored session")
            except Exception as e:
                logger.error(f"Failed to restore session: {e}")

    def create_session(self, transacted: bool = False,
                       acknowledge_mode: AcknowledgeMode = AcknowledgeMode.AUTO_ACKNOWLEDGE) -> ResilientSession:
        """Create a new resilient session."""
        session = ResilientSession(self, acknowledge_mode)
        self._sessions.append(session)
        return session

    def _create_session(self, transacted: bool, acknowledge_mode: int):
        """Create the underlying EMS session."""
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                if not self._connected:
                    self._ensure_connection()

                return self._connection.createSession(transacted, acknowledge_mode)

            except Exception as e:
                logger.warning(f"Failed to create session (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                    self._ensure_connection()
                else:
                    raise

    def _send_with_retry(self, session: ResilientSession, destination: str,
                         message: Any, destination_type: str,
                         delivery_mode: int, priority: int, time_to_live: int,
                         properties: Optional[Dict], headers: Optional[Dict],
                         correlation_id: Optional[str], reply_to: Optional[str]):
        """Send a message with automatic buffering on failure."""
        # Buffer message if reconnecting
        if self._reconnecting or not self._connected:
            logger.info(f"Connection unavailable, buffering message for destination: {destination}")
            self._buffer_message(
                destination, message, destination_type,
                delivery_mode, priority, time_to_live,
                properties, headers, correlation_id, reply_to
            )
            return

        # Try to send with retry logic
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                # Get or create producer
                if destination not in session._producers:
                    session.create_producer(destination, destination_type)

                producer = session._producers[destination]
                ems_session = session._get_session()

                # Create message based on type
                if isinstance(message, str):
                    ems_message = ems_session.createTextMessage(message)
                elif isinstance(message, bytes):
                    ems_message = ems_session.createBytesMessage()
                    ems_message.writeBytes(message)
                elif isinstance(message, dict):
                    ems_message = ems_session.createMapMessage()
                    for key, value in message.items():
                        if isinstance(value, str):
                            ems_message.setString(key, value)
                        elif isinstance(value, int):
                            ems_message.setInt(key, value)
                        elif isinstance(value, float):
                            ems_message.setDouble(key, value)
                        elif isinstance(value, bool):
                            ems_message.setBoolean(key, value)
                        elif isinstance(value, bytes):
                            ems_message.setBytes(key, value)
                        else:
                            ems_message.setObject(key, value)
                elif isinstance(message, list):
                    ems_message = ems_session.createStreamMessage()
                    for item in message:
                        ems_message.writeObject(item)
                else:
                    ems_message = ems_session.createObjectMessage(message)

                # Set message properties
                if properties:
                    for key, value in properties.items():
                        if isinstance(value, str):
                            ems_message.setStringProperty(key, value)
                        elif isinstance(value, int):
                            ems_message.setIntProperty(key, value)
                        elif isinstance(value, float):
                            ems_message.setDoubleProperty(key, value)
                        elif isinstance(value, bool):
                            ems_message.setBooleanProperty(key, value)
                        else:
                            ems_message.setObjectProperty(key, value)

                # Set correlation ID and reply-to if provided
                if correlation_id:
                    ems_message.setJMSCorrelationID(correlation_id)
                if reply_to:
                    if '/' in reply_to:
                        reply_dest = ems_session.createQueue(
                            reply_to) if 'queue' in reply_to else ems_session.createTopic(reply_to)
                        ems_message.setJMSReplyTo(reply_dest)

                # Send message
                producer.send(
                    ems_message,
                    deliveryMode=delivery_mode,
                    priority=priority,
                    timeToLive=time_to_live
                )

                self._current_retry = 0
                return

            except Exception as e:
                logger.warning(f"Send failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                # Buffer message on first failure
                if attempt == 1:
                    self._buffer_message(
                        destination, message, destination_type,
                        delivery_mode, priority, time_to_live,
                        properties, headers, correlation_id, reply_to
                    )

                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                    self._ensure_connection()
                else:
                    logger.error(f"Send failed after {self.reconnect_tries} attempts")
                    raise

    def _buffer_message(self, destination: str, message: Any, destination_type: str,
                        delivery_mode: int, priority: int, time_to_live: int,
                        properties: Optional[Dict], headers: Optional[Dict],
                        correlation_id: Optional[str], reply_to: Optional[str]):
        """Buffer a message for later sending when connection is restored."""
        try:
            # Determine message type
            if isinstance(message, str):
                message_type = 'text'
            elif isinstance(message, bytes):
                message_type = 'bytes'
            elif isinstance(message, dict):
                message_type = 'map'
            elif isinstance(message, list):
                message_type = 'stream'
            else:
                message_type = 'object'

            buffered_msg = BufferedMessage(
                destination=destination,
                message_body=message,
                message_type=message_type,
                properties=properties or {},
                headers=headers or {},
                delivery_mode=delivery_mode,
                priority=priority,
                time_to_live=time_to_live,
                correlation_id=correlation_id,
                reply_to=reply_to,
                callback=None
            )

            if self._message_buffer.full():
                # Remove oldest message to make room
                old_msg = self._message_buffer.get_nowait()
                logger.warning(f"Buffer full, dropping oldest message for destination: {old_msg.destination}")

            self._message_buffer.put_nowait(buffered_msg)
            logger.debug(
                f"Buffered message for destination: {destination}, buffer size: {self._message_buffer.qsize()}")

        except Exception as e:
            logger.error(f"Failed to buffer message: {e}")

    def _process_buffered_messages(self):
        """Background thread to process buffered messages when connection is restored."""
        while not self._stop_buffer_processor.is_set():
            try:
                if self._connected and not self._reconnecting and not self._message_buffer.empty():
                    try:
                        buffered_msg = self._message_buffer.get(timeout=1)

                        # Create a session for sending if needed
                        if not self._sessions:
                            session = self.create_session()
                        else:
                            session = self._sessions[0]

                        # Send the buffered message
                        session.send(
                            destination=buffered_msg.destination,
                            message=buffered_msg.message_body,
                            destination_type='queue',  # Default to queue
                            delivery_mode=buffered_msg.delivery_mode,
                            priority=buffered_msg.priority,
                            time_to_live=buffered_msg.time_to_live,
                            properties=buffered_msg.properties,
                            headers=buffered_msg.headers,
                            correlation_id=buffered_msg.correlation_id,
                            reply_to=buffered_msg.reply_to
                        )

                        logger.debug(f"Successfully sent buffered message to: {buffered_msg.destination}")

                    except Empty:
                        continue
                    except Exception as e:
                        logger.error(f"Failed to send buffered message: {e}")
                        # Re-queue the message for retry
                        self._message_buffer.put(buffered_msg)
                        time.sleep(0.1)
                else:
                    time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in buffer processor thread: {e}")
                time.sleep(1)

    def _flush_buffer(self):
        """Flush all buffered messages after reconnection."""
        logger.info(f"Flushing {self._message_buffer.qsize()} buffered messages")

        flushed_count = 0
        failed_count = 0

        # Create a default session for flushing if needed
        if not self._sessions:
            default_session = self.create_session()
        else:
            default_session = self._sessions[0]

        while not self._message_buffer.empty():
            try:
                buffered_msg = self._message_buffer.get_nowait()

                default_session.send(
                    destination=buffered_msg.destination,
                    message=buffered_msg.message_body,
                    destination_type='queue',
                    delivery_mode=buffered_msg.delivery_mode,
                    priority=buffered_msg.priority,
                    time_to_live=buffered_msg.time_to_live,
                    properties=buffered_msg.properties,
                    headers=buffered_msg.headers,
                    correlation_id=buffered_msg.correlation_id,
                    reply_to=buffered_msg.reply_to
                )

                flushed_count += 1

            except Empty:
                break
            except Exception as e:
                logger.error(f"Failed to flush buffered message: {e}")
                self._failed_messages.append(buffered_msg)
                failed_count += 1

        logger.info(f"Flushed {flushed_count} messages, {failed_count} failed")

    def get_metadata(self):
        """Get connection metadata."""
        if self._connection:
            metadata = self._connection.getMetaData()
            return {
                'jms_version': metadata.getJMSVersion(),
                'jms_major_version': metadata.getJMSMajorVersion(),
                'jms_minor_version': metadata.getJMSMinorVersion(),
                'provider_name': metadata.getJMSProviderName(),
                'provider_version': metadata.getProviderVersion()
            }
        return None

    def stop(self):
        """Stop the connection (pause message delivery)."""
        if self._connection:
            self._connection.stop()

    def start(self):
        """Start the connection (resume message delivery)."""
        if self._connection:
            self._connection.start()

    def close(self):
        """Close the EMS connection."""
        try:
            # Stop buffer processor
            self._stop_buffer_processor.set()

            # Try to flush remaining messages
            if self._connected and not self._message_buffer.empty():
                logger.info(f"Flushing {self._message_buffer.qsize()} messages before closing")
                self._flush_buffer()

            # Log unsent messages
            if not self._message_buffer.empty():
                logger.warning(f"Closing with {self._message_buffer.qsize()} unsent messages")

            if self._failed_messages:
                logger.warning(f"Failed to send {len(self._failed_messages)} messages")

            # Close all sessions
            for session in self._sessions:
                session.close()

            # Stop and close connection
            if self._connection:
                self._connection.stop()
                self._connection.close()

            self._connected = False
            logger.info("TIBCO EMS connection closed successfully")

        except Exception as e:
            logger.error(f"Error closing TIBCO EMS connection: {e}")

    def get_buffer_size(self) -> int:
        """Get the current number of messages in the buffer."""
        return self._message_buffer.qsize()

    def get_failed_messages(self) -> List[BufferedMessage]:
        """Get the list of messages that failed to send."""
        return self._failed_messages.copy()

    def is_connected(self) -> bool:
        """Check if the connection is active."""
        return self._connected


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Example 1: Basic usage as a drop-in replacement
    connection = ResilientTibcoEMSConnection(
        server_url='tcp://localhost:7222',
        username='admin',
        password='admin',
        reconnect_tries=5,
        reconnect_interval_seconds=30,
        buffer_max_messages=5000,
        client_id='resilient_client_001',
        connection_factory_params={
            'ConnAttemptCount': 3,
            'ConnAttemptDelay': 1000,
            'ConnAttemptTimeout': 5000,
            'ReconnAttemptCount': 3,
            'ReconnAttemptDelay': 1000,
            'ReconnAttemptTimeout': 5000
        }
    )

    # Create a session
    session = connection.create_session(
        transacted=False,
        acknowledge_mode=AcknowledgeMode.AUTO_ACKNOWLEDGE
    )

    # Example 2: Sending messages with automatic buffering
    try:
        for i in range(100):
            # Messages will be buffered if connection is lost
            session.send(
                destination='test.queue',
                message=f'Message {i}',
                destination_type='queue',
                delivery_mode=DeliveryMode.PERSISTENT.value,
                properties={'msg_id': str(i), 'timestamp': str(time.time())},
                correlation_id=f'corr_{i}'
            )
            print(f"Sent message {i}")

            # Check buffer status
            buffer_size = connection.get_buffer_size()
            if buffer_size > 0:
                print(f"Messages in buffer: {buffer_size}")

            time.sleep(1)

    except KeyboardInterrupt:
        pass
    finally:
        connection.close()

    # Example 3: Consumer with automatic reconnection
    connection2 = ResilientTibcoEMSConnection(
        server_url='tcp://localhost:7222',
        username='admin',
        password='admin'
    )

    session2 = connection2.create_session(
        acknowledge_mode=AcknowledgeMode.CLIENT_ACKNOWLEDGE
    )


    def message_listener(message):
        try:
            # Process based on message type
            if hasattr(message, 'getText'):
                print(f"Received text message: {message.getText()}")
            elif hasattr(message, 'getObject'):
                print(f"Received object message: {message.getObject()}")

            # Acknowledge the message
            message.acknowledge()
        except Exception as e:
            logger.error(f"Error processing message: {e}")


    # Create consumer (will be restored after reconnection)
    consumer = session2.create_consumer(
        destination='consumer.queue',
        destination_type='queue',
        message_listener=message_listener
    )

    print("Consumer started. Press CTRL+C to exit")
    try:
        time.sleep(60)  # Keep running
    except KeyboardInterrupt:
        pass
    finally:
        connection2.close()

    # Example 4: Request-Reply pattern with temporary queue
    connection3 = ResilientTibcoEMSConnection(
        server_url='tcp://localhost:7222',
        username='admin',
        password='admin'
    )

    session3 = connection3.create_session()

    # Create temporary queue for replies
    reply_queue = session3.create_temporary_queue()
    reply_queue_name = reply_queue.getQueueName()

    # Send request with reply-to
    session3.send(
        destination='request.queue',
        message='What is the status?',
        reply_to=reply_queue_name,
        correlation_id='req_001'
    )

    # Wait for reply
    reply = session3.receive(
        destination=reply_queue_name,
        timeout=5000  # 5 seconds
    )

    if reply:
        print(f"Received reply: {reply.getText()}")

    connection3.close()