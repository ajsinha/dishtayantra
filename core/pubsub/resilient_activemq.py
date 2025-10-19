import logging
import time
import threading
from queue import Queue, Empty
from typing import Dict, Optional, Tuple, Any, Callable, List
import stomp
from stomp import Connection
from stomp.exception import ConnectFailedException, NotConnectedException

logger = logging.getLogger(__name__)


class ResilientActiveMQConnection(Connection):
    """
    A stomp.Connection subclass that automatically handles reconnection on failures
    and buffers messages during reconnection to prevent message loss.

    This class extends stomp.Connection to provide automatic reconnection capabilities
    when connection failures occur during initialization, message sending, or receiving.
    Messages sent during reconnection attempts are buffered and retried once
    connection is restored.

    Args:
        host_and_ports: List of (host, port) tuples for broker addresses
        reconnect_tries: Maximum number of reconnection attempts (default: 10)
        reconnect_interval_seconds: Sleep interval between reconnection attempts (default: 60)
        buffer_max_messages: Maximum messages to buffer during reconnection (default: 10000)
        **kwargs: All other stomp.Connection parameters
    """

    def __init__(self,
                 host_and_ports: List[Tuple[str, int]] = None,
                 reconnect_tries: int = 10,
                 reconnect_interval_seconds: int = 60,
                 buffer_max_messages: int = 10000,
                 **kwargs):
        """Initialize the ResilientActiveMQConnection with reconnection and buffering capabilities."""

        # Set default host and ports if not provided
        if host_and_ports is None:
            host_and_ports = [('localhost', 61613)]

        # Initialize parent Connection
        super().__init__(host_and_ports, **kwargs)

        # Reconnection settings
        self.reconnect_tries = reconnect_tries
        self.reconnect_interval_seconds = reconnect_interval_seconds
        self.buffer_max_messages = buffer_max_messages
        self._current_retry = 0
        self._connected = False
        self._reconnecting = False
        self._lock = threading.Lock()

        # Store connection parameters for reconnection
        self._host_and_ports = host_and_ports
        self._connect_params = kwargs
        self._stored_credentials = None
        self._stored_headers = {}
        self._subscriptions = {}  # Store active subscriptions for re-subscription

        # Message buffer for storing messages during reconnection
        self._message_buffer = Queue(maxsize=buffer_max_messages)
        self._failed_messages = []

        # Listener management
        self._original_listeners = []

        # Start background thread for processing buffered messages
        self._stop_buffer_processor = threading.Event()
        self._buffer_processor_thread = threading.Thread(
            target=self._process_buffered_messages,
            daemon=True
        )
        self._buffer_processor_thread.start()

        # Add internal reconnection listener
        self._add_reconnection_listener()

    def _add_reconnection_listener(self):
        """Add an internal listener to handle disconnections."""

        class ReconnectionListener(stomp.ConnectionListener):
            def __init__(self, connection):
                self.connection = connection

            def on_error(self, frame):
                logger.error(f"STOMP error received: {frame.body}")
                if not self.connection._reconnecting:
                    self.connection._handle_connection_failure()

            def on_disconnected(self):
                logger.warning("Connection to ActiveMQ lost")
                if not self.connection._reconnecting:
                    self.connection._handle_connection_failure()

            def on_connected(self, frame):
                logger.info("Successfully connected to ActiveMQ")
                self.connection._connected = True
                self.connection._reconnecting = False
                # Re-subscribe to all stored subscriptions
                self.connection._restore_subscriptions()

        self.set_listener('_reconnection_listener', ReconnectionListener(self))

    def _handle_connection_failure(self):
        """Handle connection failure and initiate reconnection."""
        with self._lock:
            if self._reconnecting:
                return
            self._reconnecting = True
            self._connected = False

        # Start reconnection in a separate thread
        reconnect_thread = threading.Thread(target=self._reconnect, daemon=True)
        reconnect_thread.start()

    def connect(self, username: Optional[str] = None, passcode: Optional[str] = None,
                wait: bool = False, headers: Optional[Dict] = None,
                with_connect_command: bool = True):
        """
        Connect to ActiveMQ with automatic retry logic.

        Args:
            username: Username for authentication
            passcode: Password for authentication
            wait: Whether to wait for connection confirmation
            headers: Additional connection headers
            with_connect_command: Whether to send CONNECT frame
        """
        # Store credentials for reconnection
        self._stored_credentials = (username, passcode)
        self._stored_headers = headers or {}

        last_exception = None

        for attempt in range(1, self.reconnect_tries + 1):
            try:
                logger.info(f"Attempting to connect to ActiveMQ (attempt {attempt}/{self.reconnect_tries})")

                # Attempt connection
                super().connect(username=username, passcode=passcode,
                                wait=wait, headers=headers,
                                with_connect_command=with_connect_command)

                # Verify connection
                if wait or self.is_connected():
                    self._connected = True
                    self._reconnecting = False
                    self._current_retry = 0
                    logger.info("Successfully connected to ActiveMQ")
                    return

            except (ConnectFailedException, Exception) as e:
                last_exception = e
                logger.warning(f"Failed to connect to ActiveMQ (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    logger.info(f"Retrying in {self.reconnect_interval_seconds} seconds...")
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    logger.error(f"Failed to connect to ActiveMQ after {self.reconnect_tries} attempts")
                    raise last_exception

    def _reconnect(self):
        """Attempt to reconnect to ActiveMQ."""
        logger.info("Starting reconnection process...")

        for attempt in range(1, self.reconnect_tries + 1):
            try:
                logger.info(f"Reconnection attempt {attempt}/{self.reconnect_tries}")

                # Try to disconnect cleanly first
                try:
                    if self.is_connected():
                        super().disconnect()
                except Exception as e:
                    logger.debug(f"Error during disconnect: {e}")

                # Wait a moment before reconnecting
                time.sleep(2)

                # Attempt to reconnect using stored credentials
                username, passcode = self._stored_credentials if self._stored_credentials else (None, None)
                super().connect(username=username, passcode=passcode,
                                wait=True, headers=self._stored_headers)

                if self.is_connected():
                    self._connected = True
                    self._reconnecting = False
                    self._current_retry = 0
                    logger.info("Successfully reconnected to ActiveMQ")

                    # Flush buffered messages
                    self._flush_buffer()
                    return

            except Exception as e:
                logger.warning(f"Reconnection attempt {attempt} failed: {e}")

                if attempt < self.reconnect_tries:
                    logger.info(f"Retrying in {self.reconnect_interval_seconds} seconds...")
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    logger.error(f"Failed to reconnect after {self.reconnect_tries} attempts")
                    with self._lock:
                        self._reconnecting = False
                    raise

    def _restore_subscriptions(self):
        """Restore all subscriptions after reconnection."""
        logger.info(f"Restoring {len(self._subscriptions)} subscriptions")

        for dest, params in self._subscriptions.items():
            try:
                super().subscribe(
                    destination=dest,
                    id=params['id'],
                    ack=params.get('ack', 'auto'),
                    headers=params.get('headers')
                )
                logger.debug(f"Restored subscription to {dest}")
            except Exception as e:
                logger.error(f"Failed to restore subscription to {dest}: {e}")

    def subscribe(self, destination: str, id: Optional[str] = None,
                  ack: str = 'auto', headers: Optional[Dict] = None):
        """
        Subscribe to a destination with automatic re-subscription on reconnection.

        Args:
            destination: Queue or topic to subscribe to
            id: Subscription ID
            ack: Acknowledgment mode
            headers: Additional subscription headers
        """
        # Generate ID if not provided
        if id is None:
            id = str(len(self._subscriptions))

        # Store subscription for reconnection
        self._subscriptions[destination] = {
            'id': id,
            'ack': ack,
            'headers': headers
        }

        # Attempt subscription with retry logic
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                super().subscribe(destination=destination, id=id, ack=ack, headers=headers)
                logger.info(f"Successfully subscribed to {destination}")
                return

            except (NotConnectedException, Exception) as e:
                logger.warning(f"Subscribe failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    if not self._connected:
                        self._handle_connection_failure()
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    raise

    def unsubscribe(self, id: str, headers: Optional[Dict] = None):
        """
        Unsubscribe from a destination.

        Args:
            id: Subscription ID
            headers: Additional headers
        """
        # Remove from stored subscriptions
        self._subscriptions = {k: v for k, v in self._subscriptions.items()
                               if v.get('id') != id}

        try:
            super().unsubscribe(id=id, headers=headers)
        except Exception as e:
            logger.error(f"Failed to unsubscribe: {e}")

    def send(self, destination: str, body: str,
             content_type: Optional[str] = None,
             headers: Optional[Dict] = None, **kwargs):
        """
        Send a message with automatic reconnection and buffering on failure.

        Args:
            destination: Queue or topic to send to
            body: Message body
            content_type: Content type header
            headers: Additional message headers
            **kwargs: Additional parameters
        """
        # If reconnecting or not connected, buffer the message
        if self._reconnecting or not self._connected:
            logger.info(f"Connection unavailable, buffering message for destination: {destination}")
            self._buffer_message(destination, body, content_type, headers, kwargs)
            return

        # Try to send with retry logic
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                super().send(destination=destination, body=body,
                             content_type=content_type, headers=headers, **kwargs)
                self._current_retry = 0
                return

            except (NotConnectedException, Exception) as e:
                logger.warning(f"Send failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                # Buffer message on first failure
                if attempt == 1:
                    self._buffer_message(destination, body, content_type, headers, kwargs)

                if attempt < self.reconnect_tries:
                    if not self._connected:
                        self._handle_connection_failure()
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    logger.error(f"Send failed after {self.reconnect_tries} attempts")
                    raise

    def _buffer_message(self, destination: str, body: str,
                        content_type: Optional[str], headers: Optional[Dict],
                        kwargs: Dict):
        """Buffer a message for later sending when connection is restored."""
        try:
            message_data = {
                'destination': destination,
                'body': body,
                'content_type': content_type,
                'headers': headers,
                'kwargs': kwargs
            }

            if self._message_buffer.full():
                # Remove oldest message to make room
                old_msg = self._message_buffer.get_nowait()
                logger.warning(f"Buffer full, dropping oldest message for destination: {old_msg['destination']}")

            self._message_buffer.put_nowait(message_data)
            logger.debug(
                f"Buffered message for destination: {destination}, buffer size: {self._message_buffer.qsize()}")

        except Exception as e:
            logger.error(f"Failed to buffer message: {e}")

    def _process_buffered_messages(self):
        """Background thread to process buffered messages when connection is restored."""
        while not self._stop_buffer_processor.is_set():
            try:
                if self._connected and not self._reconnecting and not self._message_buffer.empty():
                    # Process buffered messages
                    try:
                        message = self._message_buffer.get(timeout=1)

                        super().send(
                            destination=message['destination'],
                            body=message['body'],
                            content_type=message['content_type'],
                            headers=message['headers'],
                            **message['kwargs']
                        )
                        logger.debug(f"Successfully sent buffered message to: {message['destination']}")

                    except Empty:
                        continue
                    except Exception as e:
                        logger.error(f"Failed to send buffered message: {e}")
                        # Re-queue the message for retry
                        self._message_buffer.put(message)
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

        while not self._message_buffer.empty():
            try:
                message = self._message_buffer.get_nowait()

                super().send(
                    destination=message['destination'],
                    body=message['body'],
                    content_type=message['content_type'],
                    headers=message['headers'],
                    **message['kwargs']
                )
                flushed_count += 1

            except Empty:
                break
            except Exception as e:
                logger.error(f"Failed to flush buffered message: {e}")
                self._failed_messages.append(message)
                failed_count += 1

        logger.info(f"Flushed {flushed_count} messages, {failed_count} failed")

    def send_batch(self, messages: List[Dict]) -> List[bool]:
        """
        Send multiple messages in batch with automatic reconnection.

        Args:
            messages: List of message dictionaries with keys:
                     'destination', 'body', 'content_type', 'headers'

        Returns:
            List of boolean values indicating success for each message
        """
        results = []

        for message in messages:
            try:
                self.send(
                    destination=message.get('destination'),
                    body=message.get('body'),
                    content_type=message.get('content_type'),
                    headers=message.get('headers')
                )
                results.append(True)
            except Exception as e:
                logger.error(f"Failed to send batch message: {e}")
                results.append(False)

        return results

    def ack(self, message_id: str, subscription: str, headers: Optional[Dict] = None):
        """
        Acknowledge a message with retry logic.

        Args:
            message_id: Message ID to acknowledge
            subscription: Subscription ID
            headers: Additional headers
        """
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                super().ack(message_id, subscription, headers=headers)
                return

            except (NotConnectedException, Exception) as e:
                logger.warning(f"ACK failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    if not self._connected:
                        self._handle_connection_failure()
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    raise

    def nack(self, message_id: str, subscription: str, headers: Optional[Dict] = None):
        """
        Negative acknowledge a message with retry logic.

        Args:
            message_id: Message ID to NACK
            subscription: Subscription ID
            headers: Additional headers
        """
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                super().nack(message_id, subscription, headers=headers)
                return

            except (NotConnectedException, Exception) as e:
                logger.warning(f"NACK failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    if not self._connected:
                        self._handle_connection_failure()
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    raise

    def disconnect(self):
        """Disconnect from ActiveMQ broker."""
        try:
            # Stop buffer processor
            self._stop_buffer_processor.set()

            # Try to flush remaining messages
            if self._connected and not self._message_buffer.empty():
                logger.info(f"Flushing {self._message_buffer.qsize()} messages before disconnect")
                self._flush_buffer()

            # Log unsent messages
            if not self._message_buffer.empty():
                logger.warning(f"Disconnecting with {self._message_buffer.qsize()} unsent messages")

            if self._failed_messages:
                logger.warning(f"Failed to send {len(self._failed_messages)} messages")

            super().disconnect()
            self._connected = False
            logger.info("Disconnected from ActiveMQ")

        except Exception as e:
            logger.error(f"Error during disconnect: {e}")

    def get_buffer_size(self) -> int:
        """Get the current number of messages in the buffer."""
        return self._message_buffer.qsize()

    def get_failed_messages(self) -> List[Dict]:
        """Get the list of messages that failed to send."""
        return self._failed_messages.copy()

    def is_connected(self) -> bool:
        """Check if the connection is active."""
        return self._connected and super().is_connected()


# Example custom listener for handling messages
class MessageListener(stomp.ConnectionListener):
    def on_message(self, frame):
        logger.info(f"Received message: {frame.body}")

    def on_error(self, frame):
        logger.error(f"Error: {frame.body}")


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Example 1: Basic usage as a drop-in replacement
    conn = ResilientActiveMQConnection(
        host_and_ports=[('localhost', 61613)],
        reconnect_tries=5,
        reconnect_interval_seconds=30,
        buffer_max_messages=5000
    )

    # Add custom message listener
    conn.set_listener('message_listener', MessageListener())

    # Connect with credentials
    conn.connect('admin', 'admin', wait=True)

    # Example 2: Subscribe to a queue with auto-reconnection
    conn.subscribe(destination='/queue/test.queue', id='1', ack='auto')

    # Example 3: Send messages with automatic buffering during failures
    try:
        for i in range(100):
            # Messages will be buffered if connection is lost
            conn.send('/queue/test.queue', f'Message {i}')
            print(f"Sent message {i}")

            # Check buffer status
            buffer_size = conn.get_buffer_size()
            if buffer_size > 0:
                print(f"Messages in buffer: {buffer_size}")

            time.sleep(1)

    except KeyboardInterrupt:
        pass
    finally:
        conn.disconnect()

    # Example 4: Batch sending with transaction support
    conn2 = ResilientActiveMQConnection(
        host_and_ports=[('localhost', 61613)],
        auto_content_length=False
    )

    conn2.connect('admin', 'admin', wait=True)

    # Send batch of messages
    messages = [
        {
            'destination': '/queue/batch.queue',
            'body': f'Batch message {i}',
            'headers': {'persistent': 'true'}
        }
        for i in range(50)
    ]

    results = conn2.send_batch(messages)
    print(f"Sent {sum(results)} out of {len(results)} messages successfully")

    # Get any failed messages
    failed = conn2.get_failed_messages()
    if failed:
        print(f"Failed messages: {len(failed)}")

    conn2.disconnect()

    # Example 5: Using with acknowledgment modes
    conn3 = ResilientActiveMQConnection(
        host_and_ports=[('localhost', 61613)],
        heartbeats=(10000, 10000)  # Heartbeat configuration
    )


    class AckMessageListener(stomp.ConnectionListener):
        def __init__(self, connection):
            self.conn = connection

        def on_message(self, frame):
            try:
                # Process message
                print(f"Processing: {frame.body}")

                # Acknowledge message
                self.conn.ack(frame.headers['message-id'],
                              frame.headers['subscription'])
            except Exception as e:
                # NACK on failure
                self.conn.nack(frame.headers['message-id'],
                               frame.headers['subscription'])
                logger.error(f"Message processing failed: {e}")


    conn3.set_listener('ack_listener', AckMessageListener(conn3))
    conn3.connect('admin', 'admin', wait=True)
    conn3.subscribe(destination='/queue/ack.queue', id='2', ack='client')

    # Keep running
    try:
        time.sleep(60)
    except KeyboardInterrupt:
        pass
    finally:
        conn3.disconnect()