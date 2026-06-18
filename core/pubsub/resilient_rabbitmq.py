import logging
import time
import threading
import functools
from queue import Queue, Empty
from typing import Dict, Optional, Any, List, Callable, Union
import pika
from pika import BlockingConnection, ConnectionParameters, PlainCredentials
from pika.channel import Channel
from pika.exceptions import AMQPConnectionError, AMQPChannelError, ChannelClosedByBroker
from pika.spec import Basic, BasicProperties

logger = logging.getLogger(__name__)


class ResilientChannel:
    """A pika.Channel wrapper that survives connection drops by replaying topology.

    A RabbitMQ channel is connection-scoped: when the connection dies the channel and
    everything declared on it (QoS, exchanges, queues, bindings, consumers) are gone -
    unlike Kafka, where the broker holds durable topic state. So this wrapper records
    every topology-defining call as it happens (via ``__getattr__`` interception) and,
    after a reconnect, re-creates the channel and **replays** those calls to rebuild
    server-side state transparently. Callers use it exactly like a pika channel."""

    def __init__(self, connection, channel_number=None):
        self.connection = connection
        self.channel_number = channel_number
        self._channel = None
        self._consumers = {}  # Store consumer callbacks for re-registration
        self._qos_settings = None
        self._exchanges = set()  # Track declared exchanges
        self._queues = set()  # Track declared queues
        self._bindings = []  # Track queue bindings

    def _get_channel(self):
        """Return the live channel, lazily (re)creating it after a drop and replaying
        the tracked topology so the new channel is immediately usable."""
        if self._channel is None or not self._channel.is_open:
            self._channel = self.connection._create_channel(self.channel_number)
            self._restore_channel_state()
        return self._channel

    def _restore_channel_state(self):
        """Replay recorded QoS, exchange/queue declares, bindings, and consumers onto
        a freshly created channel. Each replay is best-effort (logged on failure) so
        one bad declaration doesn't abort the whole restore."""
        if self._channel:
            # Restore QoS settings
            if self._qos_settings:
                self._channel.basic_qos(**self._qos_settings)

            # Re-declare exchanges
            for exchange_params in self._exchanges:
                try:
                    self._channel.exchange_declare(**exchange_params)
                except Exception as e:
                    logger.warning(f"Failed to restore exchange: {e}")

            # Re-declare queues
            for queue_params in self._queues:
                try:
                    self._channel.queue_declare(**queue_params)
                except Exception as e:
                    logger.warning(f"Failed to restore queue: {e}")

            # Restore bindings
            for binding in self._bindings:
                try:
                    self._channel.queue_bind(**binding)
                except Exception as e:
                    logger.warning(f"Failed to restore binding: {e}")

            # Re-register consumers
            for queue, callback_info in self._consumers.items():
                try:
                    self._channel.basic_consume(
                        queue=queue,
                        on_message_callback=callback_info['callback'],
                        auto_ack=callback_info['auto_ack'],
                        exclusive=callback_info.get('exclusive', False),
                        consumer_tag=callback_info.get('consumer_tag'),
                        arguments=callback_info.get('arguments')
                    )
                    logger.debug(f"Restored consumer for queue: {queue}")
                except Exception as e:
                    logger.error(f"Failed to restore consumer for queue {queue}: {e}")

    def __getattr__(self, name):
        """Delegate to the live pika channel, but intercept topology-defining calls
        (basic_qos / exchange_declare / queue_declare / queue_bind / basic_consume) to
        record them for replay after a reconnect, and run every call through the
        connection's retry logic. This is what makes resilience transparent: callers
        issue ordinary pika operations and the wrapper remembers what to rebuild."""
        channel = self._get_channel()
        attr = getattr(channel, name)

        if callable(attr):
            @functools.wraps(attr)
            def wrapper(*args, **kwargs):
                # Track certain operations for restoration
                if name == 'basic_qos':
                    self._qos_settings = kwargs
                elif name == 'exchange_declare':
                    self._exchanges.add(frozenset(kwargs.items()))
                elif name == 'queue_declare':
                    self._queues.add(frozenset(kwargs.items()))
                elif name == 'queue_bind':
                    self._bindings.append(kwargs)
                elif name == 'basic_consume':
                    queue = args[0] if args else kwargs.get('queue')
                    callback = kwargs.get('on_message_callback')
                    if queue and callback:
                        self._consumers[queue] = {
                            'callback': callback,
                            'auto_ack': kwargs.get('auto_ack', False),
                            'exclusive': kwargs.get('exclusive', False),
                            'consumer_tag': kwargs.get('consumer_tag'),
                            'arguments': kwargs.get('arguments')
                        }

                # Execute with retry logic
                return self.connection._execute_with_retry(attr, *args, **kwargs)

            return wrapper
        return attr

    def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
        """Publish a message with automatic buffering on failure."""
        return self.connection._publish_with_retry(
            self._get_channel(), exchange, routing_key, body, properties, mandatory
        )

    def close(self):
        """Close the channel."""
        if self._channel and self._channel.is_open:
            try:
                self._channel.close()
            except Exception as e:
                logger.debug(f"Error closing channel: {e}")
        self._channel = None


class ResilientRabbitBlockingConnection(BlockingConnection):
    """
    A pika.BlockingConnection subclass that automatically handles reconnection on failures
    and buffers messages during reconnection to prevent message loss.

    This class extends pika.BlockingConnection to provide automatic reconnection capabilities
    when connection failures occur during initialization, channel operations, or message publishing.
    Messages sent during reconnection attempts are buffered and retried once connection is restored.

    Args:
        parameters: Connection parameters (single or list)
        reconnect_tries: Maximum number of reconnection attempts (default: 10)
        reconnect_interval_seconds: Sleep interval between reconnection attempts (default: 60)
        buffer_max_messages: Maximum messages to buffer during reconnection (default: 10000)
    """

    def __init__(self,
                 parameters: Union[ConnectionParameters, List[ConnectionParameters]] = None,
                 reconnect_tries: int = 10,
                 reconnect_interval_seconds: int = 60,
                 buffer_max_messages: int = 10000):
        """Initialize the ResilientRabbitBlockingConnection with reconnection and buffering."""

        # Set default parameters if not provided
        if parameters is None:
            parameters = ConnectionParameters('localhost')

        # Store parameters for reconnection
        self._parameters = parameters if isinstance(parameters, list) else [parameters]
        self.reconnect_tries = reconnect_tries
        self.reconnect_interval_seconds = reconnect_interval_seconds
        self.buffer_max_messages = buffer_max_messages

        # Connection state
        self._current_retry = 0
        self._connected = False
        self._reconnecting = False
        self._lock = threading.Lock()

        # Channel management
        self._channels = {}
        self._channel_counter = 1

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
        """Connect, retrying on a FIXED interval (tries × ``reconnect_interval_seconds``,
        blocking; not exponential backoff). Raises the last error if all attempts fail
        rather than leaving a half-open connection."""
        last_exception = None

        for attempt in range(1, self.reconnect_tries + 1):
            try:
                logger.info(f"Attempting to connect to RabbitMQ (attempt {attempt}/{self.reconnect_tries})")

                # Initialize the parent BlockingConnection
                super().__init__(self._parameters)

                self._connected = True
                self._reconnecting = False
                self._current_retry = 0
                logger.info("Successfully connected to RabbitMQ")
                return

            except (AMQPConnectionError, Exception) as e:
                last_exception = e
                logger.warning(f"Failed to connect to RabbitMQ (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    logger.info(f"Retrying in {self.reconnect_interval_seconds} seconds...")
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    logger.error(f"Failed to connect to RabbitMQ after {self.reconnect_tries} attempts")
                    raise last_exception

    def _reconnect(self):
        """Single-flight recovery: reconnect, then rebuild and resume.

        Guarded by ``_reconnecting`` under ``_lock`` so that several operations failing
        at once trigger only ONE reconnection, not a thundering herd. Orchestrates the
        full recovery in order: close -> ``_connect_with_retry`` -> ``_restore_channels``
        (replays each channel's topology) -> ``_flush_buffer`` (drains messages buffered
        during the outage, so nothing is lost)."""
        with self._lock:
            if self._reconnecting:
                return
            self._reconnecting = True

        logger.info("Starting reconnection process...")

        try:
            # Close existing connection if any
            try:
                if self.is_open:
                    super().close()
            except Exception as e:
                logger.debug(f"Error closing existing connection: {e}")

            # Reset connection state
            self._connected = False

            # Reconnect with retry logic
            self._connect_with_retry()

            # Restore channels
            self._restore_channels()

            # Flush buffered messages
            self._flush_buffer()

        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            raise
        finally:
            with self._lock:
                self._reconnecting = False

    def _restore_channels(self):
        """Force every tracked ResilientChannel to drop and recreate its pika channel;
        recreation triggers that channel's topology replay (QoS/exchanges/queues/
        bindings/consumers). Best-effort per channel so one failure doesn't block the rest."""
        logger.info(f"Restoring {len(self._channels)} channels")

        for channel_num, resilient_channel in self._channels.items():
            try:
                # Force channel recreation
                resilient_channel._channel = None
                resilient_channel._get_channel()
                logger.debug(f"Restored channel {channel_num}")
            except Exception as e:
                logger.error(f"Failed to restore channel {channel_num}: {e}")

    def channel(self, channel_number: Optional[int] = None) -> ResilientChannel:
        """
        Create a new channel with resilience support.

        Args:
            channel_number: Optional channel number

        Returns:
            ResilientChannel wrapper
        """
        if channel_number is None:
            channel_number = self._channel_counter
            self._channel_counter += 1

        if channel_number not in self._channels:
            self._channels[channel_number] = ResilientChannel(self, channel_number)

        return self._channels[channel_number]

    def _create_channel(self, channel_number: Optional[int] = None) -> Channel:
        """Create a raw pika channel."""
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                if not self.is_open:
                    self._reconnect()

                return super().channel(channel_number)

            except (AMQPConnectionError, AMQPChannelError) as e:
                logger.warning(f"Failed to create channel (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                    if not self._connected:
                        self._reconnect()
                else:
                    raise

    def _execute_with_retry(self, func: Callable, *args, **kwargs):
        """Run any channel operation with reconnect-and-retry.

        On an AMQP connection/channel error, reconnect if the connection is down, wait
        1s, and retry up to ``reconnect_tries``; re-raise once exhausted. This is the
        single choke point through which ``__getattr__``-delegated channel calls pass,
        so resilience is uniform across every operation."""
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                return func(*args, **kwargs)

            except (AMQPConnectionError, AMQPChannelError, AttributeError) as e:
                logger.warning(f"Operation failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    if not self._connected or not self.is_open:
                        self._reconnect()
                    time.sleep(1)
                else:
                    raise

    def _publish_with_retry(self, channel: Channel, exchange: str, routing_key: str,
                            body: bytes, properties: Optional[BasicProperties] = None,
                            mandatory: bool = False):
        """Publish, buffering instead of losing on failure.

        If the connection is down or a reconnect is in flight, the message is buffered
        (the background processor sends it after recovery) rather than dropped or
        blocked on; otherwise it publishes with retry. Same cross-outage ordering
        caveat as the other connectors: buffered messages may be delivered after
        messages published once the link recovered."""
        # Buffer message if reconnecting
        if self._reconnecting or not self._connected:
            logger.info(f"Connection unavailable, buffering message for routing_key: {routing_key}")
            self._buffer_message(channel, exchange, routing_key, body, properties, mandatory)
            return

        # Try to publish with retry logic
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=body,
                    properties=properties,
                    mandatory=mandatory
                )
                self._current_retry = 0
                return

            except (AMQPConnectionError, AMQPChannelError, AttributeError) as e:
                logger.warning(f"Publish failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                # Buffer message on first failure
                if attempt == 1:
                    self._buffer_message(channel, exchange, routing_key, body, properties, mandatory)

                if attempt < self.reconnect_tries:
                    if not self._connected:
                        self._reconnect()
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    logger.error(f"Publish failed after {self.reconnect_tries} attempts")
                    raise

    def _buffer_message(self, channel: Channel, exchange: str, routing_key: str,
                        body: bytes, properties: Optional[BasicProperties],
                        mandatory: bool):
        """Buffer a message for later sending when connection is restored."""
        try:
            message_data = {
                'channel': channel,
                'exchange': exchange,
                'routing_key': routing_key,
                'body': body,
                'properties': properties,
                'mandatory': mandatory
            }

            if self._message_buffer.full():
                # Remove oldest message to make room
                old_msg = self._message_buffer.get_nowait()
                logger.warning(f"Buffer full, dropping oldest message for routing_key: {old_msg['routing_key']}")

            self._message_buffer.put_nowait(message_data)
            logger.debug(
                f"Buffered message for routing_key: {routing_key}, buffer size: {self._message_buffer.qsize()}")

        except Exception as e:
            logger.error(f"Failed to buffer message: {e}")

    def _process_buffered_messages(self):
        """Background daemon loop: once connected and not mid-reconnect, drain the
        buffer and publish each message, recreating the channel if it has closed.
        A send that fails is re-queued (to the back) for another attempt — so transient
        failures recover, but note a persistently failing message will keep cycling
        rather than being dead-lettered. Idle-polls every 0.1s."""
        while not self._stop_buffer_processor.is_set():
            try:
                if self._connected and not self._reconnecting and not self._message_buffer.empty():
                    try:
                        message = self._message_buffer.get(timeout=1)

                        # Get the channel or create a new one if needed
                        channel = message['channel']
                        if not hasattr(channel, '_channel') or not channel._channel.is_open:
                            channel = self.channel()

                        channel.basic_publish(
                            exchange=message['exchange'],
                            routing_key=message['routing_key'],
                            body=message['body'],
                            properties=message['properties'],
                            mandatory=message['mandatory']
                        )
                        logger.debug(f"Successfully sent buffered message to: {message['routing_key']}")

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
        """Drain the buffer synchronously during reconnection (called from
        ``_reconnect`` after channels are restored), so backlogged messages go out as
        soon as the link is back rather than waiting for the background poll."""
        logger.info(f"Flushing {self._message_buffer.qsize()} buffered messages")

        flushed_count = 0
        failed_count = 0

        # Create a default channel for flushing if needed
        default_channel = self.channel()

        while not self._message_buffer.empty():
            try:
                message = self._message_buffer.get_nowait()

                # Use the original channel if available, otherwise use default
                channel = message['channel']
                if not hasattr(channel, '_channel') or not channel._channel.is_open:
                    channel = default_channel

                channel.basic_publish(
                    exchange=message['exchange'],
                    routing_key=message['routing_key'],
                    body=message['body'],
                    properties=message['properties'],
                    mandatory=message['mandatory']
                )
                flushed_count += 1

            except Empty:
                break
            except Exception as e:
                logger.error(f"Failed to flush buffered message: {e}")
                self._failed_messages.append(message)
                failed_count += 1

        logger.info(f"Flushed {flushed_count} messages, {failed_count} failed")

    def publish_batch(self, channel: ResilientChannel, messages: List[Dict]) -> List[bool]:
        """
        Publish multiple messages in batch with automatic reconnection.

        Args:
            channel: Channel to use for publishing
            messages: List of message dictionaries with keys:
                     'exchange', 'routing_key', 'body', 'properties', 'mandatory'

        Returns:
            List of boolean values indicating success for each message
        """
        results = []

        for message in messages:
            try:
                channel.basic_publish(
                    exchange=message.get('exchange', ''),
                    routing_key=message.get('routing_key'),
                    body=message.get('body'),
                    properties=message.get('properties'),
                    mandatory=message.get('mandatory', False)
                )
                results.append(True)
            except Exception as e:
                logger.error(f"Failed to publish batch message: {e}")
                results.append(False)

        return results

    def close(self):
        """Close the connection."""
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

            # Close all channels
            for channel in self._channels.values():
                channel.close()

            # Close connection
            if self.is_open:
                super().close()

            self._connected = False
            logger.info("RabbitMQ connection closed successfully")

        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {e}")

    def get_buffer_size(self) -> int:
        """Get the current number of messages in the buffer."""
        return self._message_buffer.qsize()

    def get_failed_messages(self) -> List[Dict]:
        """Get the list of messages that failed to send."""
        return self._failed_messages.copy()

    @property
    def is_open(self) -> bool:
        """Check if connection is open."""
        try:
            return self._connected and super().is_open
        except:
            return False


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Example 1: Basic usage as a drop-in replacement
    parameters = pika.ConnectionParameters(
        host='localhost',
        port=5672,
        credentials=PlainCredentials('guest', 'guest')
    )

    connection = ResilientRabbitBlockingConnection(
        parameters=parameters,
        reconnect_tries=5,
        reconnect_interval_seconds=30,
        buffer_max_messages=5000
    )

    # Create a resilient channel
    channel = connection.channel()

    # Declare exchange and queue (will be restored after reconnection)
    channel.exchange_declare(exchange='test_exchange', exchange_type='direct', durable=True)
    channel.queue_declare(queue='test_queue', durable=True)
    channel.queue_bind(exchange='test_exchange', queue='test_queue', routing_key='test_key')

    # Example 2: Publishing messages with automatic buffering
    try:
        for i in range(100):
            # Messages will be buffered if connection is lost
            channel.basic_publish(
                exchange='test_exchange',
                routing_key='test_key',
                body=f'Message {i}'.encode(),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                )
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
    connection2 = ResilientRabbitBlockingConnection(parameters=parameters)
    channel2 = connection2.channel()

    # Set up QoS (will be restored after reconnection)
    channel2.basic_qos(prefetch_count=1)

    # Declare queue
    channel2.queue_declare(queue='consumer_queue', durable=True)


    def callback(ch, method, properties, body):
        print(f"Received: {body.decode()}")
        ch.basic_ack(delivery_tag=method.delivery_tag)


    # Set up consumer (will be restored after reconnection)
    channel2.basic_consume(queue='consumer_queue', on_message_callback=callback, auto_ack=False)

    print("Starting consumer... Press CTRL+C to exit")
    try:
        channel2.start_consuming()
    except KeyboardInterrupt:
        channel2.stop_consuming()
        connection2.close()

    # Example 4: Batch publishing
    connection3 = ResilientRabbitBlockingConnection(parameters=parameters)
    channel3 = connection3.channel()

    messages = [
        {
            'exchange': '',
            'routing_key': 'batch_queue',
            'body': f'Batch message {i}'.encode(),
            'properties': pika.BasicProperties(delivery_mode=2)
        }
        for i in range(50)
    ]

    results = connection3.publish_batch(channel3, messages)
    print(f"Successfully sent {sum(results)} out of {len(results)} messages")

    # Get any failed messages
    failed = connection3.get_failed_messages()
    if failed:
        print(f"Failed messages: {len(failed)}")

    connection3.close()

    # Example 5: Using with RPC pattern
    connection4 = ResilientRabbitBlockingConnection(parameters=parameters)
    channel4 = connection4.channel()

    # Declare RPC queue
    result = channel4.queue_declare(queue='', exclusive=True)
    callback_queue = result.method.queue


    class RPCClient:
        def __init__(self, channel, callback_queue):
            self.channel = channel
            self.callback_queue = callback_queue
            self.response = None
            self.corr_id = None

            self.channel.basic_consume(
                queue=callback_queue,
                on_message_callback=self.on_response,
                auto_ack=True
            )

        def on_response(self, ch, method, props, body):
            if self.corr_id == props.correlation_id:
                self.response = body

        def call(self, n):
            import uuid
            self.response = None
            self.corr_id = str(uuid.uuid4())

            self.channel.basic_publish(
                exchange='',
                routing_key='rpc_queue',
                properties=pika.BasicProperties(
                    reply_to=self.callback_queue,
                    correlation_id=self.corr_id,
                ),
                body=str(n).encode()
            )

            while self.response is None:
                connection4.process_data_events()

            return int(self.response)


    rpc_client = RPCClient(channel4, callback_queue)

    try:
        print("Requesting fib(30)")
        response = rpc_client.call(30)
        print(f"Got response: {response}")
    except Exception as e:
        print(f"RPC call failed: {e}")
    finally:
        connection4.close()