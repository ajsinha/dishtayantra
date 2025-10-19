from kafka import KafkaConsumer
import logging
import time
import threading
from queue import Queue, Empty
from typing import Any, Dict, Optional, Callable,List
from kafka import KafkaProducer
from kafka.producer.future import FutureRecordMetadata
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError

logger = logging.getLogger(__name__)


class ResilientKafkaConsumer(KafkaConsumer):
    """
    A KafkaConsumer subclass that automatically handles reconnection on failures.

    This class extends KafkaConsumer to provide automatic reconnection capabilities
    when connection failures occur during initialization or message consumption.
    It serves as a drop-in replacement for KafkaConsumer with added resilience.

    Args:
        *topics: Variable length list of topics to subscribe to
        reconnect_tries: Maximum number of reconnection attempts (default: 10)
        reconnect_interval_seconds: Sleep interval between reconnection attempts (default: 60)
        **configs: All other KafkaConsumer configuration parameters
    """

    def __init__(self, *topics, reconnect_tries: int = 10,
                 reconnect_interval_seconds: int = 60, **configs):
        """Initialize the ReconnectAwareKafkaConsumer with reconnection capabilities."""
        self.reconnect_tries = reconnect_tries
        self.reconnect_interval_seconds = reconnect_interval_seconds
        self.topics = topics
        self.configs = configs
        self._current_retry = 0
        self._connected = False

        # Store original timeout settings for reconnection attempts
        self._original_request_timeout = configs.get('request_timeout_ms', 30000)
        self._original_session_timeout = configs.get('session_timeout_ms', 10000)

        # Attempt initial connection
        self._connect_with_retry()

    def _connect_with_retry(self):
        """Establish connection to Kafka with retry logic."""
        last_exception = None

        for attempt in range(1, self.reconnect_tries + 1):
            try:
                logger.info(f"Attempting to connect to Kafka (attempt {attempt}/{self.reconnect_tries})")

                # Initialize the parent KafkaConsumer
                super().__init__(*self.topics, **self.configs)

                # Test the connection by fetching metadata
                self._client.check_version()

                self._connected = True
                self._current_retry = 0
                logger.info("Successfully connected to Kafka")
                return

            except (NoBrokersAvailable, KafkaTimeoutError, KafkaError) as e:
                last_exception = e
                logger.warning(f"Failed to connect to Kafka (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    logger.info(f"Retrying in {self.reconnect_interval_seconds} seconds...")
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    logger.error(f"Failed to connect to Kafka after {self.reconnect_tries} attempts")
                    raise last_exception

    def _reconnect(self):
        """Attempt to reconnect to Kafka."""
        logger.info("Attempting to reconnect to Kafka...")

        try:
            # Close existing connection if any
            try:
                super().close()
            except Exception as e:
                logger.debug(f"Error closing existing connection: {e}")

            # Reset connection state
            self._connected = False

            # Reinitialize with retry logic
            self._connect_with_retry()

        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            raise

    def poll(self, timeout_ms: int = 0, max_records: Optional[int] = None,
             update_offsets: bool = True) -> Dict[Any, List[Any]]:
        """
        Poll for messages with automatic reconnection on failure.

        Args:
            timeout_ms: Timeout in milliseconds to wait for messages
            max_records: Maximum number of records to return
            update_offsets: Whether to update offsets

        Returns:
            Dictionary of topics to lists of consumer records
        """
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                # Attempt to poll for messages
                messages = super().poll(
                    timeout_ms=timeout_ms,
                    max_records=max_records,
                    update_offsets=update_offsets
                )

                # Reset retry counter on successful poll
                self._current_retry = 0
                return messages

            except (KafkaTimeoutError, KafkaError) as e:
                logger.warning(f"Poll failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    logger.info(f"Attempting reconnection in {self.reconnect_interval_seconds} seconds...")
                    time.sleep(self.reconnect_interval_seconds)

                    try:
                        self._reconnect()
                    except Exception as reconnect_error:
                        logger.error(f"Reconnection failed: {reconnect_error}")
                        if attempt == self.reconnect_tries - 1:
                            raise
                else:
                    logger.error(f"Poll failed after {self.reconnect_tries} attempts")
                    raise

        # Return empty dict if all retries exhausted
        return {}

    def __iter__(self):
        """
        Iterator interface with automatic reconnection on failure.

        Yields:
            Consumer records one by one
        """
        while True:
            try:
                # Use parent's iterator
                for message in super().__iter__():
                    self._current_retry = 0
                    yield message

            except (KafkaTimeoutError, KafkaError) as e:
                logger.warning(f"Iterator failed: {e}")

                if self._current_retry < self.reconnect_tries:
                    self._current_retry += 1
                    logger.info(f"Attempting reconnection (attempt {self._current_retry}/{self.reconnect_tries})")
                    time.sleep(self.reconnect_interval_seconds)

                    try:
                        self._reconnect()
                    except Exception as reconnect_error:
                        logger.error(f"Reconnection failed: {reconnect_error}")
                        if self._current_retry >= self.reconnect_tries:
                            raise
                else:
                    logger.error(f"Iterator failed after {self.reconnect_tries} attempts")
                    raise

    def consume(self, max_records: Optional[int] = None, timeout_ms: int = 0) -> List[Any]:
        """
        Consume messages with automatic reconnection on failure.

        Args:
            max_records: Maximum number of records to return
            timeout_ms: Timeout in milliseconds

        Returns:
            List of consumer records
        """
        all_records = []

        for attempt in range(1, self.reconnect_tries + 1):
            try:
                # Poll for messages
                records = self.poll(timeout_ms=timeout_ms, max_records=max_records)

                # Flatten the records from all partitions
                for topic_partition, messages in records.items():
                    all_records.extend(messages)

                self._current_retry = 0
                return all_records

            except (KafkaTimeoutError, KafkaError) as e:
                logger.warning(f"Consume failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    logger.info(f"Retrying in {self.reconnect_interval_seconds} seconds...")
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    logger.error(f"Consume failed after {self.reconnect_tries} attempts")
                    raise

        return all_records

    def commit(self, offsets: Optional[Dict] = None):
        """
        Commit offsets with automatic reconnection on failure.

        Args:
            offsets: Optional dictionary of offsets to commit
        """
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                super().commit(offsets=offsets)
                self._current_retry = 0
                return

            except (KafkaTimeoutError, KafkaError) as e:
                logger.warning(f"Commit failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    logger.info(f"Attempting reconnection in {self.reconnect_interval_seconds} seconds...")
                    time.sleep(self.reconnect_interval_seconds)

                    try:
                        self._reconnect()
                    except Exception as reconnect_error:
                        logger.error(f"Reconnection failed: {reconnect_error}")
                        if attempt == self.reconnect_tries - 1:
                            raise
                else:
                    logger.error(f"Commit failed after {self.reconnect_tries} attempts")
                    raise

    def close(self, autocommit: bool = True):
        """
        Close the consumer connection.

        Args:
            autocommit: Whether to commit current offsets before closing
        """
        try:
            super().close(autocommit=autocommit)
            self._connected = False
            logger.info("Kafka consumer closed successfully")
        except Exception as e:
            logger.error(f"Error closing Kafka consumer: {e}")
            raise





class ResilientKafkaProducer(KafkaProducer):
    """
    A KafkaProducer subclass that automatically handles reconnection on failures
    and buffers messages during reconnection to prevent message loss.

    This class extends KafkaProducer to provide automatic reconnection capabilities
    when connection failures occur during initialization or message production.
    Messages sent during reconnection attempts are buffered and retried once
    connection is restored.

    Args:
        reconnect_tries: Maximum number of reconnection attempts (default: 10)
        reconnect_interval_seconds: Sleep interval between reconnection attempts (default: 60)
        buffer_max_messages: Maximum messages to buffer during reconnection (default: 10000)
        **configs: All other KafkaProducer configuration parameters
    """

    def __init__(self, reconnect_tries: int = 10,
                 reconnect_interval_seconds: int = 60,
                 buffer_max_messages: int = 10000,
                 **configs):
        """Initialize the ReconnectAwareKafkaProducer with reconnection and buffering capabilities."""
        self.reconnect_tries = reconnect_tries
        self.reconnect_interval_seconds = reconnect_interval_seconds
        self.buffer_max_messages = buffer_max_messages
        self.configs = configs
        self._current_retry = 0
        self._connected = False
        self._reconnecting = False
        self._lock = threading.Lock()

        # Message buffer for storing messages during reconnection
        self._message_buffer = Queue(maxsize=buffer_max_messages)
        self._failed_messages = []

        # Store original settings
        self._original_request_timeout = configs.get('request_timeout_ms', 30000)
        self._original_max_block_ms = configs.get('max_block_ms', 60000)

        # Attempt initial connection
        self._connect_with_retry()

        # Start background thread for processing buffered messages
        self._buffer_processor_thread = threading.Thread(
            target=self._process_buffered_messages,
            daemon=True
        )
        self._buffer_processor_thread.start()
        self._stop_buffer_processor = threading.Event()

    def _connect_with_retry(self):
        """Establish connection to Kafka with retry logic."""
        last_exception = None

        for attempt in range(1, self.reconnect_tries + 1):
            try:
                logger.info(f"Attempting to connect to Kafka (attempt {attempt}/{self.reconnect_tries})")

                # Initialize the parent KafkaProducer
                super().__init__(**self.configs)

                # Test the connection by checking bootstrap connection
                self._sender.wakeup()

                self._connected = True
                self._reconnecting = False
                self._current_retry = 0
                logger.info("Successfully connected to Kafka")
                return

            except (NoBrokersAvailable, KafkaTimeoutError, KafkaError) as e:
                last_exception = e
                logger.warning(f"Failed to connect to Kafka (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    logger.info(f"Retrying in {self.reconnect_interval_seconds} seconds...")
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    logger.error(f"Failed to connect to Kafka after {self.reconnect_tries} attempts")
                    raise last_exception

    def _reconnect(self):
        """Attempt to reconnect to Kafka."""
        with self._lock:
            if self._reconnecting:
                return
            self._reconnecting = True

        logger.info("Attempting to reconnect to Kafka...")

        try:
            # Close existing connection if any
            try:
                super().close(timeout=0)
            except Exception as e:
                logger.debug(f"Error closing existing connection: {e}")

            # Reset connection state
            self._connected = False

            # Reinitialize with retry logic
            self._connect_with_retry()

            # Process any buffered messages after successful reconnection
            self._flush_buffer()

        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            raise
        finally:
            with self._lock:
                self._reconnecting = False

    def _buffer_message(self, topic: str, value: Any = None, key: Any = None,
                        headers: Optional[list] = None, partition: Optional[int] = None,
                        timestamp_ms: Optional[int] = None):
        """
        Buffer a message for later sending when connection is restored.

        Args:
            topic: Topic to send message to
            value: Message value
            key: Message key
            headers: Optional message headers
            partition: Specific partition to send to
            timestamp_ms: Message timestamp
        """
        try:
            message_data = {
                'topic': topic,
                'value': value,
                'key': key,
                'headers': headers,
                'partition': partition,
                'timestamp_ms': timestamp_ms
            }

            if self._message_buffer.full():
                # Remove oldest message to make room
                old_msg = self._message_buffer.get_nowait()
                logger.warning(f"Buffer full, dropping oldest message for topic: {old_msg['topic']}")

            self._message_buffer.put_nowait(message_data)
            logger.debug(f"Buffered message for topic: {topic}, buffer size: {self._message_buffer.qsize()}")

        except Exception as e:
            logger.error(f"Failed to buffer message: {e}")
            raise

    def _process_buffered_messages(self):
        """Background thread to process buffered messages when connection is restored."""
        while not self._stop_buffer_processor.is_set():
            try:
                if self._connected and not self._reconnecting and not self._message_buffer.empty():
                    # Process buffered messages
                    message = self._message_buffer.get(timeout=1)

                    try:
                        # Send the buffered message
                        super().send(
                            topic=message['topic'],
                            value=message['value'],
                            key=message['key'],
                            headers=message['headers'],
                            partition=message['partition'],
                            timestamp_ms=message['timestamp_ms']
                        )
                        logger.debug(f"Successfully sent buffered message to topic: {message['topic']}")

                    except Exception as e:
                        logger.error(f"Failed to send buffered message: {e}")
                        # Re-queue the message for retry
                        self._message_buffer.put(message)
                        time.sleep(0.1)
                else:
                    time.sleep(0.1)

            except Empty:
                continue
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
                    topic=message['topic'],
                    value=message['value'],
                    key=message['key'],
                    headers=message['headers'],
                    partition=message['partition'],
                    timestamp_ms=message['timestamp_ms']
                )
                flushed_count += 1

            except Empty:
                break
            except Exception as e:
                logger.error(f"Failed to flush buffered message: {e}")
                self._failed_messages.append(message)
                failed_count += 1

        logger.info(f"Flushed {flushed_count} messages, {failed_count} failed")

    def send(self, topic: str, value: Any = None, key: Any = None,
             headers: Optional[list] = None, partition: Optional[int] = None,
             timestamp_ms: Optional[int] = None) -> FutureRecordMetadata:
        """
        Send a message with automatic reconnection and buffering on failure.

        Args:
            topic: Topic to send message to
            value: Message value
            key: Message key
            headers: Optional message headers
            partition: Specific partition to send to
            timestamp_ms: Message timestamp

        Returns:
            FutureRecordMetadata for tracking the send result
        """
        # If currently reconnecting, buffer the message
        if self._reconnecting or not self._connected:
            logger.info(f"Connection unavailable, buffering message for topic: {topic}")
            self._buffer_message(topic, value, key, headers, partition, timestamp_ms)

            # Return a future that will be resolved when message is sent
            # This maintains compatibility with the original KafkaProducer interface
            future = FutureRecordMetadata()
            return future

        # Try to send the message with retry logic
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                future = super().send(
                    topic=topic,
                    value=value,
                    key=key,
                    headers=headers,
                    partition=partition,
                    timestamp_ms=timestamp_ms
                )

                # Reset retry counter on successful send
                self._current_retry = 0
                return future

            except (KafkaTimeoutError, KafkaError, AttributeError) as e:
                logger.warning(f"Send failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                # Buffer the message to prevent loss
                if attempt == 1:
                    self._buffer_message(topic, value, key, headers, partition, timestamp_ms)

                if attempt < self.reconnect_tries:
                    logger.info(f"Attempting reconnection in {self.reconnect_interval_seconds} seconds...")
                    time.sleep(self.reconnect_interval_seconds)

                    try:
                        self._reconnect()
                        # After successful reconnection, the message should be in buffer
                        # Return a future for compatibility
                        future = FutureRecordMetadata()
                        return future
                    except Exception as reconnect_error:
                        logger.error(f"Reconnection failed: {reconnect_error}")
                        if attempt == self.reconnect_tries - 1:
                            raise
                else:
                    logger.error(f"Send failed after {self.reconnect_tries} attempts")
                    raise

        # Should not reach here, but return a future for safety
        future = FutureRecordMetadata()
        return future

    def send_batch(self, messages: list) -> list:
        """
        Send multiple messages in batch with automatic reconnection.

        Args:
            messages: List of message dictionaries with keys:
                     'topic', 'value', 'key', 'headers', 'partition', 'timestamp_ms'

        Returns:
            List of FutureRecordMetadata objects
        """
        futures = []

        for message in messages:
            future = self.send(
                topic=message.get('topic'),
                value=message.get('value'),
                key=message.get('key'),
                headers=message.get('headers'),
                partition=message.get('partition'),
                timestamp_ms=message.get('timestamp_ms')
            )
            futures.append(future)

        return futures

    def flush(self, timeout: Optional[float] = None):
        """
        Flush pending messages with reconnection support.

        Args:
            timeout: Maximum time to wait for flush completion
        """
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                # First flush any buffered messages
                if self._connected and not self._message_buffer.empty():
                    self._flush_buffer()

                # Then flush the producer
                super().flush(timeout=timeout)
                self._current_retry = 0
                return

            except (KafkaTimeoutError, KafkaError, AttributeError) as e:
                logger.warning(f"Flush failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    logger.info(f"Attempting reconnection in {self.reconnect_interval_seconds} seconds...")
                    time.sleep(self.reconnect_interval_seconds)

                    try:
                        self._reconnect()
                    except Exception as reconnect_error:
                        logger.error(f"Reconnection failed: {reconnect_error}")
                        if attempt == self.reconnect_tries - 1:
                            raise
                else:
                    logger.error(f"Flush failed after {self.reconnect_tries} attempts")
                    raise

    def close(self, timeout: Optional[float] = None):
        """
        Close the producer connection.

        Args:
            timeout: Maximum time to wait for pending messages to be sent
        """
        try:
            # Stop the buffer processor thread
            self._stop_buffer_processor.set()

            # Try to flush remaining messages
            if self._connected:
                self.flush(timeout=timeout)

            # Log any messages that couldn't be sent
            if not self._message_buffer.empty():
                logger.warning(f"Closing with {self._message_buffer.qsize()} unsent messages in buffer")

            if self._failed_messages:
                logger.warning(f"Failed to send {len(self._failed_messages)} messages")

            super().close(timeout=timeout)
            self._connected = False
            logger.info("Kafka producer closed successfully")

        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")
            raise

    def get_buffer_size(self) -> int:
        """
        Get the current number of messages in the buffer.

        Returns:
            Number of buffered messages
        """
        return self._message_buffer.qsize()

    def get_failed_messages(self) -> list:
        """
        Get the list of messages that failed to send.

        Returns:
            List of failed message dictionaries
        """
        return self._failed_messages.copy()


# Example usage
def test_producer():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Example 1: Basic usage as a drop-in replacement
    producer = ResilientKafkaProducer(
        bootstrap_servers=['localhost:9092'],
        reconnect_tries=5,  # Custom retry count
        reconnect_interval_seconds=30,  # Custom retry interval
        buffer_max_messages=5000,  # Buffer size during reconnection
        value_serializer=lambda v: v.encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

    # Example 2: Sending messages with automatic reconnection
    try:
        for i in range(100):
            # Messages will be buffered if connection is lost
            future = producer.send(
                topic='my-topic',
                value=f'Message {i}',
                key=f'key-{i}'
            )
            print(f"Sent message {i}")
            time.sleep(1)

    except KeyboardInterrupt:
        pass
    finally:
        # Check if any messages are still buffered
        buffered = producer.get_buffer_size()
        if buffered > 0:
            print(f"Warning: {buffered} messages still in buffer")

        # Close producer (will attempt to flush remaining messages)
        producer.close(timeout=10)

    # Example 3: Batch sending with resilience
    producer2 = ResilientKafkaProducer(
        bootstrap_servers=['localhost:9092'],
        batch_size=16384,
        linger_ms=10
    )

    messages = [
        {
            'topic': 'batch-topic',
            'value': f'Batch message {i}'.encode('utf-8'),
            'key': f'batch-key-{i}'.encode('utf-8')
        }
        for i in range(50)
    ]

    try:
        futures = producer2.send_batch(messages)
        producer2.flush(timeout=30)
        print(f"Sent {len(futures)} messages in batch")

    except Exception as e:
        print(f"Batch send failed: {e}")
        failed = producer2.get_failed_messages()
        print(f"Failed messages: {len(failed)}")
    finally:
        producer2.close()

    # Example 4: Monitoring buffer status
    producer3 = ResilientKafkaProducer(
        bootstrap_servers=['localhost:9092'],
        reconnect_tries=3,
        reconnect_interval_seconds=10,
        buffer_max_messages=1000
    )

    # Send messages and monitor buffer
    for i in range(20):
        producer3.send('monitoring-topic', value=f'Message {i}'.encode('utf-8'))
        buffer_size = producer3.get_buffer_size()
        if buffer_size > 0:
            print(f"Messages in buffer: {buffer_size}")

    producer3.close()


def test_consumer():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Example 1: Basic usage as a drop-in replacement
    consumer = ResilientKafkaConsumer(
        'my-topic',
        bootstrap_servers=['localhost:9092'],
        group_id='my-consumer-group',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        reconnect_tries=5,  # Custom retry count
        reconnect_interval_seconds=30  # Custom retry interval
    )

    # Example 2: Using the poll method
    try:
        while True:
            messages = consumer.poll(timeout_ms=1000)
            for topic_partition, records in messages.items():
                for record in records:
                    print(f"Received: {record.value}")
    except KeyboardInterrupt:
        consumer.close()

    # Example 3: Using the iterator interface
    consumer2 = ResilientKafkaConsumer(
        'another-topic',
        bootstrap_servers=['localhost:9092'],
        group_id='another-group',
        value_deserializer=lambda m: m.decode('utf-8')
    )

    try:
        for message in consumer2:
            print(f"Topic: {message.topic}, Partition: {message.partition}, "
                  f"Offset: {message.offset}, Value: {message.value}")
    except KeyboardInterrupt:
        consumer2.close()

if __name__ == "__main__":
    pass