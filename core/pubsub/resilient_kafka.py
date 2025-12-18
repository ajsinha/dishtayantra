"""
Resilient Kafka Consumer and Producer with Dual Library Support

This module provides resilient Kafka consumer and producer implementations
with automatic reconnection and support for both kafka-python and confluent-kafka.

Features:
    - Automatic reconnection on failures
    - Message buffering during disconnection
    - Configurable retry settings
    - Support for kafka-python and confluent-kafka
    
Configuration:
    kafka_library: 'kafka-python' | 'confluent-kafka' (default: 'kafka-python')

Performance Comparison:
    - kafka-python: Pure Python, ~50K msg/sec
    - confluent-kafka: C-based (librdkafka), ~500K msg/sec

PATENT PENDING: The Multi-Broker Message Routing Architecture is subject to
pending patent applications.

Copyright © 2025-2030 Ashutosh Sinha. All rights reserved.
DishtaYantra™ is a trademark of Ashutosh Sinha.
"""

import logging
import time
import threading
import json
from queue import Queue, Empty
from typing import Any, Dict, Optional, Callable, List
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# =============================================================================
# Library Detection
# =============================================================================

KAFKA_PYTHON_AVAILABLE = False
CONFLUENT_KAFKA_AVAILABLE = False

try:
    from kafka import KafkaConsumer as KafkaPythonConsumer
    from kafka import KafkaProducer as KafkaPythonProducer
    from kafka.producer.future import FutureRecordMetadata
    from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError
    KAFKA_PYTHON_AVAILABLE = True
except ImportError:
    pass

try:
    from confluent_kafka import Producer as ConfluentProducer
    from confluent_kafka import Consumer as ConfluentConsumer
    from confluent_kafka import KafkaError as ConfluentKafkaError
    from confluent_kafka import KafkaException as ConfluentKafkaException
    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    pass


# =============================================================================
# Abstract Resilient Classes
# =============================================================================

class AbstractResilientConsumer(ABC):
    """Abstract base for resilient Kafka consumers."""
    
    @abstractmethod
    def poll(self, timeout_ms: int = 0, max_records: Optional[int] = None) -> Dict:
        pass
    
    @abstractmethod
    def consume(self, max_records: Optional[int] = None, timeout_ms: int = 0) -> List:
        pass
    
    @abstractmethod
    def commit(self, offsets: Optional[Dict] = None):
        pass
    
    @abstractmethod
    def close(self):
        pass
    
    @abstractmethod
    def __iter__(self):
        pass


class AbstractResilientProducer(ABC):
    """Abstract base for resilient Kafka producers."""
    
    @abstractmethod
    def send(self, topic: str, value: Any = None, key: Any = None,
             headers: Optional[List] = None, partition: Optional[int] = None,
             timestamp_ms: Optional[int] = None) -> Any:
        pass
    
    @abstractmethod
    def flush(self, timeout: Optional[float] = None):
        pass
    
    @abstractmethod
    def close(self, timeout: Optional[float] = None):
        pass
    
    @abstractmethod
    def send_batch(self, messages: List[Dict]) -> List:
        pass


# =============================================================================
# kafka-python Resilient Consumer
# =============================================================================

class ResilientKafkaPythonConsumer(AbstractResilientConsumer):
    """
    Resilient Kafka consumer using kafka-python with automatic reconnection.
    """
    
    def __init__(self, *topics, reconnect_tries: int = 10,
                 reconnect_interval_seconds: int = 60, **configs):
        self.reconnect_tries = reconnect_tries
        self.reconnect_interval_seconds = reconnect_interval_seconds
        self.topics = topics
        self.configs = configs
        self._current_retry = 0
        self._connected = False
        self._consumer = None
        
        self._connect_with_retry()
    
    def _connect_with_retry(self):
        """Establish connection with retry logic."""
        from kafka import KafkaConsumer
        from kafka.errors import NoBrokersAvailable, KafkaTimeoutError, KafkaError
        
        last_exception = None
        
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                logger.info(f"Attempting to connect to Kafka (attempt {attempt}/{self.reconnect_tries})")
                
                self._consumer = KafkaConsumer(*self.topics, **self.configs)
                self._consumer._client.check_version()
                
                self._connected = True
                self._current_retry = 0
                logger.info("Successfully connected to Kafka")
                return
                
            except (NoBrokersAvailable, KafkaTimeoutError, KafkaError) as e:
                last_exception = e
                logger.warning(f"Failed to connect (attempt {attempt}/{self.reconnect_tries}): {e}")
                
                if attempt < self.reconnect_tries:
                    logger.info(f"Retrying in {self.reconnect_interval_seconds} seconds...")
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    logger.error(f"Failed after {self.reconnect_tries} attempts")
                    raise last_exception
    
    def _reconnect(self):
        """Attempt to reconnect."""
        logger.info("Attempting to reconnect to Kafka...")
        try:
            if self._consumer:
                try:
                    self._consumer.close()
                except:
                    pass
            self._connected = False
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            raise
    
    def poll(self, timeout_ms: int = 0, max_records: Optional[int] = None,
             update_offsets: bool = True) -> Dict:
        """Poll for messages with automatic reconnection."""
        from kafka.errors import KafkaTimeoutError, KafkaError
        
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                messages = self._consumer.poll(
                    timeout_ms=timeout_ms,
                    max_records=max_records,
                    update_offsets=update_offsets
                )
                self._current_retry = 0
                return messages
                
            except (KafkaTimeoutError, KafkaError) as e:
                logger.warning(f"Poll failed (attempt {attempt}): {e}")
                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                    try:
                        self._reconnect()
                    except:
                        if attempt == self.reconnect_tries - 1:
                            raise
                else:
                    raise
        return {}
    
    def consume(self, max_records: Optional[int] = None, timeout_ms: int = 0) -> List:
        """Consume messages with automatic reconnection."""
        all_records = []
        records = self.poll(timeout_ms=timeout_ms, max_records=max_records)
        for topic_partition, messages in records.items():
            all_records.extend(messages)
        return all_records
    
    def commit(self, offsets: Optional[Dict] = None):
        """Commit offsets with automatic reconnection."""
        from kafka.errors import KafkaTimeoutError, KafkaError
        
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                self._consumer.commit(offsets=offsets)
                return
            except (KafkaTimeoutError, KafkaError) as e:
                logger.warning(f"Commit failed (attempt {attempt}): {e}")
                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                    try:
                        self._reconnect()
                    except:
                        pass
                else:
                    raise
    
    def close(self):
        """Close the consumer."""
        if self._consumer:
            self._consumer.close()
        self._connected = False
    
    def __iter__(self):
        """Iterator with automatic reconnection."""
        from kafka.errors import KafkaTimeoutError, KafkaError
        
        while True:
            try:
                for message in self._consumer:
                    self._current_retry = 0
                    yield message
            except (KafkaTimeoutError, KafkaError) as e:
                logger.warning(f"Iterator failed: {e}")
                if self._current_retry < self.reconnect_tries:
                    self._current_retry += 1
                    time.sleep(self.reconnect_interval_seconds)
                    try:
                        self._reconnect()
                    except:
                        if self._current_retry >= self.reconnect_tries:
                            raise
                else:
                    raise


# =============================================================================
# kafka-python Resilient Producer
# =============================================================================

class ResilientKafkaPythonProducer(AbstractResilientProducer):
    """
    Resilient Kafka producer using kafka-python with automatic reconnection.
    """
    
    def __init__(self, reconnect_tries: int = 10,
                 reconnect_interval_seconds: int = 60,
                 buffer_max_messages: int = 10000, **configs):
        self.reconnect_tries = reconnect_tries
        self.reconnect_interval_seconds = reconnect_interval_seconds
        self.buffer_max_messages = buffer_max_messages
        self.configs = configs
        self._current_retry = 0
        self._connected = False
        self._producer = None
        
        # Message buffer for resilience
        self._message_buffer = Queue(maxsize=buffer_max_messages)
        self._failed_messages = []
        self._stop_buffer_processor = threading.Event()
        
        self._connect_with_retry()
        self._start_buffer_processor()
    
    def _connect_with_retry(self):
        """Establish connection with retry logic."""
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable, KafkaTimeoutError, KafkaError
        
        last_exception = None
        
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                logger.info(f"Attempting to connect to Kafka (attempt {attempt}/{self.reconnect_tries})")
                
                self._producer = KafkaProducer(**self.configs)
                self._connected = True
                self._current_retry = 0
                logger.info("Successfully connected to Kafka")
                return
                
            except (NoBrokersAvailable, KafkaTimeoutError, KafkaError) as e:
                last_exception = e
                logger.warning(f"Failed to connect (attempt {attempt}): {e}")
                
                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    logger.error(f"Failed after {self.reconnect_tries} attempts")
                    raise last_exception
    
    def _reconnect(self):
        """Attempt to reconnect."""
        logger.info("Attempting to reconnect to Kafka...")
        try:
            if self._producer:
                try:
                    self._producer.close(timeout=1)
                except:
                    pass
            self._connected = False
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            raise
    
    def _start_buffer_processor(self):
        """Start background thread to process buffered messages."""
        def processor():
            while not self._stop_buffer_processor.is_set():
                if self._connected and not self._message_buffer.empty():
                    self._flush_buffer()
                time.sleep(0.1)
        
        self._buffer_thread = threading.Thread(target=processor, daemon=True)
        self._buffer_thread.start()
    
    def _flush_buffer(self):
        """Flush buffered messages."""
        while not self._message_buffer.empty():
            try:
                msg = self._message_buffer.get_nowait()
                self._producer.send(**msg)
            except Empty:
                break
            except Exception as e:
                logger.error(f"Failed to send buffered message: {e}")
                self._failed_messages.append(msg)
    
    def send(self, topic: str, value: Any = None, key: Any = None,
             headers: Optional[List] = None, partition: Optional[int] = None,
             timestamp_ms: Optional[int] = None) -> Any:
        """Send message with automatic buffering and reconnection."""
        message = {
            'topic': topic,
            'value': value,
            'key': key,
            'headers': headers,
            'partition': partition,
            'timestamp_ms': timestamp_ms
        }
        message = {k: v for k, v in message.items() if v is not None}
        
        if not self._connected:
            self._message_buffer.put(message)
            return None
        
        try:
            future = self._producer.send(**message)
            self._current_retry = 0
            return future
        except Exception as e:
            logger.warning(f"Send failed, buffering message: {e}")
            self._message_buffer.put(message)
            
            if self._current_retry < self.reconnect_tries:
                self._current_retry += 1
                try:
                    self._reconnect()
                except:
                    pass
            return None
    
    def send_batch(self, messages: List[Dict]) -> List:
        """Send batch of messages."""
        futures = []
        for msg in messages:
            future = self.send(
                topic=msg.get('topic'),
                value=msg.get('value'),
                key=msg.get('key'),
                headers=msg.get('headers'),
                partition=msg.get('partition'),
                timestamp_ms=msg.get('timestamp_ms')
            )
            futures.append(future)
        return futures
    
    def flush(self, timeout: Optional[float] = None):
        """Flush pending messages."""
        if self._connected:
            self._flush_buffer()
            self._producer.flush(timeout=timeout)
    
    def close(self, timeout: Optional[float] = None):
        """Close the producer."""
        self._stop_buffer_processor.set()
        
        if self._connected:
            self.flush(timeout=timeout)
        
        if not self._message_buffer.empty():
            logger.warning(f"Closing with {self._message_buffer.qsize()} unsent messages")
        
        if self._producer:
            self._producer.close(timeout=timeout)
        self._connected = False
    
    def get_buffer_size(self) -> int:
        """Get number of messages in buffer."""
        return self._message_buffer.qsize()
    
    def get_failed_messages(self) -> list:
        """Get list of failed messages."""
        return self._failed_messages.copy()


# =============================================================================
# confluent-kafka Resilient Consumer
# =============================================================================

class ResilientConfluentConsumer(AbstractResilientConsumer):
    """
    Resilient Kafka consumer using confluent-kafka with automatic reconnection.
    
    ~10x faster than kafka-python due to librdkafka C library.
    """
    
    def __init__(self, *topics, reconnect_tries: int = 10,
                 reconnect_interval_seconds: int = 60, **configs):
        self.reconnect_tries = reconnect_tries
        self.reconnect_interval_seconds = reconnect_interval_seconds
        self.topics = list(topics)
        self.configs = configs
        self._current_retry = 0
        self._connected = False
        self._consumer = None
        self._running = True
        
        # Value deserializer
        self._value_deserializer = configs.pop('value_deserializer', 
                                               lambda m: json.loads(m.decode('utf-8')))
        
        self._connect_with_retry()
    
    def _connect_with_retry(self):
        """Establish connection with retry logic."""
        from confluent_kafka import Consumer, KafkaException
        
        last_exception = None
        
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                logger.info(f"Attempting to connect to Kafka (attempt {attempt}/{self.reconnect_tries})")
                
                # Build confluent config
                conf = dict(self.configs)
                if 'bootstrap_servers' in conf:
                    conf['bootstrap.servers'] = ','.join(conf.pop('bootstrap_servers'))
                if 'group_id' in conf:
                    conf['group.id'] = conf.pop('group_id')
                conf.setdefault('auto.offset.reset', 'earliest')
                
                self._consumer = Consumer(conf)
                self._consumer.subscribe(self.topics)
                
                self._connected = True
                self._current_retry = 0
                logger.info("Successfully connected to Kafka (confluent)")
                return
                
            except KafkaException as e:
                last_exception = e
                logger.warning(f"Failed to connect (attempt {attempt}): {e}")
                
                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    raise last_exception
    
    def _reconnect(self):
        """Attempt to reconnect."""
        logger.info("Attempting to reconnect...")
        try:
            if self._consumer:
                try:
                    self._consumer.close()
                except:
                    pass
            self._connected = False
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            raise
    
    def poll(self, timeout_ms: int = 0, max_records: Optional[int] = None,
             update_offsets: bool = True) -> Dict:
        """Poll for messages with automatic reconnection."""
        from confluent_kafka import KafkaException, KafkaError
        
        timeout_sec = timeout_ms / 1000.0 if timeout_ms > 0 else 1.0
        
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                msg = self._consumer.poll(timeout=timeout_sec)
                
                if msg is None:
                    return {}
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        return {}
                    else:
                        raise KafkaException(msg.error())
                
                # Return in kafka-python compatible format
                tp = (msg.topic(), msg.partition())
                
                class MessageWrapper:
                    def __init__(self, m, deserializer):
                        self.topic = m.topic()
                        self.partition = m.partition()
                        self.offset = m.offset()
                        self.key = m.key().decode('utf-8') if m.key() else None
                        try:
                            self.value = deserializer(m.value())
                        except:
                            self.value = m.value()
                        self.timestamp = m.timestamp()
                
                return {tp: [MessageWrapper(msg, self._value_deserializer)]}
                
            except KafkaException as e:
                logger.warning(f"Poll failed (attempt {attempt}): {e}")
                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                    try:
                        self._reconnect()
                    except:
                        pass
                else:
                    raise
        return {}
    
    def consume(self, max_records: Optional[int] = None, timeout_ms: int = 0) -> List:
        """Consume messages with automatic reconnection."""
        all_records = []
        records = self.poll(timeout_ms=timeout_ms, max_records=max_records)
        for tp, messages in records.items():
            all_records.extend(messages)
        return all_records
    
    def commit(self, offsets: Optional[Dict] = None):
        """Commit offsets."""
        try:
            self._consumer.commit()
        except Exception as e:
            logger.warning(f"Commit failed: {e}")
    
    def close(self):
        """Close the consumer."""
        self._running = False
        if self._consumer:
            self._consumer.close()
        self._connected = False
    
    def __iter__(self):
        """Iterator with automatic reconnection."""
        while self._running:
            records = self.poll(timeout_ms=100)
            for tp, messages in records.items():
                for msg in messages:
                    yield msg


# =============================================================================
# confluent-kafka Resilient Producer
# =============================================================================

class ResilientConfluentProducer(AbstractResilientProducer):
    """
    Resilient Kafka producer using confluent-kafka with automatic reconnection.
    
    ~10x faster than kafka-python due to librdkafka C library.
    """
    
    def __init__(self, reconnect_tries: int = 10,
                 reconnect_interval_seconds: int = 60,
                 buffer_max_messages: int = 10000, **configs):
        self.reconnect_tries = reconnect_tries
        self.reconnect_interval_seconds = reconnect_interval_seconds
        self.buffer_max_messages = buffer_max_messages
        self.configs = configs
        self._current_retry = 0
        self._connected = False
        self._producer = None
        
        # Message buffer
        self._message_buffer = Queue(maxsize=buffer_max_messages)
        self._failed_messages = []
        self._stop_buffer_processor = threading.Event()
        
        # Value serializer
        self._value_serializer = configs.pop('value_serializer',
                                             lambda v: json.dumps(v).encode('utf-8'))
        
        self._connect_with_retry()
        self._start_buffer_processor()
    
    def _connect_with_retry(self):
        """Establish connection with retry logic."""
        from confluent_kafka import Producer, KafkaException
        
        last_exception = None
        
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                logger.info(f"Attempting to connect to Kafka (attempt {attempt}/{self.reconnect_tries})")
                
                # Build confluent config
                conf = dict(self.configs)
                if 'bootstrap_servers' in conf:
                    conf['bootstrap.servers'] = ','.join(conf.pop('bootstrap_servers'))
                
                # Performance optimizations
                conf.setdefault('queue.buffering.max.messages', 100000)
                conf.setdefault('queue.buffering.max.ms', 5)
                conf.setdefault('batch.num.messages', 10000)
                
                self._producer = Producer(conf)
                self._connected = True
                self._current_retry = 0
                logger.info("Successfully connected to Kafka (confluent)")
                return
                
            except KafkaException as e:
                last_exception = e
                logger.warning(f"Failed to connect (attempt {attempt}): {e}")
                
                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    raise last_exception
    
    def _reconnect(self):
        """Attempt to reconnect."""
        logger.info("Attempting to reconnect...")
        try:
            self._connected = False
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            raise
    
    def _start_buffer_processor(self):
        """Start background thread to process buffered messages."""
        def processor():
            while not self._stop_buffer_processor.is_set():
                if self._connected and not self._message_buffer.empty():
                    self._flush_buffer()
                if self._connected:
                    self._producer.poll(0)
                time.sleep(0.1)
        
        self._buffer_thread = threading.Thread(target=processor, daemon=True)
        self._buffer_thread.start()
    
    def _flush_buffer(self):
        """Flush buffered messages."""
        while not self._message_buffer.empty():
            try:
                msg = self._message_buffer.get_nowait()
                self._producer.produce(**msg)
            except Empty:
                break
            except Exception as e:
                logger.error(f"Failed to send buffered message: {e}")
                self._failed_messages.append(msg)
    
    def send(self, topic: str, value: Any = None, key: Any = None,
             headers: Optional[List] = None, partition: Optional[int] = None,
             timestamp_ms: Optional[int] = None) -> Any:
        """Send message with automatic buffering and reconnection."""
        # Serialize value
        serialized_value = self._value_serializer(value) if value else None
        serialized_key = key.encode('utf-8') if isinstance(key, str) else key
        
        message = {
            'topic': topic,
            'value': serialized_value,
        }
        if serialized_key is not None:
            message['key'] = serialized_key
        if headers is not None:
            message['headers'] = headers
        if partition is not None:
            message['partition'] = partition
        
        if not self._connected:
            self._message_buffer.put(message)
            return None
        
        try:
            self._producer.produce(**message)
            self._producer.poll(0)
            self._current_retry = 0
            return None
        except Exception as e:
            logger.warning(f"Send failed, buffering: {e}")
            self._message_buffer.put(message)
            
            if self._current_retry < self.reconnect_tries:
                self._current_retry += 1
                try:
                    self._reconnect()
                except:
                    pass
            return None
    
    def send_batch(self, messages: List[Dict]) -> List:
        """Send batch of messages."""
        results = []
        for msg in messages:
            result = self.send(
                topic=msg.get('topic'),
                value=msg.get('value'),
                key=msg.get('key'),
                headers=msg.get('headers'),
                partition=msg.get('partition'),
                timestamp_ms=msg.get('timestamp_ms')
            )
            results.append(result)
        return results
    
    def flush(self, timeout: Optional[float] = None):
        """Flush pending messages."""
        if self._connected:
            self._flush_buffer()
            if timeout:
                self._producer.flush(timeout=timeout)
            else:
                self._producer.flush()
    
    def close(self, timeout: Optional[float] = None):
        """Close the producer."""
        self._stop_buffer_processor.set()
        
        if self._connected:
            self.flush(timeout=timeout)
        
        if not self._message_buffer.empty():
            logger.warning(f"Closing with {self._message_buffer.qsize()} unsent messages")
        
        self._connected = False
    
    def get_buffer_size(self) -> int:
        """Get number of messages in buffer."""
        return self._message_buffer.qsize()
    
    def get_failed_messages(self) -> list:
        """Get list of failed messages."""
        return self._failed_messages.copy()


# =============================================================================
# Factory Functions
# =============================================================================

def create_resilient_consumer(*topics, kafka_library: str = 'kafka-python',
                              reconnect_tries: int = 10,
                              reconnect_interval_seconds: int = 60,
                              **configs) -> AbstractResilientConsumer:
    """
    Factory function to create resilient Kafka consumer.
    
    Args:
        *topics: Topics to subscribe to
        kafka_library: 'kafka-python' or 'confluent-kafka'
        reconnect_tries: Max reconnection attempts
        reconnect_interval_seconds: Interval between reconnection attempts
        **configs: Additional Kafka consumer configuration
        
    Returns:
        Appropriate resilient consumer instance
    """
    if kafka_library == 'confluent-kafka':
        if not CONFLUENT_KAFKA_AVAILABLE:
            logger.warning("confluent-kafka not available, falling back to kafka-python")
            kafka_library = 'kafka-python'
        else:
            return ResilientConfluentConsumer(
                *topics,
                reconnect_tries=reconnect_tries,
                reconnect_interval_seconds=reconnect_interval_seconds,
                **configs
            )
    
    if kafka_library == 'kafka-python':
        if not KAFKA_PYTHON_AVAILABLE:
            raise ImportError("No Kafka library available")
        return ResilientKafkaPythonConsumer(
            *topics,
            reconnect_tries=reconnect_tries,
            reconnect_interval_seconds=reconnect_interval_seconds,
            **configs
        )
    
    raise ValueError(f"Unknown kafka_library: {kafka_library}")


def create_resilient_producer(kafka_library: str = 'kafka-python',
                             reconnect_tries: int = 10,
                             reconnect_interval_seconds: int = 60,
                             buffer_max_messages: int = 10000,
                             **configs) -> AbstractResilientProducer:
    """
    Factory function to create resilient Kafka producer.
    
    Args:
        kafka_library: 'kafka-python' or 'confluent-kafka'
        reconnect_tries: Max reconnection attempts
        reconnect_interval_seconds: Interval between reconnection attempts
        buffer_max_messages: Max messages to buffer during disconnection
        **configs: Additional Kafka producer configuration
        
    Returns:
        Appropriate resilient producer instance
    """
    if kafka_library == 'confluent-kafka':
        if not CONFLUENT_KAFKA_AVAILABLE:
            logger.warning("confluent-kafka not available, falling back to kafka-python")
            kafka_library = 'kafka-python'
        else:
            return ResilientConfluentProducer(
                reconnect_tries=reconnect_tries,
                reconnect_interval_seconds=reconnect_interval_seconds,
                buffer_max_messages=buffer_max_messages,
                **configs
            )
    
    if kafka_library == 'kafka-python':
        if not KAFKA_PYTHON_AVAILABLE:
            raise ImportError("No Kafka library available")
        return ResilientKafkaPythonProducer(
            reconnect_tries=reconnect_tries,
            reconnect_interval_seconds=reconnect_interval_seconds,
            buffer_max_messages=buffer_max_messages,
            **configs
        )
    
    raise ValueError(f"Unknown kafka_library: {kafka_library}")


# =============================================================================
# Backward Compatibility Aliases
# =============================================================================

# These provide backward compatibility with existing code
class ResilientKafkaConsumer(ResilientKafkaPythonConsumer):
    """Backward compatibility alias for ResilientKafkaPythonConsumer."""
    pass


class ResilientKafkaProducer(ResilientKafkaPythonProducer):
    """Backward compatibility alias for ResilientKafkaPythonProducer."""
    pass


# =============================================================================
# Example Usage
# =============================================================================

def example_usage():
    """Example usage of resilient Kafka with dual library support."""
    logging.basicConfig(level=logging.INFO)
    
    print("=" * 60)
    print("Resilient Kafka with Dual Library Support")
    print("=" * 60)
    
    print(f"\nkafka-python available: {KAFKA_PYTHON_AVAILABLE}")
    print(f"confluent-kafka available: {CONFLUENT_KAFKA_AVAILABLE}")
    
    # Example: Create producer with confluent-kafka (if available)
    print("\n--- Creating Producer ---")
    producer = create_resilient_producer(
        kafka_library='confluent-kafka' if CONFLUENT_KAFKA_AVAILABLE else 'kafka-python',
        bootstrap_servers=['localhost:9092'],
        reconnect_tries=5,
        reconnect_interval_seconds=10
    )
    print(f"Producer type: {type(producer).__name__}")
    
    # Example: Create consumer
    print("\n--- Creating Consumer ---")
    consumer = create_resilient_consumer(
        'test-topic',
        kafka_library='confluent-kafka' if CONFLUENT_KAFKA_AVAILABLE else 'kafka-python',
        bootstrap_servers=['localhost:9092'],
        group_id='test-group',
        reconnect_tries=5,
        reconnect_interval_seconds=10
    )
    print(f"Consumer type: {type(consumer).__name__}")


if __name__ == '__main__':
    example_usage()
