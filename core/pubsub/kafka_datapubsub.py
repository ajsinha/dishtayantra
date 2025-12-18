"""
Kafka DataPublisher and DataSubscriber with Dual Library Support

This module provides Kafka-based data publishing and subscribing capabilities
with support for both kafka-python and confluent-kafka libraries.

Configuration:
    kafka_library: 'kafka-python' | 'confluent-kafka' (default: 'kafka-python')

Performance Comparison:
    - kafka-python: Pure Python, easier setup, ~50K msg/sec
    - confluent-kafka: C-based (librdkafka), higher performance, ~500K msg/sec

PATENT PENDING: The Multi-Broker Message Routing Architecture is subject to
pending patent applications.

Copyright © 2025-2030 Ashutosh Sinha. All rights reserved.
DishtaYantra™ is a trademark of Ashutosh Sinha.
"""

import json
import logging
import queue
from datetime import datetime
from typing import Any, Dict, Optional, Callable, List
from abc import ABC, abstractmethod

from core.pubsub.datapubsub import DataPublisher, DataSubscriber, DataAwarePayload

logger = logging.getLogger(__name__)

# =============================================================================
# Library Detection
# =============================================================================

KAFKA_PYTHON_AVAILABLE = False
CONFLUENT_KAFKA_AVAILABLE = False

try:
    from kafka import KafkaProducer as KafkaPythonProducer
    from kafka import KafkaConsumer as KafkaPythonConsumer
    from kafka.errors import KafkaError as KafkaPythonError
    KAFKA_PYTHON_AVAILABLE = True
    logger.debug("kafka-python library available")
except ImportError:
    logger.debug("kafka-python library not available")

try:
    from confluent_kafka import Producer as ConfluentProducer
    from confluent_kafka import Consumer as ConfluentConsumer
    from confluent_kafka import KafkaError as ConfluentKafkaError
    from confluent_kafka import KafkaException as ConfluentKafkaException
    CONFLUENT_KAFKA_AVAILABLE = True
    logger.debug("confluent-kafka library available")
except ImportError:
    logger.debug("confluent-kafka library not available")


def get_available_libraries() -> List[str]:
    """Get list of available Kafka libraries."""
    libs = []
    if KAFKA_PYTHON_AVAILABLE:
        libs.append('kafka-python')
    if CONFLUENT_KAFKA_AVAILABLE:
        libs.append('confluent-kafka')
    return libs


# =============================================================================
# Abstract Producer/Consumer Wrappers
# =============================================================================

class AbstractKafkaProducerWrapper(ABC):
    """Abstract wrapper for Kafka producers."""
    
    @abstractmethod
    def send(self, topic: str, value: Any, key: Optional[Any] = None,
             headers: Optional[List] = None, partition: Optional[int] = None) -> Any:
        """Send a message to Kafka topic."""
        pass
    
    @abstractmethod
    def flush(self, timeout: Optional[float] = None):
        """Flush pending messages."""
        pass
    
    @abstractmethod
    def close(self, timeout: Optional[float] = None):
        """Close the producer."""
        pass


class AbstractKafkaConsumerWrapper(ABC):
    """Abstract wrapper for Kafka consumers."""
    
    @abstractmethod
    def poll(self, timeout_ms: int = 0) -> Optional[Dict[str, Any]]:
        """Poll for messages."""
        pass
    
    @abstractmethod
    def subscribe(self, topics: List[str]):
        """Subscribe to topics."""
        pass
    
    @abstractmethod
    def close(self):
        """Close the consumer."""
        pass
    
    @abstractmethod
    def __iter__(self):
        """Iterator interface."""
        pass


# =============================================================================
# kafka-python Wrappers
# =============================================================================

class KafkaPythonProducerWrapper(AbstractKafkaProducerWrapper):
    """Wrapper for kafka-python Producer."""
    
    def __init__(self, bootstrap_servers: List[str], config: Dict[str, Any] = None):
        if not KAFKA_PYTHON_AVAILABLE:
            raise ImportError("kafka-python library not installed. Install with: pip install kafka-python")
        
        config = config or {}
        producer_config = config.get('producer_config', {})
        
        # Default serializer
        if 'value_serializer' not in producer_config:
            producer_config['value_serializer'] = lambda v: json.dumps(v).encode('utf-8')
        
        self._producer = KafkaPythonProducer(
            bootstrap_servers=bootstrap_servers,
            **producer_config
        )
        logger.info(f"kafka-python producer initialized: {bootstrap_servers}")
    
    def send(self, topic: str, value: Any, key: Optional[Any] = None,
             headers: Optional[List] = None, partition: Optional[int] = None) -> Any:
        """Send message using kafka-python."""
        kwargs = {'topic': topic, 'value': value}
        if key is not None:
            kwargs['key'] = key
        if headers is not None:
            kwargs['headers'] = headers
        if partition is not None:
            kwargs['partition'] = partition
        
        future = self._producer.send(**kwargs)
        return future
    
    def flush(self, timeout: Optional[float] = None):
        """Flush pending messages."""
        self._producer.flush(timeout=timeout)
    
    def close(self, timeout: Optional[float] = None):
        """Close the producer."""
        self._producer.close(timeout=timeout)


class KafkaPythonConsumerWrapper(AbstractKafkaConsumerWrapper):
    """Wrapper for kafka-python Consumer."""
    
    def __init__(self, topics: List[str], bootstrap_servers: List[str],
                 group_id: str, config: Dict[str, Any] = None):
        if not KAFKA_PYTHON_AVAILABLE:
            raise ImportError("kafka-python library not installed. Install with: pip install kafka-python")
        
        config = config or {}
        consumer_config = config.get('consumer_config', {})
        
        # Default deserializer
        if 'value_deserializer' not in consumer_config:
            consumer_config['value_deserializer'] = lambda m: json.loads(m.decode('utf-8'))
        
        # Set consumer timeout for non-blocking iteration
        if 'consumer_timeout_ms' not in consumer_config:
            consumer_config['consumer_timeout_ms'] = 100
        
        self._consumer = KafkaPythonConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            **consumer_config
        )
        self._topics = topics
        logger.info(f"kafka-python consumer initialized: {topics}")
    
    def poll(self, timeout_ms: int = 0) -> Optional[Dict[str, Any]]:
        """Poll for messages."""
        records = self._consumer.poll(timeout_ms=timeout_ms)
        return records
    
    def subscribe(self, topics: List[str]):
        """Subscribe to topics."""
        self._consumer.subscribe(topics)
        self._topics = topics
    
    def close(self):
        """Close the consumer."""
        self._consumer.close()
    
    def __iter__(self):
        """Iterator interface."""
        return iter(self._consumer)
    
    def get_single_message(self) -> Optional[Any]:
        """Get a single message (for compatibility)."""
        for message in self._consumer:
            return message.value
        return None


# =============================================================================
# confluent-kafka Wrappers
# =============================================================================

class ConfluentKafkaProducerWrapper(AbstractKafkaProducerWrapper):
    """
    Wrapper for confluent-kafka Producer.
    
    confluent-kafka uses librdkafka (C library) and provides significantly
    higher throughput than kafka-python (~10x faster).
    """
    
    def __init__(self, bootstrap_servers: List[str], config: Dict[str, Any] = None):
        if not CONFLUENT_KAFKA_AVAILABLE:
            raise ImportError("confluent-kafka library not installed. Install with: pip install confluent-kafka")
        
        config = config or {}
        
        # Build confluent-kafka configuration
        producer_config = {
            'bootstrap.servers': ','.join(bootstrap_servers),
        }
        
        # Map common config options
        confluent_config = config.get('confluent_config', {})
        producer_config.update(confluent_config)
        
        # Performance optimizations (can be overridden by config)
        producer_config.setdefault('queue.buffering.max.messages', 100000)
        producer_config.setdefault('queue.buffering.max.ms', 5)
        producer_config.setdefault('batch.num.messages', 10000)
        
        self._producer = ConfluentProducer(producer_config)
        self._value_serializer = config.get('value_serializer', 
                                            lambda v: json.dumps(v).encode('utf-8'))
        self._key_serializer = config.get('key_serializer',
                                          lambda k: k.encode('utf-8') if isinstance(k, str) else k)
        
        logger.info(f"confluent-kafka producer initialized: {bootstrap_servers}")
    
    def send(self, topic: str, value: Any, key: Optional[Any] = None,
             headers: Optional[List] = None, partition: Optional[int] = None) -> Any:
        """Send message using confluent-kafka."""
        # Serialize value
        serialized_value = self._value_serializer(value)
        
        # Serialize key if provided
        serialized_key = None
        if key is not None:
            serialized_key = self._key_serializer(key)
        
        # Build kwargs
        kwargs = {
            'topic': topic,
            'value': serialized_value,
        }
        if serialized_key is not None:
            kwargs['key'] = serialized_key
        if headers is not None:
            kwargs['headers'] = headers
        if partition is not None:
            kwargs['partition'] = partition
        
        # Produce (confluent-kafka is async by default)
        self._producer.produce(**kwargs)
        
        # Trigger delivery callbacks (non-blocking)
        self._producer.poll(0)
        
        return None  # confluent-kafka doesn't return futures
    
    def flush(self, timeout: Optional[float] = None):
        """Flush pending messages."""
        if timeout is not None:
            self._producer.flush(timeout=timeout)
        else:
            self._producer.flush()
    
    def close(self, timeout: Optional[float] = None):
        """Close the producer."""
        self.flush(timeout=timeout)


class ConfluentKafkaConsumerWrapper(AbstractKafkaConsumerWrapper):
    """
    Wrapper for confluent-kafka Consumer.
    
    confluent-kafka uses librdkafka (C library) and provides significantly
    higher throughput than kafka-python (~10x faster).
    """
    
    def __init__(self, topics: List[str], bootstrap_servers: List[str],
                 group_id: str, config: Dict[str, Any] = None):
        if not CONFLUENT_KAFKA_AVAILABLE:
            raise ImportError("confluent-kafka library not installed. Install with: pip install confluent-kafka")
        
        config = config or {}
        
        # Build confluent-kafka configuration
        consumer_config = {
            'bootstrap.servers': ','.join(bootstrap_servers),
            'group.id': group_id,
            'auto.offset.reset': config.get('auto_offset_reset', 'earliest'),
            'enable.auto.commit': config.get('enable_auto_commit', True),
        }
        
        # Map common config options
        confluent_config = config.get('confluent_config', {})
        consumer_config.update(confluent_config)
        
        # Performance optimizations
        consumer_config.setdefault('fetch.min.bytes', 1)
        consumer_config.setdefault('fetch.wait.max.ms', 100)
        
        self._consumer = ConfluentConsumer(consumer_config)
        self._consumer.subscribe(topics)
        self._topics = topics
        self._value_deserializer = config.get('value_deserializer',
                                              lambda m: json.loads(m.decode('utf-8')))
        self._running = True
        
        logger.info(f"confluent-kafka consumer initialized: {topics}")
    
    def poll(self, timeout_ms: int = 0) -> Optional[Dict[str, Any]]:
        """Poll for messages."""
        timeout_sec = timeout_ms / 1000.0 if timeout_ms > 0 else 0.1
        msg = self._consumer.poll(timeout=timeout_sec)
        
        if msg is None:
            return None
        
        if msg.error():
            error = msg.error()
            if error.code() == ConfluentKafkaError._PARTITION_EOF:
                return None  # End of partition, not an error
            else:
                logger.error(f"Consumer error: {error}")
                return None
        
        # Return deserialized value
        try:
            return {
                'value': self._value_deserializer(msg.value()),
                'key': msg.key().decode('utf-8') if msg.key() else None,
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'timestamp': msg.timestamp()
            }
        except Exception as e:
            logger.error(f"Error deserializing message: {e}")
            return None
    
    def subscribe(self, topics: List[str]):
        """Subscribe to topics."""
        self._consumer.subscribe(topics)
        self._topics = topics
    
    def close(self):
        """Close the consumer."""
        self._running = False
        self._consumer.close()
    
    def __iter__(self):
        """Iterator interface."""
        while self._running:
            result = self.poll(timeout_ms=100)
            if result is not None:
                yield result
    
    def get_single_message(self) -> Optional[Any]:
        """Get a single message (for compatibility)."""
        result = self.poll(timeout_ms=100)
        if result:
            return result.get('value')
        return None


# =============================================================================
# Factory Functions
# =============================================================================

def create_kafka_producer(bootstrap_servers: List[str], config: Dict[str, Any] = None) -> AbstractKafkaProducerWrapper:
    """
    Factory function to create Kafka producer based on configuration.
    
    Args:
        bootstrap_servers: List of Kafka broker addresses
        config: Configuration dictionary with optional 'kafka_library' key
        
    Returns:
        Appropriate Kafka producer wrapper
        
    Configuration Options:
        kafka_library: 'kafka-python' | 'confluent-kafka' (default: 'kafka-python')
    """
    config = config or {}
    library = config.get('kafka_library', 'kafka-python')
    
    if library == 'confluent-kafka':
        if not CONFLUENT_KAFKA_AVAILABLE:
            logger.warning("confluent-kafka not available, falling back to kafka-python")
            library = 'kafka-python'
        else:
            return ConfluentKafkaProducerWrapper(bootstrap_servers, config)
    
    if library == 'kafka-python':
        if not KAFKA_PYTHON_AVAILABLE:
            raise ImportError("No Kafka library available. Install kafka-python or confluent-kafka")
        return KafkaPythonProducerWrapper(bootstrap_servers, config)
    
    raise ValueError(f"Unknown kafka_library: {library}. Use 'kafka-python' or 'confluent-kafka'")


def create_kafka_consumer(topics: List[str], bootstrap_servers: List[str],
                         group_id: str, config: Dict[str, Any] = None) -> AbstractKafkaConsumerWrapper:
    """
    Factory function to create Kafka consumer based on configuration.
    
    Args:
        topics: List of topics to subscribe to
        bootstrap_servers: List of Kafka broker addresses
        group_id: Consumer group ID
        config: Configuration dictionary with optional 'kafka_library' key
        
    Returns:
        Appropriate Kafka consumer wrapper
        
    Configuration Options:
        kafka_library: 'kafka-python' | 'confluent-kafka' (default: 'kafka-python')
    """
    config = config or {}
    library = config.get('kafka_library', 'kafka-python')
    
    if library == 'confluent-kafka':
        if not CONFLUENT_KAFKA_AVAILABLE:
            logger.warning("confluent-kafka not available, falling back to kafka-python")
            library = 'kafka-python'
        else:
            return ConfluentKafkaConsumerWrapper(topics, bootstrap_servers, group_id, config)
    
    if library == 'kafka-python':
        if not KAFKA_PYTHON_AVAILABLE:
            raise ImportError("No Kafka library available. Install kafka-python or confluent-kafka")
        return KafkaPythonConsumerWrapper(topics, bootstrap_servers, group_id, config)
    
    raise ValueError(f"Unknown kafka_library: {library}. Use 'kafka-python' or 'confluent-kafka'")


# =============================================================================
# DataPublisher and DataSubscriber Implementations
# =============================================================================

class KafkaDataPublisher(DataPublisher):
    """
    Publisher for Kafka topics with dual library support.
    
    Supports both kafka-python and confluent-kafka libraries.
    Configure via 'kafka_library' option in config.
    
    Performance:
        - kafka-python: ~50K messages/second
        - confluent-kafka: ~500K messages/second (10x faster)
    
    Example Configuration:
        {
            'bootstrap_servers': ['localhost:9092'],
            'kafka_library': 'confluent-kafka',  # or 'kafka-python'
            'producer_config': {...},  # kafka-python specific
            'confluent_config': {...}  # confluent-kafka specific
        }
    """

    def __init__(self, name: str, destination: str, config: Dict[str, Any]):
        super().__init__(name, destination, config)
        self.topic = destination.split('/')[-1]
        self.bootstrap_servers = config.get('bootstrap_servers', ['localhost:9092'])
        self.kafka_library = config.get('kafka_library', 'kafka-python')
        
        # Create producer using factory
        self.producer = create_kafka_producer(self.bootstrap_servers, config)
        
        logger.info(f"Kafka publisher created for topic {self.topic} using {self.kafka_library}")

    def _do_publish(self, data: Any):
        """Publish to Kafka topic."""
        local_topic = self.topic
        local_data = data
        
        # Handle DataAwarePayload
        if isinstance(data, DataAwarePayload):
            local_topic, local_data = data.get_data_for_publication()
            if local_topic is None or len(local_topic) == 0:
                local_topic = self.destination

        # Send using wrapper
        self.producer.send(local_topic, value=local_data)
        self.producer.flush()

        with self._lock:
            self._last_publish = datetime.now().isoformat()
            self._publish_count += 1

        logger.debug(f"Published to Kafka topic {self.name}/{local_topic}")

    def stop(self):
        """Stop the publisher."""
        super().stop()
        if self.producer:
            self.producer.close()


class KafkaDataSubscriber(DataSubscriber):
    """
    Subscriber for Kafka topics with dual library support.
    
    Supports both kafka-python and confluent-kafka libraries.
    Configure via 'kafka_library' option in config.
    
    Performance:
        - kafka-python: ~50K messages/second
        - confluent-kafka: ~500K messages/second (10x faster)
    
    Example Configuration:
        {
            'bootstrap_servers': ['localhost:9092'],
            'group_id': 'my_consumer_group',
            'kafka_library': 'confluent-kafka',  # or 'kafka-python'
            'consumer_config': {...},  # kafka-python specific
            'confluent_config': {...}  # confluent-kafka specific
        }
    """

    def __init__(self, name: str, source: str, config: Dict[str, Any],
                 given_queue: queue.Queue = None):
        super().__init__(name, source, config, given_queue)
        self.topic = source.split('/')[-1]
        self.bootstrap_servers = config.get('bootstrap_servers', ['localhost:9092'])
        self.group_id = config.get('group_id', f'{name}_group')
        self.kafka_library = config.get('kafka_library', 'kafka-python')
        
        # Create consumer using factory
        self.consumer = create_kafka_consumer(
            [self.topic],
            self.bootstrap_servers,
            self.group_id,
            config
        )
        
        logger.info(f"Kafka subscriber created for topic {self.topic} using {self.kafka_library}")

    def _do_subscribe(self) -> Optional[Any]:
        """Subscribe from Kafka topic."""
        return self.consumer.get_single_message()

    def stop(self):
        """Stop the subscriber."""
        super().stop()
        if self.consumer:
            self.consumer.close()


# =============================================================================
# Utility Functions
# =============================================================================

def get_library_info() -> Dict[str, Any]:
    """
    Get information about available Kafka libraries.
    
    Returns:
        Dictionary with library availability and version info
    """
    info = {
        'kafka_python': {
            'available': KAFKA_PYTHON_AVAILABLE,
            'version': None,
            'performance': '~50K msg/sec'
        },
        'confluent_kafka': {
            'available': CONFLUENT_KAFKA_AVAILABLE,
            'version': None,
            'performance': '~500K msg/sec (recommended)'
        },
        'recommended': 'confluent-kafka' if CONFLUENT_KAFKA_AVAILABLE else 'kafka-python'
    }
    
    if KAFKA_PYTHON_AVAILABLE:
        try:
            import kafka
            info['kafka_python']['version'] = kafka.__version__
        except:
            pass
    
    if CONFLUENT_KAFKA_AVAILABLE:
        try:
            import confluent_kafka
            info['confluent_kafka']['version'] = confluent_kafka.version()[0]
        except:
            pass
    
    return info


# =============================================================================
# Backward Compatibility
# =============================================================================

# Legacy class names for backward compatibility
KafkaPublisher = KafkaDataPublisher
KafkaSubscriber = KafkaDataSubscriber


# =============================================================================
# Example Usage
# =============================================================================

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    print("=" * 60)
    print("Kafka Library Information")
    print("=" * 60)
    
    info = get_library_info()
    print(f"\nkafka-python: {'Available' if info['kafka_python']['available'] else 'Not installed'}")
    if info['kafka_python']['version']:
        print(f"  Version: {info['kafka_python']['version']}")
    print(f"  Performance: {info['kafka_python']['performance']}")
    
    print(f"\nconfluent-kafka: {'Available' if info['confluent_kafka']['available'] else 'Not installed'}")
    if info['confluent_kafka']['version']:
        print(f"  Version: {info['confluent_kafka']['version']}")
    print(f"  Performance: {info['confluent_kafka']['performance']}")
    
    print(f"\nRecommended: {info['recommended']}")
