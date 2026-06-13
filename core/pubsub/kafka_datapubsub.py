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
import traceback
from datetime import datetime
from typing import Any, Dict, Optional, Callable, List
from abc import ABC, abstractmethod

from core.pubsub.datapubsub import DataPublisher, DataSubscriber, DataAwarePayload

logger = logging.getLogger(__name__)

# =============================================================================
# v2.0.0 module split: library detection + abstract wrappers live in
# kafka_base.py; the per-library implementations in
# kafka_python_wrappers.py / kafka_confluent_wrappers.py. Everything is
# re-exported here so existing imports keep working unchanged.
# =============================================================================
from core.pubsub.kafka_base import (  # noqa: F401
    AbstractKafkaConsumerWrapper,
    AbstractKafkaProducerWrapper,
    CONFLUENT_KAFKA_AVAILABLE,
    KAFKA_PYTHON_AVAILABLE,
    get_available_libraries,
)
from core.pubsub.kafka_python_wrappers import (  # noqa: F401
    KafkaPythonConsumerWrapper,
    KafkaPythonProducerWrapper,
)
from core.pubsub.kafka_confluent_wrappers import (  # noqa: F401
    ConfluentKafkaConsumerWrapper,
    ConfluentKafkaProducerWrapper,
)


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
