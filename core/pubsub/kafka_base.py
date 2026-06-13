"""
Kafka Integration Base (v2.0.0 module split)
============================================

Library detection, availability flags, and the abstract producer/consumer
wrapper contracts shared by the kafka-python and confluent-kafka
implementations. Extracted verbatim from kafka_datapubsub.py so each
module stays within the 500-line architecture limit; all names are
re-exported from core.pubsub.kafka_datapubsub.

PATENT PENDING: The Multi-Broker Message Routing Architecture is subject to
pending patent applications.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
DishtaYantra(TM) is a trademark of Ashutosh Sinha.
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

    @abstractmethod
    def get_single_message(self) -> Optional[Any]:
        """Get a single message (for compatibility)."""
        pass

# =============================================================================
# kafka-python Wrappers
# =============================================================================

