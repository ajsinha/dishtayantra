"""
kafka-python Wrappers (v2.0.0 module split)
===========================================

Producer/consumer wrappers built on the pure-Python kafka-python library
(~50K msg/sec; development-friendly), extracted verbatim from
kafka_datapubsub.py. Re-exported from core.pubsub.kafka_datapubsub.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import traceback
from datetime import datetime
from typing import Any, Dict, Optional, Callable, List

from core.pubsub.kafka_base import (
    AbstractKafkaConsumerWrapper,
    AbstractKafkaProducerWrapper,
    KAFKA_PYTHON_AVAILABLE,
)

if KAFKA_PYTHON_AVAILABLE:
    from kafka import KafkaProducer as KafkaPythonProducer
    from kafka import KafkaConsumer as KafkaPythonConsumer
    from kafka.errors import KafkaError as KafkaPythonError

logger = logging.getLogger(__name__)


class KafkaPythonProducerWrapper(AbstractKafkaProducerWrapper):
    """Wrapper for kafka-python Producer with retry and recovery support."""
    
    def __init__(self, bootstrap_servers: List[str], config: Dict[str, Any] = None):
        if not KAFKA_PYTHON_AVAILABLE:
            raise ImportError("kafka-python library not installed. Install with: pip install kafka-python")
        
        config = config or {}
        self._config = config
        self._bootstrap_servers = bootstrap_servers
        
        # v1.7.6: Connection retry configuration
        self._max_retries = config.get('max_retries', 5)
        self._retry_delay = config.get('retry_delay', 3)
        self._auto_reconnect = config.get('auto_reconnect', True)
        
        producer_config = config.get('producer_config', {})
        
        # Default serializer
        if 'value_serializer' not in producer_config:
            producer_config['value_serializer'] = lambda v: json.dumps(v).encode('utf-8')
        
        self._producer_config = producer_config
        self._producer = None
        self._connected = False
        
        # v1.7.6: Connect with retry logic
        self._connect_with_retry()
    
    def _connect_with_retry(self):
        """v1.7.6: Establish Kafka producer connection with retry logic."""
        last_error = None
        
        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(f"Kafka producer connection attempt {attempt}/{self._max_retries} to {self._bootstrap_servers}")
                
                self._producer = KafkaPythonProducer(
                    bootstrap_servers=self._bootstrap_servers,
                    **self._producer_config
                )
                
                self._connected = True
                logger.info(f"kafka-python producer connected successfully (attempt={attempt})")
                return
                
            except Exception as e:
                last_error = e
                logger.warning(f"Kafka producer connection attempt {attempt}/{self._max_retries} failed: {e}")
                
                if attempt < self._max_retries:
                    logger.info(f"Retrying in {self._retry_delay} seconds...")
                    import time
                    time.sleep(self._retry_delay)
        
        logger.error(f"Failed to connect Kafka producer after {self._max_retries} attempts: {last_error}")
        raise ConnectionError(f"Could not connect to Kafka brokers {self._bootstrap_servers}: {last_error}")
    
    def _reconnect_if_needed(self):
        """v1.7.6: Reconnect producer if connection is broken."""
        if not self._auto_reconnect or self._connected:
            return
        
        logger.info("Attempting to reconnect Kafka producer...")
        try:
            if self._producer:
                try:
                    self._producer.close(timeout=1)
                except:
                    pass
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"Producer reconnection failed: {e}")
            raise
    
    def send(self, topic: str, value: Any, key: Optional[Any] = None,
             headers: Optional[List] = None, partition: Optional[int] = None) -> Any:
        """Send message using kafka-python with automatic reconnection."""
        try:
            self._reconnect_if_needed()
            
            kwargs = {'topic': topic, 'value': value}
            if key is not None:
                kwargs['key'] = key
            if headers is not None:
                kwargs['headers'] = headers
            if partition is not None:
                kwargs['partition'] = partition
            
            future = self._producer.send(**kwargs)
            return future
            
        except Exception as e:
            logger.error(f"Error sending to Kafka: {e}")
            self._connected = False
            if self._auto_reconnect:
                self._reconnect_if_needed()
                return self._producer.send(**kwargs)
            raise
    
    def flush(self, timeout: Optional[float] = None):
        """Flush pending messages."""
        if self._producer:
            self._producer.flush(timeout=timeout)
    
    def close(self, timeout: Optional[float] = None):
        """Close the producer."""
        if self._producer:
            self._producer.close(timeout=timeout)
        self._connected = False


class KafkaPythonConsumerWrapper(AbstractKafkaConsumerWrapper):
    """Wrapper for kafka-python Consumer with retry and recovery support."""
    
    def __init__(self, topics: List[str], bootstrap_servers: List[str],
                 group_id: str, config: Dict[str, Any] = None):
        if not KAFKA_PYTHON_AVAILABLE:
            raise ImportError("kafka-python library not installed. Install with: pip install kafka-python")
        
        config = config or {}
        self._config = config
        self._topics = topics
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        
        # v1.7.6: Connection retry configuration
        self._max_retries = config.get('max_retries', 5)
        self._retry_delay = config.get('retry_delay', 3)
        self._auto_reconnect = config.get('auto_reconnect', True)
        
        consumer_config = config.get('consumer_config', {})
        
        # v1.7.2: Smart deserializer that auto-packages non-JSON messages
        if 'value_deserializer' not in consumer_config:
            consumer_config['value_deserializer'] = self._smart_deserializer
        
        # Set consumer timeout for non-blocking iteration
        if 'consumer_timeout_ms' not in consumer_config:
            consumer_config['consumer_timeout_ms'] = 100
        
        # v1.7.2: Set auto.offset.reset to 'earliest' by default
        if 'auto_offset_reset' not in consumer_config:
            consumer_config['auto_offset_reset'] = 'earliest'
        
        self._consumer_config = consumer_config
        self._poll_timeout_ms = consumer_config.get('consumer_timeout_ms', 100)
        self._consumer = None
        self._connected = False
        
        # v1.7.6: Connect with retry logic
        self._connect_with_retry()
    
    def _connect_with_retry(self):
        """
        v1.7.6: Establish Kafka connection with retry logic.
        
        Attempts to connect multiple times before giving up, with configurable
        delay between attempts. This handles transient Kafka broker unavailability
        during startup.
        """
        last_error = None
        
        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(f"Kafka consumer connection attempt {attempt}/{self._max_retries} to {self._bootstrap_servers}")
                
                self._consumer = KafkaPythonConsumer(
                    *self._topics,
                    bootstrap_servers=self._bootstrap_servers,
                    group_id=self._group_id,
                    **self._consumer_config
                )
                
                self._connected = True
                logger.info(f"kafka-python consumer connected successfully: {self._topics} "
                           f"(group_id={self._group_id}, attempt={attempt})")
                return
                
            except Exception as e:
                last_error = e
                logger.warning(f"Kafka consumer connection attempt {attempt}/{self._max_retries} failed: {e}")
                
                if attempt < self._max_retries:
                    logger.info(f"Retrying in {self._retry_delay} seconds...")
                    import time
                    time.sleep(self._retry_delay)
        
        # All retries exhausted
        logger.error(f"Failed to connect to Kafka after {self._max_retries} attempts. Last error: {last_error}")
        logger.error(f"Full stack trace:\n{traceback.format_exc()}")
        raise ConnectionError(f"Could not connect to Kafka brokers {self._bootstrap_servers} after {self._max_retries} attempts: {last_error}")
    
    def _reconnect_if_needed(self):
        """
        v1.7.6: Check connection health and reconnect if broken.
        
        Called before each poll operation to ensure connection is healthy.
        """
        if not self._auto_reconnect:
            return
        
        try:
            # Check if consumer is still connected by accessing partitions
            if self._consumer:
                self._consumer.partitions_for_topic(self._topics[0] if self._topics else '')
                return  # Connection is healthy
        except Exception as e:
            logger.warning(f"Kafka connection appears broken: {e}")
            self._connected = False
        
        if not self._connected:
            logger.info("Attempting to reconnect to Kafka...")
            try:
                if self._consumer:
                    try:
                        self._consumer.close()
                    except:
                        pass
                self._connect_with_retry()
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")
                raise
    
    @staticmethod
    def _smart_deserializer(raw_bytes: bytes) -> Any:
        """
        v1.7.2: Smart deserializer that handles both JSON and non-JSON messages.
        
        - If message is valid JSON dict/list/value → returns parsed JSON
        - If message is not valid JSON → auto-packages into dict format
        
        This ensures downstream DAG components always receive a dict,
        mimicking the behavior of auto_package_non_dict=true.
        """
        if raw_bytes is None:
            return {"_raw_data": None, "_raw_type": "null", "_auto_packaged": True}
        
        try:
            # Decode bytes to string
            decoded = raw_bytes.decode('utf-8')
            
            # Try to parse as JSON
            parsed = json.loads(decoded)
            
            # If it's already a dict, return as-is
            if isinstance(parsed, dict):
                return parsed
            
            # If it's a list or primitive, wrap it
            return {
                "_raw_data": parsed,
                "_raw_type": type(parsed).__name__,
                "_auto_packaged": True
            }
            
        except json.JSONDecodeError:
            # Not valid JSON - wrap raw string in dict
            decoded = raw_bytes.decode('utf-8', errors='replace')
            logger.info(f"Auto-packaging non-JSON message: {decoded[:100]}...")
            return {
                "_raw_data": decoded,
                "_raw_type": "string",
                "_auto_packaged": True,
                "_original_format": "plain_text"
            }
        except Exception as e:
            # Fallback for any other errors
            logger.warning(f"Error deserializing message, packaging as bytes: {e}")
            return {
                "_raw_data": raw_bytes.hex() if raw_bytes else None,
                "_raw_type": "bytes",
                "_auto_packaged": True,
                "_error": str(e)
            }
    
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
        if self._consumer:
            self._consumer.close()
        self._connected = False
    
    def __iter__(self):
        """Iterator interface."""
        return iter(self._consumer)
    
    def poll(self, timeout_ms: int = 0) -> Optional[Dict[str, Any]]:
        """
        Poll for messages with automatic reconnection on failure.
        
        v1.7.6: Added reconnection support for broken connections.
        """
        try:
            # v1.7.6: Check and reconnect if needed
            self._reconnect_if_needed()
            records = self._consumer.poll(timeout_ms=timeout_ms)
            return records
        except Exception as e:
            logger.error(f"Error polling Kafka: {e}")
            self._connected = False
            if self._auto_reconnect:
                try:
                    self._reconnect_if_needed()
                    return self._consumer.poll(timeout_ms=timeout_ms)
                except Exception as retry_error:
                    logger.error(f"Retry poll failed: {retry_error}")
            return None
    
    def get_single_message(self) -> Optional[Any]:
        """
        Get a single message using poll() with automatic reconnection.
        
        v1.7.6: Added reconnection support for broken connections.
        v1.7.2: Changed from iterator-based to poll-based retrieval.
        
        Note: Message logging is handled by base DataSubscriber class.
        """
        try:
            # v1.7.6: Check and reconnect if needed
            self._reconnect_if_needed()
            
            # Use poll() instead of iterator for more reliable message retrieval
            records = self._consumer.poll(timeout_ms=self._poll_timeout_ms)
            
            if records:
                # records is a dict: {TopicPartition: [ConsumerRecord, ...]}
                for topic_partition, messages in records.items():
                    if messages:
                        # Return the first message value
                        msg = messages[0]
                        # Log Kafka-specific details (topic/partition/offset)
                        logger.debug(f"Kafka poll returned message from {topic_partition.topic} "
                                    f"partition {topic_partition.partition} offset {msg.offset}")
                        return msg.value
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting message from Kafka: {e}")
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            self._connected = False
            
            if self._auto_reconnect:
                logger.info("Attempting reconnection after message retrieval failure...")
                try:
                    self._reconnect_if_needed()
                except Exception as reconnect_error:
                    logger.error(f"Reconnection attempt failed: {reconnect_error}")
            
            return None


# =============================================================================
# confluent-kafka Wrappers
# =============================================================================

