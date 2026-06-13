"""
confluent-kafka Wrappers (v2.0.0 module split)
==============================================

Producer/consumer wrappers built on the C-based confluent-kafka library
(librdkafka, ~500K msg/sec; production-grade), extracted verbatim from
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
    CONFLUENT_KAFKA_AVAILABLE,
)

if CONFLUENT_KAFKA_AVAILABLE:
    from confluent_kafka import Producer as ConfluentProducer
    from confluent_kafka import Consumer as ConfluentConsumer
    from confluent_kafka import KafkaError as ConfluentKafkaError
    from confluent_kafka import KafkaException as ConfluentKafkaException

logger = logging.getLogger(__name__)


class ConfluentKafkaProducerWrapper(AbstractKafkaProducerWrapper):
    """
    Wrapper for confluent-kafka Producer with retry and recovery support.
    
    confluent-kafka uses librdkafka (C library) and provides significantly
    higher throughput than kafka-python (~10x faster).
    """
    
    def __init__(self, bootstrap_servers: List[str], config: Dict[str, Any] = None):
        if not CONFLUENT_KAFKA_AVAILABLE:
            raise ImportError("confluent-kafka library not installed. Install with: pip install confluent-kafka")
        
        config = config or {}
        self._config = config
        self._bootstrap_servers = bootstrap_servers
        
        # v1.7.6: Connection retry configuration
        self._max_retries = config.get('max_retries', 5)
        self._retry_delay = config.get('retry_delay', 3)
        self._auto_reconnect = config.get('auto_reconnect', True)
        
        # Build confluent-kafka configuration
        self._producer_config = {
            'bootstrap.servers': ','.join(bootstrap_servers),
        }
        
        # Map common config options
        confluent_config = config.get('confluent_config', {})
        self._producer_config.update(confluent_config)
        
        # Performance optimizations (can be overridden by config)
        self._producer_config.setdefault('queue.buffering.max.messages', 100000)
        self._producer_config.setdefault('queue.buffering.max.ms', 5)
        self._producer_config.setdefault('batch.num.messages', 10000)
        
        self._value_serializer = config.get('value_serializer', 
                                            lambda v: json.dumps(v).encode('utf-8'))
        self._key_serializer = config.get('key_serializer',
                                          lambda k: k.encode('utf-8') if isinstance(k, str) else k)
        
        self._producer = None
        self._connected = False
        
        # v1.7.6: Connect with retry logic
        self._connect_with_retry()
    
    def _connect_with_retry(self):
        """v1.7.6: Establish confluent-kafka producer connection with retry logic."""
        last_error = None
        
        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(f"Confluent-kafka producer connection attempt {attempt}/{self._max_retries}")
                
                self._producer = ConfluentProducer(self._producer_config)
                
                self._connected = True
                logger.info(f"confluent-kafka producer connected successfully (attempt={attempt})")
                return
                
            except Exception as e:
                last_error = e
                logger.warning(f"Confluent-kafka producer connection attempt {attempt}/{self._max_retries} failed: {e}")
                
                if attempt < self._max_retries:
                    logger.info(f"Retrying in {self._retry_delay} seconds...")
                    import time
                    time.sleep(self._retry_delay)
        
        logger.error(f"Failed to connect confluent-kafka producer after {self._max_retries} attempts: {last_error}")
        raise ConnectionError(f"Could not connect to Kafka: {last_error}")
    
    def _reconnect_if_needed(self):
        """v1.7.6: Reconnect producer if connection is broken."""
        if not self._auto_reconnect or self._connected:
            return
        
        logger.info("Attempting to reconnect confluent-kafka producer...")
        try:
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"Producer reconnection failed: {e}")
            raise
    
    def send(self, topic: str, value: Any, key: Optional[Any] = None,
             headers: Optional[List] = None, partition: Optional[int] = None) -> Any:
        """Send message using confluent-kafka with automatic reconnection."""
        try:
            self._reconnect_if_needed()
            
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
            
        except Exception as e:
            logger.error(f"Error sending to confluent-kafka: {e}")
            self._connected = False
            if self._auto_reconnect:
                self._reconnect_if_needed()
                self._producer.produce(**kwargs)
                self._producer.poll(0)
            else:
                raise
    
    def flush(self, timeout: Optional[float] = None):
        """Flush pending messages."""
        if self._producer:
            if timeout is not None:
                self._producer.flush(timeout=timeout)
            else:
                self._producer.flush()
    
    def close(self, timeout: Optional[float] = None):
        """Close the producer."""
        self.flush(timeout=timeout)
        self._connected = False


class ConfluentKafkaConsumerWrapper(AbstractKafkaConsumerWrapper):
    """
    Wrapper for confluent-kafka Consumer with retry and recovery support.
    
    confluent-kafka uses librdkafka (C library) and provides significantly
    higher throughput than kafka-python (~10x faster).
    """
    
    def __init__(self, topics: List[str], bootstrap_servers: List[str],
                 group_id: str, config: Dict[str, Any] = None):
        if not CONFLUENT_KAFKA_AVAILABLE:
            raise ImportError("confluent-kafka library not installed. Install with: pip install confluent-kafka")
        
        config = config or {}
        self._config = config
        self._topics = topics
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        
        # v1.7.6: Connection retry configuration
        self._max_retries = config.get('max_retries', 5)
        self._retry_delay = config.get('retry_delay', 3)
        self._auto_reconnect = config.get('auto_reconnect', True)
        
        # Build confluent-kafka configuration
        self._consumer_config = {
            'bootstrap.servers': ','.join(bootstrap_servers),
            'group.id': group_id,
            'auto.offset.reset': config.get('auto_offset_reset', 'earliest'),
            'enable.auto.commit': config.get('enable_auto_commit', True),
        }
        
        # Map common config options
        confluent_config = config.get('confluent_config', {})
        self._consumer_config.update(confluent_config)
        
        # Performance optimizations
        self._consumer_config.setdefault('fetch.min.bytes', 1)
        self._consumer_config.setdefault('fetch.wait.max.ms', 100)
        
        # v1.7.2: Use smart deserializer that auto-packages non-JSON messages
        self._value_deserializer = config.get('value_deserializer', self._smart_deserializer)
        self._running = True
        self._consumer = None
        self._connected = False
        
        # v1.7.6: Connect with retry logic
        self._connect_with_retry()
    
    def _connect_with_retry(self):
        """v1.7.6: Establish confluent-kafka connection with retry logic."""
        last_error = None
        
        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(f"Confluent-kafka consumer connection attempt {attempt}/{self._max_retries}")
                
                self._consumer = ConfluentConsumer(self._consumer_config)
                self._consumer.subscribe(self._topics)
                
                self._connected = True
                logger.info(f"confluent-kafka consumer connected successfully: {self._topics} (attempt={attempt})")
                return
                
            except Exception as e:
                last_error = e
                logger.warning(f"Confluent-kafka connection attempt {attempt}/{self._max_retries} failed: {e}")
                
                if attempt < self._max_retries:
                    logger.info(f"Retrying in {self._retry_delay} seconds...")
                    import time
                    time.sleep(self._retry_delay)
        
        logger.error(f"Failed to connect confluent-kafka after {self._max_retries} attempts: {last_error}")
        raise ConnectionError(f"Could not connect to Kafka: {last_error}")
    
    def _reconnect_if_needed(self):
        """v1.7.6: Reconnect consumer if connection is broken."""
        if not self._auto_reconnect or self._connected:
            return
        
        logger.info("Attempting to reconnect confluent-kafka consumer...")
        try:
            if self._consumer:
                try:
                    self._consumer.close()
                except:
                    pass
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"Consumer reconnection failed: {e}")
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
        """Poll for messages with automatic reconnection."""
        try:
            # v1.7.6: Check and reconnect if needed
            self._reconnect_if_needed()
            
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
                    # v1.7.6: Mark as disconnected for certain errors
                    if 'broker' in str(error).lower() or 'connection' in str(error).lower():
                        self._connected = False
                    return None
            
            # Return deserialized value using smart deserializer
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
                logger.error(f"Raw message value: {msg.value()[:200] if msg.value() else 'None'}")
                logger.error(f"Full stack trace:\n{traceback.format_exc()}")
                # v1.7.2: Return packaged error instead of None
                return {
                    'value': {
                        "_raw_data": msg.value().hex() if msg.value() else None,
                        "_raw_type": "bytes",
                        "_auto_packaged": True,
                        "_error": str(e)
                    },
                    'key': None,
                    'topic': msg.topic(),
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'timestamp': msg.timestamp()
                }
                
        except Exception as e:
            logger.error(f"Error polling confluent-kafka: {e}")
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            self._connected = False
            
            if self._auto_reconnect:
                try:
                    self._reconnect_if_needed()
                except Exception as reconnect_error:
                    logger.error(f"Reconnection failed: {reconnect_error}")
            return None
    
    def subscribe(self, topics: List[str]):
        """Subscribe to topics."""
        self._consumer.subscribe(topics)
        self._topics = topics
    
    def close(self):
        """Close the consumer."""
        self._running = False
        if self._consumer:
            self._consumer.close()
        self._connected = False
    
    def __iter__(self):
        """Iterator interface."""
        while self._running:
            result = self.poll(timeout_ms=100)
            if result is not None:
                yield result
    
    def get_single_message(self) -> Optional[Any]:
        """Get a single message with automatic reconnection."""
        try:
            # v1.7.6: Check and reconnect if needed
            self._reconnect_if_needed()
            
            result = self.poll(timeout_ms=100)
            if result:
                return result.get('value')
            return None
            
        except Exception as e:
            logger.error(f"Error getting message from confluent-kafka: {e}")
            self._connected = False
            
            if self._auto_reconnect:
                try:
                    self._reconnect_if_needed()
                except Exception as reconnect_error:
                    logger.error(f"Reconnection failed: {reconnect_error}")
            return None


# =============================================================================
# Factory Functions
# =============================================================================

