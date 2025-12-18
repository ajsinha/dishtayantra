# Kafka DataPublisher and DataSubscriber Setup Guide

## © 2025-2030 Ashutosh Sinha

**Version 1.1.2** | **Patent Pending**

---

## Overview

The Kafka implementation provides publisher and subscriber classes for Apache Kafka with support for **two Python client libraries**:

| Library | Performance | Use Case |
|---------|-------------|----------|
| **kafka-python** | ~50K msg/sec | Development, simpler setup |
| **confluent-kafka** | ~500K msg/sec | Production, high performance |

The implementation uses a **factory pattern** allowing you to switch between libraries via configuration without changing any application code.

---

## What's New in v1.1.2

### Dual Library Support (Patent Pending)

- **Seamless switching** between kafka-python and confluent-kafka
- **Zero code changes** required when switching libraries
- **Automatic fallback** if preferred library unavailable
- **Unified API** across both libraries
- **Resilient versions** for both libraries

### Performance Improvements

| Metric | kafka-python | confluent-kafka | Improvement |
|--------|--------------|-----------------|-------------|
| Throughput | ~50K msg/sec | ~500K msg/sec | **10x** |
| Latency (p99) | ~5ms | ~0.5ms | **10x** |
| CPU Usage | Higher | Lower | **50% less** |
| Memory | Higher | Lower | **30% less** |

---

## Prerequisites

### Install Apache Kafka

Using Docker (recommended):
```yaml
# docker-compose.yml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

# Start with: docker-compose up -d
```

### Install Python Client Libraries

```bash
# Option 1: kafka-python only (simpler setup)
pip install kafka-python

# Option 2: confluent-kafka only (higher performance)
pip install confluent-kafka

# Option 3: Both libraries (recommended for flexibility)
pip install kafka-python confluent-kafka
```

---

## Quick Start

### Basic Publisher (Default: kafka-python)

```python
from core.pubsub.pubsubfactory import create_publisher

config = {
    'destination': 'kafka://topic/user_events',
    'bootstrap_servers': ['localhost:9092']
}

publisher = create_publisher('event_publisher', config)
publisher.publish({'event': 'user_login', 'user_id': 123})
publisher.stop()
```

### High-Performance Publisher (confluent-kafka)

```python
from core.pubsub.pubsubfactory import create_publisher

config = {
    'destination': 'kafka://topic/user_events',
    'bootstrap_servers': ['localhost:9092'],
    'kafka_library': 'confluent-kafka'  # Switch to high-performance library
}

publisher = create_publisher('event_publisher', config)
publisher.publish({'event': 'user_login', 'user_id': 123})
publisher.stop()
```

### Basic Subscriber

```python
from core.pubsub.pubsubfactory import create_subscriber

config = {
    'source': 'kafka://topic/user_events',
    'bootstrap_servers': ['localhost:9092'],
    'group_id': 'my_consumer_group',
    'kafka_library': 'confluent-kafka'  # Optional: use high-performance library
}

subscriber = create_subscriber('event_subscriber', config)
subscriber.start()

data = subscriber.get_data(block_time=5)
if data:
    print(f"Received: {data}")

subscriber.stop()
```

---

## Library Selection

### Configuration Option

Add `kafka_library` to your configuration to select the library:

```python
config = {
    'destination': 'kafka://topic/my_topic',
    'bootstrap_servers': ['localhost:9092'],
    'kafka_library': 'confluent-kafka'  # or 'kafka-python'
}
```

### application.properties

```properties
# Kafka library selection
kafka.library=confluent-kafka

# Kafka broker configuration
kafka.bootstrap.servers=localhost:9092,localhost:9093

# Producer settings
kafka.producer.acks=all
kafka.producer.retries=3

# Consumer settings
kafka.consumer.group.id=my_app_group
kafka.consumer.auto.offset.reset=earliest
```

### DAG Node Configuration

```json
{
  "name": "kafka_input",
  "type": "DataSubscriberNode",
  "config": {
    "source": "kafka://topic/input_events",
    "bootstrap_servers": ["localhost:9092"],
    "kafka_library": "confluent-kafka",
    "group_id": "dag_processor"
  }
}
```

### Automatic Fallback

If the specified library is not available, the system automatically falls back:

```
confluent-kafka requested → not installed → falls back to kafka-python
kafka-python requested → not installed → raises ImportError
```

---

## Library-Specific Configuration

### kafka-python Configuration

```python
config = {
    'destination': 'kafka://topic/events',
    'bootstrap_servers': ['localhost:9092'],
    'kafka_library': 'kafka-python',
    
    # kafka-python specific producer config
    'producer_config': {
        'acks': 'all',
        'retries': 3,
        'batch_size': 16384,
        'linger_ms': 10,
        'compression_type': 'gzip',
        'max_in_flight_requests_per_connection': 5,
        'buffer_memory': 33554432
    }
}
```

### confluent-kafka Configuration

```python
config = {
    'destination': 'kafka://topic/events',
    'bootstrap_servers': ['localhost:9092'],
    'kafka_library': 'confluent-kafka',
    
    # confluent-kafka specific config (uses librdkafka naming)
    'confluent_config': {
        'acks': 'all',
        'retries': 3,
        'queue.buffering.max.messages': 100000,
        'queue.buffering.max.ms': 5,
        'batch.num.messages': 10000,
        'compression.type': 'gzip',
        'linger.ms': 5
    }
}
```

### Consumer Configuration

#### kafka-python Consumer

```python
config = {
    'source': 'kafka://topic/events',
    'bootstrap_servers': ['localhost:9092'],
    'group_id': 'my_group',
    'kafka_library': 'kafka-python',
    
    'consumer_config': {
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'auto_commit_interval_ms': 5000,
        'max_poll_records': 500,
        'session_timeout_ms': 30000,
        'heartbeat_interval_ms': 10000
    }
}
```

#### confluent-kafka Consumer

```python
config = {
    'source': 'kafka://topic/events',
    'bootstrap_servers': ['localhost:9092'],
    'group_id': 'my_group',
    'kafka_library': 'confluent-kafka',
    
    'confluent_config': {
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000,
        'max.poll.records': 500,
        'session.timeout.ms': 30000,
        'heartbeat.interval.ms': 10000,
        'fetch.min.bytes': 1,
        'fetch.wait.max.ms': 100
    }
}
```

---

## Resilient Kafka

Both libraries support resilient versions with automatic reconnection.

### Using Resilient Producer

```python
from core.pubsub.resilient_kafka import create_resilient_producer

# Create resilient producer with confluent-kafka
producer = create_resilient_producer(
    kafka_library='confluent-kafka',
    bootstrap_servers=['localhost:9092'],
    reconnect_tries=10,
    reconnect_interval_seconds=30,
    buffer_max_messages=10000
)

# Send messages (automatically buffered during disconnection)
producer.send('my-topic', value={'data': 'test'})
producer.flush()
producer.close()
```

### Using Resilient Consumer

```python
from core.pubsub.resilient_kafka import create_resilient_consumer

# Create resilient consumer with confluent-kafka
consumer = create_resilient_consumer(
    'my-topic',
    kafka_library='confluent-kafka',
    bootstrap_servers=['localhost:9092'],
    group_id='my-group',
    reconnect_tries=10,
    reconnect_interval_seconds=30
)

# Poll for messages (automatically reconnects on failure)
for message in consumer:
    print(f"Received: {message.value}")

consumer.close()
```

### Resilient Features

| Feature | Description |
|---------|-------------|
| **Auto-reconnect** | Automatically reconnects on broker failures |
| **Message buffering** | Buffers messages during disconnection |
| **Configurable retries** | Customize retry count and interval |
| **Graceful degradation** | Continues operating during partial failures |
| **Failed message tracking** | Track messages that couldn't be delivered |

---

## Performance Tuning

### High-Throughput Producer (confluent-kafka)

```python
config = {
    'destination': 'kafka://topic/high_volume',
    'bootstrap_servers': ['localhost:9092'],
    'kafka_library': 'confluent-kafka',
    
    'confluent_config': {
        # Batching for throughput
        'queue.buffering.max.messages': 500000,
        'queue.buffering.max.ms': 10,
        'batch.num.messages': 50000,
        
        # Compression
        'compression.type': 'lz4',
        
        # Performance
        'linger.ms': 5,
        'acks': 1  # Trade durability for speed
    }
}
```

### Low-Latency Producer (confluent-kafka)

```python
config = {
    'destination': 'kafka://topic/low_latency',
    'bootstrap_servers': ['localhost:9092'],
    'kafka_library': 'confluent-kafka',
    
    'confluent_config': {
        # Minimal batching for low latency
        'queue.buffering.max.ms': 0,
        'batch.num.messages': 1,
        'linger.ms': 0,
        
        # Immediate delivery
        'acks': 1
    }
}
```

### High-Throughput Consumer (confluent-kafka)

```python
config = {
    'source': 'kafka://topic/high_volume',
    'bootstrap_servers': ['localhost:9092'],
    'group_id': 'fast_consumer',
    'kafka_library': 'confluent-kafka',
    
    'confluent_config': {
        # Fetch more data per request
        'fetch.min.bytes': 100000,
        'fetch.max.bytes': 52428800,
        'max.partition.fetch.bytes': 1048576,
        
        # Reduce wait time
        'fetch.wait.max.ms': 100
    }
}
```

---

## Checking Library Availability

```python
from core.pubsub.kafka_datapubsub import get_library_info, get_available_libraries

# Check available libraries
libraries = get_available_libraries()
print(f"Available: {libraries}")
# Output: ['kafka-python', 'confluent-kafka']

# Get detailed info
info = get_library_info()
print(f"kafka-python: {info['kafka_python']}")
print(f"confluent-kafka: {info['confluent_kafka']}")
print(f"Recommended: {info['recommended']}")
```

---

## Migration Guide

### From kafka-python to confluent-kafka

**Step 1: Install confluent-kafka**
```bash
pip install confluent-kafka
```

**Step 2: Update configuration**
```python
# Before (kafka-python)
config = {
    'destination': 'kafka://topic/events',
    'bootstrap_servers': ['localhost:9092'],
    'producer_config': {
        'acks': 'all',
        'batch_size': 16384
    }
}

# After (confluent-kafka)
config = {
    'destination': 'kafka://topic/events',
    'bootstrap_servers': ['localhost:9092'],
    'kafka_library': 'confluent-kafka',  # Add this line
    'confluent_config': {                 # Rename and adjust config
        'acks': 'all',
        'batch.num.messages': 10000
    }
}
```

**Step 3: No code changes needed!**
```python
# Same code works with both libraries
publisher = create_publisher('event_pub', config)
publisher.publish({'data': 'test'})
publisher.stop()
```

---

## Configuration Reference

### Publisher Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `destination` | string | Required | Kafka topic URL (`kafka://topic/name`) |
| `bootstrap_servers` | list | `['localhost:9092']` | Kafka broker addresses |
| `kafka_library` | string | `'kafka-python'` | Library to use |
| `producer_config` | dict | `{}` | kafka-python specific options |
| `confluent_config` | dict | `{}` | confluent-kafka specific options |

### Subscriber Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source` | string | Required | Kafka topic URL |
| `bootstrap_servers` | list | `['localhost:9092']` | Kafka broker addresses |
| `group_id` | string | `'{name}_group'` | Consumer group ID |
| `kafka_library` | string | `'kafka-python'` | Library to use |
| `consumer_config` | dict | `{}` | kafka-python specific options |
| `confluent_config` | dict | `{}` | confluent-kafka specific options |

### Resilient Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `kafka_library` | string | `'kafka-python'` | Library to use |
| `reconnect_tries` | int | `10` | Max reconnection attempts |
| `reconnect_interval_seconds` | int | `60` | Seconds between retries |
| `buffer_max_messages` | int | `10000` | Max buffered messages (producer) |

---

## Troubleshooting

### Library Not Found

```
ImportError: No Kafka library available. Install kafka-python or confluent-kafka
```

**Solution:**
```bash
pip install kafka-python  # or
pip install confluent-kafka
```

### confluent-kafka Build Issues (Windows)

```
error: Microsoft Visual C++ 14.0 is required
```

**Solution:** Install pre-built wheel:
```bash
pip install confluent-kafka --prefer-binary
```

### Connection Timeout

```
KafkaTimeoutError: Failed to update metadata
```

**Solution:**
1. Verify Kafka broker is running
2. Check `bootstrap_servers` configuration
3. Check network connectivity
4. Increase timeout in config

### Consumer Group Issues

```
CommitFailedError: Coordinator not available
```

**Solution:**
1. Ensure `group_id` is set
2. Check `session_timeout_ms` isn't too short
3. Verify Kafka version compatibility

---

## Best Practices

### 1. Use confluent-kafka in Production

```python
# Production configuration
config = {
    'kafka_library': 'confluent-kafka',
    'bootstrap_servers': ['kafka1:9092', 'kafka2:9092', 'kafka3:9092'],
    'confluent_config': {
        'acks': 'all',
        'retries': 5,
        'compression.type': 'lz4'
    }
}
```

### 2. Use Resilient Classes for Critical Applications

```python
from core.pubsub.resilient_kafka import create_resilient_producer

producer = create_resilient_producer(
    kafka_library='confluent-kafka',
    reconnect_tries=20,
    buffer_max_messages=50000
)
```

### 3. Monitor Library Performance

```python
info = get_library_info()
if info['confluent_kafka']['available']:
    print("Using high-performance confluent-kafka")
else:
    print("Falling back to kafka-python")
```

### 4. Set Appropriate Timeouts

```python
config = {
    'confluent_config': {
        'socket.timeout.ms': 60000,
        'session.timeout.ms': 30000,
        'request.timeout.ms': 30000
    }
}
```

---

## Complete Example: High-Performance Pipeline

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
from core.pubsub.kafka_datapubsub import get_library_info
import threading
import time

# Check library availability
info = get_library_info()
library = 'confluent-kafka' if info['confluent_kafka']['available'] else 'kafka-python'
print(f"Using library: {library} ({info[library.replace('-', '_')]['performance']})")

# High-performance producer
producer_config = {
    'destination': 'kafka://topic/high_perf',
    'bootstrap_servers': ['localhost:9092'],
    'kafka_library': library,
    'confluent_config': {
        'queue.buffering.max.messages': 100000,
        'batch.num.messages': 10000,
        'compression.type': 'lz4'
    }
}

# High-performance consumer
consumer_config = {
    'source': 'kafka://topic/high_perf',
    'bootstrap_servers': ['localhost:9092'],
    'group_id': 'high_perf_consumer',
    'kafka_library': library,
    'confluent_config': {
        'fetch.min.bytes': 10000,
        'fetch.wait.max.ms': 100
    }
}

publisher = create_publisher('perf_pub', producer_config)
subscriber = create_subscriber('perf_sub', consumer_config)

# Consumer thread
def consumer_thread():
    subscriber.start()
    count = 0
    while count < 10000:
        data = subscriber.get_data(block_time=1)
        if data:
            count += 1
            if count % 1000 == 0:
                print(f"Consumed {count} messages")
    subscriber.stop()

# Start consumer
thread = threading.Thread(target=consumer_thread, daemon=True)
thread.start()

# Produce messages
start = time.time()
for i in range(10000):
    publisher.publish({'id': i, 'data': f'message_{i}'})
    
publisher.stop()
elapsed = time.time() - start
print(f"Produced 10000 messages in {elapsed:.2f}s ({10000/elapsed:.0f} msg/sec)")

thread.join(timeout=30)
```

---

## Legal Information

### Patent Notice

**PATENT PENDING**: The Multi-Broker Message Routing Architecture and dual-library abstraction layer implemented in DishtaYantra are subject to pending patent applications.

### Copyright Notice

Copyright © 2025-2030 Ashutosh Sinha. All rights reserved.

### Trademark Notice

DishtaYantra™ is a trademark of Ashutosh Sinha.

---

**DishtaYantra v1.1.2** | Patent Pending | © 2025-2030 Ashutosh Sinha
