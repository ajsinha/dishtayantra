# Message Broker Connectors Guide

Setup and connector-specific configuration for the message-broker pub/sub backends: **Kafka, RabbitMQ, ActiveMQ, TIBCO EMS, and WebSphere MQ (IBM MQ)**.

> **Common configuration** (URI schemes, shared subscriber/publisher/node
> parameters, application.yaml) is in the **Configuration Reference Guide**. This
> guide covers connector-specific detail. Each connector is a section below.

---

# Kafka DataPublisher and DataSubscriber Setup Guide

## © 2025-2030 Ashutosh Sinha

**Applies to: current release**
---

## Overview

The Kafka implementation provides publisher and subscriber classes for Apache Kafka with support for **two Python client libraries**:

| Library | Performance | Use Case |
|---------|-------------|----------|
| **kafka-python** | ~50K msg/sec | Development, simpler setup |
| **confluent-kafka** | ~500K msg/sec | Production, high performance |

The implementation uses a **factory pattern** allowing you to switch between libraries via configuration without changing any application code.

---

## Dual Kafka Library Support

### Dual Library Support 

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

### Configuration (application.yaml or application.properties)

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

#

## SSL/TLS and SASL Security

Kafka security options are passed straight through to the underlying client,
so any option the library supports works. Nest them under `producer_config`
(publishers) or `consumer_config` (subscribers) for the **kafka-python**
library, or under `confluent_config` for the **confluent-kafka** library.

**kafka-python (underscore keys):**

| Key | Purpose |
| --- | --- |
| `security_protocol` | `SSL` or `SASL_SSL` |
| `ssl_cafile` | CA bundle to verify the broker |
| `ssl_certfile` | Client certificate (mutual TLS) |
| `ssl_keyfile` | Client private key (mutual TLS) |
| `sasl_mechanism` | e.g. `PLAIN`, `SCRAM-SHA-256` (with `SASL_SSL`) |
| `sasl_plain_username` / `sasl_plain_password` | SASL credentials |

```json
{
  "source": "kafka://topic/orders",
  "bootstrap_servers": ["broker1:9093"],
  "consumer_config": {
    "security_protocol": "SASL_SSL",
    "ssl_cafile": "/certs/ca.pem",
    "sasl_mechanism": "SCRAM-SHA-256",
    "sasl_plain_username": "svc_orders",
    "sasl_plain_password": "${KAFKA_PASSWORD}"
  }
}
```

**confluent-kafka (dotted keys):** use `confluent_config` with
`security.protocol`, `ssl.ca.location`, `ssl.certificate.location`,
`ssl.key.location`, `sasl.mechanisms`, `sasl.username`, `sasl.password`.

The standard Kafka TLS port is **9093**. Combine with `"resilient": true` for
reconnection during broker failover.

## Copyright Notice

Copyright © 2025-2030 Ashutosh Sinha. All rights reserved.

### Trademark Notice

DishtaYantra™ is a trademark of Ashutosh Sinha.

---

**DishtaYantra v2.2** | © 2025-2030 Ashutosh Sinha


---

# RabbitMQ DataPublisher and DataSubscriber Setup Guide

## © 2025-2030 Ashutosh Sinha


## Overview

The RabbitMQ implementation provides publisher and subscriber classes that support both **queue** and **topic** messaging patterns, similar to ActiveMQ. The implementation uses RabbitMQ's AMQP protocol through the pika library.

## Files

1. **rabbitmq_datapubsub.py** - Contains `RabbitMQDataPublisher` and `RabbitMQDataSubscriber` classes
2. **Updated pubsubfactory.py** - Factory methods now support `rabbitmq://` destinations/sources

## Prerequisites

Install RabbitMQ server and the Python client library:

```bash
# Install RabbitMQ server (Ubuntu/Debian)
sudo apt-get install rabbitmq-server

# Or using Docker
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Install Python client
pip install pika
```

## Queue vs Topic

RabbitMQ supports two messaging patterns:

### Queue (Point-to-Point)
- Messages sent to a queue are consumed by only one subscriber
- Multiple subscribers compete for messages (load balancing)
- URL format: `rabbitmq://queue/queue_name`

### Topic (Publish-Subscribe)
- Messages are broadcast to all subscribers
- Supports wildcard routing patterns
- URL format: `rabbitmq://topic/topic_name`

## Usage

### Publishing to a Queue

Direct point-to-point messaging:

```python
from core.pubsub.pubsubfactory import create_publisher

config = {
    'destination': 'rabbitmq://queue/my_queue',
    'host': 'localhost',
    'port': 5672,
    'username': 'guest',
    'password': 'guest'
}

publisher = create_publisher('my_publisher', config)

# Publish data
publisher.publish({'event': 'user_login', 'user_id': 123})

# Stop when done
publisher.stop()
```

### Publishing to a Topic

Broadcast messages to all subscribers:

```python
config = {
    'destination': 'rabbitmq://topic/user.login',
    'host': 'localhost',
    'port': 5672,
    'username': 'guest',
    'password': 'guest',
    'exchange': 'amq.topic'  # Optional, defaults to 'amq.topic'
}

publisher = create_publisher('topic_pub', config)

# Publish to topic
publisher.publish({'event': 'user_login', 'user_id': 123})

publisher.stop()
```

### Subscribing from a Queue

Consume messages from a queue:

```python
from core.pubsub.pubsubfactory import create_subscriber

config = {
    'source': 'rabbitmq://queue/my_queue',
    'host': 'localhost',
    'port': 5672,
    'username': 'guest',
    'password': 'guest'
}

subscriber = create_subscriber('my_subscriber', config)
subscriber.start()

# Get data (blocking with timeout)
data = subscriber.get_data(block_time=5)
if data:
    print(f"Received: {data}")

subscriber.stop()
```

### Subscribing from a Topic

Subscribe to topic patterns with wildcards:

```python
config = {
    'source': 'rabbitmq://topic/user.login',
    'host': 'localhost',
    'port': 5672,
    'username': 'guest',
    'password': 'guest',
    'binding_key': 'user.*',  # Wildcard pattern
    'queue': 'my_topic_queue',  # Optional queue name
    'queue_durable': False,
    'queue_auto_delete': True
}

subscriber = create_subscriber('topic_sub', config)
subscriber.start()

# Receive messages
while True:
    data = subscriber.get_data(block_time=1)
    if data:
        print(f"Received: {data}")
```

## Destination/Source Format

### Queue Format
```
rabbitmq://queue/queue_name
```

Examples:
- `rabbitmq://queue/events`
- `rabbitmq://queue/task_queue`
- `rabbitmq://queue/sensor_data`

### Topic Format
```
rabbitmq://topic/topic_name
```

Examples:
- `rabbitmq://topic/user.login`
- `rabbitmq://topic/app.error.critical`
- `rabbitmq://topic/sensor.temperature`

## Configuration Options

### Publisher Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `destination` | string | Required | RabbitMQ destination URL |
| `host` | string | `localhost` | RabbitMQ server host |
| `port` | int | `5672` | RabbitMQ server port |
| `username` | string | `guest` | Authentication username |
| `password` | string | `guest` | Authentication password |
| `virtual_host` | string | `/` | RabbitMQ virtual host |
| `exchange` | string | `amq.topic` | Exchange name (for topics only) |
| `publish_interval` | int | `0` | Batch publishing interval (inherited) |
| `batch_size` | int | `None` | Batch size threshold (inherited) |

### Subscriber Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source` | string | Required | RabbitMQ source URL |
| `host` | string | `localhost` | RabbitMQ server host |
| `port` | int | `5672` | RabbitMQ server port |
| `username` | string | `guest` | Authentication username |
| `password` | string | `guest` | Authentication password |
| `virtual_host` | string | `/` | RabbitMQ virtual host |
| `exchange` | string | `amq.topic` | Exchange name (for topics only) |
| `binding_key` | string | Topic name | Routing pattern for topic binding |
| `queue` | string | Auto-generated | Queue name |
| `queue_durable` | bool | `False` | Queue survives broker restart (topics) |
| `queue_exclusive` | bool | `False` | Exclusive queue for this connection |
| `queue_auto_delete` | bool | `True` | Delete queue when last consumer disconnects |
| `max_depth` | int | `100000` | Internal queue maximum size (inherited) |

## Topic Wildcards

When subscribing to topics, you can use wildcard patterns in the `binding_key`:

- `*` (asterisk) - Matches exactly **one word**
- `#` (hash) - Matches **zero or more words**

### Examples

```python
# Match any user event
'binding_key': 'user.*'
# Matches: user.login, user.logout, user.signup
# Does not match: user.profile.update

# Match all user events (including nested)
'binding_key': 'user.#'
# Matches: user.login, user.logout, user.profile.update

# Match specific error levels
'binding_key': 'app.error.*'
# Matches: app.error.critical, app.error.warning

# Match all app events
'binding_key': 'app.#'
# Matches: app.error.critical, app.info.startup, app.debug.trace

# Match multiple patterns (in separate subscriptions)
'binding_key': 'user.login'
'binding_key': 'user.logout'
```

## Common Patterns

### Pattern 1: Work Queue (Load Balancing)

Multiple workers consuming from the same queue:

```python
# Producer
publisher = create_publisher('task_producer', {
    'destination': 'rabbitmq://queue/tasks'
})

# Multiple workers competing for tasks
for i in range(3):
    worker = create_subscriber(f'worker_{i}', {
        'source': 'rabbitmq://queue/tasks'
    })
    worker.start()
    
# Tasks are distributed among workers
```

### Pattern 2: Publish-Subscribe (Broadcasting)

Broadcast to all subscribers:

```python
# Publisher
publisher = create_publisher('broadcaster', {
    'destination': 'rabbitmq://topic/news.update'
})

# Multiple subscribers receive all messages
for i in range(3):
    subscriber = create_subscriber(f'listener_{i}', {
        'source': 'rabbitmq://topic/news.update',
        'binding_key': 'news.#'
    })
    subscriber.start()

# All subscribers receive each message
```

### Pattern 3: Selective Routing

Route different message types to different subscribers:

```python
# Publisher sends various events
publisher = create_publisher('event_pub', {
    'destination': 'rabbitmq://topic/app.events'
})

publisher.publish({'level': 'error', 'msg': 'Database down'})  # app.error
publisher.publish({'level': 'info', 'msg': 'User logged in'})  # app.info

# Error handler - only receives errors
error_handler = create_subscriber('error_sub', {
    'source': 'rabbitmq://topic/app.error',
    'binding_key': 'app.error.*'
})

# Info handler - only receives info
info_handler = create_subscriber('info_sub', {
    'source': 'rabbitmq://topic/app.info',
    'binding_key': 'app.info.*'
})

# All events handler
all_handler = create_subscriber('all_sub', {
    'source': 'rabbitmq://topic/app.all',
    'binding_key': 'app.#'
})
```

### Pattern 4: Priority Processing

Different queues for different priorities:

```python
# High priority queue
high_pub = create_publisher('high_priority', {
    'destination': 'rabbitmq://queue/tasks_high'
})

# Low priority queue
low_pub = create_publisher('low_priority', {
    'destination': 'rabbitmq://queue/tasks_low'
})

# Worker processes high priority first
high_worker = create_subscriber('high_worker', {
    'source': 'rabbitmq://queue/tasks_high'
})

low_worker = create_subscriber('low_worker', {
    'source': 'rabbitmq://queue/tasks_low'
})
```

## Virtual Hosts

RabbitMQ supports virtual hosts for environment isolation:

```python
# Production environment
prod_config = {
    'destination': 'rabbitmq://queue/orders',
    'virtual_host': '/production',
    'username': 'prod_user',
    'password': 'prod_pass'
}

# Development environment
dev_config = {
    'destination': 'rabbitmq://queue/orders',
    'virtual_host': '/development',
    'username': 'dev_user',
    'password': 'dev_pass'
}
```

## Monitoring

### Get Publisher Statistics

```python
stats = publisher.details()
print(f"Name: {stats['name']}")
print(f"Destination: {stats['destination']}")
print(f"Published: {stats['publish_count']} messages")
print(f"Last publish: {stats['last_publish']}")
print(f"Queue depth: {stats['queue_depth']}")
```

### Get Subscriber Statistics

```python
stats = subscriber.details()
print(f"Name: {stats['name']}")
print(f"Source: {stats['source']}")
print(f"Received: {stats['receive_count']} messages")
print(f"Last receive: {stats['last_receive']}")
print(f"Queue size: {stats['current_depth']}")
print(f"Suspended: {stats['suspended']}")
```

## RabbitMQ Management

### Web Management Interface
Access at `http://localhost:15672` (default credentials: guest/guest)

### Command Line Tools

```bash
# List queues
sudo rabbitmqctl list_queues

# List exchanges
sudo rabbitmqctl list_exchanges

# List bindings
sudo rabbitmqctl list_bindings

# Purge queue
sudo rabbitmqctl purge_queue my_queue

# Delete queue
sudo rabbitmqctl delete_queue my_queue

# Create user
sudo rabbitmqctl add_user myuser mypassword
sudo rabbitmqctl set_permissions -p / myuser ".*" ".*" ".*"
```

## Troubleshooting

### Connection Refused

**Error**: `Connection refused`

**Solution**: Ensure RabbitMQ is running
```bash
sudo systemctl status rabbitmq-server
sudo systemctl start rabbitmq-server
```

### Authentication Failed

**Error**: `ACCESS_REFUSED - Login was refused`

**Solution**: Check credentials or create user
```bash
sudo rabbitmqctl add_user myuser mypassword
sudo rabbitmqctl set_user_tags myuser administrator
sudo rabbitmqctl set_permissions -p / myuser ".*" ".*" ".*"
```

### No Messages Being Consumed

**Possible causes**:
- Subscriber not started: Call `subscriber.start()`
- Wrong queue name
- Messages published to different virtual host
- Topic binding pattern doesn't match

**Debug**:
```python
# Check if subscriber is running
stats = subscriber.details()
print(f"Queue size: {stats['current_depth']}")

# Check if publisher is working
stats = publisher.details()
print(f"Published count: {stats['publish_count']}")
```

### Topic Messages Not Received

**Common issue**: Binding key doesn't match routing key

```python
# Publisher sends to: 'user.login.success'
publisher = create_publisher('pub', {
    'destination': 'rabbitmq://topic/user.login.success'
})

# This WON'T match:
subscriber = create_subscriber('sub', {
    'source': 'rabbitmq://topic/user.login.success',
    'binding_key': 'user.login'  # ❌ Too specific
})

# This WILL match:
subscriber = create_subscriber('sub', {
    'source': 'rabbitmq://topic/user.login.success',
    'binding_key': 'user.#'  # ✅ Matches all user events
})
```

## Best Practices

1. **Use queues for work distribution** - Multiple workers processing tasks
2. **Use topics for broadcasting** - Multiple consumers need same message
3. **Enable message persistence** - Messages survive broker restart (done automatically)
4. **Handle connection errors** - Implementation auto-reconnects on failures
5. **Use virtual hosts** - Separate different environments
6. **Monitor queue depths** - Use `details()` method to check statistics
7. **Close connections** - Always call `stop()` when done
8. **Use meaningful names** - Clear queue/topic names help debugging

## Example End-to-End

### Queue Example

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time

# Start RabbitMQ server
# docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Create publisher for queue
publisher = create_publisher('task_pub', {
    'destination': 'rabbitmq://queue/tasks'
})

# Create subscriber for queue
subscriber = create_subscriber('task_sub', {
    'source': 'rabbitmq://queue/tasks'
})
subscriber.start()

# Publish tasks
for i in range(5):
    publisher.publish({'task_id': i, 'action': 'process'})
    time.sleep(0.1)

# Consume tasks
for i in range(5):
    data = subscriber.get_data(block_time=2)
    if data:
        print(f"Processing: {data}")

# Cleanup
publisher.stop()
subscriber.stop()
```

### Topic Example

```python
# Create publisher for topic
publisher = create_publisher('event_pub', {
    'destination': 'rabbitmq://topic/app.user.login'
})

# Create multiple subscribers with different patterns
error_sub = create_subscriber('error_sub', {
    'source': 'rabbitmq://topic/app.error',
    'binding_key': 'app.error.*'
})

all_sub = create_subscriber('all_sub', {
    'source': 'rabbitmq://topic/app.all',
    'binding_key': 'app.#'
})

error_sub.start()
all_sub.start()

# Publish events
publisher.publish({'event': 'login', 'user': 'john'})

# Only all_sub receives this (matches app.#)
data = all_sub.get_data(block_time=2)
print(f"All sub received: {data}")

# Cleanup
publisher.stop()
error_sub.stop()
all_sub.stop()
```

## Performance Tips

1. **Batch publishing**: Use `publish_interval` and `batch_size` from base class
2. **Connection reuse**: Share publishers when possible
3. **Message size**: Keep messages small for better throughput
4. **Prefetch**: Controlled automatically for optimal performance
5. **Persistent vs transient**: Persistent messages (default) are slower but safer

## Comparison with ActiveMQ

| Feature | RabbitMQ | ActiveMQ |
|---------|----------|----------|
| Protocol | AMQP | STOMP |
| Queue support | ✅ Yes | ✅ Yes |
| Topic support | ✅ Yes | ✅ Yes |
| Wildcards | `*` and `#` | Similar |
| Default port | 5672 | 61613 |
| Python library | pika | stomp.py |

Both implementations follow the same pattern for easy switching between message brokers!

The RabbitMQ implementation is production-ready and follows the same simple pattern as your ActiveMQ implementation!

## SSL/TLS Security

Enable AMQPS with `"use_ssl": true`. The standard TLS port is **5671** (set
`port` accordingly). Certificate options are paths to PEM files.

| Key | Purpose |
| --- | --- |
| `use_ssl` | Enable TLS (`true`/`false`, default `false`) |
| `ssl_ca_certs` | CA bundle to verify the broker |
| `ssl_certfile` | Client certificate (mutual TLS) |
| `ssl_keyfile` | Client private key (mutual TLS) |
| `ssl_server_hostname` | Hostname to verify / SNI (defaults to `host`) |
| `ssl_no_verify` | Disable verification — testing only, never in production |

```json
{
  "destination": "rabbitmq://queue/orders",
  "host": "mq.internal",
  "port": 5671,
  "username": "svc_orders",
  "password": "${RABBITMQ_PASSWORD}",
  "use_ssl": true,
  "ssl_ca_certs": "/certs/ca.pem",
  "ssl_certfile": "/certs/client.pem",
  "ssl_keyfile": "/certs/client.key"
}
```

## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.


---

# ActiveMQ DataPublisher and DataSubscriber Setup Guide

## © 2025-2030 Ashutosh Sinha

## Overview

The ActiveMQ implementation provides publisher and subscriber classes for Apache ActiveMQ, a popular open-source message broker. ActiveMQ supports both **queue** (point-to-point) and **topic** (publish-subscribe) messaging patterns through the STOMP protocol. It's widely used for enterprise messaging with excellent Java integration and cross-platform support.

## Files

1. **activemq_datapubsub.py** - Contains `ActiveMQDataPublisher` and `ActiveMQDataSubscriber` classes
2. **pubsubfactory.py** - Factory methods support `activemq://queue/` and `activemq://topic/` destinations/sources

## Prerequisites

### Install Apache ActiveMQ

Using Docker (recommended for development):
```bash
# Start ActiveMQ using Docker
docker run -d \
  --name activemq \
  -p 61616:61616 \
  -p 8161:8161 \
  -p 61613:61613 \
  rmohr/activemq:latest

# Web console: http://localhost:8161
# Default credentials: admin/admin
```

Or install manually:
```bash
# Download ActiveMQ
wget https://archive.apache.org/dist/activemq/5.18.3/apache-activemq-5.18.3-bin.tar.gz
tar -xzf apache-activemq-5.18.3-bin.tar.gz
cd apache-activemq-5.18.3

# Start ActiveMQ
bin/activemq start

# Stop ActiveMQ
bin/activemq stop

# Check status
bin/activemq status
```

### Install Python Client

```bash
pip install stomp.py
```

## ActiveMQ Concepts

### Queues (Point-to-Point)
- **Queue**: A message destination for point-to-point communication
- Each message is consumed by exactly one consumer
- Multiple consumers compete for messages (load balancing)
- Messages are removed from queue after consumption
- URL format: `activemq://queue/queue_name`

### Topics (Publish-Subscribe)
- **Topic**: A message destination for publish-subscribe communication
- Messages are broadcast to all subscribers
- Each subscriber receives a copy of every message
- Supports durable subscriptions (messages saved when subscriber offline)
- URL format: `activemq://topic/topic_name`

### STOMP Protocol
- Simple Text Oriented Messaging Protocol
- Text-based protocol (easy to debug)
- Language-agnostic (not just Java)
- Default port: 61613
- Human-readable frames

### Message Acknowledgment
- **Auto-acknowledge**: Messages acknowledged automatically
- **Client-acknowledge**: Manual acknowledgment required
- Current implementation uses auto-acknowledge

## Usage

### Publishing to a Queue

Point-to-point messaging:

```python
from core.pubsub.pubsubfactory import create_publisher

config = {
    'destination': 'activemq://queue/task_queue',
    'host': 'localhost',
    'port': 61613,
    'username': 'admin',
    'password': 'admin'
}

publisher = create_publisher('task_publisher', config)

# Publish data
publisher.publish({'task': 'process_order', 'order_id': 'ORD-123'})

# Stop when done
publisher.stop()
```

### Publishing to a Topic

Broadcast messaging:

```python
config = {
    'destination': 'activemq://topic/notifications',
    'host': 'localhost',
    'port': 61613,
    'username': 'admin',
    'password': 'admin'
}

publisher = create_publisher('notification_pub', config)

# Publish notification to all subscribers
publisher.publish({'type': 'alert', 'message': 'System maintenance at 2AM'})

publisher.stop()
```

### Subscribing from a Queue

Consume messages from a queue:

```python
from core.pubsub.pubsubfactory import create_subscriber

config = {
    'source': 'activemq://queue/task_queue',
    'host': 'localhost',
    'port': 61613,
    'username': 'admin',
    'password': 'admin'
}

subscriber = create_subscriber('task_worker', config)
subscriber.start()

# Get data (blocking with timeout)
data = subscriber.get_data(block_time=5)
if data:
    print(f"Received task: {data}")

subscriber.stop()
```

### Subscribing from a Topic

Subscribe to broadcast messages:

```python
config = {
    'source': 'activemq://topic/notifications',
    'host': 'localhost',
    'port': 61613,
    'username': 'admin',
    'password': 'admin'
}

subscriber = create_subscriber('notification_listener', config)
subscriber.start()

# Receive notifications
data = subscriber.get_data(block_time=5)
if data:
    print(f"Received notification: {data}")

subscriber.stop()
```

## Destination/Source Format

### Queue Format
```
activemq://queue/queue_name
```

Examples:
- `activemq://queue/orders`
- `activemq://queue/task_queue`
- `activemq://queue/payment_processing`

### Topic Format
```
activemq://topic/topic_name
```

Examples:
- `activemq://topic/notifications`
- `activemq://topic/events`
- `activemq://topic/system_alerts`

## Configuration Options

### Publisher Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `destination` | string | Required | ActiveMQ destination URL |
| `host` | string | `localhost` | ActiveMQ server host |
| `port` | int | `61613` | STOMP port |
| `username` | string | `admin` | Authentication username |
| `password` | string | `admin` | Authentication password |
| `publish_interval` | int | `0` | Batch publishing interval (inherited) |
| `batch_size` | int | `None` | Batch size threshold (inherited) |

### Subscriber Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source` | string | Required | ActiveMQ source URL |
| `host` | string | `localhost` | ActiveMQ server host |
| `port` | int | `61613` | STOMP port |
| `username` | string | `admin` | Authentication username |
| `password` | string | `admin` | Authentication password |
| `max_depth` | int | `100000` | Internal queue maximum size (inherited) |

**Note**: ActiveMQ subscriber uses a **push model** via listeners, not polling. Messages are pushed to the subscriber automatically.

## Common Patterns

### Pattern 1: Work Queue (Load Balancing)

Multiple workers competing for tasks:

```python
# Producer
publisher = create_publisher('task_producer', {
    'destination': 'activemq://queue/tasks'
})

# Multiple workers consuming from same queue
for i in range(3):
    worker = create_subscriber(f'worker_{i}', {
        'source': 'activemq://queue/tasks'
    })
    worker.start()

# Tasks are distributed among workers
for i in range(10):
    publisher.publish({'task_id': i, 'action': 'process'})
```

### Pattern 2: Publish-Subscribe (Broadcasting)

Broadcast to all subscribers:

```python
# Publisher
publisher = create_publisher('event_pub', {
    'destination': 'activemq://topic/events'
})

# Multiple subscribers receive all messages
logger = create_subscriber('logger', {
    'source': 'activemq://topic/events'
})

analytics = create_subscriber('analytics', {
    'source': 'activemq://topic/events'
})

alerting = create_subscriber('alerting', {
    'source': 'activemq://topic/events'
})

logger.start()
analytics.start()
alerting.start()

# All subscribers receive this message
publisher.publish({'event': 'user_login', 'user_id': 123})
```

### Pattern 3: Request-Reply

Request-response messaging:

```python
# Request sender
request_pub = create_publisher('requester', {
    'destination': 'activemq://queue/requests'
})

reply_sub = create_subscriber('reply_receiver', {
    'source': 'activemq://queue/replies'
})
reply_sub.start()

# Request handler
request_sub = create_subscriber('handler', {
    'source': 'activemq://queue/requests'
})
request_sub.start()

reply_pub = create_publisher('replier', {
    'destination': 'activemq://queue/replies'
})

# Send request
request_pub.publish({'request': 'get_user_info', 'user_id': 123})

# Handler processes and replies
request_data = request_sub.get_data(block_time=5)
if request_data:
    response = {'user_id': 123, 'name': 'John Doe'}
    reply_pub.publish(response)

# Receive reply
reply_data = reply_sub.get_data(block_time=5)
```

### Pattern 4: Priority Queues

Multiple queues for different priorities:

```python
# High priority publisher
high_pub = create_publisher('high_priority', {
    'destination': 'activemq://queue/high_priority_tasks'
})

# Low priority publisher
low_pub = create_publisher('low_priority', {
    'destination': 'activemq://queue/low_priority_tasks'
})

# Worker processes high priority first
high_worker = create_subscriber('high_worker', {
    'source': 'activemq://queue/high_priority_tasks'
})

low_worker = create_subscriber('low_worker', {
    'source': 'activemq://queue/low_priority_tasks'
})

high_worker.start()
low_worker.start()
```

### Pattern 5: Dead Letter Queue

Handle failed messages:

```python
# Main queue
main_pub = create_publisher('main_pub', {
    'destination': 'activemq://queue/main_queue'
})

main_sub = create_subscriber('processor', {
    'source': 'activemq://queue/main_queue'
})
main_sub.start()

# Dead letter queue for failures
dlq_pub = create_publisher('dlq_handler', {
    'destination': 'activemq://queue/dead_letter_queue'
})

# Process with error handling
data = main_sub.get_data(block_time=5)
if data:
    try:
        process_message(data)
    except Exception as e:
        # Send to DLQ
        dlq_pub.publish({
            'original_message': data,
            'error': str(e),
            'timestamp': time.time()
        })
```

## Push Model vs Pull Model

ActiveMQ subscriber uses a **push model** (unlike most other implementations):

```python
# When you call start(), the subscriber:
# 1. Connects to ActiveMQ
# 2. Sets up a listener
# 3. Messages are PUSHED to the listener automatically
# 4. Messages are queued internally

subscriber.start()  # Activates the listener

# get_data() retrieves from internal queue
# NOT from ActiveMQ directly
data = subscriber.get_data(block_time=5)
```

This is different from polling-based implementations where `get_data()` actively fetches from the broker.

## Monitoring

### Get Publisher Statistics

```python
stats = publisher.details()
print(f"Name: {stats['name']}")
print(f"Destination: {stats['destination']}")
print(f"Published: {stats['publish_count']} messages")
print(f"Last publish: {stats['last_publish']}")
print(f"Queue depth: {stats['queue_depth']}")
```

### Get Subscriber Statistics

```python
stats = subscriber.details()
print(f"Name: {stats['name']}")
print(f"Source: {stats['source']}")
print(f"Received: {stats['receive_count']} messages")
print(f"Last receive: {stats['last_receive']}")
print(f"Internal queue size: {stats['current_depth']}")
```

### ActiveMQ Web Console

Access the web console at `http://localhost:8161/admin/`
- View queues and topics
- Browse messages
- Monitor connections
- View statistics
- Manage destinations

## ActiveMQ Administration

### Web Console Management

Navigate to `http://localhost:8161/admin/` (default: admin/admin)

**Queues Tab:**
- View all queues
- Number of pending messages
- Number of consumers
- Number of enqueued/dequeued messages
- Purge queues
- Delete queues

**Topics Tab:**
- View all topics
- Number of producers/consumers
- Message statistics

**Subscribers Tab:**
- Active subscriptions
- Consumer information
- Client IDs

### Command Line Administration

```bash
# Using activemq command
activemq browse <queue_name>     # Browse messages
activemq purge <queue_name>       # Remove all messages
activemq dstat                    # Display statistics

# Using JMX (requires JConsole or similar)
# Connect to localhost:1099
```

### Configuration Files

**activemq.xml** - Main configuration:
```xml
<broker xmlns="http://activemq.apache.org/schema/core">
  <destinationPolicy>
    <policyMap>
      <policyEntries>
        <policyEntry queue=">" memoryLimit="64mb"/>
        <policyEntry topic=">" memoryLimit="64mb"/>
      </policyEntries>
    </policyMap>
  </destinationPolicy>

  <systemUsage>
    <systemUsage>
      <memoryUsage>
        <memoryUsage limit="512 mb"/>
      </memoryUsage>
      <storeUsage>
        <storeUsage limit="100 gb"/>
      </storeUsage>
    </systemUsage>
  </systemUsage>

  <transportConnectors>
    <transportConnector name="stomp" uri="stomp://0.0.0.0:61613"/>
    <transportConnector name="openwire" uri="tcp://0.0.0.0:61616"/>
  </transportConnectors>
</broker>
```

## Troubleshooting

### Connection Refused

**Error**: `Could not connect to ActiveMQ`

**Solution**: Check if ActiveMQ is running
```bash
# Check process
ps aux | grep activemq

# Start ActiveMQ
bin/activemq start

# Check Docker container
docker ps
docker logs activemq
```

### Authentication Failed

**Error**: `User name [X] or password is invalid`

**Solution**: Check credentials or configure security
```xml
<!-- In activemq.xml -->
<plugins>
  <simpleAuthenticationPlugin>
    <users>
      <authenticationUser username="admin" password="admin" groups="admins,publishers,consumers"/>
      <authenticationUser username="user" password="password" groups="consumers"/>
    </users>
  </simpleAuthenticationPlugin>
</plugins>
```

### Queue/Topic Not Receiving Messages

**Possible causes**:
- Subscriber not started (forgot to call `.start()`)
- Wrong queue/topic name
- Connection issues
- Subscriber stopped or crashed

**Solution**:
```python
# Always start subscriber
subscriber.start()

# Check stats
stats = subscriber.details()
print(f"Received: {stats['receive_count']}")

# Check ActiveMQ web console for pending messages
```

### Memory Limit Exceeded

**Error**: `Usage Manager Memory Limit reached`

**Solution**: Increase memory limits in activemq.xml
```xml
<systemUsage>
  <systemUsage>
    <memoryUsage>
      <memoryUsage limit="2 gb"/>  <!-- Increase limit -->
    </memoryUsage>
  </systemUsage>
</systemUsage>
```

### Port Already in Use

**Error**: `Address already in use`

**Solution**: Change port or kill existing process
```bash
# Find process using port
lsof -i :61613
netstat -tulpn | grep 61613

# Kill process
kill -9 <PID>

# Or change port in activemq.xml
<transportConnector name="stomp" uri="stomp://0.0.0.0:61614"/>
```

## Best Practices

1. **Always start subscribers** - Call `subscriber.start()` to activate listener
2. **Use meaningful destination names** - Clear queue/topic naming
3. **Handle exceptions gracefully** - Implement error handling
4. **Monitor queue depths** - Detect processing bottlenecks
5. **Use topics for broadcasting** - Multiple independent consumers
6. **Use queues for work distribution** - Competing consumers
7. **Close connections properly** - Always call `stop()`
8. **Configure memory limits** - Prevent broker from running out of memory
9. **Enable persistence** - For message durability across restarts
10. **Use dead letter queues** - Handle failed messages

## Performance Tips

1. **Batch publishing**: Use `publish_interval` and `batch_size` from base class
2. **Persistent vs non-persistent**: Non-persistent is faster
3. **Memory management**: Set appropriate memory limits
4. **Connection pooling**: Reuse publishers when possible
5. **Message size**: Keep messages small for better throughput
6. **Network optimization**: Use fast network connections
7. **Disable prefetch for fairness**: For balanced load distribution
8. **Use producer flow control**: Prevent overwhelming consumers
9. **Monitor ActiveMQ JVM**: Tune heap size if needed
10. **Clean up old messages**: Use message expiration

## Complete Examples

### Example 1: Simple Queue Processing

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time

# Create publisher
publisher = create_publisher('order_pub', {
    'destination': 'activemq://queue/orders',
    'host': 'localhost',
    'port': 61613,
    'username': 'admin',
    'password': 'admin'
})

# Create subscriber
subscriber = create_subscriber('order_processor', {
    'source': 'activemq://queue/orders',
    'host': 'localhost',
    'port': 61613,
    'username': 'admin',
    'password': 'admin'
})
subscriber.start()  # Important: Start the subscriber!

# Publish orders
orders = [
    {'order_id': 'ORD-001', 'customer': 'John', 'total': 99.99},
    {'order_id': 'ORD-002', 'customer': 'Jane', 'total': 149.99},
    {'order_id': 'ORD-003', 'customer': 'Bob', 'total': 79.99}
]

for order in orders:
    publisher.publish(order)
    print(f"Published order: {order['order_id']}")
    time.sleep(0.5)

# Process orders
for i in range(len(orders)):
    data = subscriber.get_data(block_time=5)
    if data:
        print(f"Processing order: {data['order_id']} - ${data['total']}")
    else:
        print("No data received")

# Cleanup
publisher.stop()
subscriber.stop()

print("\n✓ Example 1 completed!")
```

### Example 2: Topic Broadcasting

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
import threading

# Create publisher
publisher = create_publisher('alert_pub', {
    'destination': 'activemq://topic/system_alerts',
    'host': 'localhost',
    'port': 61613
})

# Logger service
def logger_service():
    subscriber = create_subscriber('logger', {
        'source': 'activemq://topic/system_alerts',
        'host': 'localhost',
        'port': 61613
    })
    subscriber.start()
    
    print("[LOGGER] Started")
    for i in range(5):
        data = subscriber.get_data(block_time=2)
        if data:
            print(f"[LOGGER] {data['severity']}: {data['message']}")

# Email service
def email_service():
    subscriber = create_subscriber('emailer', {
        'source': 'activemq://topic/system_alerts',
        'host': 'localhost',
        'port': 61613
    })
    subscriber.start()
    
    print("[EMAIL] Started")
    for i in range(5):
        data = subscriber.get_data(block_time=2)
        if data:
            if data['severity'] in ['error', 'critical']:
                print(f"[EMAIL] Sending email about: {data['message']}")

# SMS service
def sms_service():
    subscriber = create_subscriber('sms', {
        'source': 'activemq://topic/system_alerts',
        'host': 'localhost',
        'port': 61613
    })
    subscriber.start()
    
    print("[SMS] Started")
    for i in range(5):
        data = subscriber.get_data(block_time=2)
        if data:
            if data['severity'] == 'critical':
                print(f"[SMS] Sending SMS: {data['message']}")

# Start all services
logger_thread = threading.Thread(target=logger_service, daemon=True)
email_thread = threading.Thread(target=email_service, daemon=True)
sms_thread = threading.Thread(target=sms_service, daemon=True)

logger_thread.start()
email_thread.start()
sms_thread.start()

time.sleep(1)  # Wait for services to start

# Publish alerts
alerts = [
    {'severity': 'info', 'message': 'System started'},
    {'severity': 'warning', 'message': 'High memory usage'},
    {'severity': 'error', 'message': 'Database connection failed'},
    {'severity': 'critical', 'message': 'Service down'},
    {'severity': 'info', 'message': 'Service recovered'}
]

for alert in alerts:
    publisher.publish(alert)
    print(f"\n>>> Published alert: {alert['severity']} - {alert['message']}")
    time.sleep(1)

# Wait for processing
time.sleep(3)

# Cleanup
publisher.stop()

print("\n✓ Example 2 completed!")
```

### Example 3: Load Balanced Workers

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
import threading
import random

# Create publisher
publisher = create_publisher('task_pub', {
    'destination': 'activemq://queue/background_tasks',
    'host': 'localhost',
    'port': 61613
})

# Worker function
def worker_func(worker_id, num_tasks):
    subscriber = create_subscriber(f'worker_{worker_id}', {
        'source': 'activemq://queue/background_tasks',
        'host': 'localhost',
        'port': 61613
    })
    subscriber.start()
    
    print(f"[Worker {worker_id}] Started")
    
    processed = 0
    while processed < num_tasks:
        data = subscriber.get_data(block_time=1)
        if data:
            # Simulate processing time
            processing_time = random.uniform(0.1, 0.5)
            time.sleep(processing_time)
            print(f"[Worker {worker_id}] Processed task {data['task_id']} in {processing_time:.2f}s")
            processed += 1
    
    subscriber.stop()
    print(f"[Worker {worker_id}] Finished")

# Start 3 workers
num_workers = 3
num_tasks = 15
tasks_per_worker = num_tasks // num_workers

threads = []
for i in range(num_workers):
    thread = threading.Thread(target=worker_func, args=(i, tasks_per_worker), daemon=True)
    thread.start()
    threads.append(thread)

time.sleep(1)  # Wait for workers to start

# Publish tasks
print(f"\nPublishing {num_tasks} tasks...\n")
for i in range(num_tasks):
    task = {
        'task_id': i,
        'type': 'background_job',
        'data': f'task_data_{i}'
    }
    publisher.publish(task)
    time.sleep(0.1)

# Wait for all workers to complete
for thread in threads:
    thread.join()

# Cleanup
publisher.stop()

print("\n✓ Example 3 completed!")
```

### Example 4: Request-Reply Pattern

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
import threading
import uuid

# Request handler service
def request_handler():
    request_sub = create_subscriber('handler', {
        'source': 'activemq://queue/requests',
        'host': 'localhost',
        'port': 61613
    })
    request_sub.start()
    
    reply_pub = create_publisher('replier', {
        'destination': 'activemq://queue/replies',
        'host': 'localhost',
        'port': 61613
    })
    
    print("[HANDLER] Ready to process requests")
    
    for i in range(5):
        request = request_sub.get_data(block_time=3)
        if request:
            print(f"[HANDLER] Processing request: {request['request_id']}")
            
            # Simulate processing
            time.sleep(0.5)
            
            # Send reply
            reply = {
                'request_id': request['request_id'],
                'status': 'success',
                'result': f"Result for {request['operation']}",
                'data': f"Processed {request.get('param', 'N/A')}"
            }
            reply_pub.publish(reply)
            print(f"[HANDLER] Sent reply for: {request['request_id']}")
    
    reply_pub.stop()
    request_sub.stop()

# Start handler in background
handler_thread = threading.Thread(target=request_handler, daemon=True)
handler_thread.start()

time.sleep(1)  # Wait for handler to start

# Client - send requests and receive replies
request_pub = create_publisher('requester', {
    'destination': 'activemq://queue/requests',
    'host': 'localhost',
    'port': 61613
})

reply_sub = create_subscriber('reply_receiver', {
    'source': 'activemq://queue/replies',
    'host': 'localhost',
    'port': 61613
})
reply_sub.start()

# Send multiple requests
print("\n[CLIENT] Sending requests...\n")
request_ids = []
for i in range(5):
    request_id = str(uuid.uuid4())
    request_ids.append(request_id)
    
    request = {
        'request_id': request_id,
        'operation': f'operation_{i}',
        'param': f'param_{i}'
    }
    request_pub.publish(request)
    print(f"[CLIENT] Sent request: {request_id}")
    time.sleep(0.2)

# Receive replies
print("\n[CLIENT] Waiting for replies...\n")
for i in range(5):
    reply = reply_sub.get_data(block_time=5)
    if reply:
        print(f"[CLIENT] Received reply: {reply['request_id']} - {reply['status']}")
        print(f"         Result: {reply['result']}")

# Cleanup
request_pub.stop()
reply_sub.stop()

# Wait for handler to finish
handler_thread.join(timeout=2)

print("\n✓ Example 4 completed!")
```

### Example 5: Error Handling with Dead Letter Queue

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
import random

# Main queue publisher
main_pub = create_publisher('main_pub', {
    'destination': 'activemq://queue/processing_queue',
    'host': 'localhost',
    'port': 61613
})

# Dead letter queue publisher
dlq_pub = create_publisher('dlq_pub', {
    'destination': 'activemq://queue/dead_letter_queue',
    'host': 'localhost',
    'port': 61613
})

# Main queue subscriber
main_sub = create_subscriber('processor', {
    'source': 'activemq://queue/processing_queue',
    'host': 'localhost',
    'port': 61613
})
main_sub.start()

# DLQ subscriber (monitoring)
dlq_sub = create_subscriber('dlq_monitor', {
    'source': 'activemq://queue/dead_letter_queue',
    'host': 'localhost',
    'port': 61613
})
dlq_sub.start()

print("Publishing messages...\n")

# Publish messages (some will fail)
for i in range(10):
    message = {
        'message_id': i,
        'data': f'data_{i}',
        'should_fail': random.choice([True, False])  # Random failures
    }
    main_pub.publish(message)
    time.sleep(0.2)

print("Processing messages...\n")

# Process messages with error handling
successful = 0
failed = 0

for i in range(10):
    data = main_sub.get_data(block_time=2)
    if data:
        try:
            # Simulate processing
            if data.get('should_fail'):
                raise Exception(f"Processing failed for message {data['message_id']}")
            
            print(f"✓ Successfully processed message {data['message_id']}")
            successful += 1
            
        except Exception as e:
            print(f"✗ Failed to process message {data['message_id']}: {str(e)}")
            
            # Send to dead letter queue
            dlq_message = {
                'original_message': data,
                'error': str(e),
                'timestamp': time.time(),
                'retry_count': 0
            }
            dlq_pub.publish(dlq_message)
            failed += 1

print(f"\n--- Processing Summary ---")
print(f"Successful: {successful}")
print(f"Failed: {failed}")

# Check DLQ
print(f"\nChecking dead letter queue...\n")
time.sleep(1)

dlq_count = 0
while True:
    dlq_data = dlq_sub.get_data(block_time=1)
    if dlq_data:
        dlq_count += 1
        print(f"[DLQ] Message {dlq_data['original_message']['message_id']}: {dlq_data['error']}")
    else:
        break

print(f"\nTotal messages in DLQ: {dlq_count}")

# Cleanup
main_pub.stop()
dlq_pub.stop()
main_sub.stop()
dlq_sub.stop()

print("\n✓ Example 5 completed!")
```

## Comparison with Other Brokers

| Feature | ActiveMQ | Kafka | RabbitMQ | TIBCO EMS | IBM MQ |
|---------|----------|-------|----------|-----------|--------|
| Model | Queue/Topic | Topic only | Queue/Topic | Queue/Topic | Queue/Topic |
| Protocol | STOMP | Proprietary | AMQP | JMS | Proprietary |
| Push/Pull | Push | Pull | Pull | Pull | Pull |
| Persistence | Optional | Always | Optional | Optional | Optional |
| Default port | 61613 | 9092 | 5672 | 7222 | 1414 |
| Python library | stomp.py | kafka-python | pika | tibcoems | pymqi |
| Web console | ✅ Yes | ❌ No | ✅ Yes | ❌ No | ✅ Yes |
| Easy to setup | ✅ Very easy | Medium | Medium | Hard | Hard |
| Best for | Traditional MQ | Event streaming | Traditional MQ | Enterprise MQ | Enterprise MQ |

## When to Use ActiveMQ

**Use ActiveMQ when you need:**
- Traditional message queuing patterns
- Both queue and topic support
- Easy setup and administration
- Web-based management console
- Good Java integration
- Cross-platform messaging
- Reliable message delivery

**Consider other brokers when:**
- Need high-throughput streaming (use Kafka)
- Need AMQP protocol (use RabbitMQ)
- Need enterprise features (use TIBCO EMS or IBM MQ)
- Need message replay (use Kafka)

## Key Advantages of ActiveMQ

1. **Easy to Use**: Simple installation and configuration
2. **Web Console**: Built-in management interface
3. **Cross-Platform**: Works with many languages via STOMP
4. **Feature-Rich**: Supports queues, topics, virtual destinations
5. **Lightweight**: Lower resource requirements than enterprise brokers
6. **Open Source**: Free and well-documented
7. **Battle-Tested**: Used in production by many companies

The ActiveMQ implementation provides a solid foundation for traditional message broker patterns with excellent ease of use!

## SSL/TLS Security

Enable STOMP-over-TLS with `"use_ssl": true`. The standard ActiveMQ STOMP+SSL
port is **61614** (set `port` accordingly). Certificate options are paths to
PEM files.

| Key | Purpose |
| --- | --- |
| `use_ssl` | Enable TLS (`true`/`false`, default `false`) |
| `ssl_ca_certs` | CA bundle to verify the broker |
| `ssl_certfile` | Client certificate (mutual TLS) |
| `ssl_keyfile` | Client private key (mutual TLS) |
| `ssl_version` | Optional explicit TLS protocol version |

```json
{
  "source": "activemq://queue/orders",
  "host": "mq.internal",
  "port": 61614,
  "username": "svc_orders",
  "password": "${ACTIVEMQ_PASSWORD}",
  "use_ssl": true,
  "ssl_ca_certs": "/certs/ca.pem",
  "ssl_certfile": "/certs/client.pem",
  "ssl_keyfile": "/certs/client.key"
}
```

## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.

---

# TIBCO EMS DataPublisher and DataSubscriber Setup Guide

## © 2025-2030 Ashutosh Sinha


## Overview

The TIBCO EMS (Enterprise Message Service) implementation provides publisher and subscriber classes that support both **queue** and **topic** messaging patterns. TIBCO EMS is an enterprise-grade message broker that implements the JMS (Java Message Service) standard.

## Files

1. **tibcoems_datapubsub.py** - Contains `TibcoEMSDataPublisher` and `TibcoEMSDataSubscriber` classes
2. **Updated pubsubfactory.py** - Factory methods now support `tibcoems://` destinations/sources

## Prerequisites

### Install TIBCO EMS Server

Download and install TIBCO EMS from TIBCO's website (requires license):
- TIBCO EMS Server (default port: 7222)
- TIBCO EMS Administration tools

### Install TIBCO EMS Python Client

```bash
# The TIBCO EMS Python client is typically installed from TIBCO's distribution
# Location varies by installation, commonly found in:
# $TIBCO_HOME/ems/<version>/clients/python

# Add to PYTHONPATH or install the tibcoems package
export PYTHONPATH=$TIBCO_HOME/ems/8.x/clients/python:$PYTHONPATH

# Or copy the tibcoems module to your site-packages
cp -r $TIBCO_HOME/ems/8.x/clients/python/tibcoems /path/to/site-packages/
```

## Queue vs Topic

TIBCO EMS supports two messaging patterns:

### Queue (Point-to-Point)
- Messages sent to a queue are consumed by only one subscriber
- Multiple subscribers compete for messages (load balancing)
- URL format: `tibcoems://queue/queue_name`

### Topic (Publish-Subscribe)
- Messages are broadcast to all subscribers
- Supports durable subscriptions (messages saved when subscriber is offline)
- URL format: `tibcoems://topic/topic_name`

## Usage

### Publishing to a Queue

Direct point-to-point messaging:

```python
from core.pubsub.pubsubfactory import create_publisher

config = {
    'destination': 'tibcoems://queue/my_queue',
    'host': 'localhost',
    'port': 7222,
    'username': 'admin',
    'password': 'admin'
}

publisher = create_publisher('my_publisher', config)

# Publish data
publisher.publish({'event': 'user_login', 'user_id': 123})

# Stop when done
publisher.stop()
```

### Publishing to a Topic

Broadcast messages to all subscribers:

```python
config = {
    'destination': 'tibcoems://topic/user.events',
    'host': 'localhost',
    'port': 7222,
    'username': 'admin',
    'password': 'admin'
}

publisher = create_publisher('topic_pub', config)

# Publish to topic
publisher.publish({'event': 'user_login', 'user_id': 123})

publisher.stop()
```

### Subscribing from a Queue

Consume messages from a queue:

```python
from core.pubsub.pubsubfactory import create_subscriber

config = {
    'source': 'tibcoems://queue/my_queue',
    'host': 'localhost',
    'port': 7222,
    'username': 'admin',
    'password': 'admin'
}

subscriber = create_subscriber('my_subscriber', config)
subscriber.start()

# Get data (blocking with timeout)
data = subscriber.get_data(block_time=5)
if data:
    print(f"Received: {data}")

subscriber.stop()
```

### Subscribing from a Topic

Subscribe to topic with optional durable subscription:

```python
# Non-durable subscription (default)
config = {
    'source': 'tibcoems://topic/user.events',
    'host': 'localhost',
    'port': 7222,
    'username': 'admin',
    'password': 'admin'
}

subscriber = create_subscriber('topic_sub', config)
subscriber.start()

# Receive messages
data = subscriber.get_data(block_time=5)
if data:
    print(f"Received: {data}")

subscriber.stop()
```

### Durable Topic Subscription

Durable subscriptions preserve messages when subscriber is offline:

```python
config = {
    'source': 'tibcoems://topic/important.events',
    'host': 'localhost',
    'port': 7222,
    'username': 'admin',
    'password': 'admin',
    'durable': True,
    'client_id': 'my_app_client',
    'durable_name': 'my_durable_sub'
}

subscriber = create_subscriber('durable_sub', config)
subscriber.start()

# Even if subscriber disconnects, messages are preserved
# Reconnecting with same durable_name retrieves missed messages

subscriber.stop()
```

### Using Message Selectors

Filter messages using SQL-like selectors:

```python
config = {
    'source': 'tibcoems://queue/orders',
    'message_selector': "priority > 5 AND status = 'urgent'"
}

subscriber = create_subscriber('urgent_orders', config)
subscriber.start()

# Only receives messages matching the selector
```

## Destination/Source Format

### Queue Format
```
tibcoems://queue/queue_name
```

Examples:
- `tibcoems://queue/orders`
- `tibcoems://queue/task_queue`
- `tibcoems://queue/notifications`

### Topic Format
```
tibcoems://topic/topic_name
```

Examples:
- `tibcoems://topic/user.events`
- `tibcoems://topic/system.alerts`
- `tibcoems://topic/stock.prices`

## Configuration Options

### Publisher Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `destination` | string | Required | TIBCO EMS destination URL |
| `host` | string | `localhost` | TIBCO EMS server host |
| `port` | int | `7222` | TIBCO EMS server port |
| `username` | string | `admin` | Authentication username |
| `password` | string | `admin` | Authentication password |
| `server_url` | string | `tcp://host:port` | Full server URL |
| `use_ssl` | bool | `False` | Enable SSL/TLS (changes to ssl://) |
| `publish_interval` | int | `0` | Batch publishing interval (inherited) |
| `batch_size` | int | `None` | Batch size threshold (inherited) |

### Subscriber Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source` | string | Required | TIBCO EMS source URL |
| `host` | string | `localhost` | TIBCO EMS server host |
| `port` | int | `7222` | TIBCO EMS server port |
| `username` | string | `admin` | Authentication username |
| `password` | string | `admin` | Authentication password |
| `server_url` | string | `tcp://host:port` | Full server URL |
| `use_ssl` | bool | `False` | Enable SSL/TLS (changes to ssl://) |
| `message_selector` | string | `None` | SQL-like message filter |
| `durable` | bool | `False` | Durable subscription (topics only) |
| `client_id` | string | `{name}_client` | Client identifier for durable subs |
| `durable_name` | string | `{name}_durable` | Durable subscription name |
| `max_depth` | int | `100000` | Internal queue maximum size (inherited) |

## SSL/TLS Configuration

For secure connections:

```python
config = {
    'destination': 'tibcoems://queue/secure_queue',
    'host': 'ems-server.example.com',
    'port': 7243,  # SSL port
    'use_ssl': True,
    'username': 'secure_user',
    'password': 'secure_pass'
}

# Or specify full URL
config = {
    'destination': 'tibcoems://queue/secure_queue',
    'server_url': 'ssl://ems-server.example.com:7243',
    'username': 'secure_user',
    'password': 'secure_pass'
}
```

## Message Selectors

Message selectors use SQL-like syntax to filter messages:

### Selector Syntax

```python
# Numeric comparison
'priority > 5'
'age >= 18 AND age <= 65'

# String comparison
"status = 'active'"
"region IN ('US', 'EU', 'APAC')"

# Boolean operators
"urgent = TRUE AND priority > 7"

# LIKE operator for patterns
"product LIKE 'TECH%'"

# NULL checks
"description IS NOT NULL"

# Complex expressions
"(priority > 5 OR urgent = TRUE) AND status = 'pending'"
```

### Using Selectors

```python
# High priority messages only
config = {
    'source': 'tibcoems://queue/tasks',
    'message_selector': 'priority > 5'
}

# Specific region messages
config = {
    'source': 'tibcoems://topic/events',
    'message_selector': "region = 'US'"
}

# Urgent orders
config = {
    'source': 'tibcoems://queue/orders',
    'message_selector': "urgent = TRUE AND total > 1000"
}
```

## Common Patterns

### Pattern 1: Work Queue (Load Balancing)

Multiple workers consuming from the same queue:

```python
# Producer
publisher = create_publisher('task_producer', {
    'destination': 'tibcoems://queue/tasks'
})

# Multiple workers competing for tasks
for i in range(3):
    worker = create_subscriber(f'worker_{i}', {
        'source': 'tibcoems://queue/tasks'
    })
    worker.start()

# Tasks are distributed among workers
```

### Pattern 2: Publish-Subscribe (Broadcasting)

Broadcast to all subscribers:

```python
# Publisher
publisher = create_publisher('broadcaster', {
    'destination': 'tibcoems://topic/news.updates'
})

# Multiple subscribers receive all messages
for i in range(3):
    subscriber = create_subscriber(f'listener_{i}', {
        'source': 'tibcoems://topic/news.updates'
    })
    subscriber.start()

# All subscribers receive each message
```

### Pattern 3: Durable Subscriptions

Preserve messages for offline subscribers:

```python
# Publisher sends important events
publisher = create_publisher('event_pub', {
    'destination': 'tibcoems://topic/critical.events'
})

# Durable subscriber - receives messages even when offline
subscriber = create_subscriber('critical_handler', {
    'source': 'tibcoems://topic/critical.events',
    'durable': True,
    'client_id': 'critical_handler_client',
    'durable_name': 'critical_handler_sub'
})
subscriber.start()

# Messages sent when subscriber is offline are delivered when it reconnects
```

### Pattern 4: Priority-Based Processing

Use message selectors to prioritize work:

```python
# Publisher
publisher = create_publisher('task_pub', {
    'destination': 'tibcoems://queue/all_tasks'
})

# High priority worker
high_worker = create_subscriber('high_priority_worker', {
    'source': 'tibcoems://queue/all_tasks',
    'message_selector': 'priority > 7'
})

# Normal priority worker
normal_worker = create_subscriber('normal_worker', {
    'source': 'tibcoems://queue/all_tasks',
    'message_selector': 'priority <= 7'
})

high_worker.start()
normal_worker.start()
```

### Pattern 5: Geographic Routing

Route messages based on region:

```python
# Publisher
publisher = create_publisher('order_pub', {
    'destination': 'tibcoems://topic/orders'
})

# US orders handler
us_handler = create_subscriber('us_handler', {
    'source': 'tibcoems://topic/orders',
    'message_selector': "region = 'US'"
})

# EU orders handler
eu_handler = create_subscriber('eu_handler', {
    'source': 'tibcoems://topic/orders',
    'message_selector': "region = 'EU'"
})

us_handler.start()
eu_handler.start()
```

## Monitoring

### Get Publisher Statistics

```python
stats = publisher.details()
print(f"Name: {stats['name']}")
print(f"Destination: {stats['destination']}")
print(f"Published: {stats['publish_count']} messages")
print(f"Last publish: {stats['last_publish']}")
print(f"Queue depth: {stats['queue_depth']}")
```

### Get Subscriber Statistics

```python
stats = subscriber.details()
print(f"Name: {stats['name']}")
print(f"Source: {stats['source']}")
print(f"Received: {stats['receive_count']} messages")
print(f"Last receive: {stats['last_receive']}")
print(f"Queue size: {stats['current_depth']}")
print(f"Suspended: {stats['suspended']}")
```

## TIBCO EMS Administration

### Command Line Tools

```bash
# Start EMS server
tibemsd -config /path/to/tibemsd.conf

# Connect to admin tool
tibemsadmin -server localhost:7222 -user admin -password admin

# Common admin commands:
show queues         # List all queues
show topics         # List all topics
show connections    # Show active connections
show durables       # Show durable subscriptions

create queue myqueue
delete queue myqueue

create topic mytopic
delete topic mytopic

purge queue myqueue  # Remove all messages
```

### Configuration Files

**tibemsd.conf** - Main server configuration:
```conf
# Server settings
listen = tcp://localhost:7222
listen = ssl://localhost:7243

# Security
authorization = enabled
users = users.conf

# Queues and Topics
queues = queues.conf
topics = topics.conf
```

**queues.conf** - Queue definitions:
```conf
> my_queue
  maxbytes = 10MB
  maxmsgs = 10000

> task_queue
  maxbytes = 50MB
  prefetch = 10
```

**topics.conf** - Topic definitions:
```conf
> user.events
  maxbytes = 100MB

> system.alerts
  maxbytes = 50MB
```

## Troubleshooting

### Connection Refused

**Error**: `Unable to connect to TIBCO EMS server`

**Solution**: Ensure TIBCO EMS server is running
```bash
# Check if server is running
ps aux | grep tibemsd

# Start server
tibemsd -config /path/to/tibemsd.conf

# Check server logs
tail -f /path/to/tibco/ems/data/tibemsd.log
```

### Authentication Failed

**Error**: `Authentication failed`

**Solution**: Check credentials in users.conf
```bash
# Edit users.conf
# Add or update user
admin: password=$ADMIN_PASSWORD

# Restart server for changes to take effect
```

### Queue/Topic Not Found

**Error**: `Destination not found`

**Solution**: Create the queue/topic or enable auto-creation
```bash
# In tibemsd.conf
# Enable auto-creation of destinations
create_queue_on_send = true
create_topic_on_send = true

# Or create manually
tibemsadmin
> create queue my_queue
> create topic my_topic
```

### Durable Subscription Issues

**Error**: `Durable subscription already exists`

**Solution**: Unsubscribe or use different durable name
```python
# Use unique durable_name for each subscriber
config = {
    'durable': True,
    'durable_name': 'unique_sub_name_v2'
}

# Or programmatically unsubscribe
# The stop() method automatically unsubscribes
subscriber.stop()
```

### Message Selector Errors

**Error**: `Invalid selector`

**Solution**: Check selector syntax
```python
# Correct syntax
'priority > 5'
"status = 'active'"

# Common mistakes
'priority = 5'  # Use = not ==
"status = active"  # Strings need quotes
'priority > five'  # Must be numeric
```

## Best Practices

1. **Use queues for work distribution** - Reliable task processing
2. **Use topics for broadcasting** - Event notifications
3. **Enable durable subscriptions** - For critical topic messages
4. **Use message selectors** - Filter at server level for efficiency
5. **Set appropriate queue limits** - Prevent memory issues
6. **Monitor queue depths** - Detect processing bottlenecks
7. **Use SSL for production** - Secure communications
8. **Close connections properly** - Always call `stop()`
9. **Handle exceptions** - Implement retry logic for failures
10. **Use meaningful names** - Clear destination naming conventions

## Performance Tips

1. **Batch publishing**: Use `publish_interval` and `batch_size` from base class
2. **Prefetch settings**: Configure in tibemsd.conf for optimal throughput
3. **Message size**: Keep messages small for better performance
4. **Connection pooling**: Reuse publishers when possible
5. **Queue limits**: Set maxbytes and maxmsgs to prevent overflow
6. **Selectors**: Use selectors to reduce client-side filtering
7. **Acknowledge mode**: AUTO_ACKNOWLEDGE is fastest but less reliable

## Example End-to-End

### Queue Example

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time

# Create publisher for queue
publisher = create_publisher('task_pub', {
    'destination': 'tibcoems://queue/tasks',
    'host': 'localhost',
    'port': 7222
})

# Create subscriber for queue
subscriber = create_subscriber('task_sub', {
    'source': 'tibcoems://queue/tasks',
    'host': 'localhost',
    'port': 7222
})
subscriber.start()

# Publish tasks
for i in range(5):
    publisher.publish({'task_id': i, 'action': 'process'})
    time.sleep(0.1)

# Consume tasks
for i in range(5):
    data = subscriber.get_data(block_time=2)
    if data:
        print(f"Processing: {data}")

# Cleanup
publisher.stop()
subscriber.stop()
```

### Topic with Durable Subscription

```python
# Create publisher for topic
publisher = create_publisher('event_pub', {
    'destination': 'tibcoems://topic/critical.events',
    'host': 'localhost',
    'port': 7222
})

# Create durable subscriber
subscriber = create_subscriber('durable_sub', {
    'source': 'tibcoems://topic/critical.events',
    'durable': True,
    'client_id': 'critical_client',
    'durable_name': 'critical_sub'
})
subscriber.start()

# Publish events
publisher.publish({'event': 'system_error', 'severity': 'critical'})

# Receive events
data = subscriber.get_data(block_time=5)
print(f"Received: {data}")

# Stop subscriber (messages sent while offline will be preserved)
subscriber.stop()

# Later, reconnect with same durable_name to receive missed messages
subscriber = create_subscriber('durable_sub', {
    'source': 'tibcoems://topic/critical.events',
    'durable': True,
    'client_id': 'critical_client',
    'durable_name': 'critical_sub'
})
subscriber.start()

# Cleanup
publisher.stop()
subscriber.stop()
```

## Comparison with Other Brokers

| Feature | TIBCO EMS | ActiveMQ | RabbitMQ |
|---------|-----------|----------|----------|
| Protocol | JMS | STOMP | AMQP |
| Queue support | ✅ Yes | ✅ Yes | ✅ Yes |
| Topic support | ✅ Yes | ✅ Yes | ✅ Yes |
| Durable subs | ✅ Yes | ✅ Yes | ✅ Yes |
| Message selectors | ✅ Yes | ✅ Yes | ❌ No |
| Default port | 7222 | 61613 | 5672 |
| SSL default port | 7243 | 61614 | 5671 |
| Python library | tibcoems | stomp.py | pika |
| Enterprise support | ✅ Yes | ✅ Yes | ✅ Yes |

All implementations follow the same simple pattern for easy switching between message brokers!

The TIBCO EMS implementation is production-ready and follows the same pattern as ActiveMQ and RabbitMQ!

## SSL/TLS Security

Enable TLS with `"use_ssl": true`. When enabled, the connector uses an
`ssl://` server URL (the default becomes `ssl://host:port`); you may also
supply an explicit `server_url`.

| Key | Purpose |
| --- | --- |
| `use_ssl` | Enable TLS (`true`/`false`, default `false`) |
| `server_url` | Explicit `ssl://host:port` (optional; derived when omitted) |

```json
{
  "source": "tibcoems://topic/orders",
  "host": "ems.internal",
  "port": 7243,
  "username": "svc_orders",
  "password": "${TIBCO_PASSWORD}",
  "use_ssl": true
}
```

For client-certificate / trust-store options beyond enabling TLS, configure
them in your TIBCO EMS client environment (e.g. the `tibemsd`/client SSL
settings), as these are handled by the EMS client library rather than by
DishtaYantra.

## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.


---

# WebSphere MQ (IBM MQ) DataPublisher and DataSubscriber Setup Guide

## © 2025-2030 Ashutosh Sinha


## Overview

The WebSphere MQ (now IBM MQ) implementation provides publisher and subscriber classes that support both **queue** and **topic** messaging patterns. IBM MQ is an enterprise-grade message broker widely used in enterprise environments for reliable message delivery.

## Files

1. **websphere_datapubsub.py** - Contains `WebSphereMQDataPublisher` and `WebSphereMQDataSubscriber` classes
2. **Updated pubsubfactory.py** - Factory methods now support `websphere://` destinations/sources

## Prerequisites

### Install IBM MQ Server

Download and install IBM MQ from IBM's website:
- IBM MQ Server (default port: 1414)
- IBM MQ Explorer (GUI management tool)

Or use Docker:
```bash
docker run -d \
  --name ibmmq \
  -p 1414:1414 \
  -p 9443:9443 \
  -e LICENSE=accept \
  -e MQ_QMGR_NAME=QM1 \
  ibmcom/mq:latest
```

### Install Python Client Library

```bash
# Install pymqi - Python wrapper for IBM MQ
pip install pymqi

# Note: pymqi requires IBM MQ client libraries
# Download IBM MQ C client from IBM website and install
# Or use the redistributable client package
```

### IBM MQ Client Libraries

```bash
# Linux example - install IBM MQ client
wget https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqadv/...
tar -xzf IBM-MQC-Redist-LinuxX64.tar.gz
export LD_LIBRARY_PATH=/opt/mqm/lib64:$LD_LIBRARY_PATH

# macOS - use homebrew
brew tap ibm-messaging/homebrew-mq
brew install mq-client

# Windows - install using the MSI installer
```

## Queue Manager Concept

IBM MQ uses **Queue Managers** to manage queues and topics:
- Queue Manager: Named instance that manages messaging objects
- Default Queue Manager name: `QM1`
- Each connection requires queue manager name, channel, and host/port

## Queue vs Topic

### Queue (Point-to-Point)
- Messages sent to a queue are consumed by only one subscriber
- Multiple subscribers compete for messages (load balancing)
- URL format: `websphere://queue/queue_name`

### Topic (Publish-Subscribe)
- Messages are broadcast to all subscribers
- Supports durable and non-durable subscriptions
- URL format: `websphere://topic/topic_name`

## Usage

### Publishing to a Queue

Direct point-to-point messaging:

```python
from core.pubsub.pubsubfactory import create_publisher

config = {
    'destination': 'websphere://queue/MY.QUEUE',
    'queue_manager': 'QM1',
    'channel': 'SYSTEM.DEF.SVRCONN',
    'host': 'localhost',
    'port': 1414,
    'username': 'mquser',  # Optional
    'password': 'mqpass'   # Optional
}

publisher = create_publisher('my_publisher', config)

# Publish data
publisher.publish({'event': 'user_login', 'user_id': 123})

# Stop when done
publisher.stop()
```

### Publishing to a Topic

Broadcast messages to all subscribers:

```python
config = {
    'destination': 'websphere://topic/USER/EVENTS',
    'queue_manager': 'QM1',
    'channel': 'SYSTEM.DEF.SVRCONN',
    'host': 'localhost',
    'port': 1414
}

publisher = create_publisher('topic_pub', config)

# Publish to topic
publisher.publish({'event': 'user_login', 'user_id': 123})

publisher.stop()
```

### Subscribing from a Queue

Consume messages from a queue:

```python
from core.pubsub.pubsubfactory import create_subscriber

config = {
    'source': 'websphere://queue/MY.QUEUE',
    'queue_manager': 'QM1',
    'channel': 'SYSTEM.DEF.SVRCONN',
    'host': 'localhost',
    'port': 1414
}

subscriber = create_subscriber('my_subscriber', config)
subscriber.start()

# Get data (blocking with timeout)
data = subscriber.get_data(block_time=5)
if data:
    print(f"Received: {data}")

subscriber.stop()
```

### Subscribing from a Topic

Subscribe to topic with optional durable subscription:

```python
# Non-durable subscription (default)
config = {
    'source': 'websphere://topic/USER/EVENTS',
    'queue_manager': 'QM1',
    'channel': 'SYSTEM.DEF.SVRCONN',
    'host': 'localhost',
    'port': 1414
}

subscriber = create_subscriber('topic_sub', config)
subscriber.start()

# Receive messages
data = subscriber.get_data(block_time=5)
if data:
    print(f"Received: {data}")

subscriber.stop()
```

### Durable Topic Subscription

Durable subscriptions preserve messages when subscriber is offline:

```python
config = {
    'source': 'websphere://topic/CRITICAL/EVENTS',
    'queue_manager': 'QM1',
    'channel': 'SYSTEM.DEF.SVRCONN',
    'host': 'localhost',
    'port': 1414,
    'durable': True,
    'subscription_name': 'CRITICAL_SUB'
}

subscriber = create_subscriber('durable_sub', config)
subscriber.start()

# Messages sent when subscriber is offline are preserved
# Reconnecting with same subscription_name retrieves missed messages

subscriber.stop()
```

## Destination/Source Format

### Queue Format
```
websphere://queue/QUEUE.NAME
```

Examples:
- `websphere://queue/ORDERS`
- `websphere://queue/APP.TASK.QUEUE`
- `websphere://queue/DEV.TEST.QUEUE`

### Topic Format
```
websphere://topic/TOPIC/NAME
```

Examples:
- `websphere://topic/USER/EVENTS`
- `websphere://topic/SYSTEM/ALERTS`
- `websphere://topic/APP/LOGS/ERROR`

**Note**: IBM MQ queue and topic names are typically uppercase.

## Configuration Options

### Publisher Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `destination` | string | Required | WebSphere MQ destination URL |
| `queue_manager` | string | `QM1` | Queue manager name |
| `channel` | string | `SYSTEM.DEF.SVRCONN` | Channel name |
| `host` | string | `localhost` | MQ server host |
| `port` | int | `1414` | MQ server port |
| `conn_info` | string | `host(port)` | Full connection info |
| `username` | string | `None` | Authentication username |
| `password` | string | `None` | Authentication password |
| `use_ssl` | bool | `False` | Enable SSL/TLS |
| `ssl_cipher_spec` | string | `None` | SSL cipher specification |
| `ssl_key_repository` | string | `None` | SSL key repository path |
| `persistence` | int | Persistent | Message persistence |
| `publish_interval` | int | `0` | Batch publishing interval (inherited) |
| `batch_size` | int | `None` | Batch size threshold (inherited) |

### Subscriber Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source` | string | Required | WebSphere MQ source URL |
| `queue_manager` | string | `QM1` | Queue manager name |
| `channel` | string | `SYSTEM.DEF.SVRCONN` | Channel name |
| `host` | string | `localhost` | MQ server host |
| `port` | int | `1414` | MQ server port |
| `conn_info` | string | `host(port)` | Full connection info |
| `username` | string | `None` | Authentication username |
| `password` | string | `None` | Authentication password |
| `use_ssl` | bool | `False` | Enable SSL/TLS |
| `ssl_cipher_spec` | string | `None` | SSL cipher specification |
| `ssl_key_repository` | string | `None` | SSL key repository path |
| `wait_interval` | int | `100` | Wait timeout in milliseconds |
| `subscription_name` | string | `{name}_sub` | Subscription name (topics) |
| `durable` | bool | `False` | Durable subscription (topics) |
| `max_depth` | int | `100000` | Internal queue maximum size (inherited) |

## SSL/TLS Configuration

For secure connections:

```python
config = {
    'destination': 'websphere://queue/SECURE.QUEUE',
    'queue_manager': 'QM1',
    'channel': 'SECURE.CHANNEL',
    'host': 'mq-server.example.com',
    'port': 1414,
    'use_ssl': True,
    'ssl_cipher_spec': 'TLS_RSA_WITH_AES_256_CBC_SHA256',
    'ssl_key_repository': '/path/to/key',
    'username': 'mquser',
    'password': 'mqpass'
}
```

## Connection String Format

Alternative connection info format:

```python
# Single host
'conn_info': 'localhost(1414)'

# Multiple hosts for high availability
'conn_info': 'host1(1414),host2(1414)'

# With channel per connection table (CCDT)
'conn_info': '/path/to/AMQCLCHL.TAB'
```

## Common Patterns

### Pattern 1: Work Queue (Load Balancing)

Multiple workers consuming from the same queue:

```python
# Producer
publisher = create_publisher('task_producer', {
    'destination': 'websphere://queue/TASK.QUEUE',
    'queue_manager': 'QM1'
})

# Multiple workers competing for tasks
for i in range(3):
    worker = create_subscriber(f'worker_{i}', {
        'source': 'websphere://queue/TASK.QUEUE',
        'queue_manager': 'QM1'
    })
    worker.start()

# Tasks are distributed among workers
```

### Pattern 2: Publish-Subscribe (Broadcasting)

Broadcast to all subscribers:

```python
# Publisher
publisher = create_publisher('broadcaster', {
    'destination': 'websphere://topic/NEWS/UPDATES',
    'queue_manager': 'QM1'
})

# Multiple subscribers receive all messages
for i in range(3):
    subscriber = create_subscriber(f'listener_{i}', {
        'source': 'websphere://topic/NEWS/UPDATES',
        'queue_manager': 'QM1'
    })
    subscriber.start()

# All subscribers receive each message
```

### Pattern 3: Durable Subscriptions

Preserve messages for offline subscribers:

```python
# Publisher sends critical events
publisher = create_publisher('event_pub', {
    'destination': 'websphere://topic/CRITICAL/EVENTS',
    'queue_manager': 'QM1'
})

# Durable subscriber - receives messages even when offline
subscriber = create_subscriber('critical_handler', {
    'source': 'websphere://topic/CRITICAL/EVENTS',
    'queue_manager': 'QM1',
    'durable': True,
    'subscription_name': 'CRITICAL_HANDLER_SUB'
})
subscriber.start()

# Messages sent when subscriber is offline are delivered when it reconnects
```

### Pattern 4: Request-Reply

Request-reply messaging pattern:

```python
# Request sender
request_pub = create_publisher('requester', {
    'destination': 'websphere://queue/REQUEST.QUEUE',
    'queue_manager': 'QM1'
})

reply_sub = create_subscriber('reply_receiver', {
    'source': 'websphere://queue/REPLY.QUEUE',
    'queue_manager': 'QM1'
})
reply_sub.start()

# Send request
request_pub.publish({'request': 'get_user', 'user_id': 123})

# Wait for reply
reply = reply_sub.get_data(block_time=10)
```

### Pattern 5: Dead Letter Queue

Handle failed messages:

```python
# Main queue subscriber with error handling
main_sub = create_subscriber('main_worker', {
    'source': 'websphere://queue/MAIN.QUEUE',
    'queue_manager': 'QM1'
})

# Dead letter queue for failed messages
dlq_pub = create_publisher('dlq_handler', {
    'destination': 'websphere://queue/DEAD.LETTER.QUEUE',
    'queue_manager': 'QM1'
})

main_sub.start()

# Process messages
data = main_sub.get_data(block_time=5)
if data:
    try:
        process_message(data)
    except Exception as e:
        # Send to dead letter queue
        dlq_pub.publish({'original': data, 'error': str(e)})
```

## Monitoring

### Get Publisher Statistics

```python
stats = publisher.details()
print(f"Name: {stats['name']}")
print(f"Queue Manager: {config['queue_manager']}")
print(f"Published: {stats['publish_count']} messages")
print(f"Last publish: {stats['last_publish']}")
print(f"Queue depth: {stats['queue_depth']}")
```

### Get Subscriber Statistics

```python
stats = subscriber.details()
print(f"Name: {stats['name']}")
print(f"Queue Manager: {config['queue_manager']}")
print(f"Received: {stats['receive_count']} messages")
print(f"Last receive: {stats['last_receive']}")
print(f"Queue size: {stats['current_depth']}")
```

## IBM MQ Administration

### Command Line Tools (runmqsc)

```bash
# Start queue manager
strmqm QM1

# Enter interactive mode
runmqsc QM1

# Common commands:
DEFINE QLOCAL(MY.QUEUE) MAXDEPTH(10000)
DISPLAY QLOCAL(MY.QUEUE)
DELETE QLOCAL(MY.QUEUE)

DEFINE TOPIC(MY.TOPIC) TOPICSTR('APP/EVENTS')
DISPLAY TOPIC(MY.TOPIC)

DEFINE CHANNEL(MY.CHANNEL) CHLTYPE(SVRCONN)
DISPLAY CHANNEL(MY.CHANNEL)

# Display queue depth
DISPLAY QLOCAL(MY.QUEUE) CURDEPTH

# Clear queue
CLEAR QLOCAL(MY.QUEUE)

# End interactive mode
END
```

### Command Line Tools (dspmq, dmpmqmsg)

```bash
# Display queue managers
dspmq

# Display queue depth
echo "DISPLAY QLOCAL(MY.QUEUE) CURDEPTH" | runmqsc QM1

# Browse messages
dmpmqmsg -m QM1 -q MY.QUEUE -b

# Stop queue manager
endmqm QM1

# Force stop (if needed)
endmqm -i QM1
```

### IBM MQ Explorer

IBM MQ Explorer is a GUI tool for managing MQ objects:
- Create/delete queue managers
- Create/delete queues and topics
- Browse messages
- Monitor performance
- Configure security

## Troubleshooting

### Connection Refused

**Error**: `MQRC_CONNECTION_REFUSED (2059)`

**Solution**: Check if queue manager is running and listener is started
```bash
# Display queue manager status
dspmqm QM1

# Start queue manager
strmqm QM1

# Start listener
runmqsc QM1
START LISTENER(SYSTEM.DEFAULT.LISTENER.TCP)
END
```

### Queue Manager Not Found

**Error**: `MQRC_Q_MGR_NOT_AVAILABLE (2059)` or `MQRC_Q_MGR_NAME_ERROR (2058)`

**Solution**: Verify queue manager name
```bash
# List all queue managers
dspmq

# Use correct name in config
config = {
    'queue_manager': 'QM1'  # Use actual name from dspmq
}
```

### Authentication Failed

**Error**: `MQRC_NOT_AUTHORIZED (2035)`

**Solution**: Configure authentication and authorization
```bash
runmqsc QM1

# Allow unauthenticated access (development only)
ALTER QMGR CHLAUTH(DISABLED)
ALTER QMGR CONNAUTH('')
REFRESH SECURITY TYPE(CONNAUTH)

# Or configure proper authentication
DEFINE AUTHINFO(MY.AUTHINFO) AUTHTYPE(IDPWOS)
ALTER QMGR CONNAUTH(MY.AUTHINFO)
REFRESH SECURITY TYPE(CONNAUTH)

# Grant permissions
SET AUTHREC OBJTYPE(QMGR) PRINCIPAL('mquser') AUTHADD(CONNECT,INQ)
SET AUTHREC OBJTYPE(QUEUE) PROFILE(MY.QUEUE) PRINCIPAL('mquser') AUTHADD(GET,PUT,BROWSE,INQ)

END
```

### Queue Not Found

**Error**: `MQRC_UNKNOWN_OBJECT_NAME (2085)`

**Solution**: Create the queue
```bash
runmqsc QM1
DEFINE QLOCAL(MY.QUEUE) MAXDEPTH(10000)
END
```

### SSL/TLS Errors

**Error**: SSL handshake failed

**Solution**: Check SSL configuration
```python
# Verify cipher spec matches server
'ssl_cipher_spec': 'TLS_RSA_WITH_AES_256_CBC_SHA256'

# Verify key repository path
'ssl_key_repository': '/path/to/keystore'  # Without .kdb extension

# Ensure certificates are properly installed
```

## Best Practices

1. **Use meaningful queue names** - Follow IBM MQ naming conventions (uppercase, dots)
2. **Enable message persistence** - For important messages (done by default)
3. **Configure queue depth limits** - Prevent memory issues
4. **Use durable subscriptions** - For critical topic messages
5. **Implement error handling** - Dead letter queues for failed messages
6. **Monitor queue depths** - Detect processing bottlenecks
7. **Use SSL in production** - Secure communications
8. **Close connections properly** - Always call `stop()`
9. **Use shared queues** - For scalability with multiple queue managers
10. **Plan for high availability** - Multi-instance queue managers or MQ clusters

## Performance Tips

1. **Batch publishing**: Use `publish_interval` and `batch_size` from base class
2. **Connection pooling**: Reuse queue manager connections
3. **Message size**: Keep messages small for better throughput
4. **Persistent vs non-persistent**: Non-persistent is faster but less reliable
5. **Shared queues**: Distribute load across multiple servers
6. **MQ clustering**: For high availability and scalability
7. **Tune MAXDEPTH**: Set appropriate queue depth limits

## Example End-to-End

### Queue Example

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time

# Create publisher for queue
publisher = create_publisher('task_pub', {
    'destination': 'websphere://queue/TASK.QUEUE',
    'queue_manager': 'QM1',
    'channel': 'SYSTEM.DEF.SVRCONN',
    'host': 'localhost',
    'port': 1414
})

# Create subscriber for queue
subscriber = create_subscriber('task_sub', {
    'source': 'websphere://queue/TASK.QUEUE',
    'queue_manager': 'QM1',
    'channel': 'SYSTEM.DEF.SVRCONN',
    'host': 'localhost',
    'port': 1414
})
subscriber.start()

# Publish tasks
for i in range(5):
    publisher.publish({'task_id': i, 'action': 'process'})
    time.sleep(0.1)

# Consume tasks
for i in range(5):
    data = subscriber.get_data(block_time=2)
    if data:
        print(f"Processing: {data}")

# Cleanup
publisher.stop()
subscriber.stop()
```

### Topic with Durable Subscription

```python
# Create publisher for topic
publisher = create_publisher('event_pub', {
    'destination': 'websphere://topic/CRITICAL/EVENTS',
    'queue_manager': 'QM1'
})

# Create durable subscriber
subscriber = create_subscriber('durable_sub', {
    'source': 'websphere://topic/CRITICAL/EVENTS',
    'queue_manager': 'QM1',
    'durable': True,
    'subscription_name': 'CRITICAL_SUB'
})
subscriber.start()

# Publish events
publisher.publish({'event': 'system_error', 'severity': 'critical'})

# Receive events
data = subscriber.get_data(block_time=5)
print(f"Received: {data}")

# Stop subscriber (messages sent while offline will be preserved)
subscriber.stop()

# Later, reconnect to receive missed messages
subscriber = create_subscriber('durable_sub', {
    'source': 'websphere://topic/CRITICAL/EVENTS',
    'queue_manager': 'QM1',
    'durable': True,
    'subscription_name': 'CRITICAL_SUB'
})
subscriber.start()

# Cleanup
publisher.stop()
subscriber.stop()
```

## Comparison with Other Brokers

| Feature | IBM MQ | ActiveMQ | RabbitMQ | TIBCO EMS |
|---------|--------|----------|----------|-----------|
| Protocol | Proprietary | STOMP | AMQP | JMS |
| Queue support | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| Topic support | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| Durable subs | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| Message selectors | ❌ No | ✅ Yes | ❌ No | ✅ Yes |
| Default port | 1414 | 61613 | 5672 | 7222 |
| Python library | pymqi | stomp.py | pika | tibcoems |
| Enterprise support | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| HA/Clustering | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |

All implementations follow the same simple pattern for easy switching between message brokers!

The WebSphere MQ implementation is production-ready and follows the same pattern as ActiveMQ, RabbitMQ, and TIBCO EMS!

## SSL/TLS Security

IBM MQ has first-class TLS support. Enable it with `"use_ssl": true` and
supply the cipher specification and key repository expected by your queue
manager.

| Key | Purpose |
| --- | --- |
| `use_ssl` | Enable TLS (`true`/`false`, default `false`) |
| `ssl_cipher_spec` | MQ CipherSpec, e.g. `TLS_RSA_WITH_AES_256_CBC_SHA256` |
| `ssl_key_repository` | Path to the client key repository (without the `.kdb` suffix) |

```json
{
  "destination": "websphere://queue/ORDERS",
  "queue_manager_name": "QM1",
  "channel": "SVC.SVRCONN",
  "connection_info": "mq.internal(1414)",
  "use_ssl": true,
  "ssl_cipher_spec": "TLS_RSA_WITH_AES_256_CBC_SHA256",
  "ssl_key_repository": "/var/mqm/ssl/client"
}
```

## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.


---

