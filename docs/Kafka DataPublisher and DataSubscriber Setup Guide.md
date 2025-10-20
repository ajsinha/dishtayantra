# Kafka DataPublisher and DataSubscriber Setup Guide

## © 2025-2030 Ashutosh Sinha

## Overview

The Kafka implementation provides publisher and subscriber classes for Apache Kafka, a distributed streaming platform. Unlike traditional message brokers, Kafka is designed for high-throughput, fault-tolerant, and scalable event streaming. Kafka uses a **topic-based** publish-subscribe model with support for consumer groups and partitioning.

## Files

1. **kafka_datapubsub.py** - Contains `KafkaDataPublisher` and `KafkaDataSubscriber` classes
2. **pubsubfactory.py** - Factory methods support `kafka://topic/` destinations/sources

## Prerequisites

### Install Apache Kafka

Using Docker (recommended for development):
```bash
# Start Kafka with Zookeeper using Docker Compose
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

Or install manually:
```bash
# Download Kafka
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka_2.13-3.6.0.tgz
cd kafka_2.13-3.6.0

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka (in another terminal)
bin/kafka-server-start.sh config/server.properties
```

### Install Python Client

```bash
pip install kafka-python
```

## Kafka Concepts

### Topics
- **Topic**: A category or feed name to which records are published
- Messages are organized into topics (like channels)
- Topics are partitioned for scalability
- URL format: `kafka://topic/topic_name`

### Partitions
- Topics are divided into partitions for parallel processing
- Each partition is an ordered, immutable sequence of records
- Messages within a partition are ordered
- Different partitions can be processed by different consumers

### Consumer Groups
- Consumers can be organized into consumer groups
- Each partition is consumed by exactly one consumer in a group
- Enables parallel processing and load balancing
- Default group_id: `{subscriber_name}_group`

### Offsets
- Each message has an offset (position) in its partition
- Consumers track their offset to resume from where they left off
- Kafka stores committed offsets for consumer groups

## Usage

### Publishing to a Topic

Basic topic publishing:

```python
from core.pubsub.pubsubfactory import create_publisher

config = {
    'destination': 'kafka://topic/user_events',
    'bootstrap_servers': ['localhost:9092']
}

publisher = create_publisher('event_publisher', config)

# Publish data
publisher.publish({'event': 'user_login', 'user_id': 123, 'timestamp': '2025-01-15T10:30:00'})

# Stop when done
publisher.stop()
```

### Publishing with Custom Configuration

Advanced producer settings:

```python
config = {
    'destination': 'kafka://topic/user_events',
    'bootstrap_servers': ['localhost:9092', 'localhost:9093', 'localhost:9094'],
    'producer_config': {
        'acks': 'all',              # Wait for all replicas
        'retries': 3,               # Retry on failure
        'batch_size': 16384,        # Batch size in bytes
        'linger_ms': 10,            # Wait time for batching
        'compression_type': 'gzip', # Compress messages
        'max_in_flight_requests_per_connection': 5
    }
}

publisher = create_publisher('reliable_publisher', config)
publisher.publish({'event': 'critical_transaction', 'amount': 10000})
publisher.stop()
```

### Subscribing from a Topic

Basic topic subscription:

```python
from core.pubsub.pubsubfactory import create_subscriber

config = {
    'source': 'kafka://topic/user_events',
    'bootstrap_servers': ['localhost:9092'],
    'group_id': 'user_event_processors'
}

subscriber = create_subscriber('event_subscriber', config)
subscriber.start()

# Get data (blocking with timeout)
data = subscriber.get_data(block_time=5)
if data:
    print(f"Received: {data}")

subscriber.stop()
```

### Subscribing with Custom Configuration

Advanced consumer settings:

```python
config = {
    'source': 'kafka://topic/user_events',
    'bootstrap_servers': ['localhost:9092'],
    'group_id': 'analytics_processors',
    'consumer_config': {
        'auto_offset_reset': 'earliest',    # Start from beginning
        'enable_auto_commit': True,          # Auto-commit offsets
        'auto_commit_interval_ms': 5000,     # Commit every 5 seconds
        'max_poll_records': 100,             # Max records per poll
        'session_timeout_ms': 30000,         # Session timeout
        'heartbeat_interval_ms': 10000       # Heartbeat interval
    }
}

subscriber = create_subscriber('analytics_sub', config)
subscriber.start()

# Process messages
while True:
    data = subscriber.get_data(block_time=1)
    if data:
        process_event(data)
```

## Destination/Source Format

### Topic Format (Only)
```
kafka://topic/topic_name
```

**Note**: Kafka only supports topics, not queues. However, you can achieve queue-like behavior using consumer groups.

Examples:
- `kafka://topic/user_events`
- `kafka://topic/order_processing`
- `kafka://topic/system_logs`
- `kafka://topic/sensor_data`

## Configuration Options

### Publisher Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `destination` | string | Required | Kafka topic URL |
| `bootstrap_servers` | list | `['localhost:9092']` | Kafka broker addresses |
| `producer_config` | dict | `{}` | Additional KafkaProducer options |
| `publish_interval` | int | `0` | Batch publishing interval (inherited) |
| `batch_size` | int | `None` | Batch size threshold (inherited) |

### Common Producer Config Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `acks` | string/int | `1` | Acknowledgment level (0, 1, 'all') |
| `retries` | int | `0` | Number of retries on failure |
| `batch_size` | int | `16384` | Batch size in bytes |
| `linger_ms` | int | `0` | Wait time for batching (ms) |
| `compression_type` | string | `None` | Compression (gzip, snappy, lz4, zstd) |
| `max_in_flight_requests_per_connection` | int | `5` | Max unacknowledged requests |
| `buffer_memory` | int | `33554432` | Total memory for buffering |

### Subscriber Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source` | string | Required | Kafka topic URL |
| `bootstrap_servers` | list | `['localhost:9092']` | Kafka broker addresses |
| `group_id` | string | `{name}_group` | Consumer group ID |
| `consumer_config` | dict | `{}` | Additional KafkaConsumer options |
| `max_depth` | int | `100000` | Internal queue maximum size (inherited) |

### Common Consumer Config Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `auto_offset_reset` | string | `latest` | Where to start (earliest, latest) |
| `enable_auto_commit` | bool | `True` | Auto-commit offsets |
| `auto_commit_interval_ms` | int | `5000` | Auto-commit interval |
| `max_poll_records` | int | `500` | Max records per poll |
| `session_timeout_ms` | int | `10000` | Consumer session timeout |
| `heartbeat_interval_ms` | int | `3000` | Heartbeat interval |
| `fetch_min_bytes` | int | `1` | Min bytes per fetch request |
| `fetch_max_wait_ms` | int | `500` | Max wait for fetch request |
| `max_partition_fetch_bytes` | int | `1048576` | Max bytes per partition |

## Common Patterns

### Pattern 1: Pub-Sub (Broadcasting)

All consumers in different groups receive all messages:

```python
# Publisher
publisher = create_publisher('event_pub', {
    'destination': 'kafka://topic/system_events',
    'bootstrap_servers': ['localhost:9092']
})

# Consumer Group 1 - Logger
logger_sub = create_subscriber('logger', {
    'source': 'kafka://topic/system_events',
    'group_id': 'logging_service'
})

# Consumer Group 2 - Analytics
analytics_sub = create_subscriber('analytics', {
    'source': 'kafka://topic/system_events',
    'group_id': 'analytics_service'
})

# Consumer Group 3 - Alerting
alert_sub = create_subscriber('alerting', {
    'source': 'kafka://topic/system_events',
    'group_id': 'alert_service'
})

logger_sub.start()
analytics_sub.start()
alert_sub.start()

# Publish event - all three groups receive it
publisher.publish({'event': 'user_login', 'user_id': 123})
```

### Pattern 2: Load Balancing (Consumer Group)

Multiple consumers in same group share the load:

```python
# Publisher
publisher = create_publisher('task_pub', {
    'destination': 'kafka://topic/tasks',
    'bootstrap_servers': ['localhost:9092']
})

# Multiple workers in same consumer group
# Each worker processes different partitions
workers = []
for i in range(3):
    worker = create_subscriber(f'worker_{i}', {
        'source': 'kafka://topic/tasks',
        'group_id': 'task_processors'  # Same group
    })
    worker.start()
    workers.append(worker)

# Tasks are distributed among workers
for i in range(100):
    publisher.publish({'task_id': i, 'action': 'process'})
```

### Pattern 3: Stream Processing Pipeline

Chain multiple topics for processing stages:

```python
# Stage 1: Raw data ingestion
raw_pub = create_publisher('raw_ingest', {
    'destination': 'kafka://topic/raw_events'
})

# Stage 1 Consumer / Stage 2 Producer
enriched_pub = create_publisher('enriched_pub', {
    'destination': 'kafka://topic/enriched_events'
})

raw_sub = create_subscriber('enricher', {
    'source': 'kafka://topic/raw_events',
    'group_id': 'enrichment_service'
})
raw_sub.start()

# Enrich and forward
while True:
    raw_data = raw_sub.get_data(block_time=1)
    if raw_data:
        enriched_data = enrich(raw_data)
        enriched_pub.publish(enriched_data)

# Stage 3: Final processing
final_sub = create_subscriber('processor', {
    'source': 'kafka://topic/enriched_events',
    'group_id': 'processing_service'
})
final_sub.start()
```

### Pattern 4: Event Sourcing

Store all events as immutable log:

```python
# Event store
event_pub = create_publisher('event_store', {
    'destination': 'kafka://topic/event_store',
    'producer_config': {
        'acks': 'all',              # Ensure durability
        'compression_type': 'gzip'
    }
})

# Store all domain events
event_pub.publish({
    'event_type': 'OrderCreated',
    'order_id': 'ORD-123',
    'timestamp': '2025-01-15T10:30:00',
    'data': {'customer_id': 456, 'total': 99.99}
})

# Replay events from beginning
replay_sub = create_subscriber('replayer', {
    'source': 'kafka://topic/event_store',
    'group_id': 'replay_service',
    'consumer_config': {
        'auto_offset_reset': 'earliest'  # Start from beginning
    }
})
replay_sub.start()
```

### Pattern 5: Change Data Capture (CDC)

Capture database changes:

```python
# CDC Publisher
cdc_pub = create_publisher('cdc', {
    'destination': 'kafka://topic/db_changes',
    'producer_config': {
        'acks': 'all',
        'compression_type': 'snappy'
    }
})

# Publish database changes
cdc_pub.publish({
    'operation': 'INSERT',
    'table': 'users',
    'record': {'id': 123, 'name': 'John', 'email': 'john@example.com'},
    'timestamp': '2025-01-15T10:30:00'
})

# Consumers can sync to other systems
sync_sub = create_subscriber('sync', {
    'source': 'kafka://topic/db_changes',
    'group_id': 'sync_service'
})
sync_sub.start()
```

## Consumer Groups Explained

### Single Consumer Group (Load Balancing)
```
Topic: user_events (3 partitions)
Consumer Group: processors

Partition 0 → Consumer A
Partition 1 → Consumer B
Partition 2 → Consumer C

Each message processed by ONE consumer
```

### Multiple Consumer Groups (Broadcasting)
```
Topic: user_events (3 partitions)

Group 1: logging
  Partition 0 → Logger A
  Partition 1 → Logger A
  Partition 2 → Logger A

Group 2: analytics
  Partition 0 → Analytics A
  Partition 1 → Analytics B
  Partition 2 → Analytics B

Each message processed by EACH group
```

## Offset Management

### Auto Offset Reset

```python
# Start from earliest message (beginning of topic)
config = {
    'source': 'kafka://topic/events',
    'consumer_config': {
        'auto_offset_reset': 'earliest'
    }
}

# Start from latest message (skip existing messages)
config = {
    'source': 'kafka://topic/events',
    'consumer_config': {
        'auto_offset_reset': 'latest'
    }
}
```

### Manual Offset Commit

```python
config = {
    'source': 'kafka://topic/events',
    'consumer_config': {
        'enable_auto_commit': False  # Manual commit
    }
}

# Note: Current implementation uses auto-commit
# For manual commit, you would need to extend the subscriber
```

## Monitoring

### Get Publisher Statistics

```python
stats = publisher.details()
print(f"Name: {stats['name']}")
print(f"Topic: {stats['destination']}")
print(f"Published: {stats['publish_count']} messages")
print(f"Last publish: {stats['last_publish']}")
print(f"Queue depth: {stats['queue_depth']}")
```

### Get Subscriber Statistics

```python
stats = subscriber.details()
print(f"Name: {stats['name']}")
print(f"Topic: {stats['source']}")
print(f"Consumer Group: {config['group_id']}")
print(f"Received: {stats['receive_count']} messages")
print(f"Last receive: {stats['last_receive']}")
print(f"Internal queue size: {stats['current_depth']}")
```

## Kafka Administration

### Command Line Tools

```bash
# List topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Create topic
kafka-topics.sh --create \
  --topic user_events \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

# Describe topic
kafka-topics.sh --describe \
  --topic user_events \
  --bootstrap-server localhost:9092

# Delete topic
kafka-topics.sh --delete \
  --topic user_events \
  --bootstrap-server localhost:9092

# List consumer groups
kafka-consumer-groups.sh --list \
  --bootstrap-server localhost:9092

# Describe consumer group
kafka-consumer-groups.sh --describe \
  --group task_processors \
  --bootstrap-server localhost:9092

# Reset consumer group offset
kafka-consumer-groups.sh --reset-offsets \
  --group task_processors \
  --topic user_events \
  --to-earliest \
  --execute \
  --bootstrap-server localhost:9092

# Produce messages (testing)
kafka-console-producer.sh \
  --topic user_events \
  --bootstrap-server localhost:9092

# Consume messages (testing)
kafka-console-consumer.sh \
  --topic user_events \
  --from-beginning \
  --bootstrap-server localhost:9092
```

### Python Admin API

```python
from kafka.admin import KafkaAdminClient, NewTopic

# Create admin client
admin = KafkaAdminClient(bootstrap_servers=['localhost:9092'])

# Create topic
topic = NewTopic(name='new_events', num_partitions=3, replication_factor=1)
admin.create_topics([topic])

# List topics
topics = admin.list_topics()
print(topics)

# Delete topic
admin.delete_topics(['old_events'])

admin.close()
```

## Troubleshooting

### Connection Refused

**Error**: `NoBrokersAvailable` or connection timeout

**Solution**: Check if Kafka is running
```bash
# Check Kafka process
ps aux | grep kafka

# Start Kafka
bin/kafka-server-start.sh config/server.properties

# Check Docker containers
docker ps
```

### Topic Not Found

**Error**: `UnknownTopicOrPartitionError`

**Solution**: Create the topic or enable auto-creation
```bash
# Create topic manually
kafka-topics.sh --create \
  --topic my_topic \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

# Or enable auto-creation in server.properties
auto.create.topics.enable=true
```

### Consumer Not Receiving Messages

**Possible causes**:
- Consumer started after messages were published
- Consumer group already consumed the messages
- Wrong topic name
- Messages expired due to retention policy

**Solution**:
```python
# Reset to beginning
config = {
    'source': 'kafka://topic/events',
    'consumer_config': {
        'auto_offset_reset': 'earliest'
    }
}

# Or reset consumer group offset
kafka-consumer-groups.sh --reset-offsets \
  --group my_group \
  --topic my_topic \
  --to-earliest \
  --execute \
  --bootstrap-server localhost:9092
```

### Consumer Lag

**Error**: Consumer falling behind producers

**Solution**: Scale consumers or optimize processing
```python
# Add more consumers to the same group
for i in range(5):  # Increase from 3 to 5
    worker = create_subscriber(f'worker_{i}', {
        'source': 'kafka://topic/tasks',
        'group_id': 'workers'
    })
    worker.start()

# Or optimize polling
config = {
    'source': 'kafka://topic/tasks',
    'consumer_config': {
        'max_poll_records': 1000,  # Process more per poll
        'fetch_min_bytes': 10000   # Fetch more data
    }
}
```

### Serialization Errors

**Error**: JSON decode error

**Solution**: Ensure data is valid JSON
```python
# Publisher - ensure JSON serializable
data = {
    'timestamp': datetime.now().isoformat(),  # Convert datetime
    'user_id': 123,
    'metadata': json.dumps(complex_object)     # Pre-serialize complex objects
}
publisher.publish(data)
```

## Best Practices

1. **Use meaningful topic names** - Clear, hierarchical naming (e.g., `orders.created`, `users.updated`)
2. **Plan partition count** - Based on throughput and parallelism needs
3. **Set appropriate retention** - Balance storage and replay requirements
4. **Use compression** - Save bandwidth and storage (gzip, snappy)
5. **Handle failures gracefully** - Implement retry logic and error handling
6. **Monitor consumer lag** - Detect processing bottlenecks early
7. **Use consumer groups wisely** - Different groups for different purposes
8. **Commit offsets regularly** - Prevent duplicate processing
9. **Design for idempotency** - Messages may be redelivered
10. **Close connections properly** - Always call `stop()`

## Performance Tips

1. **Batch publishing**: Use `publish_interval` and `batch_size` from base class
2. **Producer batching**: Set `linger_ms` and `batch_size` in producer_config
3. **Compression**: Use `snappy` for balance of speed and compression
4. **Partition strategy**: More partitions = more parallelism
5. **Consumer fetch size**: Tune `fetch_min_bytes` and `max_poll_records`
6. **Replication factor**: Balance durability and performance
7. **Acks setting**: Use `acks=1` for best performance, `acks='all'` for durability
8. **Connection pooling**: Reuse producers when possible
9. **Page cache**: Kafka uses OS page cache effectively
10. **Network tuning**: Ensure network bandwidth is adequate

## Complete Examples

### Example 1: Simple Event Processing

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
import json

# Create publisher
publisher = create_publisher('event_publisher', {
    'destination': 'kafka://topic/user_events',
    'bootstrap_servers': ['localhost:9092']
})

# Create subscriber
subscriber = create_subscriber('event_processor', {
    'source': 'kafka://topic/user_events',
    'bootstrap_servers': ['localhost:9092'],
    'group_id': 'processors'
})
subscriber.start()

# Publish events
events = [
    {'event': 'user_login', 'user_id': 1, 'timestamp': '2025-01-15T10:00:00'},
    {'event': 'page_view', 'user_id': 1, 'page': '/home'},
    {'event': 'user_logout', 'user_id': 1, 'timestamp': '2025-01-15T10:30:00'}
]

for event in events:
    publisher.publish(event)
    print(f"Published: {event}")
    time.sleep(0.5)

# Process events
for i in range(len(events)):
    data = subscriber.get_data(block_time=5)
    if data:
        print(f"Processed: {data}")

# Cleanup
publisher.stop()
subscriber.stop()
```

### Example 2: Multi-Stage Processing Pipeline

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
import threading

# Stage 1: Raw data ingestion
raw_publisher = create_publisher('raw_ingest', {
    'destination': 'kafka://topic/raw_orders',
    'bootstrap_servers': ['localhost:9092']
})

# Stage 2: Enrichment service
enriched_publisher = create_publisher('enriched_pub', {
    'destination': 'kafka://topic/enriched_orders',
    'bootstrap_servers': ['localhost:9092']
})

raw_subscriber = create_subscriber('enricher', {
    'source': 'kafka://topic/raw_orders',
    'bootstrap_servers': ['localhost:9092'],
    'group_id': 'enrichment_service'
})

# Stage 3: Processing service
enriched_subscriber = create_subscriber('processor', {
    'source': 'kafka://topic/enriched_orders',
    'bootstrap_servers': ['localhost:9092'],
    'group_id': 'processing_service'
})

# Enrichment worker
def enrichment_worker():
    raw_subscriber.start()
    while True:
        raw_data = raw_subscriber.get_data(block_time=1)
        if raw_data:
            # Enrich data
            enriched = {
                **raw_data,
                'customer_name': f"Customer {raw_data['customer_id']}",
                'enriched_at': time.time()
            }
            enriched_publisher.publish(enriched)
            print(f"Enriched: {enriched}")

# Processing worker
def processing_worker():
    enriched_subscriber.start()
    while True:
        enriched_data = enriched_subscriber.get_data(block_time=1)
        if enriched_data:
            # Process data
            print(f"Processed order: {enriched_data}")

# Start workers
enrichment_thread = threading.Thread(target=enrichment_worker, daemon=True)
processing_thread = threading.Thread(target=processing_worker, daemon=True)

enrichment_thread.start()
processing_thread.start()

# Publish raw orders
orders = [
    {'order_id': 'ORD-001', 'customer_id': 123, 'amount': 99.99},
    {'order_id': 'ORD-002', 'customer_id': 456, 'amount': 149.99},
    {'order_id': 'ORD-003', 'customer_id': 789, 'amount': 79.99}
]

for order in orders:
    raw_publisher.publish(order)
    print(f"Published raw order: {order}")
    time.sleep(1)

# Wait for processing
time.sleep(5)

# Cleanup
raw_publisher.stop()
enriched_publisher.stop()
raw_subscriber.stop()
enriched_subscriber.stop()
```

### Example 3: Load Balanced Processing

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
import threading

# Create publisher
publisher = create_publisher('task_pub', {
    'destination': 'kafka://topic/tasks',
    'bootstrap_servers': ['localhost:9092']
})

# Create multiple workers in same consumer group
def worker_func(worker_id):
    subscriber = create_subscriber(f'worker_{worker_id}', {
        'source': 'kafka://topic/tasks',
        'bootstrap_servers': ['localhost:9092'],
        'group_id': 'task_workers'  # Same group = load balancing
    })
    subscriber.start()
    
    while True:
        data = subscriber.get_data(block_time=1)
        if data:
            print(f"Worker {worker_id} processing: {data}")
            time.sleep(0.5)  # Simulate processing

# Start 3 workers
threads = []
for i in range(3):
    thread = threading.Thread(target=worker_func, args=(i,), daemon=True)
    thread.start()
    threads.append(thread)

# Publish tasks
for i in range(20):
    task = {'task_id': i, 'action': 'process', 'data': f'task_{i}'}
    publisher.publish(task)
    print(f"Published task {i}")
    time.sleep(0.1)

# Wait for processing
time.sleep(10)

# Cleanup
publisher.stop()
```

### Example 4: Multi-Group Broadcasting

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
import threading

# Create publisher
publisher = create_publisher('event_pub', {
    'destination': 'kafka://topic/system_events',
    'bootstrap_servers': ['localhost:9092']
})

# Logger service (Group 1)
def logger_service():
    subscriber = create_subscriber('logger', {
        'source': 'kafka://topic/system_events',
        'bootstrap_servers': ['localhost:9092'],
        'group_id': 'logging_service'
    })
    subscriber.start()
    
    while True:
        data = subscriber.get_data(block_time=1)
        if data:
            print(f"[LOGGER] {data}")

# Analytics service (Group 2)
def analytics_service():
    subscriber = create_subscriber('analytics', {
        'source': 'kafka://topic/system_events',
        'bootstrap_servers': ['localhost:9092'],
        'group_id': 'analytics_service'
    })
    subscriber.start()
    
    while True:
        data = subscriber.get_data(block_time=1)
        if data:
            print(f"[ANALYTICS] Processing: {data['event']}")

# Alerting service (Group 3)
def alerting_service():
    subscriber = create_subscriber('alerting', {
        'source': 'kafka://topic/system_events',
        'bootstrap_servers': ['localhost:9092'],
        'group_id': 'alert_service'
    })
    subscriber.start()
    
    while True:
        data = subscriber.get_data(block_time=1)
        if data:
            if data.get('severity') == 'critical':
                print(f"[ALERT] Critical event: {data}")

# Start all services
threading.Thread(target=logger_service, daemon=True).start()
threading.Thread(target=analytics_service, daemon=True).start()
threading.Thread(target=alerting_service, daemon=True).start()

time.sleep(1)  # Wait for services to start

# Publish events
events = [
    {'event': 'user_login', 'user_id': 123, 'severity': 'info'},
    {'event': 'payment_failed', 'user_id': 456, 'severity': 'warning'},
    {'event': 'system_down', 'severity': 'critical'},
    {'event': 'user_logout', 'user_id': 123, 'severity': 'info'}
]

for event in events:
    publisher.publish(event)
    print(f"Published: {event}")
    time.sleep(1)

# Wait for processing
time.sleep(5)

# Cleanup
publisher.stop()
```

## Comparison with Other Brokers

| Feature | Kafka | ActiveMQ | RabbitMQ | TIBCO EMS | IBM MQ |
|---------|-------|----------|----------|-----------|--------|
| Model | Topic-based | Queue/Topic | Queue/Topic | Queue/Topic | Queue/Topic |
| Messaging | Pub-Sub | Pub-Sub, P2P | Pub-Sub, P2P | Pub-Sub, P2P | Pub-Sub, P2P |
| Persistence | Always | Optional | Optional | Optional | Optional |
| Ordering | Per partition | Per queue | Per queue | Per queue | Per queue |
| Scalability | Excellent | Good | Good | Good | Good |
| Throughput | Very High | Medium | Medium-High | Medium-High | Medium-High |
| Consumer groups | ✅ Yes | ❌ No | ❌ No | ❌ No | ❌ No |
| Replay messages | ✅ Yes | ❌ No | ❌ No | Limited | Limited |
| Default port | 9092 | 61613 | 5672 | 7222 | 1414 |
| Python library | kafka-python | stomp.py | pika | tibcoems | pymqi |
| Best for | Event streaming | Traditional MQ | Traditional MQ | Enterprise MQ | Enterprise MQ |

## When to Use Kafka

**Use Kafka when you need:**
- High-throughput event streaming
- Message replay capability
- Event sourcing / CQRS patterns
- Real-time data pipelines
- Log aggregation
- Stream processing
- Microservices communication with events

**Consider other brokers when:**
- You need traditional queue semantics
- Simple request-reply patterns
- Low message volume
- Simpler operations and maintenance

The Kafka implementation provides a powerful foundation for event-driven architectures and real-time data streaming!


## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.