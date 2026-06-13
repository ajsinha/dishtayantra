# In-Memory DataPublisher and DataSubscriber Setup Guide

## © 2025-2030 Ashutosh Sinha

## Overview

The In-Memory implementation provides publisher and subscriber classes for in-process messaging using Python's native data structures. Messages are stored in memory queues and topics without any external dependencies. This pattern is ideal for development, testing, microservices within the same process, fast inter-thread communication, and scenarios where persistence is not required.

## Files

1. **inmemory_datapubsub.py** - Contains `InMemoryDataPublisher` and `InMemoryDataSubscriber` classes
2. **inmemorypubsub.py** - Singleton `InMemoryPubSub` manager for queues and topics
3. **pubsubfactory.py** - Factory methods support `mem://queue/` and `mem://topic/` destinations/sources

## Prerequisites

**None!** The in-memory implementation uses only Python standard library:
- `queue` - Thread-safe FIFO queues
- `threading` - Thread synchronization
- Built-in data structures

No installation, no configuration files, no external services required!

## In-Memory Messaging Concepts

### How It Works

**Architecture:**
- **Singleton Manager**: Single `InMemoryPubSub` instance manages all queues and topics
- **Thread-Safe**: Uses locks and thread-safe queues
- **In-Process**: All communication within same Python process
- **Memory-Only**: No persistence, messages lost on process exit

**Queues (Point-to-Point):**
- Each queue is a `queue.Queue` object
- Multiple subscribers compete for messages (load balancing)
- Each message consumed by exactly one subscriber
- FIFO ordering within queue
- URL format: `mem://queue/queue_name`

**Topics (Publish-Subscribe):**
- Each topic maintains list of subscriber queues
- Messages broadcast to all subscribers
- Each subscriber gets own copy of message
- Each subscriber has independent queue
- URL format: `mem://topic/topic_name`

### Message Storage

```python
# Queues: Single shared queue
queues = {
    'tasks': Queue(),      # All subscribers share this
    'orders': Queue()
}

# Topics: List of subscriber queues
topics = {
    'events': [
        Queue(),  # Subscriber 1's queue
        Queue(),  # Subscriber 2's queue
        Queue()   # Subscriber 3's queue
    ]
}
```

### Use Cases

**Perfect for:**
- Development and testing
- Unit tests and integration tests
- Microservices in same process
- Fast inter-thread communication
- Temporary message passing
- Prototyping
- Memory-constrained environments (no broker overhead)

**Not suitable for:**
- Multi-process communication (use network-based brokers)
- Persistence required (messages lost on crash)
- Process restart recovery
- Distributed systems
- High availability requirements

## Usage

### Publishing to a Queue

Point-to-point messaging:

```python
from core.pubsub.pubsubfactory import create_publisher

config = {
    'destination': 'mem://queue/tasks'
}

publisher = create_publisher('task_pub', config)

# Publish tasks
publisher.publish({'task': 'process_order', 'order_id': 'ORD-123'})
publisher.publish({'task': 'send_email', 'to': 'user@example.com'})

publisher.stop()
```

### Publishing to a Topic

Broadcast messaging:

```python
config = {
    'destination': 'mem://topic/notifications'
}

publisher = create_publisher('notif_pub', config)

# Broadcast to all subscribers
publisher.publish({'type': 'alert', 'message': 'System maintenance scheduled'})

publisher.stop()
```

### Subscribing from a Queue

Multiple subscribers compete for messages:

```python
from core.pubsub.pubsubfactory import create_subscriber

config = {
    'source': 'mem://queue/tasks'
}

subscriber = create_subscriber('worker', config)
subscriber.start()

# Get message (blocking with timeout)
data = subscriber.get_data(block_time=5)
if data:
    print(f"Received: {data}")

subscriber.stop()
```

### Subscribing from a Topic

All subscribers receive all messages:

```python
config = {
    'source': 'mem://topic/notifications'
}

subscriber = create_subscriber('listener', config)
subscriber.start()

# Receive broadcast message
data = subscriber.get_data(block_time=5)
if data:
    print(f"Received: {data}")

subscriber.stop()
```

## Destination/Source Format

### Queue Format (Point-to-Point)
```
mem://queue/queue_name
```

**Examples:**
- `mem://queue/tasks`
- `mem://queue/orders`
- `mem://queue/processing_queue`

### Topic Format (Publish-Subscribe)
```
mem://topic/topic_name
```

**Examples:**
- `mem://topic/events`
- `mem://topic/notifications`
- `mem://topic/system_alerts`

## Configuration Options

### Publisher Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `destination` | string | Required | In-memory destination URL |
| `publish_interval` | int | `0` | Batch publishing interval (inherited) |
| `batch_size` | int | `None` | Batch size threshold (inherited) |

### Subscriber Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source` | string | Required | In-memory source URL |
| `max_depth` | int | `100000` | Queue maximum size (inherited) |

**Note**: The subscriber's `max_depth` creates its internal buffer queue. For topics, each subscriber gets its own queue with this size.

## Common Patterns

### Pattern 1: Work Queue (Load Balancing)

Multiple workers processing from shared queue:

```python
# Create publisher
publisher = create_publisher('producer', {
    'destination': 'mem://queue/work_items'
})

# Create multiple workers
workers = []
for i in range(3):
    worker = create_subscriber(f'worker_{i}', {
        'source': 'mem://queue/work_items'
    })
    worker.start()
    workers.append(worker)

# Publish work items
for i in range(10):
    publisher.publish({'item_id': i, 'action': 'process'})

# Workers compete for items
for worker in workers:
    data = worker.get_data(block_time=1)
    if data:
        print(f"{worker.name} processing: {data}")
```

### Pattern 2: Pub-Sub Broadcasting

Broadcast to multiple independent services:

```python
# Create publisher
publisher = create_publisher('broadcaster', {
    'destination': 'mem://topic/system_events'
})

# Create multiple subscribers (different services)
logger = create_subscriber('logger', {
    'source': 'mem://topic/system_events'
})

analytics = create_subscriber('analytics', {
    'source': 'mem://topic/system_events'
})

alerting = create_subscriber('alerting', {
    'source': 'mem://topic/system_events'
})

logger.start()
analytics.start()
alerting.start()

# Publish event - all receive it
publisher.publish({'event': 'user_login', 'user_id': 123})
```

### Pattern 3: Pipeline Processing

Multi-stage processing pipeline:

```python
# Stage 1: Raw data
raw_pub = create_publisher('raw_pub', {
    'destination': 'mem://queue/raw_data'
})

# Stage 2: Processing
processed_pub = create_publisher('processed_pub', {
    'destination': 'mem://queue/processed_data'
})

raw_sub = create_subscriber('processor', {
    'source': 'mem://queue/raw_data'
})
raw_sub.start()

# Process and forward
raw_data = raw_sub.get_data(block_time=1)
if raw_data:
    processed = transform(raw_data)
    processed_pub.publish(processed)
```

### Pattern 4: Request-Reply

Synchronous-style request-response:

```python
# Request publisher
request_pub = create_publisher('requester', {
    'destination': 'mem://queue/requests'
})

# Reply subscriber
reply_sub = create_subscriber('reply_receiver', {
    'source': 'mem://queue/replies'
})
reply_sub.start()

# Request handler
request_sub = create_subscriber('handler', {
    'source': 'mem://queue/requests'
})
request_sub.start()

reply_pub = create_publisher('replier', {
    'destination': 'mem://queue/replies'
})

# Send request
request_pub.publish({'request_id': 1, 'query': 'get_user'})

# Handler processes and replies
request = request_sub.get_data(block_time=1)
if request:
    response = process_request(request)
    reply_pub.publish(response)

# Receive reply
reply = reply_sub.get_data(block_time=1)
```

### Pattern 5: Event Bus

Central event bus for application:

```python
# Create central event bus
event_bus = create_publisher('event_bus', {
    'destination': 'mem://topic/app_events'
})

# Multiple components subscribe
ui_events = create_subscriber('ui', {
    'source': 'mem://topic/app_events'
})

db_events = create_subscriber('db', {
    'source': 'mem://topic/app_events'
})

cache_events = create_subscriber('cache', {
    'source': 'mem://topic/app_events'
})

ui_events.start()
db_events.start()
cache_events.start()

# Publish events
event_bus.publish({'event': 'user_updated', 'user_id': 123})
```

## Thread Safety

The in-memory implementation is **fully thread-safe**:

```python
import threading

publisher = create_publisher('pub', {
    'destination': 'mem://queue/shared'
})

# Multiple threads can safely publish
def publish_from_thread(thread_id):
    for i in range(10):
        publisher.publish({
            'thread_id': thread_id,
            'message_id': i
        })

threads = []
for i in range(5):
    thread = threading.Thread(target=publish_from_thread, args=(i,))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()
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
print(f"Internal queue: {stats['current_depth']}")
```

### Access Singleton Manager

```python
from core.pubsub.inmemorypubsub import InMemoryPubSub

# Get singleton instance
manager = InMemoryPubSub()

# Get queue details
queue_details = manager.get_queue_details('tasks')
if queue_details:
    print(f"Current depth: {queue_details['current_depth']}")
    print(f"Max depth: {queue_details['max_depth']}")
    print(f"Last publish: {queue_details['last_publish']}")

# Get topic details
topic_details = manager.get_topic_details('events')
if topic_details:
    print(f"Subscribers: {topic_details['subscriber_count']}")
    print(f"Last publish: {topic_details['last_publish']}")
```

## Troubleshooting

### Queue Full

**Error**: `queue.Full`

**Solution**: Increase max_depth or process messages faster
```python
config = {
    'source': 'mem://queue/tasks',
    'max_depth': 200000  # Increase from default 100000
}
```

### Messages Not Received

**Problem**: Subscriber gets no messages

**Solution**: Check queue/topic names match exactly
```python
# Must match exactly
publisher = create_publisher('pub', {
    'destination': 'mem://queue/tasks'  # ✓
})

subscriber = create_subscriber('sub', {
    'source': 'mem://queue/tasks'  # ✓ Same name
})
```

### Memory Usage Growing

**Problem**: Memory consumption increasing

**Solution**: Ensure subscribers are processing messages
```python
# Check if subscriber is started
subscriber.start()  # Must call start()!

# Check if getting messages
data = subscriber.get_data(block_time=1)

# Monitor queue depth
stats = subscriber.details()
print(f"Internal queue: {stats['current_depth']}")
```

### Subscriber Not Receiving Topic Messages

**Problem**: Topic subscriber gets no messages

**Solution**: Start subscriber BEFORE publishing
```python
# Wrong order
publisher.publish({'event': 'test'})  # ❌ Published before subscriber exists
subscriber.start()

# Correct order
subscriber.start()  # ✓ Start first
time.sleep(0.1)  # Give time to subscribe
publisher.publish({'event': 'test'})  # ✓ Now publish
```

## Best Practices

1. **Start subscribers first** - Especially for topics, start subscribers before publishing
2. **Call start()** - Always call `subscriber.start()` to begin receiving
3. **Close properly** - Always call `stop()` to clean up
4. **Monitor queue depths** - Watch for growing queues
5. **Use appropriate pattern** - Queues for work distribution, topics for broadcasting
6. **Thread safety** - Implementation is thread-safe, use freely in multi-threaded apps
7. **Testing** - Perfect for unit tests, no setup required
8. **Memory limits** - Set appropriate max_depth to prevent memory issues
9. **Process boundaries** - Don't use across processes, only within same process
10. **Error handling** - Handle `queue.Full` exceptions gracefully

## Performance Tips

1. **Batching**: Use `publish_interval` and `batch_size` for better throughput
2. **Queue size**: Adjust `max_depth` based on message size and volume
3. **Multiple workers**: Scale horizontally with more subscribers
4. **Pre-allocation**: Create queues/topics upfront if possible
5. **Message size**: Keep messages small for better performance
6. **Blocking vs non-blocking**: Use appropriate `block_time` settings
7. **Topic subscribers**: Each subscriber adds overhead, use queues when appropriate
8. **Memory profiling**: Monitor memory usage in long-running applications
9. **Garbage collection**: Python's GC handles cleanup automatically
10. **Lock contention**: Minimal with Python's GIL and queue implementation

## Complete Examples

### Example 1: Simple Task Queue

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
import threading

print("=== In-Memory Task Queue Example ===\n")

# Create publisher
publisher = create_publisher('task_producer', {
    'destination': 'mem://queue/tasks'
})

# Create multiple workers
def worker_func(worker_id, num_tasks):
    subscriber = create_subscriber(f'worker_{worker_id}', {
        'source': 'mem://queue/tasks'
    })
    subscriber.start()
    
    print(f"[Worker {worker_id}] Started")
    
    processed = 0
    while processed < num_tasks:
        data = subscriber.get_data(block_time=1)
        if data:
            print(f"[Worker {worker_id}] Processing task {data['task_id']}")
            time.sleep(0.2)  # Simulate work
            processed += 1
    
    subscriber.stop()
    print(f"[Worker {worker_id}] Finished")

# Start 3 workers
num_workers = 3
num_tasks = 9
tasks_per_worker = num_tasks // num_workers

threads = []
for i in range(num_workers):
    thread = threading.Thread(
        target=worker_func,
        args=(i, tasks_per_worker),
        daemon=True
    )
    thread.start()
    threads.append(thread)

time.sleep(0.5)  # Let workers start

# Publish tasks
print(f"\nPublishing {num_tasks} tasks...\n")
for i in range(num_tasks):
    publisher.publish({
        'task_id': i,
        'description': f'Task {i}'
    })
    print(f"Published task {i}")
    time.sleep(0.1)

# Wait for completion
for thread in threads:
    thread.join()

publisher.stop()

print("\n✓ Example 1 completed!")
```

### Example 2: Event Broadcasting

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
import threading

print("=== Event Broadcasting Example ===\n")

# Create event publisher
event_pub = create_publisher('event_source', {
    'destination': 'mem://topic/system_events'
})

# Logger service
def logger_service():
    subscriber = create_subscriber('logger', {
        'source': 'mem://topic/system_events'
    })
    subscriber.start()
    print("[LOGGER] Started")
    
    for i in range(5):
        event = subscriber.get_data(block_time=2)
        if event:
            print(f"[LOGGER] {event['timestamp']}: {event['level']} - {event['message']}")
    
    subscriber.stop()

# Analytics service
def analytics_service():
    subscriber = create_subscriber('analytics', {
        'source': 'mem://topic/system_events'
    })
    subscriber.start()
    print("[ANALYTICS] Started")
    
    event_counts = {}
    for i in range(5):
        event = subscriber.get_data(block_time=2)
        if event:
            level = event['level']
            event_counts[level] = event_counts.get(level, 0) + 1
    
    print(f"[ANALYTICS] Event summary: {event_counts}")
    subscriber.stop()

# Alerting service
def alerting_service():
    subscriber = create_subscriber('alerting', {
        'source': 'mem://topic/system_events'
    })
    subscriber.start()
    print("[ALERTING] Started")
    
    for i in range(5):
        event = subscriber.get_data(block_time=2)
        if event:
            if event['level'] in ['ERROR', 'CRITICAL']:
                print(f"[ALERTING] 🚨 Alert: {event['message']}")
    
    subscriber.stop()

# Start all services
logger_thread = threading.Thread(target=logger_service, daemon=True)
analytics_thread = threading.Thread(target=analytics_service, daemon=True)
alerting_thread = threading.Thread(target=alerting_service, daemon=True)

logger_thread.start()
analytics_thread.start()
alerting_thread.start()

time.sleep(0.5)  # Let services start

# Publish events
print("\nPublishing events...\n")
events = [
    {'level': 'INFO', 'message': 'Application started', 'timestamp': '10:00:00'},
    {'level': 'WARNING', 'message': 'High memory usage', 'timestamp': '10:01:00'},
    {'level': 'ERROR', 'message': 'Database connection failed', 'timestamp': '10:02:00'},
    {'level': 'INFO', 'message': 'Retrying connection', 'timestamp': '10:02:05'},
    {'level': 'INFO', 'message': 'Connection restored', 'timestamp': '10:02:10'}
]

for event in events:
    event_pub.publish(event)
    print(f"Published: {event['level']} - {event['message']}")
    time.sleep(0.5)

# Wait for processing
logger_thread.join()
analytics_thread.join()
alerting_thread.join()

event_pub.stop()

print("\n✓ Example 2 completed!")
```

### Example 3: Multi-Stage Pipeline

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
import threading

print("=== Multi-Stage Pipeline Example ===\n")

# Stage 1: Data ingestion
raw_pub = create_publisher('ingestion', {
    'destination': 'mem://queue/raw_data'
})

# Stage 2: Validation
validated_pub = create_publisher('validator_pub', {
    'destination': 'mem://queue/validated_data'
})

# Stage 3: Enrichment
enriched_pub = create_publisher('enricher_pub', {
    'destination': 'mem://queue/enriched_data'
})

# Validation worker
def validation_worker():
    raw_sub = create_subscriber('validator', {
        'source': 'mem://queue/raw_data'
    })
    raw_sub.start()
    print("[VALIDATOR] Started")
    
    processed = 0
    while processed < 5:
        data = raw_sub.get_data(block_time=2)
        if data:
            # Validate
            if 'id' in data and 'value' in data:
                print(f"[VALIDATOR] Valid: {data}")
                validated_pub.publish(data)
                processed += 1
            else:
                print(f"[VALIDATOR] Invalid: {data}")
    
    raw_sub.stop()

# Enrichment worker
def enrichment_worker():
    validated_sub = create_subscriber('enricher', {
        'source': 'mem://queue/validated_data'
    })
    validated_sub.start()
    print("[ENRICHER] Started")
    
    processed = 0
    while processed < 5:
        data = validated_sub.get_data(block_time=2)
        if data:
            # Enrich
            enriched = {
                **data,
                'enriched_at': time.time(),
                'category': 'high' if data['value'] > 50 else 'low'
            }
            print(f"[ENRICHER] Enriched: ID={enriched['id']}, Category={enriched['category']}")
            enriched_pub.publish(enriched)
            processed += 1
    
    validated_sub.stop()

# Final processor
def final_processor():
    enriched_sub = create_subscriber('processor', {
        'source': 'mem://queue/enriched_data'
    })
    enriched_sub.start()
    print("[PROCESSOR] Started")
    
    high_count = 0
    low_count = 0
    
    processed = 0
    while processed < 5:
        data = enriched_sub.get_data(block_time=2)
        if data:
            print(f"[PROCESSOR] Processing: {data}")
            if data['category'] == 'high':
                high_count += 1
            else:
                low_count += 1
            processed += 1
    
    print(f"\n[PROCESSOR] Summary: High={high_count}, Low={low_count}")
    enriched_sub.stop()

# Start all workers
validator_thread = threading.Thread(target=validation_worker, daemon=True)
enricher_thread = threading.Thread(target=enrichment_worker, daemon=True)
processor_thread = threading.Thread(target=final_processor, daemon=True)

validator_thread.start()
enricher_thread.start()
processor_thread.start()

time.sleep(0.5)  # Let workers start

# Ingest data
print("\nIngesting data...\n")
raw_data = [
    {'id': 1, 'value': 25},
    {'id': 2, 'value': 75},
    {'id': 3, 'value': 40},
    {'id': 4, 'value': 90},
    {'id': 5, 'value': 15}
]

for data in raw_data:
    raw_pub.publish(data)
    print(f"Ingested: {data}")
    time.sleep(0.3)

# Wait for completion
validator_thread.join()
enricher_thread.join()
processor_thread.join()

# Cleanup
raw_pub.stop()
validated_pub.stop()
enriched_pub.stop()

print("\n✓ Example 3 completed!")
```

### Example 4: Request-Reply Pattern

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
import threading
import uuid

print("=== Request-Reply Pattern Example ===\n")

# Request handler service
def request_handler():
    request_sub = create_subscriber('handler', {
        'source': 'mem://queue/requests'
    })
    request_sub.start()
    
    reply_pub = create_publisher('replier', {
        'destination': 'mem://queue/replies'
    })
    
    print("[HANDLER] Ready for requests")
    
    for i in range(3):
        request = request_sub.get_data(block_time=3)
        if request:
            print(f"[HANDLER] Processing request: {request['request_id']}")
            time.sleep(0.5)  # Simulate processing
            
            reply = {
                'request_id': request['request_id'],
                'status': 'success',
                'result': f"Result for {request['operation']}",
                'data': request.get('params', {})
            }
            reply_pub.publish(reply)
            print(f"[HANDLER] Sent reply: {request['request_id']}")
    
    request_sub.stop()
    reply_pub.stop()

# Start handler
handler_thread = threading.Thread(target=request_handler, daemon=True)
handler_thread.start()

time.sleep(0.5)  # Let handler start

# Client - send requests
request_pub = create_publisher('client_req', {
    'destination': 'mem://queue/requests'
})

reply_sub = create_subscriber('client_reply', {
    'source': 'mem://queue/replies'
})
reply_sub.start()

print("\n[CLIENT] Sending requests...\n")

# Send multiple requests
requests = [
    {'request_id': str(uuid.uuid4())[:8], 'operation': 'get_user', 'params': {'user_id': 123}},
    {'request_id': str(uuid.uuid4())[:8], 'operation': 'get_order', 'params': {'order_id': 456}},
    {'request_id': str(uuid.uuid4())[:8], 'operation': 'get_product', 'params': {'product_id': 789}}
]

for request in requests:
    request_pub.publish(request)
    print(f"[CLIENT] Sent request: {request['request_id']} - {request['operation']}")
    time.sleep(0.2)

print("\n[CLIENT] Waiting for replies...\n")

# Receive replies
for i in range(len(requests)):
    reply = reply_sub.get_data(block_time=5)
    if reply:
        print(f"[CLIENT] Received reply: {reply['request_id']}")
        print(f"         Status: {reply['status']}")
        print(f"         Result: {reply['result']}\n")

# Cleanup
request_pub.stop()
reply_sub.stop()
handler_thread.join(timeout=2)

print("✓ Example 4 completed!")
```

### Example 5: Testing Framework Integration

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import unittest

print("=== Testing Framework Example ===\n")

class TestMessageQueue(unittest.TestCase):
    """Example unit tests using in-memory pub/sub"""
    
    def setUp(self):
        """Setup before each test"""
        self.publisher = create_publisher('test_pub', {
            'destination': 'mem://queue/test_queue'
        })
        
        self.subscriber = create_subscriber('test_sub', {
            'source': 'mem://queue/test_queue'
        })
        self.subscriber.start()
    
    def tearDown(self):
        """Cleanup after each test"""
        self.publisher.stop()
        self.subscriber.stop()
    
    def test_simple_message(self):
        """Test simple message passing"""
        message = {'test': 'data', 'value': 123}
        self.publisher.publish(message)
        
        received = self.subscriber.get_data(block_time=1)
        self.assertIsNotNone(received)
        self.assertEqual(received['test'], 'data')
        self.assertEqual(received['value'], 123)
        print("✓ test_simple_message passed")
    
    def test_multiple_messages(self):
        """Test multiple messages in order"""
        messages = [
            {'id': 1, 'data': 'first'},
            {'id': 2, 'data': 'second'},
            {'id': 3, 'data': 'third'}
        ]
        
        for msg in messages:
            self.publisher.publish(msg)
        
        for i, expected in enumerate(messages):
            received = self.subscriber.get_data(block_time=1)
            self.assertIsNotNone(received)
            self.assertEqual(received['id'], expected['id'])
        
        print("✓ test_multiple_messages passed")
    
    def test_no_message_timeout(self):
        """Test timeout when no messages"""
        received = self.subscriber.get_data(block_time=0.5)
        self.assertIsNone(received)
        print("✓ test_no_message_timeout passed")

class TestTopicBroadcast(unittest.TestCase):
    """Example tests for topic broadcasting"""
    
    def setUp(self):
        """Setup before each test"""
        self.publisher = create_publisher('topic_pub', {
            'destination': 'mem://topic/test_topic'
        })
        
        self.sub1 = create_subscriber('sub1', {
            'source': 'mem://topic/test_topic'
        })
        self.sub2 = create_subscriber('sub2', {
            'source': 'mem://topic/test_topic'
        })
        
        self.sub1.start()
        self.sub2.start()
    
    def tearDown(self):
        """Cleanup after each test"""
        self.publisher.stop()
        self.sub1.stop()
        self.sub2.stop()
    
    def test_broadcast_to_all(self):
        """Test message broadcast to all subscribers"""
        message = {'event': 'test_event', 'data': 'broadcast'}
        self.publisher.publish(message)
        
        # Both subscribers should receive
        received1 = self.sub1.get_data(block_time=1)
        received2 = self.sub2.get_data(block_time=1)
        
        self.assertIsNotNone(received1)
        self.assertIsNotNone(received2)
        self.assertEqual(received1['event'], 'test_event')
        self.assertEqual(received2['event'], 'test_event')
        
        print("✓ test_broadcast_to_all passed")

# Run tests
print("Running unit tests...\n")
suite = unittest.TestSuite()
suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestMessageQueue))
suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestTopicBroadcast))
runner = unittest.TextTestRunner(verbosity=0)
result = runner.run(suite)

print(f"\nTests run: {result.testsRun}")
print(f"Failures: {len(result.failures)}")
print(f"Errors: {len(result.errors)}")

print("\n✓ Example 5 completed!")
```

## Comparison with Other Implementations

| Feature | In-Memory | File | SQL | ActiveMQ | Kafka | RabbitMQ |
|---------|-----------|------|-----|----------|-------|----------|
| Setup | None | None | Database | Broker | Broker | Broker |
| Dependencies | None | None | pymysql/psycopg2 | stomp.py | kafka-python | pika |
| Persistence | ❌ No | ✅ Yes | ✅ Yes | Optional | ✅ Yes | Optional |
| Multi-process | ❌ No | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| Throughput | Very High | Low | Low-Medium | Medium | Very High | Medium-High |
| Latency | Very Low | Medium | High | Low | Low | Low |
| Best for | Testing, single process | Logs, simple | DB workflows | Traditional MQ | Event streaming | Traditional MQ |
| Memory usage | High | Low | Low | Low | Medium | Low |
| Process crash | ❌ Data lost | ✅ Data safe | ✅ Data safe | ✅ Data safe | ✅ Data safe | ✅ Data safe |

## When to Use In-Memory Pub/Sub

**Use In-Memory pub/sub when:**
- Development and testing
- Unit tests and integration tests
- Single-process microservices
- Fast inter-thread communication
- Prototyping and experimentation
- Memory-based caching patterns
- No persistence required
- Simplicity and speed over reliability

**Use other implementations when:**
- Multi-process communication needed
- Persistence required
- Process restart recovery needed
- Distributed systems
- High availability requirements
- Need message durability

## Key Advantages

1. **Zero Setup**: No installation, configuration, or external services
2. **Fast**: In-process communication, no network overhead
3. **Simple**: Minimal API, easy to understand
4. **Thread-Safe**: Built-in thread safety
5. **Testing**: Perfect for unit and integration tests
6. **Lightweight**: No broker overhead
7. **Flexible**: Supports both queues and topics
8. **Free**: No licensing or infrastructure costs

## Key Limitations

1. **No Persistence**: Messages lost on crash or restart
2. **Single Process**: Cannot communicate across processes
3. **No Distribution**: Not for distributed systems
4. **Memory Bound**: Limited by available RAM
5. **No Recovery**: Cannot recover from failures
6. **Monitoring**: Limited monitoring compared to brokers

The In-Memory implementation provides the fastest and simplest pub/sub solution for single-process applications, testing, and scenarios where persistence is not required!

---

# InMemoryPubSub Manager

The sections below document the `InMemoryPubSubManager`, the central registry that backs all in-memory (`mem://` / `inmemory://`) topics and queues.

## Overview

The `InMemoryPubSub` class is a **singleton manager** that provides the underlying infrastructure for in-memory messaging. It directly manages Python `queue.Queue` objects for both point-to-point (queues) and publish-subscribe (topics) messaging patterns. This guide covers direct usage of the singleton manager, which provides more control than the DataPublisher/DataSubscriber wrappers.

## What is InMemoryPubSub?

**InMemoryPubSub** is a thread-safe singleton that:
- Manages all in-memory queues and topics
- Provides low-level messaging operations
- Maintains statistics for monitoring
- Ensures thread-safe access across the application
- Lives for the entire process lifetime

## Architecture

```
┌─────────────────────────────────────────────────────┐
│         InMemoryPubSub (Singleton)                  │
├─────────────────────────────────────────────────────┤
│                                                     │
│  Queues (Point-to-Point):                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │ Queue A  │  │ Queue B  │  │ Queue C  │         │
│  └──────────┘  └──────────┘  └──────────┘         │
│      FIFO         FIFO         FIFO                │
│                                                     │
│  Topics (Pub-Sub):                                 │
│  ┌────────────────────────────────────┐            │
│  │ Topic X                            │            │
│  │  ├─ Subscriber 1 Queue             │            │
│  │  ├─ Subscriber 2 Queue             │            │
│  │  └─ Subscriber 3 Queue             │            │
│  └────────────────────────────────────┘            │
│                                                     │
│  Statistics:                                       │
│  - Queue depths                                    │
│  - Last publish/consume times                     │
│  - Subscriber counts                              │
└─────────────────────────────────────────────────────┘
```

## Singleton Pattern

### Getting the Instance

The `InMemoryPubSub` class uses the singleton pattern - only one instance exists per process:

```python
from core.pubsub.inmemorypubsub import InMemoryPubSub

# Get the singleton instance
manager = InMemoryPubSub()

# Every call returns the same instance
manager2 = InMemoryPubSub()
assert manager is manager2  # True!
```

### Thread-Safe Initialization

The singleton is thread-safe - multiple threads can safely get the instance:

```python
import threading

def get_manager(thread_id):
    manager = InMemoryPubSub()
    print(f"Thread {thread_id}: {id(manager)}")

# All threads get same instance
threads = []
for i in range(5):
    t = threading.Thread(target=get_manager, args=(i,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

# All print same memory address
```

## Queue Operations

### Creating a Queue

```python
manager = InMemoryPubSub()

# Create a queue with default size (100,000)
manager.create_queue('task_queue')

# Create a queue with custom size
manager.create_queue('small_queue', max_size=100)

# Queues are auto-created on first publish
manager.publish_to_queue('auto_queue', {'data': 'test'})
```

### Publishing to Queue

```python
# Publish message (blocks if queue full)
manager.publish_to_queue('task_queue', {'task': 'process', 'id': 1})

# Publish with timeout
try:
    manager.publish_to_queue('task_queue', {'task': 'urgent'}, 
                            block=True, timeout=5)
except queue.Full:
    print("Queue is full!")

# Non-blocking publish
try:
    manager.publish_to_queue('task_queue', {'task': 'data'}, 
                            block=False)
except queue.Full:
    print("Queue full, message dropped")
```

### Consuming from Queue

```python
# Consume message (blocks until available)
message = manager.consume_from_queue('task_queue', block=True)

# Consume with timeout
message = manager.consume_from_queue('task_queue', 
                                     block=True, timeout=5)
if message is None:
    print("Timeout - no message available")

# Non-blocking consume
message = manager.consume_from_queue('task_queue', block=False)
if message is None:
    print("No message currently available")
```

### Queue Size

```python
# Get current queue depth
size = manager.get_queue_size('task_queue')
print(f"Queue has {size} messages")

# Check if queue is empty
if manager.get_queue_size('task_queue') == 0:
    print("Queue is empty")
```

## Topic Operations

### Creating a Topic

```python
# Create a topic
manager.create_topic('events')

# Topics are auto-created on first publish
manager.publish_to_topic('notifications', {'alert': 'System up'})
```

### Subscribing to Topic

```python
# Subscribe and get a dedicated queue
subscriber_queue = manager.subscribe_to_topic('events')

# Subscribe with custom queue size
subscriber_queue = manager.subscribe_to_topic('events', max_size=1000)

# Multiple subscribers get independent queues
sub1_queue = manager.subscribe_to_topic('events')
sub2_queue = manager.subscribe_to_topic('events')
sub3_queue = manager.subscribe_to_topic('events')
```

### Publishing to Topic

```python
# Publish message to all subscribers
manager.publish_to_topic('events', {
    'event': 'user_login',
    'user_id': 123,
    'timestamp': '2025-01-15T10:00:00'
})

# All subscriber queues receive the message
```

### Consuming from Topic Subscription

```python
# Each subscriber consumes from their own queue
subscriber_queue = manager.subscribe_to_topic('events')

# Blocking consume
message = subscriber_queue.get()

# Non-blocking consume
try:
    message = subscriber_queue.get_nowait()
except queue.Empty:
    print("No message")

# Consume with timeout
try:
    message = subscriber_queue.get(timeout=5)
except queue.Empty:
    print("Timeout")
```

## Monitoring and Statistics

### Queue Statistics

```python
# Get queue details
details = manager.get_queue_details('task_queue')

if details:
    print(f"Max depth: {details['max_depth']}")
    print(f"Current depth: {details['current_depth']}")
    print(f"Created at: {details['created_at']}")
    print(f"Last publish: {details['last_publish']}")
    print(f"Last consume: {details['last_consume']}")
```

### Topic Statistics

```python
# Get topic details
details = manager.get_topic_details('events')

if details:
    print(f"Created at: {details['created_at']}")
    print(f"Last publish: {details['last_publish']}")
    print(f"Subscriber count: {details['subscriber_count']}")
```

### List All Queues and Topics

```python
# Access internal dictionaries (read-only!)
for queue_name in manager._queues.keys():
    details = manager.get_queue_details(queue_name)
    print(f"Queue: {queue_name} - Depth: {details['current_depth']}")

for topic_name in manager._topics.keys():
    details = manager.get_topic_details(topic_name)
    print(f"Topic: {topic_name} - Subscribers: {details['subscriber_count']}")
```

## Common Patterns

### Pattern 1: Simple Message Queue

```python
manager = InMemoryPubSub()

# Producer
for i in range(10):
    manager.publish_to_queue('work', {'task_id': i})

# Consumer
while True:
    task = manager.consume_from_queue('work', block=False)
    if task is None:
        break
    print(f"Processing: {task}")
```

### Pattern 2: Multiple Producers

```python
import threading

manager = InMemoryPubSub()

def producer(producer_id):
    for i in range(5):
        manager.publish_to_queue('shared', {
            'producer': producer_id,
            'item': i
        })

# Start multiple producers
threads = []
for i in range(3):
    t = threading.Thread(target=producer, args=(i,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

# All messages are in queue
print(f"Queue size: {manager.get_queue_size('shared')}")  # 15
```

### Pattern 3: Competing Consumers

```python
import threading
import time

manager = InMemoryPubSub()

# Publish work
for i in range(20):
    manager.publish_to_queue('tasks', {'task_id': i})

# Multiple workers compete
def worker(worker_id):
    while True:
        task = manager.consume_from_queue('tasks', block=False)
        if task is None:
            break
        print(f"Worker {worker_id} processing: {task['task_id']}")
        time.sleep(0.1)

threads = []
for i in range(3):
    t = threading.Thread(target=worker, args=(i,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()
```

### Pattern 4: Topic Broadcasting

```python
manager = InMemoryPubSub()

# Create subscribers
logger_queue = manager.subscribe_to_topic('system_events')
analytics_queue = manager.subscribe_to_topic('system_events')
alerting_queue = manager.subscribe_to_topic('system_events')

# Publish event
manager.publish_to_topic('system_events', {
    'event': 'error',
    'message': 'Database connection failed'
})

# All subscribers receive it
log_msg = logger_queue.get_nowait()
analytics_msg = analytics_queue.get_nowait()
alert_msg = alerting_queue.get_nowait()

assert log_msg == analytics_msg == alert_msg
```

### Pattern 5: Fan-Out / Fan-In

```python
import threading

manager = InMemoryPubSub()

# Fan-out: One publisher, multiple workers
manager.publish_to_queue('work', {'data': 'batch_1'})
manager.publish_to_queue('work', {'data': 'batch_2'})
manager.publish_to_queue('work', {'data': 'batch_3'})

results_lock = threading.Lock()
results = []

def worker():
    work = manager.consume_from_queue('work', block=False)
    if work:
        result = process(work)
        # Fan-in: Multiple workers, one results queue
        manager.publish_to_queue('results', result)

# Process work
threads = []
for i in range(3):
    t = threading.Thread(target=worker)
    threads.append(t)
    t.start()

for t in threads:
    t.join()

# Collect results
while manager.get_queue_size('results') > 0:
    result = manager.consume_from_queue('results', block=False)
    results.append(result)
```

## Advanced Usage

### Queue as Stack (LIFO)

While queues are FIFO by default, you can implement LIFO behavior:

```python
import queue

# Create custom LIFO queue
class LIFOInMemoryPubSub(InMemoryPubSub):
    def create_stack(self, name, max_size=100000):
        with self._queue_lock:
            if name not in self._queues:
                self._queues[name] = queue.LifoQueue(maxsize=max_size)
                self._queue_stats[name] = {
                    'max_depth': max_size,
                    'created_at': datetime.now().isoformat()
                }
```

### Priority Queue

```python
import queue

class PriorityInMemoryPubSub(InMemoryPubSub):
    def create_priority_queue(self, name, max_size=100000):
        with self._queue_lock:
            if name not in self._queues:
                self._queues[name] = queue.PriorityQueue(maxsize=max_size)
                # ... stats setup

# Publish with priority
# (priority, data) tuples
manager.publish_to_queue('priority_tasks', (1, {'task': 'high'}))
manager.publish_to_queue('priority_tasks', (5, {'task': 'low'}))
```

### Custom Message Filtering

```python
def filtered_consume(manager, queue_name, filter_func):
    """Consume messages matching filter"""
    temp_messages = []
    
    while True:
        msg = manager.consume_from_queue(queue_name, block=False)
        if msg is None:
            break
        
        if filter_func(msg):
            # Found matching message, put others back
            for temp_msg in temp_messages:
                manager.publish_to_queue(queue_name, temp_msg)
            return msg
        else:
            temp_messages.append(msg)
    
    # No match found, put all back
    for temp_msg in temp_messages:
        manager.publish_to_queue(queue_name, temp_msg)
    return None

# Usage
message = filtered_consume(
    manager, 
    'tasks', 
    lambda m: m.get('priority') == 'high'
)
```

### Drain Queue

```python
def drain_queue(manager, queue_name):
    """Remove all messages from queue"""
    messages = []
    while True:
        msg = manager.consume_from_queue(queue_name, block=False)
        if msg is None:
            break
        messages.append(msg)
    return messages

# Drain and process
all_messages = drain_queue(manager, 'task_queue')
print(f"Drained {len(all_messages)} messages")
```

### Peek Queue (Non-Destructive Read)

```python
def peek_queue(manager, queue_name):
    """Look at next message without removing it"""
    msg = manager.consume_from_queue(queue_name, block=False)
    if msg is not None:
        # Put it back at front (not perfect for FIFO, but close)
        manager.publish_to_queue(queue_name, msg)
    return msg
```

## Complete Examples

### Example 1: Producer-Consumer Pattern

```python
from core.pubsub.inmemorypubsub import InMemoryPubSub
import threading
import time
import random

print("=== Producer-Consumer Pattern ===\n")

manager = InMemoryPubSub()

# Producer function
def producer(producer_id, num_items):
    print(f"[Producer {producer_id}] Starting")
    for i in range(num_items):
        item = {
            'producer_id': producer_id,
            'item_number': i,
            'data': f'data_{producer_id}_{i}',
            'timestamp': time.time()
        }
        manager.publish_to_queue('production_line', item)
        print(f"[Producer {producer_id}] Produced item {i}")
        time.sleep(random.uniform(0.1, 0.3))
    print(f"[Producer {producer_id}] Finished")

# Consumer function
def consumer(consumer_id, expected_items):
    print(f"[Consumer {consumer_id}] Starting")
    consumed = 0
    while consumed < expected_items:
        item = manager.consume_from_queue('production_line', 
                                         block=True, timeout=2)
        if item:
            print(f"[Consumer {consumer_id}] Consumed: "
                  f"Producer {item['producer_id']}, Item {item['item_number']}")
            time.sleep(random.uniform(0.05, 0.15))
            consumed += 1
        else:
            print(f"[Consumer {consumer_id}] Timeout waiting for item")
    print(f"[Consumer {consumer_id}] Finished - consumed {consumed} items")

# Start producers and consumers
num_producers = 2
num_consumers = 3
items_per_producer = 5
total_items = num_producers * items_per_producer
items_per_consumer = total_items // num_consumers

threads = []

# Start producers
for i in range(num_producers):
    t = threading.Thread(target=producer, args=(i, items_per_producer))
    threads.append(t)
    t.start()

# Start consumers
for i in range(num_consumers):
    t = threading.Thread(target=consumer, args=(i, items_per_consumer))
    threads.append(t)
    t.start()

# Wait for completion
for t in threads:
    t.join()

# Check statistics
details = manager.get_queue_details('production_line')
print(f"\nFinal queue depth: {details['current_depth']}")
print("\n✓ Example 1 completed!")
```

### Example 2: Event Bus with Topics

```python
from core.pubsub.inmemorypubsub import InMemoryPubSub
import threading
import time

print("=== Event Bus with Topics ===\n")

manager = InMemoryPubSub()

# Service functions
def logging_service(duration):
    print("[LOGGER] Starting")
    log_queue = manager.subscribe_to_topic('app_events')
    
    start_time = time.time()
    log_count = 0
    
    while time.time() - start_time < duration:
        try:
            event = log_queue.get(timeout=1)
            print(f"[LOGGER] {event['timestamp']}: {event['level']} - {event['message']}")
            log_count += 1
        except:
            pass
    
    print(f"[LOGGER] Finished - logged {log_count} events")

def analytics_service(duration):
    print("[ANALYTICS] Starting")
    analytics_queue = manager.subscribe_to_topic('app_events')
    
    event_counts = {}
    start_time = time.time()
    
    while time.time() - start_time < duration:
        try:
            event = analytics_queue.get(timeout=1)
            level = event['level']
            event_counts[level] = event_counts.get(level, 0) + 1
        except:
            pass
    
    print(f"[ANALYTICS] Event summary: {event_counts}")

def alerting_service(duration):
    print("[ALERTING] Starting")
    alert_queue = manager.subscribe_to_topic('app_events')
    
    alert_count = 0
    start_time = time.time()
    
    while time.time() - start_time < duration:
        try:
            event = alert_queue.get(timeout=1)
            if event['level'] in ['ERROR', 'CRITICAL']:
                print(f"[ALERTING] 🚨 {event['level']}: {event['message']}")
                alert_count += 1
        except:
            pass
    
    print(f"[ALERTING] Sent {alert_count} alerts")

# Event generator
def event_generator(duration):
    print("[GENERATOR] Starting")
    events = [
        {'level': 'INFO', 'message': 'Application started'},
        {'level': 'INFO', 'message': 'User logged in'},
        {'level': 'WARNING', 'message': 'High memory usage'},
        {'level': 'ERROR', 'message': 'Database connection failed'},
        {'level': 'INFO', 'message': 'Retry successful'},
        {'level': 'CRITICAL', 'message': 'Service down'},
        {'level': 'INFO', 'message': 'Service recovered'}
    ]
    
    start_time = time.time()
    event_count = 0
    
    while time.time() - start_time < duration:
        for event in events:
            event['timestamp'] = time.strftime('%H:%M:%S')
            manager.publish_to_topic('app_events', event)
            event_count += 1
            print(f"[GENERATOR] Published: {event['level']}")
            time.sleep(0.3)
    
    print(f"[GENERATOR] Published {event_count} events")

# Start all services
duration = 3  # Run for 3 seconds

threads = [
    threading.Thread(target=logging_service, args=(duration,), daemon=True),
    threading.Thread(target=analytics_service, args=(duration,), daemon=True),
    threading.Thread(target=alerting_service, args=(duration,), daemon=True),
    threading.Thread(target=event_generator, args=(duration,))
]

for t in threads:
    t.start()

for t in threads:
    t.join()

# Check statistics
details = manager.get_topic_details('app_events')
print(f"\nTopic subscribers: {details['subscriber_count']}")
print("\n✓ Example 2 completed!")
```

### Example 3: Pipeline with Multiple Stages

```python
from core.pubsub.inmemorypubsub import InMemoryPubSub
import threading
import time

print("=== Multi-Stage Pipeline ===\n")

manager = InMemoryPubSub()

# Stage 1: Data ingestion
def ingestion_stage():
    print("[INGESTION] Starting")
    raw_data = [
        {'id': 1, 'value': 10, 'status': 'new'},
        {'id': 2, 'value': 25, 'status': 'new'},
        {'id': 3, 'value': 50, 'status': 'new'},
        {'id': 4, 'value': 75, 'status': 'new'},
        {'id': 5, 'value': 100, 'status': 'new'}
    ]
    
    for data in raw_data:
        manager.publish_to_queue('raw_stage', data)
        print(f"[INGESTION] Ingested: {data}")
        time.sleep(0.2)
    
    print("[INGESTION] Finished")

# Stage 2: Validation
def validation_stage():
    print("[VALIDATION] Starting")
    processed = 0
    expected = 5
    
    while processed < expected:
        data = manager.consume_from_queue('raw_stage', block=True, timeout=3)
        if data:
            # Validate
            if data['value'] > 0:
                print(f"[VALIDATION] Valid: {data}")
                data['status'] = 'validated'
                manager.publish_to_queue('validated_stage', data)
                processed += 1
            else:
                print(f"[VALIDATION] Invalid: {data}")
    
    print(f"[VALIDATION] Finished - validated {processed} items")

# Stage 3: Enrichment
def enrichment_stage():
    print("[ENRICHMENT] Starting")
    processed = 0
    expected = 5
    
    while processed < expected:
        data = manager.consume_from_queue('validated_stage', block=True, timeout=3)
        if data:
            # Enrich
            data['category'] = 'high' if data['value'] >= 50 else 'low'
            data['enriched_at'] = time.time()
            data['status'] = 'enriched'
            print(f"[ENRICHMENT] Enriched: ID={data['id']}, Category={data['category']}")
            manager.publish_to_queue('enriched_stage', data)
            processed += 1
    
    print(f"[ENRICHMENT] Finished - enriched {processed} items")

# Stage 4: Final processing
def processing_stage():
    print("[PROCESSING] Starting")
    processed = 0
    expected = 5
    high_count = 0
    low_count = 0
    
    while processed < expected:
        data = manager.consume_from_queue('enriched_stage', block=True, timeout=3)
        if data:
            print(f"[PROCESSING] Processing: {data}")
            if data['category'] == 'high':
                high_count += 1
            else:
                low_count += 1
            processed += 1
    
    print(f"[PROCESSING] Finished - High: {high_count}, Low: {low_count}")

# Run pipeline
threads = [
    threading.Thread(target=ingestion_stage),
    threading.Thread(target=validation_stage),
    threading.Thread(target=enrichment_stage),
    threading.Thread(target=processing_stage)
]

for t in threads:
    t.start()

for t in threads:
    t.join()

print("\n✓ Example 3 completed!")
```

### Example 4: Monitoring Dashboard

```python
from core.pubsub.inmemorypubsub import InMemoryPubSub
import threading
import time

print("=== Monitoring Dashboard ===\n")

manager = InMemoryPubSub()

# Create some queues and topics
manager.create_queue('orders', max_size=1000)
manager.create_queue('notifications', max_size=500)
manager.create_topic('system_events')

# Subscribe to topic
sub1 = manager.subscribe_to_topic('system_events')
sub2 = manager.subscribe_to_topic('system_events')

# Add some messages
for i in range(10):
    manager.publish_to_queue('orders', {'order_id': i})

for i in range(5):
    manager.publish_to_queue('notifications', {'notification_id': i})

for i in range(3):
    manager.publish_to_topic('system_events', {'event_id': i})

# Monitoring function
def print_dashboard():
    print("=" * 60)
    print("INMEMORY PUBSUB MONITORING DASHBOARD")
    print("=" * 60)
    
    print("\nQUEUES:")
    print("-" * 60)
    print(f"{'Queue Name':<20} {'Current':<10} {'Max':<10} {'Last Publish'}")
    print("-" * 60)
    
    for queue_name in manager._queues.keys():
        details = manager.get_queue_details(queue_name)
        if details:
            last_pub = details['last_publish'] or 'Never'
            if last_pub != 'Never':
                last_pub = last_pub.split('T')[1][:8]
            
            print(f"{queue_name:<20} {details['current_depth']:<10} "
                  f"{details['max_depth']:<10} {last_pub}")
    
    print("\nTOPICS:")
    print("-" * 60)
    print(f"{'Topic Name':<20} {'Subscribers':<15} {'Last Publish'}")
    print("-" * 60)
    
    for topic_name in manager._topics.keys():
        details = manager.get_topic_details(topic_name)
        if details:
            last_pub = details['last_publish'] or 'Never'
            if last_pub != 'Never':
                last_pub = last_pub.split('T')[1][:8]
            
            print(f"{topic_name:<20} {details['subscriber_count']:<15} {last_pub}")
    
    print("\n" + "=" * 60)

# Display dashboard
print_dashboard()

# Consume some messages
print("\nConsuming 3 orders...")
for i in range(3):
    manager.consume_from_queue('orders', block=False)

time.sleep(0.1)

# Publish more
print("Publishing 5 more notifications...")
for i in range(5, 10):
    manager.publish_to_queue('notifications', {'notification_id': i})

time.sleep(0.1)

# Display updated dashboard
print_dashboard()

print("\n✓ Example 4 completed!")
```

### Example 5: Testing Helper

```python
from core.pubsub.inmemorypubsub import InMemoryPubSub
import unittest

print("=== Testing Helper Example ===\n")

class InMemoryPubSubTestHelper:
    """Helper class for testing with InMemoryPubSub"""
    
    def __init__(self):
        self.manager = InMemoryPubSub()
        self.created_queues = []
        self.created_topics = []
        self.topic_subscribers = []
    
    def create_test_queue(self, name, max_size=1000):
        """Create queue and track for cleanup"""
        self.manager.create_queue(name, max_size)
        self.created_queues.append(name)
        return name
    
    def create_test_topic(self, name):
        """Create topic and track for cleanup"""
        self.manager.create_topic(name)
        self.created_topics.append(name)
        return name
    
    def subscribe_to_test_topic(self, name, max_size=1000):
        """Subscribe to topic and track for cleanup"""
        queue = self.manager.subscribe_to_topic(name, max_size)
        self.topic_subscribers.append(queue)
        return queue
    
    def cleanup(self):
        """Clean up test resources"""
        # Drain all queues
        for queue_name in self.created_queues:
            while manager.get_queue_size(queue_name) > 0:
                manager.consume_from_queue(queue_name, block=False)
        
        # Note: Can't easily remove topics/subscribers from singleton
        # Best practice: use unique names per test
        print(f"Cleaned up {len(self.created_queues)} queues")

# Example test
class TestWithHelper(unittest.TestCase):
    
    def setUp(self):
        self.helper = InMemoryPubSubTestHelper()
        self.queue_name = self.helper.create_test_queue('test_queue_1')
    
    def tearDown(self):
        self.helper.cleanup()
    
    def test_message_passing(self):
        """Test basic message passing"""
        manager = self.helper.manager
        
        # Publish
        test_data = {'test': 'data', 'id': 123}
        manager.publish_to_queue(self.queue_name, test_data)
        
        # Consume
        received = manager.consume_from_queue(self.queue_name, block=False)
        
        # Assert
        self.assertIsNotNone(received)
        self.assertEqual(received['test'], 'data')
        self.assertEqual(received['id'], 123)
        print("✓ test_message_passing passed")
    
    def test_queue_fifo_order(self):
        """Test FIFO ordering"""
        manager = self.helper.manager
        
        # Publish in order
        for i in range(5):
            manager.publish_to_queue(self.queue_name, {'order': i})
        
        # Consume and verify order
        for i in range(5):
            msg = manager.consume_from_queue(self.queue_name, block=False)
            self.assertEqual(msg['order'], i)
        
        print("✓ test_queue_fifo_order passed")

# Run tests
manager = InMemoryPubSub()
suite = unittest.TestLoader().loadTestsFromTestCase(TestWithHelper)
runner = unittest.TextTestRunner(verbosity=0)
result = runner.run(suite)

print(f"\nTests run: {result.testsRun}")
print(f"Failures: {len(result.failures)}")
print(f"Errors: {len(result.errors)}")

print("\n✓ Example 5 completed!")
```

## Best Practices

### 1. Use Unique Queue/Topic Names

```python
# Good - unique names
manager.create_queue('user_service_tasks')
manager.create_queue('order_service_tasks')

# Bad - generic names can conflict
manager.create_queue('tasks')  # Which service?
```

### 2. Set Appropriate Queue Sizes

```python
# Match queue size to expected load
manager.create_queue('high_volume', max_size=100000)  # Default
manager.create_queue('low_volume', max_size=100)      # Smaller
manager.create_queue('real_time', max_size=10)        # Very small
```

### 3. Handle Queue.Full Exceptions

```python
import queue

try:
    manager.publish_to_queue('limited', message, block=False)
except queue.Full:
    # Handle gracefully
    logger.warning("Queue full, applying backpressure")
    # Option: Wait and retry
    # Option: Drop message
    # Option: Write to overflow queue
```

### 4. Clean Up Topic Subscribers

```python
# Keep references to subscriber queues
subscriber_queues = []

# Subscribe
for i in range(5):
    q = manager.subscribe_to_topic('events')
    subscriber_queues.append(q)

# Drain before exit
for q in subscriber_queues:
    while not q.empty():
        q.get_nowait()
```

### 5. Use Timeouts for Robustness

```python
# Always use timeouts in production
message = manager.consume_from_queue('tasks', 
                                     block=True, 
                                     timeout=30)  # Don't block forever

if message is None:
    # Handle timeout
    logger.warning("No message received in 30 seconds")
```

### 6. Monitor Queue Depths

```python
# Regularly check queue depths
def monitor_queues(manager):
    for queue_name in manager._queues.keys():
        size = manager.get_queue_size(queue_name)
        details = manager.get_queue_details(queue_name)
        
        if size > details['max_depth'] * 0.8:
            logger.warning(f"Queue {queue_name} is 80% full")
```

### 7. Thread Safety

```python
# The manager is thread-safe - use freely
def worker():
    while True:
        msg = manager.consume_from_queue('work', timeout=1)
        if msg:
            process(msg)

# Multiple threads are safe
for i in range(10):
    threading.Thread(target=worker, daemon=True).start()
```

## Performance Characteristics

### Time Complexity

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| publish_to_queue | O(1) | Thread-safe enqueue |
| consume_from_queue | O(1) | Thread-safe dequeue |
| publish_to_topic | O(n) | n = number of subscribers |
| get_queue_size | O(1) | Approximate due to threading |
| get_queue_details | O(1) | Dictionary lookup |

### Memory Usage

```python
# Approximate memory per message
import sys

message = {'id': 123, 'data': 'test'}
message_size = sys.getsizeof(message)  # ~240 bytes

# Queue with 100,000 messages
total_memory = message_size * 100000  # ~24 MB
```

### Throughput

```python
import time

# Measure throughput
manager = InMemoryPubSub()
num_messages = 100000

# Publishing
start = time.time()
for i in range(num_messages):
    manager.publish_to_queue('perf_test', {'id': i})
publish_time = time.time() - start

print(f"Published {num_messages} in {publish_time:.2f}s")
print(f"Throughput: {num_messages/publish_time:.0f} msg/s")

# Consuming
start = time.time()
for i in range(num_messages):
    manager.consume_from_queue('perf_test', block=False)
consume_time = time.time() - start

print(f"Consumed {num_messages} in {consume_time:.2f}s")
print(f"Throughput: {num_messages/consume_time:.0f} msg/s")
```

## Limitations and Considerations

### 1. Single Process Only
```python
# ❌ Won't work across processes
# Process 1
manager.publish_to_queue('shared', {'data': 'test'})

# Process 2 (different Python process)
# This creates a DIFFERENT singleton!
message = manager.consume_from_queue('shared')  # None
```

### 2. No Persistence
```python
# Messages lost on crash
manager.publish_to_queue('important', {'data': 'critical'})

# If process crashes here, message is lost forever
```

### 3. Memory Bounded
```python
# All messages stored in RAM
# Large message volumes can exhaust memory
for i in range(10_000_000):  # 10 million messages
    manager.publish_to_queue('huge', {'id': i})  # May run out of RAM
```

### 4. No Message TTL
```python
# Messages don't expire
# Old messages stay in queue forever until consumed
manager.publish_to_queue('old', {'timestamp': '2020-01-01'})
# Still in queue in 2025 if never consumed
```

### 5. Topic Subscriber Cleanup
```python
# Subscriber queues remain even after subscriber stops
# No automatic cleanup mechanism
# Design consideration: Use unique topic names per test
```

## Comparison: Direct API vs Wrappers

| Aspect | Direct InMemoryPubSub API | DataPublisher/Subscriber Wrappers |
|--------|---------------------------|-----------------------------------|
| Setup | Manual queue/topic creation | Automatic via factory |
| Flexibility | Full control | Simplified interface |
| Batching | Manual implementation | Built-in support |
| Statistics | Manual tracking | Automatic tracking |
| Threading | Manual thread management | Built-in threading |
| Best For | Advanced use cases | Standard pub/sub patterns |

## When to Use Direct API

**Use InMemoryPubSub directly when:**
- Need fine-grained control
- Implementing custom patterns
- Building testing infrastructure
- Need access to internal queues
- Implementing specialized behaviors
- Performance optimization needed

**Use DataPublisher/DataSubscriber when:**
- Standard pub/sub patterns
- Need batching support
- Want automatic threading
- Need consistent API across brokers
- Simpler interface preferred

## Conclusion

The `InMemoryPubSub` singleton provides a powerful, thread-safe foundation for in-memory messaging in Python applications. Its direct API offers maximum flexibility for advanced use cases, while the DataPublisher/DataSubscriber wrappers provide a simpler interface for standard patterns.

**Key Takeaways:**
- Singleton pattern ensures one instance per process
- Thread-safe for concurrent access
- Supports both queues (point-to-point) and topics (pub-sub)
- Zero external dependencies
- Perfect for testing and single-process applications
- No persistence - messages lost on process exit
- Direct API for maximum control

The InMemoryPubSub manager is an essential tool for development, testing, and in-process messaging scenarios!
