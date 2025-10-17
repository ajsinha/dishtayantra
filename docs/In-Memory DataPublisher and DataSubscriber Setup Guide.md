# In-Memory DataPublisher and DataSubscriber Setup Guide

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