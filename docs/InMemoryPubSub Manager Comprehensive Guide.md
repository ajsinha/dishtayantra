# InMemoryPubSub Manager Comprehensive Guide

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