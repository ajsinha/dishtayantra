# ActiveMQ DataPublisher and DataSubscriber Setup Guide

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