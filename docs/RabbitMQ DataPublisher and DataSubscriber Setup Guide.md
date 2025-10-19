# RabbitMQ DataPublisher and DataSubscriber Setup Guide

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


## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.