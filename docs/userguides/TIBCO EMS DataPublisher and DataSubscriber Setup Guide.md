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


## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.
