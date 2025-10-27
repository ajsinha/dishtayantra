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


## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.
