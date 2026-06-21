# Data Store Connectors Guide

Connector-specific configuration for data-store backends: **Redis, SQL (with connection pooling), and Aerospike**.

> **Common configuration** (URI schemes, shared subscriber/publisher/node
> parameters, application.yaml) is in the **Configuration Reference Guide**. This
> guide covers connector-specific detail. Each connector is a section below.

---

# Redis DataPublisher and DataSubscriber Setup Guide

## Overview

This guide provides instructions for setting up and using Redis-based DataPublisher and DataSubscriber implementations for the DAG Server framework. Redis is an in-memory data structure store used as a database, cache, and message broker with built-in pub/sub functionality.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Components](#components)
4. [Configuration](#configuration)
5. [Usage Examples](#usage-examples)
6. [Advanced Features](#advanced-features)
7. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Dependencies

- Python 3.7 or higher
- Redis server instance running and accessible
- `redis-py` library

### Python Packages

```bash
pip install redis
```

---

## Installation

### 1. Redis Server Setup

Install and start Redis server:

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install redis-server
sudo systemctl start redis-server
sudo systemctl enable redis-server
```

**macOS:**
```bash
brew install redis
brew services start redis
```

**Docker:**
```bash
docker run -d -p 6379:6379 redis:latest
```

### 2. Verify Redis Installation

```bash
redis-cli ping
# Should return: PONG
```

### 3. Client Library Installation

The `redis-py` library is required:

```bash
pip install redis
```

### 4. Module Installation

Copy `redis_datapubsub.py` to your project's appropriate location.

---

## Components

### Available Classes

#### 1. **RedisDataPublisher**

Publishes data by setting key-value pairs in Redis using the `__dagserver_key` field.

**Key Features:**
- Stores data as JSON in Redis
- Simple key-value storage pattern
- Automatic JSON serialization
- Thread-safe operations
- Connection management

**Use Cases:**
- Configuration storage
- State persistence
- Shared data caching
- Result storage

#### 2. **RedisChannelDataPublisher**

Publishes data to Redis channels using the native pub/sub mechanism.

**Key Features:**
- Real-time message broadcasting
- Multiple subscriber support
- Fire-and-forget messaging
- Low latency communication
- Built-in Redis pub/sub protocol

**Use Cases:**
- Real-time notifications
- Event broadcasting
- Live data feeds
- System alerts

#### 3. **RedisChannelDataSubscriber**

Subscribes to Redis channels and receives published messages in real-time.

**Key Features:**
- Asynchronous message reception
- Blocking and non-blocking modes
- Automatic message deserialization
- Connection persistence
- Graceful error handling

**Use Cases:**
- Event listeners
- Real-time data processing
- Alert monitoring
- Live dashboards

---

## Configuration

### Basic Configuration Structure

```python
config = {
    'host': 'localhost',      # Redis server host
    'port': 6379,             # Redis server port
    'db': 0                   # Redis database number (0-15)
}
```

### Publisher Configuration Examples

#### Key-Value Publisher Configuration

```python
# Basic configuration
publisher_config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0
}

# Remote Redis server
publisher_config = {
    'host': 'redis.example.com',
    'port': 6379,
    'db': 0
}

# Different database
publisher_config = {
    'host': 'localhost',
    'port': 6379,
    'db': 1  # Use database 1 instead of default 0
}
```

#### Channel Publisher Configuration

```python
channel_publisher_config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0
}
```

### Subscriber Configuration Examples

```python
subscriber_config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0
}
```

### Advanced Configuration

```python
# With authentication
config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'password': 'your_redis_password'  # If Redis requires authentication
}

# With connection pool settings
config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'max_connections': 50,
    'socket_timeout': 5,
    'socket_connect_timeout': 5
}

# With SSL/TLS
config = {
    'host': 'redis.example.com',
    'port': 6380,
    'db': 0,
    'ssl': True,
    'ssl_cert_reqs': 'required'
}
```

---

## Usage Examples

### Example 1: Basic Key-Value Publishing

```python
from redis_datapubsub import RedisDataPublisher

# Configuration
config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0
}

# Create publisher
publisher = RedisDataPublisher(
    name='sensor_publisher',
    destination='redis://sensor_data',
    config=config
)

# Publish data
data = {
    '__dagserver_key': 'sensor:temperature:001',
    'value': 23.5,
    'unit': 'celsius',
    'timestamp': '2025-10-22T10:30:00Z',
    'location': 'Building A'
}

publisher.publish(data)

# Stop publisher
publisher.stop()

print(f"Published {publisher._publish_count} messages")
```

### Example 2: Storing Multiple Data Points

```python
from redis_datapubsub import RedisDataPublisher
import time

config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0
}

publisher = RedisDataPublisher(
    name='metrics_publisher',
    destination='redis://metrics',
    config=config
)

# Publish multiple sensor readings
sensors = ['temp', 'humidity', 'pressure']
for i in range(10):
    for sensor in sensors:
        data = {
            '__dagserver_key': f'sensor:{sensor}:latest',
            'value': 20 + i,
            'reading_id': i,
            'sensor_type': sensor,
            'timestamp': time.time()
        }
        publisher.publish(data)
    time.sleep(1)

publisher.stop()
print(f"Total publications: {publisher._publish_count}")
```

### Example 3: Using Different Redis Databases

```python
from redis_datapubsub import RedisDataPublisher

# Publish to different databases for data separation
databases = {
    'production': 0,
    'staging': 1,
    'development': 2
}

for env, db_num in databases.items():
    config = {
        'host': 'localhost',
        'port': 6379,
        'db': db_num
    }
    
    publisher = RedisDataPublisher(
        name=f'{env}_publisher',
        destination=f'redis://{env}_data',
        config=config
    )
    
    data = {
        '__dagserver_key': f'config:{env}',
        'environment': env,
        'version': '1.0.0',
        'status': 'active'
    }
    
    publisher.publish(data)
    publisher.stop()
    
    print(f"Published to {env} database (db={db_num})")
```

### Example 4: Channel-Based Publishing

```python
from redis_datapubsub import RedisChannelDataPublisher
import json

config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0
}

# Create channel publisher
publisher = RedisChannelDataPublisher(
    name='alerts_publisher',
    destination='redischannel://system_alerts',
    config=config
)

# Publish different types of alerts
alerts = [
    {
        'severity': 'high',
        'message': 'CPU usage exceeded 90%',
        'server': 'web-server-01',
        'timestamp': '2025-10-22T10:35:00Z'
    },
    {
        'severity': 'medium',
        'message': 'Disk space below 20%',
        'server': 'db-server-01',
        'timestamp': '2025-10-22T10:36:00Z'
    },
    {
        'severity': 'low',
        'message': 'Service restarted',
        'server': 'api-server-01',
        'timestamp': '2025-10-22T10:37:00Z'
    }
]

for alert in alerts:
    publisher.publish(alert)
    print(f"Published alert: {alert['message']}")

publisher.stop()
```

### Example 5: Channel Subscription

```python
from redis_datapubsub import RedisChannelDataSubscriber
import time

config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0
}

# Create subscriber
subscriber = RedisChannelDataSubscriber(
    name='alerts_subscriber',
    source='redischannel://system_alerts',
    config=config
)

# Start subscriber
subscriber.start()

# Process messages for 30 seconds
print("Listening for alerts...")
start_time = time.time()

try:
    while time.time() - start_time < 30:
        time.sleep(1)
        
        # Check if messages were received
        msg_count = subscriber.get_message_count()
        if msg_count > 0:
            print(f"Processed {msg_count} alerts so far")
            
except KeyboardInterrupt:
    print("\nStopping subscriber...")

finally:
    subscriber.stop()
    print(f"Total alerts processed: {subscriber.get_message_count()}")
```

### Example 6: Complete Producer-Consumer Pattern

```python
from redis_datapubsub import (
    RedisChannelDataPublisher,
    RedisChannelDataSubscriber
)
import threading
import time
import random

# Configuration
config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0
}

# Create publisher and subscriber
publisher = RedisChannelDataPublisher(
    name='data_producer',
    destination='redischannel://data_stream',
    config=config
)

subscriber = RedisChannelDataSubscriber(
    name='data_consumer',
    source='redischannel://data_stream',
    config=config
)

# Start subscriber first
subscriber.start()
print("Subscriber started, waiting for messages...")

# Publish data in separate thread
def publish_data():
    print("Starting to publish data...")
    for i in range(20):
        data = {
            'id': i,
            'value': random.randint(1, 100),
            'category': random.choice(['A', 'B', 'C']),
            'timestamp': time.time()
        }
        publisher.publish(data)
        print(f"Published message {i}")
        time.sleep(0.5)
    publisher.stop()
    print("Publisher stopped")

# Start publishing
publisher_thread = threading.Thread(target=publish_data)
publisher_thread.start()

# Wait for publishing to complete
publisher_thread.join()

# Give subscriber time to process remaining messages
time.sleep(2)

# Stop subscriber
subscriber.stop()

print(f"\n=== Results ===")
print(f"Total messages processed: {subscriber.get_message_count()}")
```

### Example 7: Multi-Channel Publisher

```python
from redis_datapubsub import RedisChannelDataPublisher
import time

config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0
}

# Create publishers for different channels
channels = {
    'errors': RedisChannelDataPublisher(
        'error_publisher',
        'redischannel://errors',
        config
    ),
    'warnings': RedisChannelDataPublisher(
        'warning_publisher',
        'redischannel://warnings',
        config
    ),
    'info': RedisChannelDataPublisher(
        'info_publisher',
        'redischannel://info',
        config
    )
}

# Publish to different channels based on severity
events = [
    ('errors', {'message': 'Database connection failed', 'code': 'DB001'}),
    ('warnings', {'message': 'High memory usage detected', 'code': 'MEM001'}),
    ('info', {'message': 'User logged in', 'code': 'AUTH001'}),
    ('errors', {'message': 'API timeout', 'code': 'API001'}),
    ('info', {'message': 'Cache refreshed', 'code': 'CACHE001'})
]

for channel, event in events:
    channels[channel].publish(event)
    print(f"Published to {channel}: {event['message']}")
    time.sleep(0.5)

# Stop all publishers
for publisher in channels.values():
    publisher.stop()
```

### Example 8: Pattern-Based Multi-Channel Subscription

```python
from redis_datapubsub import RedisChannelDataSubscriber
import time

config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0
}

# Subscribe to multiple channels
subscribers = []

for channel in ['errors', 'warnings', 'info']:
    subscriber = RedisChannelDataSubscriber(
        name=f'{channel}_subscriber',
        source=f'redischannel://{channel}',
        config=config
    )
    subscriber.start()
    subscribers.append((channel, subscriber))
    print(f"Started subscriber for {channel}")

# Monitor for 15 seconds
print("\nMonitoring channels...")
time.sleep(15)

# Stop all subscribers and show results
print("\n=== Subscription Results ===")
for channel, subscriber in subscribers:
    count = subscriber.get_message_count()
    subscriber.stop()
    print(f"{channel}: {count} messages received")
```

---

## Advanced Features

### 1. Working with Redis Databases

Redis supports 16 separate databases (0-15) for data isolation:

```python
# Use different databases for different environments
environments = {
    'production': {'db': 0},
    'staging': {'db': 1},
    'testing': {'db': 2},
    'development': {'db': 3}
}

for env, db_config in environments.items():
    config = {
        'host': 'localhost',
        'port': 6379,
        'db': db_config['db']
    }
    
    publisher = RedisDataPublisher(
        name=f'{env}_pub',
        destination=f'redis://{env}',
        config=config
    )
    
    # Each environment's data is isolated
    data = {
        '__dagserver_key': 'app:config',
        'environment': env,
        'database': db_config['db']
    }
    
    publisher.publish(data)
    publisher.stop()
```

### 2. Verifying Published Data

```python
import redis
from redis_datapubsub import RedisDataPublisher

# Publish data
config = {'host': 'localhost', 'port': 6379, 'db': 0}
publisher = RedisDataPublisher('test_pub', 'redis://test', config)

data = {
    '__dagserver_key': 'test:key',
    'value': 'test_value'
}
publisher.publish(data)
publisher.stop()

# Verify with direct Redis client
client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
stored_value = client.get('test:key')
print(f"Stored in Redis: {stored_value}")
client.close()
```

### 3. Publisher Performance Monitoring

```python
from redis_datapubsub import RedisDataPublisher
import time
import json

config = {'host': 'localhost', 'port': 6379, 'db': 0}
publisher = RedisDataPublisher('perf_test', 'redis://perf', config)

# Measure publication rate
start_time = time.time()
message_count = 1000

for i in range(message_count):
    data = {
        '__dagserver_key': f'perf:test:{i}',
        'index': i,
        'timestamp': time.time()
    }
    publisher.publish(data)

end_time = time.time()
duration = end_time - start_time
rate = message_count / duration

print(f"Published {message_count} messages in {duration:.2f} seconds")
print(f"Rate: {rate:.2f} messages/second")
print(f"Last publish: {publisher._last_publish}")

publisher.stop()
```

### 4. Channel Message Broadcasting

```python
from redis_datapubsub import RedisChannelDataPublisher
import time

config = {'host': 'localhost', 'port': 6379, 'db': 0}

# Create broadcaster
broadcaster = RedisChannelDataPublisher(
    'broadcaster',
    'redischannel://broadcast',
    config
)

# Broadcast to all subscribers
messages = [
    {'type': 'announcement', 'text': 'System maintenance in 1 hour'},
    {'type': 'update', 'text': 'New features deployed'},
    {'type': 'alert', 'text': 'Security patch applied'}
]

for msg in messages:
    broadcaster.publish(msg)
    print(f"Broadcasted: {msg['text']}")
    time.sleep(1)

broadcaster.stop()
```

### 5. Handling Connection Failures

```python
from redis_datapubsub import RedisDataPublisher
import time

config = {'host': 'localhost', 'port': 6379, 'db': 0}

def publish_with_retry(publisher, data, max_retries=3):
    """Publish with automatic retry on failure"""
    for attempt in range(max_retries):
        try:
            publisher.publish(data)
            return True
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
    return False

publisher = RedisDataPublisher('retry_pub', 'redis://retry', config)

data = {
    '__dagserver_key': 'test:retry',
    'value': 'important_data'
}

if publish_with_retry(publisher, data):
    print("Data published successfully")
else:
    print("Failed to publish after retries")

publisher.stop()
```

### 6. Subscriber Message Processing

```python
from redis_datapubsub import RedisChannelDataSubscriber
import time
import json

class MessageProcessor:
    """Custom message processor"""
    
    def __init__(self):
        self.processed = []
        self.errors = []
    
    def process(self, message):
        """Process incoming message"""
        try:
            # Custom processing logic
            if message.get('priority') == 'high':
                print(f"HIGH PRIORITY: {message.get('text')}")
            self.processed.append(message)
        except Exception as e:
            self.errors.append({'message': message, 'error': str(e)})

config = {'host': 'localhost', 'port': 6379, 'db': 0}
processor = MessageProcessor()

subscriber = RedisChannelDataSubscriber(
    'processor_sub',
    'redischannel://tasks',
    config
)

subscriber.start()

# Let it run and process messages
time.sleep(10)

subscriber.stop()

print(f"Processed: {len(processor.processed)} messages")
print(f"Errors: {len(processor.errors)} errors")
```

### 7. Data Expiration (Using Redis TTL)

```python
import redis
from redis_datapubsub import RedisDataPublisher

config = {'host': 'localhost', 'port': 6379, 'db': 0}

# Publish data
publisher = RedisDataPublisher('ttl_pub', 'redis://ttl', config)

data = {
    '__dagserver_key': 'session:user123',
    'user_id': 'user123',
    'login_time': '2025-10-22T10:00:00Z'
}

publisher.publish(data)
publisher.stop()

# Set expiration using Redis client
client = redis.Redis(host='localhost', port=6379, db=0)
client.expire('session:user123', 3600)  # Expire after 1 hour
ttl = client.ttl('session:user123')
print(f"Key will expire in {ttl} seconds")
client.close()
```

---

## Troubleshooting

### Common Issues and Solutions

#### Issue 1: Connection Refused

**Error:**
```
redis.exceptions.ConnectionError: Error 111 connecting to localhost:6379. Connection refused.
```

**Solutions:**
1. Verify Redis server is running:
```bash
redis-cli ping
```

2. Check Redis server status:
```bash
sudo systemctl status redis-server  # Linux
brew services list | grep redis      # macOS
```

3. Start Redis if not running:
```bash
sudo systemctl start redis-server    # Linux
brew services start redis            # macOS
redis-server                         # Manual start
```

4. Verify host and port in configuration

#### Issue 2: Authentication Required

**Error:**
```
redis.exceptions.AuthenticationError: Authentication required
```

**Solution:**
Add password to configuration:
```python
config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'password': 'your_redis_password'
}
```

#### Issue 3: No __dagserver_key in Data

**Error:**
```
ERROR: No __dagserver_key found in data
```

**Solution:**
Ensure all published data includes the `__dagserver_key` field:
```python
# Incorrect
data = {'value': 123}

# Correct
data = {
    '__dagserver_key': 'my:key',
    'value': 123
}
```

#### Issue 4: Messages Not Received by Subscriber

**Symptoms:**
- Subscriber running but not receiving messages
- No errors in logs

**Solutions:**

1. **Verify channel names match exactly:**
```python
# Publisher
destination='redischannel://alerts'

# Subscriber  
source='redischannel://alerts'  # Must be identical
```

2. **Check subscriber is started before publishing:**
```python
subscriber.start()  # Start first
time.sleep(0.5)     # Brief wait
publisher.publish(data)  # Then publish
```

3. **Test with redis-cli:**
```bash
# Terminal 1 - Subscribe
redis-cli
SUBSCRIBE alerts

# Terminal 2 - Publish
redis-cli
PUBLISH alerts "test message"
```

4. **Check Redis configuration:**
```bash
redis-cli CONFIG GET notify-keyspace-events
```

#### Issue 5: High Memory Usage

**Symptoms:**
- Redis memory increasing
- Slow performance

**Solutions:**

1. **Set maxmemory in Redis config:**
```bash
redis-cli CONFIG SET maxmemory 2gb
redis-cli CONFIG SET maxmemory-policy allkeys-lru
```

2. **Use TTL for automatic expiration:**
```python
import redis

client = redis.Redis(host='localhost', port=6379, db=0)

# Set key with expiration
client.setex('temp:key', 300, 'value')  # Expires in 5 minutes

# Or set TTL on existing key
client.expire('existing:key', 3600)  # 1 hour
```

3. **Monitor memory usage:**
```bash
redis-cli INFO memory
```

4. **Clear specific patterns:**
```python
import redis

client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Find and delete old keys
keys = client.keys('old:pattern:*')
if keys:
    client.delete(*keys)
```

#### Issue 6: Connection Timeout

**Error:**
```
redis.exceptions.TimeoutError: Timeout reading from socket
```

**Solutions:**

1. **Increase timeout in configuration:**
```python
config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'socket_timeout': 10,
    'socket_connect_timeout': 10
}
```

2. **Check network latency:**
```bash
redis-cli --latency
```

3. **Verify Redis is not overloaded:**
```bash
redis-cli INFO stats
```

#### Issue 7: Subscriber Missing Messages

**Symptoms:**
- Some messages not received
- Inconsistent message delivery

**Solutions:**

1. **Check subscriber loop timing:**
```python
# Increase timeout for get_message
# In _do_subscribe method
message = self.pubsub.get_message(timeout=1.0)  # Increase from 0.1
```

2. **Ensure subscriber stays connected:**
```python
subscriber.start()

try:
    while True:
        time.sleep(1)
        # Keep subscriber alive
except KeyboardInterrupt:
    subscriber.stop()
```

3. **Use blocking receive (alternative approach):**
```python
# Modify for blocking behavior if needed
message = self.pubsub.get_message(timeout=None)  # Block until message
```

#### Issue 8: JSON Decode Error

**Error:**
```
json.decoder.JSONDecodeError: Expecting value
```

**Solutions:**

1. **Verify data is valid JSON before publishing:**
```python
import json

def safe_publish(publisher, data):
    try:
        json.dumps(data)  # Test serialization
        publisher.publish(data)
    except (TypeError, ValueError) as e:
        print(f"Invalid data: {e}")
        return False
    return True
```

2. **Handle decode errors in subscriber:**
```python
# In _do_subscribe method
try:
    return json.loads(message['data'])
except json.JSONDecodeError as e:
    logger.error(f"Invalid JSON: {e}")
    return None
```

### Debugging Tips

#### Enable Detailed Logging

```python
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('redis_pubsub.log'),
        logging.StreamHandler()
    ]
)

# Set logger for redis module
logging.getLogger('redis').setLevel(logging.DEBUG)
```

#### Monitor Redis Operations

```bash
# Monitor all commands in real-time
redis-cli MONITOR

# Check slow queries
redis-cli SLOWLOG GET 10

# View connected clients
redis-cli CLIENT LIST
```

#### Test Connection

```python
import redis

try:
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    response = client.ping()
    print(f"Redis connection successful: {response}")
    
    # Test basic operations
    client.set('test:key', 'test:value')
    value = client.get('test:key')
    print(f"Retrieved value: {value}")
    
    client.delete('test:key')
    client.close()
except Exception as e:
    print(f"Connection failed: {e}")
```

---

## Best Practices

### 1. Connection Management

**Always close connections:**
```python
publisher = RedisDataPublisher('pub', 'redis://data', config)
try:
    publisher.publish(data)
finally:
    publisher.stop()  # Ensures cleanup
```

**Use connection pooling for high throughput:**
```python
import redis

# Create connection pool
pool = redis.ConnectionPool(
    host='localhost',
    port=6379,
    db=0,
    max_connections=50
)

# Use pool in configuration (custom implementation)
config = {
    'connection_pool': pool
}
```

### 2. Error Handling

**Implement robust error handling:**
```python
from redis_datapubsub import RedisDataPublisher
import logging

logger = logging.getLogger(__name__)

def safe_publish(publisher, data):
    try:
        publisher.publish(data)
        return True
    except ConnectionError as e:
        logger.error(f"Connection error: {e}")
        # Implement reconnection logic
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
```

### 3. Data Serialization

**Handle complex data types:**
```python
import json
from datetime import datetime
from decimal import Decimal

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

# Use custom encoder
data_str = json.dumps(data, cls=CustomEncoder)
```

### 4. Performance Optimization

**Batch operations when possible:**
```python
import redis

# Use pipeline for multiple operations
client = redis.Redis(host='localhost', port=6379, db=0)
pipeline = client.pipeline()

for i in range(100):
    pipeline.set(f'key:{i}', f'value:{i}')

pipeline.execute()  # Execute all at once
```

**Use appropriate data structures:**
```python
# Instead of many keys
client.set('user:1:name', 'John')
client.set('user:1:email', 'john@example.com')

# Use hash
client.hset('user:1', mapping={
    'name': 'John',
    'email': 'john@example.com'
})
```

### 5. Monitoring and Alerting

**Track publisher metrics:**
```python
class MonitoredPublisher:
    def __init__(self, publisher):
        self.publisher = publisher
        self.failures = 0
        self.successes = 0
    
    def publish(self, data):
        try:
            self.publisher.publish(data)
            self.successes += 1
        except Exception as e:
            self.failures += 1
            if self.failures > 10:
                # Send alert
                send_alert(f"High failure rate: {self.failures}")
            raise
    
    def get_stats(self):
        return {
            'successes': self.successes,
            'failures': self.failures,
            'total': self.successes + self.failures
        }
```

### 6. Security

**Use authentication:**
```python
config = {
    'host': 'redis.example.com',
    'port': 6379,
    'db': 0,
    'password': 'strong_password_here'
}
```

**Use SSL/TLS for remote connections:**
```python
config = {
    'host': 'redis.example.com',
    'port': 6380,
    'db': 0,
    'password': 'password',
    'ssl': True,
    'ssl_cert_reqs': 'required',
    'ssl_ca_certs': '/path/to/ca-cert.pem'
}
```

**Sanitize data before publishing:**
```python
def sanitize_data(data):
    """Remove sensitive information"""
    sensitive_keys = ['password', 'api_key', 'secret']
    return {k: v for k, v in data.items() if k not in sensitive_keys}

safe_data = sanitize_data(user_data)
publisher.publish(safe_data)
```

### 7. Testing

**Unit test example:**
```python
import unittest
from unittest.mock import Mock, patch
from redis_datapubsub import RedisDataPublisher

class TestRedisDataPublisher(unittest.TestCase):
    
    @patch('redis.Redis')
    def test_publish(self, mock_redis):
        config = {'host': 'localhost', 'port': 6379, 'db': 0}
        publisher = RedisDataPublisher('test', 'redis://test', config)
        
        data = {
            '__dagserver_key': 'test:key',
            'value': 'test'
        }
        
        publisher.publish(data)
        
        # Verify Redis set was called
        mock_redis.return_value.set.assert_called_once()
        
        publisher.stop()

if __name__ == '__main__':
    unittest.main()
```

---

## API Reference

### RedisDataPublisher

```python
RedisDataPublisher(name, destination, config)
```

**Parameters:**
- `name` (str): Publisher name for identification and logging
- `destination` (str): Destination URL (format: 'redis://destination_name')
- `config` (dict): Configuration dictionary

**Config Options:**
- `host` (str): Redis server hostname (default: 'localhost')
- `port` (int): Redis server port (default: 6379)
- `db` (int): Redis database number 0-15 (default: 0)
- `password` (str, optional): Redis authentication password
- `socket_timeout` (int, optional): Socket timeout in seconds
- `socket_connect_timeout` (int, optional): Connection timeout in seconds
- `decode_responses` (bool): Auto-decode responses to strings (default: True)

**Methods:**
- `publish(data: dict)`: Publish data to Redis (requires `__dagserver_key` in data)
- `stop()`: Stop publisher and close Redis connection
- `is_running()`: Check if publisher is active

**Attributes:**
- `_publish_count`: Number of successful publications
- `_last_publish`: ISO timestamp of last publication

**Example:**
```python
publisher = RedisDataPublisher(
    name='my_publisher',
    destination='redis://my_data',
    config={'host': 'localhost', 'port': 6379, 'db': 0}
)

data = {
    '__dagserver_key': 'mykey',
    'value': 'myvalue'
}

publisher.publish(data)
publisher.stop()
```

### RedisChannelDataPublisher

```python
RedisChannelDataPublisher(name, destination, config)
```

**Parameters:**
- `name` (str): Publisher name for identification
- `destination` (str): Channel URL (format: 'redischannel://channel_name')
- `config` (dict): Configuration dictionary (same as RedisDataPublisher)

**Methods:**
- `publish(data: dict)`: Publish data to Redis channel
- `stop()`: Stop publisher and close connection

**Attributes:**
- `channel` (str): Channel name extracted from destination
- `_publish_count`: Number of messages published
- `_last_publish`: Timestamp of last publication

**Example:**
```python
publisher = RedisChannelDataPublisher(
    name='channel_pub',
    destination='redischannel://alerts',
    config={'host': 'localhost', 'port': 6379, 'db': 0}
)

publisher.publish({'message': 'Alert!'})
publisher.stop()
```

### RedisChannelDataSubscriber

```python
RedisChannelDataSubscriber(name, source, config)
```

**Parameters:**
- `name` (str): Subscriber name for identification
- `source` (str): Channel URL (format: 'redischannel://channel_name')
- `config` (dict): Configuration dictionary (same as RedisDataPublisher)

**Methods:**
- `start()`: Start subscriber thread
- `stop()`: Stop subscriber and close connection
- `is_running()`: Check if subscriber is active
- `get_message_count()`: Get count of processed messages

**Attributes:**
- `channel` (str): Channel name extracted from source
- `pubsub`: Redis pubsub object
- Message processing runs in background thread

**Example:**
```python
subscriber = RedisChannelDataSubscriber(
    name='channel_sub',
    source='redischannel://alerts',
    config={'host': 'localhost', 'port': 6379, 'db': 0}
)

subscriber.start()
# Messages processed automatically
time.sleep(10)
subscriber.stop()

print(f"Received {subscriber.get_message_count()} messages")
```

---

## Performance Considerations

### Throughput

- **Key-Value Publishing**: ~10,000-50,000 ops/sec (single threaded)
- **Channel Publishing**: ~5,000-20,000 messages/sec
- **Subscription**: ~5,000-20,000 messages/sec

### Latency

- **Local Redis**: <1ms
- **Network Redis**: 1-10ms (depending on network)
- **Cross-region**: 50-200ms

### Optimization Tips

1. **Use pipelining** for bulk operations
2. **Enable connection pooling** for concurrent access
3. **Use appropriate data structures** (hashes, sets, sorted sets)
4. **Set TTL** on temporary data to prevent memory bloat
5. **Monitor memory usage** and set maxmemory limits
6. **Use Redis Cluster** for horizontal scaling

---

## Comparison: Redis vs AshRedis

| Feature | Redis | AshRedis |
|---------|-------|----------|
| Protocol | Redis Protocol | Redis-compatible |
| Pub/Sub | Native PUBLISH/SUBSCRIBE | Custom channel mechanism |
| Regions | Database 0-15 | Named regions |
| TTL Support | EXPIRE command | Built-in SET EX |
| Clustering | Redis Cluster | Multi-region |
| Performance | High | Moderate |
| Use Case | General purpose | Multi-region apps |

---

## Additional Resources

- **Redis Documentation**: https://redis.io/documentation
- **redis-py Documentation**: https://redis-py.readthedocs.io/
- **Redis Commands**: https://redis.io/commands
- **Redis Pub/Sub**: https://redis.io/topics/pubsub
- **Redis Best Practices**: https://redis.io/topics/best-practices

---

## Support

For issues and questions:

1. Check the [Troubleshooting](#troubleshooting) section
2. Review Redis server logs:
   ```bash
   tail -f /var/log/redis/redis-server.log
   ```
3. Enable DEBUG logging in your application
4. Test with `redis-cli` to isolate issues
5. Check Redis server status and configuration

---

## Appendix

### Quick Command Reference

```bash
# Redis Server
redis-server                    # Start server
redis-cli ping                  # Test connection
redis-cli INFO                  # Server info
redis-cli SHUTDOWN              # Stop server

# Data Operations
redis-cli SET key value         # Set key
redis-cli GET key               # Get key
redis-cli DEL key               # Delete key
redis-cli KEYS pattern          # Find keys
redis-cli TTL key               # Check expiration

# Pub/Sub Operations
redis-cli SUBSCRIBE channel     # Subscribe
redis-cli PUBLISH channel msg   # Publish
redis-cli PUBSUB CHANNELS       # List channels

# Database Operations
redis-cli SELECT 1              # Switch database
redis-cli FLUSHDB               # Clear current database
redis-cli FLUSHALL              # Clear all databases
```

### Configuration Template

```python
# Development
DEV_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'db': 0
}

# Staging
STAGING_CONFIG = {
    'host': 'redis-staging.example.com',
    'port': 6379,
    'db': 0,
    'password': 'staging_password',
    'socket_timeout': 5
}

# Production
PROD_CONFIG = {
    'host': 'redis-prod.example.com',
    'port': 6380,
    'db': 0,
    'password': 'production_password',
    'ssl': True,
    'ssl_cert_reqs': 'required',
    'socket_timeout': 10,
    'socket_connect_timeout': 10
}
```

---

**Version:** 1.0  
**Last Updated:** October 22, 2025  
**Author:** DAG Server Development Team

## SSL/TLS Security

Enable TLS (the `rediss://` equivalent) with `"use_ssl": true`. Certificate
options are paths to PEM files.

| Key | Purpose |
| --- | --- |
| `use_ssl` | Enable TLS (`true`/`false`, default `false`) |
| `ssl_ca_certs` | CA bundle to verify the server |
| `ssl_certfile` | Client certificate (mutual TLS) |
| `ssl_keyfile` | Client private key (mutual TLS) |
| `ssl_cert_reqs` | `required` (default), `optional`, or `none` |

```json
{
  "destination": "redis://orders_channel",
  "host": "redis.internal",
  "port": 6380,
  "password": "${REDIS_PASSWORD}",
  "use_ssl": true,
  "ssl_ca_certs": "/certs/ca.pem",
  "ssl_cert_reqs": "required"
}
```


---

# SQL DataPublisher and DataSubscriber Setup Guide

## © 2025-2030 Ashutosh Sinha


## Overview

The SQL implementation provides publisher and subscriber classes for database-based messaging. Publishers insert messages as rows into database tables, and subscribers poll and retrieve them. This pattern is ideal for database-driven workflows, event sourcing with SQL storage, integrating with existing database systems, and scenarios where you need queryable message history.

## Files

1. **sql_datapubsub.py** - Contains `SQLDataPublisher` and `SQLDataSubscriber` classes
2. **pubsubfactory.py** - Factory methods support `sql://` destinations/sources

## Prerequisites

### Install Database Server

**MySQL:**
```bash
# Using Docker
docker run -d \
  --name mysql \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=rootpass \
  -e MYSQL_DATABASE=messaging \
  -e MYSQL_USER=msguser \
  -e MYSQL_PASSWORD=msgpass \
  mysql:8.0

# Or install locally (Ubuntu/Debian)
sudo apt-get install mysql-server
```

**PostgreSQL:**
```bash
# Using Docker
docker run -d \
  --name postgres \
  -p 5432:5432 \
  -e POSTGRES_DB=messaging \
  -e POSTGRES_USER=msguser \
  -e POSTGRES_PASSWORD=msgpass \
  postgres:15

# Or install locally (Ubuntu/Debian)
sudo apt-get install postgresql
```

### Install Python Libraries

```bash
# For MySQL
pip install pymysql

# For PostgreSQL
pip install psycopg2-binary

# Or both
pip install pymysql psycopg2-binary
```

## SQL-Based Messaging Concepts

### How It Works

**Publisher:**
- Inserts messages as rows into a database table
- Uses parameterized INSERT statements
- Supports batch publishing
- Thread-safe inserts

**Subscriber:**
- Polls database table for new messages
- Uses SELECT statements to retrieve rows
- Typically uses a status column or timestamp for tracking
- Deletes or marks messages as processed

### Database Schema Design

**Typical message table:**
```sql
CREATE TABLE messages (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    data JSON,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP NULL
);

-- Or with individual columns
CREATE TABLE orders (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_id VARCHAR(50),
    customer_id INT,
    amount DECIMAL(10, 2),
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Configuration File

SQL connection details are stored in a JSON configuration file:

**mysql_config.json:**
```json
{
    "db_type": "mysql",
    "host": "localhost",
    "port": 3306,
    "user": "msguser",
    "password": "msgpass",
    "database": "messaging",
    "insert_statement": "INSERT INTO messages (data, status) VALUES (%(data)s, 'pending')",
    "select_statement": "SELECT * FROM messages WHERE status = 'pending' LIMIT 1"
}
```

**postgres_config.json:**
```json
{
    "db_type": "postgres",
    "host": "localhost",
    "port": 5432,
    "user": "msguser",
    "password": "msgpass",
    "database": "messaging",
    "insert_statement": "INSERT INTO messages (data, status) VALUES (%(data)s, 'pending')",
    "select_statement": "SELECT * FROM messages WHERE status = 'pending' LIMIT 1"
}
```

### Use Cases

**Good for:**
- Database-driven workflows
- Event sourcing with SQL storage
- Integration with existing database systems
- Queryable message history
- ACID transaction requirements
- Complex message queries
- Audit trails with SQL queries

**Not suitable for:**
- High-frequency messaging (use dedicated message brokers)
- Real-time low-latency requirements
- Simple pub/sub (use message brokers)
- Large message volumes (database performance limits)

## Usage

### Setup Database Table

**MySQL:**
```sql
CREATE DATABASE IF NOT EXISTS messaging;
USE messaging;

CREATE TABLE messages (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_type VARCHAR(100),
    event_data JSON,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP NULL,
    INDEX idx_status (status),
    INDEX idx_created (created_at)
);
```

**PostgreSQL:**
```sql
CREATE DATABASE messaging;
\c messaging

CREATE TABLE messages (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(100),
    event_data JSONB,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP NULL
);

CREATE INDEX idx_status ON messages(status);
CREATE INDEX idx_created ON messages(created_at);
```

### Publishing to Database

Basic database publishing:

```python
from core.pubsub.pubsubfactory import create_publisher
import json

# Create SQL config file
sql_config = {
    "db_type": "mysql",
    "host": "localhost",
    "port": 3306,
    "user": "msguser",
    "password": "msgpass",
    "database": "messaging",
    "insert_statement": "INSERT INTO messages (event_type, event_data, status) VALUES (%(event_type)s, %(event_data)s, 'pending')"
}

with open('sql_config.json', 'w') as f:
    json.dump(sql_config, f)

# Create publisher
config = {
    'destination': 'sql://messages',
    'sql_config_file': 'sql_config.json'
}

publisher = create_publisher('db_publisher', config)

# Publish data
publisher.publish({
    'event_type': 'user_login',
    'event_data': json.dumps({'user_id': 123, 'ip': '192.168.1.1'})
})

publisher.stop()
```

### Publishing with JSON Column

For JSON/JSONB columns:

```python
# MySQL JSON column
sql_config = {
    "db_type": "mysql",
    "insert_statement": "INSERT INTO events (event_name, payload) VALUES (%(event_name)s, %(payload)s)"
}

publisher.publish({
    'event_name': 'order_created',
    'payload': json.dumps({'order_id': 'ORD-123', 'total': 99.99})
})
```

### Subscribing from Database

Basic database subscription:

```python
from core.pubsub.pubsubfactory import create_subscriber
import json

# Create SQL config file
sql_config = {
    "db_type": "mysql",
    "host": "localhost",
    "port": 3306,
    "user": "msguser",
    "password": "msgpass",
    "database": "messaging",
    "select_statement": "SELECT * FROM messages WHERE status = 'pending' ORDER BY id LIMIT 1"
}

with open('sql_config.json', 'w') as f:
    json.dump(sql_config, f)

# Create subscriber
config = {
    'source': 'sql://messages',
    'sql_config_file': 'sql_config.json',
    'poll_interval': 1  # Poll every second
}

subscriber = create_subscriber('db_subscriber', config)
subscriber.start()

# Receive messages
data = subscriber.get_data(block_time=5)
if data:
    print(f"Received: {data}")
    
    # Mark as processed (separate update needed)
    # connection.execute("UPDATE messages SET status = 'processed' WHERE id = ?", data['id'])

subscriber.stop()
```

### Subscribing with Auto-Delete

Select and delete in one query:

```sql
-- MySQL (not supported directly, need separate DELETE)
-- PostgreSQL supports RETURNING
DELETE FROM messages 
WHERE id = (
    SELECT id FROM messages 
    WHERE status = 'pending' 
    ORDER BY id LIMIT 1
)
RETURNING *;
```

## Destination/Source Format

### SQL URL Format
```
sql://table_name
```

**Examples:**
- `sql://messages`
- `sql://orders`
- `sql://events`
- `sql://audit_log`

**Note**: The actual table structure and columns are defined in the SQL config file.

## Configuration Options

### Publisher Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `destination` | string | Required | SQL destination with `sql://` prefix |
| `sql_config_file` | string | Required | Path to SQL config JSON file |
| `publish_interval` | int | `0` | Batch publishing interval (inherited) |
| `batch_size` | int | `None` | Batch size threshold (inherited) |

### Subscriber Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source` | string | Required | SQL source with `sql://` prefix |
| `sql_config_file` | string | Required | Path to SQL config JSON file |
| `poll_interval` | float | `5.0` | Database polling interval in seconds |
| `max_depth` | int | `100000` | Internal queue maximum size (inherited) |

### SQL Config File Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `db_type` | string | Yes | Database type: `mysql` or `postgres` |
| `host` | string | Yes | Database host |
| `port` | int | Yes | Database port (3306 for MySQL, 5432 for PostgreSQL) |
| `user` | string | Yes | Database username |
| `password` | string | Yes | Database password |
| `database` | string | Yes | Database name |
| `insert_statement` | string | Yes (Publisher) | Parameterized INSERT statement |
| `select_statement` | string | Yes (Subscriber) | SELECT statement for retrieving messages |

## Common Patterns

### Pattern 1: Simple Message Queue

Basic message queue using database:

```python
# Database setup
"""
CREATE TABLE job_queue (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    job_type VARCHAR(50),
    job_data JSON,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_status (status)
);
"""

# Publisher config
sql_config = {
    "db_type": "mysql",
    "host": "localhost",
    "port": 3306,
    "user": "msguser",
    "password": "msgpass",
    "database": "messaging",
    "insert_statement": "INSERT INTO job_queue (job_type, job_data, status) VALUES (%(job_type)s, %(job_data)s, 'pending')"
}

# Publish jobs
publisher = create_publisher('job_pub', {
    'destination': 'sql://job_queue',
    'sql_config_file': 'sql_config.json'
})

publisher.publish({
    'job_type': 'send_email',
    'job_data': json.dumps({'to': 'user@example.com', 'subject': 'Welcome'})
})
```

### Pattern 2: Event Sourcing

Store all events in database:

```python
# Event store table
"""
CREATE TABLE event_store (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    aggregate_id VARCHAR(100),
    event_type VARCHAR(100),
    event_data JSON,
    version INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_aggregate (aggregate_id, version)
);
"""

# Store events
sql_config = {
    "insert_statement": "INSERT INTO event_store (aggregate_id, event_type, event_data, version) VALUES (%(aggregate_id)s, %(event_type)s, %(event_data)s, %(version)s)"
}

event_store = create_publisher('event_store', {
    'destination': 'sql://event_store',
    'sql_config_file': 'sql_config.json'
})

# Store domain events
event_store.publish({
    'aggregate_id': 'ORDER-123',
    'event_type': 'OrderCreated',
    'event_data': json.dumps({'total': 99.99, 'items': 3}),
    'version': 1
})
```

### Pattern 3: Status-Based Processing

Use status column for workflow:

```python
# Orders table with status
"""
CREATE TABLE orders (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_id VARCHAR(50),
    customer_id INT,
    amount DECIMAL(10, 2),
    status VARCHAR(20) DEFAULT 'new',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP NULL,
    INDEX idx_status (status)
);
"""

# Subscriber selects 'new' orders
sql_config = {
    "select_statement": "SELECT * FROM orders WHERE status = 'new' ORDER BY created_at LIMIT 1"
}

processor = create_subscriber('order_processor', {
    'source': 'sql://orders',
    'sql_config_file': 'sql_config.json',
    'poll_interval': 2
})
processor.start()

# After processing, update status
# UPDATE orders SET status = 'processed', processed_at = NOW() WHERE id = ?
```

### Pattern 4: Priority Queue

Use priority column:

```python
# Table with priority
"""
CREATE TABLE tasks (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    task_type VARCHAR(50),
    task_data JSON,
    priority INT DEFAULT 5,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_priority_status (status, priority DESC, created_at)
);
"""

# Select highest priority first
sql_config = {
    "select_statement": "SELECT * FROM tasks WHERE status = 'pending' ORDER BY priority DESC, created_at LIMIT 1"
}
```

### Pattern 5: Change Data Capture (CDC)

Track database changes:

```python
# CDC table
"""
CREATE TABLE user_changes (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id INT,
    change_type VARCHAR(20),
    old_data JSON,
    new_data JSON,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    synced BOOLEAN DEFAULT FALSE
);
"""

# Capture changes
cdc_publisher = create_publisher('cdc', {
    'destination': 'sql://user_changes',
    'sql_config_file': 'sql_config.json'
})

cdc_publisher.publish({
    'user_id': 123,
    'change_type': 'UPDATE',
    'old_data': json.dumps({'email': 'old@example.com'}),
    'new_data': json.dumps({'email': 'new@example.com'})
})

# Sync changes
sql_config = {
    "select_statement": "SELECT * FROM user_changes WHERE synced = FALSE ORDER BY id LIMIT 1"
}
```

## Message Processing Patterns

### Pattern 1: Mark as Processed

Update status after processing:

```python
import pymysql

# Get message
data = subscriber.get_data(block_time=5)
if data:
    # Process message
    process_message(data)
    
    # Mark as processed
    conn = pymysql.connect(host='localhost', user='msguser', 
                           password='msgpass', database='messaging')
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE messages SET status = 'processed', processed_at = NOW() WHERE id = %s",
        (data['id'],)
    )
    conn.commit()
    conn.close()
```

### Pattern 2: Delete After Processing

Remove message after processing:

```python
# Process and delete
data = subscriber.get_data(block_time=5)
if data:
    process_message(data)
    
    conn = pymysql.connect(...)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM messages WHERE id = %s", (data['id'],))
    conn.commit()
    conn.close()
```

### Pattern 3: Move to Archive

Move processed messages to archive table:

```python
"""
CREATE TABLE messages_archive LIKE messages;
"""

# Process and archive
data = subscriber.get_data(block_time=5)
if data:
    process_message(data)
    
    conn = pymysql.connect(...)
    cursor = conn.cursor()
    
    # Insert into archive
    cursor.execute(
        "INSERT INTO messages_archive SELECT * FROM messages WHERE id = %s",
        (data['id'],)
    )
    
    # Delete from main table
    cursor.execute("DELETE FROM messages WHERE id = %s", (data['id'],))
    
    conn.commit()
    conn.close()
```

## Monitoring

### Get Publisher Statistics

```python
stats = publisher.details()
print(f"Name: {stats['name']}")
print(f"Destination: {stats['destination']}")
print(f"Published: {stats['publish_count']} messages")
print(f"Last publish: {stats['last_publish']}")
```

### Get Subscriber Statistics

```python
stats = subscriber.details()
print(f"Name: {stats['name']}")
print(f"Source: {stats['source']}")
print(f"Received: {stats['receive_count']} messages")
print(f"Last receive: {stats['last_receive']}")
```

### Query Database Statistics

```sql
-- Message counts by status
SELECT status, COUNT(*) as count
FROM messages
GROUP BY status;

-- Average processing time
SELECT AVG(TIMESTAMPDIFF(SECOND, created_at, processed_at)) as avg_seconds
FROM messages
WHERE status = 'processed';

-- Pending messages
SELECT COUNT(*) as pending_count
FROM messages
WHERE status = 'pending';

-- Oldest pending message
SELECT TIMESTAMPDIFF(SECOND, created_at, NOW()) as age_seconds
FROM messages
WHERE status = 'pending'
ORDER BY created_at
LIMIT 1;
```

## Troubleshooting

### Connection Failed

**Error**: `Can't connect to MySQL server`

**Solution**: Check database is running and credentials
```bash
# Test MySQL connection
mysql -h localhost -u msguser -p

# Test PostgreSQL connection
psql -h localhost -U msguser -d messaging
```

### Table Not Found

**Error**: `Table 'messaging.messages' doesn't exist`

**Solution**: Create the table
```sql
CREATE TABLE messages (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    data JSON,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Column Missing in INSERT

**Error**: `Unknown column 'event_data' in 'field list'`

**Solution**: Ensure INSERT statement matches table structure
```python
# Check table structure
DESCRIBE messages;

# Update insert_statement to match
"insert_statement": "INSERT INTO messages (column1, column2) VALUES (%(column1)s, %(column2)s)"
```

### No Messages Retrieved

**Problem**: Subscriber returns no data

**Solution**: Check SELECT statement and data
```sql
-- Verify there are pending messages
SELECT * FROM messages WHERE status = 'pending';

-- Check SELECT statement matches
SELECT * FROM messages WHERE status = 'pending' LIMIT 1;
```

### JSON Serialization Error

**Error**: `Object of type datetime is not JSON serializable`

**Solution**: Convert to string before publishing
```python
from datetime import datetime

# Wrong
publisher.publish({
    'timestamp': datetime.now()  # ❌ Error
})

# Correct
publisher.publish({
    'timestamp': datetime.now().isoformat()  # ✅ Good
})
```

### Slow Performance

**Problem**: Polling is slow or inserts are slow

**Solution**: Add indexes and optimize queries
```sql
-- Add indexes for faster queries
CREATE INDEX idx_status ON messages(status);
CREATE INDEX idx_created ON messages(created_at);
CREATE INDEX idx_status_created ON messages(status, created_at);

-- Analyze query performance
EXPLAIN SELECT * FROM messages WHERE status = 'pending' ORDER BY created_at LIMIT 1;
```

## Best Practices

1. **Use indexes** - Index status columns and timestamp columns
2. **Implement cleanup** - Archive or delete old messages
3. **Use status columns** - Track message lifecycle
4. **Handle duplicates** - Use unique constraints where appropriate
5. **Batch operations** - Use `publish_interval` for better performance
6. **Connection pooling** - Reuse database connections
7. **Transaction safety** - Consider ACID requirements
8. **Monitor queue depth** - Alert on growing backlogs
9. **Set poll intervals wisely** - Balance latency vs database load
10. **Archive old data** - Move processed messages to archive tables

## Performance Tips

1. **Batch inserts**: Use `publish_interval` and `batch_size`
2. **Use indexes**: Speed up SELECT queries
3. **Limit result sets**: Always use LIMIT in SELECT
4. **Increase poll interval**: Reduce database load
5. **Use connection pooling**: Reuse connections
6. **Partition tables**: For very large tables
7. **Optimize queries**: Use EXPLAIN to analyze
8. **Archive old data**: Keep active table small
9. **Use appropriate data types**: BIGINT for IDs, JSON/JSONB for payloads
10. **Monitor slow queries**: Use database slow query log

## Complete Examples

### Example 1: Simple Job Queue

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import json
import time
import pymysql

print("=== SQL Job Queue Example ===\n")

# Setup database and table
conn = pymysql.connect(
    host='localhost',
    user='msguser',
    password='msgpass',
    database='messaging'
)
cursor = conn.cursor()

# Create table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS job_queue (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        job_type VARCHAR(50),
        job_data JSON,
        status VARCHAR(20) DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        processed_at TIMESTAMP NULL,
        INDEX idx_status (status)
    )
""")
conn.commit()

# Clear old data
cursor.execute("DELETE FROM job_queue")
conn.commit()
conn.close()

# Create SQL config
sql_config = {
    "db_type": "mysql",
    "host": "localhost",
    "port": 3306,
    "user": "msguser",
    "password": "msgpass",
    "database": "messaging",
    "insert_statement": "INSERT INTO job_queue (job_type, job_data, status) VALUES (%(job_type)s, %(job_data)s, 'pending')",
    "select_statement": "SELECT * FROM job_queue WHERE status = 'pending' ORDER BY id LIMIT 1"
}

with open('sql_config.json', 'w') as f:
    json.dump(sql_config, f)

# Create publisher
publisher = create_publisher('job_publisher', {
    'destination': 'sql://job_queue',
    'sql_config_file': 'sql_config.json'
})

# Create subscriber
subscriber = create_subscriber('job_worker', {
    'source': 'sql://job_queue',
    'sql_config_file': 'sql_config.json',
    'poll_interval': 1
})
subscriber.start()

print("Publishing jobs...\n")

# Publish jobs
jobs = [
    {'job_type': 'send_email', 'job_data': json.dumps({'to': 'user1@example.com', 'subject': 'Welcome'})},
    {'job_type': 'generate_report', 'job_data': json.dumps({'report_id': 'RPT-001', 'format': 'PDF'})},
    {'job_type': 'send_email', 'job_data': json.dumps({'to': 'user2@example.com', 'subject': 'Newsletter'})},
    {'job_type': 'backup_database', 'job_data': json.dumps({'database': 'production', 'type': 'full'})},
    {'job_type': 'send_email', 'job_data': json.dumps({'to': 'admin@example.com', 'subject': 'Alert'})}
]

for job in jobs:
    publisher.publish(job)
    print(f"✓ Published: {job['job_type']}")
    time.sleep(0.3)

publisher.stop()
print(f"\n✓ Published {len(jobs)} jobs\n")

time.sleep(1)

print("Processing jobs...\n")

# Database connection for updates
db_conn = pymysql.connect(
    host='localhost',
    user='msguser',
    password='msgpass',
    database='messaging'
)

# Process jobs
processed = 0
for i in range(len(jobs)):
    job = subscriber.get_data(block_time=3)
    if job:
        job_data = json.loads(job['job_data'])
        print(f"Processing: {job['job_type']}")
        print(f"  Data: {job_data}")
        
        # Simulate processing
        time.sleep(0.5)
        
        # Mark as processed
        cursor = db_conn.cursor()
        cursor.execute(
            "UPDATE job_queue SET status = 'processed', processed_at = NOW() WHERE id = %s",
            (job['id'],)
        )
        db_conn.commit()
        cursor.close()
        
        processed += 1
        print(f"  ✓ Completed\n")

subscriber.stop()
db_conn.close()

print(f"✓ Processed {processed} jobs")
print("\n✓ Example 1 completed!")
```

### Example 2: Event Sourcing

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import json
import time
import pymysql
from datetime import datetime

print("=== Event Sourcing Example ===\n")

# Setup database
conn = pymysql.connect(
    host='localhost',
    user='msguser',
    password='msgpass',
    database='messaging'
)
cursor = conn.cursor()

# Create event store table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS event_store (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        aggregate_id VARCHAR(100),
        event_type VARCHAR(100),
        event_data JSON,
        event_version INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_aggregate (aggregate_id, event_version)
    )
""")
conn.commit()

# Clear old data
cursor.execute("DELETE FROM event_store")
conn.commit()
conn.close()

# SQL config
sql_config = {
    "db_type": "mysql",
    "host": "localhost",
    "port": 3306,
    "user": "msguser",
    "password": "msgpass",
    "database": "messaging",
    "insert_statement": "INSERT INTO event_store (aggregate_id, event_type, event_data, event_version) VALUES (%(aggregate_id)s, %(event_type)s, %(event_data)s, %(event_version)s)",
    "select_statement": "SELECT * FROM event_store ORDER BY id DESC LIMIT 1"
}

with open('event_store_config.json', 'w') as f:
    json.dump(sql_config, f)

# Create event store publisher
event_store = create_publisher('event_store', {
    'destination': 'sql://event_store',
    'sql_config_file': 'event_store_config.json'
})

print("Recording domain events...\n")

# Simulate order lifecycle events
order_id = 'ORDER-12345'
events = [
    {
        'aggregate_id': order_id,
        'event_type': 'OrderCreated',
        'event_data': json.dumps({
            'customer_id': 123,
            'items': [
                {'product': 'Widget', 'quantity': 2, 'price': 29.99},
                {'product': 'Gadget', 'quantity': 1, 'price': 49.99}
            ],
            'total': 109.97
        }),
        'event_version': 1
    },
    {
        'aggregate_id': order_id,
        'event_type': 'PaymentReceived',
        'event_data': json.dumps({
            'payment_method': 'credit_card',
            'amount': 109.97,
            'transaction_id': 'TXN-789'
        }),
        'event_version': 2
    },
    {
        'aggregate_id': order_id,
        'event_type': 'OrderShipped',
        'event_data': json.dumps({
            'tracking_number': 'TRACK-456',
            'carrier': 'FedEx',
            'estimated_delivery': '2025-01-20'
        }),
        'event_version': 3
    },
    {
        'aggregate_id': order_id,
        'event_type': 'OrderDelivered',
        'event_data': json.dumps({
            'delivered_at': '2025-01-19T14:30:00',
            'signature': 'Customer'
        }),
        'event_version': 4
    }
]

for event in events:
    event_store.publish(event)
    event_data = json.loads(event['event_data'])
    print(f"Event {event['event_version']}: {event['event_type']}")
    print(f"  Data: {json.dumps(event_data, indent=2)}\n")
    time.sleep(0.5)

event_store.stop()

print(f"✓ Stored {len(events)} events for {order_id}\n")

# Replay events to reconstruct state
print("Replaying events to reconstruct order state...\n")

conn = pymysql.connect(
    host='localhost',
    user='msguser',
    password='msgpass',
    database='messaging',
    cursorclass=pymysql.cursors.DictCursor
)
cursor = conn.cursor()

cursor.execute(
    "SELECT * FROM event_store WHERE aggregate_id = %s ORDER BY event_version",
    (order_id,)
)

order_state = {}
for event in cursor.fetchall():
    event_data = json.loads(event['event_data'])
    print(f"Applying: {event['event_type']}")
    
    # Build state from events
    if event['event_type'] == 'OrderCreated':
        order_state['order_id'] = order_id
        order_state['customer_id'] = event_data['customer_id']
        order_state['total'] = event_data['total']
        order_state['status'] = 'created'
    elif event['event_type'] == 'PaymentReceived':
        order_state['payment_received'] = True
        order_state['status'] = 'paid'
    elif event['event_type'] == 'OrderShipped':
        order_state['tracking_number'] = event_data['tracking_number']
        order_state['status'] = 'shipped'
    elif event['event_type'] == 'OrderDelivered':
        order_state['delivered_at'] = event_data['delivered_at']
        order_state['status'] = 'delivered'

conn.close()

print(f"\nReconstructed Order State:")
print(json.dumps(order_state, indent=2))

print("\n✓ Example 2 completed!")
```

### Example 3: Priority Queue

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import json
import time
import pymysql
import random

print("=== Priority Queue Example ===\n")

# Setup database
conn = pymysql.connect(
    host='localhost',
    user='msguser',
    password='msgpass',
    database='messaging'
)
cursor = conn.cursor()

# Create tasks table with priority
cursor.execute("""
    CREATE TABLE IF NOT EXISTS priority_tasks (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        task_name VARCHAR(100),
        task_data JSON,
        priority INT DEFAULT 5,
        status VARCHAR(20) DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_priority (status, priority DESC, created_at)
    )
""")
conn.commit()

cursor.execute("DELETE FROM priority_tasks")
conn.commit()
conn.close()

# SQL config - select highest priority first
sql_config = {
    "db_type": "mysql",
    "host": "localhost",
    "port": 3306,
    "user": "msguser",
    "password": "msgpass",
    "database": "messaging",
    "insert_statement": "INSERT INTO priority_tasks (task_name, task_data, priority, status) VALUES (%(task_name)s, %(task_data)s, %(priority)s, 'pending')",
    "select_statement": "SELECT * FROM priority_tasks WHERE status = 'pending' ORDER BY priority DESC, created_at LIMIT 1"
}

with open('priority_config.json', 'w') as f:
    json.dump(sql_config, f)

# Create publisher
publisher = create_publisher('task_pub', {
    'destination': 'sql://priority_tasks',
    'sql_config_file': 'priority_config.json'
})

# Create subscriber
subscriber = create_subscriber('task_worker', {
    'source': 'sql://priority_tasks',
    'sql_config_file': 'priority_config.json',
    'poll_interval': 1
})
subscriber.start()

print("Publishing tasks with different priorities...\n")

# Publish tasks with random priorities
tasks = []
for i in range(10):
    priority = random.choice([1, 3, 5, 7, 10])
    task = {
        'task_name': f'Task-{i:02d}',
        'task_data': json.dumps({'action': 'process', 'item_id': i}),
        'priority': priority
    }
    tasks.append(task)
    publisher.publish(task)
    
    priority_label = {10: 'CRITICAL', 7: 'HIGH', 5: 'MEDIUM', 3: 'LOW', 1: 'VERY LOW'}
    print(f"Published: {task['task_name']} - Priority {priority} ({priority_label[priority]})")
    time.sleep(0.2)

publisher.stop()
print(f"\n✓ Published {len(tasks)} tasks\n")

time.sleep(1)

print("Processing tasks in priority order...\n")

# Database connection for updates
db_conn = pymysql.connect(
    host='localhost',
    user='msguser',
    password='msgpass',
    database='messaging',
    cursorclass=pymysql.cursors.DictCursor
)

# Process tasks
for i in range(len(tasks)):
    task = subscriber.get_data(block_time=3)
    if task:
        task_data = json.loads(task['task_data'])
        priority_label = {10: 'CRITICAL', 7: 'HIGH', 5: 'MEDIUM', 3: 'LOW', 1: 'VERY LOW'}
        
        print(f"Processing: {task['task_name']}")
        print(f"  Priority: {task['priority']} ({priority_label.get(task['priority'], 'UNKNOWN')})")
        print(f"  Data: {task_data}")
        
        # Simulate processing
        time.sleep(0.3)
        
        # Mark as processed
        cursor = db_conn.cursor()
        cursor.execute(
            "UPDATE priority_tasks SET status = 'processed' WHERE id = %s",
            (task['id'],)
        )
        db_conn.commit()
        cursor.close()
        
        print(f"  ✓ Completed\n")

subscriber.stop()
db_conn.close()

print("✓ Example 3 completed!")
```

### Example 4: Change Data Capture

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import json
import time
import pymysql
from datetime import datetime

print("=== Change Data Capture Example ===\n")

# Setup database
conn = pymysql.connect(
    host='localhost',
    user='msguser',
    password='msgpass',
    database='messaging'
)
cursor = conn.cursor()

# Create CDC table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_changes (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        user_id INT,
        change_type VARCHAR(20),
        old_data JSON,
        new_data JSON,
        changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        synced BOOLEAN DEFAULT FALSE,
        INDEX idx_synced (synced)
    )
""")
conn.commit()

cursor.execute("DELETE FROM user_changes")
conn.commit()
conn.close()

# SQL config
sql_config = {
    "db_type": "mysql",
    "host": "localhost",
    "port": 3306,
    "user": "msguser",
    "password": "msgpass",
    "database": "messaging",
    "insert_statement": "INSERT INTO user_changes (user_id, change_type, old_data, new_data, synced) VALUES (%(user_id)s, %(change_type)s, %(old_data)s, %(new_data)s, FALSE)",
    "select_statement": "SELECT * FROM user_changes WHERE synced = FALSE ORDER BY id LIMIT 1"
}

with open('cdc_config.json', 'w') as f:
    json.dump(sql_config, f)

# CDC publisher
cdc_pub = create_publisher('cdc_publisher', {
    'destination': 'sql://user_changes',
    'sql_config_file': 'cdc_config.json'
})

print("Capturing database changes...\n")

# Simulate user changes
changes = [
    {
        'user_id': 123,
        'change_type': 'INSERT',
        'old_data': json.dumps(None),
        'new_data': json.dumps({'name': 'John Doe', 'email': 'john@example.com', 'status': 'active'})
    },
    {
        'user_id': 123,
        'change_type': 'UPDATE',
        'old_data': json.dumps({'email': 'john@example.com'}),
        'new_data': json.dumps({'email': 'john.doe@example.com'})
    },
    {
        'user_id': 456,
        'change_type': 'INSERT',
        'old_data': json.dumps(None),
        'new_data': json.dumps({'name': 'Jane Smith', 'email': 'jane@example.com', 'status': 'active'})
    },
    {
        'user_id': 123,
        'change_type': 'UPDATE',
        'old_data': json.dumps({'status': 'active'}),
        'new_data': json.dumps({'status': 'inactive'})
    }
]

for change in changes:
    cdc_pub.publish(change)
    print(f"Captured: {change['change_type']} for user {change['user_id']}")
    time.sleep(0.3)

cdc_pub.stop()
print(f"\n✓ Captured {len(changes)} changes\n")

time.sleep(1)

# Sync changes to external system
print("Syncing changes to external system...\n")

cdc_sub = create_subscriber('cdc_syncer', {
    'source': 'sql://user_changes',
    'sql_config_file': 'cdc_config.json',
    'poll_interval': 1
})
cdc_sub.start()

db_conn = pymysql.connect(
    host='localhost',
    user='msguser',
    password='msgpass',
    database='messaging',
    cursorclass=pymysql.cursors.DictCursor
)

synced_count = 0
for i in range(len(changes)):
    change = cdc_sub.get_data(block_time=3)
    if change:
        print(f"Syncing: {change['change_type']} for user {change['user_id']}")
        
        # Simulate sync to external system
        time.sleep(0.3)
        
        # Mark as synced
        cursor = db_conn.cursor()
        cursor.execute(
            "UPDATE user_changes SET synced = TRUE WHERE id = %s",
            (change['id'],)
        )
        db_conn.commit()
        cursor.close()
        
        synced_count += 1
        print(f"  ✓ Synced\n")

cdc_sub.stop()
db_conn.close()

print(f"✓ Synced {synced_count} changes")
print("\n✓ Example 4 completed!")
```

## Comparison with Other Implementations

| Feature | SQL | File | ActiveMQ | Kafka | RabbitMQ |
|---------|-----|------|----------|-------|----------|
| Setup | Database required | None | Broker | Broker | Broker |
| Dependencies | pymysql/psycopg2 | None | stomp.py | kafka-python | pika |
| Persistence | Always | Always | Optional | Always | Optional |
| Queryable | ✅ SQL queries | ❌ No | ❌ No | ❌ No | ❌ No |
| ACID | ✅ Yes | ❌ No | ❌ No | ❌ No | ❌ No |
| Throughput | Low-Medium | Low-Medium | Medium | Very High | Medium-High |
| Latency | High (polling) | Medium | Low | Low | Low |
| Message replay | ✅ Yes | ✅ Yes | ❌ No | ✅ Yes | ❌ No |
| Best for | DB workflows | Logs, pipelines | Traditional MQ | Event streaming | Traditional MQ |

## When to Use SQL-Based Pub/Sub

**Use SQL pub/sub when:**
- Integrating with existing database systems
- Need queryable message history
- ACID transaction requirements
- Complex message queries needed
- Event sourcing with SQL
- Audit trails with SQL queries
- Database-driven workflows

**Use other implementations when:**
- High-throughput messaging needed (use Kafka)
- Low-latency required (use message brokers)
- Real-time processing critical
- No database infrastructure available

## Key Advantages

1. **Queryable History**: Use SQL to query messages
2. **ACID Transactions**: Full database transaction support
3. **Familiar**: Standard SQL and database tools
4. **Integration**: Easy integration with existing systems
5. **Audit**: Built-in audit trail with timestamps
6. **Reporting**: Use SQL for analytics and reporting
7. **Backup**: Standard database backup tools
8. **Security**: Database security and access control

## Key Limitations

1. **Performance**: Lower throughput than dedicated message brokers
2. **Latency**: Polling introduces delays
3. **Scalability**: Limited by database performance
4. **Overhead**: Database overhead for simple messaging

The SQL implementation provides a database-native foundation for workflows requiring queryable message history, ACID transactions, and integration with existing database systems!


## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.


---

# Database Connection Pooling

© 2025-2030 Ashutosh Sinha

DishtaYantra provides enterprise-grade database connection pooling in two
layers: a `DatabaseConnectionPool` (a single pool, Apache-DBCP-style) and a
`DBConnectionPoolManager` (a singleton that manages and reuses many pools by
configuration). This guide covers both.

---

# Part 1 - DatabaseConnectionPool (a single pool)

A comprehensive `DatabaseConnectionPool` class that implements connection pooling similar to Apache DBCP with connection validation, eviction, and replenishment capabilities for multiple database backends.This comprehensive `DatabaseConnectionPool` class that implements enterprise-grade connection pooling similar to Apache DBCP has following key features:


## Key Features

### 1. **Multi-Database Support**
- PostgreSQL, MySQL, SQLite, Oracle, and SQL Server
- Database-specific optimizations and connection handling
- Automatic driver validation

### 2. **Connection Lifecycle Management**
- **Connection States**: IDLE, IN_USE, TESTING, INVALID, CLOSED
- **Automatic Creation**: Creates connections on demand up to max_total
- **Connection Validation**: Tests connections before borrowing, after returning, and while idle
- **Connection Eviction**: Removes stale, idle, or invalid connections

### 3. **Eviction Policies**
- **Idle Time**: Evict connections idle longer than threshold
- **Lifetime**: Evict connections older than max lifetime
- **Soft Min Idle**: Maintain minimum idle connections
- **LRU**: Least recently used eviction

### 4. **Advanced Features**
- **Abandoned Connection Detection**: Identifies and reclaims connections not returned by dead threads
- **Fair Mode**: FIFO queue for waiting threads
- **LIFO/FIFO**: Configurable idle connection retrieval
- **Connection Pooling Statistics**: Comprehensive metrics and monitoring
- **Thread Safety**: Full thread-safe implementation with locks and semaphores

### 5. **Configuration Options (PoolConfig)**
```python
config = PoolConfig(
    # Pool sizing
    min_idle=2,           # Minimum idle connections
    max_idle=8,           # Maximum idle connections  
    max_total=20,         # Maximum total connections
    
    # Validation
    test_on_borrow=True,  # Test before giving to client
    test_on_return=False, # Test when returning
    test_while_idle=True, # Test idle connections periodically
    validation_query=None,# Custom validation query
    
    # Eviction
    time_between_eviction_runs=30,  # Seconds
    min_evictable_idle_time=300,    # Seconds
    max_connection_lifetime=3600,   # Seconds
    
    # Behavior
    block_when_exhausted=True,      # Wait for connections
    max_wait_time=30,                # Max wait seconds
    abandoned_remove=True,          # Remove abandoned
    abandoned_timeout=300            # Abandoned timeout seconds
)
```

## Usage Examples

### Basic Usage with Context Manager
```python
# Create pool
pool = DatabaseConnectionPool(
    DatabaseType.POSTGRESQL,
    config=config,
    host='localhost',
    database='mydb',
    user='user',
    password='pass'
)

# Use connection - automatically returned to pool
with pool.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    results = cursor.fetchall()
    cursor.close()
```

### Manual Connection Management
```python
# Borrow connection
wrapper = pool.borrow_connection(timeout=10)
try:
    conn = wrapper.connection
    # Use connection
    cursor = conn.cursor()
    cursor.execute("INSERT INTO logs (message) VALUES (%s)", ("Test",))
    conn.commit()
finally:
    # Always return connection
    pool.return_connection(wrapper)
```

### Convenience Function
```python
# Quick pool creation with defaults
pool = create_pool(
    DatabaseType.MYSQL,
    min_size=5,
    max_size=20,
    host='localhost',
    database='mydb',
    user='root',
    password='password'
)
```

## Key Capabilities Similar to Apache DBCP

### 1. **Connection Validation**
- Configurable validation queries
- Test on borrow, return, and while idle
- Automatic invalidation of bad connections

### 2. **Eviction Thread**
- Background thread for connection maintenance
- Configurable eviction runs
- Multiple eviction policies

### 3. **Abandoned Connection Handling**
- Detects connections held too long
- Automatic reclamation
- Logging of abandoned connections

### 4. **Comprehensive Statistics**
```python
stats = pool.get_pool_status()
# Returns:
{
    'idle_connections': 5,
    'active_connections': 3,
    'total_connections': 8,
    'connections_created': 10,
    'connections_destroyed': 2,
    'average_wait_time': 0.05,
    'max_wait_time': 0.5
}
```

### 5. **Thread Safety**
- Thread-local connection tracking
- Prevents double-borrowing
- Safe concurrent access

## Advanced Features

### 1. **Connection Wrapper**
- Tracks connection metadata
- Use count and error tracking
- Creation and last-used timestamps
- Transaction state tracking

### 2. **Automatic Recovery**
- Replaces expired connections
- Maintains minimum idle connections
- Handles connection failures gracefully

### 3. **Resource Management**
- Automatic cleanup on pool closure
- Graceful shutdown with timeout
- Connection leak prevention

### 4. **Performance Optimizations**
- Connection reuse
- Batch connection creation
- Efficient idle connection management
- Configurable wait strategies

## Benefits Over Basic Connection Handling

1. **Resource Efficiency**: Reuses connections instead of creating new ones
2. **Performance**: Eliminates connection overhead for each operation
3. **Reliability**: Automatic recovery from connection failures
4. **Monitoring**: Built-in statistics and health checks
5. **Scalability**: Handles high concurrency efficiently
6. **Safety**: Prevents connection leaks and abandoned connections

This implementation provides enterprise-grade connection pooling that matches Apache DBCP's capabilities while being Python-native and supporting multiple database backends. It's production-ready and includes all the essential features for robust database connection management.

---

# Part 2 - DBConnectionPoolManager (managing many pools)

A singleton `DBConnectionPoolManager` class that manages multiple connection pool instances, using connection configurations as keys for pool identification and reuse.This comprehensive singleton `DBConnectionPoolManager` class manages multiple database connection pools efficiently. Here are the key features:


## Key Features

### 1. **Singleton Pattern**
- Ensures only one manager instance exists globally
- Thread-safe initialization
- Accessible via `get_pool_manager()` function

### 2. **Automatic Pool Management**
- Creates new pools based on unique connection configurations
- Reuses existing pools for identical configurations
- Generates unique keys from connection parameters (excluding passwords for security)

### 3. **Connection Configuration**
- `ConnectionConfig` dataclass for clean configuration management
- Supports all database types (PostgreSQL, MySQL, SQLite, Oracle, SQL Server)
- SSL/TLS support with certificates
- Custom application naming

### 4. **Pool Lifecycle Management**
- Automatic cleanup of idle pools
- Configurable idle timeout
- LRU eviction when pool limit reached
- Reference counting for safe pool removal

### 5. **Advanced Features**
- Named pools for custom identification
- Per-pool custom configuration
- Background cleanup thread
- Comprehensive statistics and monitoring
- Health checks across all pools
- Graceful shutdown with cleanup

## Usage Examples

### Basic Usage - Automatic Pool Creation/Reuse
```python
# Get the singleton manager
manager = get_pool_manager()

# First call creates a new pool
pool1 = manager.get_pool(
    db_type='postgresql',
    host='localhost',
    database='mydb',
    user='user',
    password='pass'
)

# Second call with same config returns existing pool
pool2 = manager.get_pool(
    db_type='postgresql',
    host='localhost',
    database='mydb',
    user='user',
    password='pass'
)

assert pool1 is pool2  # Same pool instance
```

### Using ConnectionConfig
```python
# Define configuration
config = ConnectionConfig(
    db_type=DatabaseType.POSTGRESQL,
    host='localhost',
    port=5432,
    database='testdb',
    user='postgres',
    password='password',
    ssl=True,
    application_name='MyApp'
)

# Get pool
pool = manager.get_pool(config)

# Use connection
with pool.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    results = cursor.fetchall()
```

### Direct Connection Access
```python
# Convenience method - get connection directly
with manager.get_connection(
    db_type='mysql',
    host='localhost',
    database='mydb',
    user='root',
    password='password'
) as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM orders")
    count = cursor.fetchone()[0]
```

### Named Pools
```python
# Create named pool for easy reference
analytics_pool = manager.get_pool_by_name(
    'analytics_db',
    ConnectionConfig(
        db_type=DatabaseType.POSTGRESQL,
        host='analytics.server.com',
        database='analytics',
        user='analyst',
        password='secret'
    )
)

# Later, retrieve by name
pool = manager.get_pool_by_name('analytics_db', config)
```

### Custom Pool Configuration
```python
# High-performance pool configuration
high_perf_config = PoolConfig(
    min_idle=10,
    max_idle=50,
    max_total=100,
    test_on_borrow=False,  # Skip validation for performance
    max_wait_time=5
)

# Create pool with custom config
pool = manager.get_pool(
    connection_config=ConnectionConfig(
        db_type=DatabaseType.MYSQL,
        host='high-traffic.server.com',
        database='busy_db',
        user='app',
        password='pass'
    ),
    pool_config=high_perf_config
)
```

## Key Management Features

### 1. **Automatic Pool Reuse**
The manager generates a unique SHA256 hash key from connection parameters:
- Same configuration = same pool
- Password excluded from key for security
- Efficient pool reuse across application

### 2. **Pool Limits and Eviction**
```python
# Set maximum pools
manager.set_max_pools(50)

# When limit reached, LRU idle pool is evicted
# Pools with active connections are protected
```

### 3. **Idle Pool Cleanup**
```python
# Set idle timeout
manager.set_pool_idle_timeout(1800)  # 30 minutes

# Manual cleanup
removed = manager.clear_idle_pools(300)  # Clear pools idle > 5 min

# Automatic cleanup via background thread every 5 minutes
```

### 4. **Monitoring and Statistics**
```python
# Get comprehensive statistics
stats = manager.get_statistics()
# Returns:
{
    "manager_stats": {
        "total_pools": 5,
        "total_pools_created": 10,
        "total_pools_destroyed": 5,
        "total_connections_served": 1000
    },
    "aggregate_pool_stats": {
        "total_idle_connections": 15,
        "total_active_connections": 8,
        "total_connections": 23
    },
    "pools": [...]
}

# Health check all pools
health = manager.health_check()
# Returns:
{
    "status": "healthy",
    "healthy_pools": 5,
    "unhealthy_pools": [],
    "total_pools": 5
}

# Get specific pool info
info = manager.get_pool_info(pool_key)
```

### 5. **Thread Safety**
- All operations are thread-safe
- Multiple threads can request pools concurrently
- Proper locking ensures consistency

## Configuration Options

### Manager Configuration
```python
manager = get_pool_manager()

# Set defaults for all new pools
manager.set_default_pool_config(PoolConfig(
    min_idle=5,
    max_idle=20,
    max_total=50
))

# Set manager limits
manager.set_max_pools(100)
manager.set_pool_idle_timeout(3600)
```

### ConnectionConfig Parameters
```python
config = ConnectionConfig(
    db_type=DatabaseType.POSTGRESQL,
    host='localhost',
    port=5432,
    database='mydb',
    user='user',
    password='pass',
    
    # Optional parameters
    charset='utf8',
    autocommit=False,
    ssl=True,
    ssl_ca='/path/to/ca.pem',
    ssl_cert='/path/to/cert.pem',
    ssl_key='/path/to/key.pem',
    connection_timeout=10,
    socket_timeout=30,
    application_name='MyApp'
)
```

## Benefits

1. **Resource Efficiency**: Single point of management for all database pools
2. **Automatic Reuse**: No duplicate pools for same configuration
3. **Memory Management**: Automatic cleanup of unused pools
4. **Monitoring**: Centralized statistics and health checks
5. **Thread Safety**: Safe concurrent access from multiple threads
6. **Flexibility**: Support for multiple database types and configurations
7. **Production Ready**: Graceful shutdown, error handling, and logging

## Lifecycle Management

### Automatic Cleanup
- Background thread runs every 5 minutes
- Removes pools idle longer than configured timeout
- Preserves pools with active connections

### Graceful Shutdown
```python
# Automatic on application exit via atexit
# Or manual shutdown
manager.shutdown()
```

This implementation provides enterprise-grade connection pool management with the convenience of automatic pool creation and reuse, making it ideal for applications that connect to multiple databases or have dynamic connection requirements.


---

# Aerospike DataPublisher Setup Guide

## Overview

This guide provides instructions for setting up and using the Aerospike-based DataPublisher implementation for the DAG Server framework. Aerospike is a high-performance NoSQL database designed for real-time applications with predictable low latency and high throughput.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Components](#components)
4. [Configuration](#configuration)
5. [Usage Examples](#usage-examples)
6. [Advanced Features](#advanced-features)
7. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Dependencies

- Python 3.7 or higher
- Aerospike server instance running and accessible
- `aerospike` Python client library

### Python Packages

```bash
pip install aerospike
```

**Note:** The Aerospike Python client may require additional system dependencies. See [Aerospike Python Client Installation](https://developer.aerospike.com/client/python/install).

---

## Installation

### 1. Aerospike Server Setup

**Ubuntu/Debian:**
```bash
wget -O aerospike.tgz 'https://www.aerospike.com/download/server/latest/artifact/ubuntu20'
tar -xvf aerospike.tgz
cd aerospike-server-*
sudo ./asinstall
sudo systemctl start aerospike
sudo systemctl enable aerospike
```

**Docker:**
```bash
docker run -d --name aerospike -p 3000:3000 aerospike/aerospike-server
```

**macOS:**
```bash
# Download from Aerospike website and follow instructions
# Or use Docker as shown above
```

### 2. Verify Aerospike Installation

```bash
# Check if server is running
sudo systemctl status aerospike

# Or use asadm tool
asadm -e "info"

# Or with aql
aql
> show namespaces
```

### 3. Python Client Installation

```bash
pip install aerospike
```

**Ubuntu/Debian dependencies:**
```bash
sudo apt-get install python3-dev libssl-dev
```

### 4. Module Installation

Copy `aerospike_datapubsub.py` to your project's appropriate location.

---

## Components

### Available Classes

#### **AerospikeDataPublisher**

Publishes data by writing records to Aerospike database using namespace and set.

**Key Features:**
- High-performance record storage
- Automatic key generation from `__dagserver_key`
- Namespace and set-based organization
- Multi-node cluster support
- Predictable low latency
- Thread-safe operations

**Use Cases:**
- High-velocity data ingestion
- Real-time analytics storage
- Session storage
- User profile management
- IoT data collection
- Time-series data storage

**Architecture:**
```
Data → AerospikeDataPublisher → Aerospike Cluster
                                    ├─ Namespace
                                    └─ Set
                                        └─ Records (with keys)
```

**Note:** Currently only Publisher is implemented. For reading data, use the native Aerospike Python client directly.

---

## Configuration

### Basic Configuration Structure

```python
config = {
    'hosts': [('localhost', 3000)],  # List of (host, port) tuples
}
```

### Destination Format

The destination URL format is:
```
aerospike://namespace/set_name
```

**Components:**
- `namespace`: Aerospike namespace (similar to database)
- `set_name`: Aerospike set (similar to table/collection)

### Configuration Examples

#### Single Node Configuration

```python
config = {
    'hosts': [('localhost', 3000)]
}

destination = 'aerospike://test/sensor_data'
```

#### Multi-Node Cluster Configuration

```python
config = {
    'hosts': [
        ('aerospike-node-1.example.com', 3000),
        ('aerospike-node-2.example.com', 3000),
        ('aerospike-node-3.example.com', 3000)
    ]
}

destination = 'aerospike://production/user_profiles'
```

#### With Connection Policies

```python
# Note: Extended configuration requires custom implementation
config = {
    'hosts': [('localhost', 3000)],
    'policies': {
        'timeout': 1000,  # milliseconds
        'retry': aerospike.POLICY_RETRY_ONCE
    }
}
```

---

## Usage Examples

### Example 1: Basic Data Publishing

```python
from aerospike_datapubsub import AerospikeDataPublisher

# Configuration
config = {
    'hosts': [('localhost', 3000)]
}

# Create publisher
publisher = AerospikeDataPublisher(
    name='sensor_publisher',
    destination='aerospike://test/sensors',
    config=config
)

# Publish sensor data
data = {
    '__dagserver_key': 'sensor_001',
    'temperature': 23.5,
    'humidity': 65.2,
    'pressure': 1013.25,
    'timestamp': '2025-10-22T10:30:00Z',
    'location': 'Building A'
}

publisher.publish(data)

# Stop publisher
publisher.stop()

print(f"Published {publisher._publish_count} records")
```

### Example 2: User Profile Storage

```python
from aerospike_datapubsub import AerospikeDataPublisher

config = {
    'hosts': [('localhost', 3000)]
}

# Create publisher for user profiles
publisher = AerospikeDataPublisher(
    name='user_profile_publisher',
    destination='aerospike://users/profiles',
    config=config
)

# Publish user profile
user_profile = {
    '__dagserver_key': 'user_12345',
    'username': 'johndoe',
    'email': 'john.doe@example.com',
    'first_name': 'John',
    'last_name': 'Doe',
    'age': 30,
    'preferences': {
        'theme': 'dark',
        'language': 'en',
        'notifications': True
    },
    'created_at': '2025-01-15T08:00:00Z',
    'last_login': '2025-10-22T10:30:00Z'
}

publisher.publish(user_profile)
publisher.stop()

print("User profile published successfully")
```

### Example 3: IoT Data Collection

```python
from aerospike_datapubsub import AerospikeDataPublisher
import time
import random

config = {
    'hosts': [('localhost', 3000)]
}

# Create publisher for IoT devices
publisher = AerospikeDataPublisher(
    name='iot_publisher',
    destination='aerospike://iot/device_readings',
    config=config
)

# Simulate IoT device sending data
device_ids = ['device_001', 'device_002', 'device_003']

for i in range(100):
    for device_id in device_ids:
        reading = {
            '__dagserver_key': f'{device_id}_{int(time.time())}',
            'device_id': device_id,
            'reading_id': i,
            'temperature': 20 + random.uniform(-5, 10),
            'battery_level': random.randint(50, 100),
            'signal_strength': random.randint(-90, -30),
            'timestamp': time.time()
        }
        
        publisher.publish(reading)
    
    time.sleep(1)  # Collect data every second

publisher.stop()

print(f"Collected {publisher._publish_count} IoT readings")
```

### Example 4: Session Storage

```python
from aerospike_datapubsub import AerospikeDataPublisher
import uuid
import time

config = {
    'hosts': [('localhost', 3000)]
}

# Publisher for session data
publisher = AerospikeDataPublisher(
    name='session_publisher',
    destination='aerospike://sessions/active',
    config=config
)

def create_session(user_id, ip_address):
    """Create a new session"""
    session_id = str(uuid.uuid4())
    
    session_data = {
        '__dagserver_key': session_id,
        'user_id': user_id,
        'session_id': session_id,
        'ip_address': ip_address,
        'user_agent': 'Mozilla/5.0',
        'created_at': time.time(),
        'last_activity': time.time(),
        'is_active': True
    }
    
    publisher.publish(session_data)
    return session_id

# Create sessions for different users
session_1 = create_session('user_001', '192.168.1.100')
session_2 = create_session('user_002', '192.168.1.101')

print(f"Created sessions: {session_1}, {session_2}")

publisher.stop()
```

### Example 5: Time-Series Data

```python
from aerospike_datapubsub import AerospikeDataPublisher
import time
from datetime import datetime

config = {
    'hosts': [('localhost', 3000)]
}

# Publisher for metrics
publisher = AerospikeDataPublisher(
    name='metrics_publisher',
    destination='aerospike://metrics/system',
    config=config
)

# Publish system metrics over time
for minute in range(60):
    timestamp = int(time.time())
    
    metrics = {
        '__dagserver_key': f'system_metrics_{timestamp}',
        'timestamp': timestamp,
        'datetime': datetime.now().isoformat(),
        'cpu_usage': 45.5 + (minute * 0.5),
        'memory_usage': 60.0 + (minute * 0.3),
        'disk_io': 1000 + (minute * 10),
        'network_rx': 5000 + (minute * 100),
        'network_tx': 3000 + (minute * 80)
    }
    
    publisher.publish(metrics)
    time.sleep(1)  # Collect every second (simulate 1 hour in 1 minute)

publisher.stop()

print(f"Published {publisher._publish_count} metric snapshots")
```

### Example 6: Bulk Data Publishing

```python
from aerospike_datapubsub import AerospikeDataPublisher
import time

config = {
    'hosts': [('localhost', 3000)]
}

publisher = AerospikeDataPublisher(
    name='bulk_publisher',
    destination='aerospike://analytics/events',
    config=config
)

# Generate and publish bulk events
start_time = time.time()
batch_size = 10000

for i in range(batch_size):
    event = {
        '__dagserver_key': f'event_{i}',
        'event_id': i,
        'event_type': 'page_view',
        'user_id': f'user_{i % 100}',
        'page': f'/page/{i % 50}',
        'duration': i % 300,
        'timestamp': time.time()
    }
    
    publisher.publish(event)
    
    if (i + 1) % 1000 == 0:
        print(f"Published {i + 1} events...")

end_time = time.time()
duration = end_time - start_time
rate = batch_size / duration

publisher.stop()

print(f"\n=== Bulk Publishing Results ===")
print(f"Total events: {publisher._publish_count}")
print(f"Duration: {duration:.2f} seconds")
print(f"Rate: {rate:.2f} events/second")
```

### Example 7: Multi-Namespace Publishing

```python
from aerospike_datapubsub import AerospikeDataPublisher

config = {
    'hosts': [('localhost', 3000)]
}

# Create publishers for different namespaces
publishers = {
    'users': AerospikeDataPublisher(
        'user_publisher',
        'aerospike://users/profiles',
        config
    ),
    'orders': AerospikeDataPublisher(
        'order_publisher',
        'aerospike://orders/transactions',
        config
    ),
    'logs': AerospikeDataPublisher(
        'log_publisher',
        'aerospike://logs/application',
        config
    )
}

# Publish to different namespaces
publishers['users'].publish({
    '__dagserver_key': 'user_001',
    'name': 'John Doe',
    'email': 'john@example.com'
})

publishers['orders'].publish({
    '__dagserver_key': 'order_12345',
    'user_id': 'user_001',
    'total': 99.99,
    'status': 'completed'
})

publishers['logs'].publish({
    '__dagserver_key': f'log_{int(time.time())}',
    'level': 'INFO',
    'message': 'Order completed',
    'order_id': 'order_12345'
})

# Stop all publishers
for publisher in publishers.values():
    publisher.stop()

print("Data published to multiple namespaces")
```

---

## Advanced Features

### 1. Reading Published Data

While the publisher writes data, you can read it using the native Aerospike client:

```python
import aerospike

# Connect to Aerospike
config = {'hosts': [('localhost', 3000)]}
client = aerospike.client(config).connect()

# Read a record
key = ('test', 'sensors', 'sensor_001')
(key, metadata, record) = client.get(key)

print(f"Record: {record}")
print(f"Metadata: {metadata}")

# Close connection
client.close()
```

### 2. Querying Data

```python
import aerospike
from aerospike import predicates as p

config = {'hosts': [('localhost', 3000)]}
client = aerospike.client(config).connect()

# Create secondary index (do once)
try:
    client.index_integer_create('test', 'sensors', 'temperature', 'temp_idx')
except:
    pass  # Index may already exist

# Query records
query = client.query('test', 'sensors')
query.select('temperature', 'humidity', 'location')
query.where(p.between('temperature', 20, 25))

results = []
def callback(input_tuple):
    (key, metadata, record) = input_tuple
    results.append(record)

query.foreach(callback)

print(f"Found {len(results)} records with temperature between 20-25°C")

client.close()
```

### 3. Batch Operations

```python
import aerospike

config = {'hosts': [('localhost', 3000)]}
client = aerospike.client(config).connect()

# Batch read multiple keys
keys = [
    ('test', 'sensors', 'sensor_001'),
    ('test', 'sensors', 'sensor_002'),
    ('test', 'sensors', 'sensor_003')
]

records = client.get_many(keys)

for key, metadata, record in records:
    if record:
        print(f"Key: {key[2]}, Data: {record}")

client.close()
```

### 4. Setting Time-To-Live (TTL)

```python
import aerospike

# Modify the publisher to support TTL (custom implementation)
config = {'hosts': [('localhost', 3000)]}
client = aerospike.client(config).connect()

key = ('test', 'cache', 'temp_data')
bins = {'value': 'temporary', 'created': '2025-10-22'}

# Set record with 1 hour TTL
meta = {'ttl': 3600}
client.put(key, bins, meta=meta)

# Check TTL
(key, metadata, record) = client.get(key)
print(f"TTL: {metadata['ttl']} seconds")

client.close()
```

### 5. Working with Sets

```python
import aerospike

config = {'hosts': [('localhost', 3000)]}
client = aerospike.client(config).connect()

# Write to different sets in the same namespace
sets = ['active_users', 'inactive_users', 'deleted_users']

for set_name in sets:
    key = ('users', set_name, f'user_in_{set_name}')
    bins = {'status': set_name, 'timestamp': '2025-10-22'}
    client.put(key, bins)
    print(f"Wrote to set: {set_name}")

client.close()
```

### 6. Performance Monitoring

```python
from aerospike_datapubsub import AerospikeDataPublisher
import time
import statistics

config = {'hosts': [('localhost', 3000)]}
publisher = AerospikeDataPublisher(
    'perf_test',
    'aerospike://test/performance',
    config
)

# Measure latency
latencies = []

for i in range(1000):
    start = time.time()
    
    data = {
        '__dagserver_key': f'perf_test_{i}',
        'value': i,
        'timestamp': time.time()
    }
    
    publisher.publish(data)
    
    latency = (time.time() - start) * 1000  # Convert to ms
    latencies.append(latency)

publisher.stop()

# Statistics
print(f"=== Performance Metrics ===")
print(f"Total operations: {len(latencies)}")
print(f"Mean latency: {statistics.mean(latencies):.2f} ms")
print(f"Median latency: {statistics.median(latencies):.2f} ms")
print(f"Min latency: {min(latencies):.2f} ms")
print(f"Max latency: {max(latencies):.2f} ms")
print(f"95th percentile: {statistics.quantiles(latencies, n=20)[18]:.2f} ms")
```

### 7. Error Handling and Retries

```python
from aerospike_datapubsub import AerospikeDataPublisher
import aerospike
import time

config = {'hosts': [('localhost', 3000)]}

def publish_with_retry(publisher, data, max_retries=3):
    """Publish with retry logic"""
    for attempt in range(max_retries):
        try:
            publisher.publish(data)
            return True
        except aerospike.exception.AerospikeError as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
    return False

publisher = AerospikeDataPublisher(
    'retry_pub',
    'aerospike://test/reliable',
    config
)

data = {
    '__dagserver_key': 'critical_data',
    'value': 'must_be_stored'
}

if publish_with_retry(publisher, data):
    print("Data published successfully")
else:
    print("Failed after all retries")

publisher.stop()
```

---

## Troubleshooting

### Common Issues and Solutions

#### Issue 1: Connection Refused

**Error:**
```
aerospike.exception.ClientError: (-1, 'Failed to connect')
```

**Solutions:**

1. **Verify Aerospike is running:**
```bash
sudo systemctl status aerospike
# or
ps aux | grep asd
```

2. **Check Aerospike logs:**
```bash
sudo tail -f /var/log/aerospike/aerospike.log
```

3. **Test connectivity:**
```bash
telnet localhost 3000
```

4. **Verify network configuration:**
```bash
asadm -e "info network"
```

#### Issue 2: Namespace Not Found

**Error:**
```
aerospike.exception.NamespaceNotFound: (20, 'namespace not found')
```

**Solutions:**

1. **Check available namespaces:**
```bash
asadm -e "show namespaces"
# or with aql
aql
> show namespaces
```

2. **Create namespace in configuration:**
Edit `/etc/aerospike/aerospike.conf`:
```conf
namespace test {
    replication-factor 2
    memory-size 1G
    storage-engine memory
}
```

Then restart:
```bash
sudo systemctl restart aerospike
```

#### Issue 3: No __dagserver_key in Data

**Error:**
```
ERROR: No __dagserver_key found in data
```

**Solution:**
Always include `__dagserver_key` in your data:
```python
# Incorrect
data = {'value': 123}

# Correct
data = {
    '__dagserver_key': 'my_key',
    'value': 123
}
```

#### Issue 4: Python Client Installation Fails

**Error:**
```
error: command 'gcc' failed with exit status 1
```

**Solutions:**

1. **Install build dependencies (Ubuntu/Debian):**
```bash
sudo apt-get update
sudo apt-get install build-essential python3-dev libssl-dev
```

2. **Install build dependencies (CentOS/RHEL):**
```bash
sudo yum groupinstall "Development Tools"
sudo yum install python3-devel openssl-devel
```

3. **Use pre-built wheels:**
```bash
pip install --only-binary :all: aerospike
```

#### Issue 5: Record Too Large

**Error:**
```
aerospike.exception.RecordTooBig: (13, 'record too big')
```

**Solutions:**

1. **Split large records:**
```python
# Instead of one large record
large_data = {'data': 'x' * 2000000}

# Split into multiple records
chunk_size = 100000
for i in range(0, len(large_data['data']), chunk_size):
    chunk = {
        '__dagserver_key': f'data_chunk_{i}',
        'chunk_id': i,
        'data': large_data['data'][i:i+chunk_size]
    }
    publisher.publish(chunk)
```

2. **Increase write-block-size in config:**
```conf
namespace test {
    storage-engine device {
        write-block-size 1M  # Increase from default 128K
    }
}
```

#### Issue 6: Connection Timeout

**Error:**
```
aerospike.exception.Timeout: (9, 'timeout')
```

**Solutions:**

1. **Increase timeout in client config:**
```python
config = {
    'hosts': [('localhost', 3000)],
    'policies': {
        'timeout': 5000  # 5 seconds
    }
}
```

2. **Check server performance:**
```bash
asadm -e "show statistics"
```

3. **Monitor server load:**
```bash
asadm -e "show latencies"
```

#### Issue 7: Cluster Not Formed

**Error:**
```
Cluster size is 0
```

**Solutions:**

1. **Check cluster configuration:**
```bash
asadm -e "info"
```

2. **Verify heartbeat configuration:**
Edit `/etc/aerospike/aerospike.conf`:
```conf
network {
    service {
        address any
        port 3000
    }
    
    heartbeat {
        mode multicast
        multicast-group 239.1.99.222
        port 9918
        interval 150
        timeout 10
    }
}
```

3. **Check firewall rules:**
```bash
sudo ufw status
sudo firewall-cmd --list-all
```

### Debugging Tips

#### Enable Detailed Logging

```python
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger('aerospike_datapubsub')
```

#### Monitor Aerospike Server

```bash
# Real-time monitoring
asadm

# Check specific namespace
asadm -e "show statistics namespace test"

# Monitor operations
watch -n 1 'asadm -e "show statistics namespace test"'
```

#### Test with aql (Aerospike Query Language)

```bash
aql
> INSERT INTO test.sensors (PK, temperature, humidity) VALUES ('sensor_001', 23.5, 65.2)
> SELECT * FROM test.sensors WHERE PK = 'sensor_001'
> DELETE FROM test.sensors WHERE PK = 'sensor_001'
```

---

## Best Practices

### 1. Key Design

**Use meaningful keys:**
```python
# Good key design
'user_12345'
'sensor_temp_001_20251022'
'session_a1b2c3d4'

# Avoid
'key1'
'data'
'record'
```

**Include timestamps for time-series:**
```python
import time

data = {
    '__dagserver_key': f'metric_{int(time.time())}',
    'value': 42
}
```

### 2. Namespace Organization

**Separate data by purpose:**
```
aerospike://production/users       # User data
aerospike://production/orders      # Order data
aerospike://analytics/events       # Analytics events
aerospike://cache/sessions         # Session cache
```

### 3. Error Handling

**Always wrap publish operations:**
```python
try:
    publisher.publish(data)
except Exception as e:
    logger.error(f"Failed to publish: {e}")
    # Handle error (retry, alert, etc.)
```

### 4. Connection Management

**Reuse publisher instances:**
```python
# Good - reuse connection
publisher = AerospikeDataPublisher(...)
for data in data_items:
    publisher.publish(data)
publisher.stop()

# Avoid - creating new connections
for data in data_items:
    publisher = AerospikeDataPublisher(...)  # Expensive!
    publisher.publish(data)
    publisher.stop()
```

### 5. Performance Optimization

**Batch similar operations:**
```python
# Publish related data together
for i in range(batch_size):
    publisher.publish(data_items[i])
```

**Use appropriate bin types:**
```python
# Aerospike is schemaless but use consistent types
data = {
    '__dagserver_key': 'user_001',
    'age': 30,              # integer
    'score': 95.5,          # float
    'name': 'John',         # string
    'active': True,         # boolean
    'tags': ['a', 'b'],     # list
    'meta': {'k': 'v'}      # map
}
```

### 6. Monitoring

**Track publish statistics:**
```python
print(f"Published: {publisher._publish_count}")
print(f"Last publish: {publisher._last_publish}")
```

**Monitor Aerospike health:**
```bash
# Set up monitoring
asadm -e "enable; watch 2"
```

---

## API Reference

### AerospikeDataPublisher

```python
AerospikeDataPublisher(name, destination, config)
```

**Parameters:**
- `name` (str): Publisher name for identification and logging
- `destination` (str): Destination URL (format: 'aerospike://namespace/set_name')
- `config` (dict): Configuration dictionary

**Config Options:**
- `hosts` (list): List of (host, port) tuples (default: [('localhost', 3000)])

**Methods:**
- `publish(data: dict)`: Publish data to Aerospike (requires `__dagserver_key`)
- `stop()`: Stop publisher and close Aerospike connection
- `is_running()`: Check if publisher is active

**Attributes:**
- `namespace` (str): Aerospike namespace extracted from destination
- `set_name` (str): Aerospike set name extracted from destination
- `client`: Aerospike client instance
- `_publish_count` (int): Number of successful publications
- `_last_publish` (str): ISO timestamp of last publication

**Example:**
```python
publisher = AerospikeDataPublisher(
    name='my_publisher',
    destination='aerospike://test/my_set',
    config={'hosts': [('localhost', 3000)]}
)

data = {
    '__dagserver_key': 'my_key',
    'value': 'my_value'
}

publisher.publish(data)
publisher.stop()
```

---

## Performance Considerations

### Throughput

- **Single node**: 100,000+ writes/second
- **Cluster (3 nodes)**: 300,000+ writes/second
- **With replication (RF=2)**: Approximately 50% of non-replicated throughput

### Latency

- **Sub-millisecond**: For in-memory namespaces
- **1-5 milliseconds**: For SSD-backed namespaces
- **Network latency**: Add based on infrastructure

### Optimization Tips

1. **Use in-memory storage** for lowest latency
2. **Enable write-block-size optimization** for SSDs
3. **Tune replication factor** based on durability needs
4. **Use appropriate consistency policies**
5. **Batch operations** when possible
6. **Monitor server metrics** regularly

---

## Comparison: Aerospike vs Redis vs AshRedis

| Feature | Aerospike | Redis | AshRedis |
|---------|-----------|-------|----------|
| Data Model | Key-value, Document | Key-value, Various | Key-value |
| Performance | Very High | High | Moderate |
| Scalability | Horizontal | Vertical/Cluster | Regional |
| Persistence | Yes | Optional | Yes |
| TTL Support | Native | Native | Native |
| Clustering | Native | Redis Cluster | Multi-region |
| Use Case | Big data, High velocity | Cache, Pub/Sub | Regional apps |

---

## Additional Resources

- **Aerospike Documentation**: https://docs.aerospike.com
- **Python Client Guide**: https://developer.aerospike.com/client/python
- **Best Practices**: https://docs.aerospike.com/docs/operations/plan/capacity
- **Architecture**: https://docs.aerospike.com/docs/architecture

---

## Support

For issues and questions:

1. Check the [Troubleshooting](#troubleshooting) section
2. Review Aerospike server logs: `/var/log/aerospike/aerospike.log`
3. Use asadm for diagnostics: `asadm -e "help"`
4. Enable DEBUG logging in your application
5. Check Aerospike documentation and forums

---

**Version:** 1.0  
**Last Updated:** October 22, 2025  
**Author:** DAG Server Development Team


---

