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
