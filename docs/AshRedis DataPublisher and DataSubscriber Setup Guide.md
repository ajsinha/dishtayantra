# AshRedis DataPublisher and DataSubscriber Setup Guide

## Overview

This guide provides instructions for setting up and using AshRedis-based DataPublisher and DataSubscriber implementations for the DAG Server framework. AshRedis is a Redis-compatible server that supports multi-region operations and native pub/sub functionality.

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
- AshRedis server instance running and accessible
- `ashredis_client.py` module
- DAG Server framework with `core.pubsub.datapubsub` module

### Python Packages

```bash
pip install threading
```

Note: The `ashredis_client` module is included with this distribution.

---

## Installation

### 1. AshRedis Server Setup

Ensure you have an AshRedis server instance running:

```bash
# Example: Start AshRedis server
ashredis-server --port 6379
```

### 2. Client Library Installation

Copy the following files to your project:

- `ashredis_client.py` - AshRedis Python client library
- `ashredis_datapubsub.py` - DataPublisher/DataSubscriber implementations

---

## Components

### Available Classes

#### 1. **AshRedisDataPublisher**

Publishes data by setting key-value pairs in AshRedis using the `__dagserver_key` field.

**Key Features:**
- Stores data as JSON in AshRedis
- Supports multi-region operations
- Optional TTL (Time To Live) support
- Thread-safe operations

#### 2. **AshRedisChannelDataPublisher**

Publishes data to AshRedis channels for pub/sub messaging patterns.

**Key Features:**
- Channel-based messaging
- Real-time data distribution
- Multiple subscriber support
- Region-aware publishing

#### 3. **AshRedisChannelDataSubscriber**

Subscribes to AshRedis channels and receives published messages.

**Key Features:**
- Asynchronous message reception
- Automatic reconnection handling
- Thread-safe message queue
- Callback-based message processing

---

## Configuration

### Basic Configuration Structure

```python
config = {
    'host': 'localhost',      # AshRedis server host
    'port': 6379,             # AshRedis server port
    'region': None,           # Optional: specific region name
    'ttl_seconds': None       # Optional: default TTL for keys
}
```

### Publisher Configuration Examples

#### Key-Value Publisher Configuration

```python
# Basic configuration
publisher_config = {
    'host': 'localhost',
    'port': 6379
}

# With region support
publisher_config = {
    'host': 'ashredis.example.com',
    'port': 6379,
    'region': 'us-east-1'
}

# With TTL support
publisher_config = {
    'host': 'localhost',
    'port': 6379,
    'region': 'users',
    'ttl_seconds': 3600  # 1 hour expiration
}
```

#### Channel Publisher Configuration

```python
channel_publisher_config = {
    'host': 'localhost',
    'port': 6379,
    'region': 'notifications'  # Optional
}
```

### Subscriber Configuration Examples

```python
subscriber_config = {
    'host': 'localhost',
    'port': 6379,
    'region': None
}
```

---

## Usage Examples

### Example 1: Basic Key-Value Publishing

```python
from ashredis_datapubsub import AshRedisDataPublisher

# Configuration
config = {
    'host': 'localhost',
    'port': 6379
}

# Create publisher
publisher = AshRedisDataPublisher(
    name='sensor_publisher',
    destination='ashredis://sensor_data',
    config=config
)

# Publish data
data = {
    '__dagserver_key': 'sensor:temperature:001',
    'value': 23.5,
    'unit': 'celsius',
    'timestamp': '2025-10-22T10:30:00Z'
}

publisher.publish(data)

# Stop publisher
publisher.stop()
```

### Example 2: Publishing with TTL

```python
from ashredis_datapubsub import AshRedisDataPublisher

config = {
    'host': 'localhost',
    'port': 6379,
    'ttl_seconds': 300  # 5 minutes default
}

publisher = AshRedisDataPublisher(
    name='cache_publisher',
    destination='ashredis://cache',
    config=config
)

# Data with custom TTL
data = {
    '__dagserver_key': 'cache:session:abc123',
    '__ttl_seconds': 600,  # Override default with 10 minutes
    'user_id': 'user_001',
    'session_data': {'theme': 'dark', 'language': 'en'}
}

publisher.publish(data)
publisher.stop()
```

### Example 3: Multi-Region Publishing

```python
from ashredis_datapubsub import AshRedisDataPublisher

# Publisher for US East region
us_config = {
    'host': 'localhost',
    'port': 6379,
    'region': 'us-east-1'
}

us_publisher = AshRedisDataPublisher(
    name='us_user_publisher',
    destination='ashredis://users',
    config=us_config
)

data = {
    '__dagserver_key': 'user:profile:12345',
    'name': 'John Doe',
    'email': 'john@example.com',
    'region': 'US'
}

us_publisher.publish(data)
us_publisher.stop()
```

### Example 4: Channel-Based Publishing

```python
from ashredis_datapubsub import AshRedisChannelDataPublisher

config = {
    'host': 'localhost',
    'port': 6379
}

# Create channel publisher
publisher = AshRedisChannelDataPublisher(
    name='alerts_publisher',
    destination='ashredischannel://system_alerts',
    config=config
)

# Publish alert
alert_data = {
    'severity': 'high',
    'message': 'CPU usage exceeded 90%',
    'timestamp': '2025-10-22T10:35:00Z',
    'server': 'web-server-01'
}

publisher.publish(alert_data)
publisher.stop()
```

### Example 5: Channel Subscription

```python
from ashredis_datapubsub import AshRedisChannelDataSubscriber
import time

config = {
    'host': 'localhost',
    'port': 6379
}

# Create subscriber
subscriber = AshRedisChannelDataSubscriber(
    name='alerts_subscriber',
    source='ashredischannel://system_alerts',
    config=config
)

# Start subscriber
subscriber.start()

# Process messages
try:
    while True:
        # The subscriber will process messages via the internal mechanism
        time.sleep(1)
        
        # Check subscriber stats
        if subscriber.get_message_count() > 0:
            print(f"Processed {subscriber.get_message_count()} messages")
            
except KeyboardInterrupt:
    print("Stopping subscriber...")
    subscriber.stop()
```

### Example 6: Complete Producer-Consumer Pattern

```python
from ashredis_datapubsub import (
    AshRedisChannelDataPublisher,
    AshRedisChannelDataSubscriber
)
import threading
import time

# Configuration
config = {
    'host': 'localhost',
    'port': 6379
}

# Create publisher and subscriber
publisher = AshRedisChannelDataPublisher(
    name='data_producer',
    destination='ashredischannel://data_stream',
    config=config
)

subscriber = AshRedisChannelDataSubscriber(
    name='data_consumer',
    source='ashredischannel://data_stream',
    config=config
)

# Start subscriber
subscriber.start()

# Publish data in separate thread
def publish_data():
    for i in range(10):
        data = {
            'id': i,
            'value': f'message_{i}',
            'timestamp': time.time()
        }
        publisher.publish(data)
        time.sleep(1)
    publisher.stop()

# Start publishing
publisher_thread = threading.Thread(target=publish_data)
publisher_thread.start()

# Wait and stop
time.sleep(12)
subscriber.stop()
publisher_thread.join()

print(f"Total messages processed: {subscriber.get_message_count()}")
```

---

## Advanced Features

### 1. Region-Specific Operations

AshRedis supports multi-region data storage, allowing you to isolate data by region:

```python
# Store user data in specific regions
regions = ['us-east-1', 'eu-west-1', 'ap-south-1']

for region in regions:
    config = {
        'host': 'localhost',
        'port': 6379,
        'region': region
    }
    
    publisher = AshRedisDataPublisher(
        name=f'{region}_publisher',
        destination=f'ashredis://{region}_data',
        config=config
    )
    
    data = {
        '__dagserver_key': f'user:{region}:123',
        'name': 'User 123',
        'region': region
    }
    
    publisher.publish(data)
    publisher.stop()
```

### 2. Dynamic TTL Management

Control data expiration on a per-message basis:

```python
publisher = AshRedisDataPublisher(
    name='ttl_publisher',
    destination='ashredis://cache',
    config={'host': 'localhost', 'port': 6379, 'ttl_seconds': 300}
)

# Short-lived data (1 minute)
temp_data = {
    '__dagserver_key': 'temp:session',
    '__ttl_seconds': 60,
    'data': 'temporary'
}

# Long-lived data (1 hour)
permanent_data = {
    '__dagserver_key': 'config:app',
    '__ttl_seconds': 3600,
    'data': 'configuration'
}

publisher.publish(temp_data)
publisher.publish(permanent_data)
publisher.stop()
```

### 3. Message Pattern Matching

Use the AshRedis KEYS command for pattern-based retrieval:

```python
from ashredis_client import AshRedisClient

client = AshRedisClient('localhost', 6379)
client.connect()

# Find all sensor keys
sensor_keys = client.keys('sensor:*')
print(f"Found sensors: {sensor_keys}")

# Find temperature sensors only
temp_sensors = client.keys('sensor:temperature:*')
print(f"Temperature sensors: {temp_sensors}")

client.close()
```

### 4. Monitoring Publisher/Subscriber Health

```python
# Check publisher statistics
publisher_stats = {
    'publish_count': publisher._publish_count,
    'last_publish': publisher._last_publish,
    'is_running': publisher.is_running()
}

print(f"Publisher stats: {publisher_stats}")

# Check subscriber statistics
subscriber_stats = {
    'message_count': subscriber.get_message_count(),
    'is_running': subscriber.is_running()
}

print(f"Subscriber stats: {subscriber_stats}")
```

---

## Troubleshooting

### Common Issues and Solutions

#### Issue 1: Connection Refused

**Error:**
```
ConnectionRefusedError: [Errno 111] Connection refused
```

**Solution:**
- Verify AshRedis server is running: `ps aux | grep ashredis`
- Check host and port configuration
- Verify firewall rules allow connections

#### Issue 2: No __dagserver_key in Data

**Error:**
```
ERROR: No __dagserver_key found in data
```

**Solution:**
- Ensure all published data includes `__dagserver_key` field:
```python
data = {
    '__dagserver_key': 'your:key:here',  # Required
    'other': 'data'
}
```

#### Issue 3: Messages Not Received by Subscriber

**Symptoms:**
- Subscriber running but not receiving messages
- Message count remains 0

**Solutions:**
1. Verify channel names match exactly:
```python
# Publisher
destination='ashredischannel://alerts'

# Subscriber
source='ashredischannel://alerts'  # Must match exactly
```

2. Check subscriber is started:
```python
subscriber.start()  # Must call before publishing
```

3. Verify network connectivity between publisher and subscriber

#### Issue 4: Memory Issues with Large Data

**Symptoms:**
- Slow performance
- High memory usage

**Solutions:**
1. Implement data compression:
```python
import gzip
import json

def compress_data(data):
    json_str = json.dumps(data)
    return gzip.compress(json_str.encode('utf-8'))
```

2. Use TTL to auto-expire old data:
```python
config = {
    'host': 'localhost',
    'port': 6379,
    'ttl_seconds': 300  # Auto-expire after 5 minutes
}
```

#### Issue 5: Thread Safety Concerns

**Symptoms:**
- Race conditions
- Inconsistent data

**Solutions:**
- The implementations are thread-safe by design
- Use locks when sharing publishers across threads:
```python
import threading

publisher_lock = threading.Lock()

def safe_publish(publisher, data):
    with publisher_lock:
        publisher.publish(data)
```

### Logging

Enable detailed logging for debugging:

```python
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger('ashredis_datapubsub')
```

---

## Best Practices

1. **Connection Management**
   - Always call `stop()` method when done
   - Use context managers when possible
   - Implement connection pooling for high-throughput scenarios

2. **Error Handling**
   - Wrap publish/subscribe operations in try-except blocks
   - Implement retry logic for transient failures
   - Log errors for debugging

3. **Performance Optimization**
   - Use regions to partition data
   - Set appropriate TTL values to prevent memory bloat
   - Batch operations when possible

4. **Security**
   - Use authentication if AshRedis server supports it
   - Encrypt sensitive data before publishing
   - Implement access controls at the application level

5. **Monitoring**
   - Track publish/subscribe counts
   - Monitor connection health
   - Set up alerts for failures

---

## API Reference

### AshRedisDataPublisher

```python
AshRedisDataPublisher(name, destination, config)
```

**Parameters:**
- `name` (str): Publisher name for identification
- `destination` (str): Destination URL (e.g., 'ashredis://data')
- `config` (dict): Configuration dictionary

**Config Options:**
- `host` (str): AshRedis server host (default: 'localhost')
- `port` (int): AshRedis server port (default: 6379)
- `region` (str): Optional region name (default: None)
- `ttl_seconds` (int): Default TTL for keys (default: None)

**Methods:**
- `publish(data)`: Publish data (requires `__dagserver_key` in data)
- `stop()`: Stop publisher and close connection
- `is_running()`: Check if publisher is active

### AshRedisChannelDataPublisher

```python
AshRedisChannelDataPublisher(name, destination, config)
```

**Parameters:**
- `name` (str): Publisher name
- `destination` (str): Channel URL (e.g., 'ashredischannel://alerts')
- `config` (dict): Configuration dictionary

**Methods:**
- `publish(data)`: Publish data to channel
- `stop()`: Stop publisher

### AshRedisChannelDataSubscriber

```python
AshRedisChannelDataSubscriber(name, source, config)
```

**Parameters:**
- `name` (str): Subscriber name
- `source` (str): Channel URL (e.g., 'ashredischannel://alerts')
- `config` (dict): Configuration dictionary

**Methods:**
- `start()`: Start subscriber
- `stop()`: Stop subscriber
- `get_message_count()`: Get count of processed messages

---

## Additional Resources

- AshRedis Documentation: [AshRedis Docs]
- DAG Server Framework: [DAG Server Guide]
- Redis Protocol Specification: [Redis Protocol]

---

## Support

For issues and questions:
- Check the troubleshooting section
- Review log files for error details
- Contact the development team

---

**Version:** 1.0  
**Last Updated:** October 22, 2025  
**Author:** DAG Server Development Team
