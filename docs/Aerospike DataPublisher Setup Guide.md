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
