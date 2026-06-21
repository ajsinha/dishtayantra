# Advanced Connector Patterns Guide

Meta-connectors and patterns: **Custom connectors, Holder pub/sub, the Message Router components, and Resilient connectors**.

> **Common configuration** (URI schemes, shared subscriber/publisher/node
> parameters, application.yaml) is in the **Configuration Reference Guide**. This
> guide covers connector-specific detail. Each connector is a section below.

---

# Custom DataPublisher and DataSubscriber Setup Guide

## Overview

This guide provides instructions for setting up and using the Custom DataPublisher and DataSubscriber implementations for the DAG Server framework. These classes provide a flexible delegation pattern that allows you to plug in any custom implementation without modifying the core framework.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Concepts](#concepts)
3. [Components](#components)
4. [Configuration](#configuration)
5. [Usage Examples](#usage-examples)
6. [Advanced Patterns](#advanced-patterns)
7. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Dependencies

- Python 3.7 or higher
- DAG Server framework with `core.pubsub.datapubsub` module
- Custom delegate classes (you create these)

### Python Packages

No additional packages required beyond your custom implementation dependencies.

---

## Concepts

### Delegation Pattern

The Custom DataPublisher/DataSubscriber use the **delegation pattern**:

```
User Request
     ↓
CustomDataPublisher/Subscriber (Framework integration)
     ↓
Your Custom Class (Your business logic)
     ↓
External System (Database, API, File, etc.)
```

**Benefits:**
- **Flexibility**: Use any data source or destination
- **Reusability**: Wrap existing code without modification
- **Separation**: Keep framework concerns separate from business logic
- **Testability**: Test delegate independently

### How It Works

1. **You create** a custom class with `publish()` or `subscribe()` methods
2. **CustomDataPublisher/Subscriber** instantiates your class
3. **Framework calls** CustomDataPublisher/Subscriber
4. **Delegation** happens to your custom class
5. **Your class** handles the actual work

---

## Components

### Available Classes

#### 1. **CustomDataPublisher**

Delegates publishing to your custom class.

**Key Features:**
- Instantiates your custom class dynamically
- Passes configuration to your class
- Manages lifecycle
- Tracks publish statistics
- Thread-safe operations

**Your Custom Class Requirements:**
- Must have a `publish(data)` method
- Should accept `name` and `config` in constructor

#### 2. **CustomDataSubscriber**

Delegates subscription to your custom class.

**Key Features:**
- Instantiates your custom class dynamically
- Configurable poll interval
- Handles None returns gracefully
- Manages lifecycle
- Tracks message statistics

**Your Custom Class Requirements:**
- Must have a `subscribe()` method that returns data or None
- Should accept `name` and `config` in constructor

---

## Configuration

### Configuration Structure

```python
config = {
    'delegate_module': 'path.to.module',      # Required
    'delegate_class': 'ClassName',             # Required
    'delegate_config': {                       # Optional
        # Your custom class configuration
    },
    'poll_interval': 1  # For subscriber only, default: 1
}
```

### Destination/Source Format

```
custom://identifier_name
```

The identifier is passed to your custom class as `name`.

---

## Usage Examples

### Example 1: Custom File Publisher

**Step 1: Create your custom class**

```python
# my_custom_publishers.py
import json
import os

class FilePublisher:
    """Publishes data to a file"""
    
    def __init__(self, name, config):
        self.name = name
        self.filename = config.get('filename', 'output.json')
        self.mode = config.get('mode', 'append')  # append or overwrite
    
    def publish(self, data):
        """Write data to file"""
        mode = 'a' if self.mode == 'append' else 'w'
        
        with open(self.filename, mode) as f:
            json.dump(data, f)
            f.write('\n')
```

**Step 2: Use with CustomDataPublisher**

```python
from custom_datapubsub import CustomDataPublisher

config = {
    'delegate_module': 'my_custom_publishers',
    'delegate_class': 'FilePublisher',
    'delegate_config': {
        'filename': 'sensor_data.json',
        'mode': 'append'
    }
}

publisher = CustomDataPublisher(
    name='file_publisher',
    destination='custom://sensor_file',
    config=config
)

# Publish data
data = {
    'sensor_id': 'sensor_001',
    'temperature': 23.5,
    'timestamp': '2025-10-22T10:30:00Z'
}

publisher.publish(data)
publisher.stop()

print(f"Published {publisher._publish_count} records to file")
```

### Example 2: Custom API Publisher

**Step 1: Create API publisher**

```python
# my_custom_publishers.py
import requests

class APIPublisher:
    """Publishes data to a REST API"""
    
    def __init__(self, name, config):
        self.name = name
        self.api_url = config.get('api_url')
        self.api_key = config.get('api_key')
        self.timeout = config.get('timeout', 30)
        
        if not self.api_url:
            raise ValueError("api_url is required")
    
    def publish(self, data):
        """POST data to API"""
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }
        
        response = requests.post(
            self.api_url,
            json=data,
            headers=headers,
            timeout=self.timeout
        )
        
        response.raise_for_status()
        return response.json()
```

**Step 2: Use the API publisher**

```python
from custom_datapubsub import CustomDataPublisher

config = {
    'delegate_module': 'my_custom_publishers',
    'delegate_class': 'APIPublisher',
    'delegate_config': {
        'api_url': 'https://api.example.com/data',
        'api_key': 'your_api_key_here',
        'timeout': 10
    }
}

publisher = CustomDataPublisher(
    name='api_publisher',
    destination='custom://external_api',
    config=config
)

# Publish multiple records
for i in range(10):
    data = {
        'record_id': i,
        'value': f'data_{i}',
        'timestamp': time.time()
    }
    
    try:
        publisher.publish(data)
        print(f"Published record {i}")
    except Exception as e:
        print(f"Failed to publish record {i}: {e}")

publisher.stop()
```

### Example 3: Custom Database Publisher

**Step 1: Create database publisher**

```python
# my_custom_publishers.py
import sqlite3

class SQLitePublisher:
    """Publishes data to SQLite database"""
    
    def __init__(self, name, config):
        self.name = name
        self.db_path = config.get('db_path', 'data.db')
        self.table_name = config.get('table_name', 'records')
        
        # Create connection and table
        self.conn = sqlite3.connect(self.db_path)
        self._create_table()
    
    def _create_table(self):
        """Create table if not exists"""
        self.conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.conn.commit()
    
    def publish(self, data):
        """Insert data into database"""
        import json
        
        self.conn.execute(
            f'INSERT INTO {self.table_name} (data) VALUES (?)',
            (json.dumps(data),)
        )
        self.conn.commit()
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
```

**Step 2: Use the database publisher**

```python
from custom_datapubsub import CustomDataPublisher

config = {
    'delegate_module': 'my_custom_publishers',
    'delegate_class': 'SQLitePublisher',
    'delegate_config': {
        'db_path': 'sensor_data.db',
        'table_name': 'sensor_readings'
    }
}

publisher = CustomDataPublisher(
    name='db_publisher',
    destination='custom://sqlite_db',
    config=config
)

# Publish sensor readings
for i in range(100):
    data = {
        'sensor_id': f'sensor_{i % 10}',
        'temperature': 20 + (i % 15),
        'humidity': 50 + (i % 30)
    }
    publisher.publish(data)

publisher.stop()
print(f"Stored {publisher._publish_count} readings in database")
```

### Example 4: Custom File Subscriber

**Step 1: Create file subscriber**

```python
# my_custom_subscribers.py
import json
import time

class FileSubscriber:
    """Reads data from a file line by line"""
    
    def __init__(self, name, config):
        self.name = name
        self.filename = config.get('filename', 'input.json')
        self.file_handle = None
        self._open_file()
    
    def _open_file(self):
        """Open file for reading"""
        try:
            self.file_handle = open(self.filename, 'r')
        except FileNotFoundError:
            print(f"File {self.filename} not found, waiting...")
            time.sleep(1)
    
    def subscribe(self):
        """Read next line from file"""
        if not self.file_handle:
            self._open_file()
            return None
        
        line = self.file_handle.readline()
        
        if line:
            try:
                return json.loads(line.strip())
            except json.JSONDecodeError:
                return None
        
        return None
    
    def close(self):
        """Close file handle"""
        if self.file_handle:
            self.file_handle.close()
```

**Step 2: Use the file subscriber**

```python
from custom_datapubsub import CustomDataSubscriber
import time

config = {
    'delegate_module': 'my_custom_subscribers',
    'delegate_class': 'FileSubscriber',
    'delegate_config': {
        'filename': 'sensor_data.json'
    },
    'poll_interval': 0.5
}

subscriber = CustomDataSubscriber(
    name='file_subscriber',
    source='custom://sensor_file',
    config=config
)

# Start subscriber
subscriber.start()

# Let it run for 30 seconds
time.sleep(30)

# Stop subscriber
subscriber.stop()

print(f"Processed {subscriber.get_message_count()} messages from file")
```

### Example 5: Custom Queue Subscriber

**Step 1: Create queue subscriber**

```python
# my_custom_subscribers.py
import queue

class QueueSubscriber:
    """Subscribes from a shared queue"""
    
    # Class-level shared queue
    _shared_queues = {}
    
    def __init__(self, name, config):
        self.name = name
        self.queue_name = config.get('queue_name', 'default')
        self.timeout = config.get('timeout', 1)
        
        # Get or create shared queue
        if self.queue_name not in self._shared_queues:
            self._shared_queues[self.queue_name] = queue.Queue()
        
        self.queue = self._shared_queues[self.queue_name]
    
    def subscribe(self):
        """Get item from queue"""
        try:
            return self.queue.get(timeout=self.timeout)
        except queue.Empty:
            return None
    
    @classmethod
    def put_data(cls, queue_name, data):
        """Helper to put data in queue"""
        if queue_name not in cls._shared_queues:
            cls._shared_queues[queue_name] = queue.Queue()
        cls._shared_queues[queue_name].put(data)
```

**Step 2: Use the queue subscriber**

```python
from custom_datapubsub import CustomDataSubscriber
from my_custom_subscribers import QueueSubscriber
import threading
import time

# Producer thread
def producer():
    for i in range(20):
        data = {'id': i, 'value': f'message_{i}'}
        QueueSubscriber.put_data('my_queue', data)
        time.sleep(0.5)

# Start producer
producer_thread = threading.Thread(target=producer, daemon=True)
producer_thread.start()

# Create subscriber
config = {
    'delegate_module': 'my_custom_subscribers',
    'delegate_class': 'QueueSubscriber',
    'delegate_config': {
        'queue_name': 'my_queue',
        'timeout': 2
    },
    'poll_interval': 0.1
}

subscriber = CustomDataSubscriber(
    name='queue_subscriber',
    source='custom://queue',
    config=config
)

subscriber.start()
time.sleep(15)
subscriber.stop()

print(f"Received {subscriber.get_message_count()} messages")
```

### Example 6: Custom HTTP Webhook Subscriber

**Step 1: Create webhook subscriber**

```python
# my_custom_subscribers.py
import queue
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import json

class WebhookSubscriber:
    """Receives data via HTTP webhook"""
    
    def __init__(self, name, config):
        self.name = name
        self.port = config.get('port', 8080)
        self.data_queue = queue.Queue()
        
        # Create HTTP handler class with access to queue
        parent = self
        
        class WebhookHandler(BaseHTTPRequestHandler):
            def do_POST(self):
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                
                try:
                    data = json.loads(post_data.decode('utf-8'))
                    parent.data_queue.put(data)
                    
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(b'{"status": "received"}')
                except Exception as e:
                    self.send_response(400)
                    self.end_headers()
            
            def log_message(self, format, *args):
                pass  # Suppress logs
        
        # Start HTTP server in background
        self.server = HTTPServer(('0.0.0.0', self.port), WebhookHandler)
        self.server_thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True
        )
        self.server_thread.start()
        
        print(f"Webhook server listening on port {self.port}")
    
    def subscribe(self):
        """Get data from queue"""
        try:
            return self.data_queue.get(timeout=1)
        except queue.Empty:
            return None
    
    def close(self):
        """Shutdown server"""
        self.server.shutdown()
```

**Step 2: Use the webhook subscriber**

```python
from custom_datapubsub import CustomDataSubscriber
import time
import requests

config = {
    'delegate_module': 'my_custom_subscribers',
    'delegate_class': 'WebhookSubscriber',
    'delegate_config': {
        'port': 8080
    },
    'poll_interval': 0.1
}

subscriber = CustomDataSubscriber(
    name='webhook_subscriber',
    source='custom://webhook',
    config=config
)

subscriber.start()

# Simulate sending webhooks
import threading

def send_webhooks():
    time.sleep(2)  # Wait for server to start
    for i in range(10):
        try:
            requests.post(
                'http://localhost:8080',
                json={'event': 'test', 'id': i}
            )
            time.sleep(1)
        except:
            pass

webhook_thread = threading.Thread(target=send_webhooks, daemon=True)
webhook_thread.start()

# Run for 15 seconds
time.sleep(15)
subscriber.stop()

print(f"Received {subscriber.get_message_count()} webhooks")
```

### Example 7: Custom Cloud Storage Publisher

**Step 1: Create S3 publisher**

```python
# my_custom_publishers.py
import boto3
import json
from datetime import datetime

class S3Publisher:
    """Publishes data to AWS S3"""
    
    def __init__(self, name, config):
        self.name = name
        self.bucket_name = config.get('bucket_name')
        self.prefix = config.get('prefix', 'data/')
        
        if not self.bucket_name:
            raise ValueError("bucket_name is required")
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=config.get('aws_access_key_id'),
            aws_secret_access_key=config.get('aws_secret_access_key'),
            region_name=config.get('region', 'us-east-1')
        )
    
    def publish(self, data):
        """Upload data to S3"""
        # Generate unique key
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        key = f"{self.prefix}{timestamp}.json"
        
        # Upload to S3
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType='application/json'
        )
        
        return key
```

**Step 2: Use the S3 publisher**

```python
from custom_datapubsub import CustomDataPublisher

config = {
    'delegate_module': 'my_custom_publishers',
    'delegate_class': 'S3Publisher',
    'delegate_config': {
        'bucket_name': 'my-data-bucket',
        'prefix': 'sensor_data/',
        'aws_access_key_id': 'YOUR_KEY',
        'aws_secret_access_key': 'YOUR_SECRET',
        'region': 'us-east-1'
    }
}

publisher = CustomDataPublisher(
    name='s3_publisher',
    destination='custom://s3_storage',
    config=config
)

# Publish data to S3
for i in range(10):
    data = {
        'sensor_id': f'sensor_{i}',
        'readings': [20 + i, 30 + i, 40 + i],
        'timestamp': time.time()
    }
    
    publisher.publish(data)
    print(f"Published record {i} to S3")

publisher.stop()
```

---

## Advanced Patterns

### Pattern 1: Multi-Destination Publisher

Create a publisher that writes to multiple destinations:

```python
class MultiDestinationPublisher:
    """Publishes to multiple destinations"""
    
    def __init__(self, name, config):
        self.name = name
        self.destinations = []
        
        # Initialize multiple publishers
        for dest_config in config.get('destinations', []):
            dest = self._create_destination(dest_config)
            self.destinations.append(dest)
    
    def _create_destination(self, config):
        """Create individual destination"""
        dest_type = config['type']
        
        if dest_type == 'file':
            return FilePublisher(self.name, config)
        elif dest_type == 'api':
            return APIPublisher(self.name, config)
        # Add more types as needed
    
    def publish(self, data):
        """Publish to all destinations"""
        for dest in self.destinations:
            dest.publish(data)
```

### Pattern 2: Transform Subscriber

Transform data as it's received:

```python
class TransformSubscriber:
    """Subscribes and transforms data"""
    
    def __init__(self, name, config):
        self.name = name
        self.source_subscriber = FileSubscriber(name, config)
        self.transform_func = config.get('transform_function')
    
    def subscribe(self):
        """Subscribe and transform"""
        data = self.source_subscriber.subscribe()
        
        if data and self.transform_func:
            # Apply transformation
            return self.transform_func(data)
        
        return data
```

### Pattern 3: Buffered Publisher

Buffer data before publishing:

```python
class BufferedPublisher:
    """Buffers data before publishing"""
    
    def __init__(self, name, config):
        self.name = name
        self.buffer = []
        self.buffer_size = config.get('buffer_size', 100)
        self.target_publisher = FilePublisher(name, config)
    
    def publish(self, data):
        """Add to buffer and flush if full"""
        self.buffer.append(data)
        
        if len(self.buffer) >= self.buffer_size:
            self.flush()
    
    def flush(self):
        """Write all buffered data"""
        for data in self.buffer:
            self.target_publisher.publish(data)
        self.buffer = []
```

### Pattern 4: Retry Publisher

Add retry logic to publishing:

```python
class RetryPublisher:
    """Publisher with retry logic"""
    
    def __init__(self, name, config):
        self.name = name
        self.max_retries = config.get('max_retries', 3)
        self.retry_delay = config.get('retry_delay', 1)
        self.delegate = APIPublisher(name, config)
    
    def publish(self, data):
        """Publish with retries"""
        import time
        
        for attempt in range(self.max_retries):
            try:
                self.delegate.publish(data)
                return
            except Exception as e:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise
```

### Pattern 5: Filtered Subscriber

Filter incoming data:

```python
class FilteredSubscriber:
    """Subscriber with filtering"""
    
    def __init__(self, name, config):
        self.name = name
        self.filter_func = config.get('filter_function')
        self.source = FileSubscriber(name, config)
    
    def subscribe(self):
        """Subscribe with filtering"""
        while True:
            data = self.source.subscribe()
            
            if data is None:
                return None
            
            if self.filter_func and self.filter_func(data):
                return data
            
            # If filtered out, keep trying
```

---

## Troubleshooting

### Common Issues and Solutions

#### Issue 1: Module Not Found

**Error:**
```
ModuleNotFoundError: No module named 'my_custom_publishers'
```

**Solutions:**

1. **Verify module path:**
```python
import sys
print(sys.path)
```

2. **Add module to path:**
```python
import sys
sys.path.append('/path/to/your/modules')
```

3. **Use absolute imports:**
```python
config = {
    'delegate_module': 'full.path.to.my_custom_publishers',
    'delegate_class': 'FilePublisher'
}
```

#### Issue 2: Missing delegate_module or delegate_class

**Error:**
```
ValueError: delegate_module and delegate_class must be specified in config
```

**Solution:**
Always provide both required fields:
```python
config = {
    'delegate_module': 'my_module',  # Required
    'delegate_class': 'MyClass',      # Required
    'delegate_config': {}
}
```

#### Issue 3: Delegate Class Missing Required Method

**Error:**
```
AttributeError: 'MyClass' object has no attribute 'publish'
```

**Solution:**
Ensure your class has the required method:
```python
class MyPublisher:
    def __init__(self, name, config):
        pass
    
    def publish(self, data):  # Required method
        pass
```

#### Issue 4: Delegate Constructor Error

**Error:**
```
TypeError: __init__() missing required positional argument
```

**Solution:**
Your class must accept `name` and `config`:
```python
# Correct
class MyClass:
    def __init__(self, name, config):
        pass

# Incorrect
class MyClass:
    def __init__(self):  # Wrong!
        pass
```

#### Issue 5: Subscriber Returning Wrong Type

**Error:**
Data not being processed correctly

**Solution:**
Subscriber must return dict or None:
```python
def subscribe(self):
    # Return dict with data
    return {'key': 'value'}
    
    # Or return None if no data
    return None
    
    # Don't return other types
    # return "string"  # Wrong!
```

### Debugging Tips

**Enable detailed logging:**
```python
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('custom_datapubsub')
```

**Test delegate independently:**
```python
# Test your class before using with framework
from my_custom_publishers import FilePublisher

publisher = FilePublisher('test', {'filename': 'test.json'})
publisher.publish({'test': 'data'})
```

**Verify instantiation:**
```python
from core.core_utils import instantiate_module

delegate = instantiate_module(
    'my_module',
    'MyClass',
    {'name': 'test', 'config': {}}
)

print(f"Delegate created: {delegate}")
print(f"Has publish: {hasattr(delegate, 'publish')}")
```

---

## Best Practices

### 1. Error Handling in Delegates

```python
class RobustPublisher:
    def publish(self, data):
        try:
            # Your publishing logic
            pass
        except Exception as e:
            logger.error(f"Publish failed: {e}")
            raise  # Re-raise to notify framework
```

### 2. Resource Cleanup

```python
class CleanPublisher:
    def __init__(self, name, config):
        self.resource = acquire_resource()
    
    def publish(self, data):
        pass
    
    def close(self):
        """Cleanup method"""
        if self.resource:
            self.resource.close()
```

### 3. Configuration Validation

```python
class ValidatedPublisher:
    def __init__(self, name, config):
        # Validate required config
        required = ['api_url', 'api_key']
        for key in required:
            if key not in config:
                raise ValueError(f"Missing required config: {key}")
        
        self.api_url = config['api_url']
        self.api_key = config['api_key']
```

### 4. Thread Safety

```python
import threading

class ThreadSafePublisher:
    def __init__(self, name, config):
        self.lock = threading.Lock()
    
    def publish(self, data):
        with self.lock:
            # Thread-safe publishing
            pass
```

### 5. Logging

```python
import logging

class LoggedPublisher:
    def __init__(self, name, config):
        self.logger = logging.getLogger(f'{__name__}.{name}')
    
    def publish(self, data):
        self.logger.debug(f"Publishing: {data}")
        # Publish logic
        self.logger.info("Published successfully")
```

---

## API Reference

### CustomDataPublisher

```python
CustomDataPublisher(name, destination, config)
```

**Required Config:**
- `delegate_module` (str): Python module path
- `delegate_class` (str): Class name within module
- `delegate_config` (dict, optional): Configuration for delegate

**Delegate Requirements:**
- Constructor: `__init__(self, name, config)`
- Method: `publish(self, data)`

### CustomDataSubscriber

```python
CustomDataSubscriber(name, source, config)
```

**Required Config:**
- `delegate_module` (str): Python module path
- `delegate_class` (str): Class name within module
- `delegate_config` (dict, optional): Configuration for delegate
- `poll_interval` (float, optional): Polling interval in seconds (default: 1)

**Delegate Requirements:**
- Constructor: `__init__(self, name, config)`
- Method: `subscribe(self)` → returns dict or None

---

## Additional Resources

- Python Module System: https://docs.python.org/3/tutorial/modules.html
- Design Patterns: https://refactoring.guru/design-patterns
- Dynamic Imports: https://docs.python.org/3/library/importlib.html

---

**Version:** 1.0  
**Last Updated:** October 22, 2025  
**Author:** DAG Server Development Team


---

# HolderDataSubscriber & HolderDataPublisher

## Overview

**Holder** classes are passive implementations of `DataSubscriber` and `DataPublisher` that don't interact with external systems. Instead, they allow programmatic control of data flow, making them perfect for testing, development, and controlled data pipelines.

### Key Concept

Unlike regular subscribers/publishers that connect to external systems (Kafka, databases, files), **Holders** are:
- **Passive**: No external connections
- **Controllable**: You add/retrieve data programmatically  
- **Testable**: Perfect for unit tests without infrastructure
- **Inspectable**: View all data that was published

## Components

### HolderDataSubscriber

A `DataSubscriber` where **you** control what data it receives.

```python
holder = HolderDataSubscriber('my_holder')
holder.add_data({'type': 'order', 'id': 123})  # Add data manually
data = holder.get_data()  # Consumer retrieves it normally
```

**Perfect For:**
- ✅ Testing consumers without real message queues
- ✅ Manual data injection into pipelines
- ✅ Simulating data sources in development
- ✅ Buffering between pipeline stages

### HolderDataPublisher

A `DataPublisher` that **stores** published messages instead of sending them externally.

```python
holder = HolderDataPublisher('my_pub')
holder.publish({'type': 'order', 'id': 123})  # Publish normally
messages = holder.get_published_messages()  # Verify what was published
```

**Perfect For:**
- ✅ Testing publishers without real infrastructure
- ✅ Verifying published messages in tests
- ✅ Development without external dependencies
- ✅ Debugging data pipelines

## Quick Start

### HolderDataSubscriber Usage

```python
from holder_datapubsub import HolderDataSubscriber

# Create holder
holder = HolderDataSubscriber('test_holder', max_depth=1000)

# Add data programmatically
holder.add_data({'id': 1, 'value': 'test'})
holder.add_data({'id': 2, 'value': 'test2'})

# Consumer retrieves data normally
msg1 = holder.get_data(block_time=1)
msg2 = holder.get_data(block_time=1)

print(f"Got: {msg1}")  # {'id': 1, 'value': 'test'}
print(f"Got: {msg2}")  # {'id': 2, 'value': 'test2'}
```

### HolderDataPublisher Usage

```python
from holder_datapubsub import HolderDataPublisher

# Create holder
holder = HolderDataPublisher('test_pub')

# Publish normally
holder.publish({'id': 1, 'status': 'processed'})
holder.publish({'id': 2, 'status': 'processed'})

# Retrieve and verify
messages = holder.get_published_messages()
assert len(messages) == 2
assert messages[0]['id'] == 1
```

## HolderDataSubscriber API

### Creation

```python
# Method 1: Direct instantiation
holder = HolderDataSubscriber('name', max_depth=1000)

# Method 2: Factory function
from holder_datapubsub import create_holder_subscriber
holder = create_holder_subscriber('name', max_depth=1000)
```

### Adding Data

```python
# Add single item
holder.add_data({'msg': 'test'})

# Add with timeout
holder.add_data({'msg': 'test'}, timeout=5)

# Add without blocking (raises queue.Full if full)
holder.add_data({'msg': 'test'}, block=False)

# Try add (returns True/False)
success = holder.try_add_data({'msg': 'test'})

# Add batch
messages = [{'id': 1}, {'id': 2}, {'id': 3}]
success_count, failed_count = holder.add_data_batch(messages)
```

### Retrieving Data

```python
# Get data (standard DataSubscriber method)
data = holder.get_data(block_time=1)

# Get with infinite blocking
data = holder.get_data(block_time=-1)

# Non-blocking
data = holder.get_data(block_time=None)  # Returns None if empty
```

### Queue Management

```python
# Check queue size
size = holder.get_queue_size()
# or
size = len(holder)  # Pythonic way

# Clear queue
cleared_count = holder.clear()

# Peek at next item (not thread-safe!)
next_item = holder.peek()
```

### Statistics

```python
# Get statistics
stats = holder.get_statistics()
# Returns:
# {
#     'name': 'holder_name',
#     'type': 'holder',
#     'current_depth': 5,
#     'max_depth': 1000,
#     'data_added_count': 10,
#     'data_added_failed': 0,
#     'data_retrieved_count': 5,
#     'last_receive': '2025-01-15T10:30:00',
#     'suspended': False
# }

# Or use standard details() method
details = holder.details()
```

### Standard DataSubscriber Methods

All inherited methods work normally:

```python
holder.start()    # Start subscription loop (optional)
holder.stop()     # Stop subscription loop
holder.suspend()  # Suspend processing
holder.resume()   # Resume processing
```

## HolderDataPublisher API

### Creation

```python
# Method 1: Direct instantiation
holder = HolderDataPublisher('name')

# Method 2: Factory function
from holder_datapubsub import create_holder_publisher
holder = create_holder_publisher('name')
```

### Publishing

```python
# Publish normally (standard DataPublisher method)
holder.publish({'msg': 'test'})
holder.publish({'id': 123, 'status': 'ok'})
```

### Retrieving Published Messages

```python
# Get all published messages
all_messages = holder.get_published_messages()

# Get count
count = holder.get_published_count()
# or
count = len(holder)  # Pythonic way

# Get last N messages
last_one = holder.get_last_published()  # Returns list with last message
last_five = holder.get_last_published(5)  # Returns last 5
```

### Finding Messages

```python
# Find messages matching predicate
orders = holder.find_messages(lambda m: m.get('type') == 'order')

# Find high-value orders
high_value = holder.find_messages(
    lambda m: m.get('type') == 'order' and m.get('amount', 0) > 1000
)

# Check if contains matching message
has_errors = holder.contains_message(lambda m: m.get('level') == 'ERROR')
```

### Managing Stored Messages

```python
# Clear stored messages
cleared_count = holder.clear_published_messages()
```

### Statistics

```python
# Get statistics
stats = holder.get_statistics()
# Returns:
# {
#     'name': 'pub_name',
#     'type': 'holder',
#     'messages_stored': 10,
#     'publish_count': 10,
#     'last_publish': '2025-01-15T10:30:00',
#     'queue_depth': 0,
#     'publish_interval': 0,
#     'batch_size': None
# }
```

### Standard DataPublisher Methods

All inherited methods work normally:

```python
holder.stop()  # Stop publisher and flush queues
```

## Common Use Cases

### Use Case 1: Testing Without Infrastructure

**Problem**: Need to test a consumer without running Kafka/database

**Solution**: Use HolderDataSubscriber

```python
def test_order_processor():
    # Create holder instead of real Kafka subscriber
    mock_source = HolderDataSubscriber('test_source')
    
    # Inject test data
    mock_source.add_data({'type': 'order', 'id': 1, 'amount': 100})
    mock_source.add_data({'type': 'order', 'id': 2, 'amount': 200})
    
    # Test your processor
    processor = OrderProcessor(mock_source)
    processor.run()
    
    # Verify results
    assert processor.processed_count == 2
```

### Use Case 2: Verifying Published Messages

**Problem**: Need to verify what a component published

**Solution**: Use HolderDataPublisher

```python
def test_notification_sender():
    # Create holder instead of real email/SMS publisher
    mock_publisher = HolderDataPublisher('test_notifications')
    
    # Run component
    notifier = NotificationService(mock_publisher)
    notifier.send_order_confirmation(order_id=123)
    
    # Verify what was published
    messages = mock_publisher.get_published_messages()
    assert len(messages) == 1
    assert messages[0]['type'] == 'email'
    assert messages[0]['recipient'] == 'customer@example.com'
```

### Use Case 3: End-to-End Pipeline Testing

**Problem**: Test entire pipeline without infrastructure

**Solution**: Use both holders

```python
def test_data_pipeline():
    # Setup: holders at both ends
    input_holder = HolderDataSubscriber('pipeline_input')
    output_holder = HolderDataPublisher('pipeline_output')
    
    # Create pipeline
    pipeline = DataPipeline(input_holder, output_holder)
    
    # Inject test data
    input_holder.add_data_batch([
        {'value': 10},
        {'value': 20},
        {'value': 30}
    ])
    
    # Run pipeline
    pipeline.process_all()
    
    # Verify output
    output = output_holder.get_published_messages()
    assert len(output) == 3
    assert output[0]['value'] == 20  # Expect doubled values
```

### Use Case 4: MessageRouter Testing

**Problem**: Test router without real publishers

**Solution**: Use holders as router destinations

```python
from message_router_datapubsub import MessageRouterDataPublisher

def test_message_routing():
    # Create holder destinations
    db_holder = HolderDataPublisher('db_dest')
    file_holder = HolderDataPublisher('file_dest')
    queue_holder = HolderDataPublisher('queue_dest')
    
    # Create router
    router = MessageRouterDataPublisher('router', 'router://test', {
        'child_publishers': {
            'database': db_holder,
            'file': file_holder,
            'queue': queue_holder
        },
        'resolver_class': 'example_resolvers.FieldBasedResolver'
    })
    
    # Publish through router
    router.publish({'type': 'database', 'data': 'test'})
    router.publish({'type': 'file', 'data': 'test'})
    
    # Verify routing
    assert db_holder.get_published_count() == 1
    assert file_holder.get_published_count() == 1
    assert queue_holder.get_published_count() == 0
```

### Use Case 5: Manual Data Injection

**Problem**: Need to manually feed data into a system

**Solution**: Use HolderDataSubscriber

```python
# Create holder as data source
data_source = HolderDataSubscriber('manual_feed', max_depth=10000)

# Start consumer system
consumer_system = ConsumerSystem(data_source)
consumer_system.start()

# Manually inject data as needed
while True:
    user_input = get_user_input()
    if user_input:
        data_source.add_data(user_input)
```

### Use Case 6: Development Without Infrastructure

**Problem**: Develop locally without Kafka/databases

**Solution**: Use holders during development

```python
# Development mode - use holders
if DEVELOPMENT_MODE:
    source = HolderDataSubscriber('dev_source')
    destination = HolderDataPublisher('dev_dest')
else:
    source = create_subscriber('prod', {'source': 'kafka://orders'})
    destination = create_publisher('prod', {'destination': 'sql://orders_table'})

# Application code works the same
app = Application(source, destination)
app.run()
```

## Integration with Abhikarta

### Testing Workflow Executors

```python
def test_workflow_executor():
    # Mock input queue
    input_holder = HolderDataSubscriber('workflow_input')
    
    # Mock output
    output_holder = HolderDataPublisher('workflow_output')
    
    # Test workflow
    executor = WorkflowExecutor(input_holder, output_holder)
    
    input_holder.add_data({
        'workflow_type': 'chat',
        'messages': [{'role': 'user', 'content': 'Hello'}]
    })
    
    executor.execute_next()
    
    # Verify execution
    results = output_holder.get_published_messages()
    assert len(results) == 1
    assert 'response' in results[0]
```

### Testing LLM Facades

```python
def test_llm_facade():
    # Mock publisher for LLM requests
    request_holder = HolderDataPublisher('llm_requests')
    
    # Create facade with holder
    facade = ClaudeFacade(publisher=request_holder)
    
    # Make request
    facade.generate("What is AI?")
    
    # Verify request format
    requests = request_holder.get_published_messages()
    assert requests[0]['model'] == 'claude-sonnet-4'
    assert 'What is AI?' in requests[0]['prompt']
```

### Testing Session Management

```python
def test_session_handler():
    # Mock session store
    session_holder = HolderDataPublisher('sessions')
    
    # Test handler
    handler = SessionHandler(session_holder)
    handler.create_session(user_id='user123')
    
    # Verify session was stored
    sessions = session_holder.find_messages(
        lambda m: m.get('user_id') == 'user123'
    )
    assert len(sessions) == 1
```

## Best Practices

### 1. Use for Testing, Not Production

```python
# ✅ Good - testing
def test_component():
    holder = HolderDataSubscriber('test')
    # ... test code

# ❌ Bad - production
def production_service():
    holder = HolderDataSubscriber('prod')  # Use real subscriber!
```

### 2. Clear Between Tests

```python
def setup_test():
    holder = HolderDataSubscriber('test')
    return holder

def teardown_test(holder):
    holder.clear()  # Clean up for next test
```

### 3. Verify Expected Behavior

```python
def test_processor():
    input_holder = HolderDataSubscriber('input')
    output_holder = HolderDataPublisher('output')
    
    # Add input
    input_holder.add_data({'value': 10})
    
    # Process
    processor.run()
    
    # Verify output
    output = output_holder.get_published_messages()
    assert len(output) == 1  # Check count
    assert output[0]['value'] == 20  # Check transformation
```

### 4. Use Factory Functions for Brevity

```python
# Instead of:
holder = HolderDataSubscriber('test', config={'max_depth': 100})

# Use:
holder = create_holder_subscriber('test', max_depth=100)
```

## Comparison

| Feature | HolderDataSubscriber | Regular DataSubscriber |
|---------|---------------------|----------------------|
| **External Source** | None | Kafka, DB, File, etc. |
| **Data Input** | Programmatic (`add_data()`) | Automatic (`_do_subscribe()`) |
| **Use Case** | Testing, Development | Production |
| **Startup** | Instant | Requires connection |
| **Dependencies** | None | External system required |

| Feature | HolderDataPublisher | Regular DataPublisher |
|---------|-------------------|---------------------|
| **External Destination** | None | Kafka, DB, File, etc. |
| **Data Output** | Stored internally | Sent to external system |
| **Retrieval** | `get_published_messages()` | N/A |
| **Use Case** | Testing, Verification | Production |
| **Dependencies** | None | External system required |

## Performance

### Memory Usage

**HolderDataSubscriber:**
- Memory ≈ queue_depth × message_size
- Default max_depth: 100,000

**HolderDataPublisher:**
- Memory ≈ number of messages × message_size
- No automatic cleanup - call `clear_published_messages()` periodically

### Recommendations

```python
# For tests with small data
holder = HolderDataSubscriber('test', max_depth=100)

# For tests with large data
holder = HolderDataSubscriber('test', max_depth=10000)

# Clear publisher periodically if long-running
if len(publisher) > 1000:
    publisher.clear_published_messages()
```

## Thread Safety

Both classes are **thread-safe**:
- Internal queues use threading locks
- Statistics updates are protected
- Safe to use from multiple threads

```python
# Safe to use from multiple threads
def producer_thread():
    while True:
        holder.add_data({'msg': 'from producer'})

def consumer_thread():
    while True:
        data = holder.get_data(block_time=1)
```

## Examples

See [holder_examples.py](holder_examples.py) for comprehensive examples including:
1. Basic usage
2. Batch operations
3. Message verification
4. Pipeline testing
5. MessageRouter integration
6. Factory functions

## Summary

**HolderDataSubscriber** and **HolderDataPublisher** are essential tools for:

✅ **Testing** - No external dependencies  
✅ **Development** - Work without infrastructure  
✅ **Verification** - Inspect all published messages  
✅ **Control** - Programmatic data injection  
✅ **Flexibility** - Use anywhere DataSubscriber/DataPublisher is expected  

They fully implement the DataSubscriber/DataPublisher interfaces, so they can be used anywhere those base classes are expected, making them perfect drop-in replacements for testing and development.

---

Copyright © 2025 Abhikarta. All rights reserved.

---

# Message Router Components - Complete Guide

## Overview

The Message Router system provides **intelligent routing capabilities** for both incoming (subscriber) and outgoing (publisher) messages in the DishtaYantra pub/sub infrastructure.

### Components

1. **FanoutDataSubscriber** - Routes incoming messages from one source to multiple child subscribers (Fan-Out for Subscribers)
2. **FanoutDataPublisher** - Routes outgoing messages to multiple child publishers based on strategy (Fan-Out for Publishers)

Both components use **pluggable resolver strategies** to determine routing destinations, providing flexibility for various routing patterns.

## Component Comparison

### FanoutDataSubscriber vs FanoutDataPublisher

| Feature | FanoutDataSubscriber | FanoutDataPublisher |
|---------|----------------------------|---------------------------|
| **Direction** | Incoming (Subscribe) | Outgoing (Publish) |
| **Source** | Single DataSubscriber | Application calls publish() |
| **Targets** | Multiple child DataSubscribers | Multiple child DataPublishers |
| **Use Case** | Route incoming data to processors | Route outgoing data to destinations |
| **Example** | Route orders/logs/notifications to different processors | Publish to database/file/queue based on message type |

### Both Components Share

- ✅ Pluggable resolver strategy pattern
- ✅ Dynamic child management (add/remove at runtime)
- ✅ Comprehensive routing statistics
- ✅ Unrouted message logging
- ✅ Thread-safe operations
- ✅ Production-ready error handling

## Architecture Patterns

### Pattern 1: Full Router Pipeline

```
External Source → FanoutDataSubscriber → Processing → FanoutDataPublisher → Multiple Destinations

[Source]
   ↓
[Router Subscriber]
   ├→ [Processor A] → [Router Publisher] → [DB]
   ├→ [Processor B] → [Router Publisher] → [File]
   └→ [Processor C] → [Router Publisher] → [Queue]
```

### Pattern 2: Subscriber-Only Routing

```
Single Source → FanoutDataSubscriber → Multiple Specialized Processors

[Kafka Topic]
      ↓
[Router Subscriber]
      ├→ [Order Processor]
      ├→ [Log Processor]
      └→ [Notification Processor]
```

### Pattern 3: Publisher-Only Routing

```
Application → FanoutDataPublisher → Multiple Destinations

[Your App]
     ↓
[Router Publisher]
     ├→ [Primary DB]
     ├→ [Backup DB]
     └→ [Analytics File]
```

## Quick Start Examples

> **Note on config:** `source_subscriber` and the values in `child_subscribers` / `child_publishers` are **names** (strings) of subscribers/publishers the server knows about, not live instances. The router resolves them by name. `resolver_class` is a dotted path to a class you provide that implements `resolve(message_dict) -> routing_key`.

### FanoutDataSubscriber Example

```python
from core.pubsub.fanout_datapubsub import FanoutDataSubscriber
from core.pubsub.pubsubfactory import create_subscriber

# Create source and child subscribers
source = create_subscriber('source', {'source': 'mem://queue/incoming'})
order_sub = create_subscriber('orders', {'source': 'mem://queue/orders'})
log_sub = create_subscriber('logs', {'source': 'mem://queue/logs'})

# Configure router
config = {
    'source_subscriber': 'source',          # name of the source subscriber
    'child_subscribers': {'order': 'orders', 'log': 'logs'},  # key -> subscriber NAME
    'resolver_class': 'myapp.resolvers.FieldBasedResolver'  # user-provided
}

# Create and start
router = FanoutDataSubscriber('router', config)
router.start()

# Messages with {'type': 'order'} → order_sub
# Messages with {'type': 'log'} → log_sub
```

### FanoutDataPublisher Example

```python
from core.pubsub.fanout_datapubsub import FanoutDataPublisher
from core.pubsub.pubsubfactory import create_publisher

# Create child publishers for different destinations
db_pub = create_publisher('db', {'destination': 'sql://orders_table'})
file_pub = create_publisher('file', {'destination': 'file://logs/orders.log'})
queue_pub = create_publisher('queue', {'destination': 'mem://queue/order_events'})

# Configure router
config = {
    'destination': 'router://order_router',
    'child_publishers': {
        'database': db_pub,
        'file': file_pub,
        'queue': queue_pub
    },
    'resolver_class': 'myapp.resolvers.FieldBasedResolver'  # user-provided
}

# Create router
router = FanoutDataPublisher('order_router', 'router://order_router', config)

# Publish - automatically routes to appropriate destination
router.publish({'type': 'database', 'order_id': 123, 'amount': 99.99})
```

## Configuration

### FanoutDataSubscriber Configuration

```python
config = {
    'child_subscribers': {
        'routing_key_1': subscriber_instance_1,
        'routing_key_2': subscriber_instance_2,
    },
    'resolver_class': 'full.module.path.ResolverClass',
    'unrouted_file': 'unrouted_subscriber_messages.jsonl',  # Optional
    'max_depth': 100000,  # Optional, default: 100000
    'auto_start_children': True  # Optional, default: True
}
```

### FanoutDataPublisher Configuration

```python
config = {
    'destination': 'router://router_name',
    'child_publishers': {
        'routing_key_1': publisher_instance_1,
        'routing_key_2': publisher_instance_2,
    },
    'resolver_class': 'full.module.path.ResolverClass',
    'unrouted_file': 'unrouted_publisher_messages.jsonl',  # Optional
    'publish_interval': 0,  # Optional, for batch publishing
    'batch_size': None  # Optional, for batch publishing
}
```

## Common Use Cases

### Use Case 1: Type-Based Data Pipeline

**Scenario**: Route different message types through specialized processing pipelines

```python
# Subscriber side - route incoming messages
subscriber_router = FanoutDataSubscriber(
    'input_router',
    {
        'child_subscribers': {
            'order': order_processor_sub,
            'log': log_processor_sub,
            'notification': notification_processor_sub
        },
        'resolver_class': 'myapp.resolvers.FieldBasedResolver'  # user-provided
    }
)

# Publisher side - route processed results to destinations
publisher_router = FanoutDataPublisher(
    'output_router',
    'router://output',
    {
        'child_publishers': {
            'order': database_publisher,
            'log': file_publisher,
            'notification': queue_publisher
        },
        'resolver_class': 'myapp.resolvers.FieldBasedResolver'  # user-provided
    }
)
```

### Use Case 2: Priority-Based Processing

**Scenario**: High-priority messages get fast processing, low-priority get batch processing

```python
# Subscriber side - route to priority queues
subscriber_router = FanoutDataSubscriber(
    'priority_input',
    {
        'child_subscribers': {
            'high': high_priority_processor,
            'medium': medium_priority_processor,
            'low': batch_processor
        },
        'resolver_class': 'myapp.resolvers.PriorityBasedResolver'  # user-provided
    }
)

# Publisher side - high priority to memory, low priority to database
publisher_router = FanoutDataPublisher(
    'priority_output',
    'router://priority',
    {
        'child_publishers': {
            'high': memory_queue_publisher,
            'medium': database_publisher,
            'low': archive_file_publisher
        },
        'resolver_class': 'myapp.resolvers.PriorityBasedResolver'  # user-provided
    }
)
```

### Use Case 3: Geographic Distribution

**Scenario**: Route messages to regional data centers

```python
class RegionResolver:
    def resolve(self, message_dict):
        region = message_dict.get('region', 'us-east')
        return region

# Publisher side - publish to regional endpoints
publisher_router = FanoutDataPublisher(
    'geo_router',
    'router://geo',
    {
        'child_publishers': {
            'us-east': us_east_publisher,
            'us-west': us_west_publisher,
            'eu': eu_publisher,
            'asia': asia_publisher
        },
        'resolver_class': 'myapp.resolvers.RegionResolver'
    }
)
```

### Use Case 4: Multi-Tenant Isolation

**Scenario**: Each tenant gets dedicated processing and storage

```python
class TenantResolver:
    def resolve(self, message_dict):
        tenant_id = message_dict.get('tenant_id')
        return f'tenant_{tenant_id}' if tenant_id else 'default'

# Subscriber side - tenant-specific processors
subscriber_router = FanoutDataSubscriber(
    'tenant_input',
    {
        'child_subscribers': {
            'tenant_acme': acme_processor,
            'tenant_globex': globex_processor,
            'tenant_initech': initech_processor
        },
        'resolver_class': 'myapp.resolvers.TenantResolver'
    }
)

# Publisher side - tenant-specific storage
publisher_router = FanoutDataPublisher(
    'tenant_output',
    'router://tenant',
    {
        'child_publishers': {
            'tenant_acme': acme_database,
            'tenant_globex': globex_database,
            'tenant_initech': initech_database
        },
        'resolver_class': 'myapp.resolvers.TenantResolver'
    }
)
```

### Use Case 5: Load Balancing

**Scenario**: Distribute load across multiple workers/endpoints

```python
# Subscriber side - distribute to worker pool
workers = {f'partition_{i}': worker_subs[i] for i in range(8)}
subscriber_router = FanoutDataSubscriber(
    'worker_router',
    {
        'child_subscribers': workers,
        'resolver_class': 'myapp.resolvers.ModuloHashResolver'  # user-provided
    }
)

# Publisher side - load balance across API endpoints
endpoints = {f'partition_{i}': endpoint_pubs[i] for i in range(4)}
publisher_router = FanoutDataPublisher(
    'api_router',
    'router://api',
    {
        'child_publishers': endpoints,
        'resolver_class': 'myapp.resolvers.ModuloHashResolver'  # user-provided
    }
)
```

## API Reference

### Common Methods (Both Subscriber and Publisher)

#### Management
```python
# Add child dynamically
router.add_child_subscriber(routing_key, subscriber)  # For subscriber
router.add_child_publisher(routing_key, publisher)    # For publisher

# Remove child
removed = router.remove_child_subscriber(routing_key)  # For subscriber
removed = router.remove_child_publisher(routing_key)   # For publisher

# Get child
child = router.get_child_subscriber(routing_key)  # For subscriber
child = router.get_child_publisher(routing_key)   # For publisher

# List routing keys
keys = router.list_routing_keys()

# Stop router and all children
router.stop()
```

#### Statistics
```python
# Get routing statistics
stats = router.get_routing_statistics()
# Returns: {
#   'total_published'/'total_received': int,
#   'total_routed': int,
#   'total_unrouted': int,
#   'total_errors': int,
#   'routing_efficiency': float (percentage),
#   'routes': {
#       'key1': {'count': int, 'percentage': float},
#       'key2': {'count': int, 'percentage': float}
#   }
# }

# Get child summary
summary = router.get_child_summary()

# Get detailed information
details = router.details()

# Clear unrouted message log
lines_cleared = router.clear_unrouted_file()
```

### FanoutDataSubscriber Specific Methods

```python
# Start router and optionally children
router.start()

# Suspend/resume routing
router.suspend()
router.resume()

# Get data from router's internal queue (rarely needed)
data = router.get_data(block_time=1)

# Get total receive count from all children
total = router.get_total_child_receive_count()
```

### FanoutDataPublisher Specific Methods

```python
# Publish message (will be routed automatically)
router.publish(data)

# Flush all child publishers
router.flush_all()

# Get total publish count from all children
total = router.get_total_child_publish_count()

# Get total queue depth across all children
depth = router.get_total_queue_depth()
```

## Monitoring & Observability

### Key Metrics

```python
# Check routing health
def check_routing_health(router):
    stats = router.get_routing_statistics()
    
    # Routing efficiency should be >95%
    if stats['routing_efficiency'] < 95:
        alert(f"Low routing efficiency: {stats['routing_efficiency']:.1f}%")
    
    # Should have no errors
    if stats['total_errors'] > 0:
        alert(f"Routing errors detected: {stats['total_errors']}")
    
    # Check unrouted message growth
    if stats['total_unrouted'] > 100:
        alert(f"Many unrouted messages: {stats['total_unrouted']}")
```

### Logging

```python
import logging

# Enable debug logging for detailed routing information
logging.getLogger('core.pubsub.fanout_datapubsub').setLevel(logging.DEBUG)

# Each routing decision is logged at DEBUG level
# Errors are logged at ERROR level
# Warnings (null keys, unrouted) at WARNING level
```

### Unrouted Message Analysis

Both routers log unrouted messages to JSONL files:

```python
# Analyze unrouted messages
import json

with open('unrouted_messages.jsonl', 'r') as f:
    for line in f:
        entry = json.loads(line)
        print(f"Unrouted: {entry['routing_key']} at {entry['timestamp']}")
        print(f"  Available keys: {entry['available_keys']}")
        print(f"  Data: {entry['data']}")

# After analysis, clear the file
router.clear_unrouted_file()
```

## Performance Considerations

### Throughput

| Component | Typical Throughput | Latency Overhead |
|-----------|-------------------|------------------|
| FanoutDataSubscriber | 1,000-10,000 msg/sec | <1ms |
| FanoutDataPublisher | 5,000-50,000 msg/sec | <0.5ms |

*Note: Performance depends on resolver complexity and child count*

### Resource Usage

**FanoutDataSubscriber**:
- Memory: ~(queue_depth × msg_size) × (1 + N children)
- Threads: 2 + N (router + source + N children)
- CPU: Minimal (mostly queue operations)

**FanoutDataPublisher**:
- Memory: ~(queue_depth × msg_size) × (1 + N children)
- Threads: 1 + potential N (router + optional child threads)
- CPU: Minimal (resolver + queue operations)

### Optimization Tips

1. **Keep Resolvers Fast** (<1ms)
```python
# ✅ Good: Simple field lookup
class FastResolver:
    def resolve(self, msg):
        return msg.get('type', 'default')

# ❌ Bad: External API call
class SlowResolver:
    def resolve(self, msg):
        return requests.get(f"http://api/resolve/{msg['id']}").json()['route']
```

2. **Right-Size Queues**
```python
# High priority: Small queues for low latency
high_config = {'max_depth': 100}

# Low priority: Large queues for throughput
low_config = {'max_depth': 10000}
```

3. **Monitor Queue Depths**
```python
def check_queue_health(router):
    summary = router.get_child_summary()
    for key, info in summary['children'].items():
        if info['queue_depth'] > info['max_depth'] * 0.9:
            alert(f"Queue {key} is 90% full!")
```

## Integration Patterns

### Workflow Routing

```python
# Route different workflow types to specialized executors
workflow_sub_router = FanoutDataSubscriber(
    'workflow_input',
    {
        'child_subscribers': {
            'chat': chat_executor_sub,
            'rag': rag_executor_sub,
            'planning': planning_executor_sub
        },
        'resolver_class': 'workflows.resolvers.WorkflowTypeResolver'
    }
)

# Route results to different storage backends
workflow_pub_router = FanoutDataPublisher(
    'workflow_output',
    'router://workflow_results',
    {
        'child_publishers': {
            'chat': session_database_pub,
            'rag': knowledge_base_pub,
            'planning': execution_log_pub
        },
        'resolver_class': 'workflows.resolvers.WorkflowTypeResolver'
    }
)
```

### LLM Provider Selection

```python
# Route queries based on complexity/cost
class LLMCostResolver:
    def resolve(self, msg):
        tokens = len(msg.get('prompt', '').split())
        if tokens > 2000:
            return 'opus'  # Most capable
        elif tokens > 500:
            return 'sonnet'  # Balanced
        else:
            return 'haiku'  # Fast and cheap

llm_pub_router = FanoutDataPublisher(
    'llm_router',
    'router://llm',
    {
        'child_publishers': {
            'opus': opus_facade_pub,
            'sonnet': sonnet_facade_pub,
            'haiku': haiku_facade_pub
        },
        'resolver_class': 'llm.resolvers.LLMCostResolver'
    }
)
```

## Best Practices

### 1. Design Resolvers for Your Domain

```python
# ✅ Domain-specific resolver
class OrderProcessingResolver:
    def resolve(self, msg):
        amount = msg.get('amount', 0)
        if amount > 10000:
            return 'high_value_processing'
        elif msg.get('express_shipping'):
            return 'express_fulfillment'
        elif msg.get('international'):
            return 'international_processing'
        else:
            return 'standard_processing'
```

### 2. Handle Unrouted Messages Proactively

```python
# Regular monitoring
def monitor_unrouted():
    import os
    size = os.path.getsize('unrouted_messages.jsonl')
    if size > 1_000_000:  # > 1MB
        alert("Many unrouted messages detected!")
        analyze_unrouted_patterns()
```

### 3. Use Appropriate Routing Strategies

- **Type-based**: For discrete message categories
- **Priority-based**: For QoS/SLA differentiation
- **Hash-based**: For load balancing
- **Range-based**: For numeric thresholds
- **Custom**: For complex business logic

### 4. Test Routing Logic

```python
# Unit test your resolvers
def test_resolver():
    resolver = MyResolver()
    
    assert resolver.resolve({'type': 'order'}) == 'order_queue'
    assert resolver.resolve({'type': 'log'}) == 'log_queue'
    assert resolver.resolve({'unknown': 'data'}) == 'default_queue'
```

### 5. Graceful Degradation

```python
# Provide fallback routing
class RobustResolver:
    def resolve(self, msg):
        try:
            # Primary routing logic
            key = complex_routing_logic(msg)
            if key in valid_keys:
                return key
        except Exception as e:
            logger.error(f"Resolver error: {e}")
        
        # Fallback
        return 'default'
```

## Complete Example: Order Processing System

```python
from core.pubsub.fanout_datapubsub import (
    FanoutDataSubscriber,
    FanoutDataPublisher
)


# Custom resolver for order processing
class OrderResolver:
    def resolve(self, msg):
        if msg.get('amount', 0) > 10000:
            return 'high_value'
        elif msg.get('express'):
            return 'express'
        else:
            return 'standard'


# 1. Create incoming message router
order_source = create_subscriber('orders', {'source': 'kafka://orders_topic'})

order_input_router = FanoutDataSubscriber(
    'order_input',
    {
        'child_subscribers': {
            'high_value': high_value_processor_sub,
            'express': express_processor_sub,
            'standard': standard_processor_sub
        },
        'resolver_class': '__main__.OrderResolver'
    }
)

# 2. Create outgoing message router
order_output_router = FanoutDataPublisher(
    'order_output',
    'router://order_output',
    {
        'child_publishers': {
            'high_value': premium_database_pub,
            'express': fast_queue_pub,
            'standard': batch_database_pub
        },
        'resolver_class': '__main__.OrderResolver'
    }
)

# 3. Start the pipeline
order_input_router.start()


# 4. Processors use the output router
def process_order(order):
    # ... processing logic ...
    result = {'processed': True, 'order': order}
    order_output_router.publish(result)


# 5. Monitor health
def health_check():
    input_stats = order_input_router.get_routing_statistics()
    output_stats = order_output_router.get_routing_statistics()

    print(f"Input efficiency: {input_stats['routing_efficiency']:.1f}%")
    print(f"Output efficiency: {output_stats['routing_efficiency']:.1f}%")
```

## Troubleshooting

### Issue: Messages Not Routing

**Check**:
1. Resolver logic: `print(router.resolver.resolve(test_message))`
2. Routing keys match: `print(router.list_routing_keys())`
3. Unrouted file: `cat unrouted_messages.jsonl`

### Issue: Performance Degradation

**Check**:
1. Queue depths: `router.get_child_summary()`
2. Resolver performance: Profile resolve() method
3. Child publisher/subscriber health

### Issue: Memory Growth

**Solutions**:
1. Reduce queue max_depth
2. Add more child subscribers/publishers
3. Increase processing speed of children

## Summary

The Message Router components provide flexible, high-performance routing for both incoming and outgoing messages:

- **FanoutDataSubscriber**: Route incoming data to appropriate processors
- **FanoutDataPublisher**: Route outgoing data to appropriate destinations

Both components share the same resolver pattern, making them easy to learn and use together in complete data pipelines.


---

# Resilient Connectors Guide

© 2025-2030 Ashutosh Sinha

## Overview

Every supported broker has a **resilient** variant that wraps the standard
client with automatic reconnection and message buffering, so a transient
broker outage does not lose data or crash a DAG. The resilient wrappers are
**drop-in replacements** for the underlying client classes — existing code
keeps working unchanged, gaining resilience transparently.

The simplest way to use them is the `"resilient": true` flag on a subscriber
or publisher config (see each broker's setup guide). This guide documents the
shared behaviour and the per-broker wrapper classes for advanced use.

## The shared resilience pattern

All resilient connectors implement the same three behaviours:

1. **Automatic reconnection** — connection failures are retried with
   configurable back-off; the connector keeps trying in the background
   instead of raising into the DAG.
2. **Message / command buffering** — data produced during an outage is held
   in a thread-safe in-memory queue and flushed once the connection is
   restored, preventing loss.
3. **State restoration** — subscriptions, channels, sessions, queues, QoS
   settings, and consumers are re-established automatically after a
   reconnect, so consumers resume where they left off.

### Common configuration parameters

These apply across all resilient connectors (names are consistent):

| Parameter | Default | Meaning |
| --- | --- | --- |
| `reconnect_tries` | 10 | Maximum reconnection attempts |
| `reconnect_interval_seconds` | 60 | Sleep between reconnection attempts |
| `buffer_max_messages` | 10,000 | Maximum items buffered during an outage |

Buffering can be turned off where the underlying client supports it (e.g.
Redis exposes `buffer_commands`). When the buffer fills, the oldest items are
the ones at risk — size it for your worst expected outage window.

### Enabling resilience from a DAG

```jsonc
// Publisher
{ "destination": "kafka://topic/orders", "resilient": true }
// Subscriber
{ "source": "rabbitmq://queue/inbound", "resilient": true }
```

The factory selects the resilient wrapper automatically when `resilient` is
true. No code changes are required.

## Per-broker wrapper classes

For programmatic use outside a DAG, each broker provides a wrapper class with
the same semantics. They live under `core.pubsub.resilient_*`.

### Kafka — `ReconnectAwareKafkaProducer` / resilient consumer
Extends the Kafka producer/consumer. Buffers `send()` calls during outages,
processes the buffer on a background thread once reconnected, and supports
`send_batch()`, `flush()`, and a clean `close()` that drains the buffer.
Helpers: `get_buffer_size()`, `get_failed_messages()`. Works with both the
`kafka-python` and `confluent-kafka` libraries.

### Redis — `ResilientRedisClient`
Extends `redis.Redis`. Adds reconnection, optional command buffering
(`buffer_commands`, `buffer_max_commands`), a resilient pipeline with retry
for batch operations, and automatic restoration of pub/sub subscriptions
after a reconnect.

### RabbitMQ — `ResilientRabbitBlockingConnection`
Extends `pika.BlockingConnection`. Provides a `ResilientChannel` wrapper that
tracks exchanges, queues, bindings, QoS, and consumers, and automatically
restores all of that channel state after a reconnect.

### ActiveMQ — `ResilientActiveMQConnection`
Extends `stomp.Connection`. Handles connection failures during any operation,
buffers messages during outages, and automatically restores subscriptions
after reconnection.

### TIBCO EMS — `ResilientTibcoEMSConnection`
Provides resilient JMS-style connectivity. A `ResilientSession` tracks
producers, consumers, and subscriptions and restores them after a reconnect.
Extra parameters: `server_url` (default `tcp://localhost:7222`) and
`client_id` for durable subscriptions.

### IBM WebSphere MQ / IBM MQ — `ResilientWebSphereMQQueueManager`
Extends `pymqi.QueueManager`. Restores open queues after reconnection and
supports the full MQ feature set (transactions, PCF commands, pub/sub,
SSL/TLS). Extra parameters: `queue_manager_name`, `channel`,
`connection_info` (`host(port)`), `use_ssl`, `ssl_cipher_spec`.

## When to use resilient connectors

- **Use them** for production pipelines against remote brokers, where network
  blips, broker restarts, and failovers are expected.
- **Skip them** for the in-memory/LMDB transports (there is no remote
  connection to lose) and for short-lived local development runs.

For broker-specific connection settings (URIs, auth, topics vs queues), see
that broker's own setup guide.

## TLS/SSL with resilient connectors

Resilience and transport security are independent and compose freely: set
`"resilient": true` to get reconnection/buffering, and set the broker's TLS
keys (e.g. `use_ssl`, `ssl_ca_certs`) to encrypt the connection. Each broker
has its own SSL/TLS configuration — see the **SSL/TLS Security** section in
that broker's setup guide for the exact keys and a sample (Kafka also supports
SASL). Cloud services (AWS SQS/Kinesis/SNS, Azure Service Bus/Event Hubs, and
the S3/Azure Blob/GCS object stores) are encrypted in transit by default
through their SDKs.


---

