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
