# File DataPublisher and DataSubscriber Setup Guide

## Overview

The File implementation provides publisher and subscriber classes for file-based messaging. Publishers append JSON messages to files, and subscribers read them in real-time (similar to `tail -f`). This pattern is ideal for logging, data pipelines, file-based integration, and scenarios where you need persistent message storage without a message broker.

## Files

1. **file_datapubsub.py** - Contains `FileDataPublisher` and `FileDataSubscriber` classes
2. **pubsubfactory.py** - Factory methods support `file://` destinations/sources

## Prerequisites

No external dependencies required! The file implementation uses only Python standard library:
- `json` - For message serialization
- `os` - For file operations
- Built-in file I/O

## File-Based Messaging Concepts

### How It Works

**Publisher:**
- Appends JSON-formatted messages to a file (one message per line)
- Creates directories automatically if they don't exist
- Thread-safe (uses locks from base class)
- Each message is a complete JSON object on one line

**Subscriber:**
- Reads new lines from file as they're written (tail-like behavior)
- Tracks read position to continue from where it left off
- Polls file at regular intervals
- Deserializes JSON messages

### Message Format

Each line in the file is a complete JSON object:
```
{"event": "user_login", "user_id": 123, "timestamp": "2025-01-15T10:00:00"}
{"event": "page_view", "user_id": 123, "page": "/home"}
{"event": "user_logout", "user_id": 123, "timestamp": "2025-01-15T10:30:00"}
```

### Use Cases

**Good for:**
- Application logging
- Event audit trails
- Data export/import
- File-based integration with other systems
- Simple message persistence
- Development and testing
- Log aggregation pipelines

**Not suitable for:**
- High-frequency messaging (use Kafka, RabbitMQ)
- Complex routing (use message brokers)
- Distributed systems (use proper message queues)
- Transactional messaging
- Message acknowledgment/retries

## Usage

### Publishing to a File

Basic file publishing:

```python
from core.pubsub.pubsubfactory import create_publisher

config = {
    'destination': 'file:///var/log/events.log'
}

publisher = create_publisher('event_logger', config)

# Publish events
publisher.publish({
    'event': 'user_login',
    'user_id': 123,
    'timestamp': '2025-01-15T10:00:00'
})

publisher.publish({
    'event': 'page_view',
    'user_id': 123,
    'page': '/dashboard'
})

# Stop when done
publisher.stop()
```

### Publishing with Batching

Use batch publishing for better performance:

```python
config = {
    'destination': 'file:///var/log/events.log',
    'publish_interval': 5,      # Flush every 5 seconds
    'batch_size': 100           # Or when 100 messages queued
}

publisher = create_publisher('batch_logger', config)

# Messages are batched automatically
for i in range(1000):
    publisher.publish({'event': 'metric', 'value': i})

# Stop flushes remaining messages
publisher.stop()
```

### Subscribing from a File

Tail-like file reading:

```python
from core.pubsub.pubsubfactory import create_subscriber

config = {
    'source': 'file:///var/log/events.log',
    'read_interval': 1  # Poll every 1 second
}

subscriber = create_subscriber('event_processor', config)
subscriber.start()

# Read messages as they arrive
while True:
    data = subscriber.get_data(block_time=2)
    if data:
        print(f"Received: {data}")
    else:
        print("No new messages")

subscriber.stop()
```

### Reading Existing File

Process messages from existing file:

```python
config = {
    'source': 'file:///var/log/archived_events.log',
    'read_interval': 0.1  # Faster polling for batch processing
}

subscriber = create_subscriber('batch_processor', config)
subscriber.start()

# Process all messages
processed = 0
while True:
    data = subscriber.get_data(block_time=1)
    if data:
        process_event(data)
        processed += 1
    else:
        break  # No more messages

print(f"Processed {processed} events")
subscriber.stop()
```

## Destination/Source Format

### File URL Format
```
file:///absolute/path/to/file.log
file://./relative/path/to/file.log
```

**Examples:**
- `file:///var/log/app/events.log` - Absolute path
- `file://./logs/events.log` - Relative path
- `file:///tmp/data.jsonl` - Temp directory
- `file://~/logs/app.log` - Home directory (will be expanded)

**Path Handling:**
- Absolute paths: Start with `/` (Unix) or `C:\` (Windows)
- Relative paths: Relative to current working directory
- Directories are created automatically if they don't exist
- File is created on first write

## Configuration Options

### Publisher Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `destination` | string | Required | File path with `file://` prefix |
| `publish_interval` | int | `0` | Batch publishing interval in seconds |
| `batch_size` | int | `None` | Flush after N messages |

### Subscriber Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source` | string | Required | File path with `file://` prefix |
| `read_interval` | float | `1.0` | Polling interval in seconds |
| `max_depth` | int | `100000` | Internal queue maximum size |

## Common Patterns

### Pattern 1: Application Logging

Centralized event logging:

```python
# Application logger
app_logger = create_publisher('app_logger', {
    'destination': 'file:///var/log/myapp/events.log',
    'publish_interval': 5  # Batch writes every 5 seconds
})

# Log various events
app_logger.publish({
    'level': 'INFO',
    'message': 'Application started',
    'timestamp': '2025-01-15T10:00:00'
})

app_logger.publish({
    'level': 'ERROR',
    'message': 'Database connection failed',
    'error': 'Connection timeout',
    'timestamp': '2025-01-15T10:05:00'
})

# Log processor
log_processor = create_subscriber('log_analyzer', {
    'source': 'file:///var/log/myapp/events.log'
})
log_processor.start()

# Analyze logs in real-time
while True:
    log = log_processor.get_data(block_time=1)
    if log:
        if log['level'] == 'ERROR':
            send_alert(log)
```

### Pattern 2: Data Pipeline

File-based ETL pipeline:

```python
# Extract stage - write raw data
extractor = create_publisher('extractor', {
    'destination': 'file://./pipeline/raw_data.jsonl'
})

# Extract data from source
for record in data_source:
    extractor.publish(record)

# Transform stage - read, transform, write
raw_reader = create_subscriber('transformer_reader', {
    'source': 'file://./pipeline/raw_data.jsonl'
})
raw_reader.start()

transformer_writer = create_publisher('transformer_writer', {
    'destination': 'file://./pipeline/transformed_data.jsonl'
})

while True:
    raw_data = raw_reader.get_data(block_time=1)
    if raw_data:
        transformed = transform(raw_data)
        transformer_writer.publish(transformed)
    else:
        break

# Load stage - read and load to database
loader_reader = create_subscriber('loader', {
    'source': 'file://./pipeline/transformed_data.jsonl'
})
loader_reader.start()

while True:
    data = loader_reader.get_data(block_time=1)
    if data:
        database.insert(data)
    else:
        break
```

### Pattern 3: Multi-Publisher

Multiple publishers writing to same file:

```python
# Web server events
web_pub = create_publisher('web_server', {
    'destination': 'file:///var/log/system_events.log'
})

# API events
api_pub = create_publisher('api_server', {
    'destination': 'file:///var/log/system_events.log'
})

# Background worker events
worker_pub = create_publisher('worker', {
    'destination': 'file:///var/log/system_events.log'
})

# All write to same file
web_pub.publish({'source': 'web', 'event': 'request_received'})
api_pub.publish({'source': 'api', 'event': 'auth_success'})
worker_pub.publish({'source': 'worker', 'event': 'job_completed'})

# Single subscriber reads all
monitor = create_subscriber('monitor', {
    'source': 'file:///var/log/system_events.log'
})
monitor.start()
```

### Pattern 4: Audit Trail

Immutable audit log:

```python
# Audit logger
audit_log = create_publisher('audit', {
    'destination': 'file:///var/audit/system_audit.log'
})

# Log all important operations
audit_log.publish({
    'action': 'user_created',
    'user_id': 123,
    'created_by': 'admin',
    'timestamp': '2025-01-15T10:00:00',
    'ip_address': '192.168.1.100'
})

audit_log.publish({
    'action': 'permission_changed',
    'user_id': 123,
    'permission': 'admin',
    'changed_by': 'superadmin',
    'timestamp': '2025-01-15T10:05:00'
})

# Audit analysis
auditor = create_subscriber('audit_analyzer', {
    'source': 'file:///var/audit/system_audit.log'
})
auditor.start()

# Generate compliance reports
while True:
    record = auditor.get_data(block_time=1)
    if record:
        analyze_for_compliance(record)
```

### Pattern 5: Data Export/Import

Export data for external systems:

```python
# Export from database to file
exporter = create_publisher('db_exporter', {
    'destination': 'file://./exports/customers.jsonl',
    'batch_size': 1000
})

# Export customer data
for customer in database.query("SELECT * FROM customers"):
    exporter.publish({
        'id': customer.id,
        'name': customer.name,
        'email': customer.email,
        'created_at': customer.created_at.isoformat()
    })

exporter.stop()  # Flush remaining batch

# Import in another system
importer = create_subscriber('importer', {
    'source': 'file://./exports/customers.jsonl',
    'read_interval': 0.1  # Fast reading for batch import
})
importer.start()

imported = 0
while True:
    customer = importer.get_data(block_time=1)
    if customer:
        other_system.import_customer(customer)
        imported += 1
    else:
        break

print(f"Imported {imported} customers")
```

## File Management

### Directory Creation

Directories are created automatically:

```python
# This creates /var/log/myapp/ if it doesn't exist
publisher = create_publisher('app', {
    'destination': 'file:///var/log/myapp/events.log'
})
```

### File Rotation

Handle log rotation manually:

```python
import os
from datetime import datetime

# Rotate log daily
def rotate_log_if_needed(current_file):
    today = datetime.now().strftime('%Y%m%d')
    rotated_file = f"{current_file}.{today}"
    
    if os.path.exists(current_file):
        # Check if already rotated today
        if not os.path.exists(rotated_file):
            os.rename(current_file, rotated_file)
            return True
    return False

# Before publishing
rotate_log_if_needed('/var/log/myapp/events.log')

publisher = create_publisher('app', {
    'destination': 'file:///var/log/myapp/events.log'
})
```

### Multiple Files by Date

Date-based file separation:

```python
from datetime import datetime

# Get today's log file
today = datetime.now().strftime('%Y-%m-%d')
log_file = f'file:///var/log/events-{today}.log'

publisher = create_publisher('daily_logger', {
    'destination': log_file
})
```

## Monitoring

### Get Publisher Statistics

```python
stats = publisher.details()
print(f"Name: {stats['name']}")
print(f"File: {stats['destination']}")
print(f"Published: {stats['publish_count']} messages")
print(f"Last publish: {stats['last_publish']}")
print(f"Queue depth: {stats['queue_depth']}")  # Pending writes
```

### Get Subscriber Statistics

```python
stats = subscriber.details()
print(f"Name: {stats['name']}")
print(f"File: {stats['source']}")
print(f"Received: {stats['receive_count']} messages")
print(f"Last receive: {stats['last_receive']}")
print(f"Internal queue: {stats['current_depth']}")
```

### Check File Size

```python
import os

file_path = '/var/log/myapp/events.log'
if os.path.exists(file_path):
    size_bytes = os.path.getsize(file_path)
    size_mb = size_bytes / (1024 * 1024)
    print(f"File size: {size_mb:.2f} MB")
    
    # Count lines
    with open(file_path, 'r') as f:
        line_count = sum(1 for _ in f)
    print(f"Total messages: {line_count}")
```

## Troubleshooting

### Permission Denied

**Error**: `PermissionError: [Errno 13] Permission denied`

**Solution**: Ensure write permissions
```bash
# Grant write permission
chmod 664 /var/log/myapp/events.log

# Or use user-writable directory
config = {
    'destination': 'file://~/logs/events.log'  # Home directory
}
```

### Directory Not Found

**Error**: Directory doesn't exist

**Solution**: Directories are created automatically, but ensure parent has write permissions
```bash
# Ensure parent directory exists and is writable
mkdir -p /var/log/myapp
chmod 755 /var/log/myapp
```

### File Not Found (Subscriber)

**Error**: File doesn't exist when subscriber starts

**Solution**: This is normal - subscriber waits for file to be created
```python
# Subscriber waits patiently
subscriber = create_subscriber('reader', {
    'source': 'file:///var/log/events.log'
})
subscriber.start()

# File can be created later by publisher
publisher = create_publisher('writer', {
    'destination': 'file:///var/log/events.log'
})
```

### Malformed JSON

**Error**: `JSONDecodeError`

**Solution**: Ensure data is JSON-serializable
```python
from datetime import datetime

# Wrong - datetime not serializable
publisher.publish({
    'timestamp': datetime.now()  # ❌ Error!
})

# Correct - convert to string
publisher.publish({
    'timestamp': datetime.now().isoformat()  # ✅ Good
})
```

### File Growing Too Large

**Problem**: Log file becomes huge

**Solution**: Implement rotation or compression
```python
import os
import gzip
import shutil

def rotate_and_compress(log_file):
    if os.path.getsize(log_file) > 100 * 1024 * 1024:  # 100MB
        # Compress old file
        with open(log_file, 'rb') as f_in:
            with gzip.open(f'{log_file}.gz', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Clear current file
        open(log_file, 'w').close()
```

## Best Practices

1. **Use absolute paths** - Avoid confusion about working directory
2. **Implement rotation** - Prevent files from growing indefinitely
3. **Use batching** - Set `publish_interval` for better performance
4. **Handle JSON serialization** - Convert datetimes and other types
5. **Monitor file sizes** - Implement alerts for large files
6. **Use meaningful filenames** - Include date, application name
7. **Backup important logs** - Regular archival for audit trails
8. **Set appropriate permissions** - Secure sensitive logs
9. **Close publishers** - Always call `stop()` to flush buffers
10. **Test rotation** - Ensure subscriber handles rotated files

## Performance Tips

1. **Batch writing**: Use `publish_interval` and `batch_size`
2. **Fast storage**: Use SSD for high-throughput logging
3. **Reduce polling**: Increase `read_interval` for less-frequent updates
4. **Compress old logs**: Save disk space
5. **Use binary formats**: For non-human-readable data (would require custom implementation)
6. **Separate files**: Use different files for different event types
7. **Async I/O**: Let batching handle async writes
8. **Buffer tuning**: OS-level I/O buffer optimization
9. **Monitor I/O wait**: Check if disk is bottleneck
10. **Use tmpfs**: For temporary high-speed logging

## Complete Examples

### Example 1: Simple Event Logging

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
from datetime import datetime

# Create event logger
logger = create_publisher('event_logger', {
    'destination': 'file://./logs/events.log'
})

# Create log processor
processor = create_subscriber('log_processor', {
    'source': 'file://./logs/events.log',
    'read_interval': 0.5
})
processor.start()

print("Starting event logging...\n")

# Log some events
events = [
    {'level': 'INFO', 'message': 'Application started'},
    {'level': 'INFO', 'message': 'User logged in', 'user_id': 123},
    {'level': 'WARNING', 'message': 'High memory usage', 'memory_mb': 1024},
    {'level': 'ERROR', 'message': 'Database connection failed'},
    {'level': 'INFO', 'message': 'Connection retried successfully'}
]

# Publish events
for event in events:
    event['timestamp'] = datetime.now().isoformat()
    logger.publish(event)
    print(f"Logged: [{event['level']}] {event['message']}")
    time.sleep(0.5)

print("\nProcessing logs...\n")

# Process events
for i in range(len(events)):
    log_entry = processor.get_data(block_time=2)
    if log_entry:
        level = log_entry['level']
        msg = log_entry['message']
        
        # Color code by level
        prefix = {
            'INFO': '✓',
            'WARNING': '⚠',
            'ERROR': '✗'
        }.get(level, '•')
        
        print(f"{prefix} [{level}] {msg}")

# Cleanup
logger.stop()
processor.stop()

print("\n✓ Example 1 completed!")
```

### Example 2: Multi-Stage Data Pipeline

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
import os

# Clean up old files
for f in ['raw_data.jsonl', 'cleaned_data.jsonl', 'enriched_data.jsonl']:
    if os.path.exists(f'./pipeline/{f}'):
        os.remove(f'./pipeline/{f}')

print("=== Stage 1: Data Extraction ===\n")

# Stage 1: Extract raw data
extractor = create_publisher('extractor', {
    'destination': 'file://./pipeline/raw_data.jsonl'
})

# Extract sample data
raw_records = [
    {'id': 1, 'value': '100', 'status': 'active  '},
    {'id': 2, 'value': 'invalid', 'status': 'pending'},
    {'id': 3, 'value': '250', 'status': 'active'},
    {'id': 4, 'value': '75', 'status': 'INACTIVE'},
    {'id': 5, 'value': '300', 'status': 'active'}
]

for record in raw_records:
    extractor.publish(record)
    print(f"Extracted: {record}")
    time.sleep(0.2)

extractor.stop()
print(f"\n✓ Extracted {len(raw_records)} records\n")

time.sleep(0.5)

print("=== Stage 2: Data Cleaning ===\n")

# Stage 2: Clean data
raw_reader = create_subscriber('raw_reader', {
    'source': 'file://./pipeline/raw_data.jsonl',
    'read_interval': 0.1
})
raw_reader.start()

cleaner = create_publisher('cleaner', {
    'destination': 'file://./pipeline/cleaned_data.jsonl'
})

cleaned_count = 0
while True:
    raw_data = raw_reader.get_data(block_time=1)
    if raw_data:
        # Clean the data
        try:
            cleaned = {
                'id': raw_data['id'],
                'value': int(raw_data['value']),  # Convert to int
                'status': raw_data['status'].strip().lower()  # Normalize
            }
            cleaner.publish(cleaned)
            print(f"Cleaned: {cleaned}")
            cleaned_count += 1
        except ValueError:
            print(f"Skipped invalid record: {raw_data}")
    else:
        break

raw_reader.stop()
cleaner.stop()
print(f"\n✓ Cleaned {cleaned_count} records\n")

time.sleep(0.5)

print("=== Stage 3: Data Enrichment ===\n")

# Stage 3: Enrich data
cleaned_reader = create_subscriber('cleaned_reader', {
    'source': 'file://./pipeline/cleaned_data.jsonl',
    'read_interval': 0.1
})
cleaned_reader.start()

enricher = create_publisher('enricher', {
    'destination': 'file://./pipeline/enriched_data.jsonl'
})

enriched_count = 0
while True:
    clean_data = cleaned_reader.get_data(block_time=1)
    if clean_data:
        # Enrich with calculated fields
        enriched = {
            **clean_data,
            'category': 'high' if clean_data['value'] > 200 else 'low',
            'processed_at': time.time(),
            'is_active': clean_data['status'] == 'active'
        }
        enricher.publish(enriched)
        print(f"Enriched: ID={enriched['id']}, Category={enriched['category']}")
        enriched_count += 1
    else:
        break

cleaned_reader.stop()
enricher.stop()
print(f"\n✓ Enriched {enriched_count} records\n")

time.sleep(0.5)

print("=== Stage 4: Final Analysis ===\n")

# Stage 4: Analyze enriched data
analyzer = create_subscriber('analyzer', {
    'source': 'file://./pipeline/enriched_data.jsonl',
    'read_interval': 0.1
})
analyzer.start()

high_value = 0
low_value = 0
active_count = 0

while True:
    data = analyzer.get_data(block_time=1)
    if data:
        if data['category'] == 'high':
            high_value += 1
        else:
            low_value += 1
        
        if data['is_active']:
            active_count += 1
    else:
        break

analyzer.stop()

print(f"Analysis Results:")
print(f"  High value records: {high_value}")
print(f"  Low value records: {low_value}")
print(f"  Active records: {active_count}")

print("\n✓ Example 2 completed!")
```

### Example 3: Real-Time Log Monitoring

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
import threading
import random
from datetime import datetime

print("=== Real-Time Log Monitoring ===\n")

# Application simulator
def application_simulator():
    """Simulates an application generating logs"""
    app_logger = create_publisher('app', {
        'destination': 'file://./logs/application.log',
        'publish_interval': 1  # Batch every second
    })
    
    log_levels = ['INFO', 'WARNING', 'ERROR']
    messages = [
        'Request processed',
        'Database query executed',
        'Cache miss',
        'High CPU usage detected',
        'Connection timeout',
        'Retry successful'
    ]
    
    for i in range(20):
        level = random.choice(log_levels)
        message = random.choice(messages)
        
        app_logger.publish({
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'message': message,
            'request_id': f'REQ-{i:04d}'
        })
        
        time.sleep(random.uniform(0.1, 0.5))
    
    app_logger.stop()

# Start application in background
app_thread = threading.Thread(target=application_simulator, daemon=True)
app_thread.start()

time.sleep(0.5)  # Wait for first logs

# Real-time monitor
monitor = create_subscriber('monitor', {
    'source': 'file://./logs/application.log',
    'read_interval': 0.5
})
monitor.start()

print("Monitoring logs (press Ctrl+C to stop)...\n")

error_count = 0
warning_count = 0
info_count = 0

try:
    for i in range(25):  # Monitor for a while
        log = monitor.get_data(block_time=1)
        if log:
            level = log['level']
            
            # Count by level
            if level == 'ERROR':
                error_count += 1
                print(f"❌ ERROR: {log['message']} [{log['request_id']}]")
                # Send alert for errors
                print(f"   → Alert sent to ops team")
            elif level == 'WARNING':
                warning_count += 1
                print(f"⚠️  WARNING: {log['message']} [{log['request_id']}]")
            else:
                info_count += 1
                print(f"ℹ️  INFO: {log['message']} [{log['request_id']}]")
except KeyboardInterrupt:
    pass

# Wait for application to finish
app_thread.join(timeout=2)
monitor.stop()

print(f"\n--- Monitoring Summary ---")
print(f"INFO messages: {info_count}")
print(f"WARNING messages: {warning_count}")
print(f"ERROR messages: {error_count}")
print(f"Total: {info_count + warning_count + error_count}")

print("\n✓ Example 3 completed!")
```

### Example 4: Data Export and Import

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time

print("=== Data Export/Import Example ===\n")

# Simulate database export
print("Step 1: Exporting data from database...\n")

exporter = create_publisher('db_exporter', {
    'destination': 'file://./exports/customers.jsonl',
    'batch_size': 5  # Batch every 5 records
})

# Sample customer data
customers = [
    {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'country': 'US'},
    {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'country': 'UK'},
    {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com', 'country': 'CA'},
    {'id': 4, 'name': 'Alice Brown', 'email': 'alice@example.com', 'country': 'AU'},
    {'id': 5, 'name': 'Charlie Wilson', 'email': 'charlie@example.com', 'country': 'US'},
    {'id': 6, 'name': 'Diana Davis', 'email': 'diana@example.com', 'country': 'UK'},
    {'id': 7, 'name': 'Eve Miller', 'email': 'eve@example.com', 'country': 'CA'},
    {'id': 8, 'name': 'Frank Moore', 'email': 'frank@example.com', 'country': 'AU'}
]

for customer in customers:
    exporter.publish(customer)
    print(f"Exported: {customer['name']} ({customer['email']})")
    time.sleep(0.1)

exporter.stop()  # Flush remaining batch
print(f"\n✓ Exported {len(customers)} customers\n")

time.sleep(0.5)

# Simulate import to another system
print("Step 2: Importing data to external system...\n")

importer = create_subscriber('importer', {
    'source': 'file://./exports/customers.jsonl',
    'read_interval': 0.1
})
importer.start()

imported_by_country = {}
imported_count = 0

while True:
    customer = importer.get_data(block_time=1)
    if customer:
        # Simulate import processing
        country = customer['country']
        imported_by_country[country] = imported_by_country.get(country, 0) + 1
        
        print(f"Imported: {customer['name']} → {country}")
        imported_count += 1
        time.sleep(0.1)
    else:
        break

importer.stop()

print(f"\n--- Import Summary ---")
print(f"Total imported: {imported_count}")
print(f"By country:")
for country, count in sorted(imported_by_country.items()):
    print(f"  {country}: {count}")

print("\n✓ Example 4 completed!")
```

### Example 5: Audit Trail System

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import time
from datetime import datetime

print("=== Audit Trail System ===\n")

# Create audit logger
audit_logger = create_publisher('audit', {
    'destination': 'file://./audit/system_audit.log'
})

print("Recording audit events...\n")

# Record various system operations
audit_events = [
    {
        'action': 'USER_LOGIN',
        'user': 'admin',
        'ip': '192.168.1.100',
        'success': True
    },
    {
        'action': 'USER_CREATED',
        'user': 'admin',
        'target_user': 'john_doe',
        'roles': ['user', 'viewer']
    },
    {
        'action': 'PERMISSION_CHANGED',
        'user': 'admin',
        'target_user': 'john_doe',
        'permission': 'edit_documents',
        'granted': True
    },
    {
        'action': 'DOCUMENT_ACCESSED',
        'user': 'john_doe',
        'document_id': 'DOC-123',
        'document_name': 'Financial Report Q4'
    },
    {
        'action': 'DOCUMENT_MODIFIED',
        'user': 'john_doe',
        'document_id': 'DOC-123',
        'changes': ['updated_section_3', 'added_chart']
    },
    {
        'action': 'LOGIN_FAILED',
        'user': 'unknown_user',
        'ip': '203.0.113.45',
        'reason': 'invalid_credentials',
        'success': False
    },
    {
        'action': 'USER_LOGOUT',
        'user': 'john_doe',
        'session_duration_minutes': 45
    }
]

for event in audit_events:
    event['timestamp'] = datetime.now().isoformat()
    event['event_id'] = f"EVT-{time.time():.0f}"
    
    audit_logger.publish(event)
    print(f"✓ Logged: {event['action']} by {event.get('user', 'system')}")
    time.sleep(0.3)

audit_logger.stop()
print(f"\n✓ Recorded {len(audit_events)} audit events\n")

time.sleep(0.5)

# Analyze audit trail
print("Analyzing audit trail...\n")

auditor = create_subscriber('auditor', {
    'source': 'file://./audit/system_audit.log',
    'read_interval': 0.1
})
auditor.start()

# Statistics
user_actions = {}
failed_logins = []
document_accesses = []

while True:
    event = auditor.get_data(block_time=1)
    if event:
        # Count actions by user
        user = event.get('user', 'system')
        user_actions[user] = user_actions.get(user, 0) + 1
        
        # Track failed logins
        if event['action'] == 'LOGIN_FAILED':
            failed_logins.append(event)
        
        # Track document access
        if event['action'] == 'DOCUMENT_ACCESSED':
            document_accesses.append(event)
    else:
        break

auditor.stop()

print("--- Audit Analysis Report ---\n")

print("User Activity:")
for user, count in sorted(user_actions.items(), key=lambda x: x[1], reverse=True):
    print(f"  {user}: {count} actions")

print(f"\nSecurity Alerts:")
print(f"  Failed login attempts: {len(failed_logins)}")
if failed_logins:
    for failed in failed_logins:
        print(f"    - User: {failed['user']}, IP: {failed['ip']}")

print(f"\nDocument Access:")
print(f"  Total accesses: {len(document_accesses)}")
if document_accesses:
    for access in document_accesses:
        print(f"    - {access['document_name']} by {access['user']}")

print("\n✓ Example 5 completed!")
```

## Comparison with Other Brokers

| Feature | File | ActiveMQ | Kafka | RabbitMQ | TIBCO EMS | IBM MQ |
|---------|------|----------|-------|----------|-----------|--------|
| Setup | None | Easy | Medium | Medium | Hard | Hard |
| Dependencies | None | Broker | Broker | Broker | Broker | Broker |
| Persistence | Always | Optional | Always | Optional | Optional | Optional |
| Message replay | ✅ Yes | ❌ No | ✅ Yes | ❌ No | Limited | Limited |
| Human readable | ✅ Yes | ❌ No | ❌ No | ❌ No | ❌ No | ❌ No |
| Best for | Logs, simple pipelines | Traditional MQ | Event streaming | Traditional MQ | Enterprise MQ | Enterprise MQ |
| Throughput | Low-Medium | Medium | Very High | Medium-High | Medium-High | Medium-High |
| Cost | Free | Free | Free | Free | $$$ | $$$ |

## When to Use File-Based Pub/Sub

**Use File-based pub/sub when:**
- Simple logging requirements
- Data export/import between systems
- Development and testing
- Audit trails and compliance logs
- File-based integration requirements
- No message broker available
- Persistent message storage needed
- Human-readable format preferred

**Use message brokers instead when:**
- High-throughput messaging needed
- Complex routing required
- Distributed systems
- Real-time processing critical
- Message acknowledgment needed
- Transaction support required

## Key Advantages

1. **No Dependencies**: No broker installation required
2. **Simple**: Easy to understand and debug
3. **Persistent**: Messages stored permanently
4. **Human Readable**: JSON format easy to inspect
5. **Portable**: Files can be moved between systems
6. **Debuggable**: Use standard tools (grep, awk, etc.)
7. **Free**: No licensing or infrastructure costs
8. **Reliable**: Filesystem reliability

The File implementation provides a simple, reliable foundation for logging, data pipelines, and file-based integration patterns!


## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.