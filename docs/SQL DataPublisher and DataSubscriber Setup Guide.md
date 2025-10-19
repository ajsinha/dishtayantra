# SQL DataPublisher and DataSubscriber Setup Guide

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