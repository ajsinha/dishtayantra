# REST DataPublisher and DataSubscriber Setup Guide

## © 2025-2030 Ashutosh Sinha


## Overview

The REST implementation provides publisher and subscriber classes for REST API-based messaging. Publishers send data to REST endpoints via HTTP POST/PUT/PATCH, and subscribers poll REST endpoints via HTTP GET/POST. This pattern is ideal for integrating with external REST APIs, microservices communication, webhook-style publishing, API-first architectures, and cloud service integration.

## Files

1. **rest_datapubsub.py** - Contains `RESTDataPublisher` and `RESTDataSubscriber` classes
2. **pubsubfactory.py** - Factory methods support `rest://` destinations/sources

## Prerequisites

### Install Python Library

```bash
pip install requests
```

That's it! No server installation needed - you connect to existing REST APIs.

## REST-Based Messaging Concepts

### How It Works

**Publisher:**
- Sends HTTP POST/PUT/PATCH requests to REST endpoints
- Serializes data to JSON
- Supports various authentication methods
- Retries on failures
- Session-based connection pooling

**Subscriber:**
- Polls REST endpoints via HTTP GET/POST
- Schedule-based polling (configurable interval)
- Parses JSON responses
- Supports pagination
- Tracks last fetched ID for incremental updates

### Configuration File

REST connection details are stored in a JSON configuration file:

**rest_config.json (Publisher):**
```json
{
    "base_url": "https://api.example.com",
    "publish_endpoint": "events",
    "http_method": "POST",
    "auth_type": "bearer",
    "token": "your-api-token-here",
    "headers": {
        "Content-Type": "application/json",
        "X-Custom-Header": "value"
    },
    "timeout": 30,
    "verify_ssl": true,
    "max_retries": 3,
    "retry_delay": 2
}
```

**rest_config.json (Subscriber):**
```json
{
    "base_url": "https://api.example.com",
    "subscribe_endpoint": "events",
    "http_method": "GET",
    "auth_type": "api_key",
    "api_key": "your-api-key",
    "api_key_header": "X-API-Key",
    "timeout": 30,
    "verify_ssl": true,
    "query_params": {
        "status": "pending",
        "limit": 1
    },
    "response_data_key": "data",
    "pagination_enabled": false,
    "last_id_key": "id"
}
```

### Use Cases

**Perfect for:**
- REST API integration
- Microservices communication
- Webhook publishing
- Cloud service integration
- Third-party API interaction
- API-first architectures
- Event webhooks
- Data synchronization

**Not suitable for:**
- High-frequency messaging (REST overhead)
- Real-time requirements (polling delay)
- Large message volumes (HTTP overhead)
- When dedicated message brokers available

## Usage

### Publishing to REST API

Basic REST API publishing:

```python
from core.pubsub.pubsubfactory import create_publisher
import json

# Create REST config
rest_config = {
    "base_url": "https://api.example.com",
    "publish_endpoint": "events",
    "http_method": "POST",
    "auth_type": "bearer",
    "token": "your-token-here",
    "timeout": 30
}

with open('rest_config.json', 'w') as f:
    json.dump(rest_config, f)

# Create publisher
config = {
    'destination': 'rest://events',
    'rest_config_file': 'rest_config.json'
}

publisher = create_publisher('api_publisher', config)

# Publish data
publisher.publish({
    'event_type': 'user_signup',
    'user_id': 123,
    'email': 'user@example.com',
    'timestamp': '2025-01-15T10:00:00Z'
})

publisher.stop()
```

### Subscribing from REST API

Basic REST API polling:

```python
from core.pubsub.pubsubfactory import create_subscriber
import json

# Create REST config
rest_config = {
    "base_url": "https://api.example.com",
    "subscribe_endpoint": "events",
    "http_method": "GET",
    "auth_type": "api_key",
    "api_key": "your-api-key",
    "api_key_header": "X-API-Key",
    "query_params": {
        "status": "pending"
    },
    "response_data_key": "data"
}

with open('rest_config.json', 'w') as f:
    json.dump(rest_config, f)

# Create subscriber
config = {
    'source': 'rest://events',
    'rest_config_file': 'rest_config.json',
    'poll_interval': 5  # Poll every 5 seconds
}

subscriber = create_subscriber('api_subscriber', config)
subscriber.start()

# Receive data
data = subscriber.get_data(block_time=10)
if data:
    print(f"Received: {data}")

subscriber.stop()
```

## Destination/Source Format

### REST URL Format
```
rest://endpoint_name
```

The actual URL is constructed from config:
```
{base_url}/{publish_endpoint or subscribe_endpoint}
```

**Examples:**
- `rest://events` → `https://api.example.com/events`
- `rest://webhooks/notifications` → `https://api.example.com/webhooks/notifications`
- `rest://api/v1/messages` → `https://api.example.com/api/v1/messages`

## Configuration Options

### Publisher Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `destination` | string | Required | REST destination with `rest://` prefix |
| `rest_config_file` | string | Required | Path to REST config JSON file |
| `publish_interval` | int | `0` | Batch publishing interval (inherited) |
| `batch_size` | int | `None` | Batch size threshold (inherited) |

### Subscriber Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source` | string | Required | REST source with `rest://` prefix |
| `rest_config_file` | string | Required | Path to REST config JSON file |
| `poll_interval` | float | `5.0` | Polling interval in seconds |
| `max_depth` | int | `100000` | Internal queue maximum size (inherited) |

### REST Config File Options

#### Common Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `base_url` | string | Yes | Base URL of the API |
| `publish_endpoint` | string | Publisher | Endpoint path for publishing |
| `subscribe_endpoint` | string | Subscriber | Endpoint path for subscribing |
| `http_method` | string | POST/GET | HTTP method (POST, PUT, PATCH, GET) |
| `auth_type` | string | No | Authentication type (none, basic, bearer, api_key) |
| `timeout` | int | 30 | Request timeout in seconds |
| `verify_ssl` | bool | true | Verify SSL certificates |
| `headers` | object | {} | Custom HTTP headers |

#### Authentication Options

| auth_type | Required Fields | Description |
|-----------|----------------|-------------|
| `none` | None | No authentication |
| `basic` | `username`, `password` | HTTP Basic Auth |
| `bearer` | `token` | Bearer token auth |
| `api_key` | `api_key`, `api_key_header` | API key in header |

#### Publisher-Specific Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_retries` | int | 3 | Number of retry attempts |
| `retry_delay` | int | 1 | Delay between retries (seconds) |

#### Subscriber-Specific Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `query_params` | object | {} | Query string parameters |
| `response_data_key` | string | null | Key to extract data from response |
| `pagination_enabled` | bool | false | Enable pagination support |
| `pagination_key` | string | offset | Pagination parameter name |
| `last_id_key` | string | id | Field to track for incremental fetch |
| `initial_last_id` | any | null | Starting value for last_id tracking |

## Authentication Methods

### No Authentication

```json
{
    "base_url": "https://public-api.example.com",
    "auth_type": "none"
}
```

### Basic Authentication

```json
{
    "base_url": "https://api.example.com",
    "auth_type": "basic",
    "username": "your-username",
    "password": "your-password"
}
```

### Bearer Token

```json
{
    "base_url": "https://api.example.com",
    "auth_type": "bearer",
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

### API Key

```json
{
    "base_url": "https://api.example.com",
    "auth_type": "api_key",
    "api_key": "sk-1234567890abcdef",
    "api_key_header": "X-API-Key"
}
```

### Custom Headers

```json
{
    "base_url": "https://api.example.com",
    "headers": {
        "Authorization": "Custom auth-scheme token",
        "X-Custom-Auth": "custom-value",
        "X-Request-ID": "unique-id"
    }
}
```

## Response Handling

### Simple Response

```json
{
    "id": 123,
    "event": "user_signup",
    "data": {"user_id": 456}
}
```

Config:
```json
{
    "response_data_key": null
}
```

### Nested Response

```json
{
    "status": "success",
    "data": {
        "id": 123,
        "event": "user_signup"
    }
}
```

Config:
```json
{
    "response_data_key": "data"
}
```

### List Response

```json
{
    "results": [
        {"id": 1, "event": "event1"},
        {"id": 2, "event": "event2"}
    ]
}
```

Config (returns first item):
```json
{
    "response_data_key": "results"
}
```

### Pagination

```json
{
    "data": [{"id": 1}, {"id": 2}],
    "pagination": {
        "offset": 0,
        "limit": 10
    }
}
```

Config:
```json
{
    "response_data_key": "data",
    "pagination_enabled": true,
    "pagination_key": "offset"
}
```

## Common Patterns

### Pattern 1: Webhook Publishing

Send events to webhook endpoint:

```python
# Webhook config
config = {
    "base_url": "https://webhook.site",
    "publish_endpoint": "unique-webhook-id",
    "http_method": "POST",
    "auth_type": "none"
}

publisher = create_publisher('webhook', {
    'destination': 'rest://webhook',
    'rest_config_file': 'webhook_config.json'
})

# Send webhook events
publisher.publish({
    'event': 'order.created',
    'order_id': 'ORD-123',
    'total': 99.99,
    'timestamp': '2025-01-15T10:00:00Z'
})
```

### Pattern 2: API Polling with Incremental Fetch

Poll API for new records:

```python
# Config with incremental fetching
config = {
    "base_url": "https://api.example.com",
    "subscribe_endpoint": "orders",
    "http_method": "GET",
    "query_params": {
        "status": "pending",
        "limit": 1
    },
    "last_id_key": "id",
    "initial_last_id": 0
}

subscriber = create_subscriber('order_poller', {
    'source': 'rest://orders',
    'rest_config_file': 'api_config.json',
    'poll_interval': 10
})
subscriber.start()

# Automatically fetches only new orders
while True:
    order = subscriber.get_data(block_time=15)
    if order:
        process_order(order)
```

### Pattern 3: Microservice Communication

Send data between microservices:

```python
# Service A publishes
publisher = create_publisher('service_a', {
    'destination': 'rest://api/events',
    'rest_config_file': 'service_b_config.json'
})

publisher.publish({
    'service': 'service_a',
    'event': 'data_processed',
    'result': {'status': 'success'}
})

# Service B subscribes
subscriber = create_subscriber('service_b', {
    'source': 'rest://api/events',
    'rest_config_file': 'service_a_config.json',
    'poll_interval': 5
})
subscriber.start()
```

### Pattern 4: Cloud Service Integration

Integrate with cloud APIs:

```python
# AWS API Gateway example
config = {
    "base_url": "https://api-id.execute-api.region.amazonaws.com/prod",
    "publish_endpoint": "events",
    "http_method": "POST",
    "auth_type": "api_key",
    "api_key": "your-api-key",
    "api_key_header": "x-api-key"
}

publisher = create_publisher('aws_publisher', {
    'destination': 'rest://events',
    'rest_config_file': 'aws_config.json'
})
```

### Pattern 5: Third-Party API Integration

Integrate with external services:

```python
# Slack webhook example
config = {
    "base_url": "https://hooks.slack.com",
    "publish_endpoint": "services/T00/B00/XXX",
    "http_method": "POST",
    "auth_type": "none"
}

publisher = create_publisher('slack', {
    'destination': 'rest://slack',
    'rest_config_file': 'slack_config.json'
})

publisher.publish({
    'text': 'Deployment completed successfully',
    'channel': '#deployments'
})
```

## Monitoring

### Get Publisher Statistics

```python
stats = publisher.details()
print(f"Name: {stats['name']}")
print(f"Endpoint: {stats['destination']}")
print(f"Published: {stats['publish_count']} messages")
print(f"Last publish: {stats['last_publish']}")
```

### Get Subscriber Statistics

```python
stats = subscriber.details()
print(f"Name: {stats['name']}")
print(f"Endpoint: {stats['source']}")
print(f"Received: {stats['receive_count']} messages")
print(f"Last receive: {stats['last_receive']}")
print(f"Poll interval: {config['poll_interval']}s")
```

### HTTP Response Monitoring

```python
# Add logging to see HTTP details
import logging
logging.basicConfig(level=logging.DEBUG)

# Now you'll see detailed HTTP logs
# Including request/response details
```

## Troubleshooting

### Connection Refused

**Error**: `ConnectionError: Failed to establish connection`

**Solution**: Check base URL and network connectivity
```python
# Test connection manually
import requests
response = requests.get('https://api.example.com/health')
print(response.status_code)
```

### Authentication Failed

**Error**: `401 Unauthorized` or `403 Forbidden`

**Solution**: Verify authentication credentials
```json
{
    "auth_type": "bearer",
    "token": "correct-token-here"  // Check if token is valid
}
```

### SSL Certificate Error

**Error**: `SSLError: certificate verify failed`

**Solution**: Disable SSL verification (dev only) or add CA cert
```json
{
    "verify_ssl": false  // Only for development!
}
```

### Timeout Errors

**Error**: `ReadTimeout`

**Solution**: Increase timeout
```json
{
    "timeout": 60  // Increase from 30
}
```

### Empty Response

**Problem**: Subscriber gets no data

**Solution**: Check response format and data key
```python
# Test API manually
response = requests.get('https://api.example.com/events')
print(response.json())

# Update config with correct key
{
    "response_data_key": "data"  // or "results" or null
}
```

### Rate Limiting

**Error**: `429 Too Many Requests`

**Solution**: Increase poll interval
```python
config = {
    'source': 'rest://api/events',
    'poll_interval': 60  # Poll less frequently
}
```

## Best Practices

1. **Use HTTPS** - Always use secure connections
2. **Implement retries** - Handle transient failures
3. **Set timeouts** - Prevent hanging requests
4. **Validate SSL** - Don't disable in production
5. **Use appropriate auth** - Bearer tokens for APIs
6. **Monitor rate limits** - Respect API quotas
7. **Log responses** - Debug integration issues
8. **Handle errors gracefully** - Don't crash on API errors
9. **Use sessions** - Connection pooling for performance
10. **Close connections** - Always call `stop()`

## Performance Tips

1. **Session reuse**: Built-in connection pooling
2. **Batch publishing**: Use `publish_interval` and `batch_size`
3. **Adjust poll interval**: Balance freshness vs API load
4. **Connection timeout**: Set appropriate timeout values
5. **Compression**: Use gzip if API supports it
6. **Pagination**: Handle large result sets efficiently
7. **Incremental fetch**: Use last_id tracking
8. **Parallel requests**: Multiple publishers/subscribers
9. **Response caching**: Cache responses if appropriate
10. **Monitor API quotas**: Stay within limits

## Complete Examples

### Example 1: Simple Webhook Publishing

```python
from core.pubsub.pubsubfactory import create_publisher
import json
import time

print("=== Webhook Publishing Example ===\n")

# Create webhook config
webhook_config = {
    "base_url": "https://webhook.site",
    "publish_endpoint": "your-unique-webhook-id",
    "http_method": "POST",
    "auth_type": "none",
    "headers": {
        "Content-Type": "application/json"
    },
    "timeout": 10
}

with open('webhook_config.json', 'w') as f:
    json.dump(webhook_config, f, indent=2)

# Create publisher
publisher = create_publisher('webhook_pub', {
    'destination': 'rest://webhook',
    'rest_config_file': 'webhook_config.json'
})

print("Publishing events to webhook...\n")

# Publish events
events = [
    {
        'event_type': 'user.signup',
        'user_id': 123,
        'email': 'user1@example.com',
        'timestamp': '2025-01-15T10:00:00Z'
    },
    {
        'event_type': 'order.created',
        'order_id': 'ORD-001',
        'total': 99.99,
        'timestamp': '2025-01-15T10:05:00Z'
    },
    {
        'event_type': 'payment.received',
        'order_id': 'ORD-001',
        'amount': 99.99,
        'timestamp': '2025-01-15T10:06:00Z'
    }
]

for event in events:
    try:
        publisher.publish(event)
        print(f"✓ Published: {event['event_type']}")
        time.sleep(1)
    except Exception as e:
        print(f"✗ Failed: {event['event_type']} - {str(e)}")

publisher.stop()

print("\n✓ Check webhook.site to see received events")
print("✓ Example 1 completed!")
```

### Example 2: API Polling with Authentication

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import json
import time
import threading

print("=== API Polling Example ===\n")

# Simulate API server (you would use a real API)
from flask import Flask, jsonify, request
app = Flask(__name__)

messages = []
message_id = 1

@app.route('/api/messages', methods=['POST'])
def post_message():
    global message_id
    data = request.json
    data['id'] = message_id
    messages.append(data)
    message_id += 1
    return jsonify({'status': 'success', 'id': data['id']}), 201

@app.route('/api/messages', methods=['GET'])
def get_messages():
    # Get messages after a certain ID
    after_id = request.args.get('after_id', 0, type=int)
    filtered = [m for m in messages if m['id'] > after_id]
    
    if filtered:
        return jsonify({'data': filtered})
    return jsonify({'data': []}), 200

# Start API server in background
def run_server():
    app.run(port=5000, debug=False, use_reloader=False)

server_thread = threading.Thread(target=run_server, daemon=True)
server_thread.start()
time.sleep(2)  # Wait for server to start

print("API server started on http://localhost:5000\n")

# Publisher config
pub_config = {
    "base_url": "http://localhost:5000",
    "publish_endpoint": "api/messages",
    "http_method": "POST",
    "auth_type": "none",
    "timeout": 5
}

with open('api_pub_config.json', 'w') as f:
    json.dump(pub_config, f, indent=2)

# Subscriber config
sub_config = {
    "base_url": "http://localhost:5000",
    "subscribe_endpoint": "api/messages",
    "http_method": "GET",
    "auth_type": "none",
    "query_params": {},
    "response_data_key": "data",
    "last_id_key": "id",
    "initial_last_id": 0,
    "timeout": 5
}

with open('api_sub_config.json', 'w') as f:
    json.dump(sub_config, f, indent=2)

# Create publisher
publisher = create_publisher('api_pub', {
    'destination': 'rest://messages',
    'rest_config_file': 'api_pub_config.json'
})

# Create subscriber
subscriber = create_subscriber('api_sub', {
    'source': 'rest://messages',
    'rest_config_file': 'api_sub_config.json',
    'poll_interval': 2
})
subscriber.start()

print("Publishing messages...\n")

# Publish messages
messages_to_send = [
    {'content': 'Hello, World!', 'user': 'Alice'},
    {'content': 'How are you?', 'user': 'Bob'},
    {'content': 'Great, thanks!', 'user': 'Alice'}
]

for msg in messages_to_send:
    publisher.publish(msg)
    print(f"Published: {msg}")
    time.sleep(1)

print("\nPolling for messages...\n")

# Poll and receive messages
received_count = 0
for i in range(len(messages_to_send)):
    data = subscriber.get_data(block_time=5)
    if data:
        print(f"Received: {data}")
        received_count += 1

publisher.stop()
subscriber.stop()

print(f"\n✓ Published {len(messages_to_send)} messages")
print(f"✓ Received {received_count} messages")
print("✓ Example 2 completed!")
```

### Example 3: Microservice Event Bus

```python
from core.pubsub.pubsubfactory import create_publisher, create_subscriber
import json
import time
import threading

print("=== Microservice Event Bus Example ===\n")

# Simulate two microservices communicating via REST API

# Service A Publisher Config
service_a_config = {
    "base_url": "http://localhost:5001",
    "publish_endpoint": "events",
    "http_method": "POST",
    "auth_type": "bearer",
    "token": "service-a-token",
    "timeout": 10
}

# Service B Subscriber Config
service_b_config = {
    "base_url": "http://localhost:5001",
    "subscribe_endpoint": "events",
    "http_method": "GET",
    "auth_type": "bearer",
    "token": "service-b-token",
    "response_data_key": "events",
    "timeout": 10
}

# Note: This example shows configuration
# In real scenario, you'd have actual microservices running

print("Service A: Publishing events")
print("Service B: Consuming events")
print("\nEvent flow: Service A → REST API → Service B")

# Example event structure
event_examples = [
    {
        'service': 'user-service',
        'event': 'user.created',
        'user_id': 123,
        'email': 'newuser@example.com'
    },
    {
        'service': 'order-service',
        'event': 'order.placed',
        'order_id': 'ORD-456',
        'user_id': 123,
        'total': 99.99
    },
    {
        'service': 'payment-service',
        'event': 'payment.processed',
        'order_id': 'ORD-456',
        'amount': 99.99,
        'status': 'success'
    }
]

print("\nExample events that would flow through the system:")
for event in event_examples:
    print(f"  {event['service']}: {event['event']}")

print("\n✓ Example 3 configuration completed!")
print("  In production: Deploy actual microservices with these configs")
```

### Example 4: Cloud Service Integration

```python
from core.pubsub.pubsubfactory import create_publisher
import json

print("=== Cloud Service Integration Example ===\n")

# AWS API Gateway Configuration
aws_config = {
    "base_url": "https://your-api-id.execute-api.us-east-1.amazonaws.com/prod",
    "publish_endpoint": "events",
    "http_method": "POST",
    "auth_type": "api_key",
    "api_key": "your-aws-api-key",
    "api_key_header": "x-api-key",
    "headers": {
        "Content-Type": "application/json"
    },
    "timeout": 30,
    "verify_ssl": true,
    "max_retries": 3,
    "retry_delay": 2
}

print("AWS API Gateway Configuration:")
print(json.dumps(aws_config, indent=2))

# Azure Functions Configuration
azure_config = {
    "base_url": "https://your-function-app.azurewebsites.net/api",
    "publish_endpoint": "HttpTrigger",
    "http_method": "POST",
    "auth_type": "api_key",
    "api_key": "your-azure-function-key",
    "api_key_header": "x-functions-key",
    "timeout": 30
}

print("\nAzure Functions Configuration:")
print(json.dumps(azure_config, indent=2))

# Google Cloud Functions Configuration
gcp_config = {
    "base_url": "https://us-central1-your-project.cloudfunctions.net",
    "publish_endpoint": "functionName",
    "http_method": "POST",
    "auth_type": "bearer",
    "token": "your-gcp-id-token",
    "timeout": 30
}

print("\nGoogle Cloud Functions Configuration:")
print(json.dumps(gcp_config, indent=2))

print("\n✓ Example 4 completed!")
print("  Use these configurations to integrate with cloud services")
```

### Example 5: Error Handling and Retries

```python
from core.pubsub.pubsubfactory import create_publisher
import json
import time

print("=== Error Handling and Retries Example ===\n")

# Config with retry settings
retry_config = {
    "base_url": "https://unreliable-api.example.com",
    "publish_endpoint": "events",
    "http_method": "POST",
    "auth_type": "none",
    "timeout": 5,
    "max_retries": 3,
    "retry_delay": 2
}

with open('retry_config.json', 'w') as f:
    json.dump(retry_config, f, indent=2)

publisher = create_publisher('retry_pub', {
    'destination': 'rest://events',
    'rest_config_file': 'retry_config.json'
})

print("Testing publish with retries...\n")

# Simulate publishing with potential failures
test_data = {
    'test': 'data',
    'timestamp': time.time()
}

try:
    print("Attempting to publish...")
    print(f"  Max retries: {retry_config['max_retries']}")
    print(f"  Retry delay: {retry_config['retry_delay']}s")
    print(f"  Timeout: {retry_config['timeout']}s\n")
    
    publisher.publish(test_data)
    print("✓ Published successfully")
    
except Exception as e:
    print(f"✗ Failed after retries: {str(e)}")
    print("\nThis is expected behavior when API is unavailable")
    print("The publisher automatically retried 3 times with 2s delay")

publisher.stop()

print("\n✓ Example 5 completed!")
print("  Demonstrates automatic retry mechanism")
```

## Comparison with Other Implementations

| Feature | REST | SQL | File | ActiveMQ | Kafka | RabbitMQ |
|---------|------|-----|------|----------|-------|----------|
| Setup | API required | Database | None | Broker | Broker | Broker |
| Dependencies | requests | pymysql/psycopg2 | None | stomp.py | kafka-python | pika |
| Latency | High (HTTP + polling) | High (polling) | Medium | Low | Low | Low |
| Throughput | Low-Medium | Low-Medium | Low-Medium | Medium | Very High | Medium-High |
| Best for | API integration | DB workflows | Logs | Traditional MQ | Event streaming | Traditional MQ |
| Authentication | Various | DB auth | None | STOMP | SASL | Auth plugins |
| External service | ✅ Yes | ✅ Yes | ❌ No | ✅ Yes | ✅ Yes | ✅ Yes |

## When to Use REST Pub/Sub

**Use REST pub/sub when:**
- Integrating with REST APIs
- Webhook-style publishing
- Microservices communication
- Cloud service integration
- Third-party API interaction
- No message broker available
- HTTP-based architecture

**Use other implementations when:**
- Need low latency (use message brokers)
- High throughput required (use Kafka)
- Real-time messaging (use WebSockets/brokers)
- Large message volumes (use dedicated brokers)
- Complexity of REST overhead not justified

## Key Advantages

1. **API Integration**: Connect to any REST API
2. **Flexible Auth**: Support multiple authentication methods
3. **Standard Protocol**: HTTP is universal
4. **Cloud Ready**: Works with cloud services
5. **Simple**: No message broker to manage
6. **Webhooks**: Easy webhook publishing
7. **Debugging**: Use standard HTTP tools
8. **Portable**: Works anywhere with HTTP

## Key Limitations

1. **Latency**: HTTP overhead + polling delay
2. **Throughput**: Lower than dedicated brokers
3. **Polling**: Schedule-based, not real-time
4. **HTTP Overhead**: Each message is HTTP request
5. **Rate Limits**: Subject to API rate limiting
6. **No Push**: Subscriber must poll (no push model)

The REST implementation provides a flexible foundation for API-based messaging, ideal for integrating with external services and HTTP-based architectures!


## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.
