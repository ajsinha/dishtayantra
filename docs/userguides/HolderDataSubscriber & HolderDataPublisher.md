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