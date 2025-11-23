# Message Router Components - Complete Guide

## Overview

The Message Router system provides **intelligent routing capabilities** for both incoming (subscriber) and outgoing (publisher) messages in the Abhikarta LLM Platform's pub/sub infrastructure.

### Components

1. **MessageRouterDataSubscriber** - Routes incoming messages from one source to multiple child subscribers (Fan-Out for Subscribers)
2. **MessageRouterDataPublisher** - Routes outgoing messages to multiple child publishers based on strategy (Fan-Out for Publishers)

Both components use **pluggable resolver strategies** to determine routing destinations, providing flexibility for various routing patterns.

## Component Comparison

### MessageRouterDataSubscriber vs MessageRouterDataPublisher

| Feature | MessageRouterDataSubscriber | MessageRouterDataPublisher |
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
External Source → MessageRouterDataSubscriber → Processing → MessageRouterDataPublisher → Multiple Destinations

[Source]
   ↓
[Router Subscriber]
   ├→ [Processor A] → [Router Publisher] → [DB]
   ├→ [Processor B] → [Router Publisher] → [File]
   └→ [Processor C] → [Router Publisher] → [Queue]
```

### Pattern 2: Subscriber-Only Routing

```
Single Source → MessageRouterDataSubscriber → Multiple Specialized Processors

[Kafka Topic]
      ↓
[Router Subscriber]
      ├→ [Order Processor]
      ├→ [Log Processor]
      └→ [Notification Processor]
```

### Pattern 3: Publisher-Only Routing

```
Application → MessageRouterDataPublisher → Multiple Destinations

[Your App]
     ↓
[Router Publisher]
     ├→ [Primary DB]
     ├→ [Backup DB]
     └→ [Analytics File]
```

## Quick Start Examples

### MessageRouterDataSubscriber Example

```python
from core.pubsub.message_router_datapubsub import MessageRouterDataSubscriber
from core.pubsub.pubsubfactory import create_subscriber

# Create source and child subscribers
source = create_subscriber('source', {'source': 'mem://queue/incoming'})
order_sub = create_subscriber('orders', {'source': 'mem://queue/orders'})
log_sub = create_subscriber('logs', {'source': 'mem://queue/logs'})

# Configure router
config = {
    'child_subscribers': {'order': order_sub, 'log': log_sub},
    'resolver_class': 'core.pubsub.example_resolvers.FieldBasedResolver'
}

# Create and start
router = MessageRouterDataSubscriber('router', source, config)
router.start()

# Messages with {'type': 'order'} → order_sub
# Messages with {'type': 'log'} → log_sub
```

### MessageRouterDataPublisher Example

```python
from core.pubsub.message_router_datapubsub import MessageRouterDataPublisher
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
    'resolver_class': 'core.pubsub.example_resolvers.FieldBasedResolver'
}

# Create router
router = MessageRouterDataPublisher('order_router', 'router://order_router', config)

# Publish - automatically routes to appropriate destination
router.publish({'type': 'database', 'order_id': 123, 'amount': 99.99})
```

## Configuration

### MessageRouterDataSubscriber Configuration

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

### MessageRouterDataPublisher Configuration

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
subscriber_router = MessageRouterDataSubscriber(
    'input_router',
    source_subscriber,
    {
        'child_subscribers': {
            'order': order_processor_sub,
            'log': log_processor_sub,
            'notification': notification_processor_sub
        },
        'resolver_class': 'core.pubsub.example_resolvers.FieldBasedResolver'
    }
)

# Publisher side - route processed results to destinations
publisher_router = MessageRouterDataPublisher(
    'output_router',
    'router://output',
    {
        'child_publishers': {
            'order': database_publisher,
            'log': file_publisher,
            'notification': queue_publisher
        },
        'resolver_class': 'core.pubsub.example_resolvers.FieldBasedResolver'
    }
)
```

### Use Case 2: Priority-Based Processing

**Scenario**: High-priority messages get fast processing, low-priority get batch processing

```python
# Subscriber side - route to priority queues
subscriber_router = MessageRouterDataSubscriber(
    'priority_input',
    source_subscriber,
    {
        'child_subscribers': {
            'high': high_priority_processor,
            'medium': medium_priority_processor,
            'low': batch_processor
        },
        'resolver_class': 'core.pubsub.example_resolvers.PriorityBasedResolver'
    }
)

# Publisher side - high priority to memory, low priority to database
publisher_router = MessageRouterDataPublisher(
    'priority_output',
    'router://priority',
    {
        'child_publishers': {
            'high': memory_queue_publisher,
            'medium': database_publisher,
            'low': archive_file_publisher
        },
        'resolver_class': 'core.pubsub.example_resolvers.PriorityBasedResolver'
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
publisher_router = MessageRouterDataPublisher(
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
subscriber_router = MessageRouterDataSubscriber(
    'tenant_input',
    source_subscriber,
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
publisher_router = MessageRouterDataPublisher(
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
subscriber_router = MessageRouterDataSubscriber(
    'worker_router',
    source_subscriber,
    {
        'child_subscribers': workers,
        'resolver_class': 'core.pubsub.example_resolvers.ModuloHashResolver'
    }
)

# Publisher side - load balance across API endpoints
endpoints = {f'partition_{i}': endpoint_pubs[i] for i in range(4)}
publisher_router = MessageRouterDataPublisher(
    'api_router',
    'router://api',
    {
        'child_publishers': endpoints,
        'resolver_class': 'core.pubsub.example_resolvers.ModuloHashResolver'
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

### MessageRouterDataSubscriber Specific Methods

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

### MessageRouterDataPublisher Specific Methods

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
logging.getLogger('core.pubsub.message_router_datapubsub').setLevel(logging.DEBUG)

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
| MessageRouterDataSubscriber | 1,000-10,000 msg/sec | <1ms |
| MessageRouterDataPublisher | 5,000-50,000 msg/sec | <0.5ms |

*Note: Performance depends on resolver complexity and child count*

### Resource Usage

**MessageRouterDataSubscriber**:
- Memory: ~(queue_depth × msg_size) × (1 + N children)
- Threads: 2 + N (router + source + N children)
- CPU: Minimal (mostly queue operations)

**MessageRouterDataPublisher**:
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

## Integration with Abhikarta

### LangGraph Workflow Routing

```python
# Route different workflow types to specialized executors
workflow_sub_router = MessageRouterDataSubscriber(
    'workflow_input',
    workflow_source,
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
workflow_pub_router = MessageRouterDataPublisher(
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

llm_pub_router = MessageRouterDataPublisher(
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
from core.pubsub.message_router_datapubsub import (
    MessageRouterDataSubscriber,
    MessageRouterDataPublisher
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

order_input_router = MessageRouterDataSubscriber(
    'order_input',
    order_source,
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
order_output_router = MessageRouterDataPublisher(
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

- **MessageRouterDataSubscriber**: Route incoming data to appropriate processors
- **MessageRouterDataPublisher**: Route outgoing data to appropriate destinations

Both components share the same resolver pattern, making them easy to learn and use together in complete data pipelines.

---

For detailed implementation examples, see:
- `message_router_examples.py` (Subscriber examples)
- `message_router_publisher_examples.py` (Publisher examples)
- `MESSAGE_ROUTER_README.md` (Original subscriber documentation)