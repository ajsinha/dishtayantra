# InMemoryRedis Quick Reference Card

# © 2025-2030 Ashutosh Sinha

A one-page reference for the most common operations.

## Import

```python
from inmemory_redisclone import InMemoryRedisClone
from inmemoryredis_datapubsub import (
    InMemoryRedisDataPublisher,
    InMemoryRedisDataSubscriber,
    InMemoryRedisChannelDataPublisher,
    InMemoryRedisChannelDataSubscriber
)
```

## Basic Redis Operations

```python
redis = InMemoryRedisClone()

# Strings
redis.set("key", "value")
redis.set("key", "value", ex=60)  # With 60s expiration
value = redis.get("key")
redis.incr("counter")
redis.append("text", " more")

# Lists
redis.rpush("queue", "item1", "item2")
redis.lpush("stack", "top")
item = redis.rpop("queue")
items = redis.lrange("queue", 0, -1)

# Sets
redis.sadd("tags", "python", "redis")
members = redis.smembers("tags")
common = redis.sinter("set1", "set2")

# Sorted Sets
redis.zadd("scores", {"Alice": 100, "Bob": 90})
top = redis.zrange("scores", 0, 2, withscores=True)

# Hashes
redis.hset("user:1", "name", "John")
redis.hmset("user:2", {"name": "Jane", "age": "25"})
data = redis.hgetall("user:1")

# Expiration
redis.expire("key", 300)
ttl = redis.ttl("key")

# Keys
redis.delete("key1", "key2")
exists = redis.exists("key1")
keys = redis.keys("user:*")

# Transactions
with redis.pipeline() as pipe:
    pipe.multi()
    pipe.incr("counter")
    pipe.set("flag", "done")
    results = pipe.execute()

# Pub/Sub
def handler(channel, message):
    print(message)

redis.subscribe("news", handler)
redis.publish("news", "Hello!")
```

## Key-Based Messaging

```python
redis = InMemoryRedisClone()

# Publisher
pub = InMemoryRedisDataPublisher(
    name="pub",
    destination="inmemoryredis://",
    config={
        'redis_instance': redis,
        'key_prefix': 'tasks:',
        'ttl_seconds': 600
    }
)

# Subscriber
sub = InMemoryRedisDataSubscriber(
    name="sub",
    source="inmemoryredis://",
    config={
        'redis_instance': redis,
        'key_pattern': 'tasks:*',
        'delete_on_read': True
    }
)

# Publish
pub.publish({
    '__dagserver_key': 'task_001',
    'action': 'process',
    'data': {...}
})

# Subscribe
data = sub._do_subscribe()
# Returns: {'__dagserver_key': 'tasks:task_001', 'action': 'process', ...}
```

## Channel-Based Messaging

```python
redis = InMemoryRedisClone()

# Publisher
pub = InMemoryRedisChannelDataPublisher(
    name="events",
    destination="inmemoryredischannel://notifications",
    config={'redis_instance': redis}
)

# Subscriber
sub = InMemoryRedisChannelDataSubscriber(
    name="handler",
    source="inmemoryredischannel://notifications",
    config={'redis_instance': redis}
)

# Publish
pub.publish({'event': 'alert', 'message': 'System update'})

# Subscribe
data = sub._do_subscribe()
# Returns: {'event': 'alert', 'message': 'System update'}
```

## Common Patterns

### Task Queue
```python
# Producer
producer.publish({
    '__dagserver_key': f'task_{id}',
    'job': 'send_email',
    'to': 'user@example.com'
})

# Worker
task = worker._do_subscribe()
if task:
    process(task)
```

### Event Broadcasting
```python
# One publisher, multiple subscribers
event_pub.publish({'event': 'user_login', 'user_id': 123})

# All receive
logger._do_subscribe()
analytics._do_subscribe()
notifier._do_subscribe()
```

### Caching
```python
# Write
cache.publish({
    '__dagserver_key': 'user_123',
    '__ttl_seconds': 300,
    'data': {...}
})

# Read
data = cache._do_subscribe()
```

### Request-Response
```python
# Request
client.publish({
    '__dagserver_key': 'req_001',
    'method': 'GET',
    'path': '/users'
})

# Response
server.publish({
    '__dagserver_key': 'req_001',
    'status': 200,
    'body': [...]
})
```

## Configuration Reference

### InMemoryRedisDataPublisher Config
```python
config = {
    'redis_instance': redis,      # Required
    'key_prefix': 'app:',         # Optional
    'ttl_seconds': 600            # Optional
}
```

### InMemoryRedisDataSubscriber Config
```python
config = {
    'redis_instance': redis,      # Required
    'key_pattern': 'app:*',       # Optional
    'key_prefix': 'app:',         # Optional
    'delete_on_read': True,       # Optional
    'poll_interval': 0.1          # Optional
}
```

### Channel Publisher/Subscriber Config
```python
config = {
    'redis_instance': redis,      # Required
    'channel': 'events'           # Optional (from URL)
}
```

## Data Format

### Publishing
```python
# With key
{
    '__dagserver_key': 'unique_id',  # Required for key-based
    '__ttl_seconds': 300,            # Optional
    'your_field': 'your_value',
    ...
}

# Channel
{
    'any_field': 'any_value',
    ...
}
```

### Subscribing
```python
# Key-based returns
{
    '__dagserver_key': 'prefix:unique_id',
    'your_field': 'your_value',
    ...
}

# Channel returns
{
    'any_field': 'any_value',
    ...
}
```

## Statistics

```python
# Publisher
stats = publisher.details()
# {'name': '...', 'publish_count': 10, 'last_publish': '...'}

# Subscriber
stats = subscriber.details()
# {'name': '...', 'receive_count': 5, 'last_receive': '...'}
```

## Best Practices

1. **Share Redis Instance**: Use same instance for communication
2. **Use Prefixes**: Organize with `key_prefix`
3. **Set TTLs**: Prevent memory leaks
4. **Delete on Read**: For queue patterns
5. **Pattern Match**: Use wildcards for flexible matching
6. **Channel for Events**: Real-time notifications
7. **Keys for Tasks**: Persistent work queues

## Common Commands

| Command | Usage |
|---------|-------|
| `set` | `redis.set("key", "value")` |
| `get` | `redis.get("key")` |
| `incr` | `redis.incr("counter")` |
| `lpush` | `redis.lpush("list", "item")` |
| `rpush` | `redis.rpush("list", "item")` |
| `lpop` | `redis.lpop("list")` |
| `rpop` | `redis.rpop("list")` |
| `sadd` | `redis.sadd("set", "member")` |
| `smembers` | `redis.smembers("set")` |
| `zadd` | `redis.zadd("zset", {"m": 1.0})` |
| `zrange` | `redis.zrange("zset", 0, -1)` |
| `hset` | `redis.hset("hash", "f", "v")` |
| `hgetall` | `redis.hgetall("hash")` |
| `expire` | `redis.expire("key", 60)` |
| `ttl` | `redis.ttl("key")` |
| `delete` | `redis.delete("key")` |
| `keys` | `redis.keys("pattern")` |
| `publish` | `redis.publish("ch", "msg")` |
| `subscribe` | `redis.subscribe("ch", cb)` |

## Testing Commands

```bash
# Test Redis core
python test_redis_clone.py

# Test pub/sub
python test_inmemoryredis_datapubsub.py

# Run examples
python comprehensive_example.py
```

## Files

- `inmemory_redisclone.py` - Core Redis implementation
- `inmemoryredis_datapubsub.py` - Pub/Sub integration
- `test_redis_clone.py` - Redis tests
- `test_inmemoryredis_datapubsub.py` - Pub/Sub tests
- `comprehensive_example.py` - Usage examples
- `README.md` - Redis documentation
- `INMEMORYREDIS_DATAPUBSUB_README.md` - Pub/Sub guide
- `SUITE_SUMMARY.md` - Complete overview

---



**Quick Start**: `redis = InMemoryRedisClone()` → `redis.set("key", "value")` → Done!🚀


## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.