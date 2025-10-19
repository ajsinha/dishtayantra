The `ResilientRedisClient` class extends `redis.Redis` with automatic reconnection and command buffering capabilities.It is a class that extends `redis.Redis` with automatic reconnection and command buffering capabilities. Here are the key features:

## Key Features:

1. **Drop-in Replacement**: Fully compatible with `redis.Redis`, all existing code works without modification
2. **Automatic Reconnection**: Handles connection failures with configurable retry logic
3. **Command Buffering**: Optional buffering of commands during connection outages to prevent data loss
4. **Resilient Pipeline**: Custom pipeline implementation with retry logic for batch operations
5. **Pub/Sub Support**: Automatic restoration of subscriptions after reconnection

## Configuration Parameters:

- `reconnect_tries`: Maximum reconnection attempts (default: 10)
- `reconnect_interval_seconds`: Sleep between retries (default: 60 seconds)
- `buffer_commands`: Enable/disable command buffering (default: True)
- `buffer_max_commands`: Maximum commands to buffer (default: 10,000)

## Key Components:

### Enhanced Features:

1. **Command Buffering**:
   - Optional feature to prevent data loss
   - Background thread processes buffered commands when reconnected
   - Configurable buffer size with overflow handling

2. **Resilient Pipeline**:
   - Batch operations with automatic retry
   - Transaction support maintained
   - Command stack preserved across reconnection attempts

3. **Pub/Sub Resilience**:
   - Tracks active subscriptions
   - Automatically restores channel and pattern subscriptions
   - Maintains callback associations

4. **Connection Management**:
   - Automatic connection health checks
   - Graceful connection pool management
   - Thread-safe reconnection logic

## Usage Examples:

```python
# Simple drop-in replacement
client = ResilientRedisClient(
    host='localhost',
    port=6379,
    reconnect_tries=5,
    reconnect_interval_seconds=30,
    buffer_commands=True  # Enable command buffering
)

# All Redis commands work as normal with added resilience
client.set('key', 'value')
value = client.get('key')

# Pipeline operations with automatic retry
with client.pipeline() as pipe:
    pipe.set('key1', 'value1')
    pipe.set('key2', 'value2')
    pipe.incr('counter')
    results = pipe.execute()  # Automatically retried on failure

# Commands are buffered during outages
for i in range(1000):
    client.set(f'key{i}', f'value{i}')
    # No data lost even if Redis goes down!

# Monitor buffer status
if client.get_buffer_size() > 0:
    print(f"Buffered commands: {client.get_buffer_size()}")

client.close()
```

## Key Benefits:

1. **Zero Data Loss Option**: Command buffering ensures operations aren't lost during outages
2. **Seamless Recovery**: Automatic reconnection without application restart
3. **Production Ready**: Thread-safe with comprehensive error handling
4. **Monitoring**: Built-in methods to check buffer size and failed commands
5. **Full Compatibility**: Works with all Redis data types and operations
6. **Transaction Support**: WATCH/MULTI/EXEC operations preserved
7. **Pub/Sub Resilience**: Subscriptions automatically restored

## Special Features:

- **Configurable Buffering**: Can be disabled for read-heavy workloads where buffering isn't needed
- **Pipeline Resilience**: Batch operations automatically retried as a unit
- **Method Interception**: All Redis methods automatically gain retry capabilities
- **Connection Health Checks**: Proactive connection validation before operations

The implementation ensures your Redis applications remain resilient to server restarts, network issues, and temporary outages while maintaining full compatibility with the standard `redis.Redis` client.


## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.