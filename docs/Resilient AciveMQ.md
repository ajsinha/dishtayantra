I'll create a `ResilientActiveMQConnection` class that extends `stomp.Connection` with automatic reconnection and message buffering capabilities for ActiveMQ.I've created a comprehensive `ResilientActiveMQConnection` class that extends `stomp.Connection` with automatic reconnection and message buffering capabilities. Here are the key features:

## Key Features:

1. **Automatic Reconnection**: Handles connection failures during any operation with configurable retry logic
2. **Message Buffering**: Prevents message loss by buffering messages during connection outages
3. **Subscription Management**: Automatically restores subscriptions after reconnection
4. **Drop-in Replacement**: Fully compatible with existing `stomp.Connection` code

## Configuration Parameters:

- `reconnect_tries`: Maximum reconnection attempts (default: 10)
- `reconnect_interval_seconds`: Sleep between retries (default: 60 seconds)
- `buffer_max_messages`: Maximum messages to buffer during outages (default: 10,000)

## Enhanced Methods:

1. **`connect()`**: Connects with automatic retry logic and stores credentials for reconnection
2. **`send()`**: Sends messages with automatic buffering during connection failures
3. **`subscribe()`**: Subscribes with auto-restoration after reconnection
4. **`ack()`/`nack()`**: Message acknowledgment with retry logic
5. **`send_batch()`**: Convenience method for sending multiple messages

## Key Safety Features:

1. **Automatic Message Buffering**: Messages are queued when connection is lost
2. **Background Processing**: Dedicated thread processes buffered messages once reconnected
3. **Subscription Restoration**: All active subscriptions are automatically restored
4. **Connection Monitoring**: Internal listener tracks connection state and triggers reconnection
5. **Thread-Safe Operations**: Uses locks to prevent race conditions

## Usage Examples:

```python
# Simple drop-in replacement
conn = ResilientActiveMQConnection(
    host_and_ports=[('localhost', 61613)],
    reconnect_tries=5,
    reconnect_interval_seconds=30
)

conn.connect('admin', 'admin', wait=True)
conn.subscribe('/queue/my.queue', id='1')

# Send messages - automatically buffered during outages
for i in range(1000):
    conn.send('/queue/my.queue', f'Message {i}')
    # No messages lost even if ActiveMQ goes down!

# Monitor buffer
if conn.get_buffer_size() > 0:
    print(f"Buffered messages: {conn.get_buffer_size()}")

conn.disconnect()
```

The implementation ensures your ActiveMQ client remains resilient to broker outages, network issues, and connection failures while guaranteeing that messages are not lost during reconnection attempts. The automatic subscription restoration ensures that your message consumers continue working seamlessly after reconnection.


## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.