I'll create a `ResilientWebSphereMQQueueManager` class that extends `pymqi.QueueManager` with automatic reconnection and message buffering capabilities for IBM WebSphere MQ (now IBM MQ).I've created a comprehensive `ResilientWebSphereMQQueueManager` class that extends `pymqi.QueueManager` with automatic reconnection and message buffering capabilities for IBM MQ (WebSphere MQ). Here are the key features:

## Key Features:

1. **Drop-in Replacement**: Fully extends `pymqi.QueueManager` maintaining compatibility with existing code
2. **Automatic Reconnection**: Handles connection failures with configurable retry logic
3. **Message Buffering**: Prevents message loss by buffering during connection outages
4. **Queue State Management**: Automatically restores open queues after reconnection
5. **Full MQ Feature Support**: Transactions, PCF commands, pub/sub, SSL/TLS connections

## Configuration Parameters:

- `queue_manager_name`: Name of the queue manager
- `channel`: Channel name for client connection
- `connection_info`: Connection string (host(port))
- `reconnect_tries`: Maximum reconnection attempts (default: 10)
- `reconnect_interval_seconds`: Sleep between retries (default: 60 seconds)
- `buffer_max_messages`: Maximum messages to buffer (default: 10,000)
- `use_ssl`: Enable SSL/TLS connection
- `ssl_cipher_spec`: SSL cipher specification

## Key Components:

### ResilientQueue
A wrapper around `pymqi.Queue` that:
- Automatically reopens queues after reconnection
- Provides retry logic for all queue operations
- Maintains queue state across reconnections
- Supports browsing, inquire, and set operations

### Enhanced Features:

1. **Connection Types**:
   - Client connections with channel/connection info
   - Local connections to queue manager
   - SSL/TLS secure connections
   - Authentication with username/password

2. **Message Operations**:
   - `put()` and `put1()` with automatic buffering
   - `get()` with retry logic
   - Browse messages without removing
   - Batch operations with transactions

3. **Transaction Support**:
   - `begin()`, `commit()`, `backout()` with retry logic
   - Transactional batch processing
   - Automatic rollback on failures

4. **Advanced Features**:
   - PCF command execution with retry
   - Topic subscription support
   - Request-reply patterns with correlation IDs
   - Message descriptors and put/get options
   - Queue attributes inquiry and modification

## Usage Examples:

```python
# Simple drop-in replacement for client connection
qmgr = ResilientWebSphereMQQueueManager(
    queue_manager_name='QM1',
    channel='SYSTEM.DEF.SVRCONN',
    connection_info='localhost(1414)',
    user='mquser',
    password='mqpass',
    reconnect_tries=5,
    reconnect_interval_seconds=30
)

# Open a queue with resilience
queue = qmgr.open_queue('MY.QUEUE', MQOO_OUTPUT | MQOO_INPUT_AS_Q_DEF)

# Send messages - automatically buffered during outages
for i in range(1000):
    md = pymqi.MD()
    md.Persistence = MQPER_PERSISTENT
    md.Priority = 5
    
    queue.put(f'Message {i}', md)
    # No messages lost even if MQ goes down!

# Receive messages with automatic reconnection
gmo = pymqi.GMO()
gmo.Options = MQGMO_WAIT | MQGMO_FAIL_IF_QUIESCING
gmo.WaitInterval = 5000

message = queue.get(None, None, gmo)

# Transaction support
qmgr.begin()
queue.put('Message 1')
queue.put('Message 2')
qmgr.commit()  # Atomic commit with retry

qmgr.disconnect()
```

## Key Benefits:

1. **Zero Message Loss**: Buffering ensures messages aren't lost during outages
2. **Seamless Recovery**: Automatic restoration of queue connections
3. **Production Ready**: Thread-safe with comprehensive error handling
4. **Full IBM MQ Support**: All MQ features including transactions, PCF, SSL
5. **Monitoring**: Built-in methods to check buffer size and failed messages
6. **Compatibility**: Works as drop-in replacement for `pymqi.QueueManager`

## Special Features:

- **SSL/TLS Support**: Secure connections with cipher specs and key repositories
- **PCF Commands**: Administrative operations with retry logic
- **Batch Processing**: Transactional batch message sending
- **Request-Reply**: Correlation ID based messaging patterns
- **Queue Browsing**: Non-destructive message reading
- **Dynamic Queues**: Support for temporary and model queues
- **Message Properties**: Full support for all MQ message descriptors

The implementation ensures your IBM MQ applications remain resilient to queue manager restarts, network issues, and temporary outages while maintaining full compatibility with the standard `pymqi` library.



## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.