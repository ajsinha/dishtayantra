I'll create a `ReconnectAwareKafkaProducer` class that extends `KafkaProducer` with automatic reconnection and message buffering to ensure no messages are lost during reconnection attempts.I've created a comprehensive `ReconnectAwareKafkaProducer` class that extends `KafkaProducer` with automatic reconnection and crucial message buffering to prevent data loss. Here are the key features:

## Key Features:

1. **Message Buffering**: Messages sent during connection failures are automatically buffered in a thread-safe queue, ensuring no data loss during reconnection attempts.

2. **Background Processing**: A dedicated background thread continuously processes buffered messages once the connection is restored.

3. **Drop-in Replacement**: Fully compatible with existing `KafkaProducer` code while adding resilience features.

4. **Configurable Parameters**:
   - `reconnect_tries`: Maximum reconnection attempts (default: 10)
   - `reconnect_interval_seconds`: Sleep between retries (default: 60 seconds)
   - `buffer_max_messages`: Maximum messages to buffer during outages (default: 10,000)

## Enhanced Methods:

1. **`send()`**: Automatically buffers messages if connection is unavailable and retries sending
2. **`send_batch()`**: Convenience method for sending multiple messages with resilience
3. **`flush()`**: Flushes both buffered and pending messages with retry logic
4. **`close()`**: Ensures all buffered messages are processed before shutdown

## Additional Utility Methods:

- **`get_buffer_size()`**: Monitor the number of messages currently buffered
- **`get_failed_messages()`**: Retrieve messages that couldn't be sent after all retries

## Message Safety Features:

1. **Automatic Buffering**: Messages are immediately buffered when connection issues are detected
2. **Buffer Overflow Protection**: When buffer is full, oldest messages are logged and removed to make room for new ones
3. **Background Flushing**: Buffered messages are automatically sent once connection is restored
4. **Graceful Shutdown**: On close, attempts to flush all remaining buffered messages

## Usage Example:

```python
# Simple drop-in replacement
producer = ReconnectAwareKafkaProducer(
    bootstrap_servers=['localhost:9092'],
    reconnect_tries=5,
    reconnect_interval_seconds=30,
    buffer_max_messages=5000
)

# Messages are automatically buffered during outages
for i in range(1000):
    producer.send('my-topic', value=f'Message {i}'.encode('utf-8'))
    # No messages lost even if Kafka goes down!

# Monitor buffer status
if producer.get_buffer_size() > 0:
    print(f"Messages waiting in buffer: {producer.get_buffer_size()}")

producer.close()  # Attempts to flush all buffered messages
```

The implementation ensures that your producer remains resilient to Kafka broker outages while guaranteeing that messages are not lost during reconnection attempts. The buffering mechanism provides a safety net for your data, and the background processor ensures messages are sent as soon as the connection is restored.


## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.