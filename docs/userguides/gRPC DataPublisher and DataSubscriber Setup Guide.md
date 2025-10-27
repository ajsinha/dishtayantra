# gRPC DataPublisher and DataSubscriber Setup Guide

## © 2025-2030 Ashutosh Sinha

## Overview

The gRPC implementation provides publisher and subscriber classes that communicate via gRPC protocol, enabling efficient, bidirectional streaming communication for your pub/sub system.

## Files

1. **grpc_datapubsub.py** - Contains `GRPCDataPublisher` and `GRPCDataSubscriber` classes
2. **pubsub.proto** - Protocol buffer definition for the gRPC service
3. **Updated pubsubfactory.py** - Factory methods now support `grpc://` destinations/sources

## Prerequisites

Install the required packages:

```bash
pip install grpcio grpcio-tools
```

## Generating gRPC Stubs

Before using the gRPC publisher/subscriber, you need to generate Python code from the proto file:

```bash
# Navigate to your project root
cd /path/to/your/project

# Generate the gRPC stubs
python -m grpc_tools.protoc \
    -I. \
    --python_out=. \
    --grpc_python_out=. \
    core/pubsub/pubsub.proto
```

This will create two files in `core/pubsub/grpc_generated/`:
- `pubsub_pb2.py` - Message definitions
- `pubsub_pb2_grpc.py` - Service definitions

**Note:** You may need to adjust the import paths in the generated files and create the `grpc_generated` directory.

## Usage

### Creating a Publisher

```python
from core.pubsub.pubsubfactory import create_publisher

config = {
    'destination': 'grpc://localhost:50051/my_topic',
    'host_port': 'localhost:50051',  # Optional, extracted from destination
    'use_ssl': False,                 # Set to True for SSL/TLS
    'timeout': 30,                    # Request timeout in seconds
    'max_retries': 3,                 # Number of retry attempts
    'publish_interval': 0,            # For batching (from base class)
    'batch_size': None                # For batching (from base class)
}

publisher = create_publisher('my_publisher', config)

# Publish data
publisher.publish({'key': 'value', 'data': [1, 2, 3]})

# Stop when done
publisher.stop()
```

### Creating a Subscriber

```python
from core.pubsub.pubsubfactory import create_subscriber

config = {
    'source': 'grpc://localhost:50051/my_topic',
    'host_port': 'localhost:50051',  # Optional, extracted from source
    'use_ssl': False,                 # Set to True for SSL/TLS
    'subscriber_id': 'unique_sub_1',  # Unique identifier for this subscriber
    'reconnect_delay': 5,             # Delay before reconnecting on failure
    'max_depth': 100000               # Internal queue size (from base class)
}

subscriber = create_subscriber('my_subscriber', config)

# Start the subscriber
subscriber.start()

# Get data (non-blocking)
data = subscriber.get_data()

# Get data (blocking with timeout)
data = subscriber.get_data(block_time=5)

# Get data (blocking indefinitely)
data = subscriber.get_data(block_time=-1)

# Stop when done
subscriber.stop()
```

### Destination/Source Format

The gRPC destination/source follows this format:
```
grpc://host:port/topic
```

Examples:
- `grpc://localhost:50051/events`
- `grpc://192.168.1.100:8080/sensor_data`
- `grpc://grpc-server.example.com:443/notifications`

## Configuration Options

### Publisher Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `destination` | string | Required | gRPC destination URL |
| `host_port` | string | From URL | gRPC server address |
| `use_ssl` | bool | `False` | Enable SSL/TLS encryption |
| `timeout` | int | `30` | Request timeout in seconds |
| `max_retries` | int | `3` | Number of retry attempts on failure |
| `publish_interval` | int | `0` | Batch publishing interval (inherited) |
| `batch_size` | int | `None` | Batch size threshold (inherited) |

### Subscriber Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source` | string | Required | gRPC source URL |
| `host_port` | string | From URL | gRPC server address |
| `use_ssl` | bool | `False` | Enable SSL/TLS encryption |
| `subscriber_id` | string | `{name}_sub` | Unique subscriber identifier |
| `reconnect_delay` | int | `5` | Seconds to wait before reconnecting |
| `max_depth` | int | `100000` | Internal queue maximum size (inherited) |

## SSL/TLS Configuration

For secure connections, set `use_ssl: True` and ensure your gRPC server has proper SSL certificates configured.

```python
config = {
    'destination': 'grpc://secure-server.example.com:443/my_topic',
    'use_ssl': True,
    'timeout': 30
}
```

## Implementing a gRPC Server

You'll need to implement a gRPC server that matches the `pubsub.proto` specification. Here's a basic example:

```python
import grpc
from concurrent import futures
from core.pubsub.grpc_generated import pubsub_pb2, pubsub_pb2_grpc
import time

class PubSubServicer(pubsub_pb2_grpc.PubSubServiceServicer):
    def __init__(self):
        self.subscribers = {}
    
    def Publish(self, request, context):
        topic = request.topic
        data = request.data
        
        # Distribute to subscribers
        if topic in self.subscribers:
            for queue in self.subscribers[topic]:
                queue.put((data, int(time.time())))
        
        return pubsub_pb2.PublishResponse(
            success=True,
            message="Published successfully"
        )
    
    def Subscribe(self, request, context):
        topic = request.topic
        subscriber_id = request.subscriber_id
        
        import queue
        message_queue = queue.Queue()
        
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        self.subscribers[topic].append(message_queue)
        
        try:
            while context.is_active():
                try:
                    data, timestamp = message_queue.get(timeout=1)
                    yield pubsub_pb2.Message(
                        topic=topic,
                        data=data,
                        timestamp=timestamp
                    )
                except queue.Empty:
                    continue
        finally:
            self.subscribers[topic].remove(message_queue)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pubsub_pb2_grpc.add_PubSubServiceServicer_to_server(
        PubSubServicer(), server
    )
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
```

## Features

### Automatic Reconnection
The subscriber automatically attempts to reconnect if the stream is interrupted.

### Retry Logic
The publisher retries failed publish attempts up to `max_retries` times.

### Thread-Safe Operations
Both publisher and subscriber use thread-safe operations inherited from the base classes.

### Monitoring
Use the `details()` method to get statistics:

```python
pub_stats = publisher.details()
# Returns: {name, destination, publish_interval, batch_size, 
#           last_publish, publish_count, queue_depth}

sub_stats = subscriber.details()
# Returns: {name, source, max_depth, current_depth,
#           last_receive, receive_count, suspended}
```

## Troubleshooting

### Import Errors
If you see `gRPC stubs not found`, ensure you've generated the proto files correctly and they're in the right location.

### Connection Errors
- Verify the gRPC server is running and accessible
- Check firewall settings
- Ensure the host:port is correct
- For SSL connections, verify certificate validity

### Stream Interruptions
The subscriber will automatically attempt to reconnect. Check the logs for connection status and errors.

## Example End-to-End

```python
# Terminal 1: Start gRPC server
python grpc_server.py

# Terminal 2: Create and start subscriber
from core.pubsub.pubsubfactory import create_subscriber

sub_config = {
    'source': 'grpc://localhost:50051/events',
}
subscriber = create_subscriber('event_sub', sub_config)
subscriber.start()

# Terminal 3: Create publisher and send data
from core.pubsub.pubsubfactory import create_publisher

pub_config = {
    'destination': 'grpc://localhost:50051/events',
}
publisher = create_publisher('event_pub', pub_config)
publisher.publish({'event': 'user_login', 'user_id': 123})

# Back to Terminal 2: Receive data
data = subscriber.get_data(block_time=5)
print(data)  # {'event': 'user_login', 'user_id': 123}
```


## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.