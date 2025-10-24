"""
Comprehensive Example: InMemoryRedis with DataPublisher/DataSubscriber
This example demonstrates all features working together in realistic scenarios.
"""

import time
import threading
from datetime import datetime
from inmemory_redisclone import InMemoryRedisClone
from inmemoryredis_datapubsub import (
    InMemoryRedisDataPublisher,
    InMemoryRedisDataSubscriber,
    InMemoryRedisChannelDataPublisher,
    InMemoryRedisChannelDataSubscriber
)


def example_1_task_queue():
    """Example 1: Distributed Task Queue Pattern"""
    print("\n" + "=" * 70)
    print("EXAMPLE 1: Task Queue Pattern")
    print("=" * 70)

    # Shared Redis instance
    redis = InMemoryRedisClone()

    # Task producer
    producer = InMemoryRedisDataPublisher(
        name="task_producer",
        destination="inmemoryredis://",
        config={
            'redis_instance': redis,
            'key_prefix': 'tasks:',
            'ttl_seconds': 600  # Tasks expire after 10 minutes
        }
    )

    # Multiple task consumers (workers)
    worker1 = InMemoryRedisDataSubscriber(
        name="worker_1",
        source="inmemoryredis://",
        config={
            'redis_instance': redis,
            'key_pattern': 'tasks:*',
            'delete_on_read': True,  # Remove processed tasks
            'poll_interval': 0.05
        }
    )

    worker2 = InMemoryRedisDataSubscriber(
        name="worker_2",
        source="inmemoryredis://",
        config={
            'redis_instance': redis,
            'key_pattern': 'tasks:*',
            'delete_on_read': True,
            'poll_interval': 0.05
        }
    )

    # Produce tasks
    print("\nProducing 10 tasks...")
    for i in range(10):
        producer.publish({
            '__dagserver_key': f'task_{i:03d}',
            'task_type': 'process_order',
            'order_id': f'ORD-{1000 + i}',
            'priority': 'high' if i % 3 == 0 else 'normal',
            'created_at': datetime.now().isoformat()
        })
        print(f"  ✓ Created task_{i:03d}")

    # Workers process tasks
    print("\nWorkers processing tasks...")
    time.sleep(0.2)  # Give time for initial polling

    processed_by_worker1 = []
    processed_by_worker2 = []

    for _ in range(5):  # Each worker tries to get 5 tasks
        task1 = worker1._do_subscribe()
        task2 = worker2._do_subscribe()

        if task1:
            processed_by_worker1.append(task1['__dagserver_key'])
            print(f"  Worker 1: Processed {task1['__dagserver_key']}")

        if task2:
            processed_by_worker2.append(task2['__dagserver_key'])
            print(f"  Worker 2: Processed {task2['__dagserver_key']}")

        time.sleep(0.05)

    print(f"\nWorker 1 processed: {len(processed_by_worker1)} tasks")
    print(f"Worker 2 processed: {len(processed_by_worker2)} tasks")
    print(f"Total processed: {len(processed_by_worker1) + len(processed_by_worker2)} tasks")

    # Cleanup
    producer.stop()
    worker1.stop()
    worker2.stop()


def example_2_event_notification():
    """Example 2: Event Notification System"""
    print("\n" + "=" * 70)
    print("EXAMPLE 2: Event Notification System")
    print("=" * 70)

    # Shared Redis instance
    redis = InMemoryRedisClone()

    # Event publisher
    event_publisher = InMemoryRedisChannelDataPublisher(
        name="event_system",
        destination="inmemoryredischannel://system_events",
        config={'redis_instance': redis}
    )

    # Multiple event handlers
    logger = InMemoryRedisChannelDataSubscriber(
        name="event_logger",
        source="inmemoryredischannel://system_events",
        config={'redis_instance': redis}
    )

    analytics = InMemoryRedisChannelDataSubscriber(
        name="analytics_engine",
        source="inmemoryredischannel://system_events",
        config={'redis_instance': redis}
    )

    notifications = InMemoryRedisChannelDataSubscriber(
        name="notification_service",
        source="inmemoryredischannel://system_events",
        config={'redis_instance': redis}
    )

    # Simulate various system events
    events = [
        {'event': 'user_signup', 'user_id': 12345, 'source': 'web'},
        {'event': 'purchase_complete', 'order_id': 'ORD-789', 'amount': 99.99},
        {'event': 'user_login', 'user_id': 12345, 'ip': '192.168.1.1'},
        {'event': 'error_occurred', 'service': 'payment', 'severity': 'high'},
        {'event': 'cache_cleared', 'cache_name': 'user_sessions', 'count': 150}
    ]

    print("\nPublishing system events...")
    for event in events:
        event['timestamp'] = datetime.now().isoformat()
        event_publisher.publish(event)
        print(f"  ✓ Published: {event['event']}")
        time.sleep(0.05)

    # All handlers receive and process events
    print("\nHandlers processing events...")
    time.sleep(0.2)

    print("\n  Logger received:")
    for _ in range(5):
        event = logger._do_subscribe()
        if event:
            print(f"    - {event['event']} at {event['timestamp']}")

    print("\n  Analytics received:")
    for _ in range(5):
        event = analytics._do_subscribe()
        if event:
            print(f"    - Analyzing: {event['event']}")

    print("\n  Notifications received:")
    for _ in range(5):
        event = notifications._do_subscribe()
        if event:
            if event.get('severity') == 'high':
                print(f"    - ALERT: {event['event']}")
            else:
                print(f"    - Info: {event['event']}")

    # Cleanup
    event_publisher.stop()
    logger.stop()
    analytics.stop()
    notifications.stop()


def example_3_request_response():
    """Example 3: Request-Response Pattern"""
    print("\n" + "=" * 70)
    print("EXAMPLE 3: Request-Response Pattern")
    print("=" * 70)

    # Shared Redis instance
    redis = InMemoryRedisClone()

    # Request publisher
    requester = InMemoryRedisDataPublisher(
        name="api_client",
        destination="inmemoryredis://",
        config={
            'redis_instance': redis,
            'key_prefix': 'requests:',
            'ttl_seconds': 60
        }
    )

    # Response publisher
    responder = InMemoryRedisDataPublisher(
        name="api_server",
        destination="inmemoryredis://",
        config={
            'redis_instance': redis,
            'key_prefix': 'responses:',
            'ttl_seconds': 60
        }
    )

    # Request subscriber (server side)
    request_subscriber = InMemoryRedisDataSubscriber(
        name="request_handler",
        source="inmemoryredis://",
        config={
            'redis_instance': redis,
            'key_pattern': 'requests:*',
            'delete_on_read': True
        }
    )

    # Response subscriber (client side)
    response_subscriber = InMemoryRedisDataSubscriber(
        name="response_handler",
        source="inmemoryredis://",
        config={
            'redis_instance': redis,
            'key_pattern': 'responses:*',
            'delete_on_read': True
        }
    )

    # Client sends requests
    print("\nClient sending requests...")
    request_ids = []
    for i in range(3):
        request_id = f"req_{i:03d}"
        request_ids.append(request_id)

        requester.publish({
            '__dagserver_key': request_id,
            'method': 'GET',
            'endpoint': f'/api/users/{100 + i}',
            'request_time': datetime.now().isoformat()
        })
        print(f"  ✓ Sent request: {request_id}")

    time.sleep(0.2)

    # Server processes requests and sends responses
    print("\nServer processing requests...")
    for _ in range(3):
        request = request_subscriber._do_subscribe()
        if request:
            request_id = request['__dagserver_key'].replace('requests:', '')
            print(f"  ⚙ Processing: {request_id}")

            # Simulate processing
            time.sleep(0.05)

            # Send response
            responder.publish({
                '__dagserver_key': request_id,
                'status': 200,
                'data': {'user_id': 100, 'name': 'John Doe'},
                'response_time': datetime.now().isoformat()
            })
            print(f"  ✓ Sent response: {request_id}")

    time.sleep(0.2)

    # Client receives responses
    print("\nClient receiving responses...")
    for _ in range(3):
        response = response_subscriber._do_subscribe()
        if response:
            request_id = response['__dagserver_key'].replace('responses:', '')
            print(f"  ✓ Received response for {request_id}: Status {response['status']}")

    # Cleanup
    requester.stop()
    responder.stop()
    request_subscriber.stop()
    response_subscriber.stop()


def example_4_caching_layer():
    """Example 4: Caching Layer with TTL"""
    print("\n" + "=" * 70)
    print("EXAMPLE 4: Caching Layer with TTL")
    print("=" * 70)

    # Shared Redis instance
    redis = InMemoryRedisClone()

    # Cache writer
    cache_writer = InMemoryRedisDataPublisher(
        name="cache_writer",
        destination="inmemoryredis://",
        config={
            'redis_instance': redis,
            'key_prefix': 'cache:',
            'ttl_seconds': 2  # Short TTL for demo
        }
    )

    # Cache reader
    cache_reader = InMemoryRedisDataSubscriber(
        name="cache_reader",
        source="inmemoryredis://",
        config={
            'redis_instance': redis,
            'key_pattern': 'cache:*',
            'delete_on_read': False  # Keep cache entries
        }
    )

    # Write to cache
    print("\nWriting to cache...")
    cache_entries = [
        {'key': 'user_123', 'data': {'name': 'Alice', 'role': 'admin'}},
        {'key': 'session_abc', 'data': {'user_id': 123, 'expires': '2024-10-24'}},
        {'key': 'config_app', 'data': {'debug': True, 'version': '1.0.0'}}
    ]

    for entry in cache_entries:
        cache_writer.publish({
            '__dagserver_key': entry['key'],
            '__ttl_seconds': 2,
            **entry['data']
        })
        print(f"  ✓ Cached: {entry['key']}")

    time.sleep(0.2)

    # Read from cache
    print("\nReading from cache (immediately)...")
    cache_reader.reset_seen_keys()  # Reset to read all keys

    for _ in range(3):
        entry = cache_reader._do_subscribe()
        if entry:
            key = entry['__dagserver_key'].replace('cache:', '')
            print(f"  ✓ Cache hit: {key}")
        time.sleep(0.05)

    # Wait for TTL expiration
    print("\nWaiting for cache expiration (2 seconds)...")
    time.sleep(2.5)

    # Try to read again
    print("\nReading from cache (after expiration)...")
    cache_reader.reset_seen_keys()

    found_any = False
    for _ in range(3):
        entry = cache_reader._do_subscribe()
        if entry:
            key = entry['__dagserver_key'].replace('cache:', '')
            print(f"  ✓ Cache hit: {key}")
            found_any = True
        time.sleep(0.05)

    if not found_any:
        print("  ✗ Cache miss: All entries expired")

    # Cleanup
    cache_writer.stop()
    cache_reader.stop()


def example_5_pubsub_chat():
    """Example 5: Simple Chat Room (Pub/Sub)"""
    print("\n" + "=" * 70)
    print("EXAMPLE 5: Simple Chat Room")
    print("=" * 70)

    # Shared Redis instance
    redis = InMemoryRedisClone()

    # Chat publisher
    chat_publisher = InMemoryRedisChannelDataPublisher(
        name="chat_system",
        destination="inmemoryredischannel://chatroom",
        config={'redis_instance': redis}
    )

    # Multiple users
    users = []
    for user_name in ['Alice', 'Bob', 'Charlie']:
        user = InMemoryRedisChannelDataSubscriber(
            name=f"user_{user_name}",
            source="inmemoryredischannel://chatroom",
            config={'redis_instance': redis}
        )
        users.append((user_name, user))

    # Simulate chat messages
    messages = [
        {'from': 'Alice', 'text': 'Hello everyone!'},
        {'from': 'Bob', 'text': 'Hi Alice! How are you?'},
        {'from': 'Charlie', 'text': 'Hey folks!'},
        {'from': 'Alice', 'text': 'I\'m good, thanks Bob!'},
        {'from': 'Bob', 'text': 'Welcome Charlie!'}
    ]

    print("\nChat session starting...")
    print("-" * 70)

    for msg in messages:
        # Publish message
        chat_publisher.publish({
            'from': msg['from'],
            'text': msg['text'],
            'timestamp': datetime.now().isoformat()
        })

        print(f"[{msg['from']}]: {msg['text']}")
        time.sleep(0.1)

    print("-" * 70)
    print("\nEach user's inbox:")
    time.sleep(0.2)

    # Show what each user received
    for user_name, user_subscriber in users:
        print(f"\n{user_name}'s messages:")
        message_count = 0
        for _ in range(10):  # Try to get up to 10 messages
            msg = user_subscriber._do_subscribe()
            if msg:
                if msg['from'] != user_name:  # Don't show own messages
                    print(f"  - From {msg['from']}: {msg['text']}")
                    message_count += 1
            else:
                break
            time.sleep(0.01)

        if message_count == 0:
            print("  (No messages)")

    # Cleanup
    chat_publisher.stop()
    for _, user_sub in users:
        user_sub.stop()


def main():
    """Run all examples"""
    print("\n" + "=" * 70)
    print("INMEMORYREDIS DATAPUBSUB - COMPREHENSIVE EXAMPLES")
    print("=" * 70)

    try:
        example_1_task_queue()
        example_2_event_notification()
        example_3_request_response()
        example_4_caching_layer()
        example_5_pubsub_chat()

        print("\n" + "=" * 70)
        print("✓✓✓ ALL EXAMPLES COMPLETED SUCCESSFULLY ✓✓✓")
        print("=" * 70 + "\n")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()