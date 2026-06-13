# Resilient Connectors Guide

© 2025-2030 Ashutosh Sinha

## Overview

Every supported broker has a **resilient** variant that wraps the standard
client with automatic reconnection and message buffering, so a transient
broker outage does not lose data or crash a DAG. The resilient wrappers are
**drop-in replacements** for the underlying client classes — existing code
keeps working unchanged, gaining resilience transparently.

The simplest way to use them is the `"resilient": true` flag on a subscriber
or publisher config (see each broker's setup guide). This guide documents the
shared behaviour and the per-broker wrapper classes for advanced use.

## The shared resilience pattern

All resilient connectors implement the same three behaviours:

1. **Automatic reconnection** — connection failures are retried with
   configurable back-off; the connector keeps trying in the background
   instead of raising into the DAG.
2. **Message / command buffering** — data produced during an outage is held
   in a thread-safe in-memory queue and flushed once the connection is
   restored, preventing loss.
3. **State restoration** — subscriptions, channels, sessions, queues, QoS
   settings, and consumers are re-established automatically after a
   reconnect, so consumers resume where they left off.

### Common configuration parameters

These apply across all resilient connectors (names are consistent):

| Parameter | Default | Meaning |
| --- | --- | --- |
| `reconnect_tries` | 10 | Maximum reconnection attempts |
| `reconnect_interval_seconds` | 60 | Sleep between reconnection attempts |
| `buffer_max_messages` | 10,000 | Maximum items buffered during an outage |

Buffering can be turned off where the underlying client supports it (e.g.
Redis exposes `buffer_commands`). When the buffer fills, the oldest items are
the ones at risk — size it for your worst expected outage window.

### Enabling resilience from a DAG

```jsonc
// Publisher
{ "destination": "kafka://topic/orders", "resilient": true }
// Subscriber
{ "source": "rabbitmq://queue/inbound", "resilient": true }
```

The factory selects the resilient wrapper automatically when `resilient` is
true. No code changes are required.

## Per-broker wrapper classes

For programmatic use outside a DAG, each broker provides a wrapper class with
the same semantics. They live under `core.pubsub.resilient_*`.

### Kafka — `ReconnectAwareKafkaProducer` / resilient consumer
Extends the Kafka producer/consumer. Buffers `send()` calls during outages,
processes the buffer on a background thread once reconnected, and supports
`send_batch()`, `flush()`, and a clean `close()` that drains the buffer.
Helpers: `get_buffer_size()`, `get_failed_messages()`. Works with both the
`kafka-python` and `confluent-kafka` libraries.

### Redis — `ResilientRedisClient`
Extends `redis.Redis`. Adds reconnection, optional command buffering
(`buffer_commands`, `buffer_max_commands`), a resilient pipeline with retry
for batch operations, and automatic restoration of pub/sub subscriptions
after a reconnect.

### RabbitMQ — `ResilientRabbitBlockingConnection`
Extends `pika.BlockingConnection`. Provides a `ResilientChannel` wrapper that
tracks exchanges, queues, bindings, QoS, and consumers, and automatically
restores all of that channel state after a reconnect.

### ActiveMQ — `ResilientActiveMQConnection`
Extends `stomp.Connection`. Handles connection failures during any operation,
buffers messages during outages, and automatically restores subscriptions
after reconnection.

### TIBCO EMS — `ResilientTibcoEMSConnection`
Provides resilient JMS-style connectivity. A `ResilientSession` tracks
producers, consumers, and subscriptions and restores them after a reconnect.
Extra parameters: `server_url` (default `tcp://localhost:7222`) and
`client_id` for durable subscriptions.

### IBM WebSphere MQ / IBM MQ — `ResilientWebSphereMQQueueManager`
Extends `pymqi.QueueManager`. Restores open queues after reconnection and
supports the full MQ feature set (transactions, PCF commands, pub/sub,
SSL/TLS). Extra parameters: `queue_manager_name`, `channel`,
`connection_info` (`host(port)`), `use_ssl`, `ssl_cipher_spec`.

## When to use resilient connectors

- **Use them** for production pipelines against remote brokers, where network
  blips, broker restarts, and failovers are expected.
- **Skip them** for the in-memory/LMDB transports (there is no remote
  connection to lose) and for short-lived local development runs.

For broker-specific connection settings (URIs, auth, topics vs queues), see
that broker's own setup guide.

## TLS/SSL with resilient connectors

Resilience and transport security are independent and compose freely: set
`"resilient": true` to get reconnection/buffering, and set the broker's TLS
keys (e.g. `use_ssl`, `ssl_ca_certs`) to encrypt the connection. Each broker
has its own SSL/TLS configuration — see the **SSL/TLS Security** section in
that broker's setup guide for the exact keys and a sample (Kafka also supports
SASL). Cloud services (AWS SQS/Kinesis/SNS, Azure Service Bus/Event Hubs, and
the S3/Azure Blob/GCS object stores) are encrypted in transit by default
through their SDKs.
