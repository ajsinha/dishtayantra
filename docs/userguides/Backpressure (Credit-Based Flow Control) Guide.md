# Backpressure (Credit-Based Flow Control) Guide

When a fast producer feeds a slow consumer, the queue between them grows without
bound — eventually exhausting memory, or (in the in-memory topic fan-out) silently
dropping messages once a subscriber's queue is full. **Credit-based backpressure**
makes the consumer the authority on how much may be in flight, so the producer's
rate is naturally pinned to the consumer's.

## How it works

The consumer grants the producer a fixed number of **credits** — the maximum
number of un-consumed items allowed in flight. The producer must spend one credit
before emitting an item; when credits run out it **waits** (or drops, by policy)
instead of growing memory. As the consumer takes items, it **returns** credits,
which lets the producer proceed. This is the same mechanism Apache Flink and
HTTP/2 use; the advantage over a plain bounded queue is that it is explicit,
observable, and policy-driven.

## Quick start

Backpressure is **off by default** — the in-memory topic fan-out behaves exactly
as before. To enable it, set in `config/application.yaml` (mirror in
`config/application.properties`):

```yaml
backpressure:
  enabled: "true"
  capacity: 1000          # max in-flight items per subscriber
  policy: block           # block | drop
  timeout_ms: 0           # under 'block', max wait before a drop (0 = forever)
```

When enabled, each topic subscriber is created with a credit-aware queue and the
publisher spends a credit per message. No consumer or producer code changes — the
returned subscriber queue releases a credit automatically on each `get()`.

## Configuration reference

| Key | Values | Meaning |
|---|---|---|
| `backpressure.enabled` | `true` \| `false` | Master switch (default `false`). |
| `backpressure.capacity` | integer ≥ 1 | Maximum in-flight items per subscriber. |
| `backpressure.policy` | `block` \| `drop` | `block` waits for a credit (true backpressure); `drop` discards (counted) when none is free. |
| `backpressure.timeout_ms` | integer ≥ 0 | Under `block`, the maximum wait before falling back to a counted drop. `0` waits indefinitely. |

## Choosing a policy

- **`block`** gives true backpressure: the publisher slows to the consumer's pace
  and no data is lost. Use it when correctness matters more than never stalling.
- **`drop`** preserves liveness: the publisher never waits and instead drops
  (counting each drop) when the consumer is behind. Use it for best-effort
  telemetry where freshness beats completeness.

> **Important caveat:** under `block`, if a single thread both produces and
> consumes the same link, the producer can stall waiting for a credit that only
> its own (now-blocked) consumer would return. For that topology, prefer `drop`
> or set a finite `timeout_ms`. Because backpressure is off by default, this only
> matters once you opt in.

## Monitoring

Per-subscriber flow-control counters are available for observability:

```python
from core.pubsub.inmemorypubsub import InMemoryPubSub
InMemoryPubSub().get_backpressure_stats()
# {topic: [{"capacity":1000,"available":987,"in_flight":13,"policy":"block",
#           "granted":50321,"released":50308,"blocked":12,"dropped":0,
#           "max_in_flight":1000,"total_wait_seconds":0.41}]}
```

Rising `in_flight`/`blocked` (or any `dropped` under `drop`) indicates a consumer
that cannot keep up. These map cleanly onto the queue-depth signal the roadmap's
observability work tracks, and can be logged or exported to a metrics backend.

## Scope

This guide covers the in-memory broker's topic fan-out, where producer and
consumer both run through `InMemoryPubSub`. The `CreditController` primitive in
`core/pubsub/backpressure.py` is reusable and is the basis for extending
credit-based flow control to other connectors in future.
