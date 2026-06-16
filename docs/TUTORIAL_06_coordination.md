# Two DAGs Coordinating in Parallel

Run two independent DAGs at the same time and have them coordinate purely
through shared in-memory queues — no shared code, no direct calls. A
**producer** emits work; a **consumer** processes it
and acknowledges back. This is the platform's pattern for decoupling a
pipeline into separately deployable, separately scheduled stages.

Everything so far has lived inside a single DAG. But real systems are often
split into stages that are owned, scheduled, and scaled independently — an
ingestion stage feeding a processing stage, for example. On this platform you do
that by running **multiple DAGs in parallel** that talk to each other
over the message bus. The simplest, zero-setup bus is **in-memory pub/sub**
(`mem://`), which is a process-wide singleton: any DAG can publish to a
queue and any other DAG in the same server can subscribe to it.

> **In plain English.** Until now everything lived on one assembly
> line. Here we run *two separate lines* at the same time that hand work to
> each other by dropping notes in shared mailboxes — they never call each
> other directly or share code. One line (the **producer**) makes
> work and posts it; the other (the **consumer**) picks it up, does
> it, and posts back a "done" note that the first line is watching for. Why bother?
> Because each line can then be owned by a different team, scheduled differently,
> and restarted on its own without disturbing the other — exactly how large
> systems keep their parts loosely connected. The only thing the two lines agree on
> is the *names of the mailboxes* (the queue names).

> **Ready-made:** two DAGs ship for this tutorial —
>
> **config/example/dags/coordination\_producer.json**
>
> and
>
> **config/example/dags/coordination\_consumer.json**
>
> .
> Copy *both* into `config/dags/`, reload, and start them both.

## The coordination loop

```
  ┌─────────────────────── coordination_producer ───────────────────────┐
  │  tick (metronome, 5s) ─► build_work ─► emit_work ──────────────┐      │
  │  results_in  ◄──────────────────────────────────────────────┐ │      │
  └──────────────────────────────────────────────────────────────┼─┼─────┘
                                                                  │ │
                              mem://queue/work_items  ◄───────────┘ │  (work)
                              mem://queue/work_results ─────────────┘  (acks)
                                                                  │ │
  ┌─────────────────────── coordination_consumer ─────────────────┼─┼─────┐
  │  work_in ─┬─► ack_build ─► ack_out ───────────────────────────┘ │     │
  │           └─► archive_out (file)                                 │     │
  └────────────────────────────────────────────────────────────────┘─────┘
```

The producer drops work items on `mem://queue/work_items`. The consumer
reads them, archives each to a file, builds an acknowledgement, and publishes it to
`mem://queue/work_results` — which the producer is listening on. The
two DAGs never reference each other; they share only queue names.

---

## New here? The idea in plain English

So far each tutorial has been a single assembly line. Real systems often run **several pipelines at once** that need to cooperate — one produces work, another consumes it. This tutorial builds **two separate DAGs** that coordinate through a shared queue: a *producer* that generates records and a *consumer* that processes them, running in parallel and handing off through the "post office" broker between them.

Why split into two instead of one big pipeline? Because the two halves can then scale, restart, and be reasoned about independently — the producer does not care who consumes, and the consumer does not care who produced. The shared queue is the clean seam between them.

**In one sentence:** two independent pipelines run side by side, one feeding the other through a shared queue — the foundation of decoupled, scalable systems.

---

## Step 1 — The producer DAG

A metronome fires every 5 seconds, a calculator stamps a work item, and a sink
publishes it. A second branch listens for acknowledgements coming back.

```
{
  "name": "coordination_producer",
  "subscribers": [
    { "name": "results_listener", "config": { "source": "mem://queue/work_results" } }
  ],
  "publishers": [
    { "name": "work_publisher", "config": { "destination": "mem://queue/work_items" } }
  ],
  "nodes": [
    { "name": "tick",       "type": "MetronomeNode",
      "config": { "interval": 5, "message": "generate" } },
    { "name": "build_work", "type": "CalculationNode", "calculator": "make_work", "config": {} },
    { "name": "emit_work",  "type": "PublisherSinkNode",
      "config": { "publishers": ["work_publisher"] } },

    { "name": "results_in", "type": "SubscriptionNode",
      "subscriber": "results_listener", "calculator": "ack_logger", "config": {} }
  ],
  "edges": [
    { "from_node": "tick",       "to_node": "build_work" },
    { "from_node": "build_work", "to_node": "emit_work" }
  ]
}
```

Note that `results_in` has no outgoing edge — it is a standalone
listener branch within the same DAG. A single DAG can both produce on one queue and
consume from another.

## Step 2 — The consumer DAG

The consumer subscribes to the producer's output queue, processes each item, and
fans out to two sinks: an acknowledgement back to the producer, and a file archive.

```
{
  "name": "coordination_consumer",
  "subscribers": [
    { "name": "work_listener", "config": { "source": "mem://queue/work_items" } }
  ],
  "publishers": [
    { "name": "results_publisher", "config": { "destination": "mem://queue/work_results" } },
    { "name": "processed_log",
      "config": { "destination": "file:///tmp/log/dagserver/coordination_processed.jsonl" } }
  ],
  "nodes": [
    { "name": "work_in",    "type": "SubscriptionNode",
      "subscriber": "work_listener", "calculator": "process_item", "config": {} },
    { "name": "ack_build",  "type": "CalculationNode", "calculator": "build_ack", "config": {} },
    { "name": "ack_out",    "type": "PublisherSinkNode",
      "config": { "publishers": ["results_publisher"] } },
    { "name": "archive_out","type": "PublisherSinkNode",
      "config": { "publishers": ["processed_log"] } }
  ],
  "edges": [
    { "from_node": "work_in",   "to_node": "ack_build" },
    { "from_node": "work_in",   "to_node": "archive_out" },
    { "from_node": "ack_build", "to_node": "ack_out" }
  ]
}
```

> **The contract is the queue name.** The producer publishes to
> `mem://queue/work_items`; the consumer subscribes to the same string.
> That shared name is the entire integration surface. Swap the consumer for a
> different DAG that reads the same queue and the producer neither knows nor cares.

## Step 3 — Run them in parallel

1. Copy **both** JSON files into `config/dags/`.
2. Click **Reload DAGs** (or restart). You will see both
   `coordination_producer` and `coordination_consumer` on the
   dashboard.
3. Click **Start** on *both*. They now run concurrently, each on
   its own compute thread.
4. Open each DAG's **View Logs** (filtered to that DAG) to watch the
   hand-off: the producer emitting every 5s, the consumer processing and acking.
5. Check `/tmp/log/dagserver/coordination_processed.jsonl` — the
   consumer's file archive fills up as work flows through.

> **Warning —**
>
> **Order of starting does not matter, but liveness does.** In-memory
> queues hold messages until consumed, so if you start the producer first, items
> queue up until the consumer starts. But the in-memory bus lives *inside the
> server process* — it is not durable across restarts. For coordination that
> must survive restarts or span multiple servers, use a real broker
> (Kafka, RabbitMQ, Redis) by changing only the queue URIs — the DAG structure is
> identical. See the per-broker [User Guides](/help/userguides).

## Why split into two DAGs?

- **Independent scheduling.** The producer can run on a market-hours
  schedule while the consumer runs 24/7 to drain the backlog.
- **Independent scaling.** Put the heavy consumer on the
  [worker pool](/help/worker-pool) or use
  [AutoClone](/help/autoclone) to add consumer copies at
  peak, without touching the producer.
- **Independent deployment.** Reload or fix one stage without stopping
  the other — they only share a queue name.
- **Fan-out / fan-in across DAGs.** Several consumer DAGs can read the
  same queue to load-balance, or several producers can feed one consumer.

> **What this demonstrates:** two independently-scheduled DAGs running in
> parallel and coordinating through the shared in-memory bus, a bidirectional
> request/acknowledge loop, fan-out to multiple sinks, and a clean path to swap the
> in-memory bus for a durable broker — the foundation for decomposing a large
> system into cooperating pipelines.

## You have completed the tutorials

For depth on the pieces used here, see the per-broker
[User Guides](/help/userguides),
[Worker Pool](/help/worker-pool),
[AutoClone](/help/autoclone), and
[Scheduling](/help/scheduling).
