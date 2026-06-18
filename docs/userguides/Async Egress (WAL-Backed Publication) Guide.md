# Async Egress (WAL-Backed Publication) Guide

Async egress decouples publication to external brokers (Kafka, Redis, RabbitMQ,
filesystem, …) from the DAG compute thread. With it enabled, a publisher's
`publish()` becomes a **non-blocking append to a write-ahead log (WAL)** and
returns immediately; a background drainer writes to the real broker. A slow or
briefly-unavailable broker no longer stalls the compute sweep, and messages are
buffered durably instead of lost.

> **Status / scope:** opt-in and **off by default**. This release implements a
> bounded egress worker pool (default 4), the portable WAL backends, per-destination
> FIFO ordering, per-publisher opt-in (with in-memory destinations always inline),
> auto-reconnect, and bounded buffering. A cross-process egress supervisor, the
> management UI, the `lmdb` backend, and exactly-once are later phases (see
> `docs/design/A5-async-egress-subsystem.md`).

## What it does (and doesn't change)

- **Frees the compute thread.** `publish()` appends to the WAL and returns; the
  network/disk write happens on a drainer thread.
- **Per-destination FIFO.** A single ordered drainer per destination writes in the
  exact order the DAG produced messages — so a Sell never overtakes its Buy.
- **No message loss on a blip.** A record is acked (and later reclaimed) only after
  the broker accepts it; on a write failure the drainer **stops the line**, retries
  the same record with backoff, and on restart resumes from the last acked offset.
- **Bounded memory/disk.** The WAL has a hard size cap; when full, the overflow
  policy applies (block = backpressure, or drop) — it never grows without bound.
- **No second configuration.** Endpoints come from your existing publisher
  definitions; async egress wraps them transparently. Nothing about your DAGs
  changes, and with the feature off the behaviour is byte-identical to before.

## Enabling it

In `config/application.yaml` (or the matching `config/application.properties`):

```yaml
egress:
  async:
    enabled: "true"           # master switch (default false)
    default: "true"           # when on, the default for publishers that don't set async_egress
  worker:
    count: 4                  # bounded egress worker pool size, per process (default 4)
  wal:
    backend: filelog          # filelog | sqlite | memory  (lmdb is a future opt-in)
    path: ./data/egress_wal
    max_bytes: 2147483648     # per-destination WAL cap (bound = no OOM/disk-fill)
    high_water_pct: 80
    segment_bytes: 134217728
    fsync: interval           # always | interval | os  (durability vs speed)
    fsync_interval_ms: 50
  batch:
    max_records: 500
  overflow:
    policy: block             # block (backpressure) | drop
    block_timeout_ms: 0       # 0 = wait indefinitely (true backpressure)
```

With `default: "true"` every eligible publisher uses the WAL (opt-out per publisher);
set `default: "false"` for opt-in mode, where only publishers that explicitly set
`async_egress: true` use the WAL.

## Per-publisher control (mixing WAL and direct)

Async egress is decided **per publisher**, so a DAG can mix WAL-backed and direct
publishers. In a publisher's own config (in the DAG definition) set:

```json
{ "name": "orders_out",
  "config": { "destination": "kafka://topic/orders", "async_egress": true } }
```

- `async_egress: true` → this publisher uses the WAL.
- `async_egress: false` → this publisher always publishes inline (direct), even with
  the master switch on.
- unset → follows `egress.async.default`.

**In-memory destinations never use the WAL.** Any publisher whose destination is
`mem://`, `inmemory://`, or `memory://` is always inline regardless of config — a
durable buffer in front of an in-process queue would add latency for no benefit.

## How many egress workers run

`egress.worker.count` (default **4**) bounds the egress worker threads **per
process**. Destinations are multiplexed onto this fixed pool — each destination is
assigned to one worker by a stable hash of its WAL key, so one destination is always
drained by one worker (preserving per-destination FIFO), and many destinations share
the bounded pool rather than spawning a thread each. A stalled destination backs off
without blocking the other destinations on the same worker. In worker mode each DAG's
process has its own pool of this size.

## Choosing a WAL backend (portability)

None of the default backends require a native library:

| Backend | Durable? | Notes |
|---|---|---|
| `filelog` *(default)* | yes | Segmented append log, pure Python stdlib, cross-platform (Linux/macOS/Windows). |
| `sqlite` | yes | Built-in `sqlite3` in WAL mode; transactional; trivially portable. |
| `memory` | no | In-process ring; fastest; loss on crash. For loss-tolerant streams. |
| `lmdb` | yes | Opt-in, only if the `lmdb` package is installed (future). |

So a host without LMDB loses nothing — use `filelog` or `sqlite`.

## Durability and crash recovery

- `filelog` frames each record with a length + CRC32, so a partial trailing write
  from a crash is detected and truncated on restart (not read as garbage). `sqlite`
  (WAL mode) is transactional. `memory` does not survive a crash.
- The **acked offset is durable** (advanced only after the broker acks). On restart
  the drainer reopens the WAL, finds the last acked offset, and replays the un-acked
  tail in order → at-least-once. Pair with an idempotent/keyed producer to dedup.
- `fsync` policy trades durability for speed: `always` (per write, safest), `interval`
  (group-commit, the default), `os` (page cache — survives process crash, not power
  loss).
- **flush vs fsync (policy):** every WAL write is *flushed to the OS* immediately —
  cheap, makes the record visible to the drainer at once, and lets it survive a
  *process* crash (the kernel owns the page cache). `fsync` is the separate, tunable
  step that pushes the page cache to physical disk to survive *power loss*. So
  visibility and process-crash safety are always on; only power-loss durability is a
  knob.

## Keeping the WAL from growing

The drainer periodically **reclaims** fully-acked data (deletes acked `filelog`
segments / `DELETE` acked `sqlite` rows / drops acked `memory` entries). Combined
with the `max_bytes` cap and the overflow policy, the WAL self-trims to roughly
"un-acked backlog + active segment" and can never fill the host: when it reaches
the cap, `block` applies backpressure to the source (or `drop` sheds, counted).

## Connection loss

If the broker drops, the drainer auto-retries the current record with exponential
backoff (a per-destination circuit "opens" after repeated failures), holding the
line so nothing is reordered or lost; when the broker returns it resumes exactly
where it left off. The outage shows up as a growing WAL and backpressure, not as
lost messages.

## Multiprocess / worker mode

When DAGs run across multiple worker processes, async egress keeps working — and
scales out — because a whole DAG is pinned to one worker (`dag_name -> worker_id`),
so a DAG's publishers, WAL, and drainer all live in the same process:

- Each worker process appends to and drains **its own** DAGs' WALs, so N workers
  drain in parallel across all cores with no shared queue or cross-process locking —
  massively parallel egress for free.
- Each WAL is namespaced by `(DAG, publisher)`. DAG names are universally unique, so
  this is globally unique without a worker id — and because the key follows the DAG
  (not the worker slot), a DAG reassigned to a different worker after a restart still
  reopens its own WAL and resumes from the last acked offset.
- Per-destination FIFO is preserved per producing DAG. Different DAGs writing to the
  same physical destination interleave (independent producers) — if you need one
  global order to a destination, produce that stream from a single DAG.

Turning on worker mode requires no change to your DAGs or to egress config.

## Limitations (this release)

- Egress runs as a bounded pool of in-process worker **threads** (not yet separate
  egress processes / a cross-process supervisor).
- `lmdb` backend and exactly-once delivery are future phases.
- Ordering is guaranteed **per destination**, not across different destinations.

See the *Async Egress* tutorial for a hands-on walkthrough, and
`docs/design/A5-async-egress-subsystem.md` for the full design.
