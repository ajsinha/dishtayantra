# A5 — Decoupled Async Egress Subsystem — Design

Status: **proposed (design).** Roadmap item A5 (pairs with D2 backpressure and B2
exactly-once sinks). This document specifies how publication to **external**
brokers (Kafka, Redis, RabbitMQ/ActiveMQ, filesystem, S3, …) is decoupled from the
DAG compute thread so it runs **independently and at high throughput**, without
ever stalling compute and without unbounded memory growth. It is **opt-in and
off by default**; a deployment that does not enable it publishes exactly as today.

Three things are explicit requirements and are designed in full below:
- a **master on/off switch** (and per-publisher opt-in) — §8;
- a **configurable WAL backend**, including whether LMDB is used at all — §6, §8;
- a **dedicated management UI page** — §9.

---

## 1. Motivation

There is one compute thread per DAG, and `topological_sort()` is computed once at
the top of `do_compute()` (`core/dag/compute_graph.py`); the sweep then runs in a
fixed order under the equality gate. Today a `PublicationNode` calls the broker
connector's `publish()` **inline on that thread**. For the in-memory broker that
is microseconds. For an external broker it is a network or disk round-trip with
variable latency — so a slow or saturated broker does not merely slow egress, it
**stalls the entire sweep** and backs pressure into the whole DAG.

The naive fix — "just make publish async into an in-memory buffer" — trades a stall
for an **out-of-memory crash**: if the broker is persistently slower than inflow,
an unbounded buffer grows until the process dies. So the real design is *bounded*
async: a fast non-blocking handoff, a durable buffer that absorbs bursts, parallel
independent writers, and **backpressure as the hard floor** so sustained slowness
slows the source (or sheds on a stated policy) rather than exhausting memory.

---

## 2. Goals / non-goals

**Goals**
- Compute thread never blocks on external broker I/O.
- High egress throughput via batching and parallel, per-destination writers.
- Bounded memory under sustained broker slowness (no OOM); explicit overflow policy.
- Durable buffering so a transient broker outage does not lose data or stall compute.
- At-least-once delivery with crash recovery; a path to exactly-once via B2.
- A **hard cap** on the number of egress worker processes; DAGs multiplex onto a
  bounded pool and are allocated automatically as they are added.
- **No second configuration surface (the key requirement):** egress workers derive
  every broker endpoint (Kafka bootstrap, Redis URL, file path, …) from the DAG's
  *existing* publisher definitions. Operators configure publishers once, as today;
  there is no separate egress/connection config to maintain.
- **Per-destination FIFO ordering is guaranteed:** messages are written to a given
  destination in the exact order the DAG produced them. This is a correctness
  requirement, not a tuning option — reordering could, e.g., fire a Sell before its
  Buy is committed. Enforced by a single ordered writer per destination (per
  partition/key) and order-preserving retries (§5.2).
- Crash-durable buffering: the WAL survives process crash and restart (at-least-once).
- Fully opt-in, configurable (incl. whether LMDB is used), with a management UI.

**Non-goals**
- Not a general task queue (that is the Celery/JobDispatch discussion — batch
  fan-out, not streaming egress).
- Not changing the in-memory broker fast path (it stays inline and synchronous).
- Not exactly-once in v1 (designed for, delivered with B2 transactional sinks).

---

## 3. Architecture overview

```
   ┌─ compute process (one compute thread per DAG) ──────────┐
   │  publish(batch)  →  non-blocking append to WAL  → return │
   └───────────────┬─────────────────────────────────────────┘
                   │ WAL = durable buffer + zero-copy IPC channel
                   ▼
   ┌─ Egress Supervisor (DishtaYantra-managed, bounded pool) ──┐
   │  allocates work onto a CAPPED worker pool (by destination, │
   │  or pin a whole DAG to one worker for strict ordering)     │
   │  ┌─ worker ─────────────────────────────────────────────┐ │
   │  │ tail WAL → batch → write → ack → retry / circuit-brk  │ │
   │  │ connector built AUTOMATICALLY from the publisher spec  │ │
   │  │ already in the DAG config — NO separate egress config  │ │
   │  └───────────────────────┬──────────────────────────────┘ │
   └──────────────────────────┼────────────────────────────────┘
                              ▼
              Kafka / Redis / RabbitMQ / filesystem / S3 / …
```

Key properties: the compute thread's publish cost is O(append); the **WAL is both
the durable buffer and the cross-process handoff** (no pickling of bulk data); each
worker **auto-builds its connectors from the publisher specs already in the DAG
config**, so there is no separate egress configuration; writers batch and
parallelize, with ordering preserved per destination (or per DAG when pinned).

---

## 4. The handoff boundary

When async egress is enabled for a publisher, `PublicationNode.publish()` does not
call the connector. It **appends** the record/`RecordBatch` to that publisher's WAL
segment and returns. What frees the compute thread is this non-blocking handoff —
the WAL and the egress processes are what make it durable and fast, respectively.
Post-A1 the value is already an Arrow `RecordBatch`, so the append is a columnar
write and the writers can emit columnar formats (Parquet/Arrow) with no repack.

## 5. Egress supervisor, worker pool & auto-configuration

> **Shipped first cut (v5.9.0):** the implemented model is a *bounded in-process
> worker pool* sized by `egress.worker.count` (default 4); destinations are assigned
> to a worker by a stable hash of the WAL key (one destination → one worker, so
> per-destination FIFO holds), and a stalled destination backs off without starving
> siblings. Per-publisher opt-in is the `async_egress` flag in a publisher's config
> (with `egress.async.default` as the fallback), and in-memory destinations are always
> inline. The richer model below — separate egress *processes*, `egress.worker.max_total`
> with `by_destination`/`by_dag` allocation, and a supervisor — is the future target;
> the WAL-on-disk design already permits moving to it without changing the contract.

DishtaYantra spawns and manages the egress tier itself using the existing
`core/multiprocessing` / `core/workers` machinery — no external service to deploy.

- **A bounded worker pool with a hard cap.** `egress.worker.max_total` (§8) caps how
  many egress processes may ever exist. Work is *allocated onto* this fixed pool; the
  cap is never exceeded no matter how many DAGs or destinations appear.
- **Allocation strategy (policy).** Default is **by destination** — a worker (or a
  small key-partitioned pool) per broker destination, so a single writer per
  destination/partition preserves order and slow destinations don't starve fast
  ones. When strict cross-publisher ordering for a DAG is required, set the policy to
  **pin the whole DAG to one worker** instead. Either way it is *allocation onto the
  capped pool*, not unbounded spawning.
- **Automatic allocation as DAGs are added.** When a DAG starts, its publishers are
  assigned to workers automatically (least-loaded), reusing a worker already
  connected to the same destination where possible; when a DAG stops, its work is
  released. No manual assignment, and low-traffic destinations multiplex onto shared
  workers so the cap is respected.
- **Spawn clean, never fork a warmed parent.** DishtaYantra is heavily threaded
  (compute thread, uvicorn, broker clients); `fork()` of a multithreaded process is
  unsafe and Python is moving its default away from it. Children are started with
  `spawn`/`forkserver` (or early, before the thread/connection soup) and build their
  own connections rather than inheriting live handles.
- **Supervision.** Heartbeats, restart-on-crash, and on restart the worker resumes
  draining its WAL **from the last durably-acked offset** (at-least-once). Clean
  drain/checkpoint on shutdown.

### 5.1 Auto-configuration from publisher specs (no second config surface)

This is the key requirement: **operators never configure egress endpoints
separately.** A publisher already declares its destination and connection in the DAG
config (e.g. a Kafka publisher's bootstrap servers + topic, a Redis URL, a file
path). The egress worker, when it picks up that publisher's work, **reconstructs the
connector from that exact publisher spec via the existing connector factory** — the
same code path the inline publish uses today. Concretely:

- The WAL record carries the *publisher identity* (DAG id + publisher name) and the
  payload, not connection details.
- The worker resolves the publisher's spec from the loaded DAG definition and builds
  the connector with the identical settings the DAG already specifies.
- Workers that serve several DAGs pointing at the same broker **dedup/pool the
  connection**, so one Kafka cluster used by ten DAGs needs one producer, not ten.

The only *new* configuration A5 introduces is egress **behaviour** (the on/off
toggle, worker cap, WAL backend, batching, overflow policy) — never endpoint or
credential details, which continue to live with the publishers.

### 5.2 Ordering guarantee (per-destination FIFO)

**Requirement:** messages must reach a destination in the same order the DAG
produced them. In a trading pipeline this is correctness, not cosmetics — a Sell
emitted after its Buy must never be written first. The pipeline preserves order at
every stage:

- **Append order = production order.** The compute thread appends to the
  destination's WAL in emission order; the WAL is an ordered log.
- **Single ordered writer per destination.** Exactly one writer drains a given
  destination, reading the WAL strictly in offset order and writing in that order.
  Order to a destination is therefore total per producing DAG.
- **Parallelism only where it cannot reorder.** Throughput comes from parallel
  writers *across* destinations, and *within* a destination only along independent
  partition keys (e.g. a Kafka partition per instrument): a key maps to exactly one
  writer (consistent hashing), so per-key order holds while different keys run in
  parallel. `writers_per_destination > 1` is therefore **key-partitioned, never
  round-robin** — round-robin would reorder and is not allowed. The safe default is
  one writer per destination (total order).
- **Order-preserving retries — the subtle part.** If writing message *N* fails, the
  writer must **not** advance to *N+1* until *N* succeeds; it stops the line for that
  destination/partition and retries *N* (with backoff / circuit breaker). Letting
  *N+1* through while *N* retries would reorder — exactly the Sell-before-Buy hazard.
  So a stuck destination experiences head-of-line blocking *by design*: its WAL backs
  up and backpressure flows to the source, rather than messages skipping ahead.
- **Recovery preserves order.** On restart the writer resumes from the last acked
  offset and replays the un-acked tail in order; an idempotent/keyed producer dedups
  the boundary without reordering.

**Scope note (honest limitation):** this guarantees order *to a single destination*.
It does **not** by itself sequence messages across *different* destinations. If a Buy
and a Sell are sent to the *same* gateway/topic, per-destination FIFO fully protects
them. If they go to two different destinations and must still be causally ordered,
that needs an explicit cross-destination sequencing step (route both through one
destination, or a barrier) — called out so it is a conscious design choice, not a
surprise.

### 5.3 Connection loss & recovery (no message lost)

Destination workers must ride out broker restarts, network blips, and credential
refreshes without dropping a message — and the WAL-plus-ack contract gives this
almost for free:

- **Auto-reconnect.** On a connection/write failure the writer reconnects with
  exponential backoff + jitter, guarded by a per-destination **circuit breaker**
  (closed → open on repeated failure → half-open probe → closed). It rebuilds the
  connector from the publisher spec (§5.1) — no operator action, no separate config.
- **Nothing is lost while down.** A record is reclaimed from the WAL only after the
  broker **acks** it (§6.2). So every un-acked record stays durably in the WAL through
  the entire outage; the worker simply cannot advance its checkpoint until writes
  succeed again.
- **Nothing is reordered.** Per §5.2 the writer **stops the line** while disconnected
  — it does not skip ahead to later messages — so on recovery it resumes exactly at
  the last acked offset and replays the un-acked tail **in order**.
- **At-least-once at the seam.** A record that was acked by the broker but whose
  checkpoint was not yet persisted before a crash will replay on reconnect; an
  idempotent/keyed producer (e.g. Kafka idempotence) dedups that boundary case.
- **Visible, bounded.** During the outage the destination's WAL grows and backpressure
  flows to the source (bounded by the cap + overflow policy); the management UI shows
  connection state, reconnect attempts, and circuit-breaker status — the failure
  surfaces as lag and a breaker light, never as silent message loss.

### 5.4 Multiprocess / worker mode (massively parallel egress)

DishtaYantra can run DAGs across many worker processes (worker config). The egress
design composes with this **naturally**, because of one existing invariant:

- **A whole DAG is pinned to one worker process.** `core/workers/dag_affinity.py`
  maps `dag_name -> worker_id`; a DAG is never split across processes. So all of a
  DAG's PublicationNodes — and therefore its WALs and its drainers — live together
  in that one process.

From that, everything follows without cross-process machinery:

- **Per-DAG FIFO holds for free.** Because a DAG's single ordered drainer runs in the
  same process that produced the messages, per-destination FIFO for that DAG is
  preserved with zero cross-process coordination.
- **Massively parallel by construction.** Each worker process independently appends
  to and drains *its own* DAGs' WALs. N workers drain concurrently across all cores —
  no global lock, no shared queue, no IPC on the hot path. Adding workers adds egress
  throughput linearly.
- **Collision-free, restart-safe WAL naming.** Each WAL is namespaced by
  `(dag, publisher)`. **DAG names are universally unique**, so this key is globally
  unique on its own — no worker id is required. Keying by the stable DAG name (not
  the worker slot) is also what makes resume correct: if a DAG is reassigned to a
  *different* worker after a restart (rebalancing), it still reopens its own WAL and
  resumes from the last acked offset — a worker-id key would orphan that tail. Two
  processes therefore never share a WAL, with no extra coordination.
- **Ordering scope across processes.** Messages to the *same physical destination*
  from *different* DAGs/workers interleave — they are independent producers that were
  never globally ordered (identical to any multi-producer Kafka setup). Per-destination
  FIFO is guaranteed **per producing DAG**. If a single global order to one
  destination is required, that stream must be produced by one DAG (a funnel) — the
  documented cross-producer limitation (§5.2), unchanged by worker mode.
- **Relationship to the future supervisor.** Because the WAL is on disk, a later
  shared egress-process tier could pick up WALs written by any worker. But the
  in-process per-worker drainer model implemented here is the better fit for worker
  mode and already saturates the cores; the supervisor remains an option, not a
  requirement.

Net: turning on worker mode does not change the publication contract. Each process
gets its own bounded, durable, ordered, self-reconnecting egress, and the system
scales out across processes for free.

## 6. The WAL (configurable backend — incl. LMDB on/off)

The WAL is an append-only, per-destination log that is simultaneously the durable
buffer and the inter-process channel. Its backend is **configurable** (§8), and
**no backend requires a native library** unless you choose `lmdb`:

- `filelog` *(portable default)* — a segmented append-only log written with the
  Python **stdlib** (`open`/`os.write`/`os.fsync`, optional `mmap` for reads). Works
  on Linux, macOS, and Windows with **no native dependency**. Durable; records are
  length-prefixed + checksummed for torn-tail detection.
- `sqlite` — uses Python's built-in `sqlite3` (ships with CPython on every platform).
  SQLite in WAL mode is transactional and crash-safe, and reclamation is a simple
  `DELETE ... WHERE offset <= acked`. A durable, fully portable option with zero
  extra dependencies.
- `lmdb` — the existing `core/lmdb` zero-copy store (memory-mapped, ACID). Fastest
  durable option, but needs the LMDB wheel/native lib, so it is **opt-in** and used
  only where it is available.
- `memory` — in-process shared-memory ring buffer, **no disk durability**; lowest
  latency, for loss-tolerant streams.

**Portability:** because `filelog` and `sqlite` are pure-stdlib, a platform without
LMDB loses nothing — pick `filelog` (default) or `sqlite` and the async path is fully
durable. LMDB is purely a performance opt-in. Each backend implements the same small
contract — append, tail-from-offset, mark-acked(checkpoint), reclaim-acked, and a
**bounded size** with a high-water mark — so backends are interchangeable per
deployment (and per publisher).

### 6.1 Surviving crash and restart

Yes — surviving process crash and restart is the WAL's core job, with two honest
distinctions to design around:

- **Process crash vs. power loss.** Data already written to a file lives in the OS
  page cache, so it survives a *process* crash (even `kill -9`) without an explicit
  flush, because the kernel owns the cache. Surviving *power loss / kernel panic*
  additionally requires the bytes to be `fsync`'d to disk. So durability is a
  policy, `egress.wal.fsync = always | interval | os`: `always` (fsync per
  append/batch — strongest, slowest), `interval` (group-commit every N ms — fast,
  with a small loss window on power loss), or `os` (rely on the page cache — survives
  process crash, not power loss).
- **Backend guarantees.** `lmdb` is transactional and crash-resistant (copy-on-write,
  ACID, no torn writes). `sqlite` (WAL mode) is likewise transactional and crash-safe.
  `filelog` uses length-prefix + checksum framing so a partial trailing write from a
  crash is detected and truncated on recovery rather than read as garbage. `memory`
  does **not** survive a crash — that is its stated trade-off.

Recovery is the resume already described: the **acked offset (checkpoint) is itself
durable** (updated only after the broker acks), so on restart the worker reopens each
WAL, finds the last acked offset, and replays the un-acked tail. That yields
at-least-once delivery; pair it with idempotent/keyed producers to minimise
duplicates. My recommendation: default `filelog` + `fsync=interval` for a good
durability/throughput/portability balance, `sqlite` if you want a transactional store
with no extra deps, and `lmdb` where it is available and you want maximum speed.

### 6.2 WAL maintenance — who keeps it from eating the host

The WAL must never grow without bound or it will fill the disk. Maintenance is
explicit, owned by the **egress subsystem (not the compute thread)**, and bounded by
config:

- **Segmented log + reclaim-acked.** The WAL is split into fixed-size **segments**
  (rolling files / row ranges). The durable checkpoint records the last acked offset;
  once *every* record in a segment is at or below the checkpoint, that whole segment
  is **reclaimed** — deleted for `filelog`, `DELETE ... WHERE offset <= acked`
  (+ periodic checkpoint/`VACUUM`) for `sqlite`, freed pages for `lmdb`, overwritten
  for the `memory` ring. Reclaiming whole acked segments is cheap (no rewrite).
- **A background janitor.** A low-priority maintenance loop in the egress supervisor
  runs on an interval (and on segment roll), reclaiming fully-acked segments,
  advancing the checkpoint, and compacting. It never touches the compute path.
- **Hard size cap = no runaway.** `egress.wal.max_bytes` bounds each destination's
  WAL. When the WAL approaches the cap (high-water %), the **overflow policy** kicks
  in — block (backpressure to source), spill, drop/sample, or dead-letter (§7) — so
  the WAL **cannot** eat all the disk; it hits a wall and applies policy instead.
- **Disk-pressure guard.** The janitor also watches free disk; below a floor it
  raises an alert and escalates the overflow policy, so a near-full volume degrades
  deliberately rather than crashing the host.
- **Retention.** Acked data is reclaimed promptly by default (the WAL is a buffer, not
  an archive). An optional retention window can keep acked segments briefly for audit
  /replay before reclamation, bounded by the same cap.

Net: the WAL self-trims to roughly "un-acked backlog + one active segment" in steady
state, and is hard-capped under sustained slowness — its footprint is bounded and
visible on the management UI, never open-ended.

## 7. Backpressure, bounded memory, delivery & HA

- **Bounded everything; backpressure is the floor.** Memory buffer (small, fast) →
  WAL spill (large, disk-bounded) → when the WAL nears its cap, the configured
  policy applies: **block** (propagate backpressure into the DAG — in many setups
  the source is itself a broker, so this parks the backlog durably *upstream* in
  Kafka rather than in our heap), **spill** (already spilling), **drop/sample**
  (best-effort when the source cannot be slowed), or **dead-letter**. Reuses the
  `CreditController` from `core/pubsub/backpressure.py`.
- **Delivery semantics.** At-least-once: a WAL entry is marked done only after the
  broker acks; on restart the writer replays from the last acked offset. Exactly-once
  arrives with B2 (transactional/idempotent sinks committed on checkpoint).
- **HA.** On failover the standby spawns its own egress processes and resumes from
  the shared/replicated WAL; per-destination segments make resume independent and
  clean. Un-drained tail is preserved as long as the WAL is durable/replicated.

## 8. Configurability (master switch, per-publisher, WAL backend)

Off by default — backward compatible. These keys configure egress **behaviour only**;
**no broker endpoints or credentials appear here** — those are read from the existing
publisher definitions (§5.1). Keys mirror the `backpressure.*` style in both
`config/application.yaml` and `config/application.properties` (parity-tested; yaml
booleans quoted). Added when implemented:

```
# master switch — when false, all publication is the existing inline path
egress.async.enabled=false

# WAL backend: filelog | sqlite | lmdb | memory
#   filelog/sqlite = pure stdlib, cross-platform (no native dep); lmdb = opt-in/fast
egress.wal.backend=filelog
egress.wal.path=./data/egress_wal
egress.wal.max_bytes=2147483648          # per-destination WAL cap (bound = no OOM/disk-fill)
egress.wal.high_water_pct=80             # warn / start backpressure threshold
egress.wal.segment_bytes=134217728       # rolling segment size (reclaimed once fully acked)
egress.wal.fsync=interval                # always | interval | os  (durability vs speed)
egress.wal.fsync_interval_ms=50          # group-commit window when fsync=interval
egress.wal.maintenance_interval_ms=1000  # janitor: reclaim fully-acked segments
egress.wal.retain_acked_seconds=0        # keep acked segments briefly for audit (0 = reclaim asap)
egress.wal.disk_free_floor_mb=512        # below this, alert + escalate overflow policy

# egress worker pool (bounded)
egress.worker.max_total=16               # HARD CAP on egress processes (never exceeded)
egress.worker.allocation=by_destination  # by_destination | by_dag (pin DAG for strict order)
egress.worker.writers_per_destination=1  # 1 = total per-destination order; >1 must be
                                         # key-partitioned (per-key order), never round-robin
egress.worker.start_method=spawn         # spawn | forkserver
egress.worker.idle_retire_seconds=300    # release an idle worker's assignment

# batching
egress.batch.max_records=1000
egress.batch.max_bytes=4194304
egress.batch.linger_ms=20

# overflow policy when WAL is full: block | spill | drop | dead_letter
egress.overflow.policy=block
egress.overflow.dead_letter=               # destination URI for dead_letter policy
```

Per-publisher override: a publisher's config may set `async: true|false`,
`wal_backend`, `overflow_policy`, and `batch.*` to opt in/out and tune individually,
so one DAG can mix inline and async publishers. Required keys fail loud; nothing
defaults silently.

## 9. Dedicated management UI page

A new admin page, **Egress Management**, added under the existing **Admin** menu in
`web/templates/base.html`, served at `/admin/egress` (route name `egress_management`,
admin-auth, registered in `routes/admin_routes.py` next to `/admin/monitoring`). It
must read live data from a metrics endpoint (extends `routes/metrics_routes.py`) and
honor the light/dark/green themes (CSS variables; include a `[data-theme="green"]`
block from the start — see the lesson from the DAG Designer).

**Displays**
- **Global state:** async egress on/off, WAL backend in use, total egress processes
  vs cap, aggregate throughput and lag.
- **Per-destination table:** destination URI, worker/pool status (running / restarting
  / idle-retired / crashed), WAL depth and high-water %, in-flight, throughput
  (records/s, bytes/s), write latency p50/p95/p99, retries, drops, dead-lettered,
  and **circuit-breaker state** (closed / open / half-open).
- **Trend charts:** WAL depth and egress lag over time per destination (reuse the
  charting already used on the monitoring page), so operators see a slow broker as
  rising lag *before* it saturates.

**Controls** (admin actions, each confirmed; all fail loud and are audit-logged)
- Toggle async egress on/off globally and per-publisher (writes the config / live
  flag).
- **Pause / resume / drain / flush** a destination's egress.
- **Restart** a stuck worker; **reset** an open circuit breaker.
- Change a destination's **overflow policy** and **batch** tuning live.

The page is the operator's window into exactly the failure the async design guards
against: it makes buffer growth and broker slowness visible and actionable instead
of a silent slide toward OOM.

## 10. Observability

Each egress process reports depth, high-water, in-flight, throughput, latency
histograms, retries, drops, dead-letters, and circuit-breaker state; the supervisor
aggregates them into the existing metrics/Prometheus surface and the
backpressure-stats accessor. Alerts fire on WAL high-water and sustained lag.

## 11. Backward compatibility

- `egress.async.enabled=false` by default → the inline publish path is unchanged;
  no WAL, no extra processes, no new dependency exercised.
- The in-memory broker fast path is never routed through async egress.
- LMDB is only touched when `egress.wal.backend=lmdb`; the feature runs without it
  via `filelog`, `sqlite`, or `memory` (all dependency-free except lmdb).
- Existing DAG configs load and behave identically; async is opt-in per publisher.

## 12. Risks & mitigations

| Risk | Mitigation |
|------|------------|
| Unbounded memory if buffer grows | Every tier bounded; backpressure floor (block/spill/drop); WAL cap with high-water alerts. |
| IPC re-introduces serialization cost | WAL (mmap/LMDB) *is* the channel — zero-copy tail; no pickling of bulk data; control messages only over Queue. |
| `fork()` of a multithreaded parent deadlocks | `spawn`/`forkserver`; children self-connect; no inherited live broker handles. |
| Process explosion under many destinations | Pool-per-destination with a hard total cap; multiplex low-traffic destinations; idle-retire. |
| Data loss on crash (async) | Durable WAL (filelog/sqlite/lmdb) + durable acked-offset resume = at-least-once; `memory` backend documents loss-on-crash. |
| Failover drops un-drained tail | Shared/replicated WAL; standby resumes per-destination segments. |
| Reordering at egress (e.g. Sell before Buy) | Per-destination FIFO (§5.2): single ordered writer per destination/key, key-partitioned (never round-robin) parallelism, and order-preserving retries that stop-the-line on failure rather than skip ahead. |

## 13. Phased delivery (maps to roadmap A5)

1. **Non-blocking handoff + bounded in-memory buffer + backpressure floor** (no WAL
   yet): frees the compute thread safely; `memory` backend only.
2. **WAL tier** with configurable backend (`filelog`, then `sqlite`/`lmdb`); durable
   checkpoint + segment reclamation (the janitor); acked-offset
   resume; at-least-once.
3. **Egress supervisor + per-destination processes** (spawn-clean, supervision,
   pool-per-destination, cap, idle-retire); batching + partitioned writers.
4. **Management UI page** + metrics; overflow policies incl. dead-letter; circuit
   breakers.
5. **HA resume** of WAL segments; **exactly-once** via B2 transactional sinks.

## 14. Open questions

- Default allocation policy — `by_destination` is the default. Per-destination FIFO
  ordering is a firm guarantee (§5.2), enforced by a single ordered writer per
  destination regardless of allocation; `by_dag` pinning is no longer required for
  ordering and is dropped unless another need for it emerges.
- **Resolved:** ordering is **per-destination FIFO** (not global cross-destination).
  Cross-destination causal ordering, if ever needed, is handled explicitly by routing
  through one destination or a barrier (§5.2 scope note), not by the egress tier.
- Default WAL backend — `filelog` + `fsync=interval` (durable, pure stdlib, cross-
  platform) is the proposed default; `sqlite` for a transactional store with no extra
  deps; `lmdb` opt-in where available for max speed.
- One WAL segment per destination vs per DAG vs a shared log with offsets — isolation
  vs file count; interacts with the allocation policy above.
- Should the supervisor live in the compute process or be its own small parent, for
  cleaner restart-of-everything semantics?
- How async egress interacts with C5 per-key dynamic destinations — likely shared,
  capped egress workers multiplexing many keyed WAL segments rather than a process
  per key.
