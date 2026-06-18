# Tutorial: Async Publication — Single-Process vs Multiprocess Workers

This tutorial shows how WAL-backed async publication behaves in the two ways
DishtaYantra can run DAGs: **single-process** (one server process) and
**multiprocess** (DAGs spread across worker processes). The publication contract is
identical in both; what changes is how many egress pools exist and how they
parallelize. Read the *Async Egress (WAL-Backed Publication) Guide* first for the
basics; here we focus on the worker dimension.

## The one rule that makes this simple

A whole DAG is pinned to exactly one worker process (`core/workers/dag_affinity.py`
maps `dag_name -> worker_id`); a DAG is never split across processes. So a DAG's
publication nodes, their WALs, and the egress workers that drain them always live
**together in one process**. Everything below follows from that.

---

## Mode A — Single process (no DAG workers)

This is the default: one server process runs every DAG. Enable async egress:

```yaml
egress:
  async:
    enabled: "true"
    default: "true"      # eligible publishers use the WAL unless they opt out
  worker:
    count: 4             # bounded egress worker pool (per process)
  wal:
    backend: filelog
```

What happens:

- One process holds **one bounded pool of up to `worker.count` egress threads** (4).
- Every destination (one per `(DAG, publisher)`) is assigned to one of those workers
  by a stable hash, so a destination is always drained by a single worker
  (per-destination FIFO), and many destinations share the 4 workers.
- A publisher can opt out with `async_egress: false`, and in-memory destinations
  (`mem://`, …) are always inline.

You can watch the pool multiplex destinations without a server:

```python
import time
from core.egress.wal import MemoryWal
from core.egress.drainer import AsyncEgressManager

mgr = AsyncEgressManager()
sinks = {}
for d in range(10):                      # 10 destinations...
    key = f"dagX.dest{d}"
    sinks[key] = []
    wal = MemoryWal()
    for i in range(5):
        wal.append({"i": i})
    mgr.register(key, wal, sinks[key].append, max_workers=4)

time.sleep(0.3)
print("worker threads:", mgr.worker_count())   # <= 4 (bounded pool)
print("dest0 order:", [m["i"] for m in sinks["dagX.dest0"]])  # [0,1,2,3,4]
mgr.stop_all()
```

Ten destinations, at most four worker threads — they share the pool, each draining
its assigned WALs in order.

---

## Mode B — Multiprocess (DAG workers)

Turn on worker mode (see the *Worker Pool* tutorial / your worker config) so DAGs are
distributed across processes. Async egress config is **unchanged** — you don't
configure egress per process.

What happens, by the one rule above:

- Each worker process builds the DAGs assigned to it, so **each process gets its own
  egress pool of `worker.count` threads and its own WALs** for those DAGs.
- The pools run **in parallel across processes**: no shared queue, no cross-process
  lock on the publish path. Effective egress parallelism is
  `worker.count × number_of_worker_processes`.
- WALs are namespaced by `(DAG, publisher)`. DAG names are universally unique, so the
  WAL follows the DAG: if a DAG is reassigned to a different worker after a restart,
  it reopens its own WAL and resumes from the last acked offset.

You can simulate two worker processes in one shell — two independent managers, each
draining its own DAG's WAL, writing to disk under distinct namespaced directories:

```python
import os, time, tempfile
from core.egress.wal import create_wal
from core.egress.drainer import AsyncEgressManager

base = tempfile.mkdtemp()

def worker_process(dag_name, sink):
    mgr = AsyncEgressManager()                       # one pool per "process"
    key = f"{dag_name}.orders_out"
    wal = create_wal("filelog", key, {"path": os.path.join(base, "egress_wal")})
    mgr.register(key, wal, sink.append, max_workers=4)
    return mgr, wal

sinkA, sinkB = [], []
mgrA, walA = worker_process("priceDagA", sinkA)      # "worker 0"
mgrB, walB = worker_process("priceDagB", sinkB)      # "worker 1"
for i in range(5):
    walA.append({"i": i}); walB.append({"i": i})
time.sleep(0.4)

print("A drained:", [m["i"] for m in sinkA])         # [0,1,2,3,4]
print("B drained:", [m["i"] for m in sinkB])         # [0,1,2,3,4]
print("WAL dirs :", sorted(os.listdir(os.path.join(base, "egress_wal"))))
# -> ['priceDagA.orders_out', 'priceDagB.orders_out']  (namespaced by DAG)
mgrA.stop_all(); mgrB.stop_all()
```

Each "process" drains its own DAG independently and in parallel, and the on-disk
WALs are isolated by DAG name — which is exactly why a restarted worker resumes the
right log.

---

## Ordering across the boundary

Per-destination FIFO is guaranteed **per producing DAG**: because a DAG lives in one
process with one ordered writer per destination, its messages reach a destination in
production order in both modes. If two *different* DAGs (possibly on different
workers) write to the *same* physical destination, their messages interleave — they
are independent producers and were never globally ordered. If you need one global
order to a destination, produce that stream from a single DAG.

## Choosing settings

- `egress.worker.count` — per process. In single-process mode it's your total egress
  parallelism; in worker mode multiply by the number of worker processes. Start at 4.
- `egress.wal.backend` — `filelog` (default, durable, stdlib) or `sqlite`; `memory`
  for loss-tolerant. Each process writes its own WAL files under `egress.wal.path`.
- `egress.wal.fsync` — `interval` (default) balances durability and throughput;
  `always` for strictest durability.

## Verify checklist

- With async on, `publish()` returns immediately and the destination still receives
  every message, in order.
- Under `egress.wal.path` you see one subdirectory per `(DAG, publisher)`; in worker
  mode the set spans the DAGs each worker owns.
- Kill and restart a worker mid-flow: its DAGs resume from the acked offset with no
  loss and no reordering.

See also: *Async Egress (WAL-Backed Publication) Guide*, the basic *Async Egress*
tutorial, the *Worker Pool* tutorial, and `docs/design/A5-async-egress-subsystem.md`.
