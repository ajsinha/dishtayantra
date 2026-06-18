# Tutorial: Async Egress (WAL-Backed Publication)

This walkthrough shows how publication becomes non-blocking and durable when async
egress is on, how ordering and recovery behave, and how it scales in worker mode.
You can paste the snippets into a Python shell at the repo root.

## 1. The idea in one minute

Normally `PublicationNode` calls `publisher.publish(data)` inline on the compute
thread — a slow broker stalls the sweep. With async egress, `publish()` instead
**appends to a write-ahead log (WAL)** and returns; a background drainer writes to
the broker in order, retrying on failure, and acks each record only once the broker
accepts it. The compute thread never blocks on the network.

## 2. See the WAL by itself

```python
from core.egress.wal import create_wal

wal = create_wal("filelog", "demo", {"path": "/tmp/dy_demo_wal", "fsync": "always"})
o1 = wal.append({"side": "BUY",  "qty": 100})
o2 = wal.append({"side": "SELL", "qty": 100})
print(wal.read_next(10))         # [(1, {'side':'BUY',...}), (2, {'side':'SELL',...})]
wal.ack(1)                       # commit the BUY
print(wal.committed_offset())    # 1
wal.close()
```

Reopen it and note that committed state is durable and reading resumes after the
last ack — this is what makes crash recovery work:

```python
wal = create_wal("filelog", "demo", {"path": "/tmp/dy_demo_wal"})
print(wal.committed_offset())    # 1  (survived the close)
print(wal.read_next(10))         # [(2, {'side':'SELL',...})]  resumes after BUY
wal.close()
```

Backends are interchangeable: swap `"filelog"` for `"sqlite"` (also pure stdlib) or
`"memory"` (fast, no durability). No LMDB required.

## 3. Wrap a publisher (non-blocking + ordered)

Any object with `publish(data)` can be wrapped. Here a toy publisher stands in for a
real Kafka/Redis connector:

```python
from core.egress.async_publisher import AsyncPublisher

class ToyBroker:
    name = "orders_topic"
    def __init__(self): self.sent = []
    def publish(self, data): self.sent.append(data)

broker = ToyBroker()
pub = AsyncPublisher(broker, {"backend": "memory"}, name="orders_topic")

for i in range(5):
    pub.publish({"seq": i})      # returns immediately (append-only)

import time; time.sleep(0.1)     # let the drainer run
print([m["seq"] for m in broker.sent])   # [0, 1, 2, 3, 4]  - strict FIFO
print(pub.stats())               # written, committed_offset, connected, ...
pub.close()
```

## 4. Watch ordering survive a flaky broker

A channel **stops the line** on failure: it retries the *same* record before moving
on, so a Sell can never overtake its Buy even across reconnects. (`pump()` does one
bounded unit of work — handy to drive deterministically in a demo.)

```python
from core.egress.drainer import DestinationChannel
from core.egress.wal import MemoryWal

wal = MemoryWal()
sent, fails = [], {"n": 4}
def flaky(rec):
    if fails["n"] > 0:           # first 4 attempts "lose the connection"
        fails["n"] -= 1
        raise ConnectionError("broker down")
    sent.append(rec)

for i in range(6):
    wal.append({"seq": i})
ch = DestinationChannel("orders", wal, flaky, backoff_initial_ms=0, backoff_max_ms=0)
for _ in range(100):            # pump until drained (retries the stalled record)
    if not ch.pump():
        break
print([m["seq"] for m in sent]) # [0,1,2,3,4,5] - nothing lost, nothing reordered
```

In the running server you never call `pump()` yourself — a bounded pool of egress
worker threads does it. This snippet just makes the stop-the-line behaviour visible.

## 5. Turn it on for real (config)

In `config/application.yaml`:

```yaml
egress:
  async:
    enabled: "true"
    default: "true"      # default for publishers that don't set async_egress
  worker:
    count: 4             # bounded egress worker pool size (per process)
  wal:
    backend: filelog
    fsync: interval
  overflow:
    policy: block        # backpressure when the WAL is full
```

By default every eligible publisher runs through the WAL — no change to your DAG
definitions, and broker endpoints come from your existing publisher configs. You can
mix per publisher: set `async_egress: false` in a publisher's config to keep it
inline, or `true` to force it on. **In-memory destinations** (`mem://`, …) always
publish inline. With `enabled: "false"` (the default) behaviour is exactly as before.

## 6. It scales in worker mode

A whole DAG is pinned to one worker process, so its publishers, WALs, and egress
pool sit together in that process. Each worker drains its own WALs in parallel, and
WALs are namespaced by `(DAG, publisher)` — DAG names are universally unique, so a
DAG reopens and resumes its own WAL no matter which worker runs it. Effective egress
parallelism is `worker.count` × number of worker processes. See the dedicated
tutorial **"Async Publication: Single-Process vs Multiprocess Workers"** for a
side-by-side walkthrough.

## 7. What's next

This release ships a bounded egress worker pool (default 4), per-publisher opt-in
(in-memory destinations always inline), the portable WAL backends, per-destination
FIFO, auto-reconnect, and bounded/maintained WALs. A cross-process egress
supervisor, a management UI, the `lmdb` backend, and exactly-once are later phases —
see `docs/design/A5-async-egress-subsystem.md` and the *Async Egress* user guide.
