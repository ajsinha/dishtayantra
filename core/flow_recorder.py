"""
Flow Recorder - the capture side of Flow Time-Travel
====================================================

Records the DAG's *change-log* so a time window can be replayed later. Because
the engine's equality gate only fires a node when its output actually changes,
the history of a DAG is exactly the ordered stream of "node fired: inputs ->
output at time T". That stream is what this module captures.

Design goals (in priority order):

1. **Zero cost when disabled.** The hot path begins with a single boolean read
   (`self._enabled`); when the feature is off the call returns immediately and
   adds no measurable overhead to compute.
2. **Never block the compute thread.** When enabled, recording does only
   in-memory work and a non-blocking ``put_nowait`` onto a bounded queue. A
   background daemon thread drains the queue and performs all I/O. If the queue
   is full the event is dropped and a counter is incremented - the compute
   thread is never stalled by a slow or stuck store.
3. **Never raise into the caller.** Like :func:`core.audit_log.audit`, any
   error here is swallowed and counted, never propagated into the engine.

Disable/enable is a runtime switch (:meth:`disable` / :meth:`enable`) so an
operator can turn capture off instantly under load without a restart.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import itertools
import json
import logging
import queue
import random
import threading
import time

logger = logging.getLogger(__name__)

DEFAULT_MAXSIZE = 100_000          # bounded queue depth (events)
DEFAULT_MAX_PAYLOAD_BYTES = 2_048  # per-side (input/output) JSON cap
DEFAULT_BATCH_MAX = 512            # store write batch size
DEFAULT_SAMPLE_RATE = 1.0          # 1.0 = record every fire


class _RollingDistribution:
    """Per-DAG rolling 24h histogram kept in memory so the UI can read the event
    distribution without scanning the flow DB.

    120 fixed 12-minute buckets per DAG, updated from the drain thread (off the
    compute path) as events are written. Tracks event count and distinct
    compute-cycle count per bucket. Reads are O(120) - serving the bottom-pane
    chart for many concurrent replays costs no database work.
    """

    BUCKETS = 120
    BUCKET_MS = 24 * 3600 * 1000 // 120     # 720000 ms = 12 minutes

    def __init__(self, buckets=BUCKETS, bucket_ms=BUCKET_MS):
        self.n = int(buckets)
        self.bw = int(bucket_ms)
        self._lock = threading.Lock()
        self._dags = {}        # dag -> {bucket_index: [events, cycles, last_cycle]}

    def update_many(self, events):
        """Fold a drain batch into the per-DAG buckets under one lock."""
        with self._lock:
            touched = set()
            for ev in events:
                ts = ev.get("ts_ms")
                if not ts:
                    continue
                dag = ev.get("dag_id") or "default"
                bidx = int(ts) // self.bw
                d = self._dags.setdefault(dag, {})
                e = d.get(bidx)
                if e is None:
                    e = [0, 0, None]
                    d[bidx] = e
                e[0] += 1
                c = ev.get("cycle_id")
                if c is not None and c != e[2]:   # cycle_ids are monotonic per
                    e[1] += 1                     # bucket -> exact distinct count
                    e[2] = c
                touched.add(dag)
            for dag in touched:                   # prune buckets older than 24h
                d = self._dags[dag]
                if len(d) > self.n + 8:
                    cutoff = max(d) - self.n
                    for k in [k for k in d if k <= cutoff]:
                        del d[k]

    def snapshot(self, dag, now_ms=None):
        """Dense last-24h buckets for ``dag`` aligned to fixed 12-min boundaries
        ending at NOW. Same shape as the SQL histogram."""
        now_ms = int(now_ms or time.time() * 1000)
        end = now_ms // self.bw
        start = end - self.n + 1
        out = []
        with self._lock:
            d = self._dags.get(dag, {})
            for i, bidx in enumerate(range(start, end + 1)):
                e = d.get(bidx)
                t0 = bidx * self.bw
                out.append({"i": i, "t0": t0, "t1": t0 + self.bw,
                            "events": e[0] if e else 0,
                            "cycles": e[1] if e else 0})
        return {"from": start * self.bw, "to": (end + 1) * self.bw,
                "bucket_ms": self.bw, "buckets": out}


class FlowRecorder:
    """Non-blocking, disable-able recorder of node-fire events.

    A single process-wide instance (``FLOW_RECORDER``) is shared by the engine.
    The store is injected (:meth:`set_store`) so the recorder stays agnostic of
    SQL / LMDB / Paimon / noop backends.
    """

    def __init__(self):
        self._enabled = False
        self._dag_overrides = {}   # dag_id -> bool; overrides the global flag
        self._store = None
        self._q = None
        self._thread = None
        self._stop = threading.Event()
        self._seq = itertools.count(1)
        self._lock = threading.Lock()
        self._dist = _RollingDistribution()   # in-memory rolling 24h distribution
        # tunables
        self._maxsize = DEFAULT_MAXSIZE
        self._max_payload_bytes = DEFAULT_MAX_PAYLOAD_BYTES
        self._batch_max = DEFAULT_BATCH_MAX
        self._sample_rate = DEFAULT_SAMPLE_RATE
        # counters (plain ints; GIL makes += safe enough for monitoring)
        self._fired = 0
        self._dropped = 0
        self._written = 0
        self._errors = 0

    # ----------------------------------------------------------------- config
    def configure(self, *, enabled=False, store=None, maxsize=DEFAULT_MAXSIZE,
                  max_payload_bytes=DEFAULT_MAX_PAYLOAD_BYTES,
                  batch_max=DEFAULT_BATCH_MAX, sample_rate=DEFAULT_SAMPLE_RATE):
        """Apply configuration. Safe to call once at startup."""
        self._maxsize = max(int(maxsize), 1)
        self._max_payload_bytes = max(int(max_payload_bytes), 64)
        self._batch_max = max(int(batch_max), 1)
        self._sample_rate = min(max(float(sample_rate), 0.0), 1.0)
        if store is not None:
            self._store = store
        if enabled:
            self.enable()
        return self

    def set_store(self, store):
        self._store = store

    # ------------------------------------------------------------- lifecycle
    def start(self):
        """Start the background drain thread (idempotent)."""
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            if self._q is None:
                self._q = queue.Queue(maxsize=self._maxsize)
            self._stop.clear()
            self._thread = threading.Thread(target=self._drain_loop,
                                            name="flow-recorder", daemon=True)
            self._thread.start()
            logger.info("Flow recorder drain thread started (maxsize=%d, "
                        "batch_max=%d, sample_rate=%.3f)", self._maxsize,
                        self._batch_max, self._sample_rate)

    def stop(self, flush=True, timeout=5.0):
        """Stop the drain thread; optionally flush the remaining queue."""
        self._enabled = False
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)
        if flush:
            self._flush_remaining()

    def enable(self, dag=None):
        """Turn capture on at runtime; starts the drain thread if needed.

        With no ``dag`` this flips the global switch (all DAGs). With a ``dag``
        it sets a per-DAG override so that one DAG records regardless of the
        global flag (and the global flag still governs every other DAG).
        """
        self.start()
        if dag:
            self._dag_overrides[str(dag)] = True
            logger.info("Flow recording ENABLED for dag=%s", dag)
        else:
            self._enabled = True
            logger.info("Flow recording ENABLED (all DAGs)")

    def disable(self, dag=None):
        """Turn capture off at runtime. Global (no ``dag``) makes the hot path a
        no-op immediately; per-DAG sets an override so just that DAG stops while
        others keep following the global flag. The drain thread keeps running to
        flush what is already queued."""
        if dag:
            self._dag_overrides[str(dag)] = False
            logger.info("Flow recording DISABLED for dag=%s", dag)
        else:
            self._enabled = False
            logger.info("Flow recording DISABLED (all DAGs)")

    def clear_dag_override(self, dag):
        """Remove a per-DAG override so the DAG follows the global flag again."""
        self._dag_overrides.pop(str(dag), None)

    def _effective_enabled(self, dag):
        """Whether capture is on for a specific dag: a per-DAG override wins,
        otherwise the global flag applies."""
        return self._dag_overrides.get(dag, self._enabled)

    def is_enabled(self, dag=None):
        return self._effective_enabled(str(dag)) if dag else self._enabled

    # --------------------------------------------------------------- hot path
    def record_node_fire(self, node):
        """Record one node-fire. Called from the engine's equality gate.

        Cheap gate first: when capture is globally off and there are no per-DAG
        overrides, this returns with effectively zero cost. Otherwise it resolves
        the node's DAG and honours any per-DAG override. Never blocks, never
        raises.
        """
        if not self._enabled and not self._dag_overrides:
            return
        try:
            dag = self._resolve_dag(node)
            if not self._effective_enabled(dag):
                return
            if self._sample_rate < 1.0 and random.random() >= self._sample_rate:
                return
            evt = self._event_from_node(node, dag)
            self._q.put_nowait(evt)
        except queue.Full:
            self._dropped += 1
        except Exception:  # noqa: BLE001 - recording must never break compute
            self._errors += 1

    @staticmethod
    def _resolve_dag(node):
        """Resolve a node's DAG id cheaply. Real engine nodes carry their DAG via
        self._graph (set by ComputeGraph.set_graph) and have no 'dag_id'
        attribute; prefer an explicit dag_id, else the graph name, else default."""
        g = getattr(node, "_graph", None)
        return str(getattr(node, "dag_id", None)
                   or (getattr(g, "name", None) if g is not None else None)
                   or "default")

    @staticmethod
    def _resolve_cycle(node):
        """Current compute-cycle id for this fire. The engine stamps
        graph._compute_cycle before each sweep, so every node fired in one sweep
        (one propagation wave) shares it. None when unavailable (e.g. the generic
        record() path or a node without a graph)."""
        g = getattr(node, "_graph", None)
        return getattr(g, "_compute_cycle", None) if g is not None else None

    def record(self, dag_id, node_id, inputs=None, output=None, targets=None,
               compute_us=None, cycle_id=None):
        """Generic entry point (tests / non-Node callers). Same guarantees.

        The hot path only takes a cheap snapshot and enqueues - all JSON
        serialization happens later on the drain thread, so the compute thread
        is never charged for it."""
        dag = str(dag_id or "default")
        if not self._effective_enabled(dag):
            return
        try:
            if self._sample_rate < 1.0 and random.random() >= self._sample_rate:
                return
            self._q.put_nowait({
                "dag_id": dag,
                "node_id": str(node_id),
                "seq": next(self._seq),
                "ts_ms": int(time.time() * 1000),
                "inputs": self._snap(inputs),
                "output": self._snap(output),
                "targets": list(targets or []),
                "compute_us": int(compute_us) if compute_us is not None else None,
                "cycle_id": cycle_id,
            })
            self._fired += 1
        except queue.Full:
            self._dropped += 1
        except Exception:  # noqa: BLE001
            self._errors += 1

    def _event_from_node(self, node, dag=None):
        """Snapshot a live Node cheaply (defensive). No JSON here - the drain
        thread serializes. Arrow batches become a tiny summary immediately so we
        never hold a reference to a large columnar buffer."""
        try:
            from core.dag.edge_value import ev_describe
            inp = ev_describe(getattr(node, "_input", None))
            out = ev_describe(getattr(node, "_output", None))
        except Exception:  # noqa: BLE001
            inp = getattr(node, "_input", None)
            out = getattr(node, "_output", None)
        targets = []
        try:
            targets = [e.to_node.name for e in getattr(node, "_outgoing_edges", [])]
        except Exception:  # noqa: BLE001
            pass
        self._fired += 1
        if dag is None:
            dag = self._resolve_dag(node)
        return {
            "dag_id": str(dag),
            "node_id": str(getattr(node, "name", "?")),
            "seq": next(self._seq),
            "ts_ms": int(time.time() * 1000),
            "inputs": self._snap(inp),
            "output": self._snap(out),
            "targets": targets,
            "compute_us": None,
            "cycle_id": self._resolve_cycle(node),
        }

    @staticmethod
    def _snap(value):
        """Cheap, shallow snapshot taken on the hot path. Dicts/lists are
        shallow-copied so a later mutation can't corrupt the captured event;
        everything else passes through by reference. Deliberately avoids deep
        copies and JSON - those costs belong on the drain thread."""
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, (list, tuple)):
            return list(value)
        return value

    def _bound(self, value):
        """JSON-encode and cap the payload (drain thread only). Oversize values
        keep a head sample + marker so one fat record can't blow the row size."""
        try:
            s = json.dumps(value, default=str)
        except Exception:  # noqa: BLE001
            s = json.dumps(str(value))
        if len(s) > self._max_payload_bytes:
            return json.dumps({
                "__truncated__": True,
                "bytes": len(s),
                "head": s[:self._max_payload_bytes],
            })
        return s

    def _to_row(self, evt):
        """Serialize a queued snapshot into a store row (drain thread)."""
        return {
            "dag_id": evt["dag_id"], "node_id": evt["node_id"],
            "seq": evt["seq"], "ts_ms": evt["ts_ms"],
            "inputs_json": self._bound(evt.get("inputs")),
            "output_json": self._bound(evt.get("output")),
            "targets_json": json.dumps(evt.get("targets") or []),
            "compute_us": evt.get("compute_us"),
            "cycle_id": evt.get("cycle_id"),
        }

    # ------------------------------------------------------------- drain side
    def _drain_loop(self):
        while not self._stop.is_set():
            batch = self._collect_batch()
            if batch:
                self._write(batch)
            elif not self._stop.is_set():
                time.sleep(0.02)

    def _collect_batch(self):
        batch = []
        try:
            first = self._q.get(timeout=0.2)
            batch.append(first)
        except queue.Empty:
            return batch
        except Exception:  # noqa: BLE001
            return batch
        while len(batch) < self._batch_max:
            try:
                batch.append(self._q.get_nowait())
            except queue.Empty:
                break
            except Exception:  # noqa: BLE001
                break
        return batch

    def _write(self, batch):
        if not self._store:
            return
        try:
            self._dist.update_many(batch)             # cheap in-memory roll-up
        except Exception:  # noqa: BLE001 - distribution must never break writes
            pass
        try:
            rows = [self._to_row(e) for e in batch]   # JSON work on drain thread
            self._store.write_batch(rows)
            self._written += len(rows)
        except Exception:  # noqa: BLE001 - a slow/broken store must not crash us
            self._errors += 1
            logger.exception("Flow store write_batch failed (%d events dropped)",
                             len(batch))

    def distribution(self, dag_id, now_ms=None):
        """In-memory rolling last-24h distribution for a DAG (120 x 12-min
        buckets). O(120), no DB - safe to read for many concurrent replays."""
        return self._dist.snapshot(str(dag_id or "default"), now_ms=now_ms)

    def _flush_remaining(self):
        if self._q is None:
            return
        drained = []
        try:
            while True:
                drained.append(self._q.get_nowait())
        except queue.Empty:
            pass
        except Exception:  # noqa: BLE001
            pass
        for i in range(0, len(drained), self._batch_max):
            self._write(drained[i:i + self._batch_max])

    # ------------------------------------------------------------------ stats
    def stats(self):
        return {
            "enabled": self._enabled,
            "dag_overrides": dict(self._dag_overrides),
            "queued": self._q.qsize() if self._q is not None else 0,
            "maxsize": self._maxsize,
            "fired": self._fired,
            "written": self._written,
            "dropped": self._dropped,
            "errors": self._errors,
            "sample_rate": self._sample_rate,
            "store": type(self._store).__name__ if self._store else None,
        }


# Process-wide singleton shared by the engine hot path.
FLOW_RECORDER = FlowRecorder()


def get_flow_recorder() -> FlowRecorder:
    return FLOW_RECORDER
