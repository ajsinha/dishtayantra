"""Flow-log replay & recovery foundation (read-only over the flow store).

Slice 1 of ``docs/design/replay-and-recovery.md``: turn the recorded change-log
into (a) an ordered event stream for replay and (b) a per-node latest-output map
for warm-start recovery. This module is **fully offline and side-effect-free** -
it only reads the flow store; it never touches the engine, the network, or any
live node state. Later slices feed (a) through a sandbox ``ComputeGraph`` and seed
live state from (b).

Fidelity note: the recorder caps payloads at ``max_payload_bytes`` and replaces an
oversized value with ``{"__truncated__": True, ...}``. A truncated payload cannot
be faithfully replayed or restored, so this module exposes helpers to detect and
report truncation rather than silently trusting it.
"""

import time

TRUNCATED_KEY = "__truncated__"


def _is_truncated(value):
    return isinstance(value, dict) and value.get(TRUNCATED_KEY) is True


def event_is_faithful(ev):
    """True if neither the input nor the output payload was capped at capture
    time. Replay can only faithfully reproduce faithful events."""
    return not (_is_truncated(ev.get("inputs")) or _is_truncated(ev.get("output")))


class ReplaySource:
    """Streams a recorded window for a DAG as ordered events.

    Read-only: pulls from any ``FlowStore`` via ``iter_export`` (memory-bounded),
    with optional filtering by node set, ``instance`` (origin, for shared flow
    DBs), and time window. Events arrive in ``seq`` order (seq is monotonic with
    capture time). This is the input "tape" a replay runner will later feed into a
    sandbox graph; on its own it changes nothing.
    """

    def __init__(self, store, dag_id, t0_ms=None, t1_ms=None, nodes=None,
                 instance=None, chunk=2000):
        self.store = store
        self.dag_id = dag_id
        self.t0_ms = t0_ms
        self.t1_ms = t1_ms
        self.nodes = list(nodes) if nodes else None
        self.instance = instance
        self.chunk = chunk

    def events(self):
        """Yield events in ``seq`` order. Each event is the store's dict shape:
        ``dag_id, node_id, seq, ts_ms, inputs, output, targets, compute_us,
        instance, host, port``."""
        it = getattr(self.store, "iter_export", None)
        if callable(it):
            yield from self.store.iter_export(
                self.dag_id, self.t0_ms, self.t1_ms, self.nodes,
                chunk=self.chunk, instance=self.instance)
        else:  # minimal stores: bounded single query
            for ev in self.store.query(self.dag_id, self.t0_ms, self.t1_ms,
                                       self.nodes, limit=10_000_000,
                                       instance=self.instance):
                yield ev

    def fidelity_report(self):
        """Scan the window once and report faithful vs truncated events, so a
        caller can decide whether a replay over this window will be accurate."""
        total = faithful = 0
        truncated_nodes = set()
        for ev in self.events():
            total += 1
            if event_is_faithful(ev):
                faithful += 1
            else:
                truncated_nodes.add(ev.get("node_id"))
        return {
            "dag_id": self.dag_id,
            "events": total,
            "faithful": faithful,
            "truncated": total - faithful,
            "truncated_nodes": sorted(n for n in truncated_nodes if n),
            "fully_faithful": total == faithful,
        }


def recovery_seed(store, dag_id, ts_ms=None, instance=None):
    """Per-node latest output at or before ``ts_ms`` (default: now) - the
    warm-start seed for log-based recovery. Returns ``{node_id: output}``.

    Read-only. Nodes that never fired are absent (they start cold, exactly as
    today). The caller (a later slice, behind ``flow_recorder.recover_on_start``)
    seeds ``node._output`` from this map; it never re-fires nodes.
    """
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)
    snap = store.state_at(dag_id, ts_ms, instance=instance) or {}
    nodes = snap.get("nodes", {}) if isinstance(snap, dict) else {}
    return {nid: row.get("output") for nid, row in nodes.items()}


def recovery_seed_detail(store, dag_id, ts_ms=None, instance=None):
    """Like :func:`recovery_seed` but keeps ``ts_ms``/``seq`` per node and flags
    any node whose seed payload was truncated at capture (unreliable to restore -
    such a node should be left cold rather than seeded with a partial value)."""
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)
    snap = store.state_at(dag_id, ts_ms, instance=instance) or {}
    nodes = snap.get("nodes", {}) if isinstance(snap, dict) else {}
    out = {}
    for nid, row in nodes.items():
        out[nid] = {
            "output": row.get("output"),
            "ts_ms": row.get("ts_ms"),
            "seq": row.get("seq"),
            "faithful": not _is_truncated(row.get("output")),
        }
    return {"dag_id": dag_id, "ts_ms": int(ts_ms), "nodes": out}
