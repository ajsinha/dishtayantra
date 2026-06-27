"""Tests for the Flow Time-Travel recorder and stores.

These lock in the behaviours that matter operationally:
  * disabling is a true no-op (the kill switch),
  * the hot path never blocks and never raises into compute,
  * nothing is lost when the store keeps up,
  * overflow is shed as *counted drops*, not errors or exceptions,
  * the SQLite store round-trips range queries / state reconstruction / export,
  * the store factory has no silent fallback.

They are deliberately timing-tolerant: we drain deterministically via
``stop(flush=True)`` rather than sleeping and hoping.
"""
import time

import pytest

from core.flow_recorder import FlowRecorder
from core.flow_store import (
    make_flow_store,
    NoopFlowStore,
    SqliteFlowStore,
)


# --------------------------------------------------------------------- doubles
class CollectingStore:
    """Captures every row written, so tests can assert on persistence."""

    def __init__(self):
        self.rows = []

    def write_batch(self, events):
        self.rows.extend(events)


class SlowStore:
    """Simulates a store that cannot keep up (each batch blocks)."""

    def __init__(self, delay=0.01):
        self.delay = delay
        self.rows = []

    def write_batch(self, events):
        time.sleep(self.delay)
        self.rows.extend(events)


class BrokenStore:
    """Always raises on write - must be absorbed, never propagated."""

    def write_batch(self, events):
        raise RuntimeError("store is down")


def _make_node(name, inp, out, targets=None, dag_id="d1"):
    """Minimal stand-in for a DAG Node, duck-typed exactly like a real engine
    node: it carries its DAG via ._graph.name (set by ComputeGraph.set_graph),
    NOT a 'dag_id' attribute (real nodes have none)."""
    n = type("N", (), {})()
    n.name = name
    n._graph = type("G", (), {"name": dag_id})() if dag_id is not None else None
    n._input = inp
    n._output = out
    n._outgoing_edges = [
        type("E", (), {"to_node": type("T", (), {"name": t})()})()
        for t in (targets or [])
    ]
    return n


# ----------------------------------------------------------------- kill switch
def test_disabled_is_a_true_noop():
    """When disabled, recording persists nothing and touches no store."""
    store = CollectingStore()
    rec = FlowRecorder().configure(enabled=False, store=store)
    # never started, never enabled
    for i in range(1000):
        rec.record("d1", "n1", inputs={"i": i}, output={"o": i})
    assert rec.is_enabled() is False
    assert store.rows == []
    assert rec.stats()["written"] == 0


def test_runtime_enable_then_disable():
    """enable() captures; disable() stops capturing immediately."""
    store = CollectingStore()
    rec = FlowRecorder().configure(store=store)
    rec.enable()
    for i in range(50):
        rec.record("d1", "n1", inputs={"i": i}, output={"o": i})
    rec.disable()
    after_disable = len(store.rows)  # may still be draining; capture count
    for i in range(50):
        rec.record("d1", "n1", inputs={"i": i}, output={"o": i})
    rec.stop(flush=True)
    # the 50 post-disable records must not have been captured
    assert rec.stats()["written"] <= 50
    assert after_disable <= 50


# ------------------------------------------------------------------- no loss
def test_no_loss_under_sustainable_load():
    store = CollectingStore()
    rec = FlowRecorder().configure(enabled=True, store=store, maxsize=10000)
    n = 2000
    for i in range(n):
        rec.record("d1", "n%d" % (i % 4), inputs={"i": i}, output={"o": i})
    rec.stop(flush=True)
    s = rec.stats()
    assert s["written"] == n, s
    assert s["dropped"] == 0, s
    assert s["errors"] == 0, s
    assert len(store.rows) == n


def test_node_fire_path_records():
    store = CollectingStore()
    rec = FlowRecorder().configure(enabled=True, store=store)
    rec.record_node_fire(_make_node("validate", {"raw": 1}, {"ok": True},
                                    targets=["fx"]))
    rec.stop(flush=True)
    assert len(store.rows) == 1
    assert store.rows[0]["node_id"] == "validate"
    assert store.rows[0]["dag_id"] == "d1"


def test_node_fire_uses_graph_name_for_dag_id():
    """The engine hook tags events with the node's DAG name (from ._graph.name),
    since real nodes have no dag_id attribute."""
    store = CollectingStore()
    rec = FlowRecorder().configure(enabled=True, store=store)
    rec.record_node_fire(_make_node("enrich", {"a": 1}, {"b": 2},
                                    dag_id="trade-etl"))
    rec.stop(flush=True)
    assert store.rows[0]["dag_id"] == "trade-etl"


def test_node_fire_without_graph_defaults():
    store = CollectingStore()
    rec = FlowRecorder().configure(enabled=True, store=store)
    rec.record_node_fire(_make_node("x", {"a": 1}, {"b": 2}, dag_id=None))
    rec.stop(flush=True)
    assert store.rows[0]["dag_id"] == "default"


def test_per_dag_disable_while_global_on():
    """Global on, one DAG disabled -> that DAG stops, others keep recording."""
    store = CollectingStore()
    rec = FlowRecorder().configure(enabled=True, store=store)
    rec.disable(dag="noisy")
    rec.record_node_fire(_make_node("n", {"a": 1}, {"b": 2}, dag_id="noisy"))
    rec.record_node_fire(_make_node("n", {"a": 1}, {"b": 2}, dag_id="keep"))
    rec.stop(flush=True)
    dags = {r["dag_id"] for r in store.rows}
    assert "keep" in dags and "noisy" not in dags


def test_per_dag_enable_while_global_off():
    """Global off, one DAG enabled -> only that DAG records."""
    store = CollectingStore()
    rec = FlowRecorder().configure(enabled=False, store=store)
    rec.enable(dag="wanted")
    rec.record_node_fire(_make_node("n", {"a": 1}, {"b": 2}, dag_id="wanted"))
    rec.record_node_fire(_make_node("n", {"a": 1}, {"b": 2}, dag_id="other"))
    rec.stop(flush=True)
    dags = {r["dag_id"] for r in store.rows}
    assert "wanted" in dags and "other" not in dags


def test_status_reports_dag_overrides_and_clear():
    rec = FlowRecorder().configure(enabled=True, store=CollectingStore())
    rec.disable(dag="x")
    assert rec.stats()["dag_overrides"] == {"x": False}
    assert rec.is_enabled() is True and rec.is_enabled("x") is False
    rec.clear_dag_override("x")
    assert rec.stats()["dag_overrides"] == {}
    assert rec.is_enabled("x") is True
    rec.stop()


# --------------------------------------------------------------- overload safety
def test_overload_sheds_drops_without_blocking_or_raising():
    """A stuck store must not stall the producer; overflow becomes drops."""
    store = SlowStore(delay=0.02)
    rec = FlowRecorder().configure(enabled=True, store=store, maxsize=100)
    t0 = time.time()
    n = 5000
    for i in range(n):
        # must never raise into the caller even when the queue is jammed
        rec.record("d1", "n1", inputs={"i": i}, output={"o": i})
    produce_secs = time.time() - t0
    # 5000 enqueue attempts against a 100-deep queue + 20ms/batch store
    # should still finish fast - the producer is never blocked on the store.
    assert produce_secs < 2.0, produce_secs
    s = rec.stats()
    assert s["errors"] == 0, s
    assert s["dropped"] > 0, s            # overflow was shed
    assert s["dropped"] + s["queued"] + s["written"] <= n
    rec.stop(flush=False)


def test_broken_store_never_propagates():
    """Store exceptions are absorbed and counted, never raised into compute."""
    rec = FlowRecorder().configure(enabled=True, store=BrokenStore())
    for i in range(200):
        rec.record("d1", "n1", inputs={"i": i}, output={"o": i})  # no raise
    rec.stop(flush=True)
    s = rec.stats()
    assert s["written"] == 0
    assert s["errors"] >= 1, s            # failures surfaced as a counter


# ------------------------------------------------------------------- sampling
def test_sample_rate_zero_records_nothing():
    store = CollectingStore()
    rec = FlowRecorder().configure(enabled=True, store=store, sample_rate=0.0)
    for i in range(500):
        rec.record("d1", "n1", inputs={"i": i}, output={"o": i})
    rec.stop(flush=True)
    assert rec.stats()["written"] == 0
    assert store.rows == []


# ------------------------------------------------------- payload bounding
def test_oversize_payload_is_bounded():
    store = CollectingStore()
    rec = FlowRecorder().configure(enabled=True, store=store,
                                   max_payload_bytes=128)
    big = {"blob": "x" * 10000}
    rec.record("d1", "n1", inputs=big, output=big)
    rec.stop(flush=True)
    assert len(store.rows) == 1
    # the stored JSON must be capped, with a truncation marker
    out = store.rows[0]["output_json"]
    assert "__truncated__" in out
    assert len(out) < 1024


# ----------------------------------------------------- sqlite store correctness
def test_sqlite_store_roundtrip(tmp_path):
    db = str(tmp_path / "flow.db")
    store = SqliteFlowStore(db)
    base = int(time.time() * 1000)
    rows = []
    for i in range(10):
        rows.append({
            "dag_id": "d1", "node_id": "n%d" % (i % 3), "seq": i + 1,
            "ts_ms": base + i * 1000,
            "inputs_json": '{"i": %d}' % i, "output_json": '{"o": %d}' % i,
            "targets_json": "[]", "compute_us": i,
        })
    store.write_batch(rows)

    # full range
    all_rows = store.query("d1")
    assert len(all_rows) == 10

    # bounded range [t2, t5] inclusive
    win = store.query("d1", t0_ms=base + 2000, t1_ms=base + 5000)
    assert {r["seq"] for r in win} == {3, 4, 5, 6}

    # node filter
    only_n0 = store.query("d1", nodes=["n0"])
    assert all(r["node_id"] == "n0" for r in only_n0)

    # pagination via after_seq
    page = store.query("d1", after_seq=8)
    assert {r["seq"] for r in page} == {9, 10}

    # state reconstruction: latest output per node at/<= a time
    st = store.state_at("d1", base + 4000)
    assert set(st["nodes"].keys()) == {"n0", "n1", "n2"}

    # export generator yields everything
    assert sum(1 for _ in store.iter_export("d1")) == 10

    # distinct dags: returns per-dag metadata for the UI dropdown
    dags = store.distinct_dags()
    assert len(dags) == 1
    assert dags[0]["dag_id"] == "d1"
    assert dags[0]["count"] == 10
    assert dags[0]["min_ts"] == base and dags[0]["max_ts"] == base + 9000


def test_sqlite_purge_by_time(tmp_path):
    db = str(tmp_path / "flow.db")
    store = SqliteFlowStore(db)
    base = 1_000_000
    store.write_batch([{
        "dag_id": "d1", "node_id": "n1", "seq": i + 1,
        "ts_ms": base + i * 1000, "inputs_json": "{}", "output_json": "{}",
        "targets_json": "[]", "compute_us": None,
    } for i in range(10)])
    # purge everything strictly older than the 5th event
    removed = store.purge_older_than_ms(base + 5000)
    assert removed == 5
    assert len(store.query("d1")) == 5


# ----------------------------------------------------------- store factory
def test_make_flow_store_has_no_silent_fallback():
    assert isinstance(make_flow_store("noop"), NoopFlowStore)
    with pytest.raises(ValueError):
        make_flow_store("does-not-exist")


def test_recorder_stats_shape():
    rec = FlowRecorder().configure(store=NoopFlowStore())
    s = rec.stats()
    for key in ("enabled", "queued", "maxsize", "fired", "written",
                "dropped", "errors", "sample_rate", "store"):
        assert key in s
