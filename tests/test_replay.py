"""Tests for the flow-log replay & recovery foundation (core/replay.py).

All read-only over a real SqliteFlowStore; no engine involved.
"""
import json
import os
import tempfile

from core.flow_store import SqliteFlowStore
from core.replay import (ReplaySource, event_is_faithful, recovery_seed,
                         recovery_seed_detail)


def _store():
    p = os.path.join(tempfile.mkdtemp(), "flow.db")
    return SqliteFlowStore(p, provenance={"instance": "A", "host": "h", "port": 1})


def _row(node, seq, ts, out, inp=None, trunc=False):
    o = {"__truncated__": True, "sample": "x"} if trunc else out
    return {
        "dag_id": "etl", "node_id": node, "seq": seq, "ts_ms": ts,
        "inputs_json": json.dumps(inp or {"v": seq}),
        "output_json": json.dumps(o),
        "targets_json": None, "compute_us": 1,
    }


def test_replaysource_streams_in_order():
    s = _store()
    s.write_batch([_row("src", 1, 1000, {"px": 10}),
                   _row("calc", 2, 1001, {"px": 20}),
                   _row("src", 3, 1002, {"px": 30})])
    seqs = [e["seq"] for e in ReplaySource(s, "etl").events()]
    assert seqs == [1, 2, 3]


def test_replaysource_filters_window_and_nodes():
    s = _store()
    s.write_batch([_row("src", 1, 1000, {"a": 1}),
                   _row("calc", 2, 2000, {"a": 2}),
                   _row("src", 3, 3000, {"a": 3})])
    only_src = list(ReplaySource(s, "etl", nodes=["src"]).events())
    assert {e["node_id"] for e in only_src} == {"src"}
    windowed = list(ReplaySource(s, "etl", t0_ms=1500, t1_ms=2500).events())
    assert [e["seq"] for e in windowed] == [2]


def test_fidelity_report_flags_truncation():
    s = _store()
    s.write_batch([_row("src", 1, 1000, {"ok": 1}),
                   _row("big", 2, 1001, None, trunc=True)])
    rep = ReplaySource(s, "etl").fidelity_report()
    assert rep["events"] == 2
    assert rep["faithful"] == 1
    assert rep["truncated"] == 1
    assert rep["truncated_nodes"] == ["big"]
    assert rep["fully_faithful"] is False


def test_event_is_faithful():
    assert event_is_faithful({"inputs": {"a": 1}, "output": {"b": 2}})
    assert not event_is_faithful({"inputs": {"a": 1},
                                  "output": {"__truncated__": True}})


def test_recovery_seed_latest_output_per_node():
    s = _store()
    # src fires twice; the later value must win
    s.write_batch([_row("src", 1, 1000, {"px": 10}),
                   _row("src", 2, 2000, {"px": 99}),
                   _row("calc", 3, 1500, {"y": 5})])
    seed = recovery_seed(s, "etl")
    assert seed == {"src": {"px": 99}, "calc": {"y": 5}}


def test_recovery_seed_detail_flags_truncated_node():
    s = _store()
    s.write_batch([_row("src", 1, 1000, {"px": 10}),
                   _row("big", 2, 2000, None, trunc=True)])
    d = recovery_seed_detail(s, "etl")
    assert d["nodes"]["src"]["faithful"] is True
    assert d["nodes"]["big"]["faithful"] is False
    assert d["nodes"]["src"]["seq"] == 1
