"""Tests for the in-memory rolling 24h distribution (per DAG, 120 buckets)."""
import time

import pytest
from fastapi.testclient import TestClient


def test_rolling_distribution_counts_and_distinct_cycles():
    from core.flow_recorder import _RollingDistribution
    d = _RollingDistribution()
    now = int(time.time() * 1000)
    # two cycles in the current bucket: cycle 1 (3 fires), cycle 2 (2 fires)
    evs = [{"dag_id": "d", "ts_ms": now, "cycle_id": 1} for _ in range(3)]
    evs += [{"dag_id": "d", "ts_ms": now, "cycle_id": 2} for _ in range(2)]
    d.update_many(evs)
    snap = d.snapshot("d", now_ms=now)
    assert len(snap["buckets"]) == 120
    assert snap["bucket_ms"] == 24 * 3600 * 1000 // 120
    assert sum(b["events"] for b in snap["buckets"]) == 5
    assert sum(b["cycles"] for b in snap["buckets"]) == 2     # 2 distinct cycles


def test_rolling_distribution_spreads_over_time():
    from core.flow_recorder import _RollingDistribution
    d = _RollingDistribution()
    now = int(time.time() * 1000)
    bw = d.bw
    # one cycle in each of 5 distinct (older) buckets
    evs = [{"dag_id": "d", "ts_ms": now - k * bw, "cycle_id": 100 + k}
           for k in range(5)]
    d.update_many(evs)
    snap = d.snapshot("d", now_ms=now)
    nonzero = [b for b in snap["buckets"] if b["events"] > 0]
    assert len(nonzero) == 5


def test_rolling_distribution_prunes_beyond_24h():
    from core.flow_recorder import _RollingDistribution
    d = _RollingDistribution()
    now = int(time.time() * 1000)
    # an event 30h ago should be pruned/aged out of the 24h snapshot
    d.update_many([{"dag_id": "d", "ts_ms": now - 30 * 3600 * 1000, "cycle_id": 1},
                   {"dag_id": "d", "ts_ms": now, "cycle_id": 2}])
    snap = d.snapshot("d", now_ms=now)
    assert sum(b["events"] for b in snap["buckets"]) == 1   # only the recent one


def test_distribution_endpoint_served_from_memory():
    from web.dishtayantra_webapp import DishtaYantraWebApp
    from core.flow_recorder import FLOW_RECORDER as rec
    c = TestClient(DishtaYantraWebApp.get_instance().app, follow_redirects=True)
    assert c.post("/login", data={"username": "admin",
                                  "password": "admin123"}).status_code == 200
    for cyc in range(1, 8):
        for n in ("a", "b"):
            rec.record(dag_id="distapi", node_id=n, inputs={}, output={"o": cyc},
                       targets=["b"], cycle_id=cyc)
    time.sleep(1.0)
    j = c.get("/api/flow/distapi/distribution").json()
    assert len(j["buckets"]) == 120
    assert sum(b["cycles"] for b in j["buckets"]) == 7
    assert sum(b["events"] for b in j["buckets"]) == 14
