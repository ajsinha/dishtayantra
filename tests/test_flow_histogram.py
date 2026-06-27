"""Tests for the replay distribution histogram (/api/flow/{dag}/histogram)."""
import time

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def admin_client():
    from web.dishtayantra_webapp import DishtaYantraWebApp
    c = TestClient(DishtaYantraWebApp.get_instance().app, follow_redirects=True)
    assert c.post("/login", data={"username": "admin",
                                  "password": "admin123"}).status_code == 200
    return c


def _seed_spread(store, dag, n_cycles, t0, t1):
    """Write n_cycles compute cycles (2 fires each) spread across [t0, t1)."""
    rows = []
    span = t1 - t0
    for c in range(1, n_cycles + 1):
        ts = t0 + int((c - 0.5) / n_cycles * span)
        for node in ("a", "b"):
            rows.append({"dag_id": dag, "node_id": node, "seq": len(rows) + 1,
                         "ts_ms": ts, "inputs_json": None, "output_json": None,
                         "targets_json": None, "compute_us": None, "cycle_id": c})
    store.write_batch(rows)


def test_histogram_counts_and_distribution(admin_client):
    from web.dishtayantra_webapp import DishtaYantraWebApp
    store = DishtaYantraWebApp.get_instance().flow_store
    now = int(time.time() * 1000)
    t0 = now - 3600 * 1000                       # 1-hour window
    _seed_spread(store, "histtest", 12, t0, now)

    j = admin_client.get(
        f"/api/flow/histtest/histogram?from={t0}&to={now}&buckets=12").json()
    b = j["buckets"]
    assert len(b) == 12
    assert sum(x["cycles"] for x in b) == 12     # 12 distinct cycles total
    assert sum(x["events"] for x in b) == 24     # 2 fires each
    # the cycles are spread across the window, not piled into one bucket
    assert len([x for x in b if x["cycles"] > 0]) >= 6
    assert j["from"] == t0 and j["to"] == now


def test_histogram_window_capped_to_24h(admin_client):
    now = int(time.time() * 1000) + 5000
    j = admin_client.get(f"/api/flow/histtest/histogram?from=0&to={now}").json()
    assert 0 < (j["to"] - j["from"]) <= 24 * 3600 * 1000 + 1000
    assert len(j["buckets"]) == 120              # default bucket count


def test_histogram_requires_login():
    from web.dishtayantra_webapp import DishtaYantraWebApp
    anon = TestClient(DishtaYantraWebApp.get_instance().app,
                      follow_redirects=False)
    r = anon.get("/api/flow/histtest/histogram?from=0&to=1")
    assert r.status_code in (401, 403)
