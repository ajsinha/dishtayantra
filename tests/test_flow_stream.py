"""Tests for the SSE flow replay stream endpoint (/api/flow/{dag}/stream)."""
import re
import time

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def admin_client():
    from web.dishtayantra_webapp import DishtaYantraWebApp
    webapp = DishtaYantraWebApp.get_instance()
    c = TestClient(webapp.app, follow_redirects=True)
    assert c.post("/login", data={"username": "admin",
                                  "password": "admin123"}).status_code == 200
    return c


def _seed(dag, n):
    from core.flow_recorder import FLOW_RECORDER as rec
    for i in range(n):
        rec.record(dag_id=dag, node_id="n%d" % (i % 4),
                   inputs={"i": i}, output={"o": i}, targets=["nx"])
    time.sleep(1.0)  # let the drain thread flush to the store


def test_stream_batches_and_done(admin_client):
    _seed("stream_dag", 2300)
    now = int(time.time() * 1000) + 5000
    r = admin_client.get(f"/api/flow/stream_dag/stream?from=0&to={now}&batch=1000")
    assert r.status_code == 200
    assert "text/event-stream" in r.headers.get("content-type", "")
    body = r.text
    # 2300 events at batch=1000 -> 3 batch messages + 1 done
    assert body.count("event: batch") == 3
    assert body.count("event: done") == 1
    done = re.findall(r"event: done\ndata: (\{.*\})", body)
    assert done and '"total": 2300' in done[-1]


def test_stream_window_capped_to_24h(admin_client):
    # from far in the past -> server clamps the window to 24h (no error)
    now = int(time.time() * 1000) + 5000
    r = admin_client.get(f"/api/flow/stream_dag/stream?from=0&to={now}&batch=5000")
    assert r.status_code == 200
    done = re.findall(r'"from": (\d+), "to": (\d+)', r.text)
    assert done
    frm, to = int(done[-1][0]), int(done[-1][1])
    assert 0 < (to - frm) <= 24 * 3600 * 1000 + 1000


def test_stream_requires_login():
    from web.dishtayantra_webapp import DishtaYantraWebApp
    anon = TestClient(DishtaYantraWebApp.get_instance().app,
                      follow_redirects=False)
    r = anon.get("/api/flow/stream_dag/stream?from=0&to=1")
    assert r.status_code in (401, 403)


def test_stream_concurrency_cap_emits_busy(admin_client):
    """When all replay slots are taken, a new replay gets a clear busy notice
    instead of piling more load on the web tier."""
    from web.dishtayantra_webapp import DishtaYantraWebApp
    fr = DishtaYantraWebApp.get_instance().flow_routes
    sem = fr._stream_sem
    held = 0
    while sem.acquire(blocking=False):
        held += 1
    try:
        now = int(time.time() * 1000) + 5000
        r = admin_client.get(f"/api/flow/stream_dag/stream?from=0&to={now}")
        assert r.status_code == 200
        assert "Server is busy" in r.text
        assert "event: batch" not in r.text     # no data streamed when busy
    finally:
        for _ in range(held):
            sem.release()


def test_stream_duration_cap_configured(admin_client):
    from web.dishtayantra_webapp import DishtaYantraWebApp
    fr = DishtaYantraWebApp.get_instance().flow_routes
    assert fr._stream_max_seconds >= 5            # bounded, not unlimited
    assert fr._stream_sem._initial_value >= 1     # bounded concurrency


def test_stream_groups_by_complete_cycles(admin_client):
    """Each push carries only WHOLE compute cycles, in order - never a partial
    cycle - so the UI render is always consistent."""
    import json
    from core.flow_recorder import FLOW_RECORDER as rec
    for cyc in range(1, 11):                       # 10 cycles x 3 fires
        for n in ("a", "b", "c"):
            rec.record(dag_id="cyc_stream", node_id=n, inputs={},
                       output={"o": cyc}, targets=["c"], cycle_id=cyc)
    time.sleep(1.0)
    now = int(time.time() * 1000) + 5000
    r = admin_client.get(f"/api/flow/cyc_stream/stream?from=0&to={now}&cycles=4")
    pushes = re.findall(r"event: batch\ndata: (\{.*\})", r.text)
    seen = []
    for p in pushes:
        d = json.loads(p)
        cset = sorted({e["cycle_id"] for e in d["events"]})
        # every event in a push belongs to one of that push's complete cycles
        assert len(d["events"]) == len(cset) * 3
        seen.extend(cset)
    assert seen == list(range(1, 11))             # all cycles, in order, once


def test_status_reports_replay_capacity(admin_client):
    j = admin_client.get("/api/flow/status").json()
    assert "active_streams" in j and "max_streams" in j
    assert j["max_streams"] >= 1
    assert j["active_streams"] >= 0


def test_slot_released_after_stream_completes(admin_client):
    """A finished replay releases its concurrency slot (the generator's finally)."""
    from web.dishtayantra_webapp import DishtaYantraWebApp
    fr = DishtaYantraWebApp.get_instance().flow_routes
    now = int(time.time() * 1000) + 5000
    admin_client.get(f"/api/flow/stream_dag/stream?from=0&to={now}")  # fully consumed
    # all slots free again
    assert fr._stream_sem._value == fr._stream_sem._initial_value
    assert admin_client.get("/api/flow/status").json()["active_streams"] == 0


def test_slot_released_on_client_disconnect(admin_client):
    """Stopping mid-stream (client closes) frees the slot server-side."""
    import time as _t
    from web.dishtayantra_webapp import DishtaYantraWebApp
    fr = DishtaYantraWebApp.get_instance().flow_routes
    now = int(time.time() * 1000) + 5000
    url = f"/api/flow/stream_dag/stream?from=0&to={now}&cycles=1"
    with admin_client.stream("GET", url) as r:
        for _ in r.iter_lines():        # read one line, then disconnect early
            break
    # server should detect the disconnect and release the slot
    for _ in range(50):
        if fr._stream_sem._value == fr._stream_sem._initial_value:
            break
        _t.sleep(0.05)
    assert fr._stream_sem._value == fr._stream_sem._initial_value



