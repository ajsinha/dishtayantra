"""Tests for service-plane Phase 3 (gateway proxy) and Phase 4 (fleet view).

The proxy's forwarding logic is exercised without a live second process: a
trusted server pointing at an unreachable dummy URL proves the whitelist gate
(allowed path attempts the hop -> 502; disallowed path -> 403 before any
network). The fleet overview is checked for the always-present local plane.
"""
import os

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("cryptography")

from fastapi.testclient import TestClient  # noqa: E402

os.environ.setdefault("DY_SECRET_KEY", "unit-test-secret-passphrase")

from core.version import VERSION  # noqa: E402
from routes.service_proxy_routes import _is_allowed  # noqa: E402


@pytest.fixture()
def admin_client():
    from web.dishtayantra_webapp import DishtaYantraWebApp
    webapp = DishtaYantraWebApp.get_instance()
    c = TestClient(webapp.app, follow_redirects=True)
    assert c.post("/login", data={"username": "admin",
                                  "password": "admin123"}).status_code == 200
    return c


# ---------------------------------------------------------------- whitelist
def test_proxy_whitelist_rules():
    assert _is_allowed("GET", "/api/flow/status")
    assert _is_allowed("GET", "/metrics/json")
    assert _is_allowed("POST", "/api/dag-designer/save")
    # host-scoped + auth surfaces must never be proxyable
    assert not _is_allowed("POST", "/api/workers/pool/start")
    assert not _is_allowed("POST", "/api/workers/dag/x/migrate")
    assert not _is_allowed("GET", "/api/users")
    assert not _is_allowed("GET", "/admin/trusted-servers")
    # method matters
    assert not _is_allowed("POST", "/api/flow/status")


# -------------------------------------------------------------------- proxy
def test_proxy_no_remote_target_is_conflict(admin_client):
    r = admin_client.get("/api/proxy/api/flow/status")
    assert r.status_code == 409  # nothing selected -> nothing to proxy


def test_proxy_gate_with_remote_target(admin_client):
    from web.dishtayantra_webapp import DishtaYantraWebApp
    reg = DishtaYantraWebApp.get_instance().app.state.trusted_registry
    if reg is None:
        pytest.skip("registry unavailable")
    sid = "dummy-unreachable"
    reg.add(sid, "Dummy", "http://127.0.0.1:9", "dyk_dummy",
            role="user", verify_tls=False, added_by="test", probe=False)
    try:
        assert admin_client.post("/service/select",
                                 data={"target": sid}).status_code == 200
        # disallowed path is refused before any network call
        assert admin_client.get(
            "/api/proxy/api/workers/pool/start").status_code == 403
        # allowed path passes the gate, then fails to reach the dummy -> 502
        assert admin_client.get(
            "/api/proxy/api/flow/status").status_code == 502
    finally:
        admin_client.post("/service/select", data={"target": "local"})
        reg.remove(sid, removed_by="test")


# -------------------------------------------------------------------- fleet
def test_fleet_overview_has_local(admin_client):
    body = admin_client.get("/api/fleet/overview").json()
    assert "planes" in body and body["planes"]
    local = [p for p in body["planes"] if p["id"] == "local"]
    assert local and local[0]["reachable"] is True
    assert local[0]["manageable"] is True
    assert local[0]["version"] == VERSION


def test_fleet_page_renders(admin_client):
    assert admin_client.get("/fleet").status_code == 200


def test_fleet_sse_stream_once(admin_client):
    """The SSE stream returns an event-stream and emits one overview with
    ?once=1 (the bounded mode used by curl/tests)."""
    r = admin_client.get("/api/fleet/stream?once=1")
    assert r.status_code == 200
    assert "text/event-stream" in r.headers.get("content-type", "")
    assert "data:" in r.text
    assert "local" in r.text  # local plane present in the pushed overview


def test_down_server_shows_unreachable(admin_client):
    """A registered-but-down server appears as an explicit unreachable tile,
    and registry.health() splits reachable from manageable without raising."""
    from web.dishtayantra_webapp import DishtaYantraWebApp
    reg = DishtaYantraWebApp.get_instance().app.state.trusted_registry
    if reg is None:
        pytest.skip("registry unavailable")
    sid = "down-server"
    reg.add(sid, "Down", "http://127.0.0.1:9", "dyk_x", role="user",
            verify_tls=False, added_by="test", probe=False)
    try:
        # registry.health never raises and reports the down server as not
        # reachable (hence not manageable).
        h = reg.health(sid, timeout=2.0)
        assert h["reachable"] is False
        assert h["manageable"] is False
        assert h["error"]
        # and the fleet overview surfaces it as a tile rather than failing.
        planes = admin_client.get("/api/fleet/overview").json()["planes"]
        tile = [p for p in planes if p["id"] == sid]
        assert tile and tile[0]["reachable"] is False
        # the local plane is unaffected by the down peer (no partial-success lie)
        local = [p for p in planes if p["id"] == "local"][0]
        assert local["reachable"] is True and local["manageable"] is True
    finally:
        reg.remove(sid, removed_by="test")
