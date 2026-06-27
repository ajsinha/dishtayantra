"""Tests for the service-plane seam and JSON management contract (v5.28.0).

Covers:
  * the ServiceClient seam (LocalServiceClient identity);
  * the contract drift guard - every operation promised in SERVICE_OPERATIONS
    must actually appear in the served OpenAPI schema (so a promise that isn't
    wired fails the build rather than 404-ing a remote caller);
  * the JSON management API behaviour (info / dags / status / lifecycle / 404).
"""
import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient  # noqa: E402

from core.service.contract import (  # noqa: E402
    CAPABILITIES,
    SCHEMA_VERSION,
    SERVICE_OPERATIONS,
)
from core.version import APP_NAME, BUILD_DATE, VERSION  # noqa: E402


@pytest.fixture()
def admin_client():
    from web.dishtayantra_webapp import DishtaYantraWebApp
    webapp = DishtaYantraWebApp.get_instance()
    c = TestClient(webapp.app, follow_redirects=True)
    r = c.post("/login", data={"username": "admin", "password": "admin123"})
    assert r.status_code == 200
    return c


# ------------------------------------------------------------- local client
def test_local_client_info_matches_version():
    from core.service.client import LocalServiceClient
    info = LocalServiceClient(dag_server=None).info()
    assert info.name == APP_NAME
    assert info.version == VERSION
    assert info.build_date == BUILD_DATE
    assert info.schema_version == SCHEMA_VERSION
    assert set(info.capabilities) == set(CAPABILITIES)


def test_get_service_client_returns_local(admin_client):
    # Resolver returns the app.state local client; exercised indirectly via the
    # info endpoint returning this instance's version.
    r = admin_client.get("/api/service/info")
    assert r.status_code == 200
    assert r.json()["version"] == VERSION


# ----------------------------------------------------------- contract drift
def test_every_promised_operation_is_served():
    """SERVICE_OPERATIONS must all exist in the live OpenAPI schema."""
    from web.dishtayantra_webapp import DishtaYantraWebApp
    schema = DishtaYantraWebApp.get_instance().app.openapi()
    paths = schema.get("paths", {})
    missing = []
    for method, path, _role in SERVICE_OPERATIONS:
        node = paths.get(path)
        if node is None or method.lower() not in node:
            missing.append(f"{method} {path}")
    assert not missing, f"contract promises not served: {missing}"


# --------------------------------------------------------------- API behaviour
def test_info_endpoint_shape(admin_client):
    body = admin_client.get("/api/service/info").json()
    assert set(body) == {"name", "version", "build_date",
                         "schema_version", "capabilities"}
    assert "dag.lifecycle" in body["capabilities"]


def test_dags_and_status(admin_client):
    dags = admin_client.get("/api/service/dags").json()
    assert "dags" in dags and isinstance(dags["dags"], list)
    status = admin_client.get("/api/service/status").json()
    assert "dag_count" in status and "dags" in status


def test_lifecycle_roundtrip(admin_client):
    dags = admin_client.get("/api/service/dags").json()["dags"]
    if not dags:
        pytest.skip("no DAGs configured in this environment")
    name = dags[0]["name"]
    r = admin_client.post(f"/api/service/dag/{name}/start")
    assert r.status_code == 200 and r.json()["ok"] is True
    r = admin_client.post(f"/api/service/dag/{name}/stop")
    assert r.status_code == 200 and r.json()["ok"] is True


def test_unknown_dag_is_404(admin_client):
    r = admin_client.post("/api/service/dag/__no_such_dag__/start")
    assert r.status_code == 404
    assert r.json()["ok"] is False


def test_mutations_require_admin():
    from web.dishtayantra_webapp import DishtaYantraWebApp
    webapp = DishtaYantraWebApp.get_instance()
    anon = TestClient(webapp.app, follow_redirects=False)
    r = anon.post("/api/service/dag/whatever/start")
    # AuthGuards rejects non-admin (redirect to login or 401/403).
    assert r.status_code in (302, 303, 401, 403)
