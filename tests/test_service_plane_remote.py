"""Tests for service-plane Phase 2 (v5.29.0): remote flavour, trusted-server
registry, at-rest crypto, and the per-session switch.

The remote hop is exercised in-process by pointing a RestServiceClient at the
app's own JSON management API through a TestClient-backed transport - real HTTP
semantics (headers, Bearer auth, on-behalf, OpResult parsing, 404->ValueError)
without a second process.
"""
import os

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("cryptography")

from fastapi.testclient import TestClient  # noqa: E402

os.environ.setdefault("DY_SECRET_KEY", "unit-test-secret-passphrase")

from core.service import crypto  # noqa: E402
from core.service.errors import ServiceAuthError  # noqa: E402
from core.service.rest_client import RestServiceClient  # noqa: E402
from core.version import VERSION  # noqa: E402


def _webapp():
    from web.dishtayantra_webapp import DishtaYantraWebApp
    return DishtaYantraWebApp.get_instance()


@pytest.fixture()
def admin_key():
    """Mint a real dyk_ admin API key for remote-auth tests."""
    _webapp()  # ensure DB/users bootstrapped
    from core.db.dao import ApiKeyDAO
    clear, _rec = ApiKeyDAO().create_api_key("admin", "phase2-test", "test")
    return clear


@pytest.fixture()
def transport():
    """A TestClient usable as a RestServiceClient HTTP transport (no session)."""
    return TestClient(_webapp().app, follow_redirects=False)


# --------------------------------------------------------------------- crypto
def test_crypto_round_trip():
    tok = crypto.encrypt("dyk_secret_value")
    assert tok != "dyk_secret_value"
    assert crypto.decrypt(tok) == "dyk_secret_value"


def test_mask_redacts_middle():
    m = crypto.mask("dyk_abcdefghij1234567890")
    assert m.startswith("dyk_") and m.endswith("7890") and "..." in m


# --------------------------------------------------- RestServiceClient (remote)
def test_remote_info_matches(admin_key, transport):
    c = RestServiceClient("http://testserver", admin_key,
                          on_behalf="tester@A", http=transport)
    info = c.info()
    assert info.version == VERSION
    assert "dag.lifecycle" in info.capabilities


def test_remote_list_and_lifecycle(admin_key, transport):
    c = RestServiceClient("http://testserver", admin_key,
                          on_behalf="tester@A", http=transport)
    dags = c.list_dags()
    assert isinstance(dags, list)
    if not dags:
        pytest.skip("no DAGs configured")
    name = dags[0]["name"]
    r = c.start_dag(name)
    assert r.ok is True
    r = c.stop_dag(name)
    assert r.ok is True


def test_remote_unknown_dag_raises_valueerror(admin_key, transport):
    c = RestServiceClient("http://testserver", admin_key, http=transport)
    with pytest.raises(ValueError):
        c.start_dag("__no_such_dag__")


def test_remote_bad_key_raises_auth_error(transport):
    c = RestServiceClient("http://testserver", "dyk_not_a_real_key",
                          http=transport)
    with pytest.raises(ServiceAuthError):
        c.start_dag("anything")


# ------------------------------------------------------------------- registry
def test_registry_crud_masks_key():
    from core.service.registry import TrustedServerRegistry
    _webapp()
    reg = TrustedServerRegistry()
    sid = "unit-test-srv"
    try:
        reg.remove(sid)
    except Exception:  # noqa: BLE001
        pass
    # probe=False: we are not standing up a real remote here
    rec = reg.add(sid, "Unit Test", "http://10.0.0.9:5061", "dyk_unit_secret",
                  role="user", verify_tls=False, added_by="test", probe=False)
    assert "api_key" not in rec and "api_key_enc" not in rec
    listed = reg.list()
    assert any(s["server_id"] == sid for s in listed)
    # the clear key round-trips only via the explicit decrypt path
    assert reg.dao.get_clear_key(sid) == "dyk_unit_secret"
    reg.remove(sid)
    assert reg.get(sid) is None


# --------------------------------------------------------------------- switch
@pytest.fixture()
def logged_in():
    c = TestClient(_webapp().app, follow_redirects=True)
    assert c.post("/login", data={"username": "admin",
                                  "password": "admin123"}).status_code == 200
    return c


def test_switch_to_local_and_back(logged_in):
    r = logged_in.post("/service/select", data={"target": "local"})
    assert r.status_code == 200
    # dashboard still renders against local
    assert logged_in.get("/dashboard").status_code == 200


def test_switch_unknown_target_rejected(logged_in):
    r = logged_in.post("/service/select", data={"target": "does-not-exist"})
    assert r.status_code == 200  # redirected back with a flash; stays usable
    assert logged_in.get("/dashboard").status_code == 200


def test_trusted_servers_page_admin_only(logged_in):
    assert logged_in.get("/admin/trusted-servers").status_code == 200
    anon = TestClient(_webapp().app, follow_redirects=False)
    r = anon.get("/admin/trusted-servers")
    assert r.status_code in (302, 303, 401, 403)
