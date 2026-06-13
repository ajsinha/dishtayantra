"""End-to-end web auth flow tests using FastAPI's TestClient.

The webapp singleton is constructed once per session (it loads the real
config/application.properties via the conftest-initialized singleton, which
points the database at a temporary file).
"""
import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient  # noqa: E402


@pytest.fixture(scope='module')
def client():
    from web.dishtayantra_webapp import DishtaYantraWebApp
    webapp = DishtaYantraWebApp.get_instance()
    with TestClient(webapp.app, follow_redirects=False) as c:
        yield c
    webapp.shutdown()


@pytest.fixture()
def anon_client():
    """A cookie-free client (the shared one may carry a session)."""
    from web.dishtayantra_webapp import DishtaYantraWebApp
    webapp = DishtaYantraWebApp.get_instance()
    return TestClient(webapp.app, follow_redirects=False)


@pytest.fixture()
def admin_client():
    from web.dishtayantra_webapp import DishtaYantraWebApp
    webapp = DishtaYantraWebApp.get_instance()
    c = TestClient(webapp.app, follow_redirects=True)
    r = c.post('/login', data={'username': 'admin', 'password': 'admin123'})
    assert r.status_code == 200
    return c


def test_root_serves_landing_page_anonymously(client):
    """v2.1.0: anonymous visitors get the public landing page; the sign-in
    path is one click away."""
    r = client.get('/')
    assert r.status_code == 200
    assert 'DishtaYantra' in r.text
    assert '/login' in r.text  # sign-in CTA present


def test_root_redirects_authenticated_users_to_dashboard(client):
    c = TestClient(client.app, follow_redirects=True)
    c.post('/login', data={'username': 'admin', 'password': 'admin123'})
    r = c.get('/', follow_redirects=False)
    assert r.status_code == 303
    assert r.headers['location'] == '/dashboard'


def test_login_page_renders(client):
    r = client.get('/login')
    assert r.status_code == 200
    assert 'DishtaYantra' in r.text


def test_bad_login_bounces_back(client):
    r = client.post('/login', data={'username': 'admin',
                                    'password': 'nope'})
    assert r.status_code == 303
    assert r.headers['location'] == '/login'


def test_good_login_reaches_dashboard(client):
    r = client.post('/login', data={'username': 'admin',
                                    'password': 'admin123'})
    assert r.status_code == 303
    assert r.headers['location'] == '/dashboard'


def test_anonymous_page_redirects(anon_client):
    r = anon_client.get('/dashboard')
    assert r.status_code == 303
    assert r.headers['location'] == '/login'


def test_anonymous_api_gets_401_json(anon_client):
    r = anon_client.get('/users/api/list')
    assert r.status_code == 401
    assert r.json()['success'] is False


def test_health_is_public(client):
    from core.version import VERSION
    r = client.get('/health')
    assert r.status_code == 200
    assert r.json()['version'] == VERSION


def test_metrics_is_public(client):
    r = client.get('/metrics')
    assert r.status_code == 200
    assert 'dishtayantra' in r.text.lower() or '#' in r.text


def test_admin_pages_render(admin_client):
    for path in ['/dashboard', '/admin/monitoring', '/users', '/cache',
                 '/dag-designer', '/admin/workers', '/admin/logs',
                 '/jvm', '/cpp', '/rust']:
        r = admin_client.get(path)
        assert r.status_code == 200, f'{path} -> {r.status_code}'


def test_designer_components_include_cloud_schemes(admin_client):
    r = admin_client.get('/api/dag-designer/components')
    assert r.status_code == 200
    body = r.text
    assert 's3://' in body and 'azureblob://' in body and 'gcs://' in body
