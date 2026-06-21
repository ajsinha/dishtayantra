"""End-to-end tests for the admin API-key management UI and key auth.

Exercises the web flow (create / list / revoke) plus the guard behavior that a
created key authenticates and a revoked key does not.
"""
import re

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient  # noqa: E402


@pytest.fixture()
def anon_client():
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


def _extract_key(html):
    m = re.search(r'id="clearKey">([^<]+)<', html)
    return m.group(1).strip() if m else None


def test_page_requires_admin(anon_client):
    r = anon_client.get('/admin/api-keys')
    assert r.status_code in (302, 303)
    assert '/login' in r.headers.get('location', '')


def test_admin_can_view_page(admin_client):
    r = admin_client.get('/admin/api-keys')
    assert r.status_code == 200
    assert 'Create a key' in r.text


def test_create_authenticate_and_revoke(admin_client, anon_client):
    # create
    r = admin_client.post('/admin/api-keys/create',
                          data={'username': 'admin', 'key_name': 'pytest-key'})
    assert r.status_code == 200
    assert 'API key created' in r.text
    key = _extract_key(r.text)
    assert key and key.startswith('dyk_')

    # the clear key authenticates against an admin /api endpoint
    ok = anon_client.get('/api/workers/status',
                         headers={'Authorization': f'Bearer {key}'})
    assert ok.status_code == 200

    # it also works via the X-API-Key header
    ok2 = anon_client.get('/api/workers/status', headers={'X-API-Key': key})
    assert ok2.status_code == 200

    # listed on the page
    assert 'pytest-key' in admin_client.get('/admin/api-keys').text

    # revoke it
    from core.user_registry import UserRegistry
    kid = next(k['id'] for k in UserRegistry().list_api_keys('admin')
               if k['name'] == 'pytest-key' and k['is_active'])
    admin_client.post(f'/admin/api-keys/{kid}/revoke')

    # revoked key is rejected (JSON 401/403 on /api paths)
    denied = anon_client.get('/api/workers/status',
                             headers={'Authorization': f'Bearer {key}'})
    assert denied.status_code in (401, 403)


def test_create_rejects_unknown_user(admin_client):
    r = admin_client.post('/admin/api-keys/create',
                          data={'username': 'no_such_user', 'key_name': 'x'})
    # redirects back with an error flash; no key created
    assert r.status_code == 200
    assert 'API key created' not in r.text
