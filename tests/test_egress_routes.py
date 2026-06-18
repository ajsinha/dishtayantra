"""Tests for the read-only egress monitoring routes (/egress, /egress/stats)."""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope='module')
def webapp():
    from web.dishtayantra_webapp import DishtaYantraWebApp
    wa = DishtaYantraWebApp.get_instance()
    yield wa


@pytest.fixture()
def anon_client(webapp):
    return TestClient(webapp.app, follow_redirects=False)


@pytest.fixture()
def admin_client(webapp):
    c = TestClient(webapp.app, follow_redirects=True)
    c.post('/login', data={'username': 'admin', 'password': 'admin123'})
    return c


def test_egress_stats_requires_auth(anon_client):
    """Unauthenticated access must not return the stats JSON."""
    r = anon_client.get('/egress/stats')
    assert r.status_code != 200


def test_egress_stats_shape(admin_client):
    """Authenticated stats endpoint returns the documented shape."""
    r = admin_client.get('/egress/stats', headers={'Accept': 'application/json'})
    assert r.status_code == 200
    body = r.json()
    assert 'worker_count' in body and isinstance(body['worker_count'], int)
    assert 'destinations' in body and isinstance(body['destinations'], list)


def test_egress_page_renders(admin_client):
    """The monitoring page renders for a logged-in user."""
    r = admin_client.get('/egress')
    assert r.status_code == 200
    assert 'Egress Monitoring' in r.text
