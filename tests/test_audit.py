"""Tests for the audit trail: the safe helper, the DAO, and the admin view."""
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
    assert c.post('/login',
                  data={'username': 'admin', 'password': 'admin123'}).status_code == 200
    return c


def test_audit_helper_records_and_queries():
    from core.audit_log import audit
    from core.db.dao import AuditDAO
    audit('test.event', actor='tester', target='thing', detail='hello',
          source_ip='10.0.0.1')
    rows = AuditDAO().list_events(limit=50, action='test.event')
    assert rows and rows[0]['actor'] == 'tester'
    assert rows[0]['target'] == 'thing' and rows[0]['source_ip'] == '10.0.0.1'
    assert rows[0]['success'] is True


def test_audit_helper_never_raises(monkeypatch):
    # Even if the DAO blows up, audit() must not propagate.
    import core.audit_log as al

    class Boom:
        def record(self, **kw):
            raise RuntimeError("db down")

    monkeypatch.setattr('core.db.dao.AuditDAO', lambda *a, **k: Boom())
    al.audit('test.boom', actor='x')  # must not raise


def test_login_is_audited(anon_client):
    from core.db.dao import AuditDAO
    anon_client.post('/login', data={'username': 'admin', 'password': 'admin123'})
    anon_client.post('/login', data={'username': 'admin', 'password': 'nope'})
    acts = {e['action'] for e in AuditDAO().list_events(limit=200)}
    assert 'auth.login' in acts
    assert 'auth.login_failed' in acts


def test_audit_page_admin_only(anon_client):
    r = anon_client.get('/admin/audit')
    assert r.status_code in (302, 303)
    assert '/login' in r.headers.get('location', '')


def test_audit_page_renders_and_filters(admin_client):
    # generate a distinct event then confirm the page + filter show it
    admin_client.post('/admin/api-keys/create',
                      data={'username': 'admin', 'key_name': 'audit-ui-key'})
    page = admin_client.get('/admin/audit')
    assert page.status_code == 200 and 'Audit Trail' in page.text
    assert 'apikey.create' in page.text
    filtered = admin_client.get('/admin/audit?action=apikey.create')
    assert filtered.status_code == 200 and 'apikey.create' in filtered.text


def test_purge_removes_old_keeps_recent():
    from datetime import datetime, timedelta, timezone
    from core.db.dao import AuditDAO
    from core.db.models import AuditEvent
    dao = AuditDAO()
    dao.record('retention.recent', actor='t')
    with dao.db.session_scope() as s:
        s.add(AuditEvent(action='retention.old', actor='t', success=True,
                         created_at=datetime.now(timezone.utc).replace(tzinfo=None)
                         - timedelta(days=30)))
    removed = dao.purge_older_than(15)
    assert removed >= 1
    actions = {e['action'] for e in dao.list_events(limit=500)}
    assert 'retention.old' not in actions
    assert 'retention.recent' in actions


def test_purge_disabled_for_non_positive_days():
    from core.db.dao import AuditDAO
    dao = AuditDAO()
    assert dao.purge_older_than(0) == 0
    assert dao.purge_older_than(-1) == 0


def test_retention_runner_disabled_does_not_start():
    from core.audit_retention import AuditRetention
    r = AuditRetention(retention_days=0)
    r.start()
    assert r._thread is None  # disabled -> no thread
    r.stop()
