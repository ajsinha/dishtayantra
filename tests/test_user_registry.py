"""Tests for the DB-backed UserRegistry facade (v2.0.0)."""
import json

import pytest

from core.db.database_manager import DatabaseManager
from core.user_registry import UserRegistry


class _DbProps:
    """Property stub configuring an isolated SQLite database."""

    def __init__(self, db_path):
        self._mapping = {'db.engine': 'sqlite',
                         'db.sqlite.path': str(db_path)}

    def get(self, key, default=None):
        return self._mapping.get(key, default)


@pytest.fixture()
def registry(tmp_path):
    """Fresh registry with an isolated DB and no legacy users.json."""
    DatabaseManager.reset_instance()
    DatabaseManager.set_instance(DatabaseManager(
        props=_DbProps(tmp_path / 'ur.db')))
    UserRegistry.reset_instance()
    reg = UserRegistry(users_file=str(tmp_path / 'no_such_users.json'))
    yield reg
    reg.stop_reload()
    UserRegistry.reset_instance()
    DatabaseManager.reset_instance()


def test_bootstrap_admin_created(registry):
    assert registry.user_exists('admin')
    assert registry.authenticate('admin', 'admin123') is not None
    assert registry.authenticate('admin', 'wrong') is None


def test_create_modify_delete_user(registry):
    assert registry.create_user(
        'zoe', {'password': 'pw123456', 'full_name': 'Zoe',
                'roles': ['user']}, 'admin')
    assert registry.authenticate('zoe', 'pw123456')
    assert registry.modify_user('zoe', {'full_name': 'Zoe Z'}, 'admin')
    assert registry.get_user('zoe')['full_name'] == 'Zoe Z'
    assert registry.delete_user('zoe', 'admin')
    assert not registry.user_exists('zoe')


def test_role_checks(registry):
    registry.create_user('op', {'password': 'pw123456',
                                'roles': ['operator']}, 'admin')
    assert registry.has_role('op', 'operator')
    assert not registry.has_role('op', 'admin')
    assert registry.has_any_role('op', ['admin', 'operator'])
    assert not registry.has_any_role('op', ['admin'])


def test_last_admin_protected(registry):
    # 'admin' is the only admin: stripping the role / deleting must fail.
    assert not registry.revoke_role('admin', 'admin', 'admin')
    assert not registry.delete_user('admin', 'admin')
    assert registry.has_role('admin', 'admin')


def test_api_keys_via_registry(registry):
    clear, record = registry.create_api_key('admin', 'test-key', 'admin')
    info = registry.authenticate_api_key(clear)
    assert info and info['username'] == 'admin'
    keys = registry.list_api_keys('admin')
    assert any(k['id'] == record['id'] for k in keys)
    registry.revoke_api_key(record['id'], 'admin')
    assert registry.authenticate_api_key(clear) is None


def test_legacy_json_migration(tmp_path):
    """A legacy users.json is imported once and renamed .migrated."""
    DatabaseManager.reset_instance()
    DatabaseManager.set_instance(DatabaseManager(
        props=_DbProps(tmp_path / 'mig.db')))
    UserRegistry.reset_instance()

    users_file = tmp_path / 'users.json'
    users_file.write_text(json.dumps({
        'legacy_user': {'password': 'oldpass1', 'full_name': 'Legacy',
                        'roles': ['user']},
        'legacy_admin': {'password': 'oldpass2', 'roles': ['admin']},
    }))

    reg = UserRegistry(users_file=str(users_file))
    try:
        assert reg.user_exists('legacy_user')
        assert reg.authenticate('legacy_user', 'oldpass1') is not None
        assert reg.has_role('legacy_admin', 'admin')
        # File renamed so migration never repeats.
        assert not users_file.exists()
        assert users_file.with_suffix('.json.migrated').exists()
    finally:
        reg.stop_reload()
        UserRegistry.reset_instance()
        DatabaseManager.reset_instance()
