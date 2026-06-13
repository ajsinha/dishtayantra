"""Tests for the SQLAlchemy DAO layer (users, roles, API keys)."""
import pytest

from core.db.database_manager import DatabaseManager
from core.db.dao import ApiKeyDAO, PasswordHasher, UserDAO


class _DbProps:
    """Property stub configuring an isolated SQLite database."""

    def __init__(self, db_path):
        self._mapping = {'db.engine': 'sqlite',
                         'db.sqlite.path': str(db_path)}

    def get(self, key, default=None):
        return self._mapping.get(key, default)


@pytest.fixture()
def daos(tmp_path):
    """Fresh isolated database per test."""
    DatabaseManager.reset_instance()
    db = DatabaseManager(props=_DbProps(tmp_path / 'test.db'))
    yield UserDAO(db), ApiKeyDAO(db)
    DatabaseManager.reset_instance()


def test_password_hash_roundtrip():
    hashed = PasswordHasher.hash_password('s3cret!')
    assert hashed.startswith('pbkdf2_sha256$')
    assert PasswordHasher.verify_password('s3cret!', hashed)
    assert not PasswordHasher.verify_password('wrong', hashed)


def test_create_and_authenticate_user(daos):
    user_dao, _ = daos
    user_dao.create_user('alice', 'pw123456', 'Alice A', ['user'], 'test')
    assert user_dao.authenticate('alice', 'pw123456') is not None
    assert user_dao.authenticate('alice', 'bad') is None
    assert user_dao.authenticate('ghost', 'pw123456') is None


def test_duplicate_user_rejected(daos):
    user_dao, _ = daos
    user_dao.create_user('bob', 'pw123456', 'Bob', ['user'], 'test')
    with pytest.raises(ValueError):
        user_dao.create_user('bob', 'pw123456', 'Bob 2', ['user'], 'test')


def test_roles_management(daos):
    user_dao, _ = daos
    user_dao.create_user('carol', 'pw123456', 'Carol', ['user'], 'test')
    user_dao.add_role('carol', 'admin', 'test')
    assert 'admin' in user_dao.get_user('carol')['roles']
    user_dao.revoke_role('carol', 'admin', 'test')
    assert 'admin' not in user_dao.get_user('carol')['roles']


def test_update_user(daos):
    user_dao, _ = daos
    user_dao.create_user('dave', 'pw123456', 'Dave', ['user'], 'test')
    user_dao.update_user('dave', password=None, full_name='David',
                         roles=['user', 'operator'], modified_by='test')
    user = user_dao.get_user('dave')
    assert user['full_name'] == 'David'
    assert sorted(user['roles']) == ['operator', 'user']


def test_delete_user(daos):
    user_dao, _ = daos
    user_dao.create_user('eve', 'pw123456', 'Eve', ['user'], 'test')
    user_dao.delete_user('eve', 'test')
    assert user_dao.get_user('eve') is None


def test_count_admins(daos):
    user_dao, _ = daos
    user_dao.create_user('root1', 'pw123456', 'R1', ['admin'], 'test')
    user_dao.create_user('root2', 'pw123456', 'R2', ['admin', 'user'], 'test')
    assert user_dao.count_admins() == 2


def test_api_key_lifecycle(daos):
    user_dao, key_dao = daos
    user_dao.create_user('frank', 'pw123456', 'Frank', ['user'], 'test')
    clear, record = key_dao.create_api_key('frank', 'ci-key', 'test')
    assert clear.startswith('dyk_')
    verified = key_dao.verify_api_key(clear)
    assert verified is not None and verified['username'] == 'frank'
    assert key_dao.verify_api_key('dyk_bogus') is None
    key_dao.revoke_api_key(record['id'], 'test')
    assert key_dao.verify_api_key(clear) is None
