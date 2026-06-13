"""
Shared pytest fixtures for the DishtaYantra v2.0.0 test suite.

Initializes the PropertiesConfigurator singleton from the repository's
config/application.properties so components relying on properties (storage,
database, HA) resolve the same configuration as the running server, then
points the database at a per-session temporary SQLite file so tests never
touch data/dishtayantra.db.
"""
import os
import sys
import tempfile

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

os.chdir(REPO_ROOT)
os.environ.setdefault('SECRET_KEY', 'test-secret')
# Keep the repository's config/users.json untouched: the webapp fixture
# would otherwise migrate + rename it. Tests use a disposable copy.
_users_fd, _users_path = tempfile.mkstemp(prefix='dy_users_', suffix='.json')
os.close(_users_fd)
os.unlink(_users_path)  # absent file -> bootstrap admin path
os.environ['USERS_FILE'] = _users_path

from core.properties_configurator import PropertiesConfigurator  # noqa: E402

# Initialize the singleton once for the whole session.
_props = PropertiesConfigurator(['config/application.properties'])
# Redirect the DB to a temp file for the entire test session.
_db_fd, _db_path = tempfile.mkstemp(prefix='dy_test_', suffix='.db')
os.close(_db_fd)
_props._properties['db.sqlite.path'] = _db_path


@pytest.fixture(scope='session')
def props():
    """The session-wide PropertiesConfigurator singleton."""
    return _props


@pytest.fixture(scope='session')
def temp_db_path():
    """Path of the temporary SQLite database used by the session."""
    return _db_path
