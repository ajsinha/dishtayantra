"""Tests for the storage abstraction (filesystem provider + factory)."""
import os
import tempfile

import pytest

from core.storage import (
    StorageConfigurationError,
    StorageObjectNotFoundError,
    get_storage_provider,
    reset_storage_provider,
)
from core.storage.filesystem_storage import FileSystemStorageProvider
from core.storage.storage_factory import create_storage_provider


class _FakeProps:
    """Minimal stand-in for PropertiesConfigurator."""

    def __init__(self, mapping):
        self._mapping = mapping

    def get(self, key, default=None):
        return self._mapping.get(key, default)


@pytest.fixture()
def fs_provider(tmp_path):
    props = _FakeProps({'storage.provider': 'filesystem',
                        'storage.filesystem.root': str(tmp_path)})
    return create_storage_provider(props)


def test_factory_builds_filesystem_provider(fs_provider):
    assert isinstance(fs_provider, FileSystemStorageProvider)
    assert fs_provider.name == 'filesystem'


def test_factory_rejects_unknown_provider():
    with pytest.raises(StorageConfigurationError):
        create_storage_provider(_FakeProps({'storage.provider': 'carrier-pigeon'}))


def test_factory_requires_provider_property():
    with pytest.raises(StorageConfigurationError):
        create_storage_provider(_FakeProps({}))


def test_write_read_roundtrip(fs_provider):
    fs_provider.write_text('a/b/c.json', '{"x": 1}')
    assert fs_provider.exists('a/b/c.json')
    assert fs_provider.read_text('a/b/c.json') == '{"x": 1}'


def test_bytes_roundtrip(fs_provider):
    payload = b'\x00\x01binary'
    fs_provider.write_bytes('bin/blob.bin', payload)
    assert fs_provider.read_bytes('bin/blob.bin') == payload


def test_read_missing_raises(fs_provider):
    with pytest.raises(StorageObjectNotFoundError):
        fs_provider.read_text('nope/missing.json')


def test_delete(fs_provider):
    fs_provider.write_text('d/x.txt', 'gone soon')
    fs_provider.delete('d/x.txt')
    assert not fs_provider.exists('d/x.txt')


def test_list_names_prefix_and_suffix(fs_provider):
    fs_provider.write_text('dags/a.json', '{}')
    fs_provider.write_text('dags/b.json', '{}')
    fs_provider.write_text('dags/readme.txt', 'no')
    fs_provider.write_text('dags/examples/c.json', '{}')
    names = fs_provider.list_names(prefix='dags', suffix='.json')
    assert 'dags/a.json' in names and 'dags/b.json' in names
    assert 'dags/readme.txt' not in names
    # Recursive by design (object-store parity); dag_server filters depth.
    assert 'dags/examples/c.json' in names


def test_path_traversal_blocked(fs_provider):
    with pytest.raises(Exception):
        fs_provider.read_text('../../etc/passwd')


def test_singleton_accessor(tmp_path):
    reset_storage_provider()
    props = _FakeProps({'storage.provider': 'filesystem',
                        'storage.filesystem.root': str(tmp_path)})
    p1 = get_storage_provider(props)
    p2 = get_storage_provider(props)
    assert p1 is p2
    reset_storage_provider()
