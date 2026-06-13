"""Tests for the HA manager abstraction (factory + socket failover)."""
import socket
import time

import pytest

from core.ha.ha_manager import (
    AlwaysPrimaryHAProvider,
    HAConfigurationError,
    create_ha_provider,
)
from core.ha.socket_ha import SocketHAProvider


class _HaProps:
    def __init__(self, mapping):
        self._mapping = mapping

    def get(self, key, default=None):
        return self._mapping.get(key, default)


def _free_port():
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def test_none_provider_always_primary():
    provider = create_ha_provider(_HaProps({'ha.provider': 'none'}))
    assert isinstance(provider, AlwaysPrimaryHAProvider)
    provider.start()
    assert provider.is_primary()
    provider.stop()


def test_factory_rejects_unknown_provider():
    with pytest.raises(HAConfigurationError):
        create_ha_provider(_HaProps({'ha.provider': 'ouija-board'}))


def test_factory_requires_provider_property():
    with pytest.raises(HAConfigurationError):
        create_ha_provider(_HaProps({}))


def test_factory_requires_socket_settings():
    with pytest.raises(HAConfigurationError):
        create_ha_provider(_HaProps({'ha.provider': 'socket'}))


def _wait_for(predicate, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.1)
    return False


def test_socket_election_and_failover():
    """First instance binds and becomes primary; the second stays
    secondary until the first releases the port, then takes over."""
    port = _free_port()
    primary_events, secondary_events = [], []

    a = SocketHAProvider('127.0.0.1', port, retry_seconds=1)
    a.on_elected = lambda: primary_events.append('a')
    b = SocketHAProvider('127.0.0.1', port, retry_seconds=1)
    b.on_elected = lambda: secondary_events.append('b')

    a.start()
    assert _wait_for(a.is_primary), "first instance never became primary"

    b.start()
    time.sleep(2.5)
    assert not b.is_primary(), "second instance must stand by as secondary"

    # Failover: primary releases the port.
    a.stop()
    assert _wait_for(b.is_primary), "secondary never took over after failover"
    assert not a.is_primary()
    b.stop()
