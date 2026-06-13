"""
High Availability Manager - Base Classes and Factory
====================================================

Configurable primary/secondary automated failover for DishtaYantra v2.0.0.

A single ``HAProvider`` implementation runs inside every server instance of a
cluster.  Exactly one instance holds the *primary* lease at any time; all
others run as warm secondaries.  When the primary dies or loses its lease,
one secondary is promoted automatically and the registered ``on_elected``
callbacks fire (the DAG server uses this to resume operation).

Select the mechanism with the ``ha.provider`` property:

    ha.provider = none | zookeeper | redis | s3 | socket

Providers:
    none       Single-instance deployments; this node is always primary.
    zookeeper  Apache Zookeeper leader election (kazoo).        [zookeeper_ha]
    redis      Redis lease lock with TTL heartbeat renewal.     [redis_ha]
    s3         S3 lease object with timestamp heartbeats.       [s3_ha]
    socket     TCP port binding - whoever binds the configured
               address is primary; secondaries retry forever.   [socket_ha]

Each provider has its own ``ha.<provider>.*`` required properties; missing
properties are a fatal configuration error (no silent defaulting), per the
v2.0.0 architecture mandate.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import threading
import traceback
from abc import ABC, abstractmethod
from typing import Callable, List

logger = logging.getLogger(__name__)

SUPPORTED_HA_PROVIDERS = ("none", "zookeeper", "redis", "s3", "socket")


class HAConfigurationError(Exception):
    """Raised when HA configuration is missing or invalid."""


class HAProvider(ABC):
    """
    Abstract base class for all HA providers.

    Lifecycle:
        start()   begin participating in the election (non-blocking)
        stop()    relinquish leadership (if held) and stop participating

    State:
        is_primary()  True when this instance currently holds the lease

    Callbacks (registered before start()):
        on_elected(cb)  invoked once each time this instance becomes primary
        on_demoted(cb)  invoked once each time this instance loses primary
    """

    def __init__(self, name: str):
        self.name = name
        self._is_primary = False
        self._state_lock = threading.Lock()
        self._elected_callbacks: List[Callable[[], None]] = []
        self._demoted_callbacks: List[Callable[[], None]] = []

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    @abstractmethod
    def start(self) -> None:
        """Start participating in the election (must not block)."""

    @abstractmethod
    def stop(self) -> None:
        """Stop participating and release leadership if held."""

    def is_primary(self) -> bool:
        """True when this instance currently holds the primary lease."""
        with self._state_lock:
            return self._is_primary

    def on_elected(self, callback: Callable[[], None]) -> None:
        """Register a callback fired when this instance becomes primary."""
        self._elected_callbacks.append(callback)

    def on_demoted(self, callback: Callable[[], None]) -> None:
        """Register a callback fired when this instance loses primary."""
        self._demoted_callbacks.append(callback)

    def details(self) -> dict:
        """Diagnostic snapshot for monitoring UIs."""
        return {
            "provider": self.name,
            "type": self.__class__.__name__,
            "is_primary": self.is_primary(),
        }

    # ------------------------------------------------------------------ #
    # Shared transition machinery for subclasses
    # ------------------------------------------------------------------ #

    def _transition_to_primary(self) -> None:
        """Mark primary and fire callbacks exactly once per transition."""
        with self._state_lock:
            if self._is_primary:
                return
            self._is_primary = True
        logger.info("HA[%s]: this instance is now PRIMARY", self.name)
        self._fire(self._elected_callbacks, "on_elected")

    def _transition_to_secondary(self) -> None:
        """Mark secondary and fire callbacks exactly once per transition."""
        with self._state_lock:
            if not self._is_primary:
                return
            self._is_primary = False
        logger.warning("HA[%s]: this instance DEMOTED to secondary", self.name)
        self._fire(self._demoted_callbacks, "on_demoted")

    def _fire(self, callbacks: List[Callable[[], None]], label: str) -> None:
        """Invoke callbacks; failures are fully logged, never swallowed silently."""
        for callback in callbacks:
            try:
                callback()
            except Exception as exc:  # noqa: BLE001
                logger.error("HA[%s]: %s callback %r failed: %s",
                             self.name, label, callback, exc)
                logger.error("Full stack trace:\n%s", traceback.format_exc())


class AlwaysPrimaryHAProvider(HAProvider):
    """``ha.provider=none`` - single instance deployments, always primary."""

    def __init__(self):
        super().__init__(name="none")

    def start(self) -> None:
        logger.info("HA disabled (ha.provider=none) - running as sole PRIMARY")
        self._transition_to_primary()

    def stop(self) -> None:
        self._transition_to_secondary()


def _require(props, key: str) -> str:
    """Fetch a required property or raise a detailed configuration error."""
    value = props.get(key)
    if value is None:
        raise HAConfigurationError(
            f"Required HA property '{key}' is missing from "
            f"application.properties (or the environment). The system will "
            f"not silently default this value - define '{key}' and restart."
        )
    return value


def create_ha_provider(props=None) -> HAProvider:
    """
    Build the configured HAProvider from ``ha.provider``.

    Raises:
        HAConfigurationError: when configuration is missing or invalid.
    """
    if props is None:
        from core.properties_configurator import PropertiesConfigurator
        props = PropertiesConfigurator()

    provider = _require(props, "ha.provider").strip().lower()
    logger.info("Creating HA provider: %s", provider)

    if provider == "none":
        return AlwaysPrimaryHAProvider()

    if provider == "zookeeper":
        from core.ha.zookeeper_ha import ZookeeperHAProvider
        return ZookeeperHAProvider(
            hosts=_require(props, "ha.zookeeper.hosts"),
            election_path=_require(props, "ha.zookeeper.election_path"),
        )

    if provider == "redis":
        from core.ha.redis_ha import RedisHAProvider
        return RedisHAProvider(
            host=_require(props, "ha.redis.host"),
            port=int(_require(props, "ha.redis.port")),
            lock_key=_require(props, "ha.redis.lock_key"),
            lease_seconds=int(_require(props, "ha.redis.lease_seconds")),
            password=props.get("ha.redis.password"),
        )

    if provider == "s3":
        from core.ha.s3_ha import S3HAProvider
        return S3HAProvider(
            bucket=_require(props, "ha.s3.bucket"),
            lease_key=_require(props, "ha.s3.lease_key"),
            region=_require(props, "ha.s3.region"),
            lease_seconds=int(_require(props, "ha.s3.lease_seconds")),
            endpoint_url=props.get("ha.s3.endpoint_url"),
        )

    if provider == "socket":
        from core.ha.socket_ha import SocketHAProvider
        return SocketHAProvider(
            host=_require(props, "ha.socket.host"),
            port=int(_require(props, "ha.socket.port")),
            retry_seconds=int(_require(props, "ha.socket.retry_seconds")),
        )

    raise HAConfigurationError(
        f"Unknown ha.provider '{provider}'. "
        f"Supported providers: {', '.join(SUPPORTED_HA_PROVIDERS)}"
    )
