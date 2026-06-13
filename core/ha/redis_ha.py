"""
Redis HA Provider
=================

Primary/secondary failover using a Redis lease lock:

    - The primary holds ``lock_key`` with a TTL of ``lease_seconds`` and
      renews it every ``lease_seconds / 3`` seconds.
    - Secondaries attempt ``SET key value NX EX lease_seconds`` on the same
      cadence; when the primary dies its lease expires and exactly one
      secondary acquires the lock (atomically) and is promoted.
    - If the primary fails to renew (Redis outage, GC pause beyond the
      lease, ...) it demotes itself immediately to avoid split-brain.

Required properties:
    ha.redis.host
    ha.redis.port
    ha.redis.lock_key        e.g. ``dishtayantra:ha:primary``
    ha.redis.lease_seconds   e.g. ``15``

Optional:
    ha.redis.password

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import socket
import threading
import traceback
import uuid

from core.ha.ha_manager import HAProvider, HAConfigurationError

logger = logging.getLogger(__name__)

# Atomic compare-and-renew: only the lease holder may extend the TTL.
_RENEW_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('expire', KEYS[1], ARGV[2])
else
    return 0
end
"""

# Atomic compare-and-delete: only the lease holder may release the lock.
_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


class RedisHAProvider(HAProvider):
    """Lease-lock based failover on a shared Redis instance."""

    def __init__(self, host: str, port: int, lock_key: str,
                 lease_seconds: int, password: str = None):
        super().__init__(name="redis")
        if lease_seconds < 3:
            raise HAConfigurationError(
                "ha.redis.lease_seconds must be >= 3 for safe renewal")
        try:
            import redis  # noqa: F401 - validate availability
        except ImportError as exc:
            raise HAConfigurationError(
                "redis-py is not installed but ha.provider=redis was "
                "selected. Install with: pip install redis"
            ) from exc
        self.host = host
        self.port = port
        self.lock_key = lock_key
        self.lease_seconds = lease_seconds
        self.password = password
        # Unique identity of this instance, stored as the lock value.
        self.instance_id = f"{socket.gethostname()}:{uuid.uuid4().hex[:12]}"
        self._client = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread = None

    def _connect(self):
        import redis
        return redis.Redis(host=self.host, port=self.port,
                           password=self.password,
                           socket_timeout=5, decode_responses=True)

    def start(self) -> None:
        """Begin acquire/renew loop in a daemon thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._lease_loop, daemon=True, name="RedisHAProvider")
        self._thread.start()
        logger.info("RedisHAProvider started (key=%s lease=%ss id=%s)",
                    self.lock_key, self.lease_seconds, self.instance_id)

    def _lease_loop(self) -> None:
        """Acquire or renew the lease on a fixed cadence."""
        interval = max(1, self.lease_seconds // 3)
        while not self._stop_event.is_set():
            try:
                if self._client is None:
                    self._client = self._connect()
                if self.is_primary():
                    renewed = self._client.eval(
                        _RENEW_SCRIPT, 1, self.lock_key,
                        self.instance_id, self.lease_seconds)
                    if not renewed:
                        logger.warning(
                            "RedisHAProvider: lease renewal failed - another "
                            "instance owns %s; demoting", self.lock_key)
                        self._transition_to_secondary()
                else:
                    acquired = self._client.set(
                        self.lock_key, self.instance_id,
                        nx=True, ex=self.lease_seconds)
                    if acquired:
                        self._transition_to_primary()
            except Exception as exc:  # noqa: BLE001
                logger.error("RedisHAProvider lease loop error: %s", exc)
                logger.error("Full stack trace:\n%s", traceback.format_exc())
                # Safety: demote on any Redis communication failure.
                self._transition_to_secondary()
                self._client = None
            self._stop_event.wait(interval)

    def stop(self) -> None:
        """Release the lease (if held) and stop participating."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self._client is not None:
            try:
                self._client.eval(_RELEASE_SCRIPT, 1, self.lock_key,
                                  self.instance_id)
            except Exception as exc:  # noqa: BLE001
                logger.error("RedisHAProvider release error: %s", exc)
                logger.error("Full stack trace:\n%s", traceback.format_exc())
        self._transition_to_secondary()
        logger.info("RedisHAProvider stopped")
