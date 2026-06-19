"""
core/egress/async_publisher.py - opt-in async egress wrapper (roadmap A5).

`AsyncPublisher` transparently wraps any publisher exposing ``publish(data)``.
When wrapped, ``publish()`` appends to the publisher's WAL and returns immediately
(freeing the compute thread); a per-destination drainer writes to the real broker
in order. The wrapper is duck-type compatible with the inner publisher, so
``PublicationNode`` calls it with no change.

Everything is OFF by default. ``maybe_wrap_publisher`` returns the publisher
unchanged unless async egress is enabled, so existing behaviour is identical.
"""

import logging
import time

from core.egress.drainer import get_manager
from core.egress.wal import WalFull, create_wal

logger = logging.getLogger(__name__)

_DEFAULTS = {
    "enabled": False,
    "default_for_publishers": True,  # when master on, wrap publishers that don't say otherwise
    "worker_count": 4,               # bounded egress worker pool size (per process)
    "backend": "filelog",
    "path": "./data/egress_wal",
    "max_bytes": 2 * 1024 ** 3,
    "high_water_pct": 80,
    "segment_bytes": 128 * 1024 * 1024,
    "fsync": "interval",
    "fsync_interval_ms": 50,
    "batch_max_records": 500,
    "overflow_policy": "block",      # block | drop
    "overflow_block_timeout_ms": 0,  # 0 = wait indefinitely (true backpressure)
}

# Destination URI schemes that must never use the WAL (local, in-process, fast).
_MEM_SCHEMES = ("mem://", "inmemory://", "memory://")


def read_egress_config(config):
    """Read egress.* settings from a configurator or a plain dict (best-effort)."""
    def g(key, default):
        full = "egress." + key
        try:
            if hasattr(config, "get_bool") and isinstance(default, bool):
                return config.get_bool(full, default)
            if hasattr(config, "get_int") and isinstance(default, int):
                return config.get_int(full, default)
            if hasattr(config, "get"):
                v = config.get(full, default)
                return v if v is not None else default
            if isinstance(config, dict):
                return config.get(full, default)
        except Exception:
            return default
        return default

    return {
        "enabled": g("async.enabled", _DEFAULTS["enabled"]),
        "default_for_publishers": g("async.default", _DEFAULTS["default_for_publishers"]),
        "worker_count": g("worker.count", _DEFAULTS["worker_count"]),
        "backend": g("wal.backend", _DEFAULTS["backend"]),
        "path": g("wal.path", _DEFAULTS["path"]),
        "max_bytes": g("wal.max_bytes", _DEFAULTS["max_bytes"]),
        "high_water_pct": g("wal.high_water_pct", _DEFAULTS["high_water_pct"]),
        "segment_bytes": g("wal.segment_bytes", _DEFAULTS["segment_bytes"]),
        "fsync": g("wal.fsync", _DEFAULTS["fsync"]),
        "fsync_interval_ms": g("wal.fsync_interval_ms", _DEFAULTS["fsync_interval_ms"]),
        "batch_max_records": g("batch.max_records", _DEFAULTS["batch_max_records"]),
        "overflow_policy": g("overflow.policy", _DEFAULTS["overflow_policy"]),
        "overflow_block_timeout_ms": g("overflow.block_timeout_ms",
                                       _DEFAULTS["overflow_block_timeout_ms"]),
    }


class AsyncPublisher:
    """Wrap an inner publisher; publish() becomes a non-blocking WAL append."""

    def __init__(self, inner, egress_cfg, name=None, wal_key=None):
        self._inner = inner
        self.name = name or getattr(inner, "name", inner.__class__.__name__)
        # The WAL key namespaces storage per (DAG, publisher); DAG names are
        # universally unique so the key follows the DAG and resumes on any worker.
        self._key = wal_key or self.name
        self.overflow_policy = egress_cfg.get("overflow_policy", "block")
        self.block_timeout = egress_cfg.get("overflow_block_timeout_ms", 0) / 1000.0
        self.dropped = 0
        wal_cfg = {k: egress_cfg[k] for k in
                   ("path", "max_bytes", "high_water_pct", "segment_bytes",
                    "fsync", "fsync_interval_ms") if k in egress_cfg}
        self._wal = create_wal(egress_cfg.get("backend", "filelog"), self._key, wal_cfg)
        # Register on the bounded worker pool; the destination is multiplexed onto
        # one of at most `worker_count` workers (default 4).
        self._channel = get_manager().register(
            self._key, self._wal, self._inner.publish,
            max_workers=egress_cfg.get("worker_count", 4),
            batch_max_records=egress_cfg.get("batch_max_records", 500))

    def publish(self, data):
        # Non-blocking handoff; apply overflow policy when the WAL is full.
        deadline = (time.monotonic() + self.block_timeout) if self.block_timeout else None
        while True:
            try:
                self._wal.append(data)
                return
            except WalFull:
                if self.overflow_policy == "drop":
                    self.dropped += 1
                    return
                # block = true backpressure: wait for the drainer to free space
                if deadline and time.monotonic() >= deadline:
                    self.dropped += 1
                    logger.warning("egress[%s]: overflow block timed out; dropped",
                                   self.name)
                    return
                time.sleep(0.001)

    def stats(self):
        s = self._channel.stats()
        s["dropped"] = self.dropped
        return s

    def async_pending(self):
        """v5.15.0: records still in the WAL waiting to be flushed to the real
        sink (0 == fully drained). Used by the maintenance drain check so
        'publications finished' truly accounts for async egress, not just the
        in-process publisher queue."""
        try:
            return self._wal.pending_count()
        except Exception:  # noqa: BLE001
            return 0

    def close(self):
        get_manager().unregister(self._key)

    def __getattr__(self, item):
        # transparent passthrough for any other publisher method/attribute
        return getattr(self._inner, item)


def _is_mem_destination(dest):
    d = str(dest or "").lower()
    return any(d.startswith(s) for s in _MEM_SCHEMES)


def maybe_wrap_publisher(publisher, config, name=None, scope=None,
                         publisher_config=None):
    """Return an AsyncPublisher if async egress applies to this publisher, else the
    publisher unchanged. Safe to call on every publisher; default config => no-op.

    Decision order (all must pass to wrap):
      1. master switch ``egress.async.enabled`` must be true;
      2. the destination must NOT be in-memory (mem://, inmemory://, memory://) -
         in-process destinations never use a WAL;
      3. per-publisher override ``async_egress`` in the publisher's own config:
         true => wrap, false => inline, unset => follow ``egress.async.default``.

    The WAL is namespaced by (scope/DAG, publisher); DAG names are universally
    unique, so the key follows the DAG and resumes on any worker after a restart.
    """
    cfg = read_egress_config(config)
    if not cfg.get("enabled"):
        return publisher

    pc = publisher_config or {}
    dest = pc.get("destination") or getattr(publisher, "destination", "") \
        or getattr(publisher, "_destination", "")
    if _is_mem_destination(dest):
        return publisher                      # mem destinations never use the WAL

    per = pc.get("async_egress", None)         # True / False / None(unspecified)
    if per is False:
        return publisher
    if per is None and not cfg.get("default_for_publishers", True):
        return publisher                       # opt-in mode: only explicit true wraps

    try:
        parts = [str(p) for p in (scope, name) if p]
        wal_key = ".".join(parts) if parts else (name or "publisher")
        return AsyncPublisher(publisher, cfg, name=name, wal_key=wal_key)
    except Exception as exc:  # never break publication because async setup failed
        logger.error("egress: failed to enable async for %s (%s); using inline",
                     name or publisher, exc)
        return publisher
