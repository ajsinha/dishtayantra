"""
Flow retention sweep
=====================

A small daemon that periodically deletes flow events older than a configured
number of hours, so the history stays bounded. Defaults to 24 hours; set
``flow_recorder.retention_hours`` in configuration to change it (<= 0 disables
purging). The sweep runs at startup and then every
``flow_recorder.retention_sweep_minutes`` minutes (default 30).

This deliberately does NOT rely on any store's automatic expiry (e.g. Paimon
snapshot/partition expiry): retention is driven explicitly here, so the 24h
window is guaranteed regardless of backend.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import datetime as _dt
import logging
import threading
import time

logger = logging.getLogger(__name__)

DEFAULT_RETENTION_HOURS = 24
DEFAULT_SWEEP_MINUTES = 30.0


class FlowRetention:
    """Periodically purge flow events older than ``retention_hours``.

    Optionally runs an on-disk maintenance pass once per day at
    ``maintenance_hour`` (local time). ``maintenance_mode`` is one of
    ``none`` (default), ``incremental``, or ``full`` and is forwarded to the
    store's ``maintenance()`` (a no-op for stores that don't support it, e.g.
    the shared-DB dao store). Usually unnecessary - deleted pages are reused so
    the file plateaus - but available for capping disk after retention changes.
    """

    def __init__(self, store, retention_hours: float = DEFAULT_RETENTION_HOURS,
                 sweep_interval_seconds: int = int(DEFAULT_SWEEP_MINUTES * 60),
                 maintenance_mode: str = "none", maintenance_hour: int = 3):
        self.store = store
        self.retention_hours = retention_hours
        self.sweep_interval = max(int(sweep_interval_seconds), 30)
        self.maintenance_mode = (maintenance_mode or "none").lower()
        self.maintenance_hour = int(maintenance_hour) % 24
        self._last_maint_day = None
        self._stop = threading.Event()
        self._thread = None

    def _maybe_maintain(self):
        """Run the daily maintenance pass at most once per day at the set hour."""
        if self.maintenance_mode not in ("incremental", "full"):
            return
        now = _dt.datetime.now()
        if now.hour != self.maintenance_hour or self._last_maint_day == now.date():
            return
        self._last_maint_day = now.date()
        try:
            did = self.store.maintenance(self.maintenance_mode)
            if did:
                logger.info("Flow maintenance (%s) completed",
                            self.maintenance_mode)
        except Exception:  # noqa: BLE001 - maintenance must not crash the server
            logger.exception("Flow maintenance pass failed")

    def purge_once(self) -> int:
        try:
            if not self.retention_hours or self.retention_hours <= 0:
                return 0
            cutoff_ms = int((time.time() - self.retention_hours * 3600) * 1000)
            removed = self.store.purge_older_than_ms(cutoff_ms)
            if removed:
                logger.info("Flow retention: purged %d event(s) older than "
                            "%.1fh", removed, self.retention_hours)
            return removed
        except Exception:  # noqa: BLE001 - retention must not crash the server
            logger.exception("Flow retention sweep failed")
            return 0

    def _loop(self):
        while not self._stop.is_set():
            self.purge_once()
            self._maybe_maintain()
            self._stop.wait(self.sweep_interval)

    def start(self):
        if not self.retention_hours or self.retention_hours <= 0:
            logger.info("Flow retention disabled (flow_recorder.retention_hours "
                        "<= 0); events are kept until the store fills")
            return
        self._thread = threading.Thread(target=self._loop, name="flow-retention",
                                        daemon=True)
        self._thread.start()
        logger.info("Flow retention started (keep %.1fh, sweep every %.1f min)",
                    self.retention_hours, self.sweep_interval / 60.0)

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
