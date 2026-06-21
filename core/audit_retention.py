"""
Audit retention sweep
=====================

A small daemon that periodically deletes audit rows older than a configured
number of days, so the audit trail stays bounded. Defaults to 15 days; set
``audit.retention_days`` in configuration to change it (<= 0 disables purging).

The sweep runs once at startup and then every ``audit.retention_sweep_hours``
hours (default 6). It is defensive: a failed sweep is logged and the loop
continues.
"""

import logging
import threading

logger = logging.getLogger(__name__)

DEFAULT_RETENTION_DAYS = 15
DEFAULT_SWEEP_HOURS = 6.0


class AuditRetention:
    """Periodically purge audit events older than ``retention_days``."""

    def __init__(self, retention_days: int = DEFAULT_RETENTION_DAYS,
                 sweep_interval_seconds: int = int(DEFAULT_SWEEP_HOURS * 3600)):
        self.retention_days = retention_days
        self.sweep_interval = max(int(sweep_interval_seconds), 60)
        self._stop = threading.Event()
        self._thread = None

    def purge_once(self) -> int:
        try:
            from core.db.dao import AuditDAO
            removed = AuditDAO().purge_older_than(self.retention_days)
            if removed:
                logger.info("Audit retention: purged %d event(s) older than "
                            "%d day(s)", removed, self.retention_days)
            return removed
        except Exception:  # noqa: BLE001 - retention must not crash the server
            logger.exception("Audit retention sweep failed")
            return 0

    def _loop(self):
        while not self._stop.is_set():
            self.purge_once()
            self._stop.wait(self.sweep_interval)

    def start(self):
        if not self.retention_days or self.retention_days <= 0:
            logger.info("Audit retention disabled (audit.retention_days <= 0); "
                        "events are kept indefinitely")
            return
        self._thread = threading.Thread(target=self._loop,
                                        name="audit-retention", daemon=True)
        self._thread.start()
        logger.info("Audit retention started (keep %d day(s), sweep every "
                    "%.1fh)", self.retention_days, self.sweep_interval / 3600.0)

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
