"""
core/egress/drainer.py - bounded egress worker pool (roadmap A5).

A configurable, bounded pool of worker threads (default 4) drains many
per-destination WALs. Each destination is a `DestinationChannel`; channels are
assigned to a worker by a stable hash of the WAL key, so one destination is always
drained by exactly one worker => per-destination FIFO is preserved.

Crucially, draining is *non-blocking across channels*: when a write fails, that
channel enters backoff and `pump()` returns without advancing its WAL, so the
worker can service its OTHER channels instead of being starved by one stuck
destination. Per-destination FIFO still holds because the stalled channel never
advances past the failed record (stop-the-line), and a record is acked only after
the broker accepts it (durable at-least-once resume on restart).
"""

import logging
import threading
import time
import zlib

logger = logging.getLogger(__name__)

DEFAULT_WORKER_COUNT = 4


class DestinationChannel:
    """Per-destination drain state + logic. One channel == one WAL."""

    def __init__(self, name, wal, writer, batch_max_records=500,
                 reclaim_every=64, backoff_initial_ms=50, backoff_max_ms=5000):
        self.name = name
        self.wal = wal
        self.writer = writer              # callable(payload) -> None, raises on failure
        self.batch_max = int(batch_max_records)
        self.reclaim_every = reclaim_every
        self.backoff_initial = backoff_initial_ms / 1000.0
        self.backoff_max = backoff_max_ms / 1000.0
        self._pending = []                # records read, not yet written (in order)
        self._backoff_until = 0.0
        self._cur_backoff = self.backoff_initial
        self._since_reclaim = 0
        # stats
        self.written = 0
        self.retries = 0
        self.last_error = None
        self.connected = True

    def pump(self):
        """Do a bounded unit of work. Returns True if it had work (made progress
        or is holding a stalled record), False if idle. Never blocks across the
        worker's other channels."""
        now = time.monotonic()
        if now < self._backoff_until:
            return False                  # in backoff; let siblings run
        if not self._pending:
            self._pending = self.wal.read_next(self.batch_max)
            if not self._pending:
                return False
        while self._pending:
            offset, payload = self._pending[0]
            try:
                self.writer(payload)
            except Exception as exc:      # connection loss / write failure
                self.retries += 1
                self.last_error = str(exc)
                if self.connected:
                    self.connected = False        # circuit opens
                    logger.warning("egress[%s]: write failing, stop-the-line: %s",
                                   self.name, exc)
                self._backoff_until = time.monotonic() + self._cur_backoff
                self._cur_backoff = min(self._cur_backoff * 2, self.backoff_max)
                return True               # holding a stalled record (not idle)
            # success: ack only now, then advance
            self.wal.ack(offset)
            self.written += 1
            self._pending.pop(0)
            if not self.connected:
                logger.info("egress[%s]: reconnected, resuming", self.name)
            self.connected = True
            self._cur_backoff = self.backoff_initial
            self._since_reclaim += 1
            if self._since_reclaim >= self.reclaim_every:
                self._since_reclaim = 0
                try:
                    self.wal.reclaim()
                except Exception as exc:
                    logger.warning("egress[%s]: reclaim failed: %s", self.name, exc)
        return True

    def close(self):
        try:
            self.wal.close()
        except Exception:
            pass

    def stats(self):
        return {
            "destination": self.name,
            "written": self.written,
            "retries": self.retries,
            "connected": self.connected,
            "committed_offset": self.wal.committed_offset(),
            "wal_bytes": self.wal.size_bytes(),
            "wal_high_water": self.wal.high_water(),
            "last_error": self.last_error,
        }


class EgressWorker(threading.Thread):
    """One pool thread; round-robins pump() across its assigned channels."""

    def __init__(self, worker_id, poll_interval_ms=10):
        super().__init__(name=f"egress-worker-{worker_id}", daemon=True)
        self.worker_id = worker_id
        self.poll = poll_interval_ms / 1000.0
        self._channels = {}
        self._lock = threading.Lock()
        self._stopping = threading.Event()

    def add_channel(self, key, ch):
        with self._lock:
            self._channels[key] = ch

    def remove_channel(self, key):
        with self._lock:
            ch = self._channels.pop(key, None)
            if ch:
                ch.close()
            return len(self._channels)

    def channel_count(self):
        with self._lock:
            return len(self._channels)

    def run(self):
        while not self._stopping.is_set():
            with self._lock:
                channels = list(self._channels.values())
            did_work = False
            for ch in channels:
                try:
                    if ch.pump():
                        did_work = True
                except Exception as exc:
                    logger.error("egress worker %s: channel %s error: %s",
                                 self.worker_id, ch.name, exc)
            if not did_work:
                self._stopping.wait(self.poll)

    def stop(self, join_timeout=5.0):
        self._stopping.set()
        self.join(timeout=join_timeout)
        with self._lock:
            for ch in self._channels.values():
                ch.close()
            self._channels.clear()


class AsyncEgressManager:
    """Owns a bounded pool of egress workers; one channel per destination WAL key.
    Process-wide singleton (each worker process has its own)."""

    def __init__(self):
        self.max_workers = DEFAULT_WORKER_COUNT
        self._configured = False
        self._workers = {}                # slot -> EgressWorker
        self._channels = {}               # key  -> (slot, channel)
        self._lock = threading.Lock()

    def configure(self, max_workers):
        with self._lock:
            if not self._configured:
                self.max_workers = max(1, int(max_workers))
                self._configured = True

    def register(self, key, wal, writer, max_workers=DEFAULT_WORKER_COUNT,
                 **channel_kwargs):
        self.configure(max_workers)
        with self._lock:
            if key in self._channels:
                return self._channels[key][1]
            ch = DestinationChannel(key, wal, writer, **channel_kwargs)
            slot = zlib.crc32(key.encode("utf-8")) % self.max_workers
            worker = self._workers.get(slot)
            if worker is None:
                worker = EgressWorker(slot)
                worker.start()
                self._workers[slot] = worker
            worker.add_channel(key, ch)
            self._channels[key] = (slot, ch)
            return ch

    def unregister(self, key):
        with self._lock:
            entry = self._channels.pop(key, None)
            if not entry:
                return
            slot, _ch = entry
            worker = self._workers.get(slot)
            if worker is not None and worker.remove_channel(key) == 0:
                worker.stop()
                self._workers.pop(slot, None)

    def get(self, key):
        entry = self._channels.get(key)
        return entry[1] if entry else None

    def worker_count(self):
        with self._lock:
            return len(self._workers)

    def stop_all(self):
        with self._lock:
            for worker in self._workers.values():
                worker.stop()
            self._workers.clear()
            self._channels.clear()

    def stats(self):
        with self._lock:
            return [ch.stats() for (_slot, ch) in self._channels.values()]


_manager = None
_manager_lock = threading.Lock()


def get_manager():
    global _manager
    if _manager is None:
        with _manager_lock:
            if _manager is None:
                _manager = AsyncEgressManager()
    return _manager
