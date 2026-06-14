"""
RateMeter - exponentially-weighted moving-average throughput meter.
==================================================================

Tracks a smooth "events per minute" rate without storing any history: a single
EWMA decayed by elapsed wall-clock time. This is the same family of technique
used by Unix load average and production metrics libraries (Dropwizard,
Prometheus rate()). It is:

  * O(1) memory   - two floats, no timestamp buffers that grow with traffic
  * O(1) per event - a cheap mark() call
  * decay-correct - an idle stream's rate decays toward zero on its own,
                    so the number always reflects RECENT activity, not a
                    misleading lifetime average

Thread-safe: all mutation is guarded by an internal lock, so it is safe to
mark() from a subscriber thread while a web request reads rate_per_minute().

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import math
import threading
import time


class RateMeter:
    """EWMA events-per-minute meter with lazy time-based decay."""

    def __init__(self, half_life_seconds: float = 30.0):
        """
        Args:
            half_life_seconds: how quickly the rate "forgets" old activity.
                A 30s half-life means a burst's contribution halves every 30s
                of silence, giving a responsive yet stable ~1-minute feel.
        """
        if half_life_seconds <= 0:
            raise ValueError("half_life_seconds must be positive")
        self._half_life = float(half_life_seconds)
        # Decay constant: value *= exp(-dt / tau) where tau = half_life/ln2.
        self._tau = self._half_life / math.log(2.0)
        self._lock = threading.Lock()
        self._rate_per_sec = None     # EWMA of events/second (None until primed)
        self._pending = 0             # events since last tick fold
        self._last_tick = None        # monotonic seconds of last fold
        self._tick = 1.0              # fold cadence (seconds)
        self._peak_per_min = 0.0
        self._total = 0

    def _fold_locked(self, now):
        """Fold whole elapsed ticks of accumulated events into the EWMA.

        Processes time in fixed `self._tick`-second steps: for each elapsed
        tick, the instantaneous rate (events in that tick / tick) is blended
        into the EWMA. Idle ticks blend in a zero rate, so the value decays
        toward zero on its own. Caller holds the lock.
        """
        if self._last_tick is None:
            self._last_tick = now
            return
        elapsed = now - self._last_tick
        if elapsed < self._tick:
            return  # not a full tick yet; keep accumulating in _pending
        alpha = 1.0 - math.exp(-self._tick / self._tau)
        ticks = int(elapsed // self._tick)
        # First tick carries all pending events; subsequent (idle) ticks are 0.
        first_rate = self._pending / self._tick
        if self._rate_per_sec is None:
            self._rate_per_sec = first_rate
        else:
            self._rate_per_sec += alpha * (first_rate - self._rate_per_sec)
        for _ in range(ticks - 1):
            self._rate_per_sec += alpha * (0.0 - self._rate_per_sec)
        self._pending = 0
        self._last_tick += ticks * self._tick

    def mark(self, count: int = 1):
        """Record `count` events as having just occurred."""
        if count <= 0:
            return
        now = time.monotonic()
        with self._lock:
            self._fold_locked(now)
            self._pending += count
            self._total += count
            # Peak tracks the settled EWMA (events/min), not the spiky
            # in-progress partial-tick estimate, so a single first event
            # cannot register an absurd peak.
            settled_per_min = (self._rate_per_sec or 0.0) * 60.0
            if settled_per_min > self._peak_per_min:
                self._peak_per_min = settled_per_min

    def _live_per_min_locked(self, now):
        """Best current estimate incl. events still pending in this tick."""
        base = (self._rate_per_sec or 0.0)
        if self._last_tick is not None:
            partial = now - self._last_tick
            if partial > 0 and self._pending:
                # blend the in-progress tick's partial rate for responsiveness
                base = max(base, self._pending / max(partial, 0.001))
        return base * 60.0

    def rate_per_minute(self) -> float:
        """Current decayed rate in events per minute (decays toward 0 when idle)."""
        now = time.monotonic()
        with self._lock:
            self._fold_locked(now)
            return self._live_per_min_locked(now)

    def peak_per_minute(self) -> float:
        with self._lock:
            return self._peak_per_min

    def total(self) -> int:
        with self._lock:
            return self._total

    def snapshot(self) -> dict:
        """Return a UI-friendly snapshot: current rate, peak, total."""
        now = time.monotonic()
        with self._lock:
            self._fold_locked(now)
            return {
                'rate_per_minute': round(self._live_per_min_locked(now), 1),
                'peak_per_minute': round(self._peak_per_min, 1),
                'total': self._total,
            }
