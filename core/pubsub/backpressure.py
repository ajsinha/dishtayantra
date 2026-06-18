"""
core/pubsub/backpressure.py - Credit-based flow control (opt-in, default off).

Problem: a fast producer feeding a slow consumer makes the queue between them
grow without bound (memory blow-up), or - as in the in-memory topic fan-out -
silently drops messages once the subscriber queue is full. Credit-based
backpressure makes the *consumer* the authority on how much may be in flight: it
grants the producer a fixed number of credits, the producer must spend one before
emitting an item, and the consumer returns a credit each time it takes one. When
credits run out the producer waits (true backpressure) instead of growing memory
or dropping data.

This module provides:
- ``CreditController``: the thread-safe credit accounting + policy + stats.
- ``CreditQueue``: a drop-in ``queue.Queue`` for the consumer side that returns a
  credit automatically after each successful ``get()`` - so no consumer call site
  changes.
- ``read_backpressure_config()``: best-effort config read; defaults to DISABLED so
  behaviour is unchanged unless explicitly turned on.
"""

import logging
import queue
import threading
import time

logger = logging.getLogger(__name__)

VALID_POLICIES = ("block", "drop")


class CreditController:
    """Thread-safe credit accounting for one producer->consumer link.

    Args:
        capacity: maximum number of in-flight (un-consumed) items.
        policy: "block" - acquire() waits for a credit (true backpressure);
                "drop"  - acquire() returns False immediately when none free.
        timeout: optional seconds to wait under the "block" policy before giving
                up (returns False, counted as a drop). None = wait indefinitely.
    """

    def __init__(self, capacity, policy="block", timeout=None):
        capacity = int(capacity)
        if capacity < 1:
            raise ValueError("backpressure capacity must be >= 1")
        if policy not in VALID_POLICIES:
            raise ValueError(f"backpressure policy must be one of {VALID_POLICIES}; got {policy!r}")
        self.capacity = capacity
        self.policy = policy
        self.timeout = timeout
        self._available = capacity
        self._cond = threading.Condition()
        # observability counters
        self._granted = 0
        self._released = 0
        self._blocked = 0
        self._dropped = 0
        self._max_in_flight = 0
        self._wait_seconds = 0.0

    def acquire(self, block=None, timeout="_default") -> bool:
        """Spend one credit. Returns True if granted (caller may emit), False if
        the caller should drop the item (no credit and not blocking, or timed out).
        """
        if block is None:
            block = (self.policy == "block")
        if timeout == "_default":
            timeout = self.timeout
        with self._cond:
            if self._available <= 0 and block:
                self._blocked += 1
                start = time.monotonic()
                got = self._cond.wait_for(lambda: self._available > 0, timeout=timeout)
                self._wait_seconds += time.monotonic() - start
                if not got:
                    self._dropped += 1
                    return False
            if self._available <= 0:
                self._dropped += 1
                return False
            self._available -= 1
            self._granted += 1
            in_flight = self.capacity - self._available
            if in_flight > self._max_in_flight:
                self._max_in_flight = in_flight
            return True

    def release(self, n=1):
        """Return ``n`` credits to the producer (called by the consumer side)."""
        with self._cond:
            self._available = min(self.capacity, self._available + n)
            self._released += n
            self._cond.notify(n)

    @property
    def available(self):
        with self._cond:
            return self._available

    @property
    def in_flight(self):
        with self._cond:
            return self.capacity - self._available

    def stats(self) -> dict:
        with self._cond:
            return {
                "capacity": self.capacity,
                "available": self._available,
                "in_flight": self.capacity - self._available,
                "policy": self.policy,
                "granted": self._granted,
                "released": self._released,
                "blocked": self._blocked,
                "dropped": self._dropped,
                "max_in_flight": self._max_in_flight,
                "total_wait_seconds": round(self._wait_seconds, 6),
            }


class CreditQueue(queue.Queue):
    """A bounded queue that returns one credit to its controller after each
    successful ``get()``. Drop-in for ``queue.Queue`` on the consumer side, so
    consumers keep calling ``.get()`` exactly as before."""

    def __init__(self, maxsize, controller: CreditController):
        super().__init__(maxsize=maxsize)
        self.credit = controller

    def get(self, block=True, timeout=None):
        item = super().get(block=block, timeout=timeout)  # raises Empty -> no credit returned
        self.credit.release()
        return item


def read_backpressure_config():
    """Return (enabled, capacity, policy, timeout_seconds).

    Best-effort; works at import time and inside worker processes. Defaults to
    DISABLED so the default runtime behaviour is unchanged.
    """
    try:
        from core.properties_configurator import PropertiesConfigurator
        from core.config_parsers import find_default_config
        cfg = PropertiesConfigurator([find_default_config("config")])
        enabled = bool(cfg.get_bool("backpressure.enabled", False))
        capacity = int(cfg.get_int("backpressure.capacity", 1000) or 1000)
        policy = (cfg.get("backpressure.policy", "block") or "block").strip().lower()
        if policy not in VALID_POLICIES:
            raise ValueError(
                f"backpressure.policy must be one of {VALID_POLICIES}; got {policy!r}")
        timeout_ms = int(cfg.get_int("backpressure.timeout_ms", 0) or 0)
        timeout = (timeout_ms / 1000.0) if timeout_ms > 0 else None
        return enabled, max(1, capacity), policy, timeout
    except ValueError:
        raise
    except Exception:
        return False, 1000, "block", None
