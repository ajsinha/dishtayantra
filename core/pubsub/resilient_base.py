"""
Resilient Pub/Sub Wrapper Base (v2.0.0)
=======================================

Generic resilience machinery shared by the resilient wrappers introduced in
v2.0.0 for the brokers that previously lacked one:

    - :mod:`core.pubsub.resilient_aerospike`
    - :mod:`core.pubsub.resilient_sql`
    - :mod:`core.pubsub.resilient_rest`
    - :mod:`core.pubsub.resilient_grpc`

(The Kafka / RabbitMQ / Redis / ActiveMQ / TIBCO EMS / WebSphere MQ resilient
clients shipped earlier remain unchanged.)

Design - decorator/wrapper pattern around the plain DataPublisher /
DataSubscriber implementations:

    ResilientDataPublisherWrapper
        - publish attempts are retried with exponential backoff
          (``retry_max`` attempts, ``retry_backoff_seconds`` base)
        - while the broker is down, messages are buffered in a bounded
          in-memory queue (``buffer_max_messages``) and drained automatically
          once a publish succeeds again
        - the inner publisher is fully re-created (``reconnect``) after the
          retry budget is exhausted, transparently restoring connections

    ResilientDataSubscriberWrapper
        - subscribe errors trigger the same backoff + inner re-create cycle
        - the subscriber never busy-spins on a dead broker

Every failure is logged with the FULL stack trace - nothing is swallowed.

Config keys understood by both wrappers (with safe operational defaults):

    retry_max               max attempts per operation        (default 5)
    retry_backoff_seconds   base backoff, doubled per attempt (default 2)
    buffer_max_messages     publisher outage buffer size      (default 10000)

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import threading
import time
import traceback
from collections import deque
from typing import Callable, Optional

import queue

from core.pubsub.datapubsub import DataPublisher, DataSubscriber

logger = logging.getLogger(__name__)


class ResilientDataPublisherWrapper(DataPublisher):
    """Retries, buffers, and reconnects around an inner DataPublisher."""

    def __init__(self, name, destination, config,
                 inner_factory: Callable[[], DataPublisher]):
        """
        Args:
            inner_factory: Zero-arg callable producing a fresh inner
                publisher (used at startup and on every reconnect).
        """
        super().__init__(name, destination, config)
        self._inner_factory = inner_factory
        self.retry_max = int(config.get('retry_max', 5))
        self.retry_backoff_seconds = float(config.get('retry_backoff_seconds', 2))
        self.buffer_max_messages = int(config.get('buffer_max_messages', 10000))
        self._outage_buffer = deque(maxlen=self.buffer_max_messages)
        self._inner_lock = threading.Lock()
        self._inner: Optional[DataPublisher] = None
        self._connect_inner(initial=True)
        logger.info("Resilient publisher '%s' wrapping %s "
                    "(retry_max=%d backoff=%ss buffer=%d)",
                    name, destination, self.retry_max,
                    self.retry_backoff_seconds, self.buffer_max_messages)

    # ------------------------------------------------------------------ #

    def _connect_inner(self, initial: bool = False) -> bool:
        """(Re)build the inner publisher; full-trace log on failure."""
        with self._inner_lock:
            try:
                if self._inner is not None:
                    try:
                        self._inner.stop()
                    except Exception:  # noqa: BLE001
                        logger.error("Error stopping stale inner publisher "
                                     "'%s':\n%s", self.name,
                                     traceback.format_exc())
                self._inner = self._inner_factory()
                logger.info("Resilient publisher '%s' %sconnected",
                            self.name, "" if initial else "re")
                return True
            except Exception as exc:  # noqa: BLE001
                self._inner = None
                logger.error("Resilient publisher '%s' connect failed: %s",
                             self.name, exc)
                logger.error("Full stack trace:\n%s", traceback.format_exc())
                if initial:
                    # During startup the broker may simply not be up yet -
                    # messages will buffer until the first reconnect works.
                    return False
                return False

    def _publish_once(self, data) -> None:
        """One raw publish through the inner publisher (may raise)."""
        with self._inner_lock:
            if self._inner is None:
                raise ConnectionError(
                    f"Resilient publisher '{self.name}' has no live inner "
                    f"connection")
            self._inner._do_publish(data)

    def _drain_outage_buffer(self) -> None:
        """Flush buffered messages after a successful reconnect/publish."""
        drained = 0
        while self._outage_buffer:
            pending = self._outage_buffer.popleft()
            try:
                self._publish_once(pending)
                drained += 1
            except Exception:  # noqa: BLE001
                # Put it back at the front and stop draining.
                self._outage_buffer.appendleft(pending)
                logger.error("Drain interrupted for '%s':\n%s", self.name,
                             traceback.format_exc())
                break
        if drained:
            logger.info("Resilient publisher '%s' drained %d buffered "
                        "message(s)", self.name, drained)

    def _do_publish(self, data):
        """Publish with retry/backoff; buffer on total failure.

        FIFO is preserved across outages: when older messages are waiting
        in the outage buffer, the new message is queued BEHIND them and
        the whole buffer is drained in order, so a recovered broker never
        receives messages out of sequence.
        """
        if self._outage_buffer:
            # Older messages first - enqueue, then attempt a full drain.
            self._outage_buffer.append(data)

        backoff = self.retry_backoff_seconds
        for attempt in range(1, self.retry_max + 1):
            try:
                if self._outage_buffer:
                    self._drain_outage_buffer()
                    if self._outage_buffer:
                        # Drain stalled mid-way - treat as a failed attempt.
                        raise ConnectionError(
                            f"outage buffer drain stalled with "
                            f"{len(self._outage_buffer)} message(s) pending")
                else:
                    self._publish_once(data)
                return
            except Exception as exc:  # noqa: BLE001
                logger.error("Resilient publisher '%s' attempt %d/%d "
                             "failed: %s", self.name, attempt,
                             self.retry_max, exc)
                logger.error("Full stack trace:\n%s", traceback.format_exc())
                if attempt < self.retry_max:
                    time.sleep(backoff)
                    backoff *= 2
                    self._connect_inner()
        # Retry budget exhausted: buffer the message for the next success
        # (unless it is already queued in the buffer from the FIFO path).
        if not self._outage_buffer or self._outage_buffer[-1] is not data:
            self._outage_buffer.append(data)
        logger.error("Resilient publisher '%s' buffering message after %d "
                     "failed attempts (buffer depth %d/%d)",
                     self.name, self.retry_max,
                     len(self._outage_buffer), self.buffer_max_messages)

    def details(self):
        info = super().details()
        info['resilient'] = True
        info['retry_max'] = self.retry_max
        info['outage_buffer_depth'] = len(self._outage_buffer)
        return info

    def stop(self):
        """Stop the wrapper and the inner publisher."""
        super().stop()
        with self._inner_lock:
            if self._inner is not None:
                try:
                    self._inner.stop()
                except Exception:  # noqa: BLE001
                    logger.error("Error stopping inner publisher '%s':\n%s",
                                 self.name, traceback.format_exc())


class ResilientDataSubscriberWrapper(DataSubscriber):
    """Retries and reconnects around an inner DataSubscriber."""

    def __init__(self, name, source, config,
                 inner_factory: Callable[[], DataSubscriber],
                 given_queue: queue.Queue = None):
        """
        Args:
            inner_factory: Zero-arg callable producing a fresh inner
                subscriber (used at startup and on every reconnect).
        """
        super().__init__(name, source, config, given_queue)
        self._inner_factory = inner_factory
        self.retry_max = int(config.get('retry_max', 5))
        self.retry_backoff_seconds = float(config.get('retry_backoff_seconds', 2))
        self._inner_lock = threading.Lock()
        self._inner: Optional[DataSubscriber] = None
        self._consecutive_errors = 0
        self._connect_inner(initial=True)
        logger.info("Resilient subscriber '%s' wrapping %s "
                    "(retry_max=%d backoff=%ss)",
                    name, source, self.retry_max, self.retry_backoff_seconds)

    def _connect_inner(self, initial: bool = False) -> bool:
        """(Re)build the inner subscriber; full-trace log on failure."""
        with self._inner_lock:
            try:
                if self._inner is not None:
                    try:
                        self._inner.stop()
                    except Exception:  # noqa: BLE001
                        logger.error("Error stopping stale inner subscriber "
                                     "'%s':\n%s", self.name,
                                     traceback.format_exc())
                self._inner = self._inner_factory()
                self._consecutive_errors = 0
                logger.info("Resilient subscriber '%s' %sconnected",
                            self.name, "" if initial else "re")
                return True
            except Exception as exc:  # noqa: BLE001
                self._inner = None
                logger.error("Resilient subscriber '%s' connect failed: %s",
                             self.name, exc)
                logger.error("Full stack trace:\n%s", traceback.format_exc())
                return False

    def _do_subscribe(self):
        """Pull one message; on error back off, reconnect, return None."""
        try:
            with self._inner_lock:
                inner = self._inner
            if inner is None:
                time.sleep(self.retry_backoff_seconds)
                self._connect_inner()
                return None
            data = inner._do_subscribe()
            self._consecutive_errors = 0
            return data
        except Exception as exc:  # noqa: BLE001
            self._consecutive_errors += 1
            logger.error("Resilient subscriber '%s' receive error #%d: %s",
                         self.name, self._consecutive_errors, exc)
            logger.error("Full stack trace:\n%s", traceback.format_exc())
            # Exponential backoff capped by retry_max doublings.
            doublings = min(self._consecutive_errors, self.retry_max)
            time.sleep(self.retry_backoff_seconds * (2 ** (doublings - 1)))
            self._connect_inner()
            return None

    def details(self):
        info = super().details()
        info['resilient'] = True
        info['retry_max'] = self.retry_max
        info['consecutive_errors'] = self._consecutive_errors
        return info

    def stop(self):
        """Stop the wrapper and the inner subscriber."""
        super().stop()
        with self._inner_lock:
            if self._inner is not None:
                try:
                    self._inner.stop()
                except Exception:  # noqa: BLE001
                    logger.error("Error stopping inner subscriber '%s':\n%s",
                                 self.name, traceback.format_exc())
