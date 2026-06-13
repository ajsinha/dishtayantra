"""
Socket HA Provider
==================

Zero-infrastructure primary/secondary failover using exclusive TCP port
binding ("spocket" mechanism):

    - All instances of the cluster run on hosts that can bind the SAME
      address (typically a shared host, a VIP, or a port on a shared
      load-balancer host reachable by every instance).
    - Whoever successfully binds ``ha.socket.host:ha.socket.port`` IS the
      primary - the operating system guarantees exclusivity, so split-brain
      is structurally impossible on a single host / VIP.
    - Secondaries retry the bind every ``ha.socket.retry_seconds`` seconds.
      The moment the primary process dies (even via kill -9) the kernel
      releases the port and exactly one secondary wins the next bind.
    - While primary, the bound socket also acts as a trivially scriptable
      health endpoint: any TCP connect succeeds and immediately receives a
      one-line status banner before the connection is closed.

Required properties:
    ha.socket.host           Address to bind (e.g. ``0.0.0.0`` or a VIP)
    ha.socket.port           Port to bind (e.g. ``5599``)
    ha.socket.retry_seconds  Secondary retry cadence (e.g. ``3``)

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import socket
import threading
import traceback

from core.ha.ha_manager import HAProvider, HAConfigurationError
from core.version import VERSION

logger = logging.getLogger(__name__)


class SocketHAProvider(HAProvider):
    """Exclusive TCP bind based failover (the 'spocket' mechanism)."""

    def __init__(self, host: str, port: int, retry_seconds: int):
        super().__init__(name="socket")
        if not host:
            raise HAConfigurationError("ha.socket.host must be set")
        if not (0 < port < 65536):
            raise HAConfigurationError(
                f"ha.socket.port must be 1-65535, got {port}")
        if retry_seconds < 1:
            raise HAConfigurationError("ha.socket.retry_seconds must be >= 1")
        self.host = host
        self.port = port
        self.retry_seconds = retry_seconds
        self._server_socket: socket.socket = None
        self._stop_event = threading.Event()
        self._bind_thread: threading.Thread = None
        self._accept_thread: threading.Thread = None

    # ------------------------------------------------------------------ #

    def start(self) -> None:
        """Begin trying to acquire the port in a daemon thread."""
        self._stop_event.clear()
        self._bind_thread = threading.Thread(
            target=self._bind_loop, daemon=True, name="SocketHAProvider-bind")
        self._bind_thread.start()
        logger.info("SocketHAProvider started (%s:%d retry=%ss)",
                    self.host, self.port, self.retry_seconds)

    def _bind_loop(self) -> None:
        """Retry the exclusive bind until acquired or stopped."""
        while not self._stop_event.is_set():
            if self._server_socket is None:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    # NOTE: deliberately NOT setting SO_REUSEADDR/SO_REUSEPORT
                    # - exclusivity of the bind IS the election mechanism.
                    sock.bind((self.host, self.port))
                    sock.listen(8)
                    sock.settimeout(1.0)
                    self._server_socket = sock
                    logger.info("SocketHAProvider: bound %s:%d - elected",
                                self.host, self.port)
                    self._accept_thread = threading.Thread(
                        target=self._accept_loop, daemon=True,
                        name="SocketHAProvider-accept")
                    self._accept_thread.start()
                    self._transition_to_primary()
                except OSError:
                    # Port held by the current primary - normal for a
                    # secondary; retry after the configured delay.
                    logger.debug(
                        "SocketHAProvider: %s:%d busy - standing by as "
                        "secondary", self.host, self.port)
                except Exception as exc:  # noqa: BLE001
                    logger.error("SocketHAProvider bind error: %s", exc)
                    logger.error("Full stack trace:\n%s",
                                 traceback.format_exc())
            self._stop_event.wait(self.retry_seconds)

    def _accept_loop(self) -> None:
        """Serve a one-line status banner to anyone probing the HA port."""
        banner = (f"DishtaYantra v{VERSION} HA primary on "
                  f"{socket.gethostname()}\n").encode("utf-8")
        while not self._stop_event.is_set() and self._server_socket is not None:
            try:
                conn, _addr = self._server_socket.accept()
                try:
                    conn.sendall(banner)
                finally:
                    conn.close()
            except socket.timeout:
                continue
            except OSError:
                break  # socket closed during stop()
            except Exception as exc:  # noqa: BLE001
                logger.error("SocketHAProvider accept error: %s", exc)
                logger.error("Full stack trace:\n%s", traceback.format_exc())

    def stop(self) -> None:
        """Release the port (demoting) and stop participating."""
        self._stop_event.set()
        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except Exception as exc:  # noqa: BLE001
                logger.error("SocketHAProvider close error: %s", exc)
                logger.error("Full stack trace:\n%s", traceback.format_exc())
            self._server_socket = None
        if self._bind_thread is not None:
            self._bind_thread.join(timeout=self.retry_seconds + 2)
        self._transition_to_secondary()
        logger.info("SocketHAProvider stopped")
