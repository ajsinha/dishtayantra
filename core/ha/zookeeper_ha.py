"""
Zookeeper HA Provider
=====================

Leader election via Apache Zookeeper using kazoo's Election recipe.

Required properties:
    ha.zookeeper.hosts          e.g. ``zk1:2181,zk2:2181,zk3:2181``
    ha.zookeeper.election_path  znode path, e.g. ``/dishtayantra/election``

Behaviour:
    - A daemon thread connects to Zookeeper and joins the election.
    - When elected, the recipe blocks inside our leadership function; we mark
      this instance primary and hold leadership until stop() is called or the
      Zookeeper session is lost (in which case we demote and rejoin).
    - If Zookeeper is unreachable at startup the provider keeps retrying;
      it never silently promotes itself (split-brain safety).

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import threading
import traceback

from core.ha.ha_manager import HAProvider, HAConfigurationError

logger = logging.getLogger(__name__)


class ZookeeperHAProvider(HAProvider):
    """Primary/secondary failover backed by Zookeeper leader election."""

    def __init__(self, hosts: str, election_path: str):
        super().__init__(name="zookeeper")
        if not hosts:
            raise HAConfigurationError("ha.zookeeper.hosts must be set")
        if not election_path:
            raise HAConfigurationError("ha.zookeeper.election_path must be set")
        try:
            from kazoo.client import KazooClient  # noqa: F401 - validate import
        except ImportError as exc:
            raise HAConfigurationError(
                "kazoo is not installed but ha.provider=zookeeper was "
                "selected. Install with: pip install kazoo"
            ) from exc
        self.hosts = hosts
        self.election_path = election_path
        self._client = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread = None
        self._leadership_release = threading.Event()

    def start(self) -> None:
        """Join the election in a background daemon thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._election_loop, daemon=True,
            name="ZookeeperHAProvider")
        self._thread.start()
        logger.info("ZookeeperHAProvider started (hosts=%s path=%s)",
                    self.hosts, self.election_path)

    def _election_loop(self) -> None:
        """Connect, join the election, and hold leadership while elected."""
        from kazoo.client import KazooClient
        from kazoo.recipe.election import Election

        while not self._stop_event.is_set():
            try:
                self._client = KazooClient(hosts=self.hosts)
                self._client.add_listener(self._connection_listener)
                self._client.start(timeout=10)
                election = Election(self._client, self.election_path)
                logger.info("ZookeeperHAProvider: joining election at %s",
                            self.election_path)
                election.run(self._hold_leadership)
            except Exception as exc:  # noqa: BLE001
                logger.error("ZookeeperHAProvider election error: %s", exc)
                logger.error("Full stack trace:\n%s", traceback.format_exc())
                self._transition_to_secondary()
            finally:
                self._close_client()
            if not self._stop_event.is_set():
                logger.warning(
                    "ZookeeperHAProvider: election ended, rejoining in 5s")
                self._stop_event.wait(5)

    def _hold_leadership(self) -> None:
        """Election callback: we are now the leader; hold until released."""
        self._transition_to_primary()
        self._leadership_release.clear()
        self._leadership_release.wait()  # block to retain leadership
        self._transition_to_secondary()

    def _connection_listener(self, state) -> None:
        """Demote on Zookeeper session loss/suspension."""
        state_name = str(state)
        if state_name in ("LOST", "SUSPENDED", "KazooState.LOST",
                          "KazooState.SUSPENDED"):
            logger.warning("ZookeeperHAProvider: session %s - demoting",
                           state_name)
            self._leadership_release.set()

    def _close_client(self) -> None:
        if self._client is not None:
            try:
                self._client.stop()
                self._client.close()
            except Exception as exc:  # noqa: BLE001
                logger.error("ZookeeperHAProvider close error: %s", exc)
                logger.error("Full stack trace:\n%s", traceback.format_exc())
            self._client = None

    def stop(self) -> None:
        """Relinquish leadership and stop participating."""
        self._stop_event.set()
        self._leadership_release.set()
        self._close_client()
        self._transition_to_secondary()
        logger.info("ZookeeperHAProvider stopped")
