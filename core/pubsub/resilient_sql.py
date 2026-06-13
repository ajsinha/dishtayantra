"""
Resilient SQL DataPublisher / DataSubscriber (v2.0.0)
=====================================================

Adds retry / backoff / outage buffering / automatic reconnection on top of
the plain :mod:`core.pubsub.sql_datapubsub` implementations using the
generic wrappers from :mod:`core.pubsub.resilient_base`.

A database restart, failover, or transient network partition no longer loses
messages: publishes are retried with exponential backoff, buffered in memory
while the database is down (bounded by ``buffer_max_messages``), and drained
automatically when connectivity returns.  The subscriber backs off and
rebuilds its connection instead of busy-spinning on a dead database.

Usage in a DAG config (same sql:// destination/source, extra keys)::

    {
      "name": "orders_out",
      "config": {
        "destination": "sql://orders_table",
        "connection_string": "${ORDERS_DB_URL}",
        "retry_max": 5,
        "retry_backoff_seconds": 2,
        "buffer_max_messages": 10000
      }
    }

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import queue

from core.pubsub.resilient_base import (
    ResilientDataPublisherWrapper,
    ResilientDataSubscriberWrapper,
)

logger = logging.getLogger(__name__)


class ResilientSQLDataPublisher(ResilientDataPublisherWrapper):
    """SQL publisher with retry, buffering, and auto-reconnect."""

    def __init__(self, name, destination, config):
        def inner_factory():
            from core.pubsub.sql_datapubsub import SQLDataPublisher
            inner_config = dict(config)
            inner_config['publish_interval'] = 0
            return SQLDataPublisher(f"{name}_inner", destination, inner_config)

        super().__init__(name, destination, config, inner_factory)


class ResilientSQLDataSubscriber(ResilientDataSubscriberWrapper):
    """SQL subscriber with backoff and auto-reconnect."""

    def __init__(self, name, source, config, given_queue: queue.Queue = None):
        def inner_factory():
            from core.pubsub.sql_datapubsub import SQLDataSubscriber
            return SQLDataSubscriber(f"{name}_inner", source, dict(config))

        super().__init__(name, source, config, inner_factory, given_queue)
