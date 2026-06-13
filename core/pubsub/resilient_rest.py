"""
Resilient REST DataPublisher / DataSubscriber (v2.0.0)
======================================================

Adds retry / backoff / outage buffering / automatic re-initialization on top
of the plain :mod:`core.pubsub.rest_datapubsub` implementations using the
generic wrappers from :mod:`core.pubsub.resilient_base`.

An unreachable or flapping HTTP endpoint no longer loses messages: POSTs are
retried with exponential backoff, buffered in memory while the endpoint is
down (bounded by ``buffer_max_messages``), and drained automatically when it
returns.  The polling subscriber backs off instead of hammering a dead
endpoint.

Usage in a DAG config (same rest:// destination/source, extra keys)::

    {
      "name": "webhook_out",
      "config": {
        "destination": "rest://api.example.com/ingest",
        "use_ssl": true,
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


class ResilientRESTDataPublisher(ResilientDataPublisherWrapper):
    """REST publisher with retry, buffering, and automatic recovery."""

    def __init__(self, name, destination, config):
        def inner_factory():
            from core.pubsub.rest_datapubsub import RESTDataPublisher
            inner_config = dict(config)
            inner_config['publish_interval'] = 0
            return RESTDataPublisher(f"{name}_inner", destination, inner_config)

        super().__init__(name, destination, config, inner_factory)


class ResilientRESTDataSubscriber(ResilientDataSubscriberWrapper):
    """REST polling subscriber with backoff and automatic recovery."""

    def __init__(self, name, source, config, given_queue: queue.Queue = None):
        def inner_factory():
            from core.pubsub.rest_datapubsub import RESTDataSubscriber
            return RESTDataSubscriber(f"{name}_inner", source, dict(config))

        super().__init__(name, source, config, inner_factory, given_queue)
