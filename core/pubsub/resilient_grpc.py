"""
Resilient gRPC DataPublisher / DataSubscriber (v2.0.0)
======================================================

Adds retry / backoff / outage buffering / automatic channel re-establishment
on top of the plain :mod:`core.pubsub.grpc_datapubsub` implementations using
the generic wrappers from :mod:`core.pubsub.resilient_base`.

A restarted or temporarily unreachable gRPC server no longer loses messages:
sends are retried with exponential backoff, buffered in memory while the
channel is down (bounded by ``buffer_max_messages``), and drained when the
channel is re-established.  The streaming subscriber tears down and rebuilds
its channel with backoff rather than spinning on a broken stream.

Usage in a DAG config (same grpc:// destination/source, extra keys)::

    {
      "name": "grpc_out",
      "config": {
        "destination": "grpc://feeds.example.com:50051/prices",
        "use_ssl": false,
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


class ResilientGRPCDataPublisher(ResilientDataPublisherWrapper):
    """gRPC publisher with retry, buffering, and channel re-establishment."""

    def __init__(self, name, destination, config):
        def inner_factory():
            from core.pubsub.grpc_datapubsub import GRPCDataPublisher
            inner_config = dict(config)
            inner_config['publish_interval'] = 0
            return GRPCDataPublisher(f"{name}_inner", destination, inner_config)

        super().__init__(name, destination, config, inner_factory)


class ResilientGRPCDataSubscriber(ResilientDataSubscriberWrapper):
    """gRPC streaming subscriber with backoff and channel re-establishment."""

    def __init__(self, name, source, config, given_queue: queue.Queue = None):
        def inner_factory():
            from core.pubsub.grpc_datapubsub import GRPCDataSubscriber
            return GRPCDataSubscriber(f"{name}_inner", source, dict(config))

        super().__init__(name, source, config, inner_factory, given_queue)
