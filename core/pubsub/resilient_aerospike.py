"""
Resilient Aerospike DataPublisher (v2.0.0)
==========================================

Adds retry / backoff / outage buffering / automatic reconnection on top of
:class:`core.pubsub.aerospike_datapubsub.AerospikeDataPublisher` using the
generic wrappers from :mod:`core.pubsub.resilient_base`.

Usage in a DAG config (same destination format, extra resilience keys)::

    {
      "name": "aero_out",
      "config": {
        "destination": "aerospike://my_namespace:my_set",
        "hosts": [["127.0.0.1", 3000]],
        "retry_max": 5,
        "retry_backoff_seconds": 2,
        "buffer_max_messages": 10000
      }
    }

Construct via ``ResilientAerospikeDataPublisher(name, destination, config)``
or point a CustomDataPublisher at this class.

Note: Aerospike has no native subscriber in DishtaYantra (it is a key-value
sink), so only the publisher wrapper exists.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging

from core.pubsub.resilient_base import ResilientDataPublisherWrapper

logger = logging.getLogger(__name__)


class ResilientAerospikeDataPublisher(ResilientDataPublisherWrapper):
    """Aerospike publisher with retry, buffering, and auto-reconnect."""

    def __init__(self, name, destination, config):
        def inner_factory():
            from core.pubsub.aerospike_datapubsub import AerospikeDataPublisher
            # The inner publisher must not run its own periodic thread -
            # the wrapper owns scheduling; force immediate publishing inside.
            inner_config = dict(config)
            inner_config['publish_interval'] = 0
            return AerospikeDataPublisher(f"{name}_inner", destination,
                                          inner_config)

        super().__init__(name, destination, config, inner_factory)
