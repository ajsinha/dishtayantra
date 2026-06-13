"""
Azure Event Hubs DataPublisher / DataSubscriber (v2.2)
======================================================

Pub/sub over Azure Event Hubs - the Azure-native high-throughput event
streaming service, the analog to AWS Kinesis and Kafka. Recommended Azure
transport for market-data fan-in: partitioned, ordered within a partition,
and replayable.

URI format::

    eventhubs://<eventhub-name>

Config keys:
    connection_string   REQUIRED. The Event Hubs namespace connection
                        string (set directly or via
                        ${AZURE_EVENTHUBS_CONNECTION_STRING}).

Publisher config:
    partition_key       Static partition key, or
    partition_key_field Message field whose value becomes the partition key
                        (related events stay ordered on one partition).

Subscriber config:
    consumer_group      Consumer group (default '$Default').
    starting_position   '-1' = end/latest (default), '@latest', or
                        '-1'/'@earliest' for the beginning.

Note: this subscriber reads with an in-process EventHubConsumerClient and
does NOT checkpoint to Azure Blob - on restart it resumes from
``starting_position``, not from the last consumed offset. For exactly-once
production checkpointing, front it with a BlobCheckpointStore (a future
enhancement); for streaming and development this is sufficient.

Resilience: set ``"resilient": true`` to wrap either endpoint.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import queue as _queue
import threading
import traceback

from core.pubsub.datapubsub import DataPublisher
from core.pubsub.datasubscriber import DataSubscriber

logger = logging.getLogger(__name__)

_PREFIX = 'eventhubs://'


def _strip_scheme(uri: str) -> str:
    return uri[len(_PREFIX):] if uri.startswith(_PREFIX) else uri


def _require_connection_string(config: dict) -> str:
    cs = config.get('connection_string')
    if not cs:
        raise ValueError(
            "eventhubs:// pub/sub requires 'connection_string' in the "
            "config (the Azure Event Hubs namespace connection string). "
            "Set it directly or via ${AZURE_EVENTHUBS_CONNECTION_STRING}.")
    return cs


class EventHubsDataPublisher(DataPublisher):
    """Publishes events to an Azure Event Hub."""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)
        try:
            from azure.eventhub import EventData, EventHubProducerClient
        except ImportError as exc:
            raise ImportError(
                "azure-eventhub is required for eventhubs:// pub/sub. "
                "Install with: pip install azure-eventhub") from exc
        self._EventData = EventData
        self._conn = _require_connection_string(config)
        self._eventhub = _strip_scheme(destination)
        self._partition_key = config.get('partition_key')
        self._partition_key_field = config.get('partition_key_field')
        self._producer = EventHubProducerClient.from_connection_string(
            self._conn, eventhub_name=self._eventhub)
        logger.info(f"EventHubsDataPublisher {name} -> {self._eventhub}")

    def _resolve_partition_key(self, data):
        if self._partition_key:
            return str(self._partition_key)
        if self._partition_key_field and isinstance(data, dict):
            value = data.get(self._partition_key_field)
            if value is not None:
                return str(value)
        return None

    def _do_publish(self, data):
        body = data if isinstance(data, str) else json.dumps(data)
        pk = self._resolve_partition_key(data)
        try:
            kwargs = {'partition_key': pk} if pk else {}
            batch = self._producer.create_batch(**kwargs)
            batch.add(self._EventData(body))
            self._producer.send_batch(batch)
        except Exception:
            logger.error(f"EventHubsDataPublisher {self.name} send failed")
            logger.error(traceback.format_exc())
            raise

    def stop(self):
        try:
            self._producer.close()
        except Exception:
            logger.error(traceback.format_exc())
        super().stop()

    def details(self):
        d = super().details()
        d.update({'type': 'EventHubsDataPublisher',
                  'eventhub': self._eventhub})
        return d


class EventHubsDataSubscriber(DataSubscriber):
    """Consumes events from an Azure Event Hub.

    A background EventHubConsumerClient pumps events into a local queue,
    which _do_subscribe drains - bridging the SDK's callback model onto the
    framework's pull-based _do_subscribe contract.
    """

    def __init__(self, name, source, config):
        super().__init__(name, source, config)
        try:
            from azure.eventhub import EventHubConsumerClient
        except ImportError as exc:
            raise ImportError(
                "azure-eventhub is required for eventhubs:// pub/sub. "
                "Install with: pip install azure-eventhub") from exc
        self._EventHubConsumerClient = EventHubConsumerClient
        self._conn = _require_connection_string(config)
        self._eventhub = _strip_scheme(source)
        self._consumer_group = config.get('consumer_group', '$Default')
        self._starting_position = config.get('starting_position', '-1')
        self._bridge = _queue.Queue(maxsize=self.max_depth)
        self._consumer = None
        self._consumer_thread = None
        self._consumer_started = False
        logger.info(f"EventHubsDataSubscriber {name} <- {self._eventhub} "
                    f"(group {self._consumer_group})")

    def _on_event(self, partition_context, event):
        if event is None:
            return
        body = event.body_as_str()
        try:
            decoded = json.loads(body)
        except (json.JSONDecodeError, TypeError):
            decoded = body
        # v3.0.0 ZERO-LOSS: never drop. Block-retry until the bridge has room.
        enqueued = False
        while not enqueued:
            try:
                self._bridge.put(decoded, timeout=1)
                enqueued = True
            except _queue.Full:
                logger.warning(f"EventHubs bridge queue full for {self.name}; "
                               f"applying backpressure (will retry, not drop)")
                continue

    def _start_consumer(self):
        self._consumer = self._EventHubConsumerClient.from_connection_string(
            self._conn, consumer_group=self._consumer_group,
            eventhub_name=self._eventhub)

        def run():
            try:
                self._consumer.receive(
                    on_event=self._on_event,
                    starting_position=self._starting_position)
            except Exception:
                logger.error(f"EventHubs consumer loop error for "
                             f"{self.name}")
                logger.error(traceback.format_exc())

        self._consumer_thread = threading.Thread(target=run, daemon=True)
        self._consumer_thread.start()
        self._consumer_started = True

    def _do_subscribe(self):
        try:
            if not self._consumer_started:
                self._start_consumer()
            try:
                return self._bridge.get(timeout=1)
            except _queue.Empty:
                return None
        except Exception:
            logger.error(f"EventHubsDataSubscriber {self.name} failed")
            logger.error(traceback.format_exc())
            raise

    def stop(self):
        try:
            if self._consumer is not None:
                self._consumer.close()
        except Exception:
            logger.error(traceback.format_exc())
        super().stop()

    def details(self):
        d = super().details()
        d.update({'type': 'EventHubsDataSubscriber',
                  'eventhub': self._eventhub,
                  'consumer_group': self._consumer_group})
        return d
