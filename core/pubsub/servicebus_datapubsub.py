"""
Azure Service Bus DataPublisher / DataSubscriber (v2.2)
=======================================================

Pub/sub over Azure Service Bus - the Azure-native enterprise messaging
service, the direct analog to AWS SQS/SNS. Supports both queues (point to
point) and topics+subscriptions (publish/subscribe), with sessions for
ordering and a dead-letter sub-queue.

URI formats::

    servicebus://queue/<queue-name>
    servicebus://topic/<topic-name>/<subscription-name>   # subscriber
    servicebus://topic/<topic-name>                       # publisher

Config keys:
    connection_string   REQUIRED. The Service Bus namespace connection
                        string (or set the
                        AZURE_SERVICEBUS_CONNECTION_STRING env var and
                        reference it with ${...} in the DAG config).

Subscriber config:
    max_wait_seconds    Receive wait time (default 10).
    max_messages        Messages per receive call (default 10).
    delete_on_read      Complete (ack) messages after delivery (default
                        true). When false, messages are abandoned and
                        redelivered - at-least-once replay.

Resilience: set ``"resilient": true`` to wrap either endpoint.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import traceback

from core.pubsub.datapubsub import DataPublisher
from core.pubsub.datasubscriber import DataSubscriber

logger = logging.getLogger(__name__)

_PREFIX = 'servicebus://'


def _require_connection_string(config: dict) -> str:
    cs = config.get('connection_string')
    if not cs:
        raise ValueError(
            "servicebus:// pub/sub requires 'connection_string' in the "
            "config (the Azure Service Bus namespace connection string). "
            "Set it directly or via ${AZURE_SERVICEBUS_CONNECTION_STRING}.")
    return cs


def _import_servicebus():
    try:
        from azure.servicebus import (  # noqa: F401
            ServiceBusClient,
            ServiceBusMessage,
        )
    except ImportError as exc:
        raise ImportError(
            "azure-servicebus is required for servicebus:// pub/sub. "
            "Install with: pip install azure-servicebus") from exc
    from azure.servicebus import ServiceBusClient, ServiceBusMessage
    return ServiceBusClient, ServiceBusMessage


def _parse_target(uri: str):
    """Return (kind, queue_name, topic_name, subscription_name)."""
    rest = uri[len(_PREFIX):] if uri.startswith(_PREFIX) else uri
    parts = [p for p in rest.split('/') if p]
    if not parts:
        raise ValueError(f"Malformed servicebus URI: {uri}")
    if parts[0] == 'queue':
        if len(parts) < 2:
            raise ValueError(f"servicebus queue URI needs a name: {uri}")
        return 'queue', parts[1], None, None
    if parts[0] == 'topic':
        if len(parts) == 2:
            return 'topic', None, parts[1], None
        if len(parts) >= 3:
            return 'topic', None, parts[1], parts[2]
        raise ValueError(f"servicebus topic URI needs a name: {uri}")
    raise ValueError(f"servicebus URI must be queue/ or topic/: {uri}")


class ServiceBusDataPublisher(DataPublisher):
    """Publishes to an Azure Service Bus queue or topic."""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)
        self._ServiceBusClient, self._ServiceBusMessage = \
            _import_servicebus()
        self._conn = _require_connection_string(config)
        kind, queue, topic, _ = _parse_target(destination)
        self._kind = kind
        self._entity = queue if kind == 'queue' else topic
        self._client = self._ServiceBusClient.from_connection_string(
            self._conn)
        logger.info(f"ServiceBusDataPublisher {name} -> {kind}/"
                    f"{self._entity}")

    def _sender(self):
        if self._kind == 'queue':
            return self._client.get_queue_sender(queue_name=self._entity)
        return self._client.get_topic_sender(topic_name=self._entity)

    def _do_publish(self, data):
        body = data if isinstance(data, str) else json.dumps(data)
        try:
            with self._sender() as sender:
                sender.send_messages(self._ServiceBusMessage(body))
        except Exception:
            logger.error(f"ServiceBusDataPublisher {self.name} send failed")
            logger.error(traceback.format_exc())
            raise

    def stop(self):
        try:
            self._client.close()
        except Exception:
            logger.error(traceback.format_exc())
        super().stop()

    def details(self):
        d = super().details()
        d.update({'type': 'ServiceBusDataPublisher',
                  'kind': self._kind, 'entity': self._entity})
        return d


class ServiceBusDataSubscriber(DataSubscriber):
    """Consumes from an Azure Service Bus queue or topic subscription."""

    def __init__(self, name, source, config):
        super().__init__(name, source, config)
        self._ServiceBusClient, _ = _import_servicebus()
        self._conn = _require_connection_string(config)
        kind, queue, topic, subscription = _parse_target(source)
        if kind == 'topic' and not subscription:
            raise ValueError(
                "servicebus topic SUBSCRIBER needs a subscription name: "
                "servicebus://topic/<topic>/<subscription>")
        self._kind = kind
        self._queue = queue
        self._topic = topic
        self._subscription = subscription
        self._max_wait = int(config.get('max_wait_seconds', 10))
        self._max_messages = int(config.get('max_messages', 10))
        self._delete_on_read = bool(config.get('delete_on_read', True))
        self._client = self._ServiceBusClient.from_connection_string(
            self._conn)
        self._receiver = None
        self._pending = []
        logger.info(f"ServiceBusDataSubscriber {name} <- {kind}/"
                    f"{queue or topic}"
                    f"{('/' + subscription) if subscription else ''}")

    def _ensure_receiver(self):
        if self._receiver is not None:
            return
        if self._kind == 'queue':
            self._receiver = self._client.get_queue_receiver(
                queue_name=self._queue)
        else:
            self._receiver = self._client.get_subscription_receiver(
                topic_name=self._topic,
                subscription_name=self._subscription)

    def _do_subscribe(self):
        try:
            self._ensure_receiver()
            if not self._pending:
                msgs = self._receiver.receive_messages(
                    max_message_count=self._max_messages,
                    max_wait_time=self._max_wait)
                self._pending = list(msgs)
            if not self._pending:
                return None
            msg = self._pending.pop(0)
            body = str(msg)
            if self._delete_on_read:
                self._receiver.complete_message(msg)
            else:
                self._receiver.abandon_message(msg)
            try:
                return json.loads(body)
            except (json.JSONDecodeError, TypeError):
                return body
        except Exception:
            logger.error(f"ServiceBusDataSubscriber {self.name} "
                         f"receive failed")
            logger.error(traceback.format_exc())
            raise

    def stop(self):
        try:
            if self._receiver is not None:
                self._receiver.close()
            self._client.close()
        except Exception:
            logger.error(traceback.format_exc())
        super().stop()

    def details(self):
        d = super().details()
        d.update({'type': 'ServiceBusDataSubscriber', 'kind': self._kind,
                  'entity': self._queue or self._topic,
                  'subscription': self._subscription,
                  'delete_on_read': self._delete_on_read})
        return d
