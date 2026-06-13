"""
Amazon SQS DataPublisher / DataSubscriber (v2.2)
================================================

Pub/sub over Amazon Simple Queue Service. SQS gives real queue semantics -
at-least-once delivery, visibility timeouts, and dead-letter queues - which
makes it the AWS-native workhorse for decoupled processing.

URI format::

    sqs://<queue-name>
    sqs://<full-queue-url>          # https://sqs.<region>.amazonaws.com/<acct>/<name>

Common config keys (beyond the standard DataPublisher/Subscriber keys):
    region              AWS region (recommended; required if not in the URL)
    endpoint_url        Custom endpoint (localstack / VPC endpoint)
    access_key_id / secret_access_key
                        Explicit credentials; otherwise boto3's chain.
    fifo                When true, treats the queue as FIFO and sends
                        MessageGroupId (default 'dishtayantra') with each
                        message for ordered delivery.

Subscriber additionally supports:
    wait_time_seconds   Long-poll wait (default 10, max 20).
    max_messages        Messages per receive call (default 10, max 10).
    visibility_timeout  Seconds a received message stays hidden (optional).
    delete_on_read      Delete after delivery (default true; queue
                        semantics). When false, messages reappear after the
                        visibility timeout - useful for at-least-once
                        replay during development.

Resilience: set ``"resilient": true`` in the DAG config to wrap either
endpoint with retry / outage-buffering / reconnect via the factory.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import traceback

from core.pubsub.datapubsub import DataPublisher
from core.pubsub.datasubscriber import DataSubscriber

logger = logging.getLogger(__name__)


def _strip_scheme(uri: str) -> str:
    return uri[len('sqs://'):] if uri.startswith('sqs://') else uri


def _boto3_sqs(config: dict):
    """Build a boto3 SQS client from config (lazy import)."""
    try:
        import boto3
    except ImportError as exc:
        raise ImportError(
            "boto3 is required for sqs:// pub/sub. "
            "Install with: pip install boto3") from exc
    kwargs = {}
    if config.get('region'):
        kwargs['region_name'] = config['region']
    if config.get('endpoint_url'):
        kwargs['endpoint_url'] = config['endpoint_url']
    if config.get('access_key_id') and config.get('secret_access_key'):
        kwargs['aws_access_key_id'] = config['access_key_id']
        kwargs['aws_secret_access_key'] = config['secret_access_key']
    return boto3.client('sqs', **kwargs)


def _resolve_queue_url(client, target: str) -> str:
    """Accept either a queue name or a full queue URL."""
    if target.startswith('http://') or target.startswith('https://'):
        return target
    return client.get_queue_url(QueueName=target)['QueueUrl']


class SQSDataPublisher(DataPublisher):
    """Publishes messages to an Amazon SQS queue."""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)
        self._fifo = bool(config.get('fifo', False))
        self._group_id = config.get('message_group_id', 'dishtayantra')
        self._client = _boto3_sqs(config)
        self._queue_url = _resolve_queue_url(self._client,
                                             _strip_scheme(destination))
        logger.info(f"SQSDataPublisher {name} -> {self._queue_url} "
                    f"(fifo={self._fifo})")

    def _do_publish(self, data):
        body = data if isinstance(data, str) else json.dumps(data)
        kwargs = {'QueueUrl': self._queue_url, 'MessageBody': body}
        if self._fifo:
            kwargs['MessageGroupId'] = self._group_id
        try:
            self._client.send_message(**kwargs)
        except Exception:
            logger.error(f"SQSDataPublisher {self.name} send failed")
            logger.error(traceback.format_exc())
            raise

    def details(self):
        d = super().details()
        d.update({'type': 'SQSDataPublisher',
                  'queue_url': self._queue_url, 'fifo': self._fifo})
        return d


class SQSDataSubscriber(DataSubscriber):
    """Consumes messages from an Amazon SQS queue (long-polling)."""

    def __init__(self, name, source, config):
        super().__init__(name, source, config)
        self._wait_time = min(int(config.get('wait_time_seconds', 10)), 20)
        self._max_messages = min(int(config.get('max_messages', 10)), 10)
        self._visibility_timeout = config.get('visibility_timeout')
        self._delete_on_read = bool(config.get('delete_on_read', True))
        self._client = _boto3_sqs(config)
        self._queue_url = _resolve_queue_url(self._client,
                                             _strip_scheme(source))
        self._pending = []  # buffered messages from the last receive batch
        logger.info(f"SQSDataSubscriber {name} <- {self._queue_url}")

    def _receive_batch(self):
        kwargs = {'QueueUrl': self._queue_url,
                  'MaxNumberOfMessages': self._max_messages,
                  'WaitTimeSeconds': self._wait_time}
        if self._visibility_timeout is not None:
            kwargs['VisibilityTimeout'] = int(self._visibility_timeout)
        resp = self._client.receive_message(**kwargs)
        return resp.get('Messages', [])

    def _do_subscribe(self):
        try:
            if not self._pending:
                self._pending = self._receive_batch()
            if not self._pending:
                return None
            msg = self._pending.pop(0)
            body = msg.get('Body', '')
            if self._delete_on_read:
                self._client.delete_message(
                    QueueUrl=self._queue_url,
                    ReceiptHandle=msg['ReceiptHandle'])
            try:
                return json.loads(body)
            except (json.JSONDecodeError, TypeError):
                return body
        except Exception:
            logger.error(f"SQSDataSubscriber {self.name} receive failed")
            logger.error(traceback.format_exc())
            raise

    def details(self):
        d = super().details()
        d.update({'type': 'SQSDataSubscriber',
                  'queue_url': self._queue_url,
                  'delete_on_read': self._delete_on_read})
        return d
