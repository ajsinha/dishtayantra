"""
Amazon SNS DataPublisher (v2.2)
===============================

Publish-only fan-out over Amazon Simple Notification Service. SNS broadcasts
each message to every subscription on a topic (SQS queues, Lambda, HTTP,
email). The canonical AWS pattern is SNS-to-SQS fan-out: publish once to a
topic, let several SQS queues each receive a copy, and consume those with
:class:`core.pubsub.sqs_datapubsub.SQSDataSubscriber`.

SNS has no native pull API, so this module provides a publisher only - to
consume SNS messages, subscribe an SQS queue to the topic and read that
queue with the sqs:// subscriber.

URI format::

    sns://<topic-name>
    sns://<full-topic-arn>          # arn:aws:sns:<region>:<acct>:<name>

Config keys:
    region              AWS region (required if not in the ARN).
    endpoint_url        Custom endpoint (localstack).
    access_key_id / secret_access_key
                        Explicit credentials; otherwise boto3's chain.
    subject             Optional Subject attached to each notification.
    fifo                When true, sends MessageGroupId for FIFO topics
                        (default group 'dishtayantra').

Resilience: set ``"resilient": true`` to wrap the publisher.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import traceback

from core.pubsub.datapubsub import DataPublisher

logger = logging.getLogger(__name__)


def _strip_scheme(uri: str) -> str:
    return uri[len('sns://'):] if uri.startswith('sns://') else uri


def _boto3_sns(config: dict):
    try:
        import boto3
    except ImportError as exc:
        raise ImportError(
            "boto3 is required for sns:// pub/sub. "
            "Install with: pip install boto3") from exc
    kwargs = {}
    if config.get('region'):
        kwargs['region_name'] = config['region']
    if config.get('endpoint_url'):
        kwargs['endpoint_url'] = config['endpoint_url']
    if config.get('access_key_id') and config.get('secret_access_key'):
        kwargs['aws_access_key_id'] = config['access_key_id']
        kwargs['aws_secret_access_key'] = config['secret_access_key']
    return boto3.client('sns', **kwargs)


class SNSDataPublisher(DataPublisher):
    """Publishes notifications to an Amazon SNS topic (fan-out)."""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)
        self._subject = config.get('subject')
        self._fifo = bool(config.get('fifo', False))
        self._group_id = config.get('message_group_id', 'dishtayantra')
        self._client = _boto3_sns(config)
        self._topic_arn = self._resolve_topic_arn(_strip_scheme(destination))
        logger.info(f"SNSDataPublisher {name} -> {self._topic_arn} "
                    f"(fifo={self._fifo})")

    def _resolve_topic_arn(self, target: str) -> str:
        if target.startswith('arn:'):
            return target
        # Create-or-get is idempotent and returns the ARN for the name.
        return self._client.create_topic(Name=target)['TopicArn']

    def _do_publish(self, data):
        body = data if isinstance(data, str) else json.dumps(data)
        kwargs = {'TopicArn': self._topic_arn, 'Message': body}
        if self._subject:
            kwargs['Subject'] = self._subject
        if self._fifo:
            kwargs['MessageGroupId'] = self._group_id
        try:
            self._client.publish(**kwargs)
        except Exception:
            logger.error(f"SNSDataPublisher {self.name} publish failed")
            logger.error(traceback.format_exc())
            raise

    def details(self):
        d = super().details()
        d.update({'type': 'SNSDataPublisher',
                  'topic_arn': self._topic_arn, 'fifo': self._fifo})
        return d
