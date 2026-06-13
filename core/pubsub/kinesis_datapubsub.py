"""
Amazon Kinesis DataPublisher / DataSubscriber (v2.2)
====================================================

Pub/sub over Amazon Kinesis Data Streams - the AWS-native ordered, sharded,
replayable stream. This is the recommended AWS transport for market-data
fan-in: records within a shard are strictly ordered by sequence number, and
consumers can replay from any point.

URI format::

    kinesis://<stream-name>

Common config keys:
    region              AWS region (recommended).
    endpoint_url        Custom endpoint (localstack).
    access_key_id / secret_access_key
                        Explicit credentials; otherwise boto3's chain.

Publisher config:
    partition_key       Static partition key, or
    partition_key_field Message field whose value becomes the partition key
                        (so related records land on the same shard, in
                        order). Falls back to 'dishtayantra' when neither is
                        given.

Subscriber config:
    iterator_type       LATEST (default) | TRIM_HORIZON | AT_TIMESTAMP.
    timestamp           Epoch seconds, required when iterator_type is
                        AT_TIMESTAMP.
    poll_interval       Seconds between GetRecords calls (default 1).
    limit               Max records per GetRecords call (default 100).

Note: this subscriber reads all shards of the stream from a single process
(suitable for moderate throughput and development). For very high-volume
production streams, run one DAG/subscriber per shard or front the stream
with the Kinesis Client Library.

Resilience: set ``"resilient": true`` to wrap either endpoint.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import traceback

from core.pubsub.datapubsub import DataPublisher
from core.pubsub.datasubscriber import DataSubscriber

logger = logging.getLogger(__name__)


def _strip_scheme(uri: str) -> str:
    return uri[len('kinesis://'):] if uri.startswith('kinesis://') else uri


def _boto3_kinesis(config: dict):
    try:
        import boto3
    except ImportError as exc:
        raise ImportError(
            "boto3 is required for kinesis:// pub/sub. "
            "Install with: pip install boto3") from exc
    kwargs = {}
    if config.get('region'):
        kwargs['region_name'] = config['region']
    if config.get('endpoint_url'):
        kwargs['endpoint_url'] = config['endpoint_url']
    if config.get('access_key_id') and config.get('secret_access_key'):
        kwargs['aws_access_key_id'] = config['access_key_id']
        kwargs['aws_secret_access_key'] = config['secret_access_key']
    return boto3.client('kinesis', **kwargs)


class KinesisDataPublisher(DataPublisher):
    """Publishes records to an Amazon Kinesis Data Stream."""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)
        self._stream = _strip_scheme(destination)
        self._partition_key = config.get('partition_key')
        self._partition_key_field = config.get('partition_key_field')
        self._client = _boto3_kinesis(config)
        logger.info(f"KinesisDataPublisher {name} -> stream {self._stream}")

    def _resolve_partition_key(self, data) -> str:
        if self._partition_key:
            return str(self._partition_key)
        if self._partition_key_field and isinstance(data, dict):
            value = data.get(self._partition_key_field)
            if value is not None:
                return str(value)
        return 'dishtayantra'

    def _do_publish(self, data):
        body = data if isinstance(data, str) else json.dumps(data)
        try:
            self._client.put_record(
                StreamName=self._stream,
                Data=body.encode('utf-8'),
                PartitionKey=self._resolve_partition_key(data))
        except Exception:
            logger.error(f"KinesisDataPublisher {self.name} put failed")
            logger.error(traceback.format_exc())
            raise

    def details(self):
        d = super().details()
        d.update({'type': 'KinesisDataPublisher', 'stream': self._stream})
        return d


class KinesisDataSubscriber(DataSubscriber):
    """Consumes records from all shards of a Kinesis stream."""

    def __init__(self, name, source, config):
        super().__init__(name, source, config)
        self._stream = _strip_scheme(source)
        self._iterator_type = config.get('iterator_type', 'LATEST')
        self._timestamp = config.get('timestamp')
        self._limit = int(config.get('limit', 100))
        self._client = _boto3_kinesis(config)
        self._shard_iterators = {}   # shard_id -> iterator
        self._pending = []           # buffered decoded records
        self._initialized = False
        logger.info(f"KinesisDataSubscriber {name} <- stream "
                    f"{self._stream} ({self._iterator_type})")

    def _init_iterators(self):
        shards = self._client.describe_stream(
            StreamName=self._stream)['StreamDescription']['Shards']
        for shard in shards:
            kwargs = {'StreamName': self._stream,
                      'ShardId': shard['ShardId'],
                      'ShardIteratorType': self._iterator_type}
            if self._iterator_type == 'AT_TIMESTAMP' and self._timestamp:
                kwargs['Timestamp'] = float(self._timestamp)
            self._shard_iterators[shard['ShardId']] = \
                self._client.get_shard_iterator(**kwargs)['ShardIterator']
        self._initialized = True

    def _poll_shards(self):
        for shard_id, iterator in list(self._shard_iterators.items()):
            if iterator is None:
                continue
            resp = self._client.get_records(ShardIterator=iterator,
                                            Limit=self._limit)
            self._shard_iterators[shard_id] = resp.get('NextShardIterator')
            for record in resp.get('Records', []):
                raw = record['Data']
                text = raw.decode('utf-8') if isinstance(raw, bytes) else raw
                try:
                    self._pending.append(json.loads(text))
                except (json.JSONDecodeError, TypeError):
                    self._pending.append(text)

    def _do_subscribe(self):
        try:
            if not self._initialized:
                self._init_iterators()
            if not self._pending:
                self._poll_shards()
            if not self._pending:
                return None
            return self._pending.pop(0)
        except Exception:
            logger.error(f"KinesisDataSubscriber {self.name} poll failed")
            logger.error(traceback.format_exc())
            raise

    def details(self):
        d = super().details()
        d.update({'type': 'KinesisDataSubscriber', 'stream': self._stream,
                  'iterator_type': self._iterator_type,
                  'shards': len(self._shard_iterators)})
        return d
