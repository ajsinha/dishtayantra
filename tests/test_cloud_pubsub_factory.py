"""
Tests for the v2.2 AWS/Azure managed-messaging pub/sub: URI parsing,
factory scheme dispatch, the resilient-wrapper routing, and fail-fast
validation. SDKs (boto3, azure-*) are mocked so these run without the
cloud libraries installed - and the modules themselves import lazily, so
their mere import is also covered.
"""
import sys
import types

import pytest


# ---------------------------------------------------------------- mocks
@pytest.fixture()
def mock_boto3(monkeypatch):
    boto3 = types.ModuleType('boto3')

    class FakeSQS:
        def get_queue_url(self, QueueName):
            return {'QueueUrl': f'https://sqs.local/{QueueName}'}
        def send_message(self, **k):
            self.last = k
        def receive_message(self, **k):
            return {'Messages': []}
        def delete_message(self, **k):
            pass

    class FakeKinesis:
        def describe_stream(self, **k):
            return {'StreamDescription': {'Shards': [{'ShardId': 's1'}]}}
        def get_shard_iterator(self, **k):
            return {'ShardIterator': 'it1'}
        def get_records(self, **k):
            return {'Records': [], 'NextShardIterator': 'it2'}
        def put_record(self, **k):
            self.last = k

    class FakeSNS:
        def create_topic(self, Name):
            return {'TopicArn': f'arn:aws:sns:local::{Name}'}
        def publish(self, **k):
            self.last = k

    boto3.client = lambda svc, **k: {
        'sqs': FakeSQS(), 'kinesis': FakeKinesis(), 'sns': FakeSNS()}[svc]
    monkeypatch.setitem(sys.modules, 'boto3', boto3)
    return boto3


@pytest.fixture()
def mock_azure(monkeypatch):
    az = types.ModuleType('azure')
    sb_mod = types.ModuleType('azure.servicebus')

    class FakeMsg:
        def __init__(self, body):
            self.body = body
        def __str__(self):
            return self.body

    class FakeSender:
        def __enter__(self):
            return self
        def __exit__(self, *a):
            pass
        def send_messages(self, m):
            self.sent = m

    class FakeSBClient:
        @classmethod
        def from_connection_string(cls, cs):
            return cls()
        def get_queue_sender(self, queue_name):
            return FakeSender()
        def get_topic_sender(self, topic_name):
            return FakeSender()
        def get_queue_receiver(self, queue_name):
            return self
        def get_subscription_receiver(self, **k):
            return self
        def receive_messages(self, **k):
            return []
        def close(self):
            pass

    sb_mod.ServiceBusClient = FakeSBClient
    sb_mod.ServiceBusMessage = FakeMsg
    az.servicebus = sb_mod

    eh_mod = types.ModuleType('azure.eventhub')

    class FakeBatch:
        def add(self, e):
            pass

    class FakeProducer:
        @classmethod
        def from_connection_string(cls, cs, eventhub_name=None):
            return cls()
        def create_batch(self, **k):
            return FakeBatch()
        def send_batch(self, b):
            self.sent = b
        def close(self):
            pass

    class FakeConsumer:
        @classmethod
        def from_connection_string(cls, cs, **k):
            return cls()
        def receive(self, **k):
            pass
        def close(self):
            pass

    eh_mod.EventData = lambda body: ('EventData', body)
    eh_mod.EventHubProducerClient = FakeProducer
    eh_mod.EventHubConsumerClient = FakeConsumer
    az.eventhub = eh_mod

    monkeypatch.setitem(sys.modules, 'azure', az)
    monkeypatch.setitem(sys.modules, 'azure.servicebus', sb_mod)
    monkeypatch.setitem(sys.modules, 'azure.eventhub', eh_mod)
    return az


AZ_CS = ('Endpoint=sb://x.servicebus.windows.net/;'
         'SharedAccessKeyName=k;SharedAccessKey=v')


# ---------------------------------------------------------------- imports
def test_modules_import_without_sdk():
    """Lazy SDK imports: the modules load even with no boto3/azure."""
    import importlib
    for mod in ['core.pubsub.sqs_datapubsub',
                'core.pubsub.kinesis_datapubsub',
                'core.pubsub.sns_datapubsub',
                'core.pubsub.servicebus_datapubsub',
                'core.pubsub.eventhubs_datapubsub']:
        importlib.import_module(mod)


def test_servicebus_uri_parsing():
    from core.pubsub.servicebus_datapubsub import _parse_target
    assert _parse_target('servicebus://queue/orders') == \
        ('queue', 'orders', None, None)
    assert _parse_target('servicebus://topic/ticks/risk') == \
        ('topic', None, 'ticks', 'risk')
    assert _parse_target('servicebus://topic/ticks') == \
        ('topic', None, 'ticks', None)
    with pytest.raises(ValueError):
        _parse_target('servicebus://bogus/x')


# ---------------------------------------------------------------- AWS
def test_aws_publisher_dispatch(mock_boto3):
    from core.pubsub.pubsubfactory import create_publisher
    assert type(create_publisher('p', {
        'destination': 'sqs://orders', 'region': 'us-east-1'})
        ).__name__ == 'SQSDataPublisher'
    assert type(create_publisher('p', {
        'destination': 'kinesis://ticks', 'region': 'us-east-1'})
        ).__name__ == 'KinesisDataPublisher'
    assert type(create_publisher('p', {
        'destination': 'sns://alerts', 'region': 'us-east-1'})
        ).__name__ == 'SNSDataPublisher'


def test_aws_subscriber_dispatch(mock_boto3):
    from core.pubsub.pubsubfactory import create_subscriber
    assert type(create_subscriber('s', {
        'source': 'sqs://orders', 'region': 'us-east-1'})
        ).__name__ == 'SQSDataSubscriber'
    assert type(create_subscriber('s', {
        'source': 'kinesis://ticks', 'region': 'us-east-1'})
        ).__name__ == 'KinesisDataSubscriber'


def test_resilient_flag_wraps(mock_boto3):
    from core.pubsub.pubsubfactory import (
        create_publisher,
        create_subscriber,
    )
    assert type(create_publisher('p', {
        'destination': 'sqs://orders', 'resilient': True,
        'region': 'us-east-1'})).__name__ == 'ResilientDataPublisherWrapper'
    assert type(create_subscriber('s', {
        'source': 'kinesis://ticks', 'resilient': True,
        'region': 'us-east-1'})).__name__ == 'ResilientDataSubscriberWrapper'


def test_sqs_publish_serializes_dict(mock_boto3):
    from core.pubsub.pubsubfactory import create_publisher
    p = create_publisher('p', {'destination': 'sqs://orders',
                               'region': 'us-east-1'})
    p._do_publish({'order': 1})
    assert p._client.last['MessageBody'] == '{"order": 1}'


def test_kinesis_partition_key_field(mock_boto3):
    from core.pubsub.pubsubfactory import create_publisher
    p = create_publisher('p', {'destination': 'kinesis://ticks',
                               'partition_key_field': 'sym',
                               'region': 'us-east-1'})
    p._do_publish({'sym': 'AAPL', 'px': 100})
    assert p._client.last['PartitionKey'] == 'AAPL'


def test_sqs_subscriber_empty_returns_none(mock_boto3):
    from core.pubsub.pubsubfactory import create_subscriber
    s = create_subscriber('s', {'source': 'sqs://orders',
                                'region': 'us-east-1'})
    assert s._do_subscribe() is None


# ---------------------------------------------------------------- Azure
def test_azure_publisher_dispatch(mock_azure):
    from core.pubsub.pubsubfactory import create_publisher
    p1 = create_publisher('p', {'destination': 'servicebus://queue/orders',
                                'connection_string': AZ_CS})
    assert type(p1).__name__ == 'ServiceBusDataPublisher'
    assert p1._kind == 'queue'
    p2 = create_publisher('p', {'destination': 'eventhubs://market',
                                'connection_string': AZ_CS})
    assert type(p2).__name__ == 'EventHubsDataPublisher'


def test_azure_subscriber_dispatch(mock_azure):
    from core.pubsub.pubsubfactory import create_subscriber
    s = create_subscriber('s', {
        'source': 'servicebus://topic/ticks/risk',
        'connection_string': AZ_CS})
    assert type(s).__name__ == 'ServiceBusDataSubscriber'
    assert s._subscription == 'risk'


def test_servicebus_requires_connection_string(mock_azure):
    from core.pubsub.pubsubfactory import create_publisher
    with pytest.raises(ValueError):
        create_publisher('p', {'destination': 'servicebus://queue/x'})


def test_servicebus_topic_subscriber_needs_subscription(mock_azure):
    from core.pubsub.pubsubfactory import create_subscriber
    with pytest.raises(ValueError):
        create_subscriber('s', {'source': 'servicebus://topic/ticks',
                                'connection_string': AZ_CS})
