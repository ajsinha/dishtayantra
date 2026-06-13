"""Tests for pub/sub routing helpers and the resilient wrappers."""
import pytest

from core.pubsub.objectstore_datapubsub import parse_objectstore_uri
from core.pubsub.resilient_base import (
    ResilientDataPublisherWrapper,
    ResilientDataSubscriberWrapper,
)


def test_parse_objectstore_uri():
    bucket, prefix = parse_objectstore_uri('s3://my-bucket/some/prefix', 's3')
    assert bucket == 'my-bucket'
    assert prefix == 'some/prefix'
    bucket, prefix = parse_objectstore_uri('gcs://b', 'gcs')
    assert bucket == 'b' and prefix == ''


def test_parse_objectstore_uri_rejects_wrong_scheme():
    with pytest.raises(ValueError):
        parse_objectstore_uri('s3://bucket/p', 'gcs')


class _FlakyInnerPublisher:
    """Inner publisher exposing the DataPublisher internals the wrapper
    relies on (_do_publish / stop), failing N times before recovering."""

    def __init__(self, fail_times):
        self.fail_times = fail_times
        self.published = []

    def _do_publish(self, data):
        if self.fail_times > 0:
            self.fail_times -= 1
            raise ConnectionError('broker down')
        self.published.append(data)

    def stop(self):
        pass


def test_resilient_publisher_retries_then_succeeds():
    inner = _FlakyInnerPublisher(fail_times=2)
    wrapper = ResilientDataPublisherWrapper(
        'test_pub', 'mock://broker',
        {'publish_interval': 0, 'retry_max': 5,
         'retry_backoff_seconds': 0.01},
        inner_factory=lambda: inner)
    wrapper.publish({'n': 1})
    assert inner.published == [{'n': 1}]
    wrapper.stop()


def test_resilient_publisher_buffers_then_drains():
    inner = _FlakyInnerPublisher(fail_times=10 ** 6)
    wrapper = ResilientDataPublisherWrapper(
        'test_pub2', 'mock://broker',
        {'publish_interval': 0, 'retry_max': 2,
         'retry_backoff_seconds': 0.01, 'buffer_max_messages': 100},
        inner_factory=lambda: inner)
    wrapper.publish({'n': 'queued'})
    assert len(wrapper._outage_buffer) == 1
    assert inner.published == []
    # Broker recovers: the next publish drains the buffer first (FIFO).
    inner.fail_times = 0
    wrapper.publish({'n': 'second'})
    assert inner.published == [{'n': 'queued'}, {'n': 'second'}]
    assert len(wrapper._outage_buffer) == 0
    wrapper.stop()


def test_resilient_publisher_reconnects_via_factory():
    """A failing connection must trigger a rebuild via inner_factory."""
    instances = []

    def factory():
        inner = _FlakyInnerPublisher(fail_times=0)
        instances.append(inner)
        return inner

    wrapper = ResilientDataPublisherWrapper(
        'test_pub3', 'mock://broker',
        {'publish_interval': 0, 'retry_max': 3,
         'retry_backoff_seconds': 0.01},
        inner_factory=factory)
    # Sabotage the live inner so the first attempt fails and forces a
    # reconnect; the freshly built instance then succeeds.
    instances[0].fail_times = 1
    wrapper.publish({'n': 'x'})
    assert len(instances) >= 2
    assert any(i.published == [{'n': 'x'}] for i in instances)
    wrapper.stop()


class _FlakyInnerSubscriber:
    def __init__(self, fail_times):
        self.fail_times = fail_times

    def _do_subscribe(self):
        if self.fail_times > 0:
            self.fail_times -= 1
            raise ConnectionError('broker down')
        return {'payload': 42}

    def stop(self):
        pass


def test_resilient_subscriber_returns_none_on_error_then_recovers():
    inner = _FlakyInnerSubscriber(fail_times=1)
    wrapper = ResilientDataSubscriberWrapper(
        'test_sub', 'mock://broker',
        {'retry_max': 3, 'retry_backoff_seconds': 0.01},
        inner_factory=lambda: inner)
    # First pull hits the error path: backs off, reconnects, returns None.
    assert wrapper._do_subscribe() is None
    # Inner has recovered (fail budget consumed): next pull delivers.
    assert wrapper._do_subscribe() == {'payload': 42}
    wrapper.stop()
