"""Tests for core.pubsub.backpressure - credit-based flow control."""

import queue
import threading
import time

import pytest

from core.pubsub.backpressure import CreditController, CreditQueue
from core.pubsub.inmemorypubsub import InMemoryPubSub


# ------------------------------------------------------------- CreditController
def test_acquire_release_accounting():
    c = CreditController(capacity=2, policy="drop")
    assert c.available == 2
    assert c.acquire() and c.acquire()        # spend both
    assert c.available == 0 and c.in_flight == 2
    assert c.acquire() is False               # none left -> drop
    c.release()
    assert c.available == 1
    assert c.acquire() is True


def test_drop_policy_counts_drops():
    c = CreditController(capacity=1, policy="drop")
    assert c.acquire() is True
    assert c.acquire() is False
    s = c.stats()
    assert s["dropped"] == 1 and s["granted"] == 1 and s["max_in_flight"] == 1


def test_block_policy_times_out():
    c = CreditController(capacity=1, policy="block", timeout=0.05)
    assert c.acquire() is True
    t0 = time.monotonic()
    assert c.acquire() is False               # blocks, then times out -> drop
    assert time.monotonic() - t0 >= 0.05
    assert c.stats()["blocked"] == 1 and c.stats()["dropped"] == 1


def test_block_policy_wakes_on_release():
    c = CreditController(capacity=1, policy="block")
    assert c.acquire() is True
    result = {}

    def producer():
        result["got"] = c.acquire(timeout=2.0)  # should block until release

    t = threading.Thread(target=producer)
    t.start()
    time.sleep(0.05)
    c.release()                                # frees the waiting producer
    t.join(timeout=2.0)
    assert result.get("got") is True


def test_invalid_args_raise():
    with pytest.raises(ValueError):
        CreditController(capacity=0)
    with pytest.raises(ValueError):
        CreditController(capacity=1, policy="explode")


# ------------------------------------------------------------------ CreditQueue
def test_credit_queue_releases_on_get():
    c = CreditController(capacity=2, policy="drop")
    q = CreditQueue(maxsize=10, controller=c)
    c.acquire(); q.put("a")
    c.acquire(); q.put("b")
    assert c.available == 0
    assert q.get() == "a"                      # get returns a credit
    assert c.available == 1
    assert q.get() == "b"
    assert c.available == 2


def test_credit_queue_empty_get_does_not_release():
    c = CreditController(capacity=1, policy="drop")
    q = CreditQueue(maxsize=10, controller=c)
    with pytest.raises(queue.Empty):
        q.get(block=False)                     # nothing taken -> no spurious credit
    assert c.available == 1


# -------------------------------------------------- InMemoryPubSub integration
@pytest.fixture
def restore_bp():
    ps = InMemoryPubSub()
    saved = ps._bp_cache
    yield ps
    ps._bp_cache = saved


def test_topic_default_is_plain_queue_unchanged(restore_bp):
    ps = restore_bp
    ps._bp_cache = (False, 1000, "block", None)   # explicit OFF
    sub = ps.subscribe_to_topic("bp_off_topic", max_size=5)
    assert type(sub) is queue.Queue               # not a CreditQueue
    assert getattr(sub, "credit", None) is None


def test_topic_backpressure_bounds_and_drops(restore_bp):
    ps = restore_bp
    ps._bp_cache = (True, 2, "drop", None)        # ON, capacity 2, drop policy
    sub = ps.subscribe_to_topic("bp_on_topic", max_size=100)
    assert isinstance(sub, CreditQueue)
    for i in range(5):                            # publish 5, only 2 credits
        ps.publish_to_topic("bp_on_topic", {"i": i})
    assert sub.qsize() == 2
    assert sub.credit.stats()["dropped"] == 3
    sub.get()                                     # consume one -> 1 credit back
    ps.publish_to_topic("bp_on_topic", {"i": 99})  # now fits again
    assert sub.qsize() == 2
