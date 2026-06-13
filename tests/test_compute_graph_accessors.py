"""Regression tests for the ComputeGraph publisher/subscriber accessors
(bug #10: publisher_by_name read self.subscribers; bug #11: misspelled
get_publiisher_by_name)."""
import types

from core.dag.compute_graph import ComputeGraph


def _bare_graph():
    """A ComputeGraph shell with distinct objects in each registry."""
    g = ComputeGraph.__new__(ComputeGraph)  # skip heavy __init__
    g.subscribers = {'feed': object()}
    g.publishers = {'feed': object(), 'out': object()}
    return g


def test_publisher_by_name_reads_publishers_dict():
    g = _bare_graph()
    assert g.publisher_by_name('feed') is g.publishers['feed']
    assert g.publisher_by_name('feed') is not g.subscribers['feed']
    assert g.publisher_by_name('out') is g.publishers['out']


def test_get_publisher_by_name_correct_spelling():
    g = _bare_graph()
    assert g.get_publisher_by_name('out') is g.publishers['out']


def test_misspelled_alias_still_works():
    g = _bare_graph()
    assert g.get_publiisher_by_name('out') is g.publishers['out']


def test_subscriber_by_name_reads_subscribers_dict():
    g = _bare_graph()
    assert g.subscriber_by_name('feed') is g.subscribers['feed']
