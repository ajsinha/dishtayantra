"""Unit tests for core/dag/edge_value.py — the value-type dispatch that lets the
engine move dicts (deep-copied, as always) or Arrow RecordBatches (by reference).

The central guarantee under test: **for non-batch values every helper behaves
exactly as the old hard-coded dict logic**, and batches are handled correctly.
"""

import copy

import pytest

from core.dag.edge_value import is_batch, ev_copy, ev_equals, ev_consolidate, ev_describe

pa = pytest.importorskip("pyarrow")


def _batch(rows):
    return pa.Table.from_pylist(rows).combine_chunks().to_batches()[0]


# ---- dict path is identical to the original behaviour ----------------------

def test_is_batch_false_for_non_batches():
    for v in ({}, {"a": 1}, [1, 2], "x", 3, None):
        assert is_batch(v) is False


def test_ev_copy_deepcopies_dicts():
    d = {"a": {"b": [1, 2]}}
    c = ev_copy(d)
    assert c == d
    c["a"]["b"].append(3)
    assert d["a"]["b"] == [1, 2]          # original untouched -> real deep copy


def test_ev_equals_matches_dict_equality():
    assert ev_equals({"a": 1}, {"a": 1}) is True
    assert ev_equals({"a": 1}, {"a": 2}) is False
    assert ev_equals({}, {}) is True


def test_ev_consolidate_dict_path_matches_old_logic():
    # no pname -> dict.update merge; falsy edge_data skipped (as before)
    out = ev_consolidate([("", {"a": 1}), ("", {"b": 2}), ("", {})])
    assert out == {"a": 1, "b": 2}
    # pname -> namespaced
    out = ev_consolidate([("x", {"a": 1}), ("y", {"b": 2})])
    assert out == {"x": {"a": 1}, "y": {"b": 2}}


def test_ev_describe_passes_through_dicts():
    d = {"a": 1}
    assert ev_describe(d) == d


# ---- batch path ------------------------------------------------------------

def test_is_batch_true_for_recordbatch():
    assert is_batch(_batch([{"a": 1}])) is True


def test_ev_copy_returns_same_batch_by_reference():
    b = _batch([{"a": 1}, {"a": 2}])
    assert ev_copy(b) is b                # immutable -> shared, zero copy


def test_ev_equals_uses_content_equality_for_batches():
    a = _batch([{"a": 1}, {"a": 2}])
    b = _batch([{"a": 1}, {"a": 2}])
    c = _batch([{"a": 1}, {"a": 3}])
    assert a is not b
    assert ev_equals(a, b) is True        # equal content
    assert ev_equals(a, c) is False


def test_ev_equals_mixed_types_not_equal():
    b = _batch([{"a": 1}])
    assert ev_equals(b, {"a": 1}) is False
    assert ev_equals({}, b) is False      # initial dict input vs first batch


def test_ev_consolidate_single_batch_passthrough():
    b = _batch([{"a": 1}])
    assert ev_consolidate([("", b)]) is b


def test_ev_consolidate_batch_fanin_fails_fast():
    b1 = _batch([{"a": 1}])
    b2 = _batch([{"a": 2}])
    with pytest.raises(ValueError):
        ev_consolidate([("", b1), ("", b2)])          # two batches
    with pytest.raises(ValueError):
        ev_consolidate([("", b1), ("", {"x": 1})])    # batch + dict


def test_ev_describe_summarizes_batch_json_safely():
    import json
    d = ev_describe(_batch([{"a": 1, "b": "x"}]))
    assert d["__arrow_batch__"] is True and d["rows"] == 1
    json.dumps(d)                          # must be JSON-serialisable
