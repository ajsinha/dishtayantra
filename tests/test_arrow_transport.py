"""Integration tests for the Arrow zero-copy transport path (RecordBatch on edges).

Covers: an Arrow-transport DAG produces identical per-trade output to the dict
envelope path; a row transformer on a batch edge fails fast; the Arrow-aware
transformers (projection / row-bridge) behave correctly on a batch.
"""

import pytest

pa = pytest.importorskip("pyarrow")

from core.dag.graph_elements import Node, Edge          # noqa: E402
from core.dag.node_implementations import SinkNode      # noqa: E402
from core.transformer.core_transformer import PassthruDataTransformer  # noqa: E402
from core.transformer.arrow_transformer import (        # noqa: E402
    ProjectionBatchTransformer, RowTransformerBatchAdapter,
)


def _batch(rows):
    return pa.Table.from_pylist(rows).combine_chunks().to_batches()[0]


def test_transport_output_identical_to_envelope():
    from perftest.run_arrow_transport_example import run_example
    r = run_example(trades=400, quiet=True)
    assert r["identical"] is True
    assert r["envelope_delivered"] == 400
    assert r["transport_delivered"] == 400
    assert r["mismatches"] == 0


def test_row_transformer_on_batch_edge_fails_fast():
    src, dst = SinkNode("src", {}), SinkNode("dst", {})
    edge = Edge(src, dst, data_transformer=PassthruDataTransformer("p", {}))
    src._output = _batch([{"a": 1}])     # source emits a RecordBatch
    with pytest.raises(TypeError):
        edge.get_data()                  # row transformer can't handle a batch


def test_projection_batch_transformer_is_correct():
    t = ProjectionBatchTransformer("proj", {"columns": ["a", "c"], "rename": {"a": "x"}})
    out = t.transform_batch(_batch([{"a": 1, "b": 2, "c": 3}]))
    assert out.schema.names == ["x", "c"]
    assert out.to_pylist() == [{"x": 1, "c": 3}]


def test_row_transformer_batch_adapter_bridges_rows():
    class _AddOne(PassthruDataTransformer):
        def transform(self, data):
            d = dict(data); d["plus"] = d["a"] + 1
            return d
    adapter = RowTransformerBatchAdapter("ad", {"wrapped_instance": _AddOne("a1", {})})
    out = adapter.transform_batch(_batch([{"a": 1}, {"a": 5}]))
    assert out.to_pylist() == [{"a": 1, "plus": 2}, {"a": 5, "plus": 6}]
    # and it still works on the dict path
    assert adapter.transform({"a": 9})["plus"] == 10
