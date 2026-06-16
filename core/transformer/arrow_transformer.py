"""Arrow-aware edge transformers.

The per-edge ``data_transformer`` gives every consumer its own "telescopic view"
of a source node's output. On a RecordBatch edge that lens becomes a *zero-copy
columnar projection* (``select``/``slice``/``rename_columns`` share the source's
buffers). An Arrow-aware transformer signals this by implementing
``transform_batch(RecordBatch) -> RecordBatch``; the engine's ``Edge.get_data``
calls it on batch edges.

``RowTransformerBatchAdapter`` bridges a legacy row transformer onto a batch edge
(per-row, correctness over speed — the transformer analogue of
``RowCalculatorBatchAdapter``). pyarrow is required only to *use* these on a batch
edge; importing the module does not require pyarrow.
"""

from __future__ import annotations

from datetime import datetime
from typing import List

from core.transformer.core_transformer import DataTransformer

try:
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except Exception:  # pragma: no cover
    pa = None
    PYARROW_AVAILABLE = False


class ArrowDataTransformer(DataTransformer):
    """Base for transformers that operate natively on a RecordBatch.

    Subclasses implement ``transform_batch``; ``transform`` (the row/dict contract)
    bridges single dicts through a 1-row batch so the same class also works on the
    dict path. Concrete projections typically just override ``transform_batch`` with
    a zero-copy ``batch.select([...])`` / ``rename_columns`` / ``slice``.
    """

    def transform_batch(self, batch: "pa.RecordBatch") -> "pa.RecordBatch":
        raise NotImplementedError

    def transform(self, data):
        self._transform_count += 1
        self._last_transform = datetime.now().isoformat()
        if PYARROW_AVAILABLE and isinstance(data, pa.RecordBatch):
            return self.transform_batch(data)
        if isinstance(data, dict):
            out = self.transform_batch(pa.Table.from_pylist([data]).combine_chunks().to_batches()[0])
            rows = out.to_pylist()
            return rows[0] if rows else data
        return data


class ProjectionBatchTransformer(ArrowDataTransformer):
    """Zero-copy columnar projection: keep/rename a subset of columns.

    Config: ``{"columns": ["a", "b"], "rename": {"a": "x"}}``. ``select`` and
    ``rename_columns`` share the source buffers, so the projected batch copies no
    data — the telescopic view, made cheap.
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        cfg = config or {}
        self._columns = cfg.get("columns")
        self._rename = cfg.get("rename") or {}

    def transform_batch(self, batch: "pa.RecordBatch") -> "pa.RecordBatch":
        out = batch
        if self._columns:
            out = out.select(self._columns)          # zero-copy column pick
        if self._rename:
            names = [self._rename.get(n, n) for n in out.schema.names]
            out = out.rename_columns(names)          # zero-copy rename
        return out


class RowTransformerBatchAdapter(DataTransformer):
    """Run an existing *row* transformer on a RecordBatch edge.

    Converts the batch to rows, applies the wrapped transformer per row, and
    rebuilds a batch. Correctness over speed; for the zero-copy path implement
    ``transform_batch`` directly (e.g. ProjectionBatchTransformer).

    Config: ``{"wrapped": {"type": "<builtin|dotted.path>", "config": {...}}}`` or a
    pre-built transformer passed as ``config["wrapped_instance"]``.
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        cfg = config or {}
        self._wrapped = cfg.get("wrapped_instance")
        if self._wrapped is None:
            spec = cfg.get("wrapped")
            if not spec:
                raise ValueError("RowTransformerBatchAdapter needs a 'wrapped' transformer")
            self._wrapped = self._build(spec)

    @staticmethod
    def _build(spec):
        if hasattr(spec, "transform"):
            return spec
        type_name = spec.get("type")
        wcfg = spec.get("config", {})
        import core.transformer.core_transformer as ct
        if hasattr(ct, type_name):
            return getattr(ct, type_name)(type_name, wcfg)
        module_path, _, cls_name = type_name.rpartition(".")
        import importlib
        cls = getattr(importlib.import_module(module_path), cls_name)
        return cls(cls_name, wcfg)

    def transform(self, data):
        self._transform_count += 1
        self._last_transform = datetime.now().isoformat()
        return self._wrapped.transform(data)

    def transform_batch(self, batch: "pa.RecordBatch") -> "pa.RecordBatch":
        self._transform_count += 1
        self._last_transform = datetime.now().isoformat()
        rows: List[dict] = batch.to_pylist()
        out_rows = [self._wrapped.transform(r) for r in rows]
        out_rows = [r for r in out_rows if r is not None]
        if not out_rows:
            return batch.slice(0, 0)
        return pa.Table.from_pylist(out_rows).combine_chunks().to_batches()[0]
