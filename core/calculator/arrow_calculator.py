"""A1 Arrow columnar calculator contract (roadmap Phase 1, step A1).

ADDITIVE AND OPT-IN BY DESIGN. This module adds new classes only; it does not
modify the engine (`core/dag/*`) or the existing `DataCalculator` machinery, so
every existing DAG, calculator, and stored config keeps working byte-for-byte.

How it stays backward compatible
--------------------------------
An ``ArrowCalculator`` IS a ``DataCalculator``. It implements the normal
``calculate(data)`` row method, so the existing reactive-dataflow engine
(`core/dag/graph_elements.py::Node.compute`) drives it with no changes. The
vectorized win comes from processing a *batch-envelope* message — a dict that
carries many records under one key — in a single columnar pass:

    row message      :  {"trade_id": 1, "price": 10.0, "currency": "EUR", ...}
    batch envelope   :  {"batch": [ {...}, {...}, ... ]}   # processed vectorized

A bare record dict is processed as a 1-row batch (so the same class is a true
drop-in for a legacy row calculator). The envelope key defaults to ``batch`` and
is configurable via ``config["arrow_batch_key"]``.

Source-node auto-accumulation of individual messages into batches is the next
A1 increment (it touches `SubscriptionNode`); this slice deliberately keeps the
engine untouched and proves the vectorized calculator path end-to-end.
"""

from __future__ import annotations

from abc import abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.calculator.core_calculator import DataCalculator

try:
    import pyarrow as pa
    import pyarrow.compute as pc  # noqa: F401  (used by subclasses)
    PYARROW_AVAILABLE = True
except Exception:  # pragma: no cover - pyarrow is pinned in requirements.txt
    PYARROW_AVAILABLE = False


def records_to_batch(records: List[Dict[str, Any]]) -> "pa.RecordBatch":
    """Convert a list of row dicts into a single Arrow RecordBatch."""
    table = pa.Table.from_pylist(records).combine_chunks()
    batches = table.to_batches()
    return batches[0] if batches else pa.record_batch([], schema=table.schema)


def append_columns(batch: "pa.RecordBatch", new_cols: Dict[str, Any]) -> "pa.RecordBatch":
    """Return a new RecordBatch with `new_cols` appended (existing cols preserved)."""
    arrays = list(batch.columns) + list(new_cols.values())
    names = list(batch.schema.names) + list(new_cols.keys())
    return pa.RecordBatch.from_arrays(arrays, names=names)


def py_round_array(arr: "pa.Array", ndigits: int) -> "pa.Array":
    """Round a float array using Python's correctly-rounded round(), so results
    are bit-identical to scalar row calculators.

    pyarrow's pc.round() scales by 10**ndigits before rounding, which disagrees
    with Python's round() at tie boundaries in rare cases (a sub-cent flip that
    can propagate to a cent). The heavy arithmetic stays vectorized; only this
    final rounding is done element-wise to guarantee exact parity.
    """
    return pa.array(
        [None if v is None else round(v, ndigits) for v in arr.to_pylist()],
        pa.float64(),
    )


class ArrowCalculator(DataCalculator):
    """Opt-in columnar calculator. Subclasses implement `calculate_batch`.

    Works unchanged on the existing engine (row mode) and vectorizes
    batch-envelope messages. Never required; adopting it changes no engine code.
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        if not PYARROW_AVAILABLE:
            raise RuntimeError(
                "pyarrow is required for ArrowCalculator (pinned in requirements.txt)"
            )
        self.batch_key = (config or {}).get("arrow_batch_key", "batch")

    @abstractmethod
    def calculate_batch(self, batch: "pa.RecordBatch") -> "pa.RecordBatch":
        """Vectorized transform over a columnar batch. Must preserve row order
        and the input columns (append new ones)."""
        raise NotImplementedError

    # ---- row/batch bridge -------------------------------------------------- #
    def process_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not records:
            return []
        out = self.calculate_batch(records_to_batch(records))
        return out.to_pylist()

    def calculate(self, data):
        """DataCalculator contract. Handles batch envelopes and single rows."""
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()

        # Zero-copy Arrow transport: the edge already carries a RecordBatch, so
        # stay columnar end to end (no dict<->Arrow conversion at this stage).
        if PYARROW_AVAILABLE and isinstance(data, pa.RecordBatch):
            return self.calculate_batch(data)

        if isinstance(data, dict) and isinstance(data.get(self.batch_key), list):
            result = dict(data)
            result[self.batch_key] = self.process_records(data[self.batch_key])
            return result
        if isinstance(data, dict):
            out = self.process_records([data])
            return out[0] if out else data
        return data  # unknown shape: passthrough (never raise on legacy input)


class RowCalculatorBatchAdapter(DataCalculator):
    """Run a legacy row `DataCalculator` over a batch-envelope message, per row.

    Lets existing (non-Arrow) calculators participate in a batched path without
    modification — the compatibility bridge for mixed DAGs.
    """

    def __init__(self, name, config, wrapped: Optional[DataCalculator] = None):
        super().__init__(name, config)
        self.batch_key = (config or {}).get("arrow_batch_key", "batch")
        self._wrapped = wrapped
        if self._wrapped is None:
            # Build the wrapped calculator from config["wrapped"], which uses the
            # same shape as a DAG calculator entry: {"type": <builtin|dotted.path>,
            # "config": {...}}.
            wrapped_cfg = (config or {}).get("wrapped")
            if wrapped_cfg:
                self._wrapped = self._build_wrapped(name + "_wrapped", wrapped_cfg)
        if self._wrapped is None:
            raise ValueError("RowCalculatorBatchAdapter needs a wrapped calculator")

    @staticmethod
    def _build_wrapped(name: str, calc_cfg: Dict[str, Any]) -> DataCalculator:
        """Resolve a calculator the same way the DAG builder does: built-in class
        name or a dotted import path. Falls back to CalculatorFactory for the
        factory-style config (calculator/python_class/java_class/...)."""
        calc_type = calc_cfg.get("type")
        cfg = calc_cfg.get("config", {})
        if calc_type:
            from core.calculator import core_calculator as _cc
            if hasattr(_cc, calc_type):  # built-in (e.g. PassthruCalculator)
                return getattr(_cc, calc_type)(name, cfg)
            import importlib
            module_path, _, class_name = calc_type.rpartition(".")
            module = importlib.import_module(module_path)
            return getattr(module, class_name)(name, cfg)
        from core.calculator.core_calculator import CalculatorFactory
        return CalculatorFactory.create(name, calc_cfg)

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()
        if isinstance(data, dict) and isinstance(data.get(self.batch_key), list):
            result = dict(data)
            result[self.batch_key] = [
                self._wrapped.calculate(r) for r in data[self.batch_key]
            ]
            return result
        return self._wrapped.calculate(data)
