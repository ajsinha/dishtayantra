"""
Custom Arrow ingress node for the high-throughput trade-ETL perf test.

The standard ``ArrowBatchingSubscriptionNode`` builds its RecordBatch with
``records_to_batch`` (``pa.Table.from_pylist``), which infers the schema from the
FIRST row of each batch. For a Kafka feed of heterogeneous trades - where
individual trade messages carry different attribute sets - that is unsafe:

  * any attribute that is NOT present in the first message of a batch is
    silently DROPPED, and
  * a key whose type differs across rows (int in one trade, str in another)
    makes the whole batch build raise ``ArrowInvalid``.

``NormalizingArrowBatchingSubscriptionNode`` fixes both by normalizing every
batch to a STABLE schema regardless of which optional attributes appear:

  * a fixed set of typed "core" columns that are expected on every trade
    (missing -> default/null, coerced to the declared type), plus
  * one ``extras`` column (JSON string per row) that losslessly carries every
    non-core attribute, so nothing is dropped and downstream vectorized
    calculators always see the same columns.

This keeps the columnar/zero-copy benefits (one immutable RecordBatch shared by
reference downstream, ``ev_equals`` change gate) while being robust to ragged
input. It is additive and opt-in: selected by node ``type``
``perftest.arrow_trade_nodes.NormalizingArrowBatchingSubscriptionNode``; the
engine and existing nodes are untouched.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

import pyarrow as pa

from core.dag.edge_value import ev_equals
from core.dag.node_implementations import ArrowBatchingSubscriptionNode

logger = logging.getLogger(__name__)


# ---- core-field coercion helpers (graceful: never raise on bad input) ------- #
def _to_str(v):
    return "" if v is None else str(v)


def _to_upper(v):
    return "" if v is None else str(v).upper().strip()


def _to_float(v):
    try:
        return float(v) if v not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


def _to_int_or_none(v):
    try:
        return None if v in (None, "") else int(v)
    except (TypeError, ValueError):
        return None


# Default trade schema: (column, pyarrow type, default, coerce_fn). These are the
# "always present" attributes; symbol/side/currency are upper-cased here so the
# normalize step is folded into ingest. Everything else rides in `extras`.
DEFAULT_CORE_FIELDS = [
    ("trade_id", pa.string(),  "",    _to_str),
    ("seq",      pa.int64(),   None,  _to_int_or_none),
    ("symbol",   pa.string(),  "",    _to_upper),
    ("side",     pa.string(),  "",    _to_upper),
    ("quantity", pa.float64(), 0.0,   _to_float),
    ("price",    pa.float64(), 0.0,   _to_float),
    ("currency", pa.string(),  "USD", _to_upper),
]

_COERCERS = {
    "str": _to_str, "upper": _to_upper, "float": _to_float, "int": _to_int_or_none,
}
_PA_TYPES = {
    "string": pa.string(), "float64": pa.float64(), "int64": pa.int64(),
    "bool": pa.bool_(),
}


class NormalizingArrowBatchingSubscriptionNode(ArrowBatchingSubscriptionNode):
    """Drain up to ``max_size`` heterogeneous trade dicts and emit ONE stable-schema
    Arrow RecordBatch (typed core columns + a JSON ``extras`` column).

    Config (all optional):
        {
          "batch": {"max_size": 5000},
          "extras_key": "extras_json",
          "core_fields": [            # override the default trade schema if needed
            {"name": "trade_id", "type": "string", "coerce": "str",   "default": ""},
            {"name": "quantity", "type": "float64","coerce": "float", "default": 0.0},
            ...
          ]
        }
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        cfg = config or {}
        self._extras_key = cfg.get("extras_key", "extras_json")
        cf = cfg.get("core_fields")
        if cf:
            self._core_fields = [
                (f["name"], _PA_TYPES.get(f.get("type", "string"), pa.string()),
                 f.get("default"), _COERCERS.get(f.get("coerce", "str"), _to_str))
                for f in cf
            ]
        else:
            self._core_fields = DEFAULT_CORE_FIELDS

    def _build_stable_batch(self, rows):
        """Heterogeneous dict rows -> fixed-schema RecordBatch (no drops, no raise)."""
        core_names = {name for name, _, _, _ in self._core_fields}
        arrays, names = [], []
        for name, patype, default, coerce in self._core_fields:
            col = []
            for r in rows:
                raw = r.get(name, default) if isinstance(r, dict) else default
                col.append(coerce(raw) if raw is not None else default)
            arrays.append(pa.array(col, type=patype))
            names.append(name)
        # Everything not in the core schema is preserved as a per-row JSON string,
        # so ragged/optional attributes are never lost and never break the schema.
        extras = []
        for r in rows:
            if isinstance(r, dict):
                extra = {k: v for k, v in r.items() if k not in core_names}
                extras.append(json.dumps(extra, default=str) if extra else "{}")
            else:
                extras.append(json.dumps({"_raw": r}, default=str))
        arrays.append(pa.array(extras, type=pa.string()))
        names.append(self._extras_key)
        return pa.RecordBatch.from_arrays(arrays, names=names)

    def compute(self) -> bool:
        if not self.isdirty() or not self.subscriber:
            return False
        if self._input_transformers or self._output_transformers:
            raise TypeError(
                f"{self.name}: NormalizingArrowBatchingSubscriptionNode does not "
                "support node-level transformers; use an edge transformer.")
        try:
            buffer = []
            while len(buffer) < self._max_batch_size:
                data = self.subscriber.get_data(block_time=0)
                if data is None:
                    break
                buffer.append(data)
            if not buffer:
                self.set_clean()
                return False

            self._messages_in += len(buffer)
            batch = self._build_stable_batch(buffer)        # stable schema, once
            output = self._calculator.calculate(batch) if self._calculator else batch

            if not ev_equals(output, self._output):
                self._output = output                        # shared by reference
                self._messages_out += 1
                for edge in self._outgoing_edges:
                    edge.to_node.set_dirty()
            self.set_clean()
            return True
        except Exception as e:  # noqa: BLE001
            self._errors.append({'time': datetime.now().isoformat(), 'error': str(e)})
            logger.error(f"Error in normalizing arrow ingest node {self.name}: {e}")
            return False
