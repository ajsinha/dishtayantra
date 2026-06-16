"""Value-type helpers for data carried on DAG edges.

The compute engine performs exactly three operations on the values that move
between nodes — **copy** (to keep nodes isolated), **compare** (the equality gate),
and **consolidate** (merge incoming edges) — plus a **describe** for the stats UI.
Historically each was hard-coded for ``dict`` (``copy.deepcopy`` / ``==`` /
``dict.update``). This module makes them type-dispatched so the same engine can
also move an Arrow ``RecordBatch`` *by reference* (zero copy), which is safe
because a ``RecordBatch`` is immutable.

Backward compatibility is the whole point: **for any non-batch value every helper
reduces to the exact previous behaviour**, and if pyarrow is not installed
``is_batch`` is always ``False`` so every path is the original dict path.
"""

from __future__ import annotations

import copy

try:
    import pyarrow as pa
    _PYARROW = True
except Exception:  # pragma: no cover - pyarrow optional for the core
    pa = None
    _PYARROW = False


def is_batch(value) -> bool:
    """True only for a pyarrow.RecordBatch. False for everything else (and always
    False when pyarrow is unavailable), so the dict path is the default."""
    return _PYARROW and isinstance(value, pa.RecordBatch)


def ev_copy(value):
    """Isolate a value before storing it on a node.

    A RecordBatch is immutable and Arrow calculators are functional, so it can be
    shared by reference with no copy. Any other value is deep-copied exactly as the
    engine always has."""
    if is_batch(value):
        return value
    return copy.deepcopy(value)


def ev_equals(a, b) -> bool:
    """True value-equality for the equality gate.

    Both batches -> RecordBatch.equals (true content comparison, gate preserved).
    Exactly one batch -> not equal. Otherwise the original ``a == b``."""
    a_batch, b_batch = is_batch(a), is_batch(b)
    if a_batch and b_batch:
        return a.equals(b)
    if a_batch or b_batch:
        return False
    return a == b


def ev_consolidate(pairs):
    """Merge incoming-edge data. ``pairs`` is a list of ``(pname, edge_data)``.

    Dict edges merge exactly as the engine always has (same truthiness skip, same
    ``pname`` namespacing). A single RecordBatch edge passes the batch through
    untouched (zero copy). RecordBatch fan-in (a batch alongside any other input)
    is out of scope for v1 and fails fast rather than mis-merging."""
    batch_pairs = [(p, d) for p, d in pairs if is_batch(d)]
    if batch_pairs:
        others = [(p, d) for p, d in pairs if not is_batch(d) and d]
        if len(batch_pairs) > 1 or others:
            raise ValueError(
                "RecordBatch fan-in is not supported in this version: a node may "
                "receive at most one RecordBatch edge and no other inputs. Combine "
                "batches with a flattening/aggregation node instead."
            )
        return batch_pairs[0][1]

    # ---- dict path: behaviourally identical to the original consolidate ----
    merged = {}
    for pname, edge_data in pairs:
        epn = (pname or '').strip()
        if edge_data:
            if len(epn) == 0:
                merged.update(edge_data)
            else:
                if epn not in merged:
                    merged[epn] = {}
                merged[epn].update(edge_data)
    return merged


def ev_describe(value):
    """JSON-safe view for ``details()`` / the stats UI. Dicts pass through
    unchanged; a RecordBatch (not JSON-serialisable) becomes a compact summary."""
    if is_batch(value):
        return {
            "__arrow_batch__": True,
            "rows": value.num_rows,
            "schema": [{"name": f.name, "type": str(f.type)} for f in value.schema],
        }
    return value
