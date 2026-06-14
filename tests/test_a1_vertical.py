"""A1 vertical slice tests: backward compatibility + exact row/Arrow parity.

Guarantees:
  * an ArrowCalculator is a drop-in for a row DataCalculator (row mode),
  * batched-envelope processing produces identical results,
  * the chained Arrow stages match the perftest row calculators exactly,
  * the same holds end-to-end through the real ComputeGraph,
  * legacy row calculators can run in a batched path via the adapter.

The engine (`core/dag/*`) and `core_calculator.py` are NOT modified by A1, so
existing DAGs/data are unaffected; these tests lock in the new path's fidelity.
"""

import logging

import pytest

pytest.importorskip("pyarrow")

from core.calculator.arrow_calculator import RowCalculatorBatchAdapter  # noqa: E402
from perftest.etl_calculators import (  # noqa: E402
    FxConvertCalculator,
    NotionalCalculator,
)
from benchmarks.arrow_trade_calculators import (  # noqa: E402
    ArrowFxConvertCalculator,
    ArrowNotionalCalculator,
)
from benchmarks.workloads import trade_generator  # noqa: E402

_FIELDS_FX = ("fx_rate_to_usd", "price_usd")
_FIELDS_NOTIONAL = ("notional_usd", "signed_notional_usd")


def _trades(n):
    gen = trade_generator()
    return [gen(i) for i in range(n)]


def test_arrow_calculator_is_drop_in_row_calculator():
    """calculate(single_dict) must match the row calculator exactly."""
    row = FxConvertCalculator("fx", {})
    arrow = ArrowFxConvertCalculator("fx", {})
    for t in _trades(500):
        r = row.calculate(dict(t))
        a = arrow.calculate(dict(t))  # single dict -> 1-row batch internally
        for f in _FIELDS_FX:
            assert r[f] == a[f], (t, f, r[f], a[f])
        assert a["trade_id"] == t["trade_id"]  # all input fields preserved


def test_batch_envelope_matches_row():
    trades = _trades(2000)
    row = FxConvertCalculator("fx", {})
    arrow = ArrowFxConvertCalculator("fx", {})
    envelope = {"batch": [dict(t) for t in trades]}
    out = arrow.calculate(envelope)
    assert isinstance(out["batch"], list) and len(out["batch"]) == 2000
    for t, a in zip(trades, out["batch"]):
        r = row.calculate(dict(t))
        for f in _FIELDS_FX:
            assert r[f] == a[f]


def test_chained_arrow_stages_exact_parity():
    trades = _trades(5000)
    rfx, rno = FxConvertCalculator("fx", {}), NotionalCalculator("no", {})
    afx, ano = ArrowFxConvertCalculator("fx", {}), ArrowNotionalCalculator("no", {})

    row_out = [rno.calculate(rfx.calculate(dict(t))) for t in trades]
    a1 = afx.process_records([dict(t) for t in trades])
    a2 = ano.process_records(a1)

    assert len(row_out) == len(a2) == 5000
    for r, a in zip(row_out, a2):
        for f in _FIELDS_FX + _FIELDS_NOTIONAL:
            assert r[f] == a[f], (f, r[f], a[f])


def test_row_calculator_batch_adapter():
    """A legacy row calculator can process a batch envelope via the adapter."""
    trades = _trades(300)
    adapter = RowCalculatorBatchAdapter("fx", {}, wrapped=FxConvertCalculator("fx", {}))
    direct = FxConvertCalculator("fx", {})
    out = adapter.calculate({"batch": [dict(t) for t in trades]})
    for t, a in zip(trades, out["batch"]):
        r = direct.calculate(dict(t))
        for f in _FIELDS_FX:
            assert r[f] == a[f]


def test_end_to_end_engine_parity():
    logging.disable(logging.INFO)
    from benchmarks.a1_vertical import run_compare

    report = run_compare(trades=2000, batch=200, quiet=True)
    assert report["row_delivered"] == 2000
    assert report["arrow_delivered"] == 2000
    assert report["correctness"]["identical"] is True
    assert report["correctness"]["max_abs_error"] == 0.0
