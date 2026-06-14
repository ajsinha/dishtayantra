"""Smoke + correctness tests for the A1 Arrow vectorization spike."""

import pytest

pytest.importorskip("pyarrow")  # pinned in requirements; skip if not installed

from benchmarks.arrow_vectorization_spike import (  # noqa: E402
    arrow_etl,
    make_data,
    row_etl,
    run_spike,
    verify_equivalence,
)


def test_arrow_matches_row_exactly():
    rows, batch = make_data(2000)
    eq = verify_equivalence(row_etl(rows), arrow_etl(batch))
    assert eq["equivalent"], eq
    assert eq["rows_checked"] == 2000
    assert eq["max_relative_error"] == 0.0


def test_arrow_output_schema():
    _, batch = make_data(100)
    out = arrow_etl(batch)
    for col in ("price_usd", "notional", "fee", "risk_score"):
        assert col in out.schema.names
    # original columns preserved
    assert "trade_id" in out.schema.names
    assert out.num_rows == 100


def test_run_spike_structure():
    report = run_spike(rows=2000, batch_size=500, repeats=1, quiet=True)
    for key in ("row_at_a_time", "arrow_vectorized", "speedup_x", "correctness"):
        assert key in report
    assert report["correctness"]["equivalent"] is True
    assert report["row_at_a_time"]["rows_per_s"] > 0
    assert report["arrow_vectorized"]["rows_per_s"] > 0
    assert report["speedup_x"] > 0
