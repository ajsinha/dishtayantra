"""Test the EOD batch-file processing example (enrichment + limit checks)."""

from perftest.run_eod_example import run_example


def test_eod_enrichment_and_limit_checks():
    r = run_example(trades=3000, quiet=True)
    assert r["sequential_delivered"] == 3000
    assert r["batched_delivered"] == 3000
    # enrichment against the small limits feed flags some breaches
    assert r["limit_breaches"] > 0
    # one-at-a-time and auto-batched paths agree
    assert r["identical_results"] is True
