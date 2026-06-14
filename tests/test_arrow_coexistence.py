"""Coexistence tests for the A1 worked example.

Proves, through the real engine:
  Q1: an all-row DAG and a mixed (row + Arrow) DAG run in the SAME process.
  Q2: a SINGLE graph containing both row and Arrow calculators produces output
      identical to the all-row equivalent.
"""

import pytest

pytest.importorskip("pyarrow")

from perftest.run_arrow_example import run_example  # noqa: E402


def test_old_and_new_coexist_same_instance_and_single_graph():
    r = run_example(trades=2000, quiet=True)

    # Q1: both graphs delivered in one process.
    assert r["q1_both_graphs_in_one_process"] is True
    assert r["row_delivered"] == 2000
    assert r["mixed_delivered"] == 2000

    # Q2: the mixed graph is bit-identical to the all-row graph.
    assert r["q2_mixed_graph_identical_to_all_row"] is True
    assert r["trades_compared"] == 2000
    assert r["max_abs_error"] == 0.0
    assert r["mismatches"] == 0

    # row + Arrow + adapter all usable in one interpreter.
    assert r["coexistence_sanity_ok"] is True
