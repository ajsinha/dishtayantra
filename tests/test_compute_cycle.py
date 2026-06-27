"""Compute-cycle id semantics: one engine sweep that does work = one cycle."""
from core.dag.compute_graph import ComputeGraph


def _graph():
    return ComputeGraph({"name": "cyc_t", "subscribers": [], "publishers": [],
                         "calculators": [], "transformers": [], "nodes": [],
                         "edges": []}, lazy_init=True)


def test_cycle_counters_start_at_zero():
    g = _graph()
    assert g._compute_cycle == 0
    assert g._cycle_seq == 0


def test_cycle_advances_only_on_work_no_gaps():
    g = _graph()
    # First working sweep -> cycle 1
    assert g._begin_compute_cycle() == 1
    assert g._commit_compute_cycle() == 1
    # Idle sweeps propose 2 but never commit -> no id is burned, no gap
    assert g._begin_compute_cycle() == 2
    assert g._begin_compute_cycle() == 2
    assert g._cycle_seq == 1
    # Next working sweep commits 2, then the following proposes 3
    assert g._commit_compute_cycle() == 2
    assert g._begin_compute_cycle() == 3
