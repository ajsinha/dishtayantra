"""Smoke tests for the benchmark harness (roadmap Phase 0)."""

import logging

from benchmarks.harness import (
    BenchmarkResult,
    LatencyStats,
    percentile,
    run_inmemory_dag_benchmark,
)
from benchmarks.workloads import trade_etl_linear_config, trade_generator


def test_percentile_basic():
    data = [float(i) for i in range(1, 101)]  # 1..100 sorted
    assert percentile(data, 0) == 1.0
    assert percentile(data, 100) == 100.0
    # ~median of 1..100
    assert 49.0 <= percentile(data, 50) <= 51.0
    assert percentile([], 95) == 0.0
    assert percentile([42.0], 99) == 42.0


def test_latency_stats_from_seconds():
    stats = LatencyStats.from_seconds([0.001, 0.002, 0.003, 0.004])
    assert stats.count == 4
    assert stats.max_ms == 4.0
    assert 2.0 <= stats.mean_ms <= 3.0
    empty = LatencyStats.from_seconds([])
    assert empty.count == 0 and empty.p99_ms == 0.0


def test_inmemory_dag_benchmark_smoke():
    logging.disable(logging.INFO)
    in_q, out_q = "test_bench_in", "test_bench_out"
    config = trade_etl_linear_config(in_q, out_q, stages=3)

    result = run_inmemory_dag_benchmark(
        name="unit-smoke",
        config=config,
        input_queue=in_q,
        output_queue=out_q,
        n_messages=100,
        gen_fn=trade_generator(),
        warmup_messages=10,
        drain_timeout_s=20.0,
    )

    assert isinstance(result, BenchmarkResult)
    assert result.delivered == 100, result.notes
    assert result.throughput_msg_s > 0
    # trade_id survives the DAG, so per-message latency should be captured.
    assert result.latency.count > 0
    assert result.latency.p99_ms >= result.latency.p50_ms

    d = result.to_dict()
    assert d["delivered"] == 100
    assert "latency" in d and "throughput_msg_s" in d
    md = result.to_markdown()
    assert "throughput" in md and "unit-smoke" in md
