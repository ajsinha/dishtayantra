"""Smoke tests for the Nexmark benchmark workload (roadmap Phase 0)."""

import logging

from benchmarks.harness import run_inmemory_dag_benchmark
from benchmarks.nexmark_calculators import (Q1CurrencyConvertCalculator,
                                            Q2AuctionFilterCalculator)
from benchmarks.nexmark_workload import bid_generator, nexmark_linear_config


def test_q1_currency_convert():
    calc = Q1CurrencyConvertCalculator("q1", {})
    out = calc.calculate({"trade_id": 1, "price": 1000})
    assert out["price_eur"] == round(1000 * 0.908, 4)
    assert out["price"] == 1000  # original preserved


def test_q2_auction_filter_flag():
    calc = Q2AuctionFilterCalculator("q2", {})
    assert calc.calculate({"auction": 246})["selected"] is True    # 246 % 123 == 0
    assert calc.calculate({"auction": 250})["selected"] is False


def test_bid_generator_deterministic():
    g1, g2 = bid_generator(seed=7), bid_generator(seed=7)
    assert [g1(i) for i in range(5)] == [g2(i) for i in range(5)]


def test_nexmark_inmemory_benchmark_smoke():
    logging.disable(logging.INFO)
    in_q, out_q = "test_nex_in", "test_nex_out"
    config = nexmark_linear_config(in_q, out_q)

    result = run_inmemory_dag_benchmark(
        name="nexmark-smoke",
        config=config,
        input_queue=in_q,
        output_queue=out_q,
        n_messages=200,
        gen_fn=bid_generator(),
        warmup_messages=10,
        drain_timeout_s=30.0,
    )
    assert result.delivered == result.n_messages == 200
    assert result.throughput_msg_s > 0
    d = result.to_dict()
    # percentiles are ordered and present
    assert d["latency"]["p50_ms"] <= d["latency"]["p99_ms"]
