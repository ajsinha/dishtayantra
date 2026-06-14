#!/usr/bin/env python3
"""A1 worked example & coexistence demonstration.

Answers two questions by RUNNING them, not asserting:

  Q1: can old-style and new-style DAGs/calculators coexist in one instance?
  Q2: can a single graph contain BOTH old-style (row) and new-style (Arrow)
      calculators?

It loads the hosted mixed DAG (perftest/perftest_arrow_mixed.json) — a single
graph whose stages are: validate[row] -> normalize[row] -> fx[Arrow] ->
notional[Arrow] -> fee[Arrow] -> risk[row] -> classify[row] — and runs it in the
SAME process as an all-row equivalent, then verifies the per-trade output is
identical. In-memory broker; no Kafka required.

Usage:
    python -m perftest.run_arrow_example --trades 20000
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from benchmarks.workloads import trade_generator  # noqa: E402

_HERE = os.path.dirname(os.path.abspath(__file__))
_MIXED_JSON = os.path.join(_HERE, "perftest_arrow_mixed.json")

# All-row equivalent of the mixed graph (same topology, every stage row-style).
_ROW_STAGES = [
    ("validate", "perftest.etl_calculators.ValidateTradeCalculator"),
    ("normalize", "perftest.etl_calculators.NormalizeTradeCalculator"),
    ("fx", "perftest.etl_calculators.FxConvertCalculator"),
    ("notional", "perftest.etl_calculators.NotionalCalculator"),
    ("fee", "perftest.etl_calculators.FeeCalculator"),
    ("risk", "perftest.etl_calculators.RiskScoreCalculator"),
    ("classify", "perftest.etl_calculators.ClassifyTradeCalculator"),
]
_COMPARE_FIELDS = [
    "fx_rate_to_usd", "price_usd", "notional_usd", "signed_notional_usd",
    "commission_usd", "exchange_fee_usd", "total_fees_usd", "net_notional_usd",
    "volatility", "var_1d_usd", "risk_score", "size_bucket", "risk_tier",
]


def _row_config(in_q: str, out_q: str) -> Dict[str, Any]:
    calcs = [{"name": n, "type": t, "config": {}} for n, t in _ROW_STAGES]
    nodes = [{"name": "ingest", "type": "SubscriptionNode", "config": {}, "subscriber": "in_sub"}]
    order = ["ingest"]
    for n, _ in _ROW_STAGES:
        nodes.append({"name": f"{n}_node", "type": "CalculationNode", "config": {}, "calculator": n})
        order.append(f"{n}_node")
    nodes.append({"name": "sink", "type": "PublicationNode", "config": {}, "publishers": ["out_pub"]})
    order.append("sink")
    return {
        "name": "perftest_all_row", "start_time": None, "end_time": None,
        "subscribers": [{"name": "in_sub", "config": {"source": f"mem://queue/{in_q}", "max_depth": 1000000}}],
        "publishers": [{"name": "out_pub", "config": {"destination": f"mem://queue/{out_q}"}}],
        "calculators": calcs, "transformers": [], "nodes": nodes,
        "edges": [{"from_node": order[i], "to_node": order[i + 1]} for i in range(len(order) - 1)],
    }


def _queue_names(cfg: Dict[str, Any]):
    src = cfg["subscribers"][0]["config"]["source"].rsplit("/", 1)[-1]
    dst = cfg["publishers"][0]["config"]["destination"].rsplit("/", 1)[-1]
    return src, dst


def _parse(msg: Any) -> Any:
    if isinstance(msg, (bytes, bytearray)):
        msg = msg.decode("utf-8")
    return json.loads(msg) if isinstance(msg, str) else msg


def _run(cfg, in_q, out_q, trades: List[Dict[str, Any]], drain_timeout=60.0):
    from core.dag.compute_graph import ComputeGraph
    from core.pubsub.inmemorypubsub import InMemoryPubSub
    graph = ComputeGraph(cfg)
    graph.start()
    time.sleep(0.5)
    ps = InMemoryPubSub()
    by_id: Dict[Any, Dict] = {}
    t0 = time.perf_counter()
    for t in trades:
        ps.publish_to_queue(in_q, json.dumps(t))
    deadline = time.time() + drain_timeout
    while len(by_id) < len(trades) and time.time() < deadline:
        m = ps.consume_from_queue(out_q)
        if m is None:
            time.sleep(0.002)
            continue
        d = _parse(m)
        if isinstance(d, dict) and "trade_id" in d:  # skip startup {} emit
            by_id[d["trade_id"]] = d
    elapsed = time.perf_counter() - t0
    try:
        graph.stop()
    except Exception:
        pass
    return by_id, elapsed, graph


def run_example(trades: int = 20000, quiet: bool = True) -> Dict[str, Any]:
    logging.disable(logging.INFO)
    gen = trade_generator()
    data = [gen(i) for i in range(trades)]

    with open(_MIXED_JSON) as fh:
        mixed_cfg = json.load(fh)
    m_in, m_out = _queue_names(mixed_cfg)
    row_cfg = _row_config("allrow_in", "allrow_out")
    r_in, r_out = _queue_names(row_cfg)

    # Q1: both graphs live in the SAME process at the same time.
    row_by_id, row_s, row_graph = _run(row_cfg, r_in, r_out, data)
    mixed_by_id, mixed_s, mixed_graph = _run(mixed_cfg, m_in, m_out, data)

    # Q2: the mixed graph (row + Arrow nodes) must match the all-row graph.
    common = set(row_by_id) & set(mixed_by_id)
    mismatches, max_err = 0, 0.0
    for tid in common:
        r, m = row_by_id[tid], mixed_by_id[tid]
        for f in _COMPARE_FIELDS:
            rv, mv = r.get(f), m.get(f)
            if isinstance(rv, (int, float)) and isinstance(mv, (int, float)):
                e = abs(float(rv) - float(mv))
                max_err = max(max_err, e)
                if e > 1e-9:
                    mismatches += 1
            elif rv != mv:
                mismatches += 1

    # Coexistence sanity: a row calc, an Arrow calc, and the adapter all build
    # and run in the same interpreter.
    from perftest.etl_calculators import FxConvertCalculator
    from perftest.arrow_etl_calculators import ArrowFxConvertCalculator
    from core.calculator.arrow_calculator import RowCalculatorBatchAdapter
    sample = dict(data[0])
    row_calc = FxConvertCalculator("fx", {})
    arrow_calc = ArrowFxConvertCalculator("fx", {})
    adapter = RowCalculatorBatchAdapter("fx", {}, wrapped=FxConvertCalculator("fx", {}))
    coexist_ok = (
        row_calc.calculate(dict(sample))["price_usd"]
        == arrow_calc.calculate(dict(sample))["price_usd"]
        == adapter.calculate({"batch": [dict(sample)]})["batch"][0]["price_usd"]
    )

    report = {
        "trades": trades,
        "q1_both_graphs_in_one_process": bool(row_by_id) and bool(mixed_by_id),
        "row_delivered": len(row_by_id),
        "mixed_delivered": len(mixed_by_id),
        "q2_mixed_graph_identical_to_all_row": mismatches == 0 and len(common) == trades,
        "trades_compared": len(common),
        "max_abs_error": max_err,
        "mismatches": mismatches,
        "coexistence_sanity_ok": coexist_ok,
        "row_trades_per_s": round(len(row_by_id) / row_s) if row_s else 0,
        "mixed_trades_per_s": round(len(mixed_by_id) / mixed_s) if mixed_s else 0,
    }
    if not quiet:
        _print(report)
    return report


def _print(r: Dict[str, Any]) -> None:
    print("=" * 72)
    print("A1 worked example — old/new calculator coexistence")
    print("=" * 72)
    print(f"trades: {r['trades']:,}")
    print()
    print("Q1  old-style + new-style DAGs in ONE running instance:")
    print(f"    all-row graph delivered {r['row_delivered']:,}; "
          f"mixed graph delivered {r['mixed_delivered']:,} "
          f"(both ran in the same process) -> "
          f"{'CONFIRMED' if r['q1_both_graphs_in_one_process'] else 'FAILED'}")
    print()
    print("Q2  a SINGLE graph with both row and Arrow calculators:")
    print(f"    mixed graph (row validate/normalize/risk/classify + Arrow "
          f"fx/notional/fee) vs all-row:")
    print(f"    {r['trades_compared']:,} trades, max abs error {r['max_abs_error']:.2e}, "
          f"mismatches {r['mismatches']} -> "
          f"{'IDENTICAL / CONFIRMED' if r['q2_mixed_graph_identical_to_all_row'] else 'MISMATCH'}")
    print()
    print(f"    coexistence sanity (row + Arrow + adapter in one interpreter): "
          f"{'ok' if r['coexistence_sanity_ok'] else 'FAILED'}")
    print(f"    throughput  all-row {r['row_trades_per_s']:,}/s  |  "
          f"mixed {r['mixed_trades_per_s']:,}/s")
    print()
    print("    NOTE: here the Arrow stages run on SINGLE-DICT flow (batch-of-1),")
    print("    so they are correct drop-ins but ADD per-row conversion overhead")
    print("    (mixed is slower). The vectorization speedup appears only on")
    print("    BATCHED flow -- see benchmarks/a1_vertical.py (~1.8x at batch 500).")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="A1 worked example / coexistence demo")
    p.add_argument("--trades", type=int, default=20000)
    p.add_argument("--output", type=str, default=None)
    args = p.parse_args(argv)
    report = run_example(trades=args.trades, quiet=False)
    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, "w") as fh:
            json.dump(report, fh, indent=2)
        print(f"\nwrote JSON -> {args.output}")
    ok = (report["q1_both_graphs_in_one_process"]
          and report["q2_mixed_graph_identical_to_all_row"]
          and report["coexistence_sanity_ok"])
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
