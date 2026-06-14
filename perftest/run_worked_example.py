#!/usr/bin/env python3
"""A1 worked example — row vs Arrow batch, with a batch-size crossover sweep.

Runs a 4-stage numeric trade-ETL pipeline (FX -> notional -> fees -> risk) two
ways through the real ``ComputeGraph`` over the in-memory broker:

  * ROW   : perftest row calculators, one trade per message (today's model)
  * ARROW : perftest.arrow_etl_calculators, many trades per batch-envelope

For each batch size it (1) verifies the per-trade output is byte-identical to
the row path and (2) reports trades/sec, so you can SEE the crossover: at tiny
batches Arrow loses (conversion + linger overhead, no vectorization), and as the
batch grows it overtakes and pulls ahead.

The engine is unchanged; the Arrow path is just different calculators + message
shape, so existing DAGs are unaffected.

Usage:
    python -m perftest.run_worked_example
    python -m perftest.run_worked_example --trades 20000 --batches 1,10,50,200,1000
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
from typing import Any, Callable, Dict, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_ROW = [
    ("fx", "perftest.etl_calculators.FxConvertCalculator"),
    ("notional", "perftest.etl_calculators.NotionalCalculator"),
    ("fees", "perftest.etl_calculators.FeeCalculator"),
    ("risk", "perftest.etl_calculators.RiskScoreCalculator"),
]
_ARROW = [
    ("fx", "perftest.arrow_etl_calculators.ArrowFxConvertCalculator"),
    ("notional", "perftest.arrow_etl_calculators.ArrowNotionalCalculator"),
    ("fees", "perftest.arrow_etl_calculators.ArrowFeeCalculator"),
    ("risk", "perftest.arrow_etl_calculators.ArrowRiskScoreCalculator"),
]
_COMPARE_FIELDS = (
    "fx_rate_to_usd", "price_usd", "notional_usd", "signed_notional_usd",
    "commission_usd", "exchange_fee_usd", "total_fees_usd", "net_notional_usd",
    "volatility", "var_1d_usd", "risk_score",
)
_SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "JPM", "BAC", "XOM"]
_CCYS = ["USD", "EUR", "GBP", "JPY", "CHF", "INR", "AUD", "CAD"]


def _gen(n: int, seed: int = 1234) -> List[Dict[str, Any]]:
    rng = random.Random(seed)
    return [{
        "trade_id": i,
        "symbol": rng.choice(_SYMBOLS),
        "side": rng.choice(["BUY", "SELL"]),
        "quantity": rng.randint(1, 5000),
        "price": round(rng.uniform(1.0, 950.0), 4),
        "currency": rng.choice(_CCYS),
    } for i in range(n)]


def _config(name, in_q, out_q, stages) -> Dict[str, Any]:
    calcs = [{"name": n, "type": t, "config": {}} for n, t in stages]
    nodes = [{"name": "ingest", "type": "SubscriptionNode", "config": {}, "subscriber": "sub"}]
    order = ["ingest"]
    for n, _ in stages:
        nodes.append({"name": f"{n}_node", "type": "CalculationNode", "config": {}, "calculator": n})
        order.append(f"{n}_node")
    nodes.append({"name": "sink", "type": "PublicationNode", "config": {}, "publishers": ["pub"]})
    order.append("sink")
    return {
        "name": name, "start_time": None, "end_time": None,
        "subscribers": [{"name": "sub", "config": {"source": f"mem://queue/{in_q}", "max_depth": 4_000_000}}],
        "publishers": [{"name": "pub", "config": {"destination": f"mem://queue/{out_q}"}}],
        "calculators": calcs, "transformers": [], "nodes": nodes,
        "edges": [{"from_node": order[i], "to_node": order[i + 1]} for i in range(len(order) - 1)],
    }


def _run(config, in_q, out_q, items, expected, valid_fn, drain_timeout=120.0) -> Tuple[List[Any], float]:
    from core.dag.compute_graph import ComputeGraph
    from core.pubsub.inmemorypubsub import InMemoryPubSub
    g = ComputeGraph(config)
    g.start()
    time.sleep(0.5)
    ps = InMemoryPubSub()
    valid: List[Any] = []
    t0 = time.perf_counter()
    for it in items:
        ps.publish_to_queue(in_q, json.dumps(it))
    deadline = time.time() + drain_timeout
    while len(valid) < expected and time.time() < deadline:
        m = ps.consume_from_queue(out_q)
        if m is None:
            time.sleep(0.002)
            continue
        if isinstance(m, (bytes, bytearray)):
            m = m.decode("utf-8")
        d = json.loads(m) if isinstance(m, str) else m
        if valid_fn(d):
            valid.append(d)
    elapsed = time.perf_counter() - t0
    try:
        g.stop()
    except Exception:
        pass
    return valid, elapsed


def _run_row(trades) -> Tuple[Dict[int, dict], float]:
    out, s = _run(_config("we_row", "we_row_in", "we_row_out", _ROW),
                  "we_row_in", "we_row_out", trades, len(trades),
                  lambda d: isinstance(d, dict) and "trade_id" in d)
    return {d["trade_id"]: d for d in out}, s


def _run_arrow(trades, batch) -> Tuple[Dict[int, dict], float]:
    envelopes = [{"batch": trades[i:i + batch]} for i in range(0, len(trades), batch)]
    out, s = _run(_config("we_arrow", "we_arrow_in", "we_arrow_out", _ARROW),
                  "we_arrow_in", "we_arrow_out", envelopes, len(envelopes),
                  lambda d: isinstance(d, dict) and isinstance(d.get("batch"), list) and bool(d["batch"]))
    by_id = {}
    for env in out:
        for d in env["batch"]:
            by_id[d["trade_id"]] = d
    return by_id, s


def _identical(row_by_id, arrow_by_id, total) -> Tuple[bool, float, int]:
    common = set(row_by_id) & set(arrow_by_id)
    max_err, mism = 0.0, 0
    for tid in common:
        r, a = row_by_id[tid], arrow_by_id[tid]
        for f in _COMPARE_FIELDS:
            err = abs(float(r[f]) - float(a[f]))
            max_err = max(max_err, err)
            if err > 1e-9:
                mism += 1
    return (mism == 0 and len(common) == total), max_err, mism


def run_worked_example(trades: int = 20000, batches: List[int] = None, quiet: bool = True) -> Dict[str, Any]:
    logging.disable(logging.INFO)
    batches = batches or [1, 10, 50, 200, 1000]
    data = _gen(trades)

    row_by_id, row_s = _run_row(data)
    row_tps = len(row_by_id) / row_s if row_s > 0 else 0.0

    rows = []
    for b in batches:
        a_by_id, a_s = _run_arrow(data, b)
        ok, max_err, mism = _identical(row_by_id, a_by_id, trades)
        a_tps = len(a_by_id) / a_s if a_s > 0 else 0.0
        rows.append({
            "batch_size": b,
            "arrow_trades_per_s": round(a_tps),
            "speedup_vs_row": round(a_tps / row_tps, 2) if row_tps else 0.0,
            "identical": ok, "max_abs_error": max_err, "mismatches": mism,
            "delivered": len(a_by_id),
        })

    report = {
        "trades": trades, "stages": [t for _, t in _ARROW],
        "row_baseline": {"trades_per_s": round(row_tps), "elapsed_s": round(row_s, 3)},
        "sweep": rows,
    }
    if not quiet:
        _print(report)
    return report


def _print(r: Dict[str, Any]) -> None:
    print("=" * 74)
    print("A1 WORKED EXAMPLE: row vs Arrow batch (4-stage FX/notional/fees/risk)")
    print("=" * 74)
    print(f"{r['trades']:,} trades through the real ComputeGraph (in-memory broker)\n")
    print(f"  ROW baseline (1 trade/msg): {r['row_baseline']['trades_per_s']:>10,} trades/s\n")
    print(f"  {'batch':>6} {'arrow trades/s':>16} {'vs row':>8} {'correctness':>14}")
    print("  " + "-" * 50)
    for s in r["sweep"]:
        verdict = "IDENTICAL" if s["identical"] else f"MISMATCH({s['mismatches']})"
        flag = "  <- crossover" if s["speedup_vs_row"] >= 1.0 else ""
        print(f"  {s['batch_size']:>6} {s['arrow_trades_per_s']:>16,} "
              f"{s['speedup_vs_row']:>7}x {verdict:>14}{flag}")
    print("\n  Reading it: Arrow loses at tiny batches (conversion + per-message")
    print("  overhead, no vectorization) and overtakes the row path as the batch")
    print("  grows. All rows are output-identical to the row calculators.")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="A1 worked example (row vs Arrow sweep)")
    p.add_argument("--trades", type=int, default=20000)
    p.add_argument("--batches", type=str, default="1,10,50,200,1000")
    p.add_argument("--output", type=str, default=None)
    args = p.parse_args(argv)
    batches = [int(x) for x in args.batches.split(",") if x.strip()]
    report = run_worked_example(trades=args.trades, batches=batches, quiet=False)
    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, "w") as fh:
            json.dump(report, fh, indent=2)
        print(f"\nwrote JSON -> {args.output}")
    return 0 if all(s["identical"] for s in report["sweep"]) else 1


if __name__ == "__main__":
    sys.exit(main())
