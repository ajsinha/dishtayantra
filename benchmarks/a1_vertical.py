#!/usr/bin/env python3
"""A1 vertical slice — end-to-end through the real engine (roadmap Phase 1).

Runs the SAME two-stage trade pipeline (FX convert -> notional) two ways through
the actual ``ComputeGraph`` over the in-memory broker:

  * ROW   : perftest row calculators, one trade per message (today's model)
  * ARROW : vectorized ArrowCalculators, many trades per batch-envelope message

then (1) verifies the per-trade outputs are identical and (2) reports
trades/sec for each. The engine itself is unchanged — the Arrow path is just a
different calculator + message shape — so existing DAGs are unaffected.

Usage:
    python -m benchmarks.a1_vertical --trades 20000 --batch 500
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from benchmarks.workloads import trade_generator  # noqa: E402

_FX = "FxConvert"
_NOT = "Notional"


def _linear_config(name, in_q, out_q, fx_type, not_type) -> Dict[str, Any]:
    return {
        "name": name, "start_time": None, "end_time": None,
        "subscribers": [{"name": "sub", "config": {"source": f"mem://queue/{in_q}", "max_depth": 2_000_000}}],
        "publishers": [{"name": "pub", "config": {"destination": f"mem://queue/{out_q}"}}],
        "calculators": [
            {"name": "fx", "type": fx_type, "config": {}},
            {"name": "notional", "type": not_type, "config": {}},
        ],
        "transformers": [],
        "nodes": [
            {"name": "ingest", "type": "SubscriptionNode", "config": {}, "subscriber": "sub"},
            {"name": "fx_node", "type": "CalculationNode", "config": {}, "calculator": "fx"},
            {"name": "notional_node", "type": "CalculationNode", "config": {}, "calculator": "notional"},
            {"name": "sink", "type": "PublicationNode", "config": {}, "publishers": ["pub"]},
        ],
        "edges": [
            {"from_node": "ingest", "to_node": "fx_node"},
            {"from_node": "fx_node", "to_node": "notional_node"},
            {"from_node": "notional_node", "to_node": "sink"},
        ],
    }


def _parse(msg: Any) -> Any:
    if isinstance(msg, (bytes, bytearray)):
        msg = msg.decode("utf-8")
    if isinstance(msg, str):
        return json.loads(msg)
    return msg


def _run(config, in_q, out_q, publish_items, expected_valid, valid_fn, drain_timeout=60.0):
    """Start a graph, publish items, drain until `expected_valid` messages pass
    `valid_fn`; return (parsed_valid_messages, elapsed_seconds).

    The reactive dataflow emits an initial empty ``{}`` once at startup; valid_fn
    filters those out so counts line up with real data.
    """
    from core.dag.compute_graph import ComputeGraph
    from core.pubsub.inmemorypubsub import InMemoryPubSub

    graph = ComputeGraph(config)
    graph.start()
    time.sleep(0.5)
    ps = InMemoryPubSub()

    valid: List[Any] = []
    t0 = time.perf_counter()
    for item in publish_items:
        ps.publish_to_queue(in_q, json.dumps(item))
    deadline = time.time() + drain_timeout
    while len(valid) < expected_valid and time.time() < deadline:
        m = ps.consume_from_queue(out_q)
        if m is None:
            time.sleep(0.002)
            continue
        d = _parse(m)
        if valid_fn(d):
            valid.append(d)
    elapsed = time.perf_counter() - t0
    try:
        graph.stop()
    except Exception:
        pass
    return valid, elapsed


def run_compare(trades: int = 20000, batch: int = 500, quiet: bool = True) -> Dict[str, Any]:
    logging.disable(logging.INFO)
    gen = trade_generator()
    data = [gen(i) for i in range(trades)]

    # ROW path: one trade per message.
    row_valid, row_s = _run(
        _linear_config("a1_row", "a1_row_in", "a1_row_out",
                       "perftest.etl_calculators.FxConvertCalculator",
                       "perftest.etl_calculators.NotionalCalculator"),
        "a1_row_in", "a1_row_out", data, trades,
        lambda d: isinstance(d, dict) and "trade_id" in d,
    )
    row_by_id = {d["trade_id"]: d for d in row_valid}

    # ARROW path: batch-envelope messages, vectorized calculators.
    envelopes = [{"batch": data[i:i + batch]} for i in range(0, trades, batch)]
    arrow_valid, arrow_s = _run(
        _linear_config("a1_arrow", "a1_arrow_in", "a1_arrow_out",
                       "benchmarks.arrow_trade_calculators.ArrowFxConvertCalculator",
                       "benchmarks.arrow_trade_calculators.ArrowNotionalCalculator"),
        "a1_arrow_in", "a1_arrow_out", envelopes, len(envelopes),
        lambda d: isinstance(d, dict) and isinstance(d.get("batch"), list) and bool(d["batch"]),
    )
    arrow_by_id = {}
    for env in arrow_valid:
        for d in env.get("batch", []):
            arrow_by_id[d["trade_id"]] = d

    # Correctness: identical per-trade outputs on the computed fields.
    fields = ("fx_rate_to_usd", "price_usd", "notional_usd", "signed_notional_usd")
    checked, max_err, mismatches = 0, 0.0, 0
    common = set(row_by_id) & set(arrow_by_id)
    for tid in common:
        r, a = row_by_id[tid], arrow_by_id[tid]
        for f in fields:
            checked += 1
            err = abs(float(r[f]) - float(a[f]))
            max_err = max(max_err, err)
            if err > 1e-9:
                mismatches += 1

    row_tps = len(row_by_id) / row_s if row_s > 0 else 0.0
    arrow_tps = len(arrow_by_id) / arrow_s if arrow_s > 0 else 0.0

    report = {
        "trades": trades,
        "batch_size": batch,
        "row_delivered": len(row_by_id),
        "arrow_delivered": len(arrow_by_id),
        "row": {"elapsed_s": round(row_s, 3), "trades_per_s": round(row_tps)},
        "arrow": {"elapsed_s": round(arrow_s, 3), "trades_per_s": round(arrow_tps)},
        "speedup_x": round((row_s / arrow_s), 2) if arrow_s > 0 else 0.0,
        "correctness": {
            "trades_compared": len(common),
            "fields_checked": checked,
            "max_abs_error": max_err,
            "mismatches": mismatches,
            "identical": mismatches == 0 and len(common) == trades,
        },
    }
    if not quiet:
        _print(report)
    return report


def _print(r: Dict[str, Any]) -> None:
    print("=" * 70)
    print("A1 vertical slice: row vs Arrow batch, end-to-end through ComputeGraph")
    print("=" * 70)
    print(f"{r['trades']:,} trades | batch {r['batch_size']}")
    print(f"  ROW   : {r['row']['elapsed_s']:>6.2f}s  {r['row']['trades_per_s']:>10,} trades/s "
          f"(delivered {r['row_delivered']:,})")
    print(f"  ARROW : {r['arrow']['elapsed_s']:>6.2f}s  {r['arrow']['trades_per_s']:>10,} trades/s "
          f"(delivered {r['arrow_delivered']:,})")
    print(f"  end-to-end speedup: {r['speedup_x']}x")
    c = r["correctness"]
    print(f"  CORRECTNESS: {c['trades_compared']:,} trades, max abs error "
          f"{c['max_abs_error']:.2e}, mismatches {c['mismatches']} -> "
          f"{'IDENTICAL' if c['identical'] else 'MISMATCH'}")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="A1 vertical slice comparison")
    p.add_argument("--trades", type=int, default=20000)
    p.add_argument("--batch", type=int, default=500)
    p.add_argument("--output", type=str, default=None)
    args = p.parse_args(argv)
    report = run_compare(trades=args.trades, batch=args.batch, quiet=False)
    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, "w") as fh:
            json.dump(report, fh, indent=2)
        print(f"\nwrote JSON -> {args.output}")
    return 0 if report["correctness"]["identical"] else 1


if __name__ == "__main__":
    sys.exit(main())
