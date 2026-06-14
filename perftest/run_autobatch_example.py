#!/usr/bin/env python3
"""A1 source-batching demonstration.

Shows that BatchingSubscriptionNode + FlatteningPublicationNode make the Arrow
vectorization win AUTOMATIC: the application sends ordinary per-message trades
and receives ordinary per-message results, while the DAG batches internally.

Compares the auto-batching Arrow DAG (perftest/perftest_arrow_autobatch.json)
against an all-row fx->notional->fee pipeline over the same trades, verifying
identical per-trade output and reporting throughput.

Usage:
    python -m perftest.run_autobatch_example --trades 20000
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
_AUTOBATCH_JSON = os.path.join(_HERE, "perftest_arrow_autobatch.json")

_ROW_STAGES = [
    ("fx", "perftest.etl_calculators.FxConvertCalculator"),
    ("notional", "perftest.etl_calculators.NotionalCalculator"),
    ("fee", "perftest.etl_calculators.FeeCalculator"),
]
_FIELDS = ["fx_rate_to_usd", "price_usd", "notional_usd", "signed_notional_usd",
           "commission_usd", "exchange_fee_usd", "total_fees_usd", "net_notional_usd"]


def _row_config(in_q, out_q):
    calcs = [{"name": n, "type": t, "config": {}} for n, t in _ROW_STAGES]
    nodes = [{"name": "ingest", "type": "SubscriptionNode", "config": {}, "subscriber": "in_sub"}]
    order = ["ingest"]
    for n, _ in _ROW_STAGES:
        nodes.append({"name": f"{n}_node", "type": "CalculationNode", "config": {}, "calculator": n})
        order.append(f"{n}_node")
    nodes.append({"name": "sink", "type": "PublicationNode", "config": {}, "publishers": ["out_pub"]})
    order.append("sink")
    return {
        "name": "all_row", "start_time": None, "end_time": None,
        "subscribers": [{"name": "in_sub", "config": {"source": f"mem://queue/{in_q}", "max_depth": 2000000}}],
        "publishers": [{"name": "out_pub", "config": {"destination": f"mem://queue/{out_q}"}}],
        "calculators": calcs, "transformers": [], "nodes": nodes,
        "edges": [{"from_node": order[i], "to_node": order[i + 1]} for i in range(len(order) - 1)],
    }


def _queues(cfg):
    s = cfg["subscribers"][0]["config"]["source"].rsplit("/", 1)[-1]
    d = cfg["publishers"][0]["config"]["destination"].rsplit("/", 1)[-1]
    return s, d


def _parse(m):
    if isinstance(m, (bytes, bytearray)):
        m = m.decode("utf-8")
    return json.loads(m) if isinstance(m, str) else m


def _run(cfg, in_q, out_q, trades, drain_timeout=90.0):
    from core.dag.compute_graph import ComputeGraph
    from core.pubsub.inmemorypubsub import InMemoryPubSub
    g = ComputeGraph(cfg); g.start(); time.sleep(0.5)
    ps = InMemoryPubSub()
    by_id: Dict[Any, Dict] = {}
    t0 = time.perf_counter()
    for t in trades:
        ps.publish_to_queue(in_q, json.dumps(t))
    deadline = time.time() + drain_timeout
    while len(by_id) < len(trades) and time.time() < deadline:
        m = ps.consume_from_queue(out_q)
        if m is None:
            time.sleep(0.002); continue
        d = _parse(m)
        if isinstance(d, dict) and "trade_id" in d:
            by_id[d["trade_id"]] = d
    elapsed = time.perf_counter() - t0
    # batching evidence from the source node, if reachable
    batch_in = batch_out = None
    nodes = getattr(g, "nodes", None)
    if isinstance(nodes, dict) and "ingest" in nodes:
        n = nodes["ingest"]
        batch_in = getattr(n, "_messages_in", None)
        batch_out = getattr(n, "_messages_out", None)
    try:
        g.stop()
    except Exception:
        pass
    return by_id, elapsed, batch_in, batch_out


def run_example(trades: int = 20000, quiet: bool = True) -> Dict[str, Any]:
    logging.disable(logging.INFO)
    gen = trade_generator()
    data = [gen(i) for i in range(trades)]

    with open(_AUTOBATCH_JSON) as fh:
        ab_cfg = json.load(fh)
    ab_in, ab_out = _queues(ab_cfg)
    row_cfg = _row_config("abrow_in", "abrow_out")
    r_in, r_out = _queues(row_cfg)

    row_by_id, row_s, _, _ = _run(row_cfg, r_in, r_out, data)
    ab_by_id, ab_s, b_in, b_out = _run(ab_cfg, ab_in, ab_out, data)

    common = set(row_by_id) & set(ab_by_id)
    mism, maxerr = 0, 0.0
    for tid in common:
        r, a = row_by_id[tid], ab_by_id[tid]
        for f in _FIELDS:
            e = abs(float(r[f]) - float(a[f])); maxerr = max(maxerr, e)
            if e > 1e-9:
                mism += 1

    report = {
        "trades": trades,
        "row_delivered": len(row_by_id),
        "autobatch_delivered": len(ab_by_id),
        "identical": mism == 0 and len(common) == trades,
        "max_abs_error": maxerr,
        "mismatches": mism,
        "row_trades_per_s": round(len(row_by_id) / row_s) if row_s else 0,
        "autobatch_trades_per_s": round(len(ab_by_id) / ab_s) if ab_s else 0,
        "speedup_x": round(row_s / ab_s, 2) if ab_s else 0.0,
        "source_messages_in": b_in,
        "source_envelopes_out": b_out,
    }
    if not quiet:
        _print(report)
    return report


def _print(r):
    print("=" * 72)
    print("A1 source-batching — per-message in/out, batched internally")
    print("=" * 72)
    print(f"trades: {r['trades']:,}")
    print(f"  per-message OUT delivered: all-row {r['row_delivered']:,} | "
          f"auto-batch {r['autobatch_delivered']:,}")
    print(f"  identical per-trade: {r['mismatches']} mismatches, max abs error "
          f"{r['max_abs_error']:.2e} -> "
          f"{'IDENTICAL' if r['identical'] else 'MISMATCH'}")
    if r["source_messages_in"] and r["source_envelopes_out"]:
        factor = r["source_messages_in"] / max(1, r["source_envelopes_out"])
        print(f"  internal batching at source: {r['source_messages_in']:,} messages "
              f"-> {r['source_envelopes_out']:,} envelopes (~{factor:.0f} msgs/batch)")
    print(f"  throughput: all-row {r['row_trades_per_s']:,}/s | "
          f"auto-batch {r['autobatch_trades_per_s']:,}/s | speedup {r['speedup_x']}x")
    print("  NOTE: the application sent and received ORDINARY per-message trades;")
    print("  batching happened inside the DAG (opt-in node types).")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="A1 source-batching demo")
    p.add_argument("--trades", type=int, default=20000)
    p.add_argument("--output", type=str, default=None)
    args = p.parse_args(argv)
    r = run_example(trades=args.trades, quiet=False)
    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, "w") as fh:
            json.dump(r, fh, indent=2)
        print(f"\nwrote JSON -> {args.output}")
    return 0 if r["identical"] else 1


if __name__ == "__main__":
    sys.exit(main())
