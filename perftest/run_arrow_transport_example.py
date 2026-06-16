#!/usr/bin/env python3
"""A1 — RecordBatch-on-edges (zero-copy transport) worked example.

Runs the SAME 3-stage Arrow ETL pipeline two ways and compares them:
  * envelope path  — BatchingSubscriptionNode + dict {"batch":[...]} on the edges
                     (v4.5.0; the engine deep-copies the envelope at every stage);
  * transport path — ArrowBatchingSubscriptionNode + a pyarrow.RecordBatch on the
                     edges (v5.1.0; the batch is shared by reference, no per-stage
                     copy; dict<->Arrow conversion happens once in, once out).

It verifies the per-trade output is identical and reports throughput, so the
benefit of removing the per-stage copies is measured, not asserted.

Usage:
    python -m perftest.run_arrow_transport_example --trades 20000
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from benchmarks.workloads import trade_generator  # noqa: E402

_HERE = os.path.dirname(os.path.abspath(__file__))
_ENVELOPE_JSON = os.path.join(_HERE, "perftest_arrow_autobatch.json")
_TRANSPORT_JSON = os.path.join(_HERE, "perftest_arrow_transport.json")

_FIELDS = ["fx_rate_to_usd", "price_usd", "notional_usd", "signed_notional_usd",
           "commission_usd", "exchange_fee_usd", "total_fees_usd", "net_notional_usd"]


def _queues(cfg):
    s = cfg["subscribers"][0]["config"]["source"].rsplit("/", 1)[-1]
    d = cfg["publishers"][0]["config"]["destination"].rsplit("/", 1)[-1]
    return s, d


def _parse(m):
    if isinstance(m, (bytes, bytearray)):
        m = m.decode("utf-8")
    return json.loads(m) if isinstance(m, str) else m


def _run(cfg, trades, drain_timeout=90.0):
    from core.dag.compute_graph import ComputeGraph
    from core.pubsub.inmemorypubsub import InMemoryPubSub
    in_q, out_q = _queues(cfg)
    g = ComputeGraph(cfg); g.start(); time.sleep(0.5)
    ps = InMemoryPubSub()
    by_id: Dict[Any, Dict] = {}
    t0 = time.perf_counter()
    for t in trades:
        ps.publish_to_queue(in_q, json.dumps(t))
    deadline = time.time() + drain_timeout
    while len(by_id) < len(trades) and time.time() < deadline:
        m = ps.consume_from_queue(out_q, block=False)
        if m is None:
            time.sleep(0.002); continue
        d = _parse(m)
        if isinstance(d, dict) and "trade_id" in d:
            by_id[d["trade_id"]] = d
    elapsed = time.perf_counter() - t0
    try:
        g.stop()
    except Exception:
        pass
    return by_id, elapsed


def run_example(trades: int = 20000, quiet: bool = True) -> Dict[str, Any]:
    logging.disable(logging.INFO)
    gen = trade_generator()
    data = [gen(i) for i in range(trades)]

    with open(_ENVELOPE_JSON) as fh:
        env_cfg = json.load(fh)
    with open(_TRANSPORT_JSON) as fh:
        tr_cfg = json.load(fh)

    env_by_id, env_s = _run(env_cfg, data)
    tr_by_id, tr_s = _run(tr_cfg, data)

    common = set(env_by_id) & set(tr_by_id)
    mism, maxerr = 0, 0.0
    for tid in common:
        e_rec, t_rec = env_by_id[tid], tr_by_id[tid]
        for f in _FIELDS:
            err = abs(float(e_rec[f]) - float(t_rec[f])); maxerr = max(maxerr, err)
            if err > 1e-9:
                mism += 1

    report = {
        "trades": trades,
        "envelope_delivered": len(env_by_id),
        "transport_delivered": len(tr_by_id),
        "identical": mism == 0 and len(common) == trades,
        "max_abs_error": maxerr,
        "mismatches": mism,
        "envelope_trades_per_s": round(len(env_by_id) / env_s) if env_s else 0,
        "transport_trades_per_s": round(len(tr_by_id) / tr_s) if tr_s else 0,
        "transport_speedup_x": round(env_s / tr_s, 2) if tr_s else 0.0,
    }
    if not quiet:
        _print(report)
    return report


def _print(r):
    print("=" * 72)
    print("A1 — RecordBatch on edges: dict envelope vs zero-copy Arrow transport")
    print("=" * 72)
    print(f"trades: {r['trades']:,}")
    print(f"  delivered: envelope={r['envelope_delivered']:,} transport={r['transport_delivered']:,}")
    print(f"  identical per-trade: {r['mismatches']} mismatches, max abs error "
          f"{r['max_abs_error']:.2e}  ->  "
          f"{'IDENTICAL' if r['identical'] else 'MISMATCH'}")
    print(f"  throughput: envelope {r['envelope_trades_per_s']:,}/s | "
          f"transport {r['transport_trades_per_s']:,}/s")
    print(f"  transport speedup vs envelope: {r['transport_speedup_x']}x")
    print("  same pipeline, same results; transport shares the RecordBatch by")
    print("  reference instead of deep-copying a dict envelope at every stage.")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="RecordBatch-on-edges worked example")
    p.add_argument("--trades", type=int, default=20000)
    args = p.parse_args(argv)
    r = run_example(trades=args.trades, quiet=False)
    return 0 if r["identical"] else 1


if __name__ == "__main__":
    sys.exit(main())
