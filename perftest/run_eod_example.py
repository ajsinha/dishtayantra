#!/usr/bin/env python3
"""End-of-day batch-file processing example.

Demonstrates using DishtaYantra for INTERRELATED batch files even though the data
is not a live stream. We have three EOD feeds:
  * trades       (the large "fact" feed)
  * fx_rates     (small "dimension" feed: currency -> USD rate)
  * client_limits(small "dimension" feed: client -> notional limit)

Pattern: load the small dimension feeds ONCE into in-memory lookup tables, then
REPLAY the trade file through the DAG (a bounded file is just a finite stream),
enriching and limit-checking each trade. Because each trade is independent given
the loaded tables, this needs no one-record-at-a-time ordering and can run at
high speed (here: a one-at-a-time path vs an auto-batched path, same results).

Usage:
    python -m perftest.run_eod_example --trades 20000
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# --- the three EOD feeds (dimension feeds are small; fact feed is large) ------
_FX_RATES = {"USD": 1.0, "EUR": 1.08, "GBP": 1.27, "JPY": 0.0067, "INR": 0.012}
_CLIENTS = ["C1", "C2", "C3", "C4", "C5"]
_CLIENT_LIMITS = {"C1": 5_000_000, "C2": 250_000, "C3": 1_000_000,
                  "C4": 75_000, "C5": 10_000_000}


def _trade_file(n: int) -> List[Dict[str, Any]]:
    rng = random.Random(20260614)
    syms = ["AAPL", "MSFT", "GOOG", "JPM", "XOM"]
    out = []
    for i in range(n):
        out.append({
            "trade_id": i,
            "client_id": rng.choice(_CLIENTS),
            "symbol": rng.choice(syms),
            "currency": rng.choice(list(_FX_RATES)),
            "quantity": rng.randint(1, 5000),
            "price": round(rng.uniform(1.0, 950.0), 4),
        })
    return out


_ENRICH = "perftest.eod_enrichment_calculators.ReferenceEnrichCalculator"
_ADAPTER = "core.calculator.arrow_calculator.RowCalculatorBatchAdapter"


def _sequential_config(in_q, out_q):
    return {
        "name": "eod_seq", "start_time": None, "end_time": None,
        "subscribers": [{"name": "s", "config": {"source": f"mem://queue/{in_q}", "max_depth": 2000000}}],
        "publishers": [{"name": "p", "config": {"destination": f"mem://queue/{out_q}"}}],
        "calculators": [{"name": "enrich", "type": _ENRICH,
                         "config": {"fx_rates": _FX_RATES, "client_limits": _CLIENT_LIMITS}}],
        "transformers": [],
        "nodes": [
            {"name": "ingest", "type": "SubscriptionNode", "config": {}, "subscriber": "s"},
            {"name": "enrich_node", "type": "CalculationNode", "config": {}, "calculator": "enrich"},
            {"name": "sink", "type": "PublicationNode", "config": {}, "publishers": ["p"]},
        ],
        "edges": [{"from_node": "ingest", "to_node": "enrich_node"},
                  {"from_node": "enrich_node", "to_node": "sink"}],
    }


def _batched_config(in_q, out_q):
    # Same enricher, wrapped by the adapter so it runs on the auto-batched path.
    return {
        "name": "eod_batched", "start_time": None, "end_time": None,
        "subscribers": [{"name": "s", "config": {"source": f"mem://queue/{in_q}", "max_depth": 2000000}}],
        "publishers": [{"name": "p", "config": {"destination": f"mem://queue/{out_q}"}}],
        "calculators": [{"name": "enrich", "type": _ADAPTER, "config": {
            "wrapped": {"type": _ENRICH,
                        "config": {"fx_rates": _FX_RATES, "client_limits": _CLIENT_LIMITS}}}}],
        "transformers": [],
        "nodes": [
            {"name": "ingest", "type": "BatchingSubscriptionNode", "config": {"batch": {"max_size": 500}}, "subscriber": "s"},
            {"name": "enrich_node", "type": "CalculationNode", "config": {}, "calculator": "enrich"},
            {"name": "sink", "type": "FlatteningPublicationNode", "config": {}, "publishers": ["p"]},
        ],
        "edges": [{"from_node": "ingest", "to_node": "enrich_node"},
                  {"from_node": "enrich_node", "to_node": "sink"}],
    }


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
        if isinstance(m, (bytes, bytearray)):
            m = m.decode("utf-8")
        d = json.loads(m) if isinstance(m, str) else m
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
    data = _trade_file(trades)

    seq, seq_s = _run(_sequential_config("eod_seq_in", "eod_seq_out"), "eod_seq_in", "eod_seq_out", data)
    bat, bat_s = _run(_batched_config("eod_bat_in", "eod_bat_out"), "eod_bat_in", "eod_bat_out", data)

    seq_breaches = sum(1 for d in seq.values() if d.get("limit_breach"))
    bat_breaches = sum(1 for d in bat.values() if d.get("limit_breach"))
    identical = (set(seq) == set(bat) and
                 all(seq[t]["notional_usd"] == bat[t]["notional_usd"] and
                     seq[t]["limit_breach"] == bat[t]["limit_breach"] for t in seq))

    report = {
        "trades": trades,
        "sequential_delivered": len(seq),
        "batched_delivered": len(bat),
        "limit_breaches": seq_breaches,
        "identical_results": identical and seq_breaches == bat_breaches,
        "sequential_trades_per_s": round(len(seq) / seq_s) if seq_s else 0,
        "batched_trades_per_s": round(len(bat) / bat_s) if bat_s else 0,
    }
    if not quiet:
        _print(report)
    return report


def _print(r):
    print("=" * 72)
    print("EOD batch-file processing — trades enriched vs FX + client-limit feeds")
    print("=" * 72)
    print(f"trade file: {r['trades']:,} records | dimension feeds loaded once into memory")
    print(f"  delivered: one-at-a-time {r['sequential_delivered']:,} | "
          f"auto-batched {r['batched_delivered']:,}")
    print(f"  limit breaches detected: {r['limit_breaches']:,}")
    print(f"  results identical across both paths: {r['identical_results']}")
    print(f"  throughput: one-at-a-time {r['sequential_trades_per_s']:,}/s | "
          f"auto-batched {r['batched_trades_per_s']:,}/s")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="EOD batch-file processing demo")
    p.add_argument("--trades", type=int, default=20000)
    args = p.parse_args(argv)
    r = run_example(trades=args.trades, quiet=False)
    return 0 if r["identical_results"] else 1


if __name__ == "__main__":
    sys.exit(main())
