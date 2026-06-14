#!/usr/bin/env python3
"""A1 Arrow data-plane spike (roadmap Phase 1, step A1 de-risking).

Proves the central A1 claim with real numbers on this workload: processing the
*same* trade-ETL numerics as an Arrow columnar batch (vectorized pyarrow.compute
kernels) is dramatically faster than the engine's current row-at-a-time model
(``calculator.calculate(one_dict)`` per message, see
``core/dag/node_implementations.py``), while producing identical results.

It is a controlled microbenchmark that isolates the single variable that A1
changes — row loop vs. columnar batch — rather than a production engine change.
The production migration would re-express the real calculators this way behind
the ``ArrowCalculator`` contract proposed in ``docs/design/A1-arrow-data-plane.md``.

Usage:
    python -m benchmarks.arrow_vectorization_spike
    python -m benchmarks.arrow_vectorization_spike --rows 500000 --batch 20000 --output a1.json
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from statistics import median
from typing import Any, Dict, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyarrow as pa
import pyarrow.compute as pc

# Shared business constants (identical in both code paths).
_CCY = ["USD", "EUR", "GBP", "JPY", "CHF", "INR", "AUD", "CAD"]
_RATE = [1.0, 1.08, 1.27, 0.0067, 1.12, 0.012, 0.66, 0.73]
_FX = dict(zip(_CCY, _RATE))
_FEE_RATE = 0.0002
_RISK_DIV = 1.0e7
_SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "JPM", "BAC", "XOM"]


# --------------------------------------------------------------------------- #
# Data generation (produced once, consumed by both paths)
# --------------------------------------------------------------------------- #
def make_data(n: int, seed: int = 7) -> Tuple[List[Dict[str, Any]], pa.RecordBatch]:
    rng = random.Random(seed)
    tid, sym, side, qty, price, ccy = [], [], [], [], [], []
    for i in range(n):
        tid.append(i)
        sym.append(rng.choice(_SYMBOLS))
        side.append(rng.choice(["BUY", "SELL"]))
        qty.append(rng.randint(1, 5000))
        price.append(round(rng.uniform(1.0, 950.0), 4))
        ccy.append(rng.choice(_CCY))
    rows = [
        {"trade_id": tid[i], "symbol": sym[i], "side": side[i],
         "quantity": qty[i], "price": price[i], "currency": ccy[i]}
        for i in range(n)
    ]
    batch = pa.record_batch({
        "trade_id": pa.array(tid, pa.int64()),
        "symbol": pa.array(sym, pa.string()),
        "side": pa.array(side, pa.string()),
        "quantity": pa.array(qty, pa.int64()),
        "price": pa.array(price, pa.float64()),
        "currency": pa.array(ccy, pa.string()),
    })
    return rows, batch


# --------------------------------------------------------------------------- #
# Path A: row-at-a-time (the engine's model today)
# --------------------------------------------------------------------------- #
def row_etl(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        rate = _FX[r["currency"]]
        price_usd = r["price"] * rate
        signed = 1.0 if r["side"] == "BUY" else -1.0
        notional = r["quantity"] * price_usd * signed
        fee = abs(notional) * _FEE_RATE
        risk = abs(notional) / _RISK_DIV
        if risk > 1.0:
            risk = 1.0
        d = dict(r)
        d["price_usd"] = price_usd
        d["notional"] = notional
        d["fee"] = fee
        d["risk_score"] = risk
        out.append(d)
    return out


# --------------------------------------------------------------------------- #
# Path B: Arrow columnar (the A1 proposal) - vectorized pyarrow.compute kernels
# --------------------------------------------------------------------------- #
def arrow_etl(batch: pa.RecordBatch) -> pa.RecordBatch:
    price = batch.column("price")
    qty = pc.cast(batch.column("quantity"), pa.float64())
    side = batch.column("side")
    ccy = batch.column("currency")

    # Vectorized FX lookup: currency -> index -> rate (no Python loop).
    idx = pc.index_in(ccy, value_set=pa.array(_CCY))
    rate = pa.array(_RATE, pa.float64()).take(idx)

    price_usd = pc.multiply(price, rate)
    signed = pc.if_else(pc.equal(side, "BUY"), 1.0, -1.0)
    notional = pc.multiply(pc.multiply(qty, price_usd), signed)
    abs_notional = pc.abs(notional)
    fee = pc.multiply(abs_notional, _FEE_RATE)
    risk_raw = pc.divide(abs_notional, _RISK_DIV)
    risk = pc.if_else(pc.greater(risk_raw, 1.0), 1.0, risk_raw)

    arrays = list(batch.columns) + [price_usd, notional, fee, risk]
    names = list(batch.schema.names) + ["price_usd", "notional", "fee", "risk_score"]
    return pa.record_batch(arrays, names=names)


# --------------------------------------------------------------------------- #
# Correctness: both paths must agree
# --------------------------------------------------------------------------- #
def verify_equivalence(rows_out: List[Dict[str, Any]], batch_out: pa.RecordBatch,
                       tol: float = 1e-6) -> Dict[str, Any]:
    cols = {f: batch_out.column(f).to_pylist()
            for f in ("price_usd", "notional", "fee", "risk_score")}
    max_abs_err = 0.0
    n = len(rows_out)
    for i in range(n):
        for f in cols:
            a = rows_out[i][f]
            b = cols[f][i]
            err = abs(a - b)
            denom = max(1.0, abs(a))
            max_abs_err = max(max_abs_err, err / denom)
    return {"rows_checked": n, "max_relative_error": max_abs_err,
            "equivalent": max_abs_err <= tol}


# --------------------------------------------------------------------------- #
# Spike
# --------------------------------------------------------------------------- #
def run_spike(rows: int = 200000, batch_size: int = 10000, repeats: int = 3,
              quiet: bool = True) -> Dict[str, Any]:
    row_data, arrow_batch = make_data(rows)

    # Correctness first (on a sample to keep the O(n*fields) check cheap).
    sample = min(rows, 20000)
    eq = verify_equivalence(row_etl(row_data[:sample]),
                            arrow_etl(arrow_batch.slice(0, sample)))

    # Time the row path.
    row_times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        row_etl(row_data)
        row_times.append(time.perf_counter() - t0)
    row_s = median(row_times)

    # Time the Arrow path, processing in micro-batches of batch_size to model a
    # streaming accumulator (the engine would batch messages at the node).
    arrow_times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        off = 0
        while off < rows:
            arrow_etl(arrow_batch.slice(off, batch_size))
            off += batch_size
        arrow_times.append(time.perf_counter() - t0)
    arrow_s = median(arrow_times)

    row_rps = rows / row_s if row_s > 0 else 0.0
    arrow_rps = rows / arrow_s if arrow_s > 0 else 0.0
    speedup = (row_s / arrow_s) if arrow_s > 0 else 0.0

    report = {
        "pyarrow_version": pa.__version__,
        "python_version": sys.version.split()[0],
        "rows": rows,
        "batch_size": batch_size,
        "repeats": repeats,
        "row_at_a_time": {"median_s": round(row_s, 4), "rows_per_s": round(row_rps)},
        "arrow_vectorized": {"median_s": round(arrow_s, 4), "rows_per_s": round(arrow_rps)},
        "speedup_x": round(speedup, 2),
        "correctness": eq,
    }
    if not quiet:
        _print(report)
    return report


def _print(r: Dict[str, Any]) -> None:
    print("=" * 70)
    print("A1 Arrow data-plane spike: row-at-a-time vs columnar vectorized")
    print("=" * 70)
    print(f"pyarrow {r['pyarrow_version']} | python {r['python_version']} | "
          f"{r['rows']:,} rows | batch {r['batch_size']:,}")
    print()
    print(f"  row-at-a-time (today) : {r['row_at_a_time']['median_s']:>7.3f}s  "
          f"{r['row_at_a_time']['rows_per_s']:>12,} rows/s")
    print(f"  Arrow vectorized (A1) : {r['arrow_vectorized']['median_s']:>7.3f}s  "
          f"{r['arrow_vectorized']['rows_per_s']:>12,} rows/s")
    print()
    print(f"  SPEEDUP: {r['speedup_x']}x")
    c = r["correctness"]
    print(f"  CORRECTNESS: max relative error {c['max_relative_error']:.2e} "
          f"over {c['rows_checked']:,} rows -> "
          f"{'IDENTICAL' if c['equivalent'] else 'MISMATCH'}")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="A1 Arrow data-plane spike")
    p.add_argument("--rows", type=int, default=200000)
    p.add_argument("--batch", type=int, default=10000)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--output", type=str, default=None)
    args = p.parse_args(argv)

    report = run_spike(rows=args.rows, batch_size=args.batch,
                       repeats=args.repeats, quiet=False)
    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, "w") as fh:
            json.dump(report, fh, indent=2)
        print(f"\nwrote JSON -> {args.output}")
    return 0 if report["correctness"]["equivalent"] else 1


if __name__ == "__main__":
    sys.exit(main())
