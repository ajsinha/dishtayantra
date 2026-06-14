#!/usr/bin/env python3
"""Free-threading readiness spike (roadmap Phase 0, step A3 de-risking).

Goes beyond ``scripts/check_free_threading.py`` (which only checks whether
imports re-enable the GIL) by *measuring* how CPU-bound calculator work scales
across threads on the current interpreter:

  * On a standard build, threads do not speed up pure-Python CPU work -> this
    quantifies the GIL ceiling we want to remove.
  * On a free-threaded build (python3.14t), wall time should drop ~linearly ->
    this quantifies the no-GIL upside for the DAG/calculator layer.

It also inventories key native extensions and flags any that re-enable the GIL
at import time (which would silently defeat free-threading).

Usage:
    python -m benchmarks.freethreading_spike
    python -m benchmarks.freethreading_spike --calls 120000 --threads 1,2,4,8
    python3.14t -m benchmarks.freethreading_spike --output ft_report.json
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Native/important extensions whose free-threading readiness matters most.
_KEY_EXTENSIONS = [
    ("pyarrow", "Arrow columnar (C++) - the planned data plane"),
    ("lmdb", "cross-worker zero-copy store (C)"),
    ("polars", "data processing (Rust)"),
    ("pandas", "data processing (C/Cython)"),
    ("psutil", "worker metrics (C)"),
    ("confluent_kafka", "Kafka (C/librdkafka)"),
    ("msgpack", "message packing (C)"),
]


def gil_status() -> Optional[bool]:
    fn = getattr(sys, "_is_gil_enabled", None)
    if fn is None:
        return None
    try:
        return bool(fn())
    except Exception:
        return None


def _make_chain():
    """A real linear trade-ETL calculator chain (pure-Python CPU work)."""
    from perftest.etl_calculators import (
        ValidateTradeCalculator,
        NormalizeTradeCalculator,
        FxConvertCalculator,
        NotionalCalculator,
        FeeCalculator,
        RiskScoreCalculator,
    )

    classes = [
        ValidateTradeCalculator,
        NormalizeTradeCalculator,
        FxConvertCalculator,
        NotionalCalculator,
        FeeCalculator,
        RiskScoreCalculator,
    ]
    return [cls(cls.__name__, {}) for cls in classes]


def _sample_trades(n: int) -> List[Dict[str, Any]]:
    import random

    rng = random.Random(7)
    syms = ["AAPL", "MSFT", "GOOG", "JPM", "XOM"]
    ccys = ["USD", "EUR", "GBP", "JPY", "INR"]
    out = []
    for i in range(n):
        out.append(
            {
                "trade_id": i,
                "symbol": rng.choice(syms),
                "side": rng.choice(["BUY", "SELL"]),
                "quantity": rng.randint(1, 5000),
                "price": round(rng.uniform(1.0, 950.0), 4),
                "currency": rng.choice(ccys),
            }
        )
    return out


def _process(trades: List[Dict[str, Any]]) -> int:
    chain = _make_chain()  # own chain per worker (no shared-state contention)
    for t in trades:
        d = t
        for calc in chain:
            d = calc.calculate(d)
    return len(trades)


def _run_at_threads(trades: List[Dict[str, Any]], n_threads: int) -> float:
    """Process all `trades` split across n_threads; return wall seconds."""
    if n_threads <= 1:
        t0 = time.perf_counter()
        _process(trades)
        return time.perf_counter() - t0

    chunk = (len(trades) + n_threads - 1) // n_threads
    parts = [trades[i:i + chunk] for i in range(0, len(trades), chunk)]
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        list(ex.map(_process, parts))
    return time.perf_counter() - t0


def _extension_inventory() -> List[Dict[str, Any]]:
    rows = []
    for name, why in _KEY_EXTENSIONS:
        before = gil_status()
        try:
            importlib.import_module(name)
        except ImportError:
            rows.append({"name": name, "status": "missing", "why": why})
            continue
        except Exception as exc:  # noqa: BLE001
            rows.append({"name": name, "status": f"import_error:{type(exc).__name__}", "why": why})
            continue
        after = gil_status()
        if before is False and after is True:
            rows.append({"name": name, "status": "RE_ENABLED_GIL", "why": why})
        else:
            rows.append({"name": name, "status": "ok", "why": why})
    return rows


def run_spike(
    calls: int = 60000,
    thread_counts: Optional[List[int]] = None,
    check_extensions: bool = True,
    quiet: bool = True,
) -> Dict[str, Any]:
    cpu = os.cpu_count() or 1
    if thread_counts is None:
        thread_counts = [t for t in (1, 2, 4, 8) if t <= max(2, cpu)]
        if 1 not in thread_counts:
            thread_counts = [1] + thread_counts

    trades = _sample_trades(calls)

    capable = hasattr(sys, "_is_gil_enabled")
    gil_before_work = gil_status()

    scaling: List[Dict[str, Any]] = []
    baseline: Optional[float] = None
    for n in thread_counts:
        wall = _run_at_threads(trades, n)
        if n == 1:
            baseline = wall
        speedup = (baseline / wall) if (baseline and wall > 0) else 0.0
        scaling.append(
            {
                "threads": n,
                "wall_s": round(wall, 4),
                "speedup": round(speedup, 3),
                "efficiency": round(speedup / n, 3) if n else 0.0,
            }
        )

    extensions = _extension_inventory() if check_extensions else []
    gil_after = gil_status()
    blockers = [e["name"] for e in extensions if e["status"] == "RE_ENABLED_GIL"]

    max_speedup = max((s["speedup"] for s in scaling), default=0.0)
    max_eff = max((s["efficiency"] for s in scaling if s["threads"] > 1), default=0.0)

    if not capable:
        verdict = (
            "STANDARD BUILD (GIL present). The flat scaling above is the GIL "
            "ceiling we aim to remove. Re-run on a python3.14t free-threaded "
            "build to measure the no-GIL upside for the calculator layer."
        )
    elif blockers:
        verdict = (
            "BLOCKED: free-threading is capable but these extensions re-enable "
            f"the GIL at import: {', '.join(blockers)}. Upgrade to free-threaded "
            "wheels before relying on no-GIL parallelism."
        )
    elif gil_after is False and max_eff >= 0.6:
        verdict = (
            f"PROMISING: GIL off and calculator work scales to ~{max_speedup:.1f}x "
            f"(peak efficiency {max_eff:.0%}). Proceed to functional + stress "
            "testing of the DAG scheduler under free-threading."
        )
    elif gil_after is False:
        verdict = (
            f"MIXED: GIL off but peak scaling only ~{max_speedup:.1f}x "
            f"(efficiency {max_eff:.0%}). Investigate contention / shared state "
            "in the hot path before committing to A3."
        )
    else:
        verdict = "GIL is ON for this run; no parallel speedup expected."

    report = {
        "python_version": sys.version.split()[0],
        "free_threading_capable": capable,
        "gil_enabled_before_work": gil_before_work,
        "gil_enabled_after_imports": gil_after,
        "cpu_count": cpu,
        "calls": calls,
        "scaling": scaling,
        "max_speedup": round(max_speedup, 3),
        "peak_parallel_efficiency": round(max_eff, 3),
        "extensions": extensions,
        "gil_reenabling_extensions": blockers,
        "verdict": verdict,
    }

    if not quiet:
        _print_report(report)
    return report


def _print_report(r: Dict[str, Any]) -> None:
    print("=" * 70)
    print("DishtaYantra free-threading readiness spike")
    print("=" * 70)
    print(f"Python: {r['python_version']}  cpu_count={r['cpu_count']}")
    cap = "yes" if r["free_threading_capable"] else "no (standard build)"
    print(f"Free-threading capable: {cap}")
    g = r["gil_enabled_after_imports"]
    print(f"GIL enabled: {'n/a' if g is None else ('on' if g else 'OFF')}")
    print("\nCPU-bound calculator scaling (lower wall_s is better):")
    print(f"  {'threads':>7} {'wall_s':>9} {'speedup':>8} {'efficiency':>11}")
    for s in r["scaling"]:
        print(f"  {s['threads']:>7} {s['wall_s']:>9} {s['speedup']:>8} "
              f"{s['efficiency']*100:>10.0f}%")
    if r["extensions"]:
        print("\nKey native extensions:")
        for e in r["extensions"]:
            print(f"  {e['name']:<18} {e['status']:<16} {e['why']}")
    print("\nVERDICT:")
    print("  " + r["verdict"])


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Free-threading readiness spike")
    p.add_argument("--calls", type=int, default=60000,
                   help="total calculator-chain invocations to distribute")
    p.add_argument("--threads", type=str, default=None,
                   help="comma list of thread counts, e.g. 1,2,4,8")
    p.add_argument("--no-extensions", action="store_true",
                   help="skip the extension GIL-reenable inventory")
    p.add_argument("--output", type=str, default=None,
                   help="optional path to write the JSON report")
    args = p.parse_args(argv)

    tcs = None
    if args.threads:
        tcs = [int(x) for x in args.threads.split(",") if x.strip()]

    report = run_spike(
        calls=args.calls,
        thread_counts=tcs,
        check_extensions=not args.no_extensions,
        quiet=False,
    )

    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, "w") as fh:
            json.dump(report, fh, indent=2)
        print(f"\nwrote JSON -> {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
