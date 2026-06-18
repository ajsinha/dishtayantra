"""
run_wasm_example.py - run the perftest_wasm DAG over the trade stream.

Loads perftest/perftest_wasm.json, feeds it the canonical perftest trade stream,
and reads back each trade with a 'notional' field computed INSIDE a sandboxed
WebAssembly module (examples/wasm/calculators.wat). Demonstrates that a compiled,
memory-isolated calculator plugs into the engine exactly like any other.

    python -m perftest.run_wasm_example [--trades N]

Requires 'pip install wasmtime'. Without it the DAG still builds (WasmCalculator
imports wasmtime lazily) but calculation raises a clear error; this script detects
that up front and prints install instructions instead of failing noisily.
"""

import argparse
import json
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from perftest.generate_trades import make_trade  # noqa: E402

_HERE = os.path.dirname(os.path.abspath(__file__))
_DAG = os.path.join(_HERE, "perftest_wasm.json")


def run_example(trades: int = 2000, quiet: bool = True):
    from core.calculator.wasm_calculator import wasmtime_available
    if not wasmtime_available():
        print("wasmtime is not installed - the WASM sandbox can't run.")
        print("Install it with:  pip install wasmtime")
        print("(The DAG itself is valid; only the runtime is missing.)")
        return None

    if quiet:
        logging.disable(logging.INFO)

    import random
    from core.dag.compute_graph import ComputeGraph
    from core.pubsub.inmemorypubsub import InMemoryPubSub

    cfg = json.load(open(_DAG))
    # module_path in the JSON is repo-relative; resolve it for the running process.
    cfg["calculators"][0]["config"]["module_path"] = os.path.join(
        os.path.dirname(_HERE), "examples", "wasm", "calculators.wat")
    in_q = cfg["subscribers"][0]["config"]["source"].rsplit("/", 1)[-1]
    out_q = cfg["publishers"][0]["config"]["destination"].rsplit("/", 1)[-1]

    rng = random.Random(20260617)
    data = [make_trade(i, rng, anomaly_pct=0.0) for i in range(trades)]

    g = ComputeGraph(cfg); g.start(); time.sleep(0.5)
    ps = InMemoryPubSub()
    for t in data:
        ps.publish_to_queue(in_q, json.dumps(t))

    by_id, deadline = {}, time.time() + 60.0
    while len(by_id) < len(data) and time.time() < deadline:
        m = ps.consume_from_queue(out_q)
        if m is None:
            time.sleep(0.002); continue
        if isinstance(m, (bytes, bytearray)):
            m = m.decode("utf-8")
        d = json.loads(m) if isinstance(m, str) else m
        if isinstance(d, dict) and "trade_id" in d:
            by_id[d["trade_id"]] = d
    try:
        g.stop()
    except Exception:
        pass

    # Verify the wasm-computed notional matches price*quantity for a sample.
    ok = sum(1 for d in by_id.values()
             if abs(d.get("notional", 0) - d["price"] * d["quantity"]) < 1e-6)
    report = {"trades": trades, "delivered": len(by_id), "notional_correct": ok}
    if not quiet or True:
        print(f"WASM example: delivered {report['delivered']}/{trades}, "
              f"notional correct on {ok}/{len(by_id)} (computed in the wasm sandbox)")
    return report


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--trades", type=int, default=2000)
    args = p.parse_args()
    run_example(args.trades, quiet=True)
