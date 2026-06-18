"""
run_wasm_calculator_example.py - end-to-end demo of a sandboxed WASM calculator.

    python examples/wasm/run_wasm_calculator_example.py

Loads examples/wasm/calculators.wat, builds a WasmCalculator that computes
notional = price * quantity inside the wasm sandbox, and runs it over a few
trade records. Also shows the `fuel` CPU cap stopping a runaway module.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from core.calculator.wasm_calculator import WasmCalculator, wasmtime_available

MODULE = os.path.join(os.path.dirname(__file__), "calculators.wat")


def main():
    if not wasmtime_available():
        print("wasmtime not installed. Run: pip install wasmtime")
        return 1

    # notional = price * quantity, computed in the wasm sandbox
    calc = WasmCalculator("notional_calc", {
        "module_path": MODULE,
        "function": "notional",
        "input_fields": ["price", "quantity"],
        "output_field": "notional",
        "fuel": 100_000,            # CPU budget per call (sandbox safety)
    })

    trades = [
        {"symbol": "AAPL", "price": 190.25, "quantity": 100},
        {"symbol": "MSFT", "price": 410.10, "quantity": 50},
        {"symbol": "NVDA", "price": 1200.0, "quantity": 12},
    ]
    print("notional = price * quantity, computed inside WebAssembly:\n")
    for t in trades:
        out = calc.calculate(t)
        print(f"  {out['symbol']:5} {out['price']:>8.2f} x {out['quantity']:>4} "
              f"-> notional = {out['notional']:,.2f}")

    # Demonstrate the fuel cap: a big loop with a tiny budget traps.
    busy = WasmCalculator("busy", {
        "module_path": MODULE, "function": "busy_sum",
        "input_fields": ["n"], "output_field": "sum", "fuel": 500,
    })
    print("\nfuel cap demo (busy_sum with only 500 fuel):")
    try:
        busy.calculate({"n": 1_000_000})
        print("  (unexpected: did not trap)")
    except RuntimeError as exc:
        print(f"  trapped as expected -> {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
