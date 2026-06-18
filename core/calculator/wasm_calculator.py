"""
core/calculator/wasm_calculator.py - run a calculator compiled to WebAssembly
inside a sandboxed wasmtime runtime (roadmap C5/C2).

A WASM calculator is logic written in *any* language (Rust, C, AssemblyScript,
TinyGo, ... or hand-written WAT), compiled to a ``.wasm`` module, and executed in
a memory-isolated VM embedded in the host. The module cannot read host memory,
files, or the network, and its CPU budget can be capped (fuel) - so untrusted or
third-party calculators run safely, and a runaway one is stopped deterministically.

What the host needs at runtime:
  1. the ``wasmtime`` package (``pip install wasmtime``) - it bundles the engine,
     so NO C/Rust compiler is needed on the DishtaYantra host;
  2. a ``.wasm`` (or ``.wat``) module exporting the function this calculator calls;
  3. the calculator config below pointing at the module.

Design / backward compatibility:
- ``wasmtime`` is imported lazily and is entirely optional. A deployment that
  never uses a WASM calculator needs nothing new and behaves exactly as before.
- If a WASM calculator is configured but the runtime is missing, construction
  fails loud with a clear message (a WASM calculator has no pure-Python
  equivalent to fall back to, unlike the native-Arrow path).
- This version passes f64 scalars across the boundary (one or more input fields
  in, one result field out). Batch/columnar (Arrow) handoff over linear memory is
  the natural next step and is described in the user guide.

Reference from a DAG by dotted path:
  "type": "core.calculator.wasm_calculator.WasmCalculator"
"""

import os
import threading
from datetime import datetime

from core.calculator.core_calculator import DataCalculator


def wasmtime_available() -> bool:
    """True if the wasmtime runtime can be imported."""
    try:
        import wasmtime  # noqa: F401
        return True
    except Exception:
        return False


class WasmRuntimeUnavailable(RuntimeError):
    """Raised when a WASM calculator is configured but wasmtime is not installed."""


class WasmCalculator(DataCalculator):
    """Run an exported WebAssembly function as a calculator.

    Config keys:
      module_path   (required) path to a .wasm or .wat module.
      function      (required) name of the exported function to call.
      input_fields  (required) record field names passed as f64 args, in order.
      output_field  (required) record field to receive the f64 result.
      fuel          (optional) instruction budget per call; caps CPU and stops a
                    runaway module deterministically. Omit for unlimited.
      max_memory_pages (optional) cap on the module's linear memory (64KiB pages).
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        # Required config - fail loud, no silent defaulting.
        self.module_path = config["module_path"]
        self.function = config["function"]
        self.input_fields = list(config["input_fields"])
        self.output_field = config["output_field"]
        if not self.input_fields:
            raise ValueError(f"WasmCalculator '{name}': input_fields must be non-empty")
        self.fuel = config.get("fuel")
        self.max_memory_pages = config.get("max_memory_pages")

        if not wasmtime_available():
            raise WasmRuntimeUnavailable(
                f"WasmCalculator '{name}' needs the wasmtime runtime. "
                f"Install it with: pip install wasmtime")
        if not os.path.exists(self.module_path):
            raise FileNotFoundError(
                f"WasmCalculator '{name}': module not found: {self.module_path}")

        import wasmtime
        cfg = wasmtime.Config()
        if self.fuel is not None:
            cfg.consume_fuel = True
        self._engine = wasmtime.Engine(cfg)
        # Compile once; .wat (text) and .wasm (bytes) both load via from_file.
        self._module = wasmtime.Module.from_file(self._engine, self.module_path)
        self._lock = threading.Lock()
        # A fresh store/instance is created per call for clean isolation and
        # fuel accounting (the compute loop is single-threaded per DAG; the lock
        # is belt-and-suspenders in case an instance is shared).
        self._wasmtime = wasmtime

    @property
    def available(self) -> bool:
        return True  # constructed only when the runtime is present

    def calculate(self, data):
        if not isinstance(data, dict):
            return data
        # Extract and validate inputs (fail loud on missing/non-numeric).
        args = []
        for field in self.input_fields:
            if field not in data:
                raise KeyError(
                    f"WasmCalculator '{self.name}': missing input field '{field}'")
            try:
                args.append(float(data[field]))
            except (TypeError, ValueError):
                raise ValueError(
                    f"WasmCalculator '{self.name}': field '{field}' is not numeric")

        wasmtime = self._wasmtime
        with self._lock:
            store = wasmtime.Store(self._engine)
            if self.fuel is not None:
                store.set_fuel(int(self.fuel))
            instance = wasmtime.Instance(store, self._module, [])
            exports = instance.exports(store)
            if self.function not in exports:
                raise KeyError(
                    f"WasmCalculator '{self.name}': module has no export "
                    f"'{self.function}'")
            try:
                result = exports[self.function](store, *args)
            except Exception as exc:
                # Trap (e.g. fuel exhausted, out-of-bounds) - surface clearly,
                # do not swallow.
                raise RuntimeError(
                    f"WasmCalculator '{self.name}': trap in '{self.function}' "
                    f"({exc})") from exc

        out = dict(data)
        out[self.output_field] = result
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()
        return out
