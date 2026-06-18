# WebAssembly (WASM) Sandboxed Calculators Guide

A WASM calculator is logic written in **any** language (Rust, C, AssemblyScript,
TinyGo, or hand-written WAT), compiled to a `.wasm` module, and executed inside a
memory-isolated WebAssembly runtime embedded in DishtaYantra. The module **cannot**
read host memory, files, or the network, and its CPU budget can be **capped**, so
you can run untrusted or third-party calculators safely and stop a runaway one
deterministically. One runtime runs every language, so a WASM calculator avoids
the per-language build steps (and the "Not Compiled" status) of the native
pybind11 / PyO3 bridges.

> **Status:** opt-in and additive. A deployment that never uses a WASM calculator
> needs nothing new and behaves exactly as before.

---

## 1. What DishtaYantra needs to run a WASM calculator

Three things, and only the first two live on the server:

1. **The wasmtime runtime** — `pip install wasmtime`. It bundles the WebAssembly
   engine, so there is **no C/C++/Rust compiler required on the DishtaYantra
   host**. wasmtime is an optional dependency; install it only if you use WASM
   calculators.
2. **A compiled module** — a `.wasm` file (or a `.wat` text module) that *exports*
   the function this calculator will call.
3. **The calculator config** (below) pointing at the module and mapping record
   fields to the function's arguments.

Note the split: a *compiler that targets WebAssembly* is needed only at
**authoring** time, on whoever builds the module — never on the production host.
The host just loads the portable `.wasm` bytes. That is the operational win: build
once, run anywhere the runtime runs, including air-gapped boxes.

If a WASM calculator is configured but `wasmtime` is not installed, construction
fails immediately with a clear message (a WASM calculator has no pure-Python
equivalent to fall back to).

---

## 2. Quick start (worked example)

An example module ships at `examples/wasm/calculators.wat`. It exports
`notional(price, qty) -> price*qty` (among others). Build a calculator that uses
it:

```python
from core.calculator.wasm_calculator import WasmCalculator

calc = WasmCalculator("notional_calc", {
    "module_path": "examples/wasm/calculators.wat",
    "function": "notional",
    "input_fields": ["price", "quantity"],   # mapped to the wasm args, in order
    "output_field": "notional",              # result lands here
    "fuel": 100_000,                         # CPU budget per call (optional)
})

calc.calculate({"symbol": "AAPL", "price": 190.25, "quantity": 100})
# -> {"symbol": "AAPL", "price": 190.25, "quantity": 100, "notional": 19025.0}
```

Run the full demo (computes notional for several trades and shows the CPU cap
trapping a runaway loop):

```bash
python examples/wasm/run_wasm_calculator_example.py
```

---

## 3. Configuration reference

| Key | Required | Meaning |
|-----|----------|---------|
| `module_path` | yes | Path to a `.wasm` or `.wat` module. |
| `function` | yes | Name of the exported function to call. |
| `input_fields` | yes | Record field names passed as `f64` arguments, **in order**. |
| `output_field` | yes | Record field that receives the `f64` result. |
| `fuel` | no | Instruction budget per call. Caps CPU and stops a runaway module with a clear trap. Omit for unlimited. |
| `max_memory_pages` | no | Cap on the module's linear memory (64 KiB pages). |

Missing required keys, a missing module file, a missing input field at runtime, a
non-numeric field, or an unknown export all raise a clear error rather than
failing silently.

## 4. Using it in a DAG

Reference the calculator by dotted path in your DAG config, like any other
calculator:

```json
{
  "name": "fx",
  "type": "core.calculator.wasm_calculator.WasmCalculator",
  "config": {
    "module_path": "examples/wasm/calculators.wat",
    "function": "affine",
    "input_fields": ["x", "scale", "offset"],
    "output_field": "y"
  }
}
```

## 5. Authoring a module

You can hand-write WAT (no toolchain needed — wasmtime compiles it), which is what
the shipped example does:

```wat
(module
  (func (export "notional") (param $price f64) (param $qty f64) (result f64)
    local.get $price local.get $qty f64.mul))
```

More realistically you compile from a high-level language to a `wasm32` target,
producing a `.wasm` file the host loads identically:

- **Rust** — `rustup target add wasm32-wasip1`, then `cargo build --target wasm32-wasip1 --release`.
- **C / C++** — clang with the WASI SDK (`--target=wasm32-wasi`).
- **AssemblyScript** — `asc calc.ts -o calc.wasm`.
- **TinyGo** — `tinygo build -target=wasi -o calc.wasm`.

Whatever the source language, the module just needs to **export** the function(s)
your calculator config names, taking and returning `f64`.

## 6. The data boundary (and where it's going)

This version uses the **scalar boundary**: each named `input_field` is passed as an
`f64` argument and the `f64` result is written to `output_field`. That is simple,
fast, and ideal for numeric per-record transforms (prices, conversions, scoring).

Richer handoff — passing whole columnar **Arrow batches** or strings over the
module's linear memory — is the natural next step and is tracked with the Arrow
data-plane work (roadmap A1) and the dynamic-topology design (C5). Doing the
handoff at *batch* granularity is also what keeps WASM fast: per-single-row calls
are dominated by the cost of crossing the host↔module boundary, so batch-at-a-time
is the intended high-throughput path.

## 7. Sandbox and resource limits

- **Isolation.** The module runs in its own linear memory and cannot touch host
  memory, the filesystem, or the network. This example needs no host capabilities
  at all (pure compute).
- **CPU cap (`fuel`).** With `fuel` set, the module gets a fixed instruction
  budget per call; exceeding it raises a clear trap (`all fuel consumed`) instead
  of hanging the server. This is the key to safely running untrusted calculators
  on a shared single node.
- **Memory cap (`max_memory_pages`).** Bounds how much linear memory the module
  may grow.

## 8. Backward compatibility

- `wasmtime` is imported lazily and is entirely optional; nothing changes for
  deployments that don't use WASM calculators, and the dependency is not pulled in
  by default.
- The calculator is referenced by dotted path; no engine or factory change is
  needed, and existing DAGs are unaffected.

## 9. Limitations (current version)

- Scalar `f64` in/out only (see §6 for the batch/Arrow direction).
- Requires the optional `wasmtime` runtime; no fallback if it's absent (by design).
- Modules needing WASI (files, clock, args) are not wired up here — the supported
  shape is pure compute over the passed scalars.
- Debugging a `.wasm` module is harder than native Python; develop and test the
  logic in its source language first, then ship the compiled module.

This is the first concrete step of roadmap item **C2** (WASM sandboxed polyglot
calculators), and it pairs with **A1** (Arrow zero-copy) for the future
batch-granularity data path.
