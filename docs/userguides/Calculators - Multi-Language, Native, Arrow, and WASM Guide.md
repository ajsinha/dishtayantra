# Calculators: Multi-Language, Native, Arrow, and WASM

How to write and integrate calculators across all supported execution models: **Python, Java (py4j), C++ (pybind11), Rust (PyO3), native Arrow columnar, and WASM-sandboxed**.

> **Common configuration** (URI schemes, shared subscriber/publisher/node
> parameters, application.yaml) is in the **Configuration Reference Guide**. This
> guide covers connector-specific detail. Each connector is a section below.

---

# Multi-Language Calculator Integration Guide

## Applies to: current release
© 2025-2030 Ashutosh Sinha

---

## Overview

DishtaYantra provides support for calculators written in multiple programming languages, enabling you to leverage the strengths of each language for different use cases.

| Language | Technology | Performance | Best For |
|----------|------------|-------------|----------|
| Python | Built-in | Baseline (~10K ops/s) | Rapid development, prototyping |
| Java | Py4J | 10-100x faster | JVM ecosystem, enterprise libraries |
| C++ | pybind11 | 50-100x faster (~1M ops/s) | Ultra-low latency, SIMD, numeric |
| Rust | PyO3 | 50-100x faster (~1M ops/s) | Memory safety, thread safety |
| REST | HTTP/JSON | Network dependent | Microservices, third-party APIs |

---

## Quick Start

### Calculator Configuration Pattern

All calculator configurations follow a similar pattern in your DAG JSON:

```json
{
  "calculators": [
    {
      "name": "my_calculator",
      "type": "CalculatorClassName",
      "config": {
        "calculator": "java|cpp|rust|rest",
        ...additional_config...
      }
    }
  ]
}
```

---

## Java Calculator (Py4J)

### Prerequisites

```bash
# Install Py4J
pip install py4j

# Java 8+ required
java -version
```

### Setup

1. **Compile Java Calculator**

```java
// src/com/company/MyCalculator.java
package com.company;

import com.dishtayantra.calculators.AbstractCalculator;
import java.util.Map;
import java.util.HashMap;

public class MyCalculator extends AbstractCalculator {
    @Override
    public Map<String, Object> calculate(Map<String, Object> data) {
        Map<String, Object> result = new HashMap<>(data);
        // Your calculation logic here
        double value = ((Number) data.get("input")).doubleValue();
        result.put("output", value * 2);
        return result;
    }
}
```

2. **Start Java Gateway**

```bash
# Compile
javac -cp .:py4j.jar src/com/company/*.java

# Start gateway
java -cp .:py4j.jar:src com.dishtayantra.gateway.DishtaYantraGateway
```

3. **Configure DAG**

```json
{
  "calculators": [
    {
      "name": "java_calc",
      "type": "com.company.MyCalculator",
      "config": {
        "calculator": "java",
        "gateway_port": 25333,
        "pool_size": 4
      }
    }
  ]
}
```

### Advanced Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `gateway_host` | localhost | Java gateway host |
| `gateway_port` | 25333 | Java gateway port |
| `pool_size` | 4 | Connection pool size |
| `timeout` | 30 | Call timeout (seconds) |

### Performance Tips

- Use connection pooling for high throughput
- Keep data structures simple (primitives, lists, maps)
- Consider batch processing for small operations

---

## C++ Calculator (pybind11)

### Prerequisites

```bash
# Install pybind11
pip install pybind11

# CMake 3.12+
cmake --version

# C++ compiler with C++17 support
g++ --version  # or clang++
```

### Setup

1. **Write C++ Calculator**

```cpp
// cpp/src/my_calculator.cpp
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <map>
#include <string>
#include <any>

namespace py = pybind11;

class MyCalculator {
public:
    py::dict calculate(py::dict data) {
        py::dict result(data);
        double value = data["input"].cast<double>();
        result["output"] = value * 2;
        return result;
    }
    
    py::dict details() {
        py::dict info;
        info["name"] = "MyCalculator";
        info["language"] = "C++";
        return info;
    }
};

PYBIND11_MODULE(my_calculator, m) {
    py::class_<MyCalculator>(m, "MyCalculator")
        .def(py::init<>())
        .def("calculate", &MyCalculator::calculate)
        .def("details", &MyCalculator::details);
}
```

2. **Build Module**

```bash
cd cpp
mkdir build && cd build
cmake ..
make
cp my_calculator*.so ../../
```

3. **Configure DAG**

```json
{
  "calculators": [
    {
      "name": "cpp_calc",
      "type": "MyCalculator",
      "config": {
        "calculator": "cpp",
        "module": "my_calculator"
      }
    }
  ]
}
```

### SIMD Optimization Example

```cpp
#include <immintrin.h>  // AVX2

class SimdCalculator {
public:
    py::dict calculate(py::dict data) {
        py::list values = data["values"].cast<py::list>();
        std::vector<double> vec;
        for (auto& v : values) vec.push_back(v.cast<double>());
        
        // SIMD processing (AVX2)
        size_t n = vec.size();
        size_t simd_n = n - (n % 4);
        
        for (size_t i = 0; i < simd_n; i += 4) {
            __m256d v = _mm256_loadu_pd(&vec[i]);
            __m256d two = _mm256_set1_pd(2.0);
            v = _mm256_mul_pd(v, two);
            _mm256_storeu_pd(&vec[i], v);
        }
        
        // Handle remainder
        for (size_t i = simd_n; i < n; i++) {
            vec[i] *= 2.0;
        }
        
        py::dict result(data);
        result["values"] = vec;
        return result;
    }
};
```

### Performance Tips

- Use zero-copy with NumPy arrays via `py::array`
- Enable compiler optimizations (-O3, -march=native)
- Batch small operations to amortize call overhead

---

## Rust Calculator (PyO3)

### Prerequisites

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install maturin
pip install maturin
```

### Setup

1. **Create Rust Project**

```bash
mkdir rust_calc && cd rust_calc
maturin init --bindings pyo3
```

2. **Write Rust Calculator**

```rust
// src/lib.rs
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;

#[pyclass]
struct MyCalculator {
    name: String,
}

#[pymethods]
impl MyCalculator {
    #[new]
    fn new(name: String) -> Self {
        MyCalculator { name }
    }
    
    fn calculate(&self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        let result = PyDict::new(py);
        
        // Copy input data
        for (key, value) in data.iter() {
            result.set_item(key, value)?;
        }
        
        // Perform calculation
        if let Ok(value) = data.get_item("input")?.extract::<f64>() {
            result.set_item("output", value * 2.0)?;
        }
        
        Ok(result.into())
    }
    
    fn details(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("language", "Rust")?;
        Ok(dict.into())
    }
}

#[pymodule]
fn rust_calc(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<MyCalculator>()?;
    Ok(())
}
```

3. **Build Module**

```bash
maturin develop --release
```

4. **Configure DAG**

```json
{
  "calculators": [
    {
      "name": "rust_calc",
      "type": "MyCalculator",
      "config": {
        "calculator": "rust",
        "module": "rust_calc"
      }
    }
  ]
}
```

### Parallel Processing with Rayon

```rust
use rayon::prelude::*;

#[pymethods]
impl ParallelCalculator {
    fn calculate(&self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        let values: Vec<f64> = data.get_item("values")?.extract()?;
        
        // Parallel processing
        let results: Vec<f64> = values
            .par_iter()
            .map(|&x| x * 2.0)
            .collect();
        
        let result = PyDict::new(py);
        result.set_item("values", results)?;
        Ok(result.into())
    }
}
```

### Performance Tips

- Use Rayon for CPU-bound parallel processing
- Minimize Python ↔ Rust data conversions
- Consider buffer protocol for large arrays

---

## REST Calculator

### Setup

No compilation required - uses HTTP endpoints.

```json
{
  "calculators": [
    {
      "name": "rest_calc",
      "config": {
        "calculator": "rest",
        "endpoint": "https://api.example.com/calculate",
        "auth_type": "api_key",
        "api_key": "${API_KEY}",
        "timeout": 10,
        "retries": 3
      }
    }
  ]
}
```

### Authentication Types

#### API Key
```json
{
  "auth_type": "api_key",
  "api_key": "${API_KEY}",
  "api_key_header": "X-API-Key"
}
```

#### Basic Auth
```json
{
  "auth_type": "basic",
  "username": "${USERNAME}",
  "password": "${PASSWORD}"
}
```

#### Bearer Token (OAuth2)
```json
{
  "auth_type": "bearer",
  "bearer_token": "${JWT_TOKEN}"
}
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `endpoint` | (required) | Full URL |
| `auth_type` | none | api_key, basic, bearer |
| `timeout` | 30 | Request timeout (seconds) |
| `retries` | 3 | Retry attempts |
| `retry_backoff` | 0.5 | Backoff multiplier |
| `verify_ssl` | true | SSL verification |
| `response_path` | null | JSONPath to extract result |
| `fallback_value` | null | Value on error |

### Example Server Implementation (Python/Flask)

```python
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/calculate', methods=['POST'])
def calculate():
    api_key = request.headers.get('X-API-Key')
    if api_key != 'your-secret-key':
        return jsonify({'error': 'Unauthorized'}), 401
    
    data = request.json
    result = data.copy()
    result['output'] = data.get('input', 0) * 2
    return jsonify(result)
```

---

## Choosing the Right Calculator Type

### Decision Matrix

| Requirement | Recommended |
|-------------|-------------|
| Rapid prototyping | Python |
| JVM library access | Java |
| Ultra-low latency | C++ |
| Memory safety critical | Rust |
| External API integration | REST |
| Numeric/SIMD processing | C++ or Rust |
| Thread-safe parallel | Rust |
| Enterprise ecosystem | Java |

### Performance Comparison

```
Latency (per call):
┌─────────────────────────────────────────────────────────┐
│ Python    │████████████████████████████████│ ~100μs    │
│ Java/Py4J │██████████████████░░░░░░░░░░░░░░│ ~200μs    │
│ C++       │█░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░│ ~100ns    │
│ Rust      │█░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░│ ~100ns    │
│ REST      │████████████████████████████████████████│ 10-1000ms │
└─────────────────────────────────────────────────────────┘
```

---

## Troubleshooting

### Java Calculator Issues

| Issue | Solution |
|-------|----------|
| Gateway not found | Start Java gateway first |
| Class not found | Check classpath and compilation |
| Timeout | Increase timeout config |
| Pool exhausted | Increase pool_size |

### C++ Calculator Issues

| Issue | Solution |
|-------|----------|
| Module not found | Check .so file location |
| Symbol errors | Rebuild with correct Python version |
| Segmentation fault | Debug with gdb, check memory |

### Rust Calculator Issues

| Issue | Solution |
|-------|----------|
| Import error | Run `maturin develop` |
| Type mismatch | Check PyO3 type conversions |
| Panic | Add error handling |

### REST Calculator Issues

| Issue | Solution |
|-------|----------|
| Connection refused | Verify endpoint URL |
| 401/403 | Check authentication |
| Timeout | Increase timeout, check API |
| SSL error | Verify certificate or disable verification |

---

## Best Practices

1. **Start with Python** for prototyping, then optimize with other languages
2. **Use connection pooling** for Java calculators
3. **Enable compiler optimizations** for C++ (-O3)
4. **Use Rayon** for parallel Rust processing
5. **Set appropriate timeouts** for REST calculators
6. **Use environment variables** for credentials
7. **Monitor performance** with calculator statistics
8. **Implement fallbacks** for REST calculators

---

## See Also

- [Help: Py4J Java Integration](/help/py4j-integration)
- [Help: pybind11 C++ Integration](/help/pybind11-integration)
- [Help: Rust PyO3 Integration](/help/rust-integration)
- [Help: REST API Integration](/help/rest-integration)

---

## Legal Information

### Copyright Notice

© 2025-2030 Ashutosh Sinha. All rights reserved.

### Trademark Notice

DishtaYantra™ is a trademark of Ashutosh Sinha.

---

**DishtaYantra v2.2** | © 2025-2030 Ashutosh Sinha


---

# Native Arrow Calculators (Zero-Copy C Data Interface) Guide

DishtaYantra can hand an Apache Arrow column directly to a **native** (compiled)
calculator with no copy and no serialization, using the standard Arrow **C Data
Interface**. This is the foundation for "polyglot at native speed": the same
mechanism works for C, C++, Rust, and Java/JNI kernels.

## The idea

A pyarrow column is exported into the two standard ABI structs (`ArrowSchema`,
`ArrowArray`) — this transfers pointers, not data. A native kernel reads the
column's buffers **in place** and produces a result, which is read back into a
pyarrow array. Because the C Data Interface is a tiny, dependency-free ABI (it
does not require the Arrow C++ library), the exact same structs are what a C++,
Rust, or JVM consumer would use. The bundled kernel is written in C and stands in
for any of them.

## Using the native calculator

`NativeAffineCalculator` computes `target_col = source_col * scale + offset` over
a RecordBatch. It is configured like any calculator (by name + config dict):

```python
from core.calculator.native_arrow import NativeAffineCalculator

calc = NativeAffineCalculator("fx", {
    "source_col": "price",     # required
    "target_col": "price_eur", # required
    "scale": 0.908,            # default 1.0
    "offset": 0.0,             # default 0.0
    "prefer_native": True,     # default True; False forces the pyarrow path
})
out_batch = calc.calculate_batch(in_batch)   # adds 'price_eur' as float64
```

`calc.used_native` tells you whether the native kernel is actually in use.

## Graceful fallback (nothing breaks)

The native path is **opt-in and safe**:

- On first use, the kernel source (`core/cpp/arrow_cdata.c`) is compiled with the
  system C compiler into a cache directory. The `.c` source ships; the compiled
  library is built locally and never packaged.
- If no C compiler is available, the build fails, or pyarrow lacks the C
  interface, the bridge reports `available = False` and the calculator
  transparently uses an **identical pyarrow implementation**. Results are the
  same either way; only the speed differs.

This means deployments without a compiler keep working unchanged.

## Supported types

The kernel reads Arrow primitive columns: `int32`, `int64`, `float32`, `float64`,
and writes `float64` output. Unsupported column types raise a clear error on the
native path (the pyarrow fallback follows pyarrow's own casting rules). Null
handling is left to the caller — mask afterwards if your data contains nulls.

## Verifying correctness

The native and fallback paths are checked for byte-parity in
`tests/test_native_arrow.py` across all supported dtypes, and a zero-copy sum
kernel proves the C side reads the exported buffers in place. When you add a new
native kernel, add the same parity assertions so the native result can never
silently diverge from the reference pyarrow result.

## Writing your own native kernel

`core/cpp/arrow_cdata.c` is the template: it defines the ABI structs, then reads
`buffers[1]` (the data buffer) directly, honoring `offset` and `length`. To add a
kernel, write a new C function with the same export/import pattern in
`core/calculator/native_arrow.py`, and gate it behind the same availability check
so the pyarrow fallback always exists.

## Where this fits

This is the first increment of roadmap item A1 ("zero-copy polyglot handoff via
the Arrow C Data Interface"). It builds on the Arrow RecordBatch-on-edges
transport; together they set up zero-copy hand-off to native calculators in any
language. See `docs/design/A1-arrow-data-plane.md` for the broader design.


---

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


---

