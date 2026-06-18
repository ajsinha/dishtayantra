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
