"""
core/calculator/native_arrow.py - Zero-copy polyglot calculator handoff (A1).

Hands an Apache Arrow column to a native (C) kernel via the Arrow C Data
Interface - no copy, no serialization - and reads the result back. The C kernel
(``core/cpp/arrow_cdata.c``) uses only the standard ABI structs, which are
exactly what a C++, Rust, or Java/JNI calculator would use, so this is the
template for native calculators in any language.

Design / safety:
- The shared library is compiled on first use with the system C compiler into a
  cache directory (keyed by source hash); the .c source ships, the .so does not.
- Everything is OPT-IN and degrades gracefully: if no compiler is available, the
  build fails, or pyarrow lacks the C interface, ``NativeArrowBridge.available``
  is False and callers fall back to an identical pyarrow implementation. No
  existing code path is affected.
"""

import ctypes
import hashlib
import logging
import os
import subprocess
import tempfile
import threading

logger = logging.getLogger(__name__)

_C_SOURCE = os.path.join(os.path.dirname(__file__), os.pardir, "cpp", "arrow_cdata.c")

_lock = threading.Lock()
_bridge_singleton = None


def _build_native_lib():
    """Compile arrow_cdata.c -> shared lib (cached by source hash). Returns the
    loaded ctypes.CDLL, or None if anything is unavailable/fails."""
    src = os.path.abspath(_C_SOURCE)
    if not os.path.exists(src):
        logger.info("native_arrow: C source not found (%s); using pyarrow fallback", src)
        return None

    compiler = os.environ.get("CC", "cc")
    if not _which(compiler):
        logger.info("native_arrow: no C compiler (%s); using pyarrow fallback", compiler)
        return None

    try:
        with open(src, "rb") as fh:
            digest = hashlib.sha256(fh.read()).hexdigest()[:16]
        cache_dir = os.path.join(tempfile.gettempdir(), "dy_native")
        os.makedirs(cache_dir, exist_ok=True)
        lib_path = os.path.join(cache_dir, f"arrow_cdata_{digest}.so")
        if not os.path.exists(lib_path):
            cmd = [compiler, "-O2", "-shared", "-fPIC", "-o", lib_path, src]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if proc.returncode != 0:
                logger.warning("native_arrow: compile failed (%s); using pyarrow fallback: %s",
                               compiler, proc.stderr.strip()[:300])
                return None
        lib = ctypes.CDLL(lib_path)
        lib.dy_abi_version.restype = ctypes.c_int
        if lib.dy_abi_version() != 1:
            logger.warning("native_arrow: unexpected ABI version; using pyarrow fallback")
            return None
        lib.dy_affine_to_f64.restype = ctypes.c_int
        lib.dy_affine_to_f64.argtypes = [
            ctypes.c_void_p, ctypes.c_void_p, ctypes.c_double, ctypes.c_double,
            ctypes.POINTER(ctypes.c_double), ctypes.c_int64,
        ]
        lib.dy_sum_i64.restype = ctypes.c_longlong
        lib.dy_sum_i64.argtypes = [ctypes.c_void_p]
        logger.info("native_arrow: loaded native Arrow C Data Interface kernel (%s)", lib_path)
        return lib
    except Exception as exc:  # pragma: no cover - environment dependent
        logger.warning("native_arrow: load failed (%s); using pyarrow fallback", exc)
        return None


def _which(name):
    for d in os.environ.get("PATH", "").split(os.pathsep):
        candidate = os.path.join(d, name)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


class NativeArrowBridge:
    """Exports a pyarrow Array across the C Data Interface to native kernels."""

    def __init__(self):
        self._lib = _build_native_lib()

    @property
    def available(self) -> bool:
        return self._lib is not None

    def affine(self, array, scale: float, offset: float):
        """Return a float64 pyarrow Array = array * scale + offset, computed by
        the native kernel reading the input zero-copy. Raises if unavailable."""
        if self._lib is None:
            raise RuntimeError("native bridge unavailable")
        import numpy as np
        import pyarrow as pa
        from pyarrow.cffi import ffi

        n = len(array)
        out = np.empty(n, dtype=np.float64)
        c_arr = ffi.new("struct ArrowArray*")
        c_sch = ffi.new("struct ArrowSchema*")
        p_arr = int(ffi.cast("uintptr_t", c_arr))
        p_sch = int(ffi.cast("uintptr_t", c_sch))
        array._export_to_c(p_arr, p_sch)
        try:
            rc = self._lib.dy_affine_to_f64(
                p_arr, p_sch, ctypes.c_double(scale), ctypes.c_double(offset),
                out.ctypes.data_as(ctypes.POINTER(ctypes.c_double)), ctypes.c_int64(n))
        finally:
            if c_arr.release != ffi.NULL:
                c_arr.release(c_arr)
            if c_sch.release != ffi.NULL:
                c_sch.release(c_sch)
        if rc != 0:
            raise RuntimeError(f"native affine failed (code {rc}; unsupported dtype?)")
        return pa.array(out)

    def sum_i64(self, array) -> int:
        """Zero-copy sum of an int64 array via the native kernel."""
        if self._lib is None:
            raise RuntimeError("native bridge unavailable")
        from pyarrow.cffi import ffi
        c_arr = ffi.new("struct ArrowArray*")
        c_sch = ffi.new("struct ArrowSchema*")
        p_arr = int(ffi.cast("uintptr_t", c_arr))
        p_sch = int(ffi.cast("uintptr_t", c_sch))
        array._export_to_c(p_arr, p_sch)
        try:
            return int(self._lib.dy_sum_i64(p_arr))
        finally:
            if c_arr.release != ffi.NULL:
                c_arr.release(c_arr)
            if c_sch.release != ffi.NULL:
                c_sch.release(c_sch)


def get_bridge() -> NativeArrowBridge:
    """Process-wide singleton (compiles/loads the native lib at most once)."""
    global _bridge_singleton
    if _bridge_singleton is None:
        with _lock:
            if _bridge_singleton is None:
                _bridge_singleton = NativeArrowBridge()
    return _bridge_singleton


# --- Optional native calculator (only import the base class if pyarrow exists) -
try:
    from core.calculator.arrow_calculator import ArrowCalculator, append_columns

    class NativeAffineCalculator(ArrowCalculator):
        """Computes ``target_col = source_col * scale + offset`` over a RecordBatch.

        Uses the native zero-copy kernel when available, otherwise an identical
        pyarrow implementation. Opt-in; does not change any existing calculator.
        """

        def __init__(self, name, config):
            super().__init__(name, config)
            # Required keys fail loudly (no silent defaulting); scale/offset
            # default to the identity transform.
            self.source_col = config["source_col"]
            self.target_col = config["target_col"]
            self.scale = float(config.get("scale", 1.0))
            self.offset = float(config.get("offset", 0.0))
            self.prefer_native = bool(config.get("prefer_native", True))
            self._bridge = get_bridge() if self.prefer_native else None

        @property
        def used_native(self) -> bool:
            return bool(self._bridge and self._bridge.available)

        def calculate_batch(self, batch):
            import pyarrow as pa
            import pyarrow.compute as pc
            column = batch.column(self.source_col)
            if self.used_native:
                result = self._bridge.affine(column, self.scale, self.offset)
            else:
                result = pc.add(pc.multiply(pc.cast(column, pa.float64()), self.scale),
                                self.offset)
            return append_columns(batch, {self.target_col: result})

except Exception:  # pragma: no cover - pyarrow missing
    NativeAffineCalculator = None
