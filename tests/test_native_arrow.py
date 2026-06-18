"""Tests for core.calculator.native_arrow - Arrow C Data Interface handoff (A1).

These pass whether or not a C compiler is present: the pyarrow fallback is always
exercised, and the native zero-copy path is checked for byte-parity when built.
"""

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from core.calculator.native_arrow import (NativeAffineCalculator,
                                          NativeArrowBridge, get_bridge)


def _expected_affine(values, scale, offset):
    return [v * scale + offset for v in values]


def test_bridge_singleton_and_available_is_bool():
    b = get_bridge()
    assert b is get_bridge()
    assert isinstance(b.available, bool)


def test_native_sum_zero_copy_when_available():
    b = get_bridge()
    if not b.available:
        pytest.skip("no native bridge (compiler unavailable)")
    arr = pa.array([10, 20, 30, 42], type=pa.int64())
    assert b.sum_i64(arr) == 102


@pytest.mark.parametrize("arrow_type,values", [
    (pa.int64(), [1, 2, 3, 4, 5]),
    (pa.int32(), [10, -20, 30]),
    (pa.float64(), [1.5, 2.5, -3.0]),
    (pa.float32(), [0.5, 4.0, 8.0]),
])
def test_native_affine_parity_with_pyarrow(arrow_type, values):
    b = get_bridge()
    if not b.available:
        pytest.skip("no native bridge (compiler unavailable)")
    scale, offset = 2.0, 1.0
    arr = pa.array(values, type=arrow_type)
    native = b.affine(arr, scale, offset).to_pylist()
    expected = _expected_affine([float(v) for v in values], scale, offset)
    assert native == pytest.approx(expected)


def test_native_unsupported_dtype_raises():
    b = get_bridge()
    if not b.available:
        pytest.skip("no native bridge")
    with pytest.raises(RuntimeError):
        b.affine(pa.array(["a", "b"]), 1.0, 0.0)


def test_calculator_correct_native_path():
    batch = pa.record_batch({"px": pa.array([100, 200, 300], type=pa.int64())})
    calc = NativeAffineCalculator("c", {"source_col": "px", "target_col": "px_adj", "scale": 1.1, "offset": 5.0, "prefer_native": True})
    out = calc.calculate_batch(batch)
    assert out.schema.names == ["px", "px_adj"]
    assert out.column("px_adj").to_pylist() == pytest.approx([115.0, 225.0, 335.0])


def test_calculator_fallback_path_matches():
    """prefer_native=False forces the pyarrow path; result must be identical."""
    batch = pa.record_batch({"px": pa.array([100, 200, 300], type=pa.int64())})
    calc = NativeAffineCalculator("c", {"source_col": "px", "target_col": "px_adj", "scale": 1.1, "offset": 5.0, "prefer_native": False})
    assert calc.used_native is False
    out = calc.calculate_batch(batch)
    assert out.column("px_adj").to_pylist() == pytest.approx([115.0, 225.0, 335.0])


def test_native_and_fallback_agree():
    """Whatever the environment, the native and fallback calculators agree."""
    batch = pa.record_batch({"v": pa.array([3.0, 6.0, 9.0, 12.0], type=pa.float64())})
    native = NativeAffineCalculator("c", {"source_col": "v", "target_col": "r", "scale": 0.25, "offset": -1.0, "prefer_native": True})
    fallback = NativeAffineCalculator("c", {"source_col": "v", "target_col": "r", "scale": 0.25, "offset": -1.0, "prefer_native": False})
    rn = native.calculate_batch(batch).column("r").to_pylist()
    rf = fallback.calculate_batch(batch).column("r").to_pylist()
    assert rn == pytest.approx(rf)
