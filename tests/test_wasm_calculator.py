"""Tests for core.calculator.wasm_calculator.

Skipped automatically where the optional wasmtime runtime is not installed, so
the suite stays green either way.
"""

import os

import pytest

pytest.importorskip("wasmtime")  # optional dependency

from core.calculator.wasm_calculator import (WasmCalculator,  # noqa: E402
                                             wasmtime_available)

MODULE = os.path.join(os.path.dirname(__file__), os.pardir,
                      "examples", "wasm", "calculators.wat")


def _calc(function, input_fields, output_field, **extra):
    cfg = {"module_path": MODULE, "function": function,
           "input_fields": input_fields, "output_field": output_field}
    cfg.update(extra)
    return WasmCalculator("c", cfg)


def test_runtime_available():
    assert wasmtime_available() is True


def test_notional_in_sandbox():
    calc = _calc("notional", ["price", "quantity"], "notional")
    out = calc.calculate({"symbol": "AAPL", "price": 190.25, "quantity": 100})
    assert out["notional"] == pytest.approx(19025.0)
    assert out["symbol"] == "AAPL"   # original fields preserved
    assert calc._calculation_count == 1


def test_affine_three_args():
    calc = _calc("affine", ["x", "scale", "offset"], "y")
    assert calc.calculate({"x": 10.0, "scale": 2.0, "offset": 1.0})["y"] == pytest.approx(21.0)


def test_fuel_cap_traps_runaway():
    calc = _calc("busy_sum", ["n"], "sum", fuel=500)
    with pytest.raises(RuntimeError):
        calc.calculate({"n": 1_000_000})


def test_fuel_enough_succeeds():
    calc = _calc("busy_sum", ["n"], "sum", fuel=100_000)
    assert calc.calculate({"n": 10})["sum"] == pytest.approx(55.0)


def test_missing_required_config_fails_loud():
    with pytest.raises(KeyError):
        WasmCalculator("c", {"module_path": MODULE, "function": "notional",
                             "output_field": "n"})  # no input_fields


def test_empty_input_fields_rejected():
    with pytest.raises(ValueError):
        _calc("notional", [], "n")


def test_missing_module_file():
    with pytest.raises(FileNotFoundError):
        WasmCalculator("c", {"module_path": "/no/such.wasm", "function": "f",
                             "input_fields": ["x"], "output_field": "y"})


def test_missing_input_field_at_runtime():
    calc = _calc("notional", ["price", "quantity"], "notional")
    with pytest.raises(KeyError):
        calc.calculate({"price": 1.0})  # quantity absent


def test_non_numeric_field_rejected():
    calc = _calc("notional", ["price", "quantity"], "notional")
    with pytest.raises(ValueError):
        calc.calculate({"price": "abc", "quantity": 100})


def test_unknown_export_rejected():
    calc = _calc("does_not_exist", ["x"], "y")
    with pytest.raises(KeyError):
        calc.calculate({"x": 1.0})
