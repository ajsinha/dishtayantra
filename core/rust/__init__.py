"""
DishtaYantra Rust Integration Module
Version: 1.7.0

Provides centralized management of Rust PyO3 modules and calculators.

Usage:
    from core.rust import get_rust_manager, create_rust_calculator, is_rust_available
    
    # Check if Rust is available
    if is_rust_available():
        # Create a calculator
        calc = create_rust_calculator("RustMathCalculator", "my_calc", {"operation": "sum"})
        result = calc.calculate({"values": [1.0, 2.0, 3.0]})

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

from .rust_manager import (
    RustManager,
    get_rust_manager,
    initialize_rust_manager,
    create_rust_calculator,
    is_rust_available,
    ModuleStatus,
    CalculatorDefinition,
    ModuleConfig
)

__all__ = [
    'RustManager',
    'get_rust_manager',
    'initialize_rust_manager',
    'create_rust_calculator',
    'is_rust_available',
    'ModuleStatus',
    'CalculatorDefinition',
    'ModuleConfig'
]
