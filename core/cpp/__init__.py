"""
DishtaYantra C++ Calculator Integration
Version: 1.7.0

Provides high-performance C++ calculator support via pybind11.

Features:
- Config-based module loading (cpp_config.json)
- Calculator instance caching
- LMDB zero-copy data exchange
- ~100ns call overhead

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

from .cpp_manager import (
    CPPManager,
    ModuleConfig,
    CalculatorDefinition,
    ModuleStatus,
    get_cpp_manager,
    initialize_cpp_manager,
    create_cpp_calculator,
    is_cpp_available
)

__all__ = [
    'CPPManager',
    'ModuleConfig',
    'CalculatorDefinition', 
    'ModuleStatus',
    'get_cpp_manager',
    'initialize_cpp_manager',
    'create_cpp_calculator',
    'is_cpp_available'
]
