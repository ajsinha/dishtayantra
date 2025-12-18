"""
LMDB Transport Module for DishtaYantra

This module provides zero-copy data exchange between the Python DAG Engine
and native calculators (Java, C++, Rust) using LMDB memory-mapped files.

Key Components:
- LMDBTransport: Core transport layer with LMDB operations
- LMDBCalculatorConfig: Configuration for LMDB-enabled calculators
- LMDBEnabledCalculator: Wrapper for calculators with LMDB support
- DataFormat: Supported serialization formats

Usage:
    from core.lmdb import LMDBTransport, wrap_with_lmdb
    
    # Direct transport usage
    transport = LMDBTransport.get_instance()
    transport.put("key", large_data)
    data = transport.get("key")
    
    # Calculator wrapping
    calculator = wrap_with_lmdb(my_calculator, config, "calc_name")

Patent-Pending Innovation:
This architecture enables heterogeneous language calculators to exchange
large datasets without serialization overhead, achieving 100-1000x speedup
compared to traditional approaches.

Copyright © 2025-2030 Ashutosh Sinha. All rights reserved.
"""

from .lmdb_transport import (
    LMDBTransport,
    LMDBConfig,
    DataFormat,
    DataEnvelope,
    LMDBCalculatorMixin,
    get_transport,
    lmdb_put,
    lmdb_get,
    lmdb_delete
)

from .lmdb_calculator import (
    LMDBCalculatorConfig,
    LMDBExchangeMode,
    LMDBDataExchange,
    LMDBEnabledCalculator,
    wrap_with_lmdb
)

__all__ = [
    # Transport
    'LMDBTransport',
    'LMDBConfig',
    'DataFormat',
    'DataEnvelope',
    'LMDBCalculatorMixin',
    'get_transport',
    'lmdb_put',
    'lmdb_get',
    'lmdb_delete',
    
    # Calculator support
    'LMDBCalculatorConfig',
    'LMDBExchangeMode',
    'LMDBDataExchange',
    'LMDBEnabledCalculator',
    'wrap_with_lmdb'
]
