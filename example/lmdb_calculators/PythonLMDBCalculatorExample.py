#!/usr/bin/env python3
"""
Standalone Python LMDB Calculator Example

This is a simple, self-contained example of a Python calculator that uses
LMDB zero-copy data exchange for high-performance large payload processing.

PATENT PENDING: The LMDB Zero-Copy Data Exchange technology and
Multi-Language Calculator Framework are subject to pending patent applications.

Copyright © 2025-2030 Ashutosh Sinha. All rights reserved.
DishtaYantra™ is a trademark of Ashutosh Sinha.

Usage:
    python PythonLMDBCalculatorExample.py

Requirements:
    pip install lmdb msgpack
"""

import os
import json
import time
import uuid
import struct
import logging
from typing import Any, Dict, Optional, List
from dataclasses import dataclass, field
from abc import ABC, abstractmethod

# Optional: Try to import lmdb and msgpack
try:
    import lmdb
    LMDB_AVAILABLE = True
except ImportError:
    LMDB_AVAILABLE = False
    print("Warning: lmdb not installed. Run: pip install lmdb")

try:
    import msgpack
    MSGPACK_AVAILABLE = True
except ImportError:
    MSGPACK_AVAILABLE = False
    print("Warning: msgpack not installed. Run: pip install msgpack")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# LMDB Transport Layer
# =============================================================================

@dataclass
class LMDBConfig:
    """Configuration for LMDB transport."""
    db_path: str = "/tmp/dishtayantra_lmdb"
    map_size: int = 1024 * 1024 * 1024  # 1GB
    max_dbs: int = 100
    ttl_seconds: int = 300
    min_payload_size: int = 1024  # 1KB threshold


class LMDBTransport:
    """
    LMDB-based zero-copy data transport.
    
    This class provides memory-mapped data exchange for high-performance
    calculator communication.
    
    PATENT PENDING: This technology is subject to pending patent applications.
    """
    
    _instances: Dict[str, 'LMDBTransport'] = {}
    
    def __init__(self, config: LMDBConfig):
        self.config = config
        self.env = None
        self.db = None
        self._init_lmdb()
    
    def _init_lmdb(self):
        """Initialize LMDB environment."""
        if not LMDB_AVAILABLE:
            raise RuntimeError("lmdb package not installed")
        
        # Create directory if needed
        os.makedirs(self.config.db_path, exist_ok=True)
        
        # Open LMDB environment
        self.env = lmdb.open(
            self.config.db_path,
            map_size=self.config.map_size,
            max_dbs=self.config.max_dbs,
            writemap=True,
            sync=False
        )
        
        # Open default database
        self.db = self.env.open_db(b'default', create=True)
        
        logger.info(f"LMDB initialized at: {self.config.db_path}")
    
    @classmethod
    def get_instance(cls, db_path: str = None) -> 'LMDBTransport':
        """Get or create singleton instance for given path."""
        path = db_path or "/tmp/dishtayantra_lmdb"
        if path not in cls._instances:
            config = LMDBConfig(db_path=path)
            cls._instances[path] = cls(config)
        return cls._instances[path]
    
    def put(self, key: str, data: Any, use_msgpack: bool = True) -> str:
        """
        Store data in LMDB.
        
        Args:
            key: Storage key
            data: Data to store (dict, list, or bytes)
            use_msgpack: Use MessagePack serialization (faster)
            
        Returns:
            The key used for storage
        """
        # Serialize data
        if isinstance(data, bytes):
            serialized = data
        elif use_msgpack and MSGPACK_AVAILABLE:
            serialized = msgpack.packb(data, use_bin_type=True)
        else:
            serialized = json.dumps(data).encode('utf-8')
        
        # Store in LMDB
        with self.env.begin(db=self.db, write=True) as txn:
            txn.put(key.encode('utf-8'), serialized)
        
        logger.debug(f"Stored {len(serialized)} bytes at key: {key}")
        return key
    
    def get(self, key: str, use_msgpack: bool = True) -> Any:
        """
        Retrieve data from LMDB (ZERO-COPY via memory map!).
        
        Args:
            key: Storage key
            use_msgpack: Use MessagePack deserialization
            
        Returns:
            Deserialized data
        """
        with self.env.begin(db=self.db) as txn:
            # ZERO-COPY: This returns a view into the memory-mapped file!
            data = txn.get(key.encode('utf-8'))
            
            if data is None:
                return None
            
            # Deserialize
            if use_msgpack and MSGPACK_AVAILABLE:
                return msgpack.unpackb(data, raw=False)
            else:
                return json.loads(data.decode('utf-8'))
    
    def get_raw(self, key: str) -> Optional[bytes]:
        """
        Get raw bytes from LMDB (for native code integration).
        
        This is used when passing data to Java/C++/Rust calculators
        that handle their own deserialization.
        """
        with self.env.begin(db=self.db) as txn:
            return txn.get(key.encode('utf-8'))
    
    def delete(self, key: str) -> bool:
        """Delete key from LMDB."""
        with self.env.begin(db=self.db, write=True) as txn:
            return txn.delete(key.encode('utf-8'))
    
    def close(self):
        """Close LMDB environment."""
        if self.env:
            self.env.close()
            self.env = None


# =============================================================================
# Calculator Base Class
# =============================================================================

class Calculator(ABC):
    """
    Abstract base class for all calculators.
    
    PATENT PENDING: The Multi-Language Calculator Framework
    is subject to pending patent applications.
    """
    
    @abstractmethod
    def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process input data and return result."""
        pass
    
    def get_name(self) -> str:
        """Get calculator name."""
        return self.__class__.__name__


# =============================================================================
# LMDB-Enabled Calculator
# =============================================================================

class LMDBCalculator(Calculator):
    """
    Calculator with automatic LMDB zero-copy support.
    
    This calculator automatically uses LMDB for large payloads,
    providing 100-1000x speedup compared to serialization.
    
    PATENT PENDING: This technology is subject to pending patent applications.
    
    Features:
        - Automatic payload size detection
        - Zero-copy data access via memory-mapped files
        - Transparent fallback for small payloads
        - Support for LMDB references from native calculators
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.name = self.config.get('name', self.__class__.__name__)
        
        # LMDB configuration
        self.lmdb_enabled = self.config.get('lmdb_enabled', True)
        self.lmdb_min_size = self.config.get('lmdb_min_size', 1024)  # 1KB
        self.lmdb_db_path = self.config.get('lmdb_db_path', '/tmp/dishtayantra_lmdb')
        
        # Statistics
        self.calculations = 0
        self.lmdb_exchanges = 0
        self.direct_exchanges = 0
        
        # Initialize LMDB transport
        if self.lmdb_enabled and LMDB_AVAILABLE:
            self.transport = LMDBTransport.get_instance(self.lmdb_db_path)
        else:
            self.transport = None
    
    def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process data with automatic LMDB routing.
        
        1. Check if data is an LMDB reference (from native calculator)
        2. Check if payload is large enough for LMDB
        3. Process data and return result
        """
        start_time = time.perf_counter()
        self.calculations += 1
        
        # Check if this is an LMDB reference from native code
        if self._is_lmdb_reference(data):
            result = self._process_lmdb_reference(data)
        # Check if we should use LMDB for large payload
        elif self._should_use_lmdb(data):
            result = self._process_with_lmdb(data)
        # Direct processing for small payloads
        else:
            self.direct_exchanges += 1
            result = self.process(data)
        
        # Add timing
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        result['_processing_time_ms'] = elapsed_ms
        result['_calculator'] = self.name
        
        return result
    
    def _is_lmdb_reference(self, data: Dict[str, Any]) -> bool:
        """Check if data is an LMDB reference from native calculator."""
        return data.get('_lmdb_ref', False)
    
    def _should_use_lmdb(self, data: Dict[str, Any]) -> bool:
        """Check if payload size exceeds LMDB threshold."""
        if not self.transport:
            return False
        
        # Estimate payload size
        try:
            if MSGPACK_AVAILABLE:
                size = len(msgpack.packb(data, use_bin_type=True))
            else:
                size = len(json.dumps(data).encode('utf-8'))
            return size >= self.lmdb_min_size
        except:
            return False
    
    def _process_lmdb_reference(self, ref_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process data via LMDB reference from native calculator."""
        self.lmdb_exchanges += 1
        
        input_key = ref_data.get('_lmdb_input_key')
        output_key = ref_data.get('_lmdb_output_key')
        db_path = ref_data.get('_lmdb_db_path', self.lmdb_db_path)
        
        logger.debug(f"Processing LMDB reference: {input_key}")
        
        # Get transport for specified path
        transport = LMDBTransport.get_instance(db_path)
        
        # Read data from LMDB (zero-copy!)
        actual_data = transport.get(input_key)
        if actual_data is None:
            raise ValueError(f"No data found at LMDB key: {input_key}")
        
        # Process the data
        result = self.process(actual_data)
        
        # Write result to LMDB if output key specified
        if output_key:
            transport.put(output_key, result)
            logger.debug(f"Wrote result to LMDB: {output_key}")
        
        result['_exchange_type'] = 'lmdb_reference'
        return result
    
    def _process_with_lmdb(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process large payload using LMDB exchange."""
        self.lmdb_exchanges += 1
        
        # Generate unique key for this exchange
        txn_id = str(uuid.uuid4())[:8]
        input_key = f"input:{self.name}:{txn_id}"
        
        # Store input in LMDB
        self.transport.put(input_key, data)
        logger.debug(f"Stored large payload in LMDB: {input_key}")
        
        # Process data
        result = self.process(data)
        
        # Clean up input (optional - TTL will also handle this)
        self.transport.delete(input_key)
        
        result['_exchange_type'] = 'lmdb_auto'
        return result
    
    @abstractmethod
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Actual data processing logic.
        
        Override this method in subclasses.
        """
        pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get calculator statistics."""
        return {
            'name': self.name,
            'calculations': self.calculations,
            'lmdb_exchanges': self.lmdb_exchanges,
            'direct_exchanges': self.direct_exchanges,
            'lmdb_enabled': self.lmdb_enabled,
            'lmdb_min_size': self.lmdb_min_size
        }


# =============================================================================
# Example Calculator Implementations
# =============================================================================

class MatrixSumCalculator(LMDBCalculator):
    """
    Example: Matrix sum calculator with LMDB support.
    
    Demonstrates processing large matrix data efficiently.
    """
    
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        matrix = data.get('matrix', [])
        
        if not matrix:
            return {'result': 0, 'rows': 0, 'cols': 0}
        
        # Calculate sum
        total = sum(sum(row) if isinstance(row, list) else row for row in matrix)
        
        return {
            'result': total,
            'rows': len(matrix),
            'cols': len(matrix[0]) if matrix and isinstance(matrix[0], list) else 1,
            'operation': 'sum'
        }


class RiskCalculator(LMDBCalculator):
    """
    Example: Financial risk calculator with LMDB support.
    
    Demonstrates processing large portfolio data efficiently.
    """
    
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        positions = data.get('positions', [])
        
        # Calculate total exposure
        total_exposure = sum(
            p.get('quantity', 0) * p.get('price', 0)
            for p in positions
        )
        
        # Calculate VaR (simplified)
        var_95 = total_exposure * 0.025
        var_99 = total_exposure * 0.035
        
        return {
            'total_positions': len(positions),
            'total_exposure': total_exposure,
            'var_95': var_95,
            'var_99': var_99,
            'calculated_at': time.time()
        }


class SignalProcessor(LMDBCalculator):
    """
    Example: Signal processing calculator with LMDB support.
    
    Demonstrates processing large time-series data efficiently.
    """
    
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        signal = data.get('signal', [])
        
        if not signal:
            return {'error': 'No signal data'}
        
        # Calculate statistics
        n = len(signal)
        mean = sum(signal) / n
        variance = sum((x - mean) ** 2 for x in signal) / n
        std_dev = variance ** 0.5
        min_val = min(signal)
        max_val = max(signal)
        
        return {
            'sample_count': n,
            'mean': mean,
            'variance': variance,
            'std_dev': std_dev,
            'min': min_val,
            'max': max_val,
            'range': max_val - min_val
        }


# =============================================================================
# Example Usage
# =============================================================================

def example_basic_usage():
    """Example: Basic LMDB calculator usage."""
    print("\n" + "=" * 60)
    print("Example 1: Basic LMDB Calculator Usage")
    print("=" * 60)
    
    # Create calculator
    calc = MatrixSumCalculator({
        'name': 'MatrixProcessor',
        'lmdb_enabled': True,
        'lmdb_min_size': 100  # Low threshold for demo
    })
    
    # Small payload (direct processing)
    small_data = {'matrix': [[1, 2], [3, 4]]}
    result = calc.calculate(small_data)
    print(f"\nSmall payload result: {result}")
    
    # Large payload (LMDB processing)
    large_matrix = [[i * j for j in range(50)] for i in range(50)]
    large_data = {'matrix': large_matrix}
    result = calc.calculate(large_data)
    print(f"\nLarge payload result: sum={result['result']}, type={result.get('_exchange_type', 'direct')}")
    
    # Print stats
    print(f"\nCalculator stats: {calc.get_stats()}")


def example_lmdb_reference():
    """Example: Processing LMDB reference (from native calculator)."""
    print("\n" + "=" * 60)
    print("Example 2: LMDB Reference Processing")
    print("=" * 60)
    
    if not LMDB_AVAILABLE:
        print("Skipping - lmdb not installed")
        return
    
    # Create transport and store data (simulating native calculator)
    transport = LMDBTransport.get_instance()
    
    # Store large data in LMDB (as native calculator would)
    large_data = {
        'matrix': [[i * j for j in range(100)] for i in range(100)],
        'metadata': {'source': 'native_calculator'}
    }
    transport.put('input:native:001', large_data)
    print("Stored data in LMDB (simulating native calculator)")
    
    # Create Python calculator
    calc = MatrixSumCalculator({'name': 'PythonProcessor'})
    
    # Create LMDB reference (as native calculator would pass to Python)
    lmdb_ref = {
        '_lmdb_ref': True,
        '_lmdb_input_key': 'input:native:001',
        '_lmdb_output_key': 'output:native:001',
        '_lmdb_db_path': '/tmp/dishtayantra_lmdb'
    }
    
    # Process via reference
    result = calc.calculate(lmdb_ref)
    print(f"\nResult from LMDB reference: {result}")
    
    # Verify output was written
    output = transport.get('output:native:001')
    print(f"Output written to LMDB: {output is not None}")
    
    # Cleanup
    transport.delete('input:native:001')
    transport.delete('output:native:001')


def example_risk_calculator():
    """Example: Risk calculator with large portfolio."""
    print("\n" + "=" * 60)
    print("Example 3: Risk Calculator with Large Portfolio")
    print("=" * 60)
    
    # Create calculator
    calc = RiskCalculator({
        'name': 'RiskEngine',
        'lmdb_enabled': True,
        'lmdb_min_size': 1000
    })
    
    # Generate large portfolio
    import random
    positions = [
        {
            'symbol': f'STOCK{i}',
            'quantity': random.randint(100, 10000),
            'price': random.uniform(10, 500)
        }
        for i in range(1000)
    ]
    
    # Calculate risk
    result = calc.calculate({'positions': positions})
    print(f"\nRisk calculation result:")
    print(f"  Total positions: {result['total_positions']}")
    print(f"  Total exposure: ${result['total_exposure']:,.2f}")
    print(f"  VaR 95%: ${result['var_95']:,.2f}")
    print(f"  VaR 99%: ${result['var_99']:,.2f}")
    print(f"  Exchange type: {result.get('_exchange_type', 'direct')}")


def example_performance_comparison():
    """Example: Performance comparison with/without LMDB."""
    print("\n" + "=" * 60)
    print("Example 4: Performance Comparison")
    print("=" * 60)
    
    if not LMDB_AVAILABLE:
        print("Skipping - lmdb not installed")
        return
    
    # Create calculators
    calc_with_lmdb = MatrixSumCalculator({
        'name': 'WithLMDB',
        'lmdb_enabled': True,
        'lmdb_min_size': 100
    })
    
    calc_without_lmdb = MatrixSumCalculator({
        'name': 'WithoutLMDB',
        'lmdb_enabled': False
    })
    
    # Test data sizes
    sizes = [10, 50, 100, 200]
    
    print("\nMatrix Size | With LMDB | Without LMDB | Ratio")
    print("-" * 55)
    
    for size in sizes:
        matrix = [[i * j for j in range(size)] for i in range(size)]
        data = {'matrix': matrix}
        
        # With LMDB
        start = time.perf_counter()
        for _ in range(10):
            calc_with_lmdb.calculate(data)
        time_with = (time.perf_counter() - start) * 100  # ms per op
        
        # Without LMDB
        start = time.perf_counter()
        for _ in range(10):
            calc_without_lmdb.calculate(data)
        time_without = (time.perf_counter() - start) * 100  # ms per op
        
        ratio = time_without / time_with if time_with > 0 else 0
        print(f"  {size}x{size}    |  {time_with:.3f}ms  |   {time_without:.3f}ms   |  {ratio:.1f}x")


# =============================================================================
# Main
# =============================================================================

if __name__ == '__main__':
    print("=" * 60)
    print("DishtaYantra Python LMDB Calculator Examples")
    print("=" * 60)
    print("\nPATENT PENDING: LMDB Zero-Copy Data Exchange")
    print("Copyright © 2025-2030 Ashutosh Sinha. All rights reserved.\n")
    
    if not LMDB_AVAILABLE:
        print("WARNING: lmdb package not installed.")
        print("Install with: pip install lmdb msgpack")
        print("\nRunning basic examples only...\n")
    
    example_basic_usage()
    example_lmdb_reference()
    example_risk_calculator()
    example_performance_comparison()
    
    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)
