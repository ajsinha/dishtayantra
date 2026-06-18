"""
LMDB-Enabled Python Calculator Example

This example demonstrates how to use LMDB zero-copy data exchange
in a Python calculator for high-performance large payload processing.

PATENT PENDING: The LMDB Zero-Copy Data Exchange technology and
Multi-Language Calculator Framework are subject to pending patent applications.

Copyright © 2025-2030 Ashutosh Sinha. All rights reserved.
DishtaYantra™ is a trademark of Ashutosh Sinha.
"""

import json
import time
import logging
from typing import Any, Dict, Optional

# DishtaYantra imports
from core.calculator.core_calculator import Calculator
from core.lmdb.lmdb_transport import (
    LMDBTransport,
    LMDBConfig,
    DataFormat,
    get_transport,
    lmdb_put,
    lmdb_get,
    lmdb_delete
)
from core.lmdb.lmdb_calculator import (
    LMDBCalculatorConfig,
    LMDBDataExchange,
    LMDBEnabledCalculator,
    wrap_with_lmdb
)

logger = logging.getLogger(__name__)


class LargeMatrixProcessor(Calculator):
    """
    Example calculator that processes large matrices using LMDB zero-copy.
    
    This demonstrates:
    - Direct LMDB transport API usage
    - Automatic large payload detection
    - Zero-copy data access for performance
    
    Use Case: Financial risk calculations, ML model inference, image processing
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__()
        self.config = config or {}
        self.name = self.config.get('name', 'LargeMatrixProcessor')
        
        # LMDB configuration
        self.lmdb_enabled = self.config.get('lmdb_enabled', True)
        self.lmdb_min_size = self.config.get('lmdb_min_size', 10240)  # 10KB
        
        # Initialize LMDB exchange if enabled
        if self.lmdb_enabled:
            lmdb_config = LMDBCalculatorConfig.from_dict(self.config)
            self.lmdb_exchange = LMDBDataExchange(self.name, lmdb_config)
            logger.info(f"LMDB enabled for {self.name} with threshold {self.lmdb_min_size} bytes")
        else:
            self.lmdb_exchange = None
        
        # Stats
        self.calculations = 0
        self.lmdb_exchanges = 0
        self.direct_exchanges = 0
    
    def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process incoming data, using LMDB for large payloads.
        
        Args:
            data: Input dictionary (may contain LMDB reference)
            
        Returns:
            Processed result dictionary
        """
        start_time = time.perf_counter()
        self.calculations += 1
        
        # Check if this is an LMDB reference
        if self._is_lmdb_reference(data):
            return self._process_via_lmdb(data)
        
        # Check if we should use LMDB based on payload size
        if self.lmdb_exchange and self.lmdb_exchange.should_use_lmdb(data):
            return self._process_with_lmdb_exchange(data)
        
        # Direct processing for small payloads
        self.direct_exchanges += 1
        result = self._process_data(data)
        
        elapsed = (time.perf_counter() - start_time) * 1000
        result['_processing_time_ms'] = elapsed
        result['_exchange_type'] = 'direct'
        
        return result
    
    def _is_lmdb_reference(self, data: Dict[str, Any]) -> bool:
        """Check if data is an LMDB reference from another calculator."""
        return data.get('_lmdb_ref', False)
    
    def _process_via_lmdb(self, ref_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process data referenced via LMDB (from native calculator)."""
        self.lmdb_exchanges += 1
        
        input_key = ref_data.get('_lmdb_input_key')
        output_key = ref_data.get('_lmdb_output_key')
        db_path = ref_data.get('_lmdb_db_path')
        
        logger.debug(f"Reading LMDB input from key: {input_key}")
        
        # Get transport and read data
        transport = get_transport(db_path)
        actual_data = transport.get(input_key)
        
        if actual_data is None:
            raise ValueError(f"No data found at LMDB key: {input_key}")
        
        # Process the data
        result = self._process_data(actual_data)
        
        # Write result back to LMDB if output key specified
        if output_key:
            transport.put(output_key, result)
            logger.debug(f"Wrote LMDB output to key: {output_key}")
        
        result['_exchange_type'] = 'lmdb_reference'
        return result
    
    def _process_with_lmdb_exchange(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process large payload using LMDB exchange."""
        self.lmdb_exchanges += 1
        
        # Store input in LMDB
        input_key = self.lmdb_exchange.put_input(data)
        logger.debug(f"Stored input in LMDB: {input_key}")
        
        # Process data
        result = self._process_data(data)
        
        # Clean up (optional - TTL will also handle this)
        self.lmdb_exchange.cleanup()
        
        result['_exchange_type'] = 'lmdb_auto'
        return result
    
    def _process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Actual data processing logic.
        
        Override this in subclasses for specific processing.
        """
        # Example: Matrix operations simulation
        matrix = data.get('matrix', [])
        operation = data.get('operation', 'sum')
        
        result = {
            'input_size': len(str(data)),
            'matrix_rows': len(matrix) if isinstance(matrix, list) else 0,
            'operation': operation,
            'processed_at': time.time()
        }
        
        if matrix and isinstance(matrix, list):
            if operation == 'sum':
                result['result'] = sum(sum(row) if isinstance(row, list) else row for row in matrix)
            elif operation == 'transpose':
                if matrix and isinstance(matrix[0], list):
                    result['result'] = [[matrix[j][i] for j in range(len(matrix))] 
                                       for i in range(len(matrix[0]))]
            elif operation == 'flatten':
                result['result'] = [item for row in matrix for item in (row if isinstance(row, list) else [row])]
        
        return result
    
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


class RiskCalculator(Calculator):
    """
    Financial risk calculator using LMDB for large portfolio data.
    
    Demonstrates LMDB usage for:
    - Large portfolio position data
    - Market data snapshots
    - Risk factor matrices
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__()
        self.config = config or {}
        self.name = self.config.get('name', 'RiskCalculator')
        
        # Wrap with LMDB automatically
        lmdb_config = LMDBCalculatorConfig.from_dict({
            **self.config,
            'lmdb_enabled': True,
            'lmdb_min_size': 50000,  # 50KB threshold for risk data
            'lmdb_data_format': 'msgpack'
        })
        self.lmdb_exchange = LMDBDataExchange(self.name, lmdb_config)
    
    def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate portfolio risk metrics."""
        
        # Handle LMDB reference
        if data.get('_lmdb_ref'):
            transport = get_transport(data.get('_lmdb_db_path'))
            data = transport.get(data.get('_lmdb_input_key'))
        
        # Extract portfolio data
        positions = data.get('positions', [])
        market_data = data.get('market_data', {})
        risk_factors = data.get('risk_factors', {})
        
        # Calculate risk metrics
        total_exposure = sum(p.get('quantity', 0) * p.get('price', 0) for p in positions)
        var_95 = total_exposure * 0.025  # Simplified VaR
        var_99 = total_exposure * 0.035  # Simplified VaR
        
        result = {
            'total_positions': len(positions),
            'total_exposure': total_exposure,
            'var_95': var_95,
            'var_99': var_99,
            'risk_factors_count': len(risk_factors),
            'calculated_at': time.time()
        }
        
        # Write to LMDB if output key specified
        if data.get('_lmdb_output_key'):
            transport = get_transport(data.get('_lmdb_db_path'))
            transport.put(data.get('_lmdb_output_key'), result)
        
        return result


# Example: Using the wrap_with_lmdb decorator
class SimpleCalculator(Calculator):
    """Simple calculator to demonstrate LMDB wrapper."""
    
    def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'doubled': data.get('value', 0) * 2,
            'squared': data.get('value', 0) ** 2
        }


# Wrap existing calculator with LMDB support
LMDBSimpleCalculator = wrap_with_lmdb(
    SimpleCalculator,
    lmdb_min_size=1024,
    lmdb_data_format='msgpack'
)


# =============================================================================
# Usage Examples
# =============================================================================

def example_direct_lmdb_usage():
    """Example: Direct LMDB transport API usage."""
    print("\n=== Direct LMDB Transport Usage ===")
    
    # Get transport instance (uses config path)
    transport = get_transport()
    
    # Store large data
    large_data = {
        'matrix': [[i * j for j in range(100)] for i in range(100)],
        'metadata': {'type': 'covariance', 'size': 100}
    }
    
    # Put data
    key = transport.put('test:matrix:001', large_data, format=DataFormat.MSGPACK)
    print(f"Stored data with key: {key}")
    
    # Get data (zero-copy via memory map)
    retrieved = transport.get('test:matrix:001')
    print(f"Retrieved matrix size: {len(retrieved['matrix'])}x{len(retrieved['matrix'][0])}")
    
    # Get raw bytes (for native code)
    raw_bytes = transport.get_raw('test:matrix:001')
    print(f"Raw bytes size: {len(raw_bytes)} bytes")
    
    # Cleanup
    transport.delete('test:matrix:001')
    print("Cleaned up test data")


def example_calculator_usage():
    """Example: Using LMDB-enabled calculator."""
    print("\n=== LMDB Calculator Usage ===")
    
    # Create calculator with LMDB enabled
    config = {
        'name': 'MatrixProcessor',
        'lmdb_enabled': True,
        'lmdb_min_size': 1024,  # 1KB threshold
        'lmdb_db_path': '/tmp/dishtayantra_lmdb',
        'lmdb_data_format': 'msgpack'
    }
    
    calc = LargeMatrixProcessor(config)
    
    # Small payload - direct processing
    small_data = {'matrix': [[1, 2], [3, 4]], 'operation': 'sum'}
    result = calc.calculate(small_data)
    print(f"Small payload result: {result}")
    
    # Large payload - LMDB exchange
    large_data = {
        'matrix': [[i * j for j in range(50)] for i in range(50)],
        'operation': 'sum'
    }
    result = calc.calculate(large_data)
    print(f"Large payload result: sum={result.get('result')}, type={result.get('_exchange_type')}")
    
    # Print stats
    print(f"Stats: {calc.get_stats()}")


def example_wrapped_calculator():
    """Example: Using wrap_with_lmdb decorator."""
    print("\n=== Wrapped Calculator Usage ===")
    
    # Use the wrapped calculator
    calc = LMDBSimpleCalculator({'name': 'WrappedCalc'})
    
    result = calc.calculate({'value': 5})
    print(f"Result: {result}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    
    example_direct_lmdb_usage()
    example_calculator_usage()
    example_wrapped_calculator()
