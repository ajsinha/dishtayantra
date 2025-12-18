"""
LMDB-Enabled Calculator Base Classes

Provides base classes for calculators that leverage LMDB memory-mapped
files for zero-copy data exchange with native code (Java, C++, Rust).

Patent-Pending Innovation:
This architecture enables sub-microsecond data exchange for payloads
of any size between heterogeneous language calculators, eliminating
the serialization bottleneck present in all other DAG frameworks.

Copyright © 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import time
import uuid
from typing import Any, Dict, Optional, Union
from dataclasses import dataclass
from enum import Enum

from .lmdb_transport import (
    LMDBTransport, 
    LMDBConfig, 
    DataFormat, 
    DataEnvelope,
    LMDBCalculatorMixin,
    get_transport
)

logger = logging.getLogger(__name__)


class LMDBExchangeMode(Enum):
    """How calculator exchanges data via LMDB"""
    INPUT_ONLY = "input"       # Read input from LMDB, return output directly
    OUTPUT_ONLY = "output"     # Receive input directly, write output to LMDB
    BIDIRECTIONAL = "both"     # Both input and output via LMDB
    REFERENCE = "reference"    # Pass LMDB key reference, calculator reads/writes


@dataclass
class LMDBCalculatorConfig:
    """Configuration for LMDB-enabled calculator"""
    # Enable LMDB transport
    enabled: bool = False
    
    # LMDB database path
    db_path: str = "/tmp/dishtayantra_lmdb"
    
    # Named database for this calculator
    db_name: str = "default"
    
    # Exchange mode
    exchange_mode: LMDBExchangeMode = LMDBExchangeMode.BIDIRECTIONAL
    
    # Data format for serialization
    data_format: DataFormat = DataFormat.MSGPACK
    
    # Input key prefix (actual key will be {prefix}:{node_name}:{txn_id})
    input_key_prefix: str = "input"
    
    # Output key prefix
    output_key_prefix: str = "output"
    
    # TTL for stored data (seconds)
    ttl_seconds: int = 300
    
    # Minimum payload size to use LMDB (smaller payloads use direct pass)
    min_payload_size: int = 1024  # 1KB
    
    # Wait timeout for native calculator (ms)
    wait_timeout_ms: int = 30000
    
    # Poll interval when waiting for output (ms)
    poll_interval_ms: int = 10
    
    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> 'LMDBCalculatorConfig':
        """Create config from dictionary"""
        return cls(
            enabled=config.get('lmdb_enabled', config.get('enabled', False)),
            db_path=config.get('lmdb_db_path', config.get('db_path', '/tmp/dishtayantra_lmdb')),
            db_name=config.get('lmdb_db_name', config.get('db_name', 'default')),
            exchange_mode=LMDBExchangeMode(config.get('lmdb_exchange_mode', 'both')),
            data_format=DataFormat(config.get('lmdb_data_format', 'msgpack')),
            input_key_prefix=config.get('lmdb_input_prefix', 'input'),
            output_key_prefix=config.get('lmdb_output_prefix', 'output'),
            ttl_seconds=config.get('lmdb_ttl', 300),
            min_payload_size=config.get('lmdb_min_size', 1024),
            wait_timeout_ms=config.get('lmdb_wait_timeout', 30000),
            poll_interval_ms=config.get('lmdb_poll_interval', 10)
        )


class LMDBDataExchange:
    """
    Handles LMDB data exchange for a calculator instance.
    
    This class manages:
    - Automatic decision on LMDB vs direct pass based on payload size
    - Key generation for input/output data
    - Waiting for native calculator output
    - Cleanup of temporary data
    """
    
    def __init__(self, config: LMDBCalculatorConfig, calculator_name: str):
        self.config = config
        self.calculator_name = calculator_name
        self._transport: Optional[LMDBTransport] = None
        self._initialized = False
    
    def initialize(self) -> bool:
        """Initialize LMDB transport"""
        if not self.config.enabled:
            return False
        
        try:
            self._transport = get_transport(self.config.db_path)
            self._initialized = True
            logger.info(f"LMDB exchange initialized for {self.calculator_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize LMDB exchange: {e}")
            return False
    
    def is_enabled(self) -> bool:
        """Check if LMDB exchange is enabled and initialized"""
        return self.config.enabled and self._initialized
    
    def should_use_lmdb(self, data: Any) -> bool:
        """Determine if LMDB should be used based on payload size"""
        if not self.is_enabled():
            return False
        
        # Estimate payload size
        try:
            import sys
            size = sys.getsizeof(data)
            
            # For containers, estimate more accurately
            if isinstance(data, dict):
                size = len(str(data))
            elif isinstance(data, (list, tuple)):
                size = len(str(data))
            elif isinstance(data, bytes):
                size = len(data)
            
            return size >= self.config.min_payload_size
        except Exception:
            return False
    
    def generate_key(self, prefix: str, txn_id: Optional[str] = None) -> str:
        """Generate a unique key for LMDB storage"""
        txn_id = txn_id or str(uuid.uuid4())[:8]
        return f"{prefix}:{self.calculator_name}:{txn_id}"
    
    def put_input(self, data: Any, txn_id: Optional[str] = None) -> Optional[str]:
        """
        Store input data in LMDB for native calculator consumption.
        
        Returns the key where data was stored.
        """
        if not self.is_enabled():
            return None
        
        key = self.generate_key(self.config.input_key_prefix, txn_id)
        
        try:
            self._transport.put(
                key=key,
                data=data,
                format=self.config.data_format,
                ttl=self.config.ttl_seconds,
                source="dag_engine",
                target=self.calculator_name,
                db_name=self.config.db_name
            )
            logger.debug(f"Stored input at {key}")
            return key
        except Exception as e:
            logger.error(f"Failed to store input: {e}")
            return None
    
    def get_output(self, txn_id: str, wait: bool = True) -> Optional[Any]:
        """
        Retrieve output data from LMDB written by native calculator.
        
        Args:
            txn_id: Transaction ID to look for
            wait: Whether to wait for output with timeout
            
        Returns:
            Output data or None
        """
        if not self.is_enabled():
            return None
        
        key = self.generate_key(self.config.output_key_prefix, txn_id)
        
        if wait:
            return self._wait_for_output(key)
        else:
            return self._transport.get(key, self.config.db_name)
    
    def _wait_for_output(self, key: str) -> Optional[Any]:
        """Wait for output with timeout"""
        start_time = time.time()
        timeout_sec = self.config.wait_timeout_ms / 1000.0
        poll_sec = self.config.poll_interval_ms / 1000.0
        
        while (time.time() - start_time) < timeout_sec:
            result = self._transport.get(key, self.config.db_name)
            if result is not None:
                return result
            time.sleep(poll_sec)
        
        logger.warning(f"Timeout waiting for output at {key}")
        return None
    
    def cleanup(self, txn_id: str):
        """Clean up input/output data for a transaction"""
        if not self.is_enabled():
            return
        
        input_key = self.generate_key(self.config.input_key_prefix, txn_id)
        output_key = self.generate_key(self.config.output_key_prefix, txn_id)
        
        self._transport.delete(input_key, self.config.db_name)
        self._transport.delete(output_key, self.config.db_name)
    
    def get_exchange_info(self) -> Dict[str, Any]:
        """Get information about the LMDB exchange for native calculators"""
        return {
            'enabled': self.is_enabled(),
            'db_path': self.config.db_path,
            'db_name': self.config.db_name,
            'input_prefix': self.config.input_key_prefix,
            'output_prefix': self.config.output_key_prefix,
            'calculator_name': self.calculator_name,
            'data_format': self.config.data_format.value,
            'ttl_seconds': self.config.ttl_seconds
        }


class LMDBEnabledCalculator:
    """
    Base class for calculators with LMDB zero-copy data exchange support.
    
    This class wraps any calculator and adds LMDB transport capabilities.
    It automatically decides when to use LMDB based on payload size.
    
    Usage in DAG configuration:
    {
        "name": "heavy_processor",
        "type": "com.example.HeavyProcessor",
        "calculator": "java",
        "lmdb_enabled": true,
        "lmdb_db_path": "/tmp/dishtayantra_lmdb",
        "lmdb_exchange_mode": "both",
        "lmdb_min_size": 10240
    }
    """
    
    def __init__(
        self,
        inner_calculator: Any,
        config: Union[Dict[str, Any], LMDBCalculatorConfig],
        calculator_name: str
    ):
        """
        Initialize LMDB-enabled calculator wrapper.
        
        Args:
            inner_calculator: The actual calculator instance
            config: LMDB configuration (dict or LMDBCalculatorConfig)
            calculator_name: Name of the calculator for key generation
        """
        self.inner = inner_calculator
        self.calculator_name = calculator_name
        
        if isinstance(config, dict):
            self.config = LMDBCalculatorConfig.from_dict(config)
        else:
            self.config = config
        
        self.exchange = LMDBDataExchange(self.config, calculator_name)
        self._txn_counter = 0
    
    def initialize(self) -> bool:
        """Initialize the calculator and LMDB exchange"""
        lmdb_ok = self.exchange.initialize()
        
        # Initialize inner calculator if it has an initialize method
        if hasattr(self.inner, 'initialize'):
            inner_ok = self.inner.initialize()
        else:
            inner_ok = True
        
        return inner_ok and (lmdb_ok or not self.config.enabled)
    
    def calculate(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute calculation with automatic LMDB exchange for large payloads.
        
        This method:
        1. Checks if payload warrants LMDB transport
        2. If yes, stores input in LMDB and passes reference
        3. Calls inner calculator
        4. If LMDB mode, retrieves output from LMDB
        5. Cleans up temporary data
        """
        # Generate transaction ID
        self._txn_counter += 1
        txn_id = f"{time.time_ns()}-{self._txn_counter}"
        
        use_lmdb = self.exchange.should_use_lmdb(input_data)
        
        if use_lmdb and self.config.exchange_mode in (
            LMDBExchangeMode.INPUT_ONLY, 
            LMDBExchangeMode.BIDIRECTIONAL,
            LMDBExchangeMode.REFERENCE
        ):
            # Store input in LMDB
            input_key = self.exchange.put_input(input_data, txn_id)
            
            if input_key is None:
                # Fallback to direct pass
                return self._call_inner(input_data)
            
            # Create reference payload
            reference_data = {
                '_lmdb_ref': True,
                '_lmdb_input_key': input_key,
                '_lmdb_output_key': self.exchange.generate_key(
                    self.config.output_key_prefix, txn_id
                ),
                '_lmdb_db_path': self.config.db_path,
                '_lmdb_db_name': self.config.db_name,
                '_lmdb_format': self.config.data_format.value,
                '_txn_id': txn_id
            }
            
            # Call inner calculator with reference
            result = self._call_inner(reference_data)
            
            # Check if output is via LMDB
            if self.config.exchange_mode in (
                LMDBExchangeMode.OUTPUT_ONLY,
                LMDBExchangeMode.BIDIRECTIONAL
            ):
                # Wait for output from LMDB
                lmdb_result = self.exchange.get_output(txn_id, wait=True)
                if lmdb_result is not None:
                    result = lmdb_result
            
            # Cleanup
            self.exchange.cleanup(txn_id)
            
            return result
        else:
            # Direct pass for small payloads
            return self._call_inner(input_data)
    
    def _call_inner(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Call the inner calculator"""
        if hasattr(self.inner, 'calculate'):
            return self.inner.calculate(data)
        elif callable(self.inner):
            return self.inner(data)
        else:
            raise RuntimeError(f"Inner calculator {self.calculator_name} not callable")
    
    def is_lmdb_enabled(self) -> bool:
        """Check if LMDB is enabled and initialized"""
        return self.exchange.is_enabled()
    
    def get_lmdb_info(self) -> Dict[str, Any]:
        """Get LMDB exchange info for debugging/monitoring"""
        return self.exchange.get_exchange_info()
    
    def close(self):
        """Close the calculator"""
        if hasattr(self.inner, 'close'):
            self.inner.close()


def wrap_with_lmdb(
    calculator: Any,
    config: Dict[str, Any],
    calculator_name: str
) -> Union[LMDBEnabledCalculator, Any]:
    """
    Wrap a calculator with LMDB support if configured.
    
    Args:
        calculator: Calculator instance to wrap
        config: Configuration dictionary
        calculator_name: Name of the calculator
        
    Returns:
        Wrapped calculator if LMDB enabled, original otherwise
    """
    if config.get('lmdb_enabled', False):
        wrapper = LMDBEnabledCalculator(calculator, config, calculator_name)
        wrapper.initialize()
        return wrapper
    else:
        return calculator
