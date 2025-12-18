"""
Rust PyO3 Calculator Integration for DishtaYantra
=================================================

This module provides integration with Rust calculators compiled using PyO3.
Rust calculators offer:
- C/C++ level performance
- Memory safety guaranteed at compile time
- Thread safety enforced by the type system
- Fearless concurrency with rayon
- Easy deployment with maturin
- LMDB memory-mapped zero-copy exchange for large payloads (v1.1.1)

The Rust struct must implement:
- #[new] fn new(name: String, config: &PyDict) -> Self
- fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject>
- fn details(&self, py: Python) -> PyResult<PyObject>
"""

import importlib
import logging
import time
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# LMDB support
try:
    from core.lmdb import LMDBCalculatorConfig, LMDBDataExchange, DataFormat
    LMDB_AVAILABLE = True
except ImportError:
    LMDB_AVAILABLE = False
    logger.debug("LMDB module not available for Rust calculator")


class RustCalculator:
    """
    Wrapper for Rust calculators compiled with PyO3.
    
    The Rust calculator struct must implement the same interface as Python's
    DataCalculator:
    
    ```rust
    use pyo3::prelude::*;
    use pyo3::types::PyDict;
    
    #[pyclass]
    struct MyCalculator {
        name: String,
        // ... fields
    }
    
    #[pymethods]
    impl MyCalculator {
        #[new]
        fn new(name: String, config: &PyDict) -> PyResult<Self>;
        
        fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject>;
        
        fn details(&self, py: Python) -> PyResult<PyObject>;
    }
    ```
    
    For LMDB-enabled calculators, the Rust side can access data via memory-mapped
    files when receiving an '_lmdb_ref' in the input dict:
    
    ```rust
    fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject> {
        if let Ok(lmdb_ref) = data.get_item("_lmdb_ref") {
            if lmdb_ref.is_some() && lmdb_ref.unwrap().is_true()? {
                // Read from LMDB using lmdb-rs crate
                let db_path: String = data.get_item("_lmdb_db_path")?.extract()?;
                let input_key: String = data.get_item("_lmdb_input_key")?.extract()?;
                // Use lmdb-rs to read data
            }
        }
        // ...
    }
    ```
    
    Example usage:
    
    ```python
    config = {
        'rust_module': 'dishtayantra_rust',
        'rust_class': 'MathCalculator',
        'operation': 'sum',
        'arguments': ['a', 'b', 'c'],
        'lmdb_enabled': True,
        'lmdb_min_size': 10240  # Use LMDB for payloads > 10KB
    }
    calc = RustCalculator('my_calc', config)
    result = calc.calculate({'a': 1.0, 'b': 2.0, 'c': 3.0})
    ```
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize Rust calculator wrapper.
        
        Args:
            name: Calculator name
            config: Configuration dictionary, must include:
                    - rust_module: Name of the compiled module (e.g., 'dishtayantra_rust')
                    - rust_class: Name of the Rust struct (e.g., 'MathCalculator')
                    Optional:
                    - lmdb_enabled: Enable LMDB zero-copy transport
                    - lmdb_db_path: LMDB database path
                    - lmdb_min_size: Min payload size to use LMDB
                    Additional config options are passed to the Rust constructor.
        """
        self.name = name
        self.config = config
        
        rust_module = config.get('rust_module', 'dishtayantra_rust')
        rust_class = config.get('rust_class')
        
        if not rust_class:
            raise ValueError("rust_class must be specified for Rust calculators")
        
        try:
            # Import the compiled module
            module = importlib.import_module(rust_module)
            calculator_class = getattr(module, rust_class)
            
            # Create Rust calculator instance
            # Pass the full config dict - Rust can extract what it needs
            self._rust_instance = calculator_class(name, config)
            
            logger.info(f"RustCalculator '{name}' initialized: {rust_module}.{rust_class}")
            
        except ImportError as e:
            raise RuntimeError(
                f"Failed to import Rust module '{rust_module}': {e}\n"
                f"Ensure the module is compiled with maturin:\n"
                f"  cd your_rust_project\n"
                f"  maturin develop --release"
            )
        except AttributeError as e:
            raise RuntimeError(f"Struct '{rust_class}' not found in module '{rust_module}': {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Rust calculator: {e}")
        
        # LMDB support (v1.1.1)
        self._lmdb_exchange: Optional[LMDBDataExchange] = None
        self._lmdb_enabled = False
        if LMDB_AVAILABLE and config.get('lmdb_enabled', False):
            lmdb_config = LMDBCalculatorConfig.from_dict(config)
            self._lmdb_exchange = LMDBDataExchange(lmdb_config, name)
            self._lmdb_enabled = self._lmdb_exchange.initialize()
            if self._lmdb_enabled:
                logger.info(f"RustCalculator '{name}' LMDB transport enabled at {lmdb_config.db_path}")
        
        # Statistics
        self._stats = {
            'calculations': 0,
            'total_time_ns': 0,
            'lmdb_exchanges': 0
        }
    
    def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute calculation on the Rust side.
        
        For large payloads (configurable, default 1KB+), uses LMDB memory-mapped
        files for zero-copy data exchange, achieving maximum performance with
        memory safety guarantees.
        
        Args:
            data: Input data dictionary
            
        Returns:
            Output data dictionary from Rust calculation
        """
        start_time = time.time_ns()
        txn_id = None
        use_lmdb = False
        
        try:
            # Check if LMDB should be used for this payload
            if self._lmdb_enabled and self._lmdb_exchange.should_use_lmdb(data):
                use_lmdb = True
                txn_id = f"{start_time}"
                
                # Store input in LMDB
                input_key = self._lmdb_exchange.put_input(data, txn_id)
                
                if input_key:
                    # Create LMDB reference for Rust
                    lmdb_ref = {
                        '_lmdb_ref': True,
                        '_lmdb_input_key': input_key,
                        '_lmdb_output_key': self._lmdb_exchange.generate_key(
                            self._lmdb_exchange.config.output_key_prefix, txn_id
                        ),
                        '_lmdb_db_path': self._lmdb_exchange.config.db_path,
                        '_lmdb_db_name': self._lmdb_exchange.config.db_name,
                        '_lmdb_format': self._lmdb_exchange.config.data_format.value,
                        '_txn_id': txn_id
                    }
                    
                    # Call Rust with LMDB reference
                    result = dict(self._rust_instance.calculate(lmdb_ref))
                    self._stats['lmdb_exchanges'] += 1
                else:
                    # Fallback to direct pass
                    use_lmdb = False
                    result = dict(self._rust_instance.calculate(data))
            else:
                # Direct call for small payloads
                result = dict(self._rust_instance.calculate(data))
            
            # If LMDB was used, check for output in LMDB
            if use_lmdb and txn_id:
                lmdb_result = self._lmdb_exchange.get_output(txn_id, wait=True)
                if lmdb_result is not None:
                    result = lmdb_result
                # Cleanup LMDB data
                self._lmdb_exchange.cleanup(txn_id)
            
            # Update stats
            elapsed_ns = time.time_ns() - start_time
            self._stats['calculations'] += 1
            self._stats['total_time_ns'] += elapsed_ns
            
            return result
            
        except Exception as e:
            # Cleanup on error
            if use_lmdb and txn_id and self._lmdb_exchange:
                try:
                    self._lmdb_exchange.cleanup(txn_id)
                except:
                    pass
            logger.error(f"RustCalculator '{self.name}' calculate error: {e}")
            raise
    
    def details(self) -> Dict[str, Any]:
        """
        Get calculator details.
        
        Returns:
            Dictionary containing calculator metadata and statistics
        """
        try:
            details = dict(self._rust_instance.details())
        except Exception:
            details = {}
        
        # Add Python-side details
        avg_time_ns = (self._stats['total_time_ns'] / self._stats['calculations']
                       if self._stats['calculations'] > 0 else 0)
        
        details.update({
            'wrapper': 'RustCalculator',
            'name': self.name,
            'language': 'Rust',
            'binding': 'PyO3',
            'rust_module': self.config.get('rust_module', 'dishtayantra_rust'),
            'rust_class': self.config.get('rust_class'),
            'lmdb_enabled': self._lmdb_enabled,
            'stats': {
                'calculations': self._stats['calculations'],
                'avg_time_ns': round(avg_time_ns, 2),
                'lmdb_exchanges': self._stats['lmdb_exchanges']
            }
        })
        
        return details
    
    def is_lmdb_enabled(self) -> bool:
        """Check if LMDB transport is enabled"""
        return self._lmdb_enabled


def is_rust_module_available(module_name: str = 'dishtayantra_rust') -> bool:
    """
    Check if a Rust module is available for import.
    
    Args:
        module_name: Name of the compiled module
        
    Returns:
        True if module can be imported, False otherwise
    """
    try:
        importlib.import_module(module_name)
        return True
    except ImportError:
        return False


def get_rust_module_info(module_name: str = 'dishtayantra_rust') -> Optional[Dict[str, Any]]:
    """
    Get information about a Rust module.
    
    Args:
        module_name: Name of the compiled module
        
    Returns:
        Dictionary with module info, or None if not available
    """
    try:
        module = importlib.import_module(module_name)
        return {
            'name': module_name,
            'file': getattr(module, '__file__', 'unknown'),
            'doc': getattr(module, '__doc__', ''),
            'available_classes': [
                name for name in dir(module)
                if not name.startswith('_') and isinstance(getattr(module, name), type)
            ]
        }
    except ImportError:
        return None
