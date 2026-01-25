"""
pybind11 C++ Calculator Integration for DishtaYantra
Version: 1.7.0

This module provides integration with C++ calculators compiled using pybind11.
C++ calculators offer:
- ~100ns call overhead (vs ~100-500μs for Py4J)
- Direct memory sharing (zero-copy possible)
- SIMD optimization opportunities
- Single .so/.pyd deployment
- LMDB memory-mapped zero-copy exchange for large payloads

The C++ class must implement:
- Constructor: ClassName(std::string name, py::dict config)
- Method: py::dict calculate(py::dict data)
- Method: py::dict details()

v1.7.0: Now uses CPP Manager for centralized module and calculator management.

Copyright © 2025 Ashutosh Sinha. All rights reserved.
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
    logger.debug("LMDB module not available for C++ calculator")

# CPP Manager support
try:
    from core.cpp import get_cpp_manager, is_cpp_available
    CPP_MANAGER_AVAILABLE = True
except ImportError:
    CPP_MANAGER_AVAILABLE = False
    logger.debug("CPP Manager not available")


class CppCalculator:
    """
    Wrapper for C++ calculators compiled with pybind11.
    
    In v1.7.0, calculators can be created in two ways:
    
    1. Using CPP Manager (recommended):
        ```python
        from core.cpp import create_cpp_calculator
        calc = create_cpp_calculator("MathCalculator", "my_calc", {"operation": "sum"})
        ```
    
    2. Direct instantiation (legacy):
        ```python
        config = {
            'cpp_module': 'dishtayantra_cpp',
            'cpp_class': 'MathCalculator',
            'operation': 'sum'
        }
        calc = CppCalculator('my_calc', config)
        ```
    
    The C++ calculator class must implement:
    
    ```cpp
    #include <pybind11/pybind11.h>
    #include <pybind11/stl.h>
    
    namespace py = pybind11;
    
    class MyCalculator {
    public:
        MyCalculator(const std::string& name, const py::dict& config);
        py::dict calculate(const py::dict& data);
        py::dict details();
    };
    ```
    
    For LMDB-enabled calculators, the C++ side can access data via memory-mapped
    files when receiving an '_lmdb_ref' in the input dict.
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize C++ calculator wrapper.
        
        Args:
            name: Calculator name
            config: Configuration dictionary, must include:
                    - cpp_module: Name of the compiled module (e.g., 'dishtayantra_cpp')
                    - cpp_class: Name of the C++ class (e.g., 'MathCalculator')
                    OR
                    - calculator_name: Name of a calculator defined in cpp_config.json
                    
                    Optional:
                    - lmdb_enabled: Enable LMDB zero-copy transport
                    - lmdb_db_path: LMDB database path
                    - lmdb_min_size: Min payload size to use LMDB
        """
        self.name = name
        self.config = config
        self._cpp_instance = None
        
        # Check if using CPP Manager
        calculator_name = config.get('calculator_name')
        
        if calculator_name and CPP_MANAGER_AVAILABLE:
            # Use CPP Manager to create calculator
            try:
                manager = get_cpp_manager()
                if not manager._initialized:
                    manager.initialize()
                
                # Get the calculator instance from manager
                self._cpp_instance = manager.create_calculator(
                    calculator_name,
                    name,
                    config
                )
                
                # Store module info
                definition = manager.calculator_definitions.get(calculator_name)
                if definition:
                    self._cpp_module = definition.module
                    self._cpp_class = definition.cpp_class
                
                logger.info(f"CppCalculator '{name}' created via CPP Manager: {calculator_name}")
                
            except Exception as e:
                logger.warning(f"Failed to use CPP Manager for {calculator_name}: {e}")
                # Fall through to direct initialization
                calculator_name = None
        
        if not calculator_name or self._cpp_instance is None:
            # Direct initialization (legacy mode)
            cpp_module = config.get('cpp_module', 'dishtayantra_cpp')
            cpp_class = config.get('cpp_class')
            
            if not cpp_class:
                raise ValueError("cpp_class must be specified for C++ calculators")
            
            self._cpp_module = cpp_module
            self._cpp_class = cpp_class
            
            try:
                # Import the compiled module
                module = importlib.import_module(cpp_module)
                calculator_class = getattr(module, cpp_class)
                
                # Create C++ calculator instance
                self._cpp_instance = calculator_class(name, config)
                
                logger.info(f"CppCalculator '{name}' initialized (direct): {cpp_module}.{cpp_class}")
                
            except ImportError as e:
                raise RuntimeError(
                    f"Failed to import C++ module '{cpp_module}': {e}\n"
                    f"Ensure the module is compiled and in Python's path.\n"
                    f"Build with: cd cpp && mkdir build && cd build && cmake .. && make"
                )
            except AttributeError as e:
                raise RuntimeError(f"Class '{cpp_class}' not found in module '{cpp_module}': {e}")
            except Exception as e:
                raise RuntimeError(f"Failed to initialize C++ calculator: {e}")
        
        # LMDB support
        self._lmdb_exchange: Optional[LMDBDataExchange] = None
        self._lmdb_enabled = False
        if LMDB_AVAILABLE and config.get('lmdb_enabled', False):
            lmdb_config = LMDBCalculatorConfig.from_dict(config)
            self._lmdb_exchange = LMDBDataExchange(lmdb_config, name)
            self._lmdb_enabled = self._lmdb_exchange.initialize()
            if self._lmdb_enabled:
                logger.info(f"CppCalculator '{name}' LMDB transport enabled at {lmdb_config.db_path}")
        
        # Statistics
        self._stats = {
            'calculations': 0,
            'total_time_ns': 0,
            'lmdb_exchanges': 0
        }
    
    def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute calculation on the C++ side.
        
        For large payloads (configurable, default 1KB+), uses LMDB memory-mapped
        files for zero-copy data exchange.
        
        Args:
            data: Input data dictionary
            
        Returns:
            Output data dictionary from C++ calculation
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
                    # Create LMDB reference for C++
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
                    
                    # Call C++ with LMDB reference
                    result = dict(self._cpp_instance.calculate(lmdb_ref))
                    self._stats['lmdb_exchanges'] += 1
                else:
                    # Fallback to direct pass
                    use_lmdb = False
                    result = dict(self._cpp_instance.calculate(data))
            else:
                # Direct call for small payloads
                result = dict(self._cpp_instance.calculate(data))
            
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
            logger.error(f"CppCalculator '{self.name}' calculate error: {e}")
            raise
    
    def details(self) -> Dict[str, Any]:
        """
        Get calculator details.
        
        Returns:
            Dictionary containing calculator metadata and statistics
        """
        try:
            details = dict(self._cpp_instance.details())
        except Exception:
            details = {}
        
        # Add Python-side details
        avg_time_ns = (self._stats['total_time_ns'] / self._stats['calculations']
                       if self._stats['calculations'] > 0 else 0)
        
        details.update({
            'wrapper': 'CppCalculator',
            'name': self.name,
            'language': 'C++',
            'binding': 'pybind11',
            'cpp_module': getattr(self, '_cpp_module', 'unknown'),
            'cpp_class': getattr(self, '_cpp_class', 'unknown'),
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


def is_cpp_module_available(module_name: str = 'dishtayantra_cpp') -> bool:
    """
    Check if a C++ module is available for import.
    
    Args:
        module_name: Name of the compiled module
        
    Returns:
        True if module can be imported, False otherwise
    """
    # First check via CPP Manager
    if CPP_MANAGER_AVAILABLE:
        manager = get_cpp_manager()
        if manager._initialized and module_name in manager.loaded_modules:
            return True
    
    # Direct import check
    try:
        importlib.import_module(module_name)
        return True
    except ImportError:
        return False


def get_cpp_module_info(module_name: str = 'dishtayantra_cpp') -> Optional[Dict[str, Any]]:
    """
    Get information about a C++ module.
    
    Args:
        module_name: Name of the compiled module
        
    Returns:
        Dictionary with module info, or None if not available
    """
    # First try CPP Manager
    if CPP_MANAGER_AVAILABLE:
        manager = get_cpp_manager()
        if manager._initialized:
            status = manager.module_status.get(module_name)
            if status and status.loaded:
                return {
                    'name': module_name,
                    'file': status.path,
                    'available_classes': status.available_classes,
                    'load_time': status.load_time.isoformat() if status.load_time else None
                }
    
    # Direct import
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


def list_available_cpp_calculators() -> list:
    """
    List all available C++ calculators from CPP Manager.
    
    Returns:
        List of calculator definitions
    """
    if CPP_MANAGER_AVAILABLE:
        manager = get_cpp_manager()
        if manager._initialized:
            return manager.list_calculators()
    return []
