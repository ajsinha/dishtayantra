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

The Rust struct must implement:
- #[new] fn new(name: String, config: &PyDict) -> Self
- fn calculate(&mut self, py: Python, data: &PyDict) -> PyResult<PyObject>
- fn details(&self, py: Python) -> PyResult<PyObject>
"""

import importlib
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


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
    
    Example usage:
    
    ```python
    config = {
        'rust_module': 'dishtayantra_rust',
        'rust_class': 'MathCalculator',
        'operation': 'sum',
        'arguments': ['a', 'b', 'c']
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
    
    def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute calculation on the Rust side.
        
        Args:
            data: Input data dictionary
            
        Returns:
            Output data dictionary from Rust calculation
        """
        try:
            result = self._rust_instance.calculate(data)
            return dict(result)
        except Exception as e:
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
        details.update({
            'wrapper': 'RustCalculator',
            'name': self.name,
            'language': 'Rust',
            'binding': 'PyO3',
            'rust_module': self.config.get('rust_module', 'dishtayantra_rust'),
            'rust_class': self.config.get('rust_class')
        })
        
        return details


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
