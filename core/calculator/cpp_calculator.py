"""
pybind11 C++ Calculator Integration for DishtaYantra
====================================================

This module provides integration with C++ calculators compiled using pybind11.
C++ calculators offer:
- ~100ns call overhead (vs ~100-500μs for Py4J)
- Direct memory sharing (zero-copy possible)
- SIMD optimization opportunities
- Single .so/.pyd deployment

The C++ class must implement:
- Constructor: ClassName(std::string name, py::dict config)
- Method: py::dict calculate(py::dict data)
- Method: py::dict details()
"""

import importlib
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class CppCalculator:
    """
    Wrapper for C++ calculators compiled with pybind11.
    
    The C++ calculator class must implement the same interface as Python's
    DataCalculator:
    
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
    
    Example usage:
    
    ```python
    config = {
        'cpp_module': 'dishtayantra_cpp',
        'cpp_class': 'MathCalculator',
        'operation': 'sum',
        'arguments': ['a', 'b', 'c']
    }
    calc = CppCalculator('my_calc', config)
    result = calc.calculate({'a': 1.0, 'b': 2.0, 'c': 3.0})
    ```
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize C++ calculator wrapper.
        
        Args:
            name: Calculator name
            config: Configuration dictionary, must include:
                    - cpp_module: Name of the compiled module (e.g., 'dishtayantra_cpp')
                    - cpp_class: Name of the C++ class (e.g., 'MathCalculator')
                    Additional config options are passed to the C++ constructor.
        """
        self.name = name
        self.config = config
        
        cpp_module = config.get('cpp_module', 'dishtayantra_cpp')
        cpp_class = config.get('cpp_class')
        
        if not cpp_class:
            raise ValueError("cpp_class must be specified for C++ calculators")
        
        try:
            # Import the compiled module
            module = importlib.import_module(cpp_module)
            calculator_class = getattr(module, cpp_class)
            
            # Create C++ calculator instance
            # Pass the full config dict - C++ can extract what it needs
            self._cpp_instance = calculator_class(name, config)
            
            logger.info(f"CppCalculator '{name}' initialized: {cpp_module}.{cpp_class}")
            
        except ImportError as e:
            raise RuntimeError(
                f"Failed to import C++ module '{cpp_module}': {e}\n"
                f"Ensure the module is compiled and in Python's path.\n"
                f"Compile with: g++ -O3 -shared -std=c++17 -fPIC "
                f"$(python3 -m pybind11 --includes) your_module.cpp "
                f"-o {cpp_module}$(python3-config --extension-suffix)"
            )
        except AttributeError as e:
            raise RuntimeError(f"Class '{cpp_class}' not found in module '{cpp_module}': {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize C++ calculator: {e}")
    
    def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute calculation on the C++ side.
        
        Args:
            data: Input data dictionary
            
        Returns:
            Output data dictionary from C++ calculation
        """
        try:
            result = self._cpp_instance.calculate(data)
            return dict(result)
        except Exception as e:
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
        details.update({
            'wrapper': 'CppCalculator',
            'name': self.name,
            'language': 'C++',
            'binding': 'pybind11',
            'cpp_module': self.config.get('cpp_module', 'dishtayantra_cpp'),
            'cpp_class': self.config.get('cpp_class')
        })
        
        return details


def is_cpp_module_available(module_name: str = 'dishtayantra_cpp') -> bool:
    """
    Check if a C++ module is available for import.
    
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


def get_cpp_module_info(module_name: str = 'dishtayantra_cpp') -> Optional[Dict[str, Any]]:
    """
    Get information about a C++ module.
    
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
