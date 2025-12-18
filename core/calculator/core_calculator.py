import copy
import logging
import importlib
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Protocol, runtime_checkable, Optional, Type

logger = logging.getLogger(__name__)

class DataCalculatorLike(Protocol):
    def __init__(self, name, config: Dict[str, Any]):
        ...

    def calculate(self, data: Any) -> Any:
        ... # The '...' indicates an abstract/required method

    def details(self) -> Dict[str, Any]:
        ...


class DataCalculator(ABC):
    """Abstract base class for data calculators"""

    def __init__(self, name, config: Dict[str,Any]):
        self.name = name
        self.config = config
        self._calculation_count = 0
        self._last_calculation = None

    @abstractmethod
    def calculate(self, data):
        """Calculate and return result"""
        pass

    def details(self) -> Dict[str, Any]:
        """Return details in JSON format"""
        return {
            'name': self.name,
            'type': self.__class__.__name__,
            'calculation_count': self._calculation_count,
            'last_calculation': self._last_calculation
        }


class CalculatorFactory:
    """
    Factory for creating calculator instances.
    
    Supports:
    - Built-in Python calculators (NullCalculator, PassthruCalculator, etc.)
    - Custom Python calculators (via python_class config)
    - Java calculators via Py4J (via java_class config)
    - C++ calculators via pybind11 (via cpp_class config)
    - Rust calculators via PyO3 (via rust_class config)
    
    Configuration Examples:
    
    1. Built-in calculator:
       {"calculator": "PassthruCalculator"}
    
    2. Custom Python calculator:
       {"calculator": "custom", "python_class": "mymodule.MyCalculator"}
    
    3. Java calculator:
       {"calculator": "java", "java_class": "com.example.MyCalculator"}
    
    4. C++ calculator:
       {"calculator": "cpp", "cpp_class": "MathCalculator", "cpp_module": "dishtayantra_cpp"}
    
    5. Rust calculator:
       {"calculator": "rust", "rust_class": "MathCalculator", "rust_module": "dishtayantra_rust"}
    """
    
    # Registry of built-in calculators
    _builtin_calculators: Dict[str, Type[DataCalculator]] = {}
    
    # Cache of instantiated calculators
    _instances: Dict[str, DataCalculatorLike] = {}
    
    @classmethod
    def register_builtin(cls, name: str, calculator_class: Type[DataCalculator]):
        """Register a built-in calculator class."""
        cls._builtin_calculators[name] = calculator_class
    
    @classmethod
    def create(cls, name: str, config: Dict[str, Any]) -> DataCalculatorLike:
        """
        Create a calculator instance based on configuration.
        
        Args:
            name: Unique name for this calculator instance
            config: Calculator configuration, must include:
                    - calculator: Calculator type name or 'java', 'cpp', 'rust', 'custom'
                    - java_class: (for Java) Fully qualified Java class name
                    - cpp_class: (for C++) C++ class name in the module
                    - rust_class: (for Rust) Rust struct name in the module
                    - python_class: (for custom) Fully qualified Python class name
                    
        Returns:
            Calculator instance implementing DataCalculatorLike
        """
        calculator_type = config.get('calculator', 'PassthruCalculator')
        
        # Check if this is a Java calculator
        if calculator_type.lower() == 'java' or 'java_class' in config:
            return cls._create_java_calculator(name, config)
        
        # Check if this is a C++ calculator
        if calculator_type.lower() == 'cpp' or 'cpp_class' in config:
            return cls._create_cpp_calculator(name, config)
        
        # Check if this is a Rust calculator
        if calculator_type.lower() == 'rust' or 'rust_class' in config:
            return cls._create_rust_calculator(name, config)
        
        # Check if this is a REST calculator
        if calculator_type.lower() == 'rest' or 'endpoint' in config:
            return cls._create_rest_calculator(name, config)
        
        # Check if this is a custom Python calculator
        if calculator_type.lower() == 'custom' or 'python_class' in config:
            return cls._create_custom_python_calculator(name, config)
        
        # Built-in calculator
        return cls._create_builtin_calculator(name, calculator_type, config)
    
    @classmethod
    def _create_java_calculator(cls, name: str, config: Dict[str, Any]) -> DataCalculatorLike:
        """Create a Java calculator via Py4J."""
        try:
            from core.calculator.java_calculator import JavaCalculator, PY4J_AVAILABLE
            
            if not PY4J_AVAILABLE:
                raise RuntimeError(
                    "Py4J is not installed. Install with: pip install py4j\n"
                    "Then start the Java Gateway server."
                )
            
            java_class = config.get('java_class')
            if not java_class:
                raise ValueError("java_class must be specified for Java calculators")
            
            logger.info(f"Creating Java calculator '{name}' for class {java_class}")
            return JavaCalculator(name, config)
            
        except ImportError as e:
            raise RuntimeError(f"Failed to import Java calculator module: {e}")
    
    @classmethod
    def _create_cpp_calculator(cls, name: str, config: Dict[str, Any]) -> DataCalculatorLike:
        """Create a C++ calculator via pybind11."""
        try:
            from core.calculator.cpp_calculator import CppCalculator
            
            cpp_class = config.get('cpp_class')
            if not cpp_class:
                raise ValueError("cpp_class must be specified for C++ calculators")
            
            cpp_module = config.get('cpp_module', 'dishtayantra_cpp')
            logger.info(f"Creating C++ calculator '{name}' for class {cpp_module}.{cpp_class}")
            return CppCalculator(name, config)
            
        except ImportError as e:
            raise RuntimeError(f"Failed to import C++ calculator module: {e}")
    
    @classmethod
    def _create_rust_calculator(cls, name: str, config: Dict[str, Any]) -> DataCalculatorLike:
        """Create a Rust calculator via PyO3."""
        try:
            from core.calculator.rust_calculator import RustCalculator
            
            rust_class = config.get('rust_class')
            if not rust_class:
                raise ValueError("rust_class must be specified for Rust calculators")
            
            rust_module = config.get('rust_module', 'dishtayantra_rust')
            logger.info(f"Creating Rust calculator '{name}' for class {rust_module}.{rust_class}")
            return RustCalculator(name, config)
            
        except ImportError as e:
            raise RuntimeError(f"Failed to import Rust calculator module: {e}")
    
    @classmethod
    def _create_rest_calculator(cls, name: str, config: Dict[str, Any]) -> DataCalculatorLike:
        """Create a REST API calculator."""
        try:
            from core.calculator.rest_calculator import RestCalculator, REQUESTS_AVAILABLE
            
            if not REQUESTS_AVAILABLE:
                raise RuntimeError(
                    "requests library is not installed. Install with: pip install requests"
                )
            
            endpoint = config.get('endpoint') or config.get('url')
            if not endpoint:
                raise ValueError("endpoint URL must be specified for REST calculators")
            
            logger.info(f"Creating REST calculator '{name}' for endpoint {endpoint}")
            return RestCalculator(name, config)
            
        except ImportError as e:
            raise RuntimeError(f"Failed to import REST calculator module: {e}")
    
    @classmethod
    def _create_custom_python_calculator(cls, name: str, config: Dict[str, Any]) -> DataCalculatorLike:
        """Create a custom Python calculator from a module path."""
        python_class = config.get('python_class')
        if not python_class:
            raise ValueError("python_class must be specified for custom calculators")
        
        try:
            # Split module and class name
            module_path, class_name = python_class.rsplit('.', 1)
            module = importlib.import_module(module_path)
            calculator_class = getattr(module, class_name)
            
            logger.info(f"Creating custom Python calculator '{name}' from {python_class}")
            return calculator_class(name, config)
            
        except (ImportError, AttributeError) as e:
            raise RuntimeError(f"Failed to load custom calculator {python_class}: {e}")
    
    @classmethod
    def _create_builtin_calculator(cls, name: str, calculator_type: str, 
                                   config: Dict[str, Any]) -> DataCalculator:
        """Create a built-in calculator."""
        if calculator_type not in cls._builtin_calculators:
            available = ', '.join(cls._builtin_calculators.keys())
            raise ValueError(
                f"Unknown calculator type: {calculator_type}. "
                f"Available: {available}"
            )
        
        calculator_class = cls._builtin_calculators[calculator_type]
        return calculator_class(name, config)
    
    @classmethod
    def get_available_calculators(cls) -> Dict[str, str]:
        """Get list of available calculator types."""
        result = {name: calc.__doc__ or '' for name, calc in cls._builtin_calculators.items()}
        result['java'] = 'Java calculator via Py4J (requires java_class config)'
        result['cpp'] = 'C++ calculator via pybind11 (requires cpp_class config)'
        result['rust'] = 'Rust calculator via PyO3 (requires rust_class config)'
        result['rest'] = 'REST API calculator via HTTP POST (requires endpoint config)'
        result['custom'] = 'Custom Python calculator (requires python_class config)'
        return result


class NullCalculator(DataCalculator):
    """Returns deep copy of supplied dictionary"""

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()
        return copy.deepcopy(data)


class PassthruCalculator(DataCalculator):
    """Returns supplied dictionary as-is"""

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()
        return data


class AttributeFilterAwayCalculator(DataCalculator):
    """Filters out specified attributes"""

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()

        filter_attrs = self.config.get('filter_attributes', [])
        result = copy.deepcopy(data)

        for attr in filter_attrs:
            result.pop(attr, None)

        return result


class AttributeFilterCalculator(DataCalculator):
    """Keeps only specified attributes"""

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()

        keep_attrs = self.config.get('keep_attributes', [])
        result = {k: copy.deepcopy(v) for k, v in data.items() if k in keep_attrs}

        return result


class ApplyDefaultsCalculator(DataCalculator):
    """Applies default values for missing or None attributes"""

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()

        defaults = self.config.get('defaults', {})
        result = copy.deepcopy(data)

        for key, default_value in defaults.items():
            if key not in result or result[key] is None:
                result[key] = default_value

        return result


class AdditionCalculator(DataCalculator):
    """Adds values of specified attributes"""

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()

        arguments = self.config.get('arguments', [])
        output_attr = self.config.get('output_attribute', 'result')

        total = 0
        for arg in arguments:
            if arg in data:
                try:
                    total += float(data[arg])
                except (ValueError, TypeError):
                    logger.warning(f"Cannot convert {arg} value to number")

        return {output_attr: total}


class MultiplicationCalculator(DataCalculator):
    """Multiplies values of specified attributes"""

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()

        arguments = self.config.get('arguments', [])
        output_attr = self.config.get('output_attribute', 'result')

        product = 1
        for arg in arguments:
            if arg in data:
                try:
                    product *= float(data[arg])
                except (ValueError, TypeError):
                    logger.warning(f"Cannot convert {arg} value to number")

        return {output_attr: product}


class AttributeNameChangeCalculator(DataCalculator):
    """Changes attribute names based on mapping"""

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()

        name_mapping = self.config.get('name_mapping', {})
        result = copy.deepcopy(data)

        for old_name, new_name in name_mapping.items():
            if old_name in result:
                result[new_name] = result.pop(old_name)

        return result


# Register all built-in calculators
CalculatorFactory.register_builtin('NullCalculator', NullCalculator)
CalculatorFactory.register_builtin('PassthruCalculator', PassthruCalculator)
CalculatorFactory.register_builtin('AttributeFilterAwayCalculator', AttributeFilterAwayCalculator)
CalculatorFactory.register_builtin('AttributeFilterCalculator', AttributeFilterCalculator)
CalculatorFactory.register_builtin('ApplyDefaultsCalculator', ApplyDefaultsCalculator)
CalculatorFactory.register_builtin('AdditionCalculator', AdditionCalculator)
CalculatorFactory.register_builtin('MultiplicationCalculator', MultiplicationCalculator)
CalculatorFactory.register_builtin('AttributeNameChangeCalculator', AttributeNameChangeCalculator)