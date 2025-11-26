import copy
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any,Protocol, runtime_checkable

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