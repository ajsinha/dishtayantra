import copy
import logging
from abc import ABC, abstractmethod
from datetime import datetime

logger = logging.getLogger(__name__)


class DataTransformer(ABC):
    """Abstract base class for data transformers"""

    def __init__(self, name, config):
        self.name = name
        self.config = config
        self._transform_count = 0
        self._last_transform = None

    @abstractmethod
    def transform(self, data):
        """Transform and return result"""
        pass

    def details(self):
        """Return details in JSON format"""
        return {
            'name': self.name,
            'type': self.__class__.__name__,
            'transform_count': self._transform_count,
            'last_transform': self._last_transform
        }


class NullDataTransformer(DataTransformer):
    """Returns deep copy of supplied dictionary"""

    def transform(self, data):
        self._transform_count += 1
        self._last_transform = datetime.now().isoformat()
        return copy.deepcopy(data)


class PassthruDataTransformer(DataTransformer):
    """Returns supplied dictionary as-is"""

    def transform(self, data):
        self._transform_count += 1
        self._last_transform = datetime.now().isoformat()
        return data


class AttributeFilterAwayDataTransformer(DataTransformer):
    """Filters out specified attributes"""

    def transform(self, data):
        self._transform_count += 1
        self._last_transform = datetime.now().isoformat()

        filter_attrs = self.config.get('filter_attributes', [])
        result = copy.deepcopy(data)

        for attr in filter_attrs:
            result.pop(attr, None)

        return result


class AttributeFilterDataTransformer(DataTransformer):
    """Keeps only specified attributes"""

    def transform(self, data):
        self._transform_count += 1
        self._last_transform = datetime.now().isoformat()

        keep_attrs = self.config.get('keep_attributes', [])
        result = {k: copy.deepcopy(v) for k, v in data.items() if k in keep_attrs}

        return result


class ApplyDefaultsDataTransformer(DataTransformer):
    """Applies default values for missing or None attributes"""

    def transform(self, data):
        self._transform_count += 1
        self._last_transform = datetime.now().isoformat()

        defaults = self.config.get('defaults', {})
        result = copy.deepcopy(data)

        for key, default_value in defaults.items():
            if key not in result or result[key] is None:
                result[key] = default_value

        return result