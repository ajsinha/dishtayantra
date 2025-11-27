from copy import deepcopy
from datetime import datetime
from typing import Dict, Any


class RandomCalculator:

    def __init__(self, name, config: Dict[str, Any]):
        self.name = name
        self.config = config
        self._calculation_count = 0
        self._last_calculation = None

    def calculate(self, data: Any) -> Any:
        import random

        # Generates a random float between 0.0 and 1.0
        self._last_calculation = datetime.now().isoformat()
        random_float = random.random()
        copy_dict = deepcopy(data)
        self._calculation_count += 1
        copy_dict['random'] = random_float
        copy_dict['counter'] = self._calculation_count
        return copy_dict

    def details(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'type': self.__class__.__name__,
            'calculation_count': self._calculation_count,
            'last_calculation': self._last_calculation
        }