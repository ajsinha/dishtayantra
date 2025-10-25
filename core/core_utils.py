import importlib
import logging
import re
import threading

logger = logging.getLogger(__name__)

class SingletonMeta(type):
    # Dictionary to store instances of classes
    _instances = {}
    _lock = threading.Lock()

    # Override the __call__ method of the metaclass
    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                # If not, create a new instance and store it in _instances dictionary
                cls._instances[cls] = super().__call__(*args, **kwargs)


        # Return the existing instance if already instantiated
        return cls._instances[cls]

def validate_name(name):
    """Validate that name contains only alphanumeric and underscore with at least one alphabetic character"""
    if not re.match(r'^(?=.*[a-zA-Z])[a-zA-Z0-9_]+$', name):
        raise ValueError(
            f"Invalid name: {name}. Must contain only alphanumeric and underscore with at least one alphabetic character")
    return True

def instantiate_module(module_path, class_name, config):
    """
    Instantiate a module given its full path, class name and config dictionary

    Args:
        module_path: Full path of the module (e.g., 'core.calculator.core_calculator')
        class_name: Name of the class to instantiate
        config: Configuration dictionary to pass to the constructor

    Returns:
        Instance of the specified class
    """
    try:
        logger.info(f"Attempting to instantiate {class_name} from {module_path}")
        module = importlib.import_module(module_path)
        class_obj = getattr(module, class_name)
        instance = class_obj(**config)
        logger.info(f"Successfully instantiated {class_name}")
        return instance
    except Exception as e:
        logger.error(f"Error instantiating {class_name} from {module_path}: {str(e)}")
        raise