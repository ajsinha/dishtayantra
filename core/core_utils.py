import importlib
import logging

logger = logging.getLogger(__name__)


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