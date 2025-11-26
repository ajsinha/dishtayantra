# core/__init__.py
"""
DishtaYantra Compute Server - Core Module
"""
import json
import uuid
from typing import Union

import importlib

def instantiate_from_full_name(full_class_path: str, *args, **kwargs):
    """
    Imports a class by its fully qualified name and instantiates it.

    :param full_class_path: A string like "module.submodule.ClassName"
    :param args: Positional arguments for the class constructor (__init__)
    :param kwargs: Keyword arguments for the class constructor (__init__)
    :return: An instance of the specified class.
    """
    if '.' not in full_class_path:
        raise ValueError(f"Invalid full class path: {full_class_path}. Must contain module name.")

    # Split the path into the module part and the class name part
    module_name, class_name = full_class_path.rsplit('.', 1)

    try:
        # 1. Dynamically import the module
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise ImportError(f"Could not import module '{module_name}': {e}")

    try:
        # 2. Get the class object from the module using getattr
        class_object = getattr(module, class_name)
    except AttributeError:
        raise AttributeError(f"Class '{class_name}' not found in module '{module_name}'.")

    # 3. Instantiate the class with the provided arguments
    instance = class_object(*args, **kwargs)
    return instance

def generate_uuid(version: int = 4) -> Union[str, uuid.UUID]:
    """
    Generates a Universally Unique Identifier (UUID).

    Args:
        version (int): The UUID version to generate.
                       Commonly 1 (MAC address + time), 3 (name-based MD5),
                       4 (random), or 5 (name-based SHA-1).
                       Defaults to 4 (random).

    Returns:
        str or uuid.UUID: The generated UUID object, which is easily
                          converted to its standard string representation.
    """
    if version == 1:
        # Generates a UUID based on the host ID and current time.
        return uuid.uuid1()
    elif version == 3:
        # Generates a UUID based on the MD5 hash of a namespace identifier and a name.
        # Requires a namespace (e.g., uuid.NAMESPACE_DNS) and a name string.
        return uuid.uuid3(uuid.NAMESPACE_DNS, 'example.com')
    elif version == 4:
        # Generates a random UUID (most common).
        return uuid.uuid4()
    elif version == 5:
        # Generates a UUID based on the SHA-1 hash of a namespace identifier and a name.
        return uuid.uuid5(uuid.NAMESPACE_DNS, 'example.com')
    else:
        raise ValueError("Unsupported UUID version. Must be 1, 3, 4, or 5.")

# To get the UUID as a string immediately:
def generate_random_uuid_string() -> str:
    """Generates a UUID version 4 and returns its standard string representation."""
    return str(uuid.uuid4())

def flatten_dict_to_escaped_json(data):
    """
    Flattens a nested dictionary, converts it to a JSON string, and
    escapes double quotes within the resulting string.

    Args:
        data (dict): The dictionary to flatten.

    Returns:
        str: A JSON string of the flattened dictionary with escaped double quotes.
    """
    json_string = json.dumps(data)

    # --- 3. Escape Double Quotes (Beyond Standard JSON Escaping) ---
    # Standard json.dumps escapes internal quotes but sometimes
    # external systems require the *entire* string's quotes to be
    # escaped again (e.g., when embedding this JSON string inside another JSON field).
    # This replaces " with \"
    escaped_json_string = json_string.replace('"', '\\"')

    return escaped_json_string