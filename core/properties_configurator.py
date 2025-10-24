"""
Properties configurator for managing application properties with auto-reload
"""
import os
import re
import threading
import time
from typing import Optional, List, Union, Dict, Any
from pathlib import Path


class PropertiesConfigurator:
    """
    Singleton thread-safe class for managing properties from configuration files.
    Supports property value resolution with ${...} patterns and auto-reload.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, properties_files: List[str] = None, reload_interval: int = 300):
        """
        Initialize the PropertiesConfigurator

        Args:
            properties_files: List of property file paths
            reload_interval: Interval in seconds for auto-reload (default: 300 seconds = 5 minutes)
        """
        if hasattr(self, '_initialized'):
            return

        self._initialized = True
        self._properties_files = properties_files or []
        self._reload_interval = reload_interval
        self._properties: Dict[str, str] = {}
        self._properties_lock = threading.RLock()
        self._stop_reload = threading.Event()
        self._file_timestamps: Dict[str, float] = {}

        # Initial load
        self._load_properties()

        # Start auto-reload thread
        self._reload_thread = threading.Thread(target=self._auto_reload_worker, daemon=True)
        self._reload_thread.start()

    def _load_properties(self):
        """Load properties from all configured files"""
        with self._properties_lock:
            new_properties = {}

            for file_path in self._properties_files:
                if not os.path.exists(file_path):
                    continue

                # Track file modification time
                self._file_timestamps[file_path] = os.path.getmtime(file_path)

                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        for line_num, line in enumerate(f, 1):
                            line = line.strip()

                            # Skip empty lines and comments
                            if not line or line.startswith('#') or line.startswith('//'):
                                continue

                            # Split by first '=' only
                            if '=' not in line:
                                continue

                            key, value = line.split('=', 1)
                            key = key.strip()
                            value = value.strip()

                            if key:
                                new_properties[key] = value

                except Exception as e:
                    print(f"Error loading properties from {file_path}: {e}")

            # Resolve all property references
            self._properties = self._resolve_all_properties(new_properties)

    def _resolve_all_properties(self, properties: Dict[str, str]) -> Dict[str, str]:
        """Resolve all ${...} references in properties"""
        resolved = {}

        for key, value in properties.items():
            resolved[key] = self._resolve_value(value, properties, set())

        return resolved

    def _resolve_value(self, value: str, properties: Dict[str, str], visited: set) -> str:
        """
        Recursively resolve ${...} references in a value, including nested references

        Args:
            value: The value to resolve
            properties: Dictionary of all properties
            visited: Set of keys already visited (to prevent circular references)

        Returns:
            Resolved value
        """
        if not value or '${' not in value:
            return value

        max_iterations = 100  # Prevent infinite loops
        iteration = 0

        while '${' in value and iteration < max_iterations:
            iteration += 1

            # Find innermost ${...} pattern
            pattern = r'\$\{([^{}]+)\}'
            matches = list(re.finditer(pattern, value))

            if not matches:
                # Handle nested patterns like ${x${y}}
                nested_pattern = r'\$\{([^}]*\$\{[^}]*\}[^}]*)\}'
                nested_matches = list(re.finditer(nested_pattern, value))

                if nested_matches:
                    # Process innermost references first
                    for match in reversed(nested_matches):
                        inner_ref = match.group(1)
                        resolved_inner = self._resolve_value(inner_ref, properties, visited)
                        value = value[:match.start()] + '${' + resolved_inner + '}' + value[match.end():]
                    continue
                else:
                    break

            # Replace all simple ${key} references
            for match in reversed(matches):
                ref_key = match.group(1)

                # Check for circular reference
                if ref_key in visited:
                    replacement = match.group(0)  # Keep original if circular
                else:
                    # First check environment variables
                    replacement = os.environ.get(ref_key)

                    # If not in env, check properties
                    if replacement is None:
                        replacement = properties.get(ref_key, match.group(0))

                        # Recursively resolve the replacement
                        if replacement != match.group(0):
                            new_visited = visited.copy()
                            new_visited.add(ref_key)
                            replacement = self._resolve_value(replacement, properties, new_visited)

                value = value[:match.start()] + replacement + value[match.end():]

        return value

    def _auto_reload_worker(self):
        """Worker thread for auto-reloading properties"""
        while not self._stop_reload.wait(self._reload_interval):
            try:
                # Check if any files have been modified
                needs_reload = False

                for file_path in self._properties_files:
                    if os.path.exists(file_path):
                        current_mtime = os.path.getmtime(file_path)
                        if file_path not in self._file_timestamps or \
                                self._file_timestamps[file_path] < current_mtime:
                            needs_reload = True
                            break

                if needs_reload:
                    self._load_properties()

            except Exception as e:
                print(f"Error in auto-reload: {e}")

    def get(self, key: str, default_value: Optional[str] = None) -> Optional[str]:
        """
        Get a property value by key

        Args:
            key: Property key
            default_value: Default value if key not found

        Returns:
            Property value or default_value
        """
        with self._properties_lock:
            return self._properties.get(key, default_value)

    def get_int(self, key: str, default_value: Optional[int] = None) -> Optional[int]:
        """
        Get a property value as integer

        Args:
            key: Property key
            default_value: Default value if key not found

        Returns:
            Property value as int or default_value
        """
        value = self.get(key)
        if value is None:
            return default_value

        try:
            return int(value)
        except (ValueError, TypeError):
            return default_value

    def get_float(self, key: str, default_value: Optional[float] = None) -> Optional[float]:
        """
        Get a property value as float

        Args:
            key: Property key
            default_value: Default value if key not found

        Returns:
            Property value as float or default_value
        """
        value = self.get(key)
        if value is None:
            return default_value

        try:
            return float(value)
        except (ValueError, TypeError):
            return default_value

    def get_list(self, key: str, delim: str = ',') -> Optional[List[str]]:
        """
        Get a property value as list by splitting with delimiter

        Args:
            key: Property key
            delim: Delimiter for splitting (default: ',')

        Returns:
            List of strings or None
        """
        value = self.get(key)
        if value is None:
            return None

        return [item.strip() for item in value.split(delim) if item.strip()]

    def get_int_list(self, key: str, delim: str = ',') -> Optional[List[int]]:
        """
        Get a property value as list of integers

        Args:
            key: Property key
            delim: Delimiter for splitting (default: ',')

        Returns:
            List of integers or None
        """
        str_list = self.get_list(key, delim)
        if str_list is None:
            return None

        result = []
        for item in str_list:
            try:
                result.append(int(item))
            except (ValueError, TypeError):
                continue  # Skip invalid integers

        return result if result else None

    def get_float_list(self, key: str, delim: str = ',') -> Optional[List[float]]:
        """
        Get a property value as list of floats

        Args:
            key: Property key
            delim: Delimiter for splitting (default: ',')

        Returns:
            List of floats or None
        """
        str_list = self.get_list(key, delim)
        if str_list is None:
            return None

        result = []
        for item in str_list:
            try:
                result.append(float(item))
            except (ValueError, TypeError):
                continue  # Skip invalid floats

        return result if result else None

    def stop_reload(self):
        """Stop the auto-reload thread"""
        self._stop_reload.set()
        if hasattr(self, '_reload_thread'):
            self._reload_thread.join(timeout=5)