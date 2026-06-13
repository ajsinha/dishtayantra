"""
PropertiesConfigurator Accessor & Resolution Mixins (v2.2 module split)
=======================================================================

Typed getters (int/float/bool/lists), pattern queries, and the
string/file/JSON content-resolution engine for PropertiesConfigurator,
extracted verbatim from properties_configurator.py to respect the
500-line architecture limit. All state lives on the configurator.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

"""
Properties configurator for managing application properties with auto-reload
Enhanced with command-line args, precedence ordering, and source tracking
"""
import json
import os
import re
import sys
import threading
import time
from typing import Optional, List, Union, Dict, Any
from pathlib import Path




class TypedAccessorsMixin:
    """Typed getters, list getters, and pattern queries."""

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

    def get_bool(self, key: str, default_value: Optional[bool] = None) -> Optional[bool]:
        """
        Get a property value as boolean

        Recognizes the following as True (case-insensitive):
        - true, yes, on, 1, y, t

        Recognizes the following as False (case-insensitive):
        - false, no, off, 0, n, f

        Args:
            key: Property key
            default_value: Default value if key not found or cannot be converted

        Returns:
            Property value as bool or default_value
        """
        value = self.get(key)
        if value is None:
            return default_value

        # Handle boolean values
        if isinstance(value, bool):
            return value

        # Handle string values (case-insensitive)
        if isinstance(value, str):
            value_lower = value.lower().strip()

            # True values
            if value_lower in ('true', 'yes', 'on', '1', 'y', 't'):
                return True

            # False values
            if value_lower in ('false', 'no', 'off', '0', 'n', 'f'):
                return False

        # If value is numeric, use Python's bool conversion
        try:
            num_value = float(value)
            return bool(num_value)
        except (ValueError, TypeError):
            pass

        # Cannot convert, return default
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

    def get_values_by_pattern(self, pattern: str) -> List[str]:
        """
        Get all property values whose keys match the given regex pattern

        Args:
            pattern: Regex pattern to match against property keys

        Returns:
            List of property values for matching keys (empty list if no matches)
        """
        with self._properties_lock:
            try:
                regex = re.compile(pattern)
                matching_values = []

                for key, value in self._properties.items():
                    if regex.match(key):
                        matching_values.append(value)

                return matching_values
            except re.error as e:
                print(f"Invalid regex pattern '{pattern}': {e}")
                return []

    def get_properties_by_pattern(self, pattern: str) -> Dict[str, str]:
        """
        Get all properties (key-value pairs) whose keys match the given regex pattern

        Args:
            pattern: Regex pattern to match against property keys

        Returns:
            Dictionary of matching properties (empty dict if no matches)
        """
        with self._properties_lock:
            try:
                regex = re.compile(pattern)
                matching_properties = {}

                for key, value in self._properties.items():
                    if regex.match(key):
                        matching_properties[key] = value

                return matching_properties
            except re.error as e:
                print(f"Invalid regex pattern '{pattern}': {e}")
                return {}

    def get_all_properties(self) -> Dict[str, str]:
        """
        Get all properties

        Returns:
            Dictionary of all properties
        """
        with self._properties_lock:
            return self._properties.copy()

    def get_all_sources(self) -> Dict[str, str]:
        """
        Get sources for all properties

        Returns:
            Dictionary mapping property keys to their sources
        """
        with self._properties_lock:
            return self._property_sources.copy()



class ContentResolutionMixin:
    """${prop} resolution inside arbitrary strings, files, and JSON."""

    def resolve_string_content(self, content: str) -> str:
        """
        Resolve ${prop_name} patterns in the given string content.
        Uses the same prioritization logic: commandline > env > file.
        Supports nested patterns and multiple placeholders.
        If a pattern cannot be resolved, it is left as-is.

        Args:
            content: String content with potential ${...} patterns

        Returns:
            Resolved string with all ${...} patterns replaced by property values
        """
        if not content or '${' not in content:
            return content

        with self._properties_lock:
            # Use the existing _resolve_value method with current properties
            # Pass empty visited set for circular reference detection
            return self._resolve_value(content, self._properties, set())

    def load_and_resolve_file_content(self, filename: Union[str, Path]) -> List[str]:
        """
        Load a text file and resolve ${prop_name} patterns in each line.
        Uses the same prioritization logic: commandline > env > file.

        Args:
            filename: Path to the text file to load and resolve

        Returns:
            List of strings representing the resolved file content (one string per line)

        Raises:
            FileNotFoundError: If the file does not exist
            IOError: If there's an error reading the file
        """
        file_path = Path(filename) if not isinstance(filename, Path) else filename

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            resolved_lines = []
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    # Don't strip the line to preserve formatting/whitespace
                    # But remove the trailing newline
                    line = line.rstrip('\n\r')
                    resolved_line = self.resolve_string_content(line)
                    resolved_lines.append(resolved_line)

            return resolved_lines

        except Exception as e:
            raise IOError(f"Error reading file {file_path}: {e}")

    def resolve_string_json_content(self, content: str) -> Dict[str, Any]:
        """
        Resolve ${prop_name} patterns in a JSON string and return as dictionary.
        Uses the same prioritization logic: commandline > env > file.

        Args:
            content: JSON string content with potential ${...} patterns

        Returns:
            Dictionary object parsed from the resolved JSON string

        Raises:
            json.JSONDecodeError: If the resolved content is not valid JSON
        """
        resolved_content = self.resolve_string_content(content)

        try:
            return json.loads(resolved_content)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Error parsing resolved JSON content: {e.msg}",
                e.doc,
                e.pos
            )

    def load_and_resolve_json_file_content(self, filename: Union[str, Path]) -> Dict[str, Any]:
        """
        Load a JSON file, resolve ${prop_name} patterns, and return as dictionary.
        Uses the same prioritization logic: commandline > env > file.

        Args:
            filename: Path to the JSON file to load and resolve

        Returns:
            Dictionary object parsed from the resolved JSON file

        Raises:
            FileNotFoundError: If the file does not exist
            IOError: If there's an error reading the file
            json.JSONDecodeError: If the resolved content is not valid JSON
        """
        file_path = Path(filename) if not isinstance(filename, Path) else filename

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            return self.resolve_string_json_content(content)

        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Error parsing JSON from file {file_path}: {e.msg}",
                e.doc,
                e.pos
            )
        except Exception as e:
            raise IOError(f"Error reading file {file_path}: {e}")

