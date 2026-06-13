"""
Configuration File Parsers (v2.2)
=================================

Format-agnostic parsing for the configuration system. Each parser turns a
file into a flat ``{dotted.key: string_value}`` dictionary, which the
:class:`core.properties_configurator.PropertiesConfigurator` then runs
through its existing ``${...}`` resolution + precedence engine. Keeping the
parser separate from the engine means YAML and ``.properties`` files
produce byte-identical resolved configuration and every existing
``props.get("db.sqlite.path")`` call keeps working unchanged.

Supported formats (chosen by file extension):
    .yaml / .yml   -> YAML, flattened to dotted keys (Spring Boot style)
    .properties    -> classic line-based key=value
    (anything else -> treated as .properties)

Design notes:
    - YAML values are flattened to STRINGS so the single ``${...}``
      resolution path and the typed getters (get_int/get_bool/get_list)
      behave identically regardless of source format. ``port: 5002`` and
      ``port: "${PORT:5002}"`` therefore resolve the same way.
    - Lists in YAML are flattened two ways for convenience:
        servers:
          - a
          - b
      becomes both ``servers`` = "a,b" (so get_list works) AND
      ``servers.0`` = "a", ``servers.1`` = "b" (indexed access).
    - ``${VAR:default}`` env-var syntax is intentionally preserved (it is
      not YAML-standard) so it works across both formats.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import os
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


class ConfigParseError(Exception):
    """Raised when a configuration file cannot be parsed."""


def detect_format(file_path: str) -> str:
    """Return 'yaml' or 'properties' based on the file extension."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext in ('.yaml', '.yml'):
        return 'yaml'
    return 'properties'


def _scalar_to_str(value: Any) -> str:
    """Render a YAML scalar as the string the resolution engine expects.

    Booleans become lowercase 'true'/'false' to match how get_bool parses
    them; None becomes an empty string; everything else uses str().
    """
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if value is None:
        return ''
    return str(value)


def flatten_yaml(data: Any, prefix: str = '') -> Dict[str, str]:
    """
    Flatten a parsed YAML document into a flat dotted-key dict of strings.

    Mappings nest with dots (``a: {b: c}`` -> ``a.b`` = "c").
    Sequences produce BOTH a joined value at the key itself and indexed
    children:
        ``tags: [x, y]`` -> ``tags`` = "x,y", ``tags.0`` = "x",
        ``tags.1`` = "y".
    Sequences of mappings only produce indexed children (no join).
    """
    out: Dict[str, str] = {}

    if isinstance(data, dict):
        for key, value in data.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            out.update(flatten_yaml(value, child_prefix))
    elif isinstance(data, (list, tuple)):
        scalar_items = [item for item in data
                        if not isinstance(item, (dict, list, tuple))]
        # Joined form (only when every element is scalar) - enables get_list.
        if prefix and scalar_items and len(scalar_items) == len(data):
            out[prefix] = ','.join(_scalar_to_str(v) for v in data)
        # Indexed children for all elements.
        for index, item in enumerate(data):
            child_prefix = f"{prefix}.{index}" if prefix else str(index)
            if isinstance(item, (dict, list, tuple)):
                out.update(flatten_yaml(item, child_prefix))
            else:
                out[child_prefix] = _scalar_to_str(item)
    else:
        if prefix:
            out[prefix] = _scalar_to_str(data)
    return out


def parse_properties_text(text: str) -> Dict[str, str]:
    """Parse classic ``key=value`` properties text into a flat dict."""
    result: Dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#') or line.startswith('//'):
            continue
        if '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        if key:
            result[key] = value.strip()
    return result


def parse_yaml_text(text: str, source: str = '<yaml>') -> Dict[str, str]:
    """Parse YAML text into a flat dotted-key dict of strings."""
    if not YAML_AVAILABLE:
        raise ConfigParseError(
            f"Cannot parse YAML configuration '{source}': PyYAML is not "
            f"installed. Install it with 'pip install pyyaml' (it is a "
            f"required dependency - see requirements.txt).")
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigParseError(
            f"Invalid YAML in configuration '{source}': {exc}") from exc
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigParseError(
            f"Configuration '{source}' must have a mapping at its root, "
            f"got {type(data).__name__}.")
    return flatten_yaml(data)


def parse_config_file(file_path: str) -> Dict[str, str]:
    """
    Parse one configuration file into a flat dotted-key dict of strings,
    dispatching on the file extension. Raises ConfigParseError on failure
    (no silent fallback - a malformed config file is a fatal error).
    """
    fmt = detect_format(file_path)
    with open(file_path, 'r', encoding='utf-8') as handle:
        text = handle.read()
    if fmt == 'yaml':
        return parse_yaml_text(text, source=file_path)
    return parse_properties_text(text)


def find_default_config(config_dir: str = 'config') -> str:
    """
    Return the canonical application config file, preferring YAML.

    Looks for application.yaml, then application.yml, then
    application.properties under ``config_dir``. v2.2: YAML is the
    recommended format, but an existing .properties deployment keeps
    working with zero changes. Raises FileNotFoundError if none exist.
    """
    candidates = ['application.yaml', 'application.yml',
                  'application.properties']
    for name in candidates:
        path = os.path.join(config_dir, name)
        if os.path.exists(path):
            return path
    raise FileNotFoundError(
        f"No application configuration found in '{config_dir}'. Expected "
        f"one of: {', '.join(candidates)}.")
