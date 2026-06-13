"""
JVM Manager Types (v2.2 module split)
=====================================

GatewayConfig / CalculatorDefinition / GatewayStatus dataclasses, extracted
verbatim from jvm_manager.py to respect the 500-line architecture limit.
Re-exported from core.jvm.jvm_manager.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

# v2.0.0 BUGFIX: lazy annotations so the module imports even when py4j
# is absent (legacy "JavaGateway" annotations defeated the graceful-
# degradation try/except and made core.jvm unimportable).
from __future__ import annotations

import os
import sys
import json
import time
import logging
import subprocess
import threading
import signal
import atexit
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# Py4J imports
try:
    from py4j.java_gateway import JavaGateway, GatewayParameters
    from py4j.protocol import Py4JNetworkError, Py4JJavaError
    PY4J_AVAILABLE = True
except ImportError:
    PY4J_AVAILABLE = False
    logger.warning("Py4J not installed. Java calculators will not be available.")


@dataclass
class GatewayConfig:
    """Configuration for a single Py4J gateway"""
    name: str
    enabled: bool = True
    host: str = "localhost"
    base_port: int = 25333
    pool_size: int = 4
    
    # JVM settings
    jvm_auto_start: bool = True
    java_home: Optional[str] = None
    heap_size_mb: int = 512
    max_heap_size_mb: int = 2048
    jvm_options: List[str] = field(default_factory=list)
    classpath: List[str] = field(default_factory=list)
    
    # Connection settings
    timeout_seconds: int = 30
    retry_attempts: int = 3
    retry_delay_seconds: int = 2
    keep_alive: bool = True


@dataclass
class CalculatorDefinition:
    """Definition of a pre-configured Java calculator"""
    name: str
    java_class: str
    description: str = ""
    gateway: str = "primary"
    default_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GatewayStatus:
    """Status of a gateway connection"""
    name: str
    connected: bool = False
    jvm_process_pid: Optional[int] = None
    jvm_started: bool = False
    active_connections: int = 0
    total_requests: int = 0
    errors: int = 0
    last_health_check: Optional[datetime] = None
    start_time: Optional[datetime] = None


