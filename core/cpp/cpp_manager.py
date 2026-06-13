"""
C++ Calculator Manager for DishtaYantra
Version: 1.7.0

Manages C++ pybind11 modules and calculator instances.
Provides centralized configuration and lifecycle management for C++ calculators.

Features:
- Config-based C++ module loading from cpp_config.json
- Calculator instance caching and pooling
- LMDB zero-copy data exchange support
- Automatic build support (CMake + pybind11)
- Shared across main process and worker processes

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

import os
import sys
import json
import time
import logging
import threading
import importlib
import importlib.util
import subprocess
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


# v2.2 module split: module lifecycle ops live in cpp_module_ops.py.
from core.cpp.cpp_module_ops import CPPModuleOpsMixin  # noqa: F401


@dataclass
class ModuleConfig:
    """Configuration for a C++ pybind11 module"""
    name: str
    enabled: bool = True
    path: Optional[str] = None
    description: str = ""
    
    # Build settings
    source_dir: str = "cpp/src"
    build_dir: str = "cpp/build"
    cmake_options: List[str] = field(default_factory=list)
    
    # LMDB settings
    lmdb_enabled: bool = False
    lmdb_db_path: str = "data/cpp_lmdb"
    lmdb_db_name: str = "cpp_exchange"
    lmdb_map_size_mb: int = 256
    lmdb_min_payload_size: int = 10240


@dataclass 
class CalculatorDefinition:
    """Definition of a pre-configured C++ calculator"""
    name: str
    module: str
    cpp_class: str
    description: str = ""
    default_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ModuleStatus:
    """Status of a loaded C++ module"""
    name: str
    loaded: bool = False
    path: Optional[str] = None
    available_classes: List[str] = field(default_factory=list)
    load_time: Optional[datetime] = None
    last_error: Optional[str] = None
    calculator_count: int = 0


class CPPManager(CPPModuleOpsMixin):
    """
    Central manager for C++ pybind11 modules and calculators.
    
    This class handles:
    - Loading configuration from cpp_config.json
    - Loading/unloading C++ modules
    - Managing calculator instances
    - LMDB transport configuration
    - Build automation (optional)
    
    Usage:
        cpp_manager = CPPManager.get_instance()
        cpp_manager.initialize()
        
        # Create a calculator instance
        calc = cpp_manager.create_calculator("MathCalculator", "my_calc", {"operation": "sum"})
        
        # Execute calculation
        result = calc.calculate({"values": [1, 2, 3]})
    """
    
    _instance: Optional['CPPManager'] = None
    _lock = threading.Lock()
    
    @classmethod
    def get_instance(cls) -> 'CPPManager':
        """Get the singleton instance of CPPManager"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    @classmethod
    def reset_instance(cls):
        """Reset the singleton instance (for testing)"""
        with cls._lock:
            if cls._instance is not None:
                cls._instance.shutdown()
            cls._instance = None
    
    def __init__(self, config_path: str = None):
        """
        Initialize the CPP Manager.
        
        Args:
            config_path: Path to cpp_config.json (uses default if None)
        """
        self.config_path = config_path
        self.config: Dict[str, Any] = {}
        self.module_configs: Dict[str, ModuleConfig] = {}
        self.calculator_definitions: Dict[str, CalculatorDefinition] = {}
        
        # Runtime state
        self.loaded_modules: Dict[str, Any] = {}  # module_name -> module object
        self.module_status: Dict[str, ModuleStatus] = {}
        self.calculator_cache: Dict[str, Any] = {}  # cache_key -> calculator instance
        
        # Thread safety
        self._module_lock = threading.Lock()
        self._calculator_lock = threading.Lock()
        
        # Manager settings
        self._initialized = False
        self._enabled = False
        self._auto_load = False
        self._config_hash: Optional[str] = None
        self._config_monitor_thread: Optional[threading.Thread] = None
        self._stop_monitor = threading.Event()
        
        # Statistics
        self._stats = {
            'modules_loaded': 0,
            'calculators_created': 0,
            'total_calculations': 0,
            'cache_hits': 0
        }
        
        logger.info("CPP Manager created")
    
    def initialize(self, config_path: str = None) -> bool:
        """
        Initialize the CPP Manager from configuration.
        
        Args:
            config_path: Path to cpp_config.json (optional)
            
        Returns:
            True if initialization successful
        """
        if self._initialized:
            logger.warning("CPP Manager already initialized")
            return True
        
        # Find config path
        if config_path:
            self.config_path = config_path
        elif not self.config_path:
            # Default path relative to application
            self.config_path = self._find_config_path()
        
        if not self.config_path or not os.path.exists(self.config_path):
            logger.warning(f"CPP config not found at {self.config_path}")
            return False
        
        try:
            logger.info(f"Initializing CPP Manager from {self.config_path}")
            
            # Load configuration
            with open(self.config_path, 'r') as f:
                self.config = json.load(f)
            
            # Store config hash for change detection
            self._config_hash = self._compute_config_hash()
            
            # Parse manager settings
            manager_config = self.config.get('cpp_manager', {})
            self._enabled = manager_config.get('enabled', True)
            self._auto_load = manager_config.get('auto_load_on_startup', True)
            
            if not self._enabled:
                logger.info("CPP Manager is disabled in configuration")
                return True
            
            # Parse module configurations
            self._parse_module_configs()
            
            # Parse calculator definitions
            self._parse_calculator_definitions()
            
            # Auto-load modules if configured
            if self._auto_load:
                self._load_all_modules()
            
            # Start config monitor
            config_check_interval = manager_config.get('config_check_interval_seconds', 600)
            if config_check_interval > 0:
                self._start_config_monitor(config_check_interval)
            
            self._initialized = True
            logger.info(f"CPP Manager initialized: {len(self.module_configs)} modules, "
                       f"{len(self.calculator_definitions)} calculator definitions")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize CPP Manager: {e}")
            return False
    
    def _find_config_path(self) -> Optional[str]:
        """Find the cpp_config.json file"""
        search_paths = [
            'config/cpp_config.json',
            '../config/cpp_config.json',
            os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'cpp_config.json'),
        ]
        
        for path in search_paths:
            abs_path = os.path.abspath(path)
            if os.path.exists(abs_path):
                return abs_path
        
        return None
    
    def _compute_config_hash(self) -> str:
        """Compute hash of config file for change detection"""
        import hashlib
        with open(self.config_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    
    def _parse_module_configs(self):
        """Parse module configurations from config"""
        modules = self.config.get('modules', [])
        
        for mod_cfg in modules:
            config = ModuleConfig(
                name=mod_cfg.get('name'),
                enabled=mod_cfg.get('enabled', True),
                path=mod_cfg.get('path'),
                description=mod_cfg.get('description', ''),
                source_dir=mod_cfg.get('build', {}).get('source_dir', 'cpp/src'),
                build_dir=mod_cfg.get('build', {}).get('build_dir', 'cpp/build'),
                cmake_options=mod_cfg.get('build', {}).get('cmake_options', []),
                lmdb_enabled=mod_cfg.get('lmdb', {}).get('enabled', False),
                lmdb_db_path=mod_cfg.get('lmdb', {}).get('db_path', 'data/cpp_lmdb'),
                lmdb_db_name=mod_cfg.get('lmdb', {}).get('db_name', 'cpp_exchange'),
                lmdb_map_size_mb=mod_cfg.get('lmdb', {}).get('map_size_mb', 256),
                lmdb_min_payload_size=mod_cfg.get('lmdb', {}).get('min_payload_size', 10240)
            )
            
            self.module_configs[config.name] = config
            self.module_status[config.name] = ModuleStatus(name=config.name)
            
            logger.debug(f"Parsed module config: {config.name}")
    
    def _parse_calculator_definitions(self):
        """Parse calculator definitions from config"""
        calculators = self.config.get('calculators', [])
        
        for calc_cfg in calculators:
            definition = CalculatorDefinition(
                name=calc_cfg.get('name'),
                module=calc_cfg.get('module', 'dishtayantra_cpp'),
                cpp_class=calc_cfg.get('cpp_class'),
                description=calc_cfg.get('description', ''),
                default_config=calc_cfg.get('default_config', {})
            )
            
            self.calculator_definitions[definition.name] = definition
            logger.debug(f"Parsed calculator definition: {definition.name}")
    
    def create_calculator(self, 
                         calculator_name: str,
                         instance_name: str = None,
                         config: Dict[str, Any] = None,
                         use_cache: bool = True) -> Any:
        """
        Create a C++ calculator instance.
        
        Args:
            calculator_name: Name of the calculator definition
            instance_name: Optional instance name (defaults to calculator_name)
            config: Optional config override (merged with defaults)
            use_cache: Whether to use cached instances
            
        Returns:
            Calculator instance
        """
        definition = self.calculator_definitions.get(calculator_name)
        if not definition:
            raise ValueError(f"Unknown calculator: {calculator_name}")
        
        instance_name = instance_name or calculator_name
        
        # Build cache key
        cache_key = f"{calculator_name}:{instance_name}:{hash(json.dumps(config or {}, sort_keys=True))}"
        
        # Check cache
        if use_cache:
            with self._calculator_lock:
                if cache_key in self.calculator_cache:
                    self._stats['cache_hits'] += 1
                    return self.calculator_cache[cache_key]
        
        # Ensure module is loaded
        if not self.is_module_loaded(definition.module):
            if not self.load_module(definition.module):
                raise RuntimeError(f"Failed to load module {definition.module} for calculator {calculator_name}")
        
        module = self.loaded_modules[definition.module]
        
        # Get calculator class
        if not hasattr(module, definition.cpp_class):
            raise AttributeError(f"Class '{definition.cpp_class}' not found in module '{definition.module}'")
        
        calculator_class = getattr(module, definition.cpp_class)
        
        # Merge config
        final_config = dict(definition.default_config)
        if config:
            final_config.update(config)
        
        # Add module config for LMDB
        module_config = self.module_configs.get(definition.module)
        if module_config and module_config.lmdb_enabled:
            final_config['lmdb_enabled'] = True
            final_config['lmdb_db_path'] = module_config.lmdb_db_path
            final_config['lmdb_min_size'] = module_config.lmdb_min_payload_size
        
        # Create instance
        try:
            calculator = calculator_class(instance_name, final_config)
            calculator._module_name = definition.module  # Track for cleanup
            
            # Cache if enabled
            if use_cache:
                with self._calculator_lock:
                    self.calculator_cache[cache_key] = calculator
            
            self._stats['calculators_created'] += 1
            logger.debug(f"Created C++ calculator: {calculator_name} ({instance_name})")
            
            return calculator
            
        except Exception as e:
            logger.error(f"Failed to create calculator {calculator_name}: {e}")
            raise
    
    def get_calculator_class(self, calculator_name: str) -> Optional[type]:
        """
        Get the C++ class for a calculator definition.
        
        Args:
            calculator_name: Name of the calculator definition
            
        Returns:
            Calculator class or None
        """
        definition = self.calculator_definitions.get(calculator_name)
        if not definition:
            return None
        
        if not self.is_module_loaded(definition.module):
            if not self.load_module(definition.module):
                return None
        
        module = self.loaded_modules.get(definition.module)
        if module and hasattr(module, definition.cpp_class):
            return getattr(module, definition.cpp_class)
        
        return None
    
    def list_modules(self) -> List[Dict[str, Any]]:
        """List all configured modules with status"""
        result = []
        for name, config in self.module_configs.items():
            status = self.module_status.get(name, ModuleStatus(name=name))
            result.append({
                'name': name,
                'enabled': config.enabled,
                'description': config.description,
                'loaded': status.loaded,
                'path': status.path,
                'available_classes': status.available_classes,
                'calculator_count': status.calculator_count,
                'load_time': status.load_time.isoformat() if status.load_time else None,
                'last_error': status.last_error,
                'lmdb_enabled': config.lmdb_enabled
            })
        return result
    
    def list_calculators(self) -> List[Dict[str, Any]]:
        """List all calculator definitions"""
        result = []
        for name, definition in self.calculator_definitions.items():
            module_loaded = self.is_module_loaded(definition.module)
            result.append({
                'name': name,
                'module': definition.module,
                'cpp_class': definition.cpp_class,
                'description': definition.description,
                'default_config': definition.default_config,
                'available': module_loaded
            })
        return result
    
    def get_status(self) -> Dict[str, Any]:
        """Get overall CPP Manager status"""
        return {
            'initialized': self._initialized,
            'enabled': self._enabled,
            'config_path': self.config_path,
            'modules_configured': len(self.module_configs),
            'modules_loaded': len(self.loaded_modules),
            'calculators_defined': len(self.calculator_definitions),
            'calculators_cached': len(self.calculator_cache),
            'stats': dict(self._stats)
        }
    
    def get_config(self) -> Dict[str, Any]:
        """Get the current configuration"""
        return dict(self.config)
    
    def shutdown(self):
        """Shutdown the CPP Manager"""
        logger.info("Shutting down CPP Manager...")
        
        # Stop config monitor
        self._stop_monitor.set()
        if self._config_monitor_thread:
            self._config_monitor_thread.join(timeout=2)
        
        # Clear caches
        with self._calculator_lock:
            self.calculator_cache.clear()
        
        # Unload modules
        with self._module_lock:
            self.loaded_modules.clear()
        
        self._initialized = False
        logger.info("CPP Manager shutdown complete")


# Module-level convenience functions
_manager: Optional[CPPManager] = None


def get_cpp_manager() -> CPPManager:
    """Get the global CPP Manager instance"""
    global _manager
    if _manager is None:
        _manager = CPPManager.get_instance()
    return _manager


def initialize_cpp_manager(config_path: str = None) -> bool:
    """Initialize the global CPP Manager"""
    return get_cpp_manager().initialize(config_path)


def create_cpp_calculator(calculator_name: str, 
                         instance_name: str = None,
                         config: Dict[str, Any] = None) -> Any:
    """Create a C++ calculator using the global manager"""
    return get_cpp_manager().create_calculator(calculator_name, instance_name, config)


def is_cpp_available() -> bool:
    """Check if any C++ modules are available"""
    manager = get_cpp_manager()
    return manager._initialized and len(manager.loaded_modules) > 0
