"""
Rust Manager for DishtaYantra
Version: 1.7.0

Centralized management of Rust PyO3 modules and calculators.

v1.7.0: Initial implementation with config-based module loading,
        calculator definitions, LMDB support, and hot-reload.

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

import os
import sys
import json
import time
import hashlib
import logging
import threading
import importlib
import subprocess
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class ModuleStatus(Enum):
    """Status of a Rust module"""
    NOT_LOADED = "not_loaded"
    LOADING = "loading"
    LOADED = "loaded"
    ERROR = "error"
    BUILDING = "building"


@dataclass
class CalculatorDefinition:
    """Definition of a Rust calculator from config"""
    name: str
    module: str
    rust_class: str
    description: str
    default_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ModuleConfig:
    """Configuration for a Rust module"""
    name: str
    enabled: bool
    path: Optional[str]
    description: str
    build_config: Dict[str, Any] = field(default_factory=dict)
    lmdb_config: Dict[str, Any] = field(default_factory=dict)


class RustManager:
    """
    Centralized manager for Rust PyO3 modules and calculators.
    
    Features:
    - Config-based module loading from rust_config.json
    - Automatic initialization on startup
    - Calculator instance caching
    - LMDB zero-copy data exchange support
    - Hot-reload when config changes
    - Build automation via maturin/cargo
    
    Usage:
        from core.rust import get_rust_manager, create_rust_calculator
        
        # Get manager singleton
        manager = get_rust_manager()
        
        # Create calculator by name
        calc = create_rust_calculator("RustMathCalculator", "my_calc", {"operation": "sum"})
        
        # Execute calculation
        result = calc.calculate({"values": [1.0, 2.0, 3.0]})
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._config_path: Optional[str] = None
        self._config: Dict[str, Any] = {}
        self._config_hash: str = ""
        
        # Module tracking
        self.module_configs: Dict[str, ModuleConfig] = {}
        self.loaded_modules: Dict[str, Any] = {}
        self.module_status: Dict[str, ModuleStatus] = {}
        self.module_errors: Dict[str, str] = {}
        
        # Calculator tracking
        self.calculator_definitions: Dict[str, CalculatorDefinition] = {}
        self.calculator_cache: Dict[str, Any] = {}
        
        # Statistics
        self.stats = {
            "modules_loaded": 0,
            "calculators_created": 0,
            "cache_hits": 0,
            "total_calculations": 0,
            "last_config_reload": None
        }
        
        # Config monitor thread
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_monitor = threading.Event()
        
        self._initialized = False
    
    def initialize(self, config_path: str = "config/rust_config.json") -> bool:
        """
        Initialize the Rust Manager with configuration.
        
        Args:
            config_path: Path to rust_config.json
            
        Returns:
            True if initialization succeeded
        """
        self._config_path = config_path
        
        if not os.path.exists(config_path):
            logger.warning(f"Rust config not found: {config_path}")
            return False
        
        try:
            self._load_config()
            self._parse_modules()
            self._parse_calculators()
            
            # Auto-load modules if configured
            manager_config = self._config.get("rust_manager", {})
            if manager_config.get("auto_load_on_startup", True):
                self._auto_load_modules()
            
            # Start config monitor
            check_interval = manager_config.get("config_check_interval_seconds", 600)
            if check_interval > 0:
                self._start_config_monitor(check_interval)
            
            self._initialized = True
            logger.info(f"RustManager initialized: {len(self.module_configs)} modules, "
                       f"{len(self.calculator_definitions)} calculators defined")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize RustManager: {e}")
            return False
    
    def _load_config(self):
        """Load configuration from JSON file"""
        with open(self._config_path, 'r') as f:
            self._config = json.load(f)
        
        # Calculate config hash for change detection
        config_str = json.dumps(self._config, sort_keys=True)
        self._config_hash = hashlib.md5(config_str.encode()).hexdigest()
        self.stats["last_config_reload"] = time.time()
    
    def _parse_modules(self):
        """Parse module configurations from config"""
        self.module_configs.clear()
        
        for module_data in self._config.get("modules", []):
            name = module_data.get("name")
            if not name:
                continue
                
            config = ModuleConfig(
                name=name,
                enabled=module_data.get("enabled", True),
                path=module_data.get("path"),
                description=module_data.get("description", ""),
                build_config=module_data.get("build", {}),
                lmdb_config=module_data.get("lmdb", {})
            )
            
            self.module_configs[name] = config
            self.module_status[name] = ModuleStatus.NOT_LOADED
            
            logger.debug(f"Registered Rust module config: {name}")
    
    def _parse_calculators(self):
        """Parse calculator definitions from config"""
        self.calculator_definitions.clear()
        
        for calc_data in self._config.get("calculators", []):
            name = calc_data.get("name")
            if not name:
                continue
                
            definition = CalculatorDefinition(
                name=name,
                module=calc_data.get("module", "dishtayantra_rust"),
                rust_class=calc_data.get("rust_class", name),
                description=calc_data.get("description", ""),
                default_config=calc_data.get("default_config", {})
            )
            
            self.calculator_definitions[name] = definition
            logger.debug(f"Registered Rust calculator definition: {name}")
    
    def _auto_load_modules(self):
        """Automatically load enabled modules"""
        for name, config in self.module_configs.items():
            if config.enabled:
                try:
                    self.load_module(name)
                except Exception as e:
                    logger.warning(f"Failed to auto-load Rust module '{name}': {e}")
    
    def load_module(self, module_name: str) -> bool:
        """
        Load a Rust module.
        
        Args:
            module_name: Name of the module to load
            
        Returns:
            True if module loaded successfully
        """
        if module_name not in self.module_configs:
            logger.error(f"Unknown Rust module: {module_name}")
            return False
        
        config = self.module_configs[module_name]
        if not config.enabled:
            logger.info(f"Rust module '{module_name}' is disabled")
            return False
        
        self.module_status[module_name] = ModuleStatus.LOADING
        
        try:
            # Import the module
            if config.path:
                # Custom path - add to sys.path if needed
                module_dir = os.path.dirname(config.path)
                if module_dir and module_dir not in sys.path:
                    sys.path.insert(0, module_dir)
            
            module = importlib.import_module(module_name)
            
            # Verify it's a valid Rust/PyO3 module
            if not hasattr(module, '__name__'):
                raise ImportError(f"Invalid module: {module_name}")
            
            self.loaded_modules[module_name] = module
            self.module_status[module_name] = ModuleStatus.LOADED
            self.module_errors.pop(module_name, None)
            self.stats["modules_loaded"] = len(self.loaded_modules)
            
            # Get available calculator classes
            available_classes = []
            for calc_def in self.calculator_definitions.values():
                if calc_def.module == module_name:
                    if hasattr(module, calc_def.rust_class):
                        available_classes.append(calc_def.rust_class)
            
            logger.info(f"Loaded Rust module '{module_name}' with classes: {available_classes}")
            return True
            
        except ImportError as e:
            self.module_status[module_name] = ModuleStatus.ERROR
            self.module_errors[module_name] = str(e)
            logger.warning(f"Failed to import Rust module '{module_name}': {e}")
            return False
        except Exception as e:
            self.module_status[module_name] = ModuleStatus.ERROR
            self.module_errors[module_name] = str(e)
            logger.error(f"Error loading Rust module '{module_name}': {e}")
            return False
    
    def unload_module(self, module_name: str) -> bool:
        """
        Unload a Rust module.
        
        Args:
            module_name: Name of the module to unload
            
        Returns:
            True if module unloaded successfully
        """
        if module_name not in self.loaded_modules:
            return False
        
        try:
            # Clear calculator cache for this module
            to_remove = [k for k, v in self.calculator_cache.items() 
                        if k.startswith(f"{module_name}:")]
            for key in to_remove:
                del self.calculator_cache[key]
            
            # Remove from loaded modules
            del self.loaded_modules[module_name]
            self.module_status[module_name] = ModuleStatus.NOT_LOADED
            self.stats["modules_loaded"] = len(self.loaded_modules)
            
            logger.info(f"Unloaded Rust module '{module_name}'")
            return True
            
        except Exception as e:
            logger.error(f"Error unloading Rust module '{module_name}': {e}")
            return False
    
    def reload_module(self, module_name: str) -> bool:
        """Reload a Rust module"""
        self.unload_module(module_name)
        
        # Force reimport
        if module_name in sys.modules:
            del sys.modules[module_name]
        
        return self.load_module(module_name)
    
    def build_module(self, module_name: str) -> Tuple[bool, str]:
        """
        Build a Rust module using maturin/cargo.
        
        Args:
            module_name: Name of the module to build
            
        Returns:
            Tuple of (success, output/error message)
        """
        if module_name not in self.module_configs:
            return False, f"Unknown module: {module_name}"
        
        config = self.module_configs[module_name]
        build_config = config.build_config
        
        if not build_config:
            return False, "No build configuration for this module"
        
        source_dir = build_config.get("source_dir", "rust")
        
        if not os.path.exists(source_dir):
            return False, f"Source directory not found: {source_dir}"
        
        self.module_status[module_name] = ModuleStatus.BUILDING
        
        try:
            # Try maturin first (preferred for PyO3)
            cargo_options = build_config.get("cargo_options", ["--release"])
            
            # Check if maturin is available
            result = subprocess.run(["which", "maturin"], capture_output=True)
            use_maturin = result.returncode == 0
            
            if use_maturin:
                cmd = ["maturin", "develop"] + cargo_options
            else:
                cmd = ["cargo", "build"] + cargo_options
            
            logger.info(f"Building Rust module '{module_name}': {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                cwd=source_dir,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                self.module_status[module_name] = ModuleStatus.NOT_LOADED
                output = result.stdout or "Build completed successfully"
                logger.info(f"Built Rust module '{module_name}'")
                return True, output
            else:
                self.module_status[module_name] = ModuleStatus.ERROR
                error = result.stderr or result.stdout or "Build failed"
                self.module_errors[module_name] = error
                logger.error(f"Build failed for '{module_name}': {error}")
                return False, error
                
        except subprocess.TimeoutExpired:
            self.module_status[module_name] = ModuleStatus.ERROR
            return False, "Build timed out after 5 minutes"
        except FileNotFoundError as e:
            self.module_status[module_name] = ModuleStatus.ERROR
            return False, f"Build tool not found: {e}"
        except Exception as e:
            self.module_status[module_name] = ModuleStatus.ERROR
            return False, f"Build error: {e}"
    
    def create_calculator(self, calculator_name: str, instance_name: str = None,
                         config_override: Dict[str, Any] = None) -> Any:
        """
        Create a calculator instance by name.
        
        Args:
            calculator_name: Name of the calculator from config
            instance_name: Optional instance name (for caching)
            config_override: Optional config overrides
            
        Returns:
            Calculator instance
        """
        if calculator_name not in self.calculator_definitions:
            raise ValueError(f"Unknown calculator: {calculator_name}")
        
        definition = self.calculator_definitions[calculator_name]
        instance_name = instance_name or calculator_name
        
        # Check cache
        cache_key = f"{calculator_name}:{instance_name}:{hash(str(config_override))}"
        if cache_key in self.calculator_cache:
            self.stats["cache_hits"] += 1
            return self.calculator_cache[cache_key]
        
        # Ensure module is loaded
        module_name = definition.module
        if module_name not in self.loaded_modules:
            if not self.load_module(module_name):
                raise RuntimeError(f"Failed to load module: {module_name}")
        
        module = self.loaded_modules[module_name]
        
        # Get calculator class
        if not hasattr(module, definition.rust_class):
            raise AttributeError(
                f"Calculator class '{definition.rust_class}' not found in module '{module_name}'"
            )
        
        calculator_class = getattr(module, definition.rust_class)
        
        # Merge default config with override
        final_config = {**definition.default_config}
        if config_override:
            final_config.update(config_override)
        
        # Create instance
        try:
            calculator = calculator_class(instance_name, final_config)
            
            # Cache if enabled
            perf_config = self._config.get("performance", {})
            if perf_config.get("cache_calculator_instances", True):
                self.calculator_cache[cache_key] = calculator
            
            self.stats["calculators_created"] += 1
            logger.debug(f"Created Rust calculator: {calculator_name} as {instance_name}")
            
            return calculator
            
        except Exception as e:
            logger.error(f"Failed to create calculator '{calculator_name}': {e}")
            raise
    
    def get_calculator_class(self, calculator_name: str) -> Optional[type]:
        """Get the calculator class without creating an instance"""
        if calculator_name not in self.calculator_definitions:
            return None
        
        definition = self.calculator_definitions[calculator_name]
        module_name = definition.module
        
        if module_name not in self.loaded_modules:
            return None
        
        module = self.loaded_modules[module_name]
        return getattr(module, definition.rust_class, None)
    
    def list_modules(self) -> List[Dict[str, Any]]:
        """List all configured modules with their status"""
        modules = []
        for name, config in self.module_configs.items():
            modules.append({
                "name": name,
                "enabled": config.enabled,
                "description": config.description,
                "loaded": name in self.loaded_modules,
                "status": self.module_status.get(name, ModuleStatus.NOT_LOADED).value,
                "error": self.module_errors.get(name),
                "lmdb_enabled": config.lmdb_config.get("enabled", False)
            })
        return modules
    
    def list_calculators(self) -> List[Dict[str, Any]]:
        """List all calculator definitions with availability"""
        calculators = []
        for name, definition in self.calculator_definitions.items():
            module_loaded = definition.module in self.loaded_modules
            class_available = False
            
            if module_loaded:
                module = self.loaded_modules[definition.module]
                class_available = hasattr(module, definition.rust_class)
            
            calculators.append({
                "name": name,
                "module": definition.module,
                "rust_class": definition.rust_class,
                "description": definition.description,
                "default_config": definition.default_config,
                "available": module_loaded and class_available,
                "module_loaded": module_loaded
            })
        return calculators
    
    def get_status(self) -> Dict[str, Any]:
        """Get overall manager status"""
        return {
            "initialized": self._initialized,
            "config_path": self._config_path,
            "modules_configured": len(self.module_configs),
            "modules_loaded": len(self.loaded_modules),
            "calculators_defined": len(self.calculator_definitions),
            "calculators_cached": len(self.calculator_cache),
            "stats": self.stats.copy()
        }
    
    def get_module_info(self, module_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a module"""
        if module_name not in self.module_configs:
            return None
        
        config = self.module_configs[module_name]
        module = self.loaded_modules.get(module_name)
        
        info = {
            "name": module_name,
            "enabled": config.enabled,
            "description": config.description,
            "status": self.module_status.get(module_name, ModuleStatus.NOT_LOADED).value,
            "error": self.module_errors.get(module_name),
            "loaded": module is not None,
            "path": config.path,
            "build_config": config.build_config,
            "lmdb_config": config.lmdb_config
        }
        
        if module:
            info["module_path"] = getattr(module, "__file__", None)
            info["available_classes"] = [
                calc.rust_class for calc in self.calculator_definitions.values()
                if calc.module == module_name and hasattr(module, calc.rust_class)
            ]
        
        return info
    
    def get_calculator_info(self, calculator_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a calculator"""
        if calculator_name not in self.calculator_definitions:
            return None
        
        definition = self.calculator_definitions[calculator_name]
        module_loaded = definition.module in self.loaded_modules
        class_available = False
        
        if module_loaded:
            module = self.loaded_modules[definition.module]
            class_available = hasattr(module, definition.rust_class)
        
        return {
            "name": calculator_name,
            "module": definition.module,
            "rust_class": definition.rust_class,
            "description": definition.description,
            "default_config": definition.default_config,
            "available": module_loaded and class_available,
            "module_loaded": module_loaded,
            "instances_cached": sum(1 for k in self.calculator_cache.keys() 
                                   if k.startswith(f"{calculator_name}:"))
        }
    
    def _start_config_monitor(self, interval: int):
        """Start background thread to monitor config changes"""
        def monitor():
            while not self._stop_monitor.wait(interval):
                try:
                    self._check_config_changes()
                except Exception as e:
                    logger.error(f"Config monitor error: {e}")
        
        self._monitor_thread = threading.Thread(target=monitor, daemon=True)
        self._monitor_thread.start()
        logger.debug(f"Started config monitor (interval: {interval}s)")
    
    def _check_config_changes(self):
        """Check if config file has changed and reload if needed"""
        if not self._config_path or not os.path.exists(self._config_path):
            return
        
        try:
            with open(self._config_path, 'r') as f:
                new_config = json.load(f)
            
            config_str = json.dumps(new_config, sort_keys=True)
            new_hash = hashlib.md5(config_str.encode()).hexdigest()
            
            if new_hash != self._config_hash:
                logger.info("Rust config changed, reloading...")
                self._reload_config()
                
        except Exception as e:
            logger.error(f"Error checking config changes: {e}")
    
    def _reload_config(self):
        """Reload configuration and update modules/calculators"""
        try:
            old_modules = set(self.loaded_modules.keys())
            
            self._load_config()
            self._parse_modules()
            self._parse_calculators()
            
            # Reload changed modules
            new_modules = set(self.module_configs.keys())
            
            # Unload removed modules
            for module_name in old_modules - new_modules:
                self.unload_module(module_name)
            
            # Reload existing modules
            for module_name in old_modules & new_modules:
                if self.module_configs[module_name].enabled:
                    self.reload_module(module_name)
            
            # Load new modules
            manager_config = self._config.get("rust_manager", {})
            if manager_config.get("auto_load_on_startup", True):
                for module_name in new_modules - old_modules:
                    if self.module_configs[module_name].enabled:
                        self.load_module(module_name)
            
            logger.info("Rust config reloaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to reload config: {e}")
    
    def shutdown(self):
        """Shutdown the manager"""
        self._stop_monitor.set()
        
        for module_name in list(self.loaded_modules.keys()):
            self.unload_module(module_name)
        
        self.calculator_cache.clear()
        logger.info("RustManager shutdown complete")
    
    def is_initialized(self) -> bool:
        """Check if manager is initialized"""
        return self._initialized


# Module-level functions for convenience
_manager: Optional[RustManager] = None


def get_rust_manager() -> RustManager:
    """Get the RustManager singleton"""
    global _manager
    if _manager is None:
        _manager = RustManager()
    return _manager


def initialize_rust_manager(config_path: str = "config/rust_config.json") -> bool:
    """Initialize the RustManager with config"""
    manager = get_rust_manager()
    return manager.initialize(config_path)


def create_rust_calculator(calculator_name: str, instance_name: str = None,
                          config_override: Dict[str, Any] = None) -> Any:
    """
    Create a Rust calculator by name.
    
    Args:
        calculator_name: Name of the calculator from rust_config.json
        instance_name: Optional instance name
        config_override: Optional config overrides
        
    Returns:
        Calculator instance ready for use
    """
    manager = get_rust_manager()
    return manager.create_calculator(calculator_name, instance_name, config_override)


def is_rust_available() -> bool:
    """Check if any Rust modules are loaded"""
    manager = get_rust_manager()
    return len(manager.loaded_modules) > 0
