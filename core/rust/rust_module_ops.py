"""
Rust Module Operations Mixin (v2.2 module split)
================================================

Module load/unload/reload, maturin/cargo build, and the config-file
monitor for RustManager, extracted verbatim from rust_manager.py to
respect the 500-line architecture limit. All state lives on the manager.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

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
    """Status of a Rust module."""
    NOT_LOADED = "not_loaded"
    LOADING = "loading"
    LOADED = "loaded"
    ERROR = "error"
    BUILDING = "building"


class RustModuleOpsMixin:
    """PyO3 module lifecycle + maturin/cargo build + config monitoring."""

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
    
