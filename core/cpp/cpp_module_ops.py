"""
CPP Module Operations Mixin (v2.2 module split)
===============================================

Module load/unload/reload/build and the config-file monitor for
CPPManager, extracted verbatim from cpp_manager.py to respect the
500-line architecture limit. All state lives on the manager.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

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


@dataclass


class CPPModuleOpsMixin:
    """pybind11 module lifecycle + CMake build + config monitoring."""

    def _load_all_modules(self):
        """Load all enabled modules"""
        for name, config in self.module_configs.items():
            if config.enabled:
                self.load_module(name)
    
    def load_module(self, module_name: str) -> bool:
        """
        Load a C++ pybind11 module.
        
        Args:
            module_name: Name of the module to load
            
        Returns:
            True if module loaded successfully
        """
        with self._module_lock:
            if module_name in self.loaded_modules:
                logger.debug(f"Module {module_name} already loaded")
                return True
            
            config = self.module_configs.get(module_name)
            if not config:
                logger.error(f"Unknown module: {module_name}")
                return False
            
            status = self.module_status[module_name]
            
            try:
                # If specific path provided, add to sys.path
                if config.path:
                    module_dir = os.path.dirname(os.path.abspath(config.path))
                    if module_dir not in sys.path:
                        sys.path.insert(0, module_dir)
                
                # Try to import the module
                module = importlib.import_module(module_name)
                
                # Get available classes
                available_classes = [
                    name for name in dir(module)
                    if not name.startswith('_') and isinstance(getattr(module, name), type)
                ]
                
                self.loaded_modules[module_name] = module
                
                # Update status
                status.loaded = True
                status.path = getattr(module, '__file__', None)
                status.available_classes = available_classes
                status.load_time = datetime.now()
                status.last_error = None
                
                # Count calculators from this module
                status.calculator_count = sum(
                    1 for d in self.calculator_definitions.values()
                    if d.module == module_name
                )
                
                self._stats['modules_loaded'] += 1
                
                logger.info(f"Loaded C++ module: {module_name} ({len(available_classes)} classes)")
                return True
                
            except ImportError as e:
                status.loaded = False
                status.last_error = str(e)
                logger.warning(f"Failed to import C++ module '{module_name}': {e}")
                logger.info(f"Module may need to be built. Run: cd cpp && mkdir build && cd build && cmake .. && make")
                return False
                
            except Exception as e:
                status.loaded = False
                status.last_error = str(e)
                logger.error(f"Error loading C++ module '{module_name}': {e}")
                return False
    
    def unload_module(self, module_name: str) -> bool:
        """
        Unload a C++ module.
        
        Args:
            module_name: Name of the module to unload
            
        Returns:
            True if unloaded successfully
        """
        with self._module_lock:
            if module_name not in self.loaded_modules:
                return True
            
            # Clear cached calculators from this module
            with self._calculator_lock:
                keys_to_remove = [
                    k for k, v in self.calculator_cache.items()
                    if hasattr(v, '_module_name') and v._module_name == module_name
                ]
                for key in keys_to_remove:
                    del self.calculator_cache[key]
            
            # Remove from loaded modules
            del self.loaded_modules[module_name]
            
            # Update status
            if module_name in self.module_status:
                self.module_status[module_name].loaded = False
            
            logger.info(f"Unloaded C++ module: {module_name}")
            return True
    
    def reload_module(self, module_name: str) -> bool:
        """
        Reload a C++ module.
        
        Args:
            module_name: Name of the module to reload
            
        Returns:
            True if reloaded successfully
        """
        self.unload_module(module_name)
        return self.load_module(module_name)
    
    def is_module_loaded(self, module_name: str) -> bool:
        """Check if a module is loaded"""
        return module_name in self.loaded_modules
    
    def get_module(self, module_name: str) -> Optional[Any]:
        """Get a loaded module object"""
        return self.loaded_modules.get(module_name)
    

    def _start_config_monitor(self, interval: int):
        """Start config file monitor thread"""
        def monitor_loop():
            while not self._stop_monitor.wait(interval):
                try:
                    new_hash = self._compute_config_hash()
                    if new_hash != self._config_hash:
                        logger.info("CPP config file changed, reloading...")
                        self._reload_config()
                except Exception as e:
                    logger.error(f"Error in config monitor: {e}")
        
        self._config_monitor_thread = threading.Thread(
            target=monitor_loop,
            name="CPPConfigMonitor",
            daemon=True
        )
        self._config_monitor_thread.start()
        logger.debug(f"Started CPP config monitor (interval: {interval}s)")
    
    def _reload_config(self):
        """Reload configuration from file"""
        try:
            with open(self.config_path, 'r') as f:
                new_config = json.load(f)
            
            self.config = new_config
            self._config_hash = self._compute_config_hash()
            
            # Re-parse configurations
            old_modules = set(self.module_configs.keys())
            self.module_configs.clear()
            self.calculator_definitions.clear()
            
            self._parse_module_configs()
            self._parse_calculator_definitions()
            
            # Reload modules that were previously loaded
            new_modules = set(self.module_configs.keys())
            
            # Unload removed modules
            for mod in old_modules - new_modules:
                if mod in self.loaded_modules:
                    self.unload_module(mod)
            
            # Load new/updated modules
            for mod in new_modules:
                if self.module_configs[mod].enabled:
                    self.reload_module(mod)
            
            logger.info("CPP configuration reloaded")
            
        except Exception as e:
            logger.error(f"Failed to reload CPP config: {e}")
    
    def build_module(self, module_name: str) -> bool:
        """
        Build a C++ module using CMake.
        
        Args:
            module_name: Name of the module to build
            
        Returns:
            True if build successful
        """
        config = self.module_configs.get(module_name)
        if not config:
            logger.error(f"Unknown module: {module_name}")
            return False
        
        try:
            # Find project root
            if self.config_path:
                project_root = os.path.dirname(os.path.dirname(os.path.abspath(self.config_path)))
            else:
                project_root = os.getcwd()
            
            source_dir = os.path.join(project_root, config.source_dir)
            build_dir = os.path.join(project_root, config.build_dir)
            
            if not os.path.exists(source_dir):
                logger.error(f"Source directory not found: {source_dir}")
                return False
            
            # Create build directory
            os.makedirs(build_dir, exist_ok=True)
            
            # Run CMake configure
            cmake_cmd = ['cmake', source_dir] + config.cmake_options
            logger.info(f"Running: {' '.join(cmake_cmd)}")
            
            result = subprocess.run(cmake_cmd, cwd=build_dir, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"CMake configure failed: {result.stderr}")
                return False
            
            # Run make
            make_cmd = ['make', '-j4']
            logger.info(f"Running: {' '.join(make_cmd)}")
            
            result = subprocess.run(make_cmd, cwd=build_dir, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"Build failed: {result.stderr}")
                return False
            
            logger.info(f"Successfully built module: {module_name}")
            return True
            
        except Exception as e:
            logger.error(f"Build error: {e}")
            return False
    
