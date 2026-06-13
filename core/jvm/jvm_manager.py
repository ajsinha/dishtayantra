"""
JVM Manager for DishtaYantra
Version: 1.6.0

Manages Java Virtual Machine instances and Py4J gateway connections.
Provides centralized configuration and lifecycle management for Java calculators.

Features:
- Config-based JVM initialization from jvm_config.json
- Multiple gateway instance support
- Automatic JVM subprocess management
- Health monitoring and auto-restart
- Shared across main process and worker processes

Copyright © 2025 Ashutosh Sinha. All rights reserved.
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


# v2.2 module split: types + lifecycle + config/monitoring live in sibling
# modules. Every public name remains importable from this module.
from core.jvm.jvm_types import (  # noqa: F401
    CalculatorDefinition,
    GatewayConfig,
    GatewayStatus,
)
from core.jvm.jvm_lifecycle import JVMLifecycleMixin
from core.jvm.jvm_config import JVMConfigMonitorMixin


class JVMManager(JVMLifecycleMixin, JVMConfigMonitorMixin):
    """
    Central manager for JVM instances and Py4J gateways.
    
    This class handles:
    - Loading configuration from jvm_config.json
    - Starting/stopping JVM subprocesses
    - Managing Py4J gateway connections
    - Health monitoring
    - Calculator registry
    
    Usage:
        jvm_manager = JVMManager.get_instance()
        jvm_manager.initialize()
        
        # Get a gateway for calculator operations
        gateway = jvm_manager.get_gateway("primary")
        
        # Create a Java calculator instance
        calc = jvm_manager.create_calculator("MathCalculator", "my_calc", {"factor": 3})
    """
    
    _instance: Optional['JVMManager'] = None
    _lock = threading.Lock()
    
    @classmethod
    def get_instance(cls) -> 'JVMManager':
        """Get the singleton instance of JVMManager"""
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
        Initialize the JVM Manager.
        
        Args:
            config_path: Path to jvm_config.json (uses default if None)
        """
        self.config_path = config_path
        self.config: Dict[str, Any] = {}
        self.gateway_configs: Dict[str, GatewayConfig] = {}
        self.calculator_definitions: Dict[str, CalculatorDefinition] = {}
        
        # Runtime state
        self.gateways: Dict[str, JavaGateway] = {}
        self.gateway_pools: Dict[str, List[JavaGateway]] = {}
        self.jvm_processes: Dict[str, subprocess.Popen] = {}
        self.gateway_status: Dict[str, GatewayStatus] = {}
        
        # Thread safety
        self._init_lock = threading.RLock()
        self._gateway_lock = threading.RLock()
        
        # State
        self._initialized = False
        self._shutting_down = False
        
        # Health monitoring
        self._health_thread: Optional[threading.Thread] = None
        self._health_running = False
        
        # Config monitoring (v1.6.0)
        self._config_thread: Optional[threading.Thread] = None
        self._config_running = False
        self._config_last_modified: Optional[float] = None
        self._config_hash: Optional[str] = None
        
        # Register shutdown handler
        atexit.register(self._atexit_handler)
    
    def _atexit_handler(self):
        """Handle process exit"""
        if not self._shutting_down:
            self.shutdown()
    
    def initialize(self, auto_connect: bool = True) -> bool:
        """
        Initialize the JVM Manager.
        
        Args:
            auto_connect: If True, attempt to connect to gateways
            
        Returns:
            True if initialization successful
        """
        with self._init_lock:
            if self._initialized:
                return True
            
            if not PY4J_AVAILABLE:
                logger.error("Py4J not available, cannot initialize JVM Manager")
                return False
            
            # Load config if not already loaded
            if not self.config:
                self.load_config()
            
            # Check if enabled
            jvm_config = self.config.get('jvm_manager', {})
            if not jvm_config.get('enabled', True):
                logger.info("JVM Manager disabled in config")
                return False
            
            logger.info("Initializing JVM Manager...")
            
            # Start JVMs if configured
            for name, gw_config in self.gateway_configs.items():
                if gw_config.enabled and gw_config.jvm_auto_start:
                    self._start_jvm(name)
            
            # Connect to gateways
            if auto_connect:
                for name, gw_config in self.gateway_configs.items():
                    if gw_config.enabled:
                        self._connect_gateway(name)
            
            # Start health monitoring
            self._start_health_monitoring()
            
            # Start config monitoring (v1.6.0)
            self._start_config_monitoring()
            
            self._initialized = True
            logger.info("JVM Manager initialized successfully")
            return True
    
    def get_gateway(self, gateway_name: str = "primary") -> Optional[JavaGateway]:
        """
        Get a Py4J gateway connection.
        
        Args:
            gateway_name: Name of the gateway (default: "primary")
            
        Returns:
            JavaGateway instance or None if not available
        """
        if not self._initialized:
            logger.warning("JVM Manager not initialized")
            return None
        
        with self._gateway_lock:
            if gateway_name in self.gateways:
                return self.gateways[gateway_name]
        
        # Try to connect
        if self._connect_gateway(gateway_name):
            return self.gateways.get(gateway_name)
        
        return None
    
    def get_gateway_from_pool(self, gateway_name: str = "primary") -> Optional[JavaGateway]:
        """
        Get a gateway from the pool (round-robin).
        
        Args:
            gateway_name: Name of the gateway
            
        Returns:
            JavaGateway instance or None
        """
        with self._gateway_lock:
            pool = self.gateway_pools.get(gateway_name, [])
            if pool:
                # Round-robin selection using thread-local counter
                if not hasattr(threading.current_thread(), '_gw_counter'):
                    threading.current_thread()._gw_counter = 0
                
                idx = threading.current_thread()._gw_counter % len(pool)
                threading.current_thread()._gw_counter += 1
                
                return pool[idx]
        
        return self.get_gateway(gateway_name)
    
    def create_calculator(self, calculator_name: str, instance_name: str, 
                         config: Dict[str, Any] = None) -> Optional[Any]:
        """
        Create a Java calculator instance.
        
        Args:
            calculator_name: Name of the calculator (from jvm_config.json definitions)
            instance_name: Unique name for this instance
            config: Configuration overrides
            
        Returns:
            Java calculator instance or None
        """
        if not self._initialized:
            logger.warning("JVM Manager not initialized")
            return None
        
        # Get calculator definition
        defn = self.calculator_definitions.get(calculator_name)
        if defn is None:
            logger.warning(f"Unknown calculator: {calculator_name}")
            # Try direct class name
            java_class = calculator_name
            gateway_name = "primary"
            default_config = {}
        else:
            java_class = defn.java_class
            gateway_name = defn.gateway
            default_config = defn.default_config.copy()
        
        # Merge config
        if config:
            default_config.update(config)
        
        # Get gateway
        gateway = self.get_gateway(gateway_name)
        if gateway is None:
            logger.error(f"No gateway available for calculator {calculator_name}")
            return None
        
        try:
            # Create calculator using gateway entry point
            entry_point = gateway.entry_point
            calc_instance = entry_point.createCalculator(java_class, instance_name, default_config)
            
            logger.info(f"Created Java calculator: {instance_name} ({java_class})")
            return calc_instance
            
        except Exception as e:
            logger.error(f"Failed to create calculator {calculator_name}: {e}")
            return None
    
    def get_calculator_definitions(self) -> Dict[str, CalculatorDefinition]:
        """Get all calculator definitions"""
        return self.calculator_definitions.copy()
    
    def get_registered_calculators(self, gateway_name: str = "primary") -> List[str]:
        """
        Get list of calculators registered in the Java gateway.
        
        Args:
            gateway_name: Name of the gateway
            
        Returns:
            List of calculator names
        """
        gateway = self.get_gateway(gateway_name)
        if gateway is None:
            return []
        
        try:
            entry_point = gateway.entry_point
            return list(entry_point.getRegisteredCalculators())
        except Exception as e:
            logger.error(f"Failed to get registered calculators: {e}")
            return []
    
    def get_status(self) -> Dict[str, Any]:
        """Get status of all gateways and JVMs"""
        status = {
            'initialized': self._initialized,
            'py4j_available': PY4J_AVAILABLE,
            'gateways': {}
        }
        
        for name, gw_status in self.gateway_status.items():
            status['gateways'][name] = {
                'name': gw_status.name,
                'connected': gw_status.connected,
                'jvm_pid': gw_status.jvm_process_pid,
                'jvm_started': gw_status.jvm_started,
                'active_connections': gw_status.active_connections,
                'total_requests': gw_status.total_requests,
                'errors': gw_status.errors,
                'last_health_check': gw_status.last_health_check.isoformat() if gw_status.last_health_check else None,
                'start_time': gw_status.start_time.isoformat() if gw_status.start_time else None
            }
        
        return status
    
    def shutdown(self):
        """Shutdown the JVM Manager and all connections"""
        with self._init_lock:
            if self._shutting_down:
                return
            
            self._shutting_down = True
            logger.info("Shutting down JVM Manager...")
            
            # Stop health monitoring
            self._health_running = False
            if self._health_thread and self._health_thread.is_alive():
                self._health_thread.join(timeout=5)
            
            # Stop config monitoring
            self._config_running = False
            if hasattr(self, '_config_thread') and self._config_thread and self._config_thread.is_alive():
                self._config_thread.join(timeout=5)
            
            # Close gateway connections
            with self._gateway_lock:
                for name, pool in self.gateway_pools.items():
                    for gateway in pool:
                        try:
                            gateway.shutdown()
                        except:
                            pass
                self.gateway_pools.clear()
                self.gateways.clear()
            
            # Stop JVM processes
            shutdown_timeout = self.config.get('jvm_manager', {}).get('shutdown_timeout_seconds', 30)
            for name, proc in list(self.jvm_processes.items()):
                if proc.poll() is None:
                    logger.info(f"Stopping JVM for {name} (PID: {proc.pid})...")
                    proc.terminate()
                    try:
                        proc.wait(timeout=shutdown_timeout)
                    except subprocess.TimeoutExpired:
                        logger.warning(f"Force killing JVM for {name}")
                        proc.kill()
            
            self.jvm_processes.clear()
            self._initialized = False
            logger.info("JVM Manager shutdown complete")
    
    def is_initialized(self) -> bool:
        """Check if the JVM Manager is initialized"""
        return self._initialized
    
    def is_gateway_available(self, gateway_name: str = "primary") -> bool:
        """Check if a gateway is available"""
        status = self.gateway_status.get(gateway_name)
        return status is not None and status.connected
    
_jvm_manager: Optional["JVMManager"] = None


def get_jvm_manager() -> JVMManager:
    """Get the global JVM Manager instance"""
    global _jvm_manager
    if _jvm_manager is None:
        _jvm_manager = JVMManager.get_instance()
    return _jvm_manager

def initialize_jvm_manager(config_path: str = None, auto_connect: bool = True) -> bool:
    """Initialize the global JVM Manager"""
    manager = get_jvm_manager()
    manager.load_config(config_path)
    return manager.initialize(auto_connect)

def get_gateway(gateway_name: str = "primary") -> Optional[JavaGateway]:
    """Get a Py4J gateway from the global manager"""
    return get_jvm_manager().get_gateway(gateway_name)

def create_calculator(calculator_name: str, instance_name: str, 
                     config: Dict[str, Any] = None) -> Optional[Any]:
    """Create a Java calculator using the global manager"""
    return get_jvm_manager().create_calculator(calculator_name, instance_name, config)

def is_jvm_available() -> bool:
    """Check if JVM/Py4J is available"""
    manager = get_jvm_manager()
    return manager.is_initialized() and manager.is_gateway_available()
