"""
JVM Config & Monitoring Mixin (v2.2 module split)
=================================================

Configuration loading/parsing, hot-reload (config-file monitor), and
gateway health monitoring for JVMManager, extracted verbatim from
jvm_manager.py to respect the 500-line architecture limit. All state
lives on the manager.

NOTE: the legacy class historically defined
_get_config_hash and reload_config twice; both definitions are preserved
here IN ORDER so the later ones keep winning exactly as before.

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


from core.jvm.jvm_types import CalculatorDefinition, GatewayConfig, GatewayStatus


class JVMConfigMonitorMixin:
    """Config load/parse/hot-reload + health monitoring."""

    def load_config(self, config_path: str = None) -> bool:
        """
        Load configuration from jvm_config.json.
        
        Args:
            config_path: Path to config file (uses default if None)
            
        Returns:
            True if config loaded successfully
        """
        if config_path:
            self.config_path = config_path
        
        # Determine config path
        if self.config_path is None:
            # Try default locations
            possible_paths = [
                'config/jvm_config.json',
                '../config/jvm_config.json',
                os.path.join(os.path.dirname(__file__), '../../config/jvm_config.json')
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    self.config_path = path
                    break
        
        if self.config_path is None or not os.path.exists(self.config_path):
            logger.warning(f"JVM config not found, using defaults")
            self._use_default_config()
            return False
        
        try:
            with open(self.config_path, 'r') as f:
                self.config = json.load(f)
            
            logger.info(f"Loaded JVM config from {self.config_path}")
            self._parse_config()
            return True
            
        except Exception as e:
            logger.error(f"Failed to load JVM config: {e}")
            self._use_default_config()
            return False
    
    def _use_default_config(self):
        """Use default configuration"""
        self.config = {
            'jvm_manager': {'enabled': True, 'auto_start_on_load': True},
            'gateways': [{
                'name': 'primary',
                'enabled': True,
                'host': 'localhost',
                'base_port': 25333,
                'pool_size': 4,
                'jvm': {
                    'auto_start': False,
                    'heap_size_mb': 512,
                    'max_heap_size_mb': 2048,
                    'jvm_options': [],
                    'classpath': []
                },
                'connection': {
                    'timeout_seconds': 30,
                    'retry_attempts': 3,
                    'retry_delay_seconds': 2
                }
            }],
            'calculators': {'definitions': {}}
        }
        self._parse_config()
    
    def _parse_config(self):
        """Parse loaded configuration into structured objects"""
        # Parse gateway configurations
        self.gateway_configs.clear()
        for gw_config in self.config.get('gateways', []):
            name = gw_config.get('name', 'default')
            jvm_config = gw_config.get('jvm', {})
            conn_config = gw_config.get('connection', {})
            
            config = GatewayConfig(
                name=name,
                enabled=gw_config.get('enabled', True),
                host=gw_config.get('host', 'localhost'),
                base_port=gw_config.get('base_port', 25333),
                pool_size=gw_config.get('pool_size', 4),
                jvm_auto_start=jvm_config.get('auto_start', False),
                java_home=jvm_config.get('java_home'),
                heap_size_mb=jvm_config.get('heap_size_mb', 512),
                max_heap_size_mb=jvm_config.get('max_heap_size_mb', 2048),
                jvm_options=jvm_config.get('jvm_options', []),
                classpath=jvm_config.get('classpath', []),
                timeout_seconds=conn_config.get('timeout_seconds', 30),
                retry_attempts=conn_config.get('retry_attempts', 3),
                retry_delay_seconds=conn_config.get('retry_delay_seconds', 2),
                keep_alive=conn_config.get('keep_alive', True)
            )
            self.gateway_configs[name] = config
            self.gateway_status[name] = GatewayStatus(name=name)
        
        # Parse calculator definitions
        self.calculator_definitions.clear()
        calc_defs = self.config.get('calculators', {}).get('definitions', {})
        for calc_name, calc_config in calc_defs.items():
            defn = CalculatorDefinition(
                name=calc_name,
                java_class=calc_config.get('java_class', ''),
                description=calc_config.get('description', ''),
                gateway=calc_config.get('gateway', 'primary'),
                default_config=calc_config.get('default_config', {})
            )
            self.calculator_definitions[calc_name] = defn
        
        logger.info(f"Parsed {len(self.gateway_configs)} gateway configs, "
                   f"{len(self.calculator_definitions)} calculator definitions")
    

    def _get_config_hash(self) -> str:
        """Get a hash of the current config file for change detection"""
        import hashlib
        if self.config_path and os.path.exists(self.config_path):
            with open(self.config_path, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        return ""
    
    def _check_config_changed(self) -> bool:
        """Check if configuration file has changed"""
        if not hasattr(self, '_last_config_hash'):
            self._last_config_hash = self._get_config_hash()
            return False
        
        current_hash = self._get_config_hash()
        if current_hash != self._last_config_hash:
            self._last_config_hash = current_hash
            return True
        return False
    
    def reload_config(self) -> Dict[str, bool]:
        """
        Reload configuration and restart affected JVMs.
        
        Returns:
            Dict mapping gateway names to restart success status
        """
        logger.info("Reloading JVM configuration...")
        
        # Store old config
        old_gateway_configs = {
            name: {
                'heap_size_mb': cfg.heap_size_mb,
                'max_heap_size_mb': cfg.max_heap_size_mb,
                'jvm_options': cfg.jvm_options.copy(),
                'classpath': cfg.classpath.copy(),
                'pool_size': cfg.pool_size
            }
            for name, cfg in self.gateway_configs.items()
        }
        
        # Reload config
        self.load_config()
        
        # Check which gateways need restart
        results = {}
        for name, cfg in self.gateway_configs.items():
            if name in old_gateway_configs:
                old_cfg = old_gateway_configs[name]
                needs_restart = (
                    cfg.heap_size_mb != old_cfg['heap_size_mb'] or
                    cfg.max_heap_size_mb != old_cfg['max_heap_size_mb'] or
                    cfg.jvm_options != old_cfg['jvm_options'] or
                    cfg.classpath != old_cfg['classpath'] or
                    cfg.pool_size != old_cfg['pool_size']
                )
                
                if needs_restart and cfg.enabled and cfg.jvm_auto_start:
                    logger.info(f"Configuration changed for {name}, restarting...")
                    results[name] = self.restart_jvm(name)
                else:
                    results[name] = True  # No restart needed
            else:
                # New gateway
                if cfg.enabled and cfg.jvm_auto_start:
                    results[name] = self._start_jvm(name) and self._connect_gateway(name)
        
        return results
    

    def _start_health_monitoring(self):
        """Start the health monitoring thread"""
        if self._health_running:
            return
        
        check_interval = self.config.get('jvm_manager', {}).get('health_check_interval_seconds', 30)
        config_check_interval = self.config.get('jvm_manager', {}).get('config_check_interval_seconds', 600)  # 10 minutes
        
        self._health_running = True
        self._last_config_check = time.time()
        self._config_check_interval = config_check_interval
        
        self._health_thread = threading.Thread(
            target=self._health_monitoring_loop,
            args=(check_interval,),
            name="JVMHealthMonitor",
            daemon=True
        )
        self._health_thread.start()
    
    def _health_monitoring_loop(self, interval: int):
        """Health monitoring loop"""
        while self._health_running:
            try:
                self._check_health()
                
                # Check for config changes periodically
                if hasattr(self, '_config_check_interval') and hasattr(self, '_last_config_check'):
                    if time.time() - self._last_config_check >= self._config_check_interval:
                        self._last_config_check = time.time()
                        if self._check_config_changed():
                            logger.info("Configuration file changed, reloading...")
                            self.reload_config()
                
                time.sleep(interval)
            except Exception as e:
                logger.error(f"Health check error: {e}")
                time.sleep(5)
    
    def _check_health(self):
        """Check health of all gateways and JVMs"""
        for name in list(self.gateway_configs.keys()):
            status = self.gateway_status.get(name)
            if status is None:
                continue
            
            # Check JVM process
            if name in self.jvm_processes:
                proc = self.jvm_processes[name]
                if proc.poll() is not None:
                    # Process died
                    logger.warning(f"JVM for {name} died (exit code: {proc.returncode})")
                    status.jvm_started = False
                    status.connected = False
                    
                    # Auto-restart if configured
                    config = self.gateway_configs[name]
                    if config.jvm_auto_start:
                        logger.info(f"Auto-restarting JVM for {name}...")
                        self._start_jvm(name)
                        time.sleep(2)
                        self._connect_gateway(name)
            
            # Check gateway connection
            if status.connected:
                try:
                    gateway = self.gateways.get(name)
                    if gateway:
                        gateway.jvm.System.currentTimeMillis()
                        status.last_health_check = datetime.now()
                except Exception as e:
                    logger.warning(f"Gateway {name} connection lost: {e}")
                    status.connected = False
                    status.errors += 1
    

    def _start_config_monitoring(self):
        """Start the config file monitoring thread"""
        if hasattr(self, '_config_running') and self._config_running:
            return
        
        # Get interval from config (default 600 seconds = 10 minutes)
        monitor_interval = self.config.get('jvm_manager', {}).get('config_check_interval_seconds', 600)
        
        self._config_running = True
        self._config_last_modified = self._get_config_modified_time()
        self._config_hash = self._get_config_hash()
        
        self._config_thread = threading.Thread(
            target=self._config_monitoring_loop,
            args=(monitor_interval,),
            name="JVMConfigMonitor",
            daemon=True
        )
        self._config_thread.start()
        logger.info(f"Config monitoring started (interval: {monitor_interval} seconds)")
    
    def _config_monitoring_loop(self, interval: int):
        """Config monitoring loop"""
        while self._config_running:
            try:
                time.sleep(interval)
                self._check_config_changes()
            except Exception as e:
                logger.error(f"Config monitoring error: {e}")
                time.sleep(60)
    
    def _get_config_modified_time(self) -> Optional[float]:
        """Get the modification time of the config file"""
        if self.config_path and os.path.exists(self.config_path):
            return os.path.getmtime(self.config_path)
        return None
    
    def _get_config_hash(self) -> Optional[str]:
        """Get a hash of the config file contents"""
        import hashlib
        if self.config_path and os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'rb') as f:
                    return hashlib.md5(f.read()).hexdigest()
            except:
                pass
        return None
    
    def _check_config_changes(self):
        """Check if config file has changed and reload if needed"""
        current_modified = self._get_config_modified_time()
        current_hash = self._get_config_hash()
        
        if current_modified != self._config_last_modified or current_hash != self._config_hash:
            logger.info("JVM config file changed, checking for updates...")
            
            # Reload config
            old_config = self.config.copy()
            old_gateway_configs = {k: v.__dict__.copy() for k, v in self.gateway_configs.items()}
            
            self.load_config()
            
            # Check which gateways have changed
            for name, new_config in self.gateway_configs.items():
                old = old_gateway_configs.get(name)
                if old is None:
                    # New gateway added
                    logger.info(f"New gateway '{name}' added, starting...")
                    if new_config.enabled and new_config.jvm_auto_start:
                        self._start_jvm(name)
                        time.sleep(2)
                        self._connect_gateway(name)
                elif self._gateway_config_changed(old, new_config):
                    # Config changed, restart JVM
                    logger.info(f"Gateway '{name}' config changed, restarting JVM...")
                    self.restart_jvm(name)
            
            # Check for removed gateways
            for name in old_gateway_configs:
                if name not in self.gateway_configs:
                    logger.info(f"Gateway '{name}' removed, shutting down...")
                    self.kill_jvm(name)
            
            self._config_last_modified = current_modified
            self._config_hash = current_hash
    
    def _gateway_config_changed(self, old: Dict, new: GatewayConfig) -> bool:
        """Check if gateway config has changed in a way that requires restart"""
        critical_fields = [
            ('base_port', new.base_port),
            ('pool_size', new.pool_size),
            ('heap_size_mb', new.heap_size_mb),
            ('max_heap_size_mb', new.max_heap_size_mb),
            ('jvm_options', new.jvm_options),
            ('classpath', new.classpath),
        ]
        
        for field, new_value in critical_fields:
            if old.get(field) != new_value:
                return True
        
        return False
    
    def reload_config(self) -> Dict[str, Any]:
        """
        Manually reload configuration from file.
        
        Returns:
            Dict with reload results
        """
        logger.info("Manually reloading JVM configuration...")
        
        results = {
            'reloaded': False,
            'gateways_restarted': [],
            'errors': []
        }
        
        try:
            # Store old configs
            old_gateway_configs = {k: v.__dict__.copy() for k, v in self.gateway_configs.items()}
            
            # Reload config
            if not self.load_config():
                results['errors'].append("Failed to load configuration file")
                return results
            
            results['reloaded'] = True
            
            # Check for changes
            for name, new_config in self.gateway_configs.items():
                old = old_gateway_configs.get(name)
                
                if old is None:
                    # New gateway added
                    if new_config.enabled and new_config.jvm_auto_start:
                        if self._start_jvm(name):
                            time.sleep(2)
                            self._connect_gateway(name)
                            results['gateways_restarted'].append(f"{name} (new)")
                elif self._gateway_config_changed(old, new_config):
                    # Config changed, restart JVM
                    if self.restart_jvm(name):
                        results['gateways_restarted'].append(name)
                    else:
                        results['errors'].append(f"Failed to restart {name}")
            
            logger.info(f"Config reload complete: {len(results['gateways_restarted'])} gateways restarted")
            
        except Exception as e:
            logger.error(f"Error during config reload: {e}")
            results['errors'].append(str(e))
        
        return results


