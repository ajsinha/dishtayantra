"""
JVM Lifecycle Mixin (v2.2 module split)
=======================================

JVM process start/stop/kill/restart, java command construction, and
gateway connect/disconnect for JVMManager, extracted verbatim from
jvm_manager.py to respect the 500-line architecture limit. All state
lives on the manager.

NOTE: the legacy class historically defined
restart_jvm twice; both definitions are preserved here IN ORDER so the
later one keeps winning exactly as before.

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


class JVMLifecycleMixin:
    """JVM process + Py4J gateway lifecycle."""

    def _start_jvm(self, gateway_name: str) -> bool:
        """
        Start a JVM subprocess for the specified gateway.
        
        Args:
            gateway_name: Name of the gateway configuration
            
        Returns:
            True if JVM started successfully
        """
        if gateway_name not in self.gateway_configs:
            logger.error(f"Unknown gateway: {gateway_name}")
            return False
        
        config = self.gateway_configs[gateway_name]
        
        if gateway_name in self.jvm_processes:
            proc = self.jvm_processes[gateway_name]
            if proc.poll() is None:  # Process still running
                logger.info(f"JVM for {gateway_name} already running (PID: {proc.pid})")
                return True
        
        logger.info(f"Starting JVM for gateway '{gateway_name}'...")
        
        # Build Java command
        java_cmd = self._build_java_command(config)
        
        if not java_cmd:
            logger.error(f"Failed to build Java command for {gateway_name}")
            return False
        
        try:
            # Start the JVM process
            proc = subprocess.Popen(
                java_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            self.jvm_processes[gateway_name] = proc
            self.gateway_status[gateway_name].jvm_process_pid = proc.pid
            self.gateway_status[gateway_name].jvm_started = True
            self.gateway_status[gateway_name].start_time = datetime.now()
            
            logger.info(f"JVM for {gateway_name} started (PID: {proc.pid})")
            logger.debug(f"JVM command: {' '.join(java_cmd)}")
            
            # Wait for gateway to be ready with progress check
            startup_timeout = 10  # seconds
            for i in range(startup_timeout):
                # Check if process died
                if proc.poll() is not None:
                    stdout, stderr = proc.communicate()
                    logger.error(f"JVM for {gateway_name} exited prematurely (code: {proc.returncode})")
                    if stderr:
                        logger.error(f"JVM stderr: {stderr[:500]}")
                    if stdout:
                        logger.debug(f"JVM stdout: {stdout[:500]}")
                    self.gateway_status[gateway_name].jvm_started = False
                    return False
                
                time.sleep(1)
                
                # Try a quick connection test after 3 seconds
                if i >= 3:
                    try:
                        import socket
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(0.5)
                        result = sock.connect_ex((config.host, config.base_port))
                        sock.close()
                        if result == 0:
                            logger.info(f"JVM gateway {gateway_name} is ready on port {config.base_port}")
                            return True
                    except Exception:
                        pass
            
            # Final check
            if proc.poll() is None:
                logger.info(f"JVM for {gateway_name} appears to be running (PID: {proc.pid})")
                return True
            else:
                stdout, stderr = proc.communicate()
                logger.error(f"JVM for {gateway_name} failed to start properly")
                if stderr:
                    logger.error(f"JVM stderr: {stderr[:1000]}")
                return False
            
        except Exception as e:
            logger.error(f"Failed to start JVM for {gateway_name}: {e}")
            return False
    
    def _build_java_command(self, config: GatewayConfig) -> Optional[List[str]]:
        """Build the Java command to start the gateway server"""
        # Find Java executable
        java_exe = 'java'
        if config.java_home:
            java_exe = os.path.join(config.java_home, 'bin', 'java')
        
        # Verify Java is available
        import shutil
        if not shutil.which(java_exe):
            logger.error(f"Java executable not found: {java_exe}")
            logger.error("Please install JDK 11+ or set java_home in jvm_config.json")
            return None
        
        # Build classpath
        classpath_parts = []
        base_dir = os.path.dirname(self.config_path or '.')
        project_root = os.path.abspath(os.path.join(base_dir, '..'))
        
        for cp in config.classpath:
            # Resolve relative paths from project root
            if not os.path.isabs(cp):
                resolved_cp = os.path.join(project_root, cp)
            else:
                resolved_cp = cp
            classpath_parts.append(resolved_cp)
        
        if not classpath_parts:
            # Use default classpath
            classpath_parts = [
                os.path.join(project_root, 'java', 'lib', '*'),
            ]
        
        # Verify classpath exists
        classpath_valid = False
        for cp in classpath_parts:
            # Handle wildcards
            if cp.endswith('*'):
                cp_dir = os.path.dirname(cp)
                if os.path.isdir(cp_dir):
                    jars = [f for f in os.listdir(cp_dir) if f.endswith('.jar')]
                    if jars:
                        classpath_valid = True
                        logger.debug(f"Found JARs in {cp_dir}: {jars}")
                    else:
                        logger.warning(f"No JAR files found in {cp_dir}")
                        logger.warning("Run 'java/build.sh' to compile Java code and download dependencies")
            elif os.path.exists(cp):
                classpath_valid = True
        
        if not classpath_valid:
            logger.error("No valid classpath entries found!")
            logger.error("Please run: cd java && ./build.sh")
            return None
        
        classpath = os.pathsep.join(classpath_parts)
        
        # Build command
        cmd = [java_exe]
        
        # Memory settings
        cmd.append(f'-Xms{config.heap_size_mb}m')
        cmd.append(f'-Xmx{config.max_heap_size_mb}m')
        
        # Additional JVM options
        cmd.extend(config.jvm_options)
        
        # Classpath
        cmd.extend(['-cp', classpath])
        
        # Main class
        cmd.append('com.dishtayantra.gateway.DishtaYantraGateway')
        
        # Arguments
        cmd.extend(['--host', config.host])
        cmd.extend(['--port', str(config.base_port)])
        cmd.extend(['--pool-size', str(config.pool_size)])
        
        logger.debug(f"Built Java command: {' '.join(cmd)}")
        
        return cmd
    
    def _connect_gateway(self, gateway_name: str) -> bool:
        """
        Connect to a Py4J gateway.
        
        Args:
            gateway_name: Name of the gateway to connect to
            
        Returns:
            True if connection successful
        """
        if gateway_name not in self.gateway_configs:
            logger.error(f"Unknown gateway: {gateway_name}")
            return False
        
        config = self.gateway_configs[gateway_name]
        
        logger.info(f"Connecting to gateway '{gateway_name}' at {config.host}:{config.base_port}")
        
        pool = []
        for i in range(config.pool_size):
            port = config.base_port + i
            
            for attempt in range(config.retry_attempts):
                try:
                    gateway = JavaGateway(
                        gateway_parameters=GatewayParameters(
                            address=config.host,
                            port=port,
                            auto_convert=True,
                            eager_load=True
                        )
                    )
                    
                    # Test connection
                    gateway.jvm.System.currentTimeMillis()
                    
                    pool.append(gateway)
                    logger.debug(f"Connected to {config.host}:{port}")
                    break
                    
                except Exception as e:
                    if attempt < config.retry_attempts - 1:
                        logger.warning(f"Connection to {config.host}:{port} failed, "
                                      f"retrying in {config.retry_delay_seconds}s...")
                        time.sleep(config.retry_delay_seconds)
                    else:
                        logger.error(f"Failed to connect to {config.host}:{port}: {e}")
        
        if pool:
            with self._gateway_lock:
                self.gateway_pools[gateway_name] = pool
                self.gateways[gateway_name] = pool[0]  # Default gateway
                self.gateway_status[gateway_name].connected = True
                self.gateway_status[gateway_name].active_connections = len(pool)
            
            logger.info(f"Connected to gateway '{gateway_name}' with {len(pool)} connections")
            return True
        else:
            logger.error(f"Failed to establish any connections to gateway '{gateway_name}'")
            return False
    

    def stop_jvm(self, gateway_name: str, force: bool = False) -> bool:
        """
        Stop a JVM subprocess.
        
        Args:
            gateway_name: Name of the gateway/JVM to stop
            force: If True, force kill immediately
            
        Returns:
            True if JVM was stopped
        """
        if gateway_name not in self.jvm_processes:
            logger.warning(f"No JVM process found for {gateway_name}")
            return False
        
        proc = self.jvm_processes[gateway_name]
        if proc.poll() is not None:
            logger.info(f"JVM for {gateway_name} already stopped")
            return True
        
        logger.info(f"Stopping JVM for {gateway_name} (PID: {proc.pid})...")
        
        # Disconnect gateway first
        with self._gateway_lock:
            if gateway_name in self.gateway_pools:
                for gw in self.gateway_pools[gateway_name]:
                    try:
                        gw.shutdown()
                    except:
                        pass
                self.gateway_pools.pop(gateway_name, None)
            self.gateways.pop(gateway_name, None)
            self.gateway_status[gateway_name].connected = False
        
        # Stop the process
        try:
            if force:
                proc.kill()
            else:
                proc.terminate()
            
            shutdown_timeout = self.config.get('jvm_manager', {}).get('shutdown_timeout_seconds', 30)
            proc.wait(timeout=shutdown_timeout)
            
            self.gateway_status[gateway_name].jvm_started = False
            self.gateway_status[gateway_name].jvm_process_pid = None
            logger.info(f"JVM for {gateway_name} stopped")
            return True
            
        except subprocess.TimeoutExpired:
            logger.warning(f"Timeout stopping JVM for {gateway_name}, force killing...")
            proc.kill()
            return True
        except Exception as e:
            logger.error(f"Error stopping JVM for {gateway_name}: {e}")
            return False
    
    def restart_jvm(self, gateway_name: str) -> bool:
        """
        Restart a JVM subprocess.
        
        Args:
            gateway_name: Name of the gateway/JVM to restart
            
        Returns:
            True if JVM was restarted successfully
        """
        logger.info(f"Restarting JVM for {gateway_name}...")
        
        # Stop if running
        if gateway_name in self.jvm_processes:
            proc = self.jvm_processes[gateway_name]
            if proc.poll() is None:
                self.stop_jvm(gateway_name)
        
        # Wait a moment
        time.sleep(1)
        
        # Start JVM
        if not self._start_jvm(gateway_name):
            logger.error(f"Failed to restart JVM for {gateway_name}")
            return False
        
        # Wait for startup
        time.sleep(2)
        
        # Reconnect
        if not self._connect_gateway(gateway_name):
            logger.warning(f"JVM restarted but gateway connection failed for {gateway_name}")
            return True  # JVM restarted, but connection failed
        
        logger.info(f"JVM for {gateway_name} restarted successfully")
        return True
    

    def kill_jvm(self, gateway_name: str) -> bool:
        """
        Kill a JVM process.
        
        Args:
            gateway_name: Name of the gateway whose JVM to kill
            
        Returns:
            True if JVM was killed successfully
        """
        if gateway_name not in self.jvm_processes:
            logger.warning(f"No JVM process found for {gateway_name}")
            return False
        
        proc = self.jvm_processes[gateway_name]
        if proc.poll() is not None:
            logger.info(f"JVM for {gateway_name} already stopped")
            return True
        
        logger.info(f"Killing JVM for {gateway_name} (PID: {proc.pid})...")
        
        # Disconnect gateway first
        self._disconnect_gateway(gateway_name)
        
        # Terminate the process
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            logger.warning(f"Force killing JVM for {gateway_name}")
            proc.kill()
            proc.wait(timeout=5)
        
        # Update status
        status = self.gateway_status.get(gateway_name)
        if status:
            status.jvm_started = False
            status.jvm_process_pid = None
            status.connected = False
        
        del self.jvm_processes[gateway_name]
        logger.info(f"JVM for {gateway_name} killed successfully")
        return True
    
    def restart_jvm(self, gateway_name: str) -> bool:
        """
        Restart a JVM process.
        
        Args:
            gateway_name: Name of the gateway whose JVM to restart
            
        Returns:
            True if JVM was restarted successfully
        """
        logger.info(f"Restarting JVM for {gateway_name}...")
        
        # Kill existing JVM
        if gateway_name in self.jvm_processes:
            self.kill_jvm(gateway_name)
            time.sleep(1)
        
        # Start new JVM
        if not self._start_jvm(gateway_name):
            logger.error(f"Failed to start JVM for {gateway_name}")
            return False
        
        # Wait for JVM to be ready
        time.sleep(2)
        
        # Reconnect gateway
        if not self._connect_gateway(gateway_name):
            logger.error(f"Failed to reconnect gateway {gateway_name}")
            return False
        
        logger.info(f"JVM for {gateway_name} restarted successfully")
        return True
    
    def _disconnect_gateway(self, gateway_name: str):
        """Disconnect from a gateway"""
        with self._gateway_lock:
            pool = self.gateway_pools.get(gateway_name, [])
            for gateway in pool:
                try:
                    gateway.shutdown()
                except:
                    pass
            
            if gateway_name in self.gateway_pools:
                del self.gateway_pools[gateway_name]
            if gateway_name in self.gateways:
                del self.gateways[gateway_name]
            
            status = self.gateway_status.get(gateway_name)
            if status:
                status.connected = False
                status.active_connections = 0
    
