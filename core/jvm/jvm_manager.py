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


class JVMManager:
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


# Module-level convenience functions
_jvm_manager: Optional[JVMManager] = None

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
