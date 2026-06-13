"""
Java Gateway Pool (Py4J) - v2.0.0 module split
==============================================

Gateway pooling for Java calculator integration, extracted verbatim from
java_calculator.py so each module stays within the 500-line architecture
limit. Public names are re-exported from core.calculator.java_calculator,
so existing imports keep working.

Contains:
    JavaGatewayPool          - pool of persistent Py4J JavaGateway connections
    get_gateway_pool         - process-wide pool accessor
    _JVMManagedGatewayPool   - JVMManager-backed pool variant (v1.6.0)
    shutdown_gateway_pool    - global pool teardown

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

# v2.0.0 BUGFIX: lazy annotations so the module imports even when py4j
# is absent (the legacy "-> JavaGateway" annotations made the module
# unimportable without py4j, defeating the graceful-degradation guard).
from __future__ import annotations

import threading
import logging
import time
import queue
from typing import Dict, Any, Optional, List
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# Py4J imports - will be available when py4j is installed
try:
    from py4j.java_gateway import JavaGateway, GatewayParameters, CallbackServerParameters
    from py4j.protocol import Py4JNetworkError, Py4JJavaError
    PY4J_AVAILABLE = True
except ImportError:
    PY4J_AVAILABLE = False
    logger.warning("Py4J not installed. Java calculators will not be available. Install with: pip install py4j")

# LMDB support
try:
    from core.lmdb import LMDBCalculatorConfig, LMDBDataExchange, DataFormat
    LMDB_AVAILABLE = True
except ImportError:
    LMDB_AVAILABLE = False
    logger.debug("LMDB module not available for Java calculator")



class JavaGatewayPool:
    """
    A pool of JavaGateway connections for high-performance concurrent access.
    
    This pool maintains multiple connections to Java Gateway servers,
    allowing truly parallel execution of Java code from multiple Python threads.
    
    Architecture:
    - Each gateway connects to a separate port on the Java side
    - OR multiple gateways can connect to the same multi-threaded Java server
    - Thread-local storage ensures each thread gets a dedicated gateway
    """
    
    def __init__(self, 
                 pool_size: int = 4,
                 gateway_addresses: List[Dict[str, Any]] = None,
                 auto_start_java: bool = False,
                 java_classpath: str = None,
                 java_options: List[str] = None):
        """
        Initialize the gateway pool.
        
        Args:
            pool_size: Number of gateway connections to maintain
            gateway_addresses: List of dicts with 'host' and 'port' for each gateway
                              If None, uses default localhost:25333, 25334, etc.
            auto_start_java: If True, attempt to start Java gateway servers
            java_classpath: Classpath for Java gateway server
            java_options: Additional JVM options
        """
        if not PY4J_AVAILABLE:
            raise RuntimeError("Py4J is not installed. Install with: pip install py4j")
        
        self.pool_size = pool_size
        self.auto_start_java = auto_start_java
        self.java_classpath = java_classpath
        self.java_options = java_options or []
        
        # Gateway addresses
        if gateway_addresses:
            self.gateway_addresses = gateway_addresses
        else:
            # Default: multiple ports on localhost
            self.gateway_addresses = [
                {'host': 'localhost', 'port': 25333 + i}
                for i in range(pool_size)
            ]
        
        # Gateway pool using a queue for thread-safe access
        self._gateway_queue = queue.Queue()
        self._gateways: List[JavaGateway] = []
        self._lock = threading.Lock()
        self._initialized = False
        
        # Thread-local storage for sticky gateway assignment
        self._thread_local = threading.local()
        
        # Statistics
        self._stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'reconnections': 0
        }
        self._stats_lock = threading.Lock()
    
    def initialize(self):
        """Initialize all gateway connections."""
        with self._lock:
            if self._initialized:
                return
            
            logger.info(f"Initializing JavaGatewayPool with {self.pool_size} connections")
            
            for i, addr in enumerate(self.gateway_addresses[:self.pool_size]):
                try:
                    gateway = self._create_gateway(addr['host'], addr['port'])
                    self._gateways.append(gateway)
                    self._gateway_queue.put(gateway)
                    logger.info(f"Gateway {i} connected to {addr['host']}:{addr['port']}")
                except Exception as e:
                    logger.error(f"Failed to connect gateway {i} to {addr['host']}:{addr['port']}: {e}")
            
            if not self._gateways:
                raise RuntimeError("Failed to initialize any gateway connections")
            
            self._initialized = True
            logger.info(f"JavaGatewayPool initialized with {len(self._gateways)} active connections")
    
    def _create_gateway(self, host: str, port: int) -> JavaGateway:
        """Create a new gateway connection."""
        gateway = JavaGateway(
            gateway_parameters=GatewayParameters(
                address=host,
                port=port,
                auto_convert=True,  # Auto-convert Python collections to Java
                eager_load=True     # Load all classes eagerly for better error messages
            )
        )
        # Test the connection
        gateway.jvm.System.currentTimeMillis()
        return gateway
    
    def get_gateway(self) -> JavaGateway:
        """
        Get a gateway from the pool.
        
        Uses thread-local storage for sticky assignment - each thread
        gets assigned a gateway and keeps using it for efficiency.
        """
        if not self._initialized:
            self.initialize()
        
        # Check for thread-local gateway
        if hasattr(self._thread_local, 'gateway') and self._thread_local.gateway:
            return self._thread_local.gateway
        
        # Get a gateway from the queue (round-robin)
        try:
            gateway = self._gateway_queue.get_nowait()
            self._gateway_queue.put(gateway)  # Put it back for others
            self._thread_local.gateway = gateway
            return gateway
        except queue.Empty:
            # All gateways busy, return first available
            if self._gateways:
                gateway = self._gateways[0]
                self._thread_local.gateway = gateway
                return gateway
            raise RuntimeError("No gateway connections available")
    
    def execute_on_gateway(self, func, *args, **kwargs):
        """
        Execute a function using a gateway from the pool.
        
        Handles errors and automatic reconnection.
        """
        with self._stats_lock:
            self._stats['total_requests'] += 1
        
        gateway = self.get_gateway()
        
        try:
            result = func(gateway, *args, **kwargs)
            with self._stats_lock:
                self._stats['successful_requests'] += 1
            return result
        except (Py4JNetworkError, Py4JJavaError) as e:
            with self._stats_lock:
                self._stats['failed_requests'] += 1
            logger.error(f"Gateway error: {e}")
            
            # Try to reconnect
            self._try_reconnect(gateway)
            raise
    
    def _try_reconnect(self, failed_gateway: JavaGateway):
        """Attempt to reconnect a failed gateway."""
        with self._lock:
            try:
                idx = self._gateways.index(failed_gateway)
                addr = self.gateway_addresses[idx]
                
                # Close old connection
                try:
                    failed_gateway.shutdown()
                except:
                    pass
                
                # Create new connection
                new_gateway = self._create_gateway(addr['host'], addr['port'])
                self._gateways[idx] = new_gateway
                
                # Update thread-local if this was the thread's gateway
                if hasattr(self._thread_local, 'gateway') and self._thread_local.gateway == failed_gateway:
                    self._thread_local.gateway = new_gateway
                
                with self._stats_lock:
                    self._stats['reconnections'] += 1
                
                logger.info(f"Successfully reconnected gateway {idx}")
            except Exception as e:
                logger.error(f"Failed to reconnect gateway: {e}")
    
    def shutdown(self):
        """Shutdown all gateway connections."""
        with self._lock:
            for gateway in self._gateways:
                try:
                    gateway.shutdown()
                except:
                    pass
            self._gateways.clear()
            self._initialized = False
            logger.info("JavaGatewayPool shutdown complete")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        with self._stats_lock:
            return {
                **self._stats,
                'pool_size': self.pool_size,
                'active_connections': len(self._gateways)
            }


# Global gateway pool instance
_global_gateway_pool: Optional[JavaGatewayPool] = None
_pool_lock = threading.Lock()


def get_gateway_pool(config: Dict[str, Any] = None) -> JavaGatewayPool:
    """
    Get or create the global gateway pool.
    
    v1.6.0: Now integrates with JVMManager if available for centralized
    gateway management. Falls back to legacy pool creation if JVMManager
    is not initialized.
    """
    global _global_gateway_pool
    
    with _pool_lock:
        # First, try to use JVM Manager (v1.6.0)
        try:
            from core.jvm import get_jvm_manager, is_jvm_available
            jvm_manager = get_jvm_manager()
            if jvm_manager.is_initialized():
                # Use JVM Manager's gateway pool
                # Create a wrapper that uses JVM Manager's gateways
                if _global_gateway_pool is None:
                    _global_gateway_pool = _JVMManagedGatewayPool(jvm_manager)
                return _global_gateway_pool
        except ImportError:
            pass  # JVM Manager not available, use legacy pool
        except Exception as e:
            logger.debug(f"JVM Manager not available, using legacy pool: {e}")
        
        # Legacy pool creation
        if _global_gateway_pool is None:
            pool_config = config or {}
            _global_gateway_pool = JavaGatewayPool(
                pool_size=pool_config.get('pool_size', 4),
                gateway_addresses=pool_config.get('gateway_addresses'),
                auto_start_java=pool_config.get('auto_start_java', False),
                java_classpath=pool_config.get('java_classpath'),
                java_options=pool_config.get('java_options')
            )
        return _global_gateway_pool


class _JVMManagedGatewayPool:
    """
    Wrapper around JVMManager to provide JavaGatewayPool interface.
    v1.6.0: Allows seamless integration with existing JavaCalculator code.
    """
    
    def __init__(self, jvm_manager):
        self.jvm_manager = jvm_manager
        self._stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'reconnections': 0
        }
        self._stats_lock = threading.Lock()
        self._initialized = True
    
    def initialize(self):
        """Already initialized via JVMManager"""
        pass
    
    def get_gateway(self) -> JavaGateway:
        """Get a gateway from JVMManager"""
        gateway = self.jvm_manager.get_gateway_from_pool("primary")
        if gateway is None:
            raise RuntimeError("No gateway available from JVMManager")
        return gateway
    
    def execute_on_gateway(self, func, *args, **kwargs):
        """Execute function using JVMManager's gateway"""
        with self._stats_lock:
            self._stats['total_requests'] += 1
        
        gateway = self.get_gateway()
        try:
            result = func(gateway, *args, **kwargs)
            with self._stats_lock:
                self._stats['successful_requests'] += 1
            return result
        except Exception as e:
            with self._stats_lock:
                self._stats['failed_requests'] += 1
            raise
    
    def shutdown(self):
        """Delegate to JVMManager"""
        # Don't shutdown JVMManager here, it's managed centrally
        pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics"""
        with self._stats_lock:
            jvm_status = self.jvm_manager.get_status()
            primary_status = jvm_status.get('gateways', {}).get('primary', {})
            return {
                **self._stats,
                'pool_size': primary_status.get('active_connections', 0),
                'active_connections': primary_status.get('active_connections', 0),
                'jvm_managed': True
            }


def shutdown_gateway_pool():
    """Shutdown the global gateway pool."""
    global _global_gateway_pool
    
    with _pool_lock:
        if _global_gateway_pool:
            _global_gateway_pool.shutdown()
            _global_gateway_pool = None


