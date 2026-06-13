import logging
import hashlib
import json
import threading
import weakref
import atexit
from typing import Dict, Optional, Any, List, Tuple, Union
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from contextlib import contextmanager
import pickle

# Import the DatabaseConnectionPool and related classes from previous implementation
from core.db.db_connection_pool import (
    DatabaseConnectionPool,
    DatabaseType,
    PoolConfig,
    ConnectionWrapper,
    PoolStatistics
)

logger = logging.getLogger(__name__)


# v2.2 module split: config types + housekeeping live in db_pool_config.py.
from core.db.db_pool_config import (  # noqa: F401
    ConnectionConfig,
    PoolHousekeepingMixin,
    PoolInfo,
    PoolStatsMixin,
)


class DBConnectionPoolManager(PoolHousekeepingMixin, PoolStatsMixin):
    """
    Singleton manager for database connection pools.

    This class manages multiple connection pools, creating new ones as needed
    and reusing existing pools based on connection configuration.

    Features:
    - Singleton pattern ensures only one manager instance
    - Automatic pool creation based on connection config
    - Pool reuse for identical configurations
    - Lazy pool initialization
    - Automatic cleanup of unused pools
    - Thread-safe operations
    - Comprehensive monitoring and statistics
    """

    _instance = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls):
        """Implement singleton pattern."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize the pool manager."""
        # Only initialize once
        if DBConnectionPoolManager._initialized:
            return

        with DBConnectionPoolManager._lock:
            if DBConnectionPoolManager._initialized:
                return

            logger.info("Initializing DBConnectionPoolManager")

            # Pool storage
            self._pools: Dict[str, PoolInfo] = {}
            self._pool_lock = threading.RLock()

            # Default pool configuration
            self._default_pool_config = PoolConfig(
                min_idle=2,
                max_idle=10,
                max_total=20,
                test_on_borrow=True,
                test_while_idle=True,
                time_between_eviction_runs=60,
                min_evictable_idle_time=300,
                max_connection_lifetime=3600
            )

            # Manager configuration
            self._max_pools = 50  # Maximum number of pools to manage
            self._pool_idle_timeout = 1800  # Remove pools idle for 30 minutes
            self._enable_jmx = True  # Enable monitoring

            # Cleanup thread
            self._cleanup_thread = None
            self._stop_cleanup = threading.Event()
            self._cleanup_interval = 300  # 5 minutes

            # Statistics
            self._total_pools_created = 0
            self._total_pools_destroyed = 0
            self._total_connections_served = 0

            # Weak references for tracking pool usage
            self._pool_references = weakref.WeakValueDictionary()

            # Register cleanup on exit
            atexit.register(self.shutdown)

            # Start cleanup thread
            self._start_cleanup_thread()

            DBConnectionPoolManager._initialized = True

            logger.info("DBConnectionPoolManager initialized successfully")

    def get_pool(self,
                 connection_config: Optional[ConnectionConfig] = None,
                 pool_config: Optional[PoolConfig] = None,
                 **connection_kwargs) -> DatabaseConnectionPool:
        """
        Get or create a connection pool for the given configuration.

        Args:
            connection_config: ConnectionConfig object
            pool_config: Optional PoolConfig for the pool
            **connection_kwargs: Alternative way to specify connection parameters

        Returns:
            DatabaseConnectionPool instance
        """
        # Create ConnectionConfig if not provided
        if connection_config is None:
            if not connection_kwargs:
                raise ValueError("Either connection_config or connection_kwargs must be provided")

            # Extract db_type from kwargs
            db_type_str = connection_kwargs.pop('db_type', None)
            if not db_type_str:
                raise ValueError("db_type must be specified")

            db_type = DatabaseType(db_type_str) if isinstance(db_type_str, str) else db_type_str

            connection_config = ConnectionConfig(
                db_type=db_type,
                host=connection_kwargs.pop('host', None),
                port=connection_kwargs.pop('port', None),
                database=connection_kwargs.pop('database', None),
                user=connection_kwargs.pop('user', None),
                password=connection_kwargs.pop('password', None),
                **connection_kwargs
            )

        # Generate pool key
        pool_key = connection_config.to_key()

        with self._pool_lock:
            # Check if pool already exists
            if pool_key in self._pools:
                pool_info = self._pools[pool_key]
                pool_info.update_access()
                pool_info.reference_count += 1

                logger.debug(f"Reusing existing pool {pool_key[:8]}... "
                             f"(access_count={pool_info.access_count})")

                self._total_connections_served += 1
                return pool_info.pool

            # Check pool limit
            if len(self._pools) >= self._max_pools:
                # Try to evict an idle pool
                self._evict_idle_pool()

                # Check again
                if len(self._pools) >= self._max_pools:
                    raise RuntimeError(f"Maximum number of pools ({self._max_pools}) reached")

            # Create new pool
            logger.info(f"Creating new pool for key {pool_key[:8]}...")

            # Use provided pool config or default
            if pool_config is None:
                pool_config = self._default_pool_config

            # Create the pool
            connection_kwargs = connection_config.to_connection_kwargs()
            pool = DatabaseConnectionPool(
                db_type=connection_config.db_type,
                config=pool_config,
                **connection_kwargs
            )

            # Create pool info
            pool_info = PoolInfo(
                pool_key=pool_key,
                pool=pool,
                config=connection_config,
                pool_config=pool_config,
                created_time=datetime.now(),
                last_accessed_time=datetime.now(),
                access_count=1,
                reference_count=1
            )

            # Store the pool
            self._pools[pool_key] = pool_info
            self._pool_references[pool_key] = pool

            self._total_pools_created += 1
            self._total_connections_served += 1

            logger.info(f"Created new pool {pool_key[:8]}... "
                        f"(total_pools={len(self._pools)})")

            return pool

    def get_pool_by_name(self,
                         name: str,
                         connection_config: ConnectionConfig,
                         pool_config: Optional[PoolConfig] = None) -> DatabaseConnectionPool:
        """
        Get or create a named pool.

        This is an alternative interface that uses a custom name instead of
        auto-generated key.

        Args:
            name: Custom name for the pool
            connection_config: Connection configuration
            pool_config: Optional pool configuration

        Returns:
            DatabaseConnectionPool instance
        """
        # Override the key generation to use the custom name
        with self._pool_lock:
            if name in self._pools:
                pool_info = self._pools[name]
                pool_info.update_access()
                pool_info.reference_count += 1
                return pool_info.pool

            # Create new pool with custom name as key
            pool = self.get_pool(connection_config, pool_config)

            # Update the key in our storage
            original_key = connection_config.to_key()
            if original_key != name and original_key in self._pools:
                pool_info = self._pools.pop(original_key)
                pool_info.pool_key = name
                self._pools[name] = pool_info

            return pool

    @contextmanager
    def get_connection(self,
                       connection_config: Optional[ConnectionConfig] = None,
                       pool_config: Optional[PoolConfig] = None,
                       timeout: Optional[float] = None,
                       **connection_kwargs):
        """
        Get a database connection from a pool (context manager).

        This is a convenience method that gets a pool and then a connection
        from that pool.

        Args:
            connection_config: Connection configuration
            pool_config: Optional pool configuration
            timeout: Timeout for getting connection
            **connection_kwargs: Alternative connection parameters

        Yields:
            Database connection
        """
        pool = self.get_pool(connection_config, pool_config, **connection_kwargs)

        with pool.get_connection(timeout) as conn:
            yield conn

    def remove_pool(self, pool_key: str) -> bool:
        """
        Remove a pool from management.

        Args:
            pool_key: Key of the pool to remove

        Returns:
            True if pool was removed, False if not found
        """
        with self._pool_lock:
            if pool_key not in self._pools:
                return False

            pool_info = self._pools.pop(pool_key)

            # Close the pool
            try:
                pool_info.pool.close()
            except Exception as e:
                logger.error(f"Error closing pool {pool_key[:8]}...: {e}")

            self._total_pools_destroyed += 1

            logger.info(f"Removed pool {pool_key[:8]}...")
            return True

    def shutdown(self):
        """Shutdown the pool manager and close all pools."""
        logger.info("Shutting down DBConnectionPoolManager")

        # Stop cleanup thread
        if self._cleanup_thread:
            self._stop_cleanup.set()
            self._cleanup_thread.join(timeout=5)

        # Close all pools
        with self._pool_lock:
            for pool_key in list(self._pools.keys()):
                self.remove_pool(pool_key)

            self._pools.clear()

        logger.info(f"DBConnectionPoolManager shutdown complete. "
                    f"Total pools created: {self._total_pools_created}, "
                    f"destroyed: {self._total_pools_destroyed}")

    @classmethod
    def reset_instance(cls):
        """Reset the singleton instance (mainly for testing)."""
        with cls._lock:
            if cls._instance:
                cls._instance.shutdown()
            cls._instance = None
            cls._initialized = False


# Convenience function to get the singleton instance
def get_pool_manager() -> DBConnectionPoolManager:
    """Get the singleton DBConnectionPoolManager instance."""
    return DBConnectionPoolManager()


# Example usage

# v2.2: the usage demo moved to examples/demo_db_pool_manager.py
