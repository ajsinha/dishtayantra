"""
Pool Manager Config & Housekeeping (v2.2 module split)
======================================================

ConnectionConfig / PoolInfo dataclasses plus the housekeeping mixin (idle
pool eviction, cleanup thread, tuning setters), extracted verbatim from
db_connection_pool_manager.py to respect the 500-line architecture limit.
Re-exported from core.db.db_connection_pool_manager.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

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


@dataclass


@dataclass
class ConnectionConfig:
    """Configuration for a database connection."""
    db_type: DatabaseType
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    # Additional connection parameters
    charset: Optional[str] = None
    autocommit: bool = False
    ssl: bool = False
    ssl_ca: Optional[str] = None
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    connection_timeout: int = 10
    socket_timeout: int = 0
    application_name: Optional[str] = None

    def to_key(self) -> str:
        """Generate a unique key for this configuration."""
        # Create a dictionary of non-sensitive config items for the key
        key_dict = {
            'db_type': self.db_type.value,
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            # Don't include password in the key for security
            'charset': self.charset,
            'autocommit': self.autocommit,
            'ssl': self.ssl,
            'ssl_ca': self.ssl_ca,
            'ssl_cert': self.ssl_cert,
            'ssl_key': self.ssl_key,
            'connection_timeout': self.connection_timeout,
            'socket_timeout': self.socket_timeout,
            'application_name': self.application_name
        }

        # Create a stable JSON representation
        key_json = json.dumps(key_dict, sort_keys=True)

        # Generate SHA256 hash for the key
        return hashlib.sha256(key_json.encode()).hexdigest()

    def to_connection_kwargs(self) -> Dict[str, Any]:
        """Convert to kwargs suitable for database connection."""
        kwargs = {}

        if self.db_type == DatabaseType.POSTGRESQL:
            if self.host:
                kwargs['host'] = self.host
            if self.port:
                kwargs['port'] = self.port
            if self.database:
                kwargs['database'] = self.database
            if self.user:
                kwargs['user'] = self.user
            if self.password:
                kwargs['password'] = self.password
            if self.application_name:
                kwargs['application_name'] = self.application_name
            if self.ssl:
                kwargs['sslmode'] = 'require'
                if self.ssl_ca:
                    kwargs['sslrootcert'] = self.ssl_ca
                if self.ssl_cert:
                    kwargs['sslcert'] = self.ssl_cert
                if self.ssl_key:
                    kwargs['sslkey'] = self.ssl_key

        elif self.db_type == DatabaseType.MYSQL:
            if self.host:
                kwargs['host'] = self.host
            if self.port:
                kwargs['port'] = self.port
            if self.database:
                kwargs['database'] = self.database
            if self.user:
                kwargs['user'] = self.user
            if self.password:
                kwargs['password'] = self.password
            if self.charset:
                kwargs['charset'] = self.charset
            kwargs['autocommit'] = self.autocommit
            if self.ssl:
                ssl_dict = {}
                if self.ssl_ca:
                    ssl_dict['ca'] = self.ssl_ca
                if self.ssl_cert:
                    ssl_dict['cert'] = self.ssl_cert
                if self.ssl_key:
                    ssl_dict['key'] = self.ssl_key
                if ssl_dict:
                    kwargs['ssl'] = ssl_dict

        elif self.db_type == DatabaseType.SQLITE:
            kwargs['database'] = self.database or ':memory:'
            kwargs['timeout'] = self.connection_timeout

        elif self.db_type == DatabaseType.SQLSERVER:
            # Build connection string for SQL Server
            conn_str_parts = []
            conn_str_parts.append(f"DRIVER={{ODBC Driver 17 for SQL Server}}")
            if self.host:
                conn_str_parts.append(f"SERVER={self.host},{self.port or 1433}")
            if self.database:
                conn_str_parts.append(f"DATABASE={self.database}")
            if self.user:
                conn_str_parts.append(f"UID={self.user}")
            if self.password:
                conn_str_parts.append(f"PWD={self.password}")
            if self.ssl:
                conn_str_parts.append("Encrypt=yes")
                conn_str_parts.append("TrustServerCertificate=yes")
            kwargs['connection_string'] = ';'.join(conn_str_parts)

        elif self.db_type == DatabaseType.ORACLE:
            if self.host and self.port:
                import cx_Oracle
                kwargs['dsn'] = cx_Oracle.makedsn(
                    self.host,
                    self.port,
                    service_name=self.database
                )
            if self.user:
                kwargs['user'] = self.user
            if self.password:
                kwargs['password'] = self.password

        return kwargs


@dataclass
class PoolInfo:
    """Information about a managed pool."""
    pool_key: str
    pool: DatabaseConnectionPool
    config: ConnectionConfig
    pool_config: PoolConfig
    created_time: datetime
    last_accessed_time: datetime
    access_count: int = 0
    reference_count: int = 0

    def update_access(self):
        """Update access statistics."""
        self.last_accessed_time = datetime.now()
        self.access_count += 1




class PoolHousekeepingMixin:
    """Idle-pool eviction, cleanup thread, and tuning setters for
    DBConnectionPoolManager (all state lives on the manager)."""

    def clear_idle_pools(self, idle_time_seconds: Optional[int] = None):
        """
        Clear pools that have been idle for specified time.

        Args:
            idle_time_seconds: Idle time threshold (uses default if None)
        """
        idle_time = idle_time_seconds or self._pool_idle_timeout
        now = datetime.now()
        pools_to_remove = []

        with self._pool_lock:
            for pool_key, pool_info in self._pools.items():
                # Check if pool is idle
                idle_duration = (now - pool_info.last_accessed_time).total_seconds()

                if idle_duration > idle_time and pool_info.reference_count == 0:
                    # Check if pool has no active connections
                    status = pool_info.pool.get_pool_status()
                    if status['active_connections'] == 0:
                        pools_to_remove.append(pool_key)

        # Remove idle pools
        removed = 0
        for pool_key in pools_to_remove:
            if self.remove_pool(pool_key):
                removed += 1

        if removed > 0:
            logger.info(f"Cleared {removed} idle pools")

        return removed

    def _evict_idle_pool(self):
        """Evict the least recently used idle pool."""
        with self._pool_lock:
            # Find the least recently used pool with no active connections
            oldest_pool_key = None
            oldest_access_time = datetime.now()

            for pool_key, pool_info in self._pools.items():
                if pool_info.reference_count == 0:
                    status = pool_info.pool.get_pool_status()
                    if status['active_connections'] == 0:
                        if pool_info.last_accessed_time < oldest_access_time:
                            oldest_access_time = pool_info.last_accessed_time
                            oldest_pool_key = pool_key

            if oldest_pool_key:
                self.remove_pool(oldest_pool_key)
                logger.info(f"Evicted idle pool {oldest_pool_key[:8]}...")
                return True

            return False

    def _cleanup_thread_run(self):
        """Background thread for cleaning up idle pools."""
        logger.info("Starting pool cleanup thread")

        while not self._stop_cleanup.is_set():
            try:
                # Wait for cleanup interval
                self._stop_cleanup.wait(self._cleanup_interval)

                if self._stop_cleanup.is_set():
                    break

                # Clear idle pools
                removed = self.clear_idle_pools()

                # Log statistics periodically
                if self._enable_jmx:
                    stats = self.get_statistics()
                    logger.debug(f"Pool manager stats: {stats}")

            except Exception as e:
                logger.error(f"Error in cleanup thread: {e}")

        logger.info("Pool cleanup thread stopped")

    def _start_cleanup_thread(self):
        """Start the background cleanup thread."""
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            return

        self._stop_cleanup.clear()
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_thread_run,
            daemon=True,
            name="DBPoolManager-Cleanup"
        )
        self._cleanup_thread.start()

    def set_default_pool_config(self, config: PoolConfig):
        """
        Set the default pool configuration for new pools.

        Args:
            config: Default PoolConfig to use
        """
        self._default_pool_config = config
        logger.info("Updated default pool configuration")

    def set_max_pools(self, max_pools: int):
        """
        Set the maximum number of pools to manage.

        Args:
            max_pools: Maximum number of pools
        """
        if max_pools < 1:
            raise ValueError("max_pools must be at least 1")

        self._max_pools = max_pools
        logger.info(f"Set maximum pools to {max_pools}")

    def set_pool_idle_timeout(self, timeout_seconds: int):
        """
        Set the idle timeout for pools.

        Args:
            timeout_seconds: Idle timeout in seconds
        """
        self._pool_idle_timeout = timeout_seconds
        logger.info(f"Set pool idle timeout to {timeout_seconds} seconds")


class PoolStatsMixin:
    """Pool info / statistics / health-check reporting for
    DBConnectionPoolManager (all state lives on the manager)."""

    def get_pool_info(self, pool_key: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a specific pool.

        Args:
            pool_key: Key of the pool

        Returns:
            Dictionary with pool information or None if not found
        """
        with self._pool_lock:
            if pool_key not in self._pools:
                return None

            pool_info = self._pools[pool_key]
            status = pool_info.pool.get_pool_status()

            return {
                'pool_key': pool_key,
                'db_type': pool_info.config.db_type.value,
                'database': pool_info.config.database,
                'created_time': pool_info.created_time.isoformat(),
                'last_accessed_time': pool_info.last_accessed_time.isoformat(),
                'access_count': pool_info.access_count,
                'reference_count': pool_info.reference_count,
                'pool_status': status
            }

    def get_all_pools_info(self) -> List[Dict[str, Any]]:
        """
        Get information about all managed pools.

        Returns:
            List of dictionaries with pool information
        """
        with self._pool_lock:
            pools_info = []
            for pool_key in self._pools:
                info = self.get_pool_info(pool_key)
                if info:
                    pools_info.append(info)
            return pools_info

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get manager statistics.

        Returns:
            Dictionary with manager statistics
        """
        with self._pool_lock:
            total_idle = 0
            total_active = 0
            total_connections = 0

            for pool_info in self._pools.values():
                status = pool_info.pool.get_pool_status()
                total_idle += status['idle_connections']
                total_active += status['active_connections']
                total_connections += status['total_connections']

            return {
                'manager_stats': {
                    'total_pools': len(self._pools),
                    'total_pools_created': self._total_pools_created,
                    'total_pools_destroyed': self._total_pools_destroyed,
                    'total_connections_served': self._total_connections_served,
                    'max_pools': self._max_pools,
                    'pool_idle_timeout': self._pool_idle_timeout
                },
                'aggregate_pool_stats': {
                    'total_idle_connections': total_idle,
                    'total_active_connections': total_active,
                    'total_connections': total_connections
                },
                'pools': [
                    {
                        'key': key[:8] + '...',
                        'db_type': info.config.db_type.value,
                        'database': info.config.database,
                        'access_count': info.access_count,
                        'idle': info.pool.get_pool_status()['idle_connections'],
                        'active': info.pool.get_pool_status()['active_connections']
                    }
                    for key, info in self._pools.items()
                ]
            }

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on all pools.

        Returns:
            Dictionary with health status
        """
        with self._pool_lock:
            healthy_pools = 0
            unhealthy_pools = []

            for pool_key, pool_info in self._pools.items():
                try:
                    # Try to get a connection
                    with pool_info.pool.get_connection(timeout=5) as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT 1")
                        cursor.close()
                    healthy_pools += 1
                except Exception as e:
                    unhealthy_pools.append({
                        'pool_key': pool_key[:8] + '...',
                        'error': str(e)
                    })

            return {
                'status': 'healthy' if not unhealthy_pools else 'degraded',
                'healthy_pools': healthy_pools,
                'unhealthy_pools': unhealthy_pools,
                'total_pools': len(self._pools)
            }

