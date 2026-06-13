import logging
import time
import threading
import queue
from queue import Queue, Empty, Full
from typing import Dict, Optional, Any, List, Tuple, Union, Callable
from dataclasses import dataclass, field
from enum import Enum
from contextlib import contextmanager
from datetime import datetime, timedelta
import weakref
import uuid

# Database driver imports
try:
    import psycopg2
    from psycopg2.pool import ThreadedConnectionPool

    HAS_POSTGRESQL = True
except ImportError:
    HAS_POSTGRESQL = False

try:
    import pymysql

    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

try:
    import pyodbc

    HAS_SQLSERVER = True
except ImportError:
    HAS_SQLSERVER = False

try:
    import sqlite3

    HAS_SQLITE = True
except ImportError:
    HAS_SQLITE = False

try:
    import cx_Oracle

    HAS_ORACLE = True
except ImportError:
    HAS_ORACLE = False

logger = logging.getLogger(__name__)



# v2.2 module split: types + maintenance mixin live in db_pool_types.py.
from core.db.db_pool_types import (  # noqa: F401
    ConnectionState,
    ConnectionWrapper,
    DatabaseType,
    EvictionPolicy,
    PoolConfig,
    PoolMaintenanceMixin,
    PoolStatistics,
)


class DatabaseConnectionPool(PoolMaintenanceMixin):
    """
    Enterprise-grade database connection pool similar to Apache DBCP.

    Features:
    - Multiple database support (PostgreSQL, MySQL, SQLite, Oracle, SQL Server)
    - Connection validation and health checking
    - Automatic eviction of bad/idle connections
    - Connection lifecycle management
    - Abandoned connection detection and removal
    - Comprehensive statistics and monitoring
    - Thread-safe operations
    """

    def __init__(self,
                 db_type: DatabaseType,
                 config: Optional[PoolConfig] = None,
                 **connection_kwargs):
        """
        Initialize the connection pool.

        Args:
            db_type: Type of database
            config: Pool configuration
            **connection_kwargs: Database connection parameters
        """
        self.db_type = db_type
        self.config = config or PoolConfig()
        self.connection_kwargs = connection_kwargs

        # Validate database driver
        self._validate_database_type()

        # Connection storage
        self._idle_connections = Queue(maxsize=self.config.max_total)
        self._active_connections = {}  # thread_id -> ConnectionWrapper
        self._all_connections = {}  # connection_id -> ConnectionWrapper

        # Thread safety
        self._lock = threading.RLock()
        self._semaphore = threading.Semaphore(self.config.max_total)
        self._waiters = Queue() if self.config.fair else None

        # Pool state
        self._closed = False
        self._total_connections = 0

        # Statistics
        self.stats = PoolStatistics()

        # Eviction thread
        self._eviction_thread = None
        self._stop_eviction = threading.Event()

        # Abandoned connection tracking
        self._abandoned_tracker = weakref.WeakValueDictionary()

        # Initialize the pool
        self._initialize_pool()

    def _validate_database_type(self):
        """Validate that required database driver is installed."""
        if self.db_type == DatabaseType.POSTGRESQL and not HAS_POSTGRESQL:
            raise ImportError("PostgreSQL support requires 'psycopg2' package")
        elif self.db_type == DatabaseType.MYSQL and not HAS_MYSQL:
            raise ImportError("MySQL support requires 'pymysql' package")
        elif self.db_type == DatabaseType.SQLSERVER and not HAS_SQLSERVER:
            raise ImportError("SQL Server support requires 'pyodbc' package")
        elif self.db_type == DatabaseType.SQLITE and not HAS_SQLITE:
            raise ImportError("SQLite support should be built-in")
        elif self.db_type == DatabaseType.ORACLE and not HAS_ORACLE:
            raise ImportError("Oracle support requires 'cx_Oracle' package")

    def _initialize_pool(self):
        """Initialize the connection pool with minimum connections."""
        logger.info(f"Initializing connection pool for {self.db_type.value}")

        # Create initial connections
        for i in range(self.config.min_idle):
            try:
                conn_wrapper = self._create_connection()
                if conn_wrapper:
                    self._idle_connections.put(conn_wrapper)
                    logger.debug(f"Created initial connection {i + 1}/{self.config.min_idle}")
            except Exception as e:
                logger.warning(f"Failed to create initial connection: {e}")

        # Start eviction thread
        if self.config.time_between_eviction_runs > 0:
            self._start_eviction_thread()

    def _create_connection(self) -> Optional[ConnectionWrapper]:
        """Create a new database connection."""
        with self._lock:
            if self._closed:
                raise RuntimeError("Pool is closed")

            if self._total_connections >= self.config.max_total:
                return None

        try:
            # Create raw connection based on database type
            if self.db_type == DatabaseType.POSTGRESQL:
                conn = psycopg2.connect(**self.connection_kwargs)
                test_query = "SELECT 1"

            elif self.db_type == DatabaseType.MYSQL:
                conn = pymysql.connect(**self.connection_kwargs)
                test_query = "SELECT 1"

            elif self.db_type == DatabaseType.SQLITE:
                conn = sqlite3.connect(**self.connection_kwargs)
                test_query = "SELECT 1"

            elif self.db_type == DatabaseType.SQLSERVER:
                conn_str = self.connection_kwargs.get('connection_string', '')
                conn = pyodbc.connect(conn_str)
                test_query = "SELECT 1"

            elif self.db_type == DatabaseType.ORACLE:
                conn = cx_Oracle.connect(**self.connection_kwargs)
                test_query = "SELECT 1 FROM DUAL"

            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")

            # Set connection properties
            if self.config.socket_timeout > 0:
                self._set_socket_timeout(conn, self.config.socket_timeout)

            # Create wrapper
            wrapper = ConnectionWrapper(
                connection=conn,
                test_query=self.config.validation_query or test_query
            )

            # Register connection
            with self._lock:
                self._all_connections[wrapper.connection_id] = wrapper
                self._total_connections += 1

            self.stats.record_creation()
            logger.debug(f"Created new connection {wrapper.connection_id}")

            return wrapper

        except Exception as e:
            logger.error(f"Failed to create connection: {e}")
            self.stats.record_invalidation()
            raise

    def _set_socket_timeout(self, conn: Any, timeout: int):
        """Set socket timeout on the connection."""
        if self.db_type == DatabaseType.POSTGRESQL:
            conn.set_session(readonly=False, autocommit=False)
            cursor = conn.cursor()
            cursor.execute(f"SET statement_timeout = {timeout * 1000}")
            cursor.close()
        elif self.db_type == DatabaseType.MYSQL:
            conn.query(f"SET SESSION wait_timeout = {timeout}")

    @contextmanager
    def get_connection(self, timeout: Optional[float] = None):
        """
        Get a connection from the pool (context manager).

        Args:
            timeout: Maximum time to wait for a connection

        Yields:
            Database connection object
        """
        conn_wrapper = self.borrow_connection(timeout)
        try:
            yield conn_wrapper.connection
        finally:
            self.return_connection(conn_wrapper)

    def borrow_connection(self, timeout: Optional[float] = None) -> ConnectionWrapper:
        """
        Borrow a connection from the pool.

        Args:
            timeout: Maximum time to wait for a connection

        Returns:
            ConnectionWrapper object
        """
        if self._closed:
            raise RuntimeError("Pool is closed")

        timeout = timeout or self.config.max_wait_time
        start_time = time.time()
        thread_id = threading.get_ident()

        # Check if thread already has a connection
        with self._lock:
            if thread_id in self._active_connections:
                wrapper = self._active_connections[thread_id]
                logger.warning(f"Thread {thread_id} already has connection {wrapper.connection_id}")
                return wrapper

        wrapper = None
        wait_time = 0

        while wrapper is None:
            try:
                # Try to get an idle connection
                remaining_timeout = max(0, timeout - (time.time() - start_time))

                if remaining_timeout <= 0 and self.config.block_when_exhausted:
                    raise TimeoutError("Timeout waiting for connection")

                try:
                    wrapper = self._idle_connections.get(
                        block=self.config.block_when_exhausted,
                        timeout=remaining_timeout if self.config.block_when_exhausted else 0
                    )
                except Empty:
                    # Try to create a new connection if under max_total
                    with self._lock:
                        if self._total_connections < self.config.max_total:
                            wrapper = self._create_connection()

                    if wrapper is None:
                        if not self.config.block_when_exhausted:
                            raise RuntimeError("No connections available")
                        continue

                # Validate connection if required
                if wrapper and self.config.test_on_borrow:
                    if not self._validate_connection(wrapper):
                        self._destroy_connection(wrapper)
                        wrapper = None
                        continue

                # Check if connection needs replacement
                if wrapper and wrapper.is_expired(self.config.max_connection_lifetime):
                    self._destroy_connection(wrapper)
                    wrapper = self._create_connection()

            except TimeoutError:
                wait_time = time.time() - start_time
                self.stats.record_wait(wait_time)
                raise

        # Mark connection as active
        wrapper.state = ConnectionState.IN_USE
        wrapper.borrowed_by = threading.current_thread()
        wrapper.update_last_used()

        with self._lock:
            self._active_connections[thread_id] = wrapper

        wait_time = time.time() - start_time
        if wait_time > 0:
            self.stats.record_wait(wait_time)

        self.stats.record_borrow()
        logger.debug(f"Borrowed connection {wrapper.connection_id} for thread {thread_id}")

        return wrapper

    def return_connection(self, wrapper: ConnectionWrapper):
        """
        Return a connection to the pool.

        Args:
            wrapper: ConnectionWrapper to return
        """
        if self._closed:
            self._destroy_connection(wrapper)
            return

        thread_id = threading.get_ident()

        # Remove from active connections
        with self._lock:
            if thread_id in self._active_connections:
                if self._active_connections[thread_id].connection_id != wrapper.connection_id:
                    logger.warning(f"Thread {thread_id} returning different connection than borrowed")
                del self._active_connections[thread_id]

        # Check if connection should be destroyed
        should_destroy = False

        if wrapper.state == ConnectionState.INVALID:
            should_destroy = True
        elif wrapper.in_transaction:
            # Rollback any open transaction
            try:
                wrapper.connection.rollback()
                wrapper.in_transaction = False
            except:
                should_destroy = True
        elif self.config.test_on_return and not self._validate_connection(wrapper):
            should_destroy = True
        elif self._idle_connections.qsize() >= self.config.max_idle:
            should_destroy = True

        if should_destroy:
            self._destroy_connection(wrapper)
        else:
            wrapper.state = ConnectionState.IDLE
            wrapper.borrowed_by = None

            try:
                self._idle_connections.put_nowait(wrapper)
            except Full:
                self._destroy_connection(wrapper)

        self.stats.record_return()
        logger.debug(f"Returned connection {wrapper.connection_id}")

    def invalidate_connection(self, wrapper: ConnectionWrapper):
        """
        Invalidate a connection, marking it for destruction.

        Args:
            wrapper: ConnectionWrapper to invalidate
        """
        wrapper.state = ConnectionState.INVALID
        self.return_connection(wrapper)

    def get_pool_status(self) -> Dict[str, Any]:
        """Get current pool status and statistics."""
        with self._lock:
            idle_count = self._idle_connections.qsize()
            active_count = len(self._active_connections)

            return {
                'pool_state': 'closed' if self._closed else 'active',
                'idle_connections': idle_count,
                'active_connections': active_count,
                'total_connections': self._total_connections,
                'max_total': self.config.max_total,
                'max_idle': self.config.max_idle,
                'min_idle': self.config.min_idle,
                'statistics': self.stats.get_stats()
            }

    def clear_idle_connections(self):
        """Clear all idle connections from the pool."""
        cleared = 0

        while not self._idle_connections.empty():
            try:
                wrapper = self._idle_connections.get_nowait()
                self._destroy_connection(wrapper)
                cleared += 1
            except Empty:
                break

        logger.info(f"Cleared {cleared} idle connections")
        self._ensure_min_idle()

    def close(self):
        """Close the connection pool and all connections."""
        if self._closed:
            return

        logger.info("Closing connection pool")
        self._closed = True

        # Stop eviction thread
        if self._eviction_thread:
            self._stop_eviction.set()
            self._eviction_thread.join(timeout=5)

        # Close all connections
        with self._lock:
            # Close idle connections
            while not self._idle_connections.empty():
                try:
                    wrapper = self._idle_connections.get_nowait()
                    self._destroy_connection(wrapper)
                except Empty:
                    break

            # Close active connections
            for wrapper in list(self._active_connections.values()):
                self._destroy_connection(wrapper)

            self._active_connections.clear()
            self._all_connections.clear()

        logger.info(f"Connection pool closed. Final stats: {self.stats.get_stats()}")


# Convenience function to create pre-configured pools
def create_pool(db_type: DatabaseType,
                min_size: int = 2,
                max_size: int = 10,
                **connection_kwargs) -> DatabaseConnectionPool:
    """
    Create a pre-configured connection pool.

    Args:
        db_type: Type of database
        min_size: Minimum pool size
        max_size: Maximum pool size
        **connection_kwargs: Database connection parameters

    Returns:
        Configured DatabaseConnectionPool
    """
    config = PoolConfig(
        min_idle=min_size,
        max_idle=max_size,
        max_total=max_size,
        test_on_borrow=True,
        test_while_idle=True,
        time_between_eviction_runs=30,
        min_evictable_idle_time=300,
        max_connection_lifetime=3600
    )

    return DatabaseConnectionPool(db_type, config, **connection_kwargs)


# Example usage

# v2.2: the usage demo moved to examples/demo_db_connection_pool.py
