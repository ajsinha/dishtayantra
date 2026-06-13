"""
Connection Pool Types & Maintenance (v2.2 module split)
=======================================================

Enums, dataclasses, statistics, and the pool-maintenance mixin (validation,
destruction, eviction thread, abandoned-connection sweep), extracted
verbatim from db_connection_pool.py to respect the 500-line architecture
limit. Re-exported from core.db.db_connection_pool.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

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




class DatabaseType(Enum):
    """Supported database types."""
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLSERVER = "sqlserver"
    SQLITE = "sqlite"
    ORACLE = "oracle"


class ConnectionState(Enum):
    """Connection states."""
    IDLE = "idle"
    IN_USE = "in_use"
    TESTING = "testing"
    INVALID = "invalid"
    CLOSED = "closed"


class EvictionPolicy(Enum):
    """Connection eviction policies."""
    IDLE_TIME = "idle_time"  # Evict connections idle for too long
    LIFETIME = "lifetime"  # Evict connections that are too old
    SOFT_MIN_IDLE = "soft_min_idle"  # Soft minimum idle connections
    LRU = "lru"  # Least recently used


@dataclass
class ConnectionWrapper:
    """Wrapper for database connections with metadata."""
    connection: Any
    connection_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    state: ConnectionState = ConnectionState.IDLE
    created_time: datetime = field(default_factory=datetime.now)
    last_used_time: datetime = field(default_factory=datetime.now)
    last_tested_time: datetime = field(default_factory=datetime.now)
    use_count: int = 0
    error_count: int = 0
    in_transaction: bool = False
    test_query: str = "SELECT 1"
    borrowed_by: Optional[threading.Thread] = None

    def update_last_used(self):
        """Update last used timestamp."""
        self.last_used_time = datetime.now()
        self.use_count += 1

    def is_expired(self, max_lifetime_seconds: int) -> bool:
        """Check if connection has exceeded maximum lifetime."""
        if max_lifetime_seconds <= 0:
            return False
        age = (datetime.now() - self.created_time).total_seconds()
        return age > max_lifetime_seconds

    def is_idle_too_long(self, max_idle_seconds: int) -> bool:
        """Check if connection has been idle for too long."""
        if max_idle_seconds <= 0:
            return False
        idle_time = (datetime.now() - self.last_used_time).total_seconds()
        return idle_time > max_idle_seconds

    def needs_validation(self, test_interval_seconds: int) -> bool:
        """Check if connection needs validation."""
        if test_interval_seconds <= 0:
            return False
        time_since_test = (datetime.now() - self.last_tested_time).total_seconds()
        return time_since_test > test_interval_seconds



@dataclass
class PoolConfig:
    """Configuration for the connection pool."""
    # Basic settings
    min_idle: int = 2  # Minimum idle connections
    max_idle: int = 8  # Maximum idle connections
    max_total: int = 20  # Maximum total connections

    # Connection validation
    test_on_borrow: bool = True  # Test connection before borrowing
    test_on_return: bool = False  # Test connection on return
    test_while_idle: bool = True  # Test idle connections periodically
    validation_query: Optional[str] = None  # Query to validate connections
    validation_timeout: int = 5  # Timeout for validation query (seconds)

    # Eviction settings
    time_between_eviction_runs: int = 30  # Seconds between eviction runs
    min_evictable_idle_time: int = 300  # Min idle time before eviction (seconds)
    max_connection_lifetime: int = 3600  # Max connection lifetime (seconds)
    num_tests_per_eviction_run: int = 3  # Number of connections to test per run
    eviction_policy: EvictionPolicy = EvictionPolicy.IDLE_TIME

    # Behavior settings
    block_when_exhausted: bool = True  # Block when pool is exhausted
    max_wait_time: int = 30  # Max wait time for connection (seconds)
    lifo: bool = True  # Last-in-first-out for idle connections
    fair: bool = False  # Fair mode (FIFO for waiting threads)

    # Connection settings
    connection_timeout: int = 10  # Timeout for creating connections (seconds)
    socket_timeout: int = 0  # Socket timeout (0 = no timeout)

    # Monitoring
    jmx_enabled: bool = True  # Enable JMX-style monitoring
    abandoned_remove: bool = True  # Remove abandoned connections
    abandoned_timeout: int = 300  # Timeout for abandoned connections (seconds)
    log_abandoned: bool = True  # Log abandoned connection info


class PoolStatistics:
    """Statistics for the connection pool."""

    def __init__(self):
        self.connections_created = 0
        self.connections_destroyed = 0
        self.connections_borrowed = 0
        self.connections_returned = 0
        self.connections_validated = 0
        self.connections_invalidated = 0
        self.wait_time_total = 0.0
        self.wait_count = 0
        self.max_wait_time = 0.0
        self._lock = threading.Lock()

    def record_creation(self):
        with self._lock:
            self.connections_created += 1

    def record_destruction(self):
        with self._lock:
            self.connections_destroyed += 1

    def record_borrow(self):
        with self._lock:
            self.connections_borrowed += 1

    def record_return(self):
        with self._lock:
            self.connections_returned += 1

    def record_validation(self):
        with self._lock:
            self.connections_validated += 1

    def record_invalidation(self):
        with self._lock:
            self.connections_invalidated += 1

    def record_wait(self, wait_time: float):
        with self._lock:
            self.wait_time_total += wait_time
            self.wait_count += 1
            self.max_wait_time = max(self.max_wait_time, wait_time)

    def get_average_wait_time(self) -> float:
        with self._lock:
            return self.wait_time_total / self.wait_count if self.wait_count > 0 else 0.0

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'connections_created': self.connections_created,
                'connections_destroyed': self.connections_destroyed,
                'connections_borrowed': self.connections_borrowed,
                'connections_returned': self.connections_returned,
                'connections_validated': self.connections_validated,
                'connections_invalidated': self.connections_invalidated,
                'average_wait_time': self.get_average_wait_time(),
                'max_wait_time': self.max_wait_time,
                'total_wait_count': self.wait_count
            }




class PoolMaintenanceMixin:
    """Validation, destruction, eviction, and abandoned-connection sweep
    for DatabaseConnectionPool (all state lives on the pool)."""

    def _validate_connection(self, wrapper: ConnectionWrapper) -> bool:
        """Validate a connection is still good."""
        if wrapper.state == ConnectionState.CLOSED:
            return False

        wrapper.state = ConnectionState.TESTING

        try:
            cursor = wrapper.connection.cursor()
            cursor.execute(wrapper.test_query)
            cursor.fetchone()
            cursor.close()

            wrapper.last_tested_time = datetime.now()
            wrapper.state = ConnectionState.IDLE
            self.stats.record_validation()

            return True

        except Exception as e:
            logger.debug(f"Connection {wrapper.connection_id} validation failed: {e}")
            wrapper.state = ConnectionState.INVALID
            wrapper.error_count += 1
            self.stats.record_invalidation()
            return False

    def _destroy_connection(self, wrapper: ConnectionWrapper):
        """Destroy a connection and remove from pool."""
        if wrapper.state == ConnectionState.CLOSED:
            return

        try:
            wrapper.connection.close()
        except:
            pass

        wrapper.state = ConnectionState.CLOSED

        with self._lock:
            # Remove from all tracking
            self._all_connections.pop(wrapper.connection_id, None)
            self._total_connections -= 1

        self.stats.record_destruction()
        logger.debug(f"Destroyed connection {wrapper.connection_id}")

    def _ensure_min_idle(self):
        """Ensure minimum idle connections are maintained."""
        with self._lock:
            current_idle = self._idle_connections.qsize()
            current_active = len(self._active_connections)
            current_total = current_idle + current_active

            needed = max(0, min(
                self.config.min_idle - current_idle,
                self.config.max_total - current_total
            ))

            for _ in range(needed):
                try:
                    wrapper = self._create_connection()
                    if wrapper:
                        self._idle_connections.put_nowait(wrapper)
                except Full:
                    break
                except Exception as e:
                    logger.warning(f"Failed to create connection for min idle: {e}")

    def _evict_connections(self):
        """Evict idle connections based on eviction policy."""
        evicted = []
        tested = 0
        max_tests = self.config.num_tests_per_eviction_run

        # Collect connections to test
        connections_to_test = []

        for _ in range(self._idle_connections.qsize()):
            try:
                wrapper = self._idle_connections.get_nowait()
                connections_to_test.append(wrapper)
            except Empty:
                break

        # Test and evict connections
        for wrapper in connections_to_test:
            if tested >= max_tests:
                # Put back untested connections
                try:
                    self._idle_connections.put_nowait(wrapper)
                except Full:
                    evicted.append(wrapper)
                continue

            tested += 1
            should_evict = False

            # Check eviction criteria
            if wrapper.is_expired(self.config.max_connection_lifetime):
                should_evict = True
                logger.debug(f"Connection {wrapper.connection_id} exceeded lifetime")

            elif wrapper.is_idle_too_long(self.config.min_evictable_idle_time):
                if self._idle_connections.qsize() > self.config.min_idle:
                    should_evict = True
                    logger.debug(f"Connection {wrapper.connection_id} idle too long")

            elif wrapper.error_count > 3:
                should_evict = True
                logger.debug(f"Connection {wrapper.connection_id} has too many errors")

            elif self.config.test_while_idle and not self._validate_connection(wrapper):
                should_evict = True
                logger.debug(f"Connection {wrapper.connection_id} failed validation")

            if should_evict:
                evicted.append(wrapper)
            else:
                try:
                    self._idle_connections.put_nowait(wrapper)
                except Full:
                    evicted.append(wrapper)

        # Destroy evicted connections
        for wrapper in evicted:
            self._destroy_connection(wrapper)

        # Ensure minimum idle connections
        self._ensure_min_idle()

        return len(evicted)

    def _eviction_thread_run(self):
        """Background thread for connection eviction."""
        logger.info("Starting eviction thread")

        while not self._stop_eviction.is_set():
            try:
                # Wait for configured interval
                self._stop_eviction.wait(self.config.time_between_eviction_runs)

                if self._stop_eviction.is_set():
                    break

                # Run eviction
                evicted = self._evict_connections()
                if evicted > 0:
                    logger.info(f"Evicted {evicted} connections")

                # Check for abandoned connections
                if self.config.abandoned_remove:
                    self._remove_abandoned_connections()

            except Exception as e:
                logger.error(f"Error in eviction thread: {e}")

        logger.info("Eviction thread stopped")

    def _start_eviction_thread(self):
        """Start the background eviction thread."""
        if self._eviction_thread and self._eviction_thread.is_alive():
            return

        self._stop_eviction.clear()
        self._eviction_thread = threading.Thread(
            target=self._eviction_thread_run,
            daemon=True
        )
        self._eviction_thread.start()

    def _remove_abandoned_connections(self):
        """Remove connections that have been abandoned by threads."""
        now = datetime.now()
        abandoned = []

        with self._lock:
            for thread_id, wrapper in list(self._active_connections.items()):
                # Check if thread is still alive
                if wrapper.borrowed_by and not wrapper.borrowed_by.is_alive():
                    abandoned.append((thread_id, wrapper))
                    logger.warning(f"Found abandoned connection from dead thread: {wrapper.connection_id}")

                # Check if connection has been borrowed too long
                elif (now - wrapper.last_used_time).total_seconds() > self.config.abandoned_timeout:
                    abandoned.append((thread_id, wrapper))
                    if self.config.log_abandoned:
                        logger.warning(f"Connection {wrapper.connection_id} abandoned for "
                                       f"{(now - wrapper.last_used_time).total_seconds():.1f} seconds")

            # Return abandoned connections to pool or destroy them
            for thread_id, wrapper in abandoned:
                del self._active_connections[thread_id]

                # Validate and return to pool or destroy
                if self._validate_connection(wrapper):
                    wrapper.state = ConnectionState.IDLE
                    wrapper.borrowed_by = None
                    try:
                        self._idle_connections.put_nowait(wrapper)
                    except Full:
                        self._destroy_connection(wrapper)
                else:
                    self._destroy_connection(wrapper)
