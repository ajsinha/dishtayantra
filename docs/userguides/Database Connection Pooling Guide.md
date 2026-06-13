# Database Connection Pooling

© 2025-2030 Ashutosh Sinha

DishtaYantra provides enterprise-grade database connection pooling in two
layers: a `DatabaseConnectionPool` (a single pool, Apache-DBCP-style) and a
`DBConnectionPoolManager` (a singleton that manages and reuses many pools by
configuration). This guide covers both.

---

# Part 1 - DatabaseConnectionPool (a single pool)

A comprehensive `DatabaseConnectionPool` class that implements connection pooling similar to Apache DBCP with connection validation, eviction, and replenishment capabilities for multiple database backends.This comprehensive `DatabaseConnectionPool` class that implements enterprise-grade connection pooling similar to Apache DBCP has following key features:


## Key Features

### 1. **Multi-Database Support**
- PostgreSQL, MySQL, SQLite, Oracle, and SQL Server
- Database-specific optimizations and connection handling
- Automatic driver validation

### 2. **Connection Lifecycle Management**
- **Connection States**: IDLE, IN_USE, TESTING, INVALID, CLOSED
- **Automatic Creation**: Creates connections on demand up to max_total
- **Connection Validation**: Tests connections before borrowing, after returning, and while idle
- **Connection Eviction**: Removes stale, idle, or invalid connections

### 3. **Eviction Policies**
- **Idle Time**: Evict connections idle longer than threshold
- **Lifetime**: Evict connections older than max lifetime
- **Soft Min Idle**: Maintain minimum idle connections
- **LRU**: Least recently used eviction

### 4. **Advanced Features**
- **Abandoned Connection Detection**: Identifies and reclaims connections not returned by dead threads
- **Fair Mode**: FIFO queue for waiting threads
- **LIFO/FIFO**: Configurable idle connection retrieval
- **Connection Pooling Statistics**: Comprehensive metrics and monitoring
- **Thread Safety**: Full thread-safe implementation with locks and semaphores

### 5. **Configuration Options (PoolConfig)**
```python
config = PoolConfig(
    # Pool sizing
    min_idle=2,           # Minimum idle connections
    max_idle=8,           # Maximum idle connections  
    max_total=20,         # Maximum total connections
    
    # Validation
    test_on_borrow=True,  # Test before giving to client
    test_on_return=False, # Test when returning
    test_while_idle=True, # Test idle connections periodically
    validation_query=None,# Custom validation query
    
    # Eviction
    time_between_eviction_runs=30,  # Seconds
    min_evictable_idle_time=300,    # Seconds
    max_connection_lifetime=3600,   # Seconds
    
    # Behavior
    block_when_exhausted=True,      # Wait for connections
    max_wait_time=30,                # Max wait seconds
    abandoned_remove=True,          # Remove abandoned
    abandoned_timeout=300            # Abandoned timeout seconds
)
```

## Usage Examples

### Basic Usage with Context Manager
```python
# Create pool
pool = DatabaseConnectionPool(
    DatabaseType.POSTGRESQL,
    config=config,
    host='localhost',
    database='mydb',
    user='user',
    password='pass'
)

# Use connection - automatically returned to pool
with pool.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    results = cursor.fetchall()
    cursor.close()
```

### Manual Connection Management
```python
# Borrow connection
wrapper = pool.borrow_connection(timeout=10)
try:
    conn = wrapper.connection
    # Use connection
    cursor = conn.cursor()
    cursor.execute("INSERT INTO logs (message) VALUES (%s)", ("Test",))
    conn.commit()
finally:
    # Always return connection
    pool.return_connection(wrapper)
```

### Convenience Function
```python
# Quick pool creation with defaults
pool = create_pool(
    DatabaseType.MYSQL,
    min_size=5,
    max_size=20,
    host='localhost',
    database='mydb',
    user='root',
    password='password'
)
```

## Key Capabilities Similar to Apache DBCP

### 1. **Connection Validation**
- Configurable validation queries
- Test on borrow, return, and while idle
- Automatic invalidation of bad connections

### 2. **Eviction Thread**
- Background thread for connection maintenance
- Configurable eviction runs
- Multiple eviction policies

### 3. **Abandoned Connection Handling**
- Detects connections held too long
- Automatic reclamation
- Logging of abandoned connections

### 4. **Comprehensive Statistics**
```python
stats = pool.get_pool_status()
# Returns:
{
    'idle_connections': 5,
    'active_connections': 3,
    'total_connections': 8,
    'connections_created': 10,
    'connections_destroyed': 2,
    'average_wait_time': 0.05,
    'max_wait_time': 0.5
}
```

### 5. **Thread Safety**
- Thread-local connection tracking
- Prevents double-borrowing
- Safe concurrent access

## Advanced Features

### 1. **Connection Wrapper**
- Tracks connection metadata
- Use count and error tracking
- Creation and last-used timestamps
- Transaction state tracking

### 2. **Automatic Recovery**
- Replaces expired connections
- Maintains minimum idle connections
- Handles connection failures gracefully

### 3. **Resource Management**
- Automatic cleanup on pool closure
- Graceful shutdown with timeout
- Connection leak prevention

### 4. **Performance Optimizations**
- Connection reuse
- Batch connection creation
- Efficient idle connection management
- Configurable wait strategies

## Benefits Over Basic Connection Handling

1. **Resource Efficiency**: Reuses connections instead of creating new ones
2. **Performance**: Eliminates connection overhead for each operation
3. **Reliability**: Automatic recovery from connection failures
4. **Monitoring**: Built-in statistics and health checks
5. **Scalability**: Handles high concurrency efficiently
6. **Safety**: Prevents connection leaks and abandoned connections

This implementation provides enterprise-grade connection pooling that matches Apache DBCP's capabilities while being Python-native and supporting multiple database backends. It's production-ready and includes all the essential features for robust database connection management.

---

# Part 2 - DBConnectionPoolManager (managing many pools)

A singleton `DBConnectionPoolManager` class that manages multiple connection pool instances, using connection configurations as keys for pool identification and reuse.This comprehensive singleton `DBConnectionPoolManager` class manages multiple database connection pools efficiently. Here are the key features:


## Key Features

### 1. **Singleton Pattern**
- Ensures only one manager instance exists globally
- Thread-safe initialization
- Accessible via `get_pool_manager()` function

### 2. **Automatic Pool Management**
- Creates new pools based on unique connection configurations
- Reuses existing pools for identical configurations
- Generates unique keys from connection parameters (excluding passwords for security)

### 3. **Connection Configuration**
- `ConnectionConfig` dataclass for clean configuration management
- Supports all database types (PostgreSQL, MySQL, SQLite, Oracle, SQL Server)
- SSL/TLS support with certificates
- Custom application naming

### 4. **Pool Lifecycle Management**
- Automatic cleanup of idle pools
- Configurable idle timeout
- LRU eviction when pool limit reached
- Reference counting for safe pool removal

### 5. **Advanced Features**
- Named pools for custom identification
- Per-pool custom configuration
- Background cleanup thread
- Comprehensive statistics and monitoring
- Health checks across all pools
- Graceful shutdown with cleanup

## Usage Examples

### Basic Usage - Automatic Pool Creation/Reuse
```python
# Get the singleton manager
manager = get_pool_manager()

# First call creates a new pool
pool1 = manager.get_pool(
    db_type='postgresql',
    host='localhost',
    database='mydb',
    user='user',
    password='pass'
)

# Second call with same config returns existing pool
pool2 = manager.get_pool(
    db_type='postgresql',
    host='localhost',
    database='mydb',
    user='user',
    password='pass'
)

assert pool1 is pool2  # Same pool instance
```

### Using ConnectionConfig
```python
# Define configuration
config = ConnectionConfig(
    db_type=DatabaseType.POSTGRESQL,
    host='localhost',
    port=5432,
    database='testdb',
    user='postgres',
    password='password',
    ssl=True,
    application_name='MyApp'
)

# Get pool
pool = manager.get_pool(config)

# Use connection
with pool.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    results = cursor.fetchall()
```

### Direct Connection Access
```python
# Convenience method - get connection directly
with manager.get_connection(
    db_type='mysql',
    host='localhost',
    database='mydb',
    user='root',
    password='password'
) as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM orders")
    count = cursor.fetchone()[0]
```

### Named Pools
```python
# Create named pool for easy reference
analytics_pool = manager.get_pool_by_name(
    'analytics_db',
    ConnectionConfig(
        db_type=DatabaseType.POSTGRESQL,
        host='analytics.server.com',
        database='analytics',
        user='analyst',
        password='secret'
    )
)

# Later, retrieve by name
pool = manager.get_pool_by_name('analytics_db', config)
```

### Custom Pool Configuration
```python
# High-performance pool configuration
high_perf_config = PoolConfig(
    min_idle=10,
    max_idle=50,
    max_total=100,
    test_on_borrow=False,  # Skip validation for performance
    max_wait_time=5
)

# Create pool with custom config
pool = manager.get_pool(
    connection_config=ConnectionConfig(
        db_type=DatabaseType.MYSQL,
        host='high-traffic.server.com',
        database='busy_db',
        user='app',
        password='pass'
    ),
    pool_config=high_perf_config
)
```

## Key Management Features

### 1. **Automatic Pool Reuse**
The manager generates a unique SHA256 hash key from connection parameters:
- Same configuration = same pool
- Password excluded from key for security
- Efficient pool reuse across application

### 2. **Pool Limits and Eviction**
```python
# Set maximum pools
manager.set_max_pools(50)

# When limit reached, LRU idle pool is evicted
# Pools with active connections are protected
```

### 3. **Idle Pool Cleanup**
```python
# Set idle timeout
manager.set_pool_idle_timeout(1800)  # 30 minutes

# Manual cleanup
removed = manager.clear_idle_pools(300)  # Clear pools idle > 5 min

# Automatic cleanup via background thread every 5 minutes
```

### 4. **Monitoring and Statistics**
```python
# Get comprehensive statistics
stats = manager.get_statistics()
# Returns:
{
    "manager_stats": {
        "total_pools": 5,
        "total_pools_created": 10,
        "total_pools_destroyed": 5,
        "total_connections_served": 1000
    },
    "aggregate_pool_stats": {
        "total_idle_connections": 15,
        "total_active_connections": 8,
        "total_connections": 23
    },
    "pools": [...]
}

# Health check all pools
health = manager.health_check()
# Returns:
{
    "status": "healthy",
    "healthy_pools": 5,
    "unhealthy_pools": [],
    "total_pools": 5
}

# Get specific pool info
info = manager.get_pool_info(pool_key)
```

### 5. **Thread Safety**
- All operations are thread-safe
- Multiple threads can request pools concurrently
- Proper locking ensures consistency

## Configuration Options

### Manager Configuration
```python
manager = get_pool_manager()

# Set defaults for all new pools
manager.set_default_pool_config(PoolConfig(
    min_idle=5,
    max_idle=20,
    max_total=50
))

# Set manager limits
manager.set_max_pools(100)
manager.set_pool_idle_timeout(3600)
```

### ConnectionConfig Parameters
```python
config = ConnectionConfig(
    db_type=DatabaseType.POSTGRESQL,
    host='localhost',
    port=5432,
    database='mydb',
    user='user',
    password='pass',
    
    # Optional parameters
    charset='utf8',
    autocommit=False,
    ssl=True,
    ssl_ca='/path/to/ca.pem',
    ssl_cert='/path/to/cert.pem',
    ssl_key='/path/to/key.pem',
    connection_timeout=10,
    socket_timeout=30,
    application_name='MyApp'
)
```

## Benefits

1. **Resource Efficiency**: Single point of management for all database pools
2. **Automatic Reuse**: No duplicate pools for same configuration
3. **Memory Management**: Automatic cleanup of unused pools
4. **Monitoring**: Centralized statistics and health checks
5. **Thread Safety**: Safe concurrent access from multiple threads
6. **Flexibility**: Support for multiple database types and configurations
7. **Production Ready**: Graceful shutdown, error handling, and logging

## Lifecycle Management

### Automatic Cleanup
- Background thread runs every 5 minutes
- Removes pools idle longer than configured timeout
- Preserves pools with active connections

### Graceful Shutdown
```python
# Automatic on application exit via atexit
# Or manual shutdown
manager.shutdown()
```

This implementation provides enterprise-grade connection pool management with the convenience of automatic pool creation and reuse, making it ideal for applications that connect to multiple databases or have dynamic connection requirements.
