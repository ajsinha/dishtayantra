"""Demo of core.db.db_connection_pool_manager (moved out of the module in
v2.2 so the production module stays within the 500-line limit). Run:

    python examples/demo_db_pool_manager.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from core.db.db_connection_pool import DatabaseType  # noqa: E402
from core.db.db_connection_pool_manager import (  # noqa: F401,E402
    ConnectionConfig, DBConnectionPoolManager, get_pool_manager,
)

if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Get the singleton manager
    manager = get_pool_manager()

    # Example 1: Get pool using ConnectionConfig
    pg_config = ConnectionConfig(
        db_type=DatabaseType.POSTGRESQL,
        host='localhost',
        port=5432,
        database='testdb',
        user='postgres',
        password='password'
    )

    # This creates a new pool
    pool1 = manager.get_pool(pg_config)
    print(f"Got pool 1: {pool1}")

    # This reuses the existing pool (same config)
    pool2 = manager.get_pool(pg_config)
    print(f"Got pool 2: {pool2}")
    print(f"Pools are same: {pool1 is pool2}")  # True

    # Example 2: Get pool using kwargs
    mysql_pool = manager.get_pool(
        db_type='mysql',
        host='localhost',
        port=3306,
        database='mydb',
        user='root',
        password='password'
    )

    # Example 3: Use connection directly with context manager
    with manager.get_connection(
            db_type='postgresql',
            host='localhost',
            database='testdb',
            user='postgres',
            password='password'
    ) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        print(f"PostgreSQL version: {cursor.fetchone()}")
        cursor.close()

    # Example 4: Named pools
    named_pool = manager.get_pool_by_name(
        'my_analytics_db',
        ConnectionConfig(
            db_type=DatabaseType.POSTGRESQL,
            host='analytics.server.com',
            database='analytics',
            user='analyst',
            password='secret'
        )
    )

    # Example 5: Custom pool configuration
    custom_pool_config = PoolConfig(
        min_idle=5,
        max_idle=15,
        max_total=30,
        test_on_borrow=True,
        max_wait_time=60
    )

    high_traffic_pool = manager.get_pool(
        connection_config=ConnectionConfig(
            db_type=DatabaseType.MYSQL,
            host='high-traffic.server.com',
            database='busy_db',
            user='app_user',
            password='app_pass'
        ),
        pool_config=custom_pool_config
    )

    # Example 6: Get statistics
    stats = manager.get_statistics()
    print(f"Manager statistics: {json.dumps(stats, indent=2)}")

    # Example 7: Health check
    health = manager.health_check()
    print(f"Health check: {json.dumps(health, indent=2)}")

    # Example 8: Get all pools info
    all_pools = manager.get_all_pools_info()
    for pool_info in all_pools:
        print(f"Pool {pool_info['pool_key'][:8]}...: "
              f"DB={pool_info['database']}, "
              f"Accesses={pool_info['access_count']}")

    # Example 9: Clear idle pools
    manager.set_pool_idle_timeout(60)  # 1 minute for testing
    removed = manager.clear_idle_pools(60)
    print(f"Removed {removed} idle pools")

    # Example 10: Multiple database types with same manager
    databases = [
        ConnectionConfig(
            db_type=DatabaseType.POSTGRESQL,
            host='pg.server.com',
            database='pg_db',
            user='pg_user',
            password='pg_pass'
        ),
        ConnectionConfig(
            db_type=DatabaseType.MYSQL,
            host='mysql.server.com',
            database='mysql_db',
            user='mysql_user',
            password='mysql_pass'
        ),
        ConnectionConfig(
            db_type=DatabaseType.SQLITE,
            database='local.db'
        )
    ]

    # Create pools for all databases
    pools = []
    for db_config in databases:
        pool = manager.get_pool(db_config)
        pools.append(pool)
        print(f"Created pool for {db_config.db_type.value}: {db_config.database}")

    # Use all pools concurrently
    import concurrent.futures


    def use_pool(pool, pool_index):
        """Worker function to use a pool."""
        for i in range(3):
            with pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                print(f"Pool {pool_index} - Query {i}: {result}")


    with concurrent.futures.ThreadPoolExecutor(max_workers=len(pools)) as executor:
        futures = []
        for i, pool in enumerate(pools):
            futures.append(executor.submit(use_pool, pool, i))
        concurrent.futures.wait(futures)

    # Final statistics
    final_stats = manager.get_statistics()
    print(f"Final statistics: {json.dumps(final_stats, indent=2)}")

    # Cleanup (automatically called on exit)
    # manager.shutdown()