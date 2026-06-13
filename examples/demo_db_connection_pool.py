"""Demo of core.db.db_connection_pool (moved out of the module in v2.2
so the production module stays within the 500-line limit). Run directly:

    python examples/demo_db_connection_pool.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from core.db.db_connection_pool import (  # noqa: F401,E402
    DatabaseConnectionPool, DatabaseType, PoolConfig, create_pool,
)

if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Example 1: PostgreSQL pool with custom configuration
    pg_config = PoolConfig(
        min_idle=5,
        max_idle=10,
        max_total=20,
        test_on_borrow=True,
        test_while_idle=True,
        time_between_eviction_runs=60,
        min_evictable_idle_time=300,
        max_connection_lifetime=3600,
        abandoned_remove=True,
        abandoned_timeout=180,
        max_wait_time=30
    )

    pg_pool = DatabaseConnectionPool(
        DatabaseType.POSTGRESQL,
        config=pg_config,
        host='localhost',
        port=5432,
        database='testdb',
        user='postgres',
        password='password'
    )

    # Use connection with context manager
    with pg_pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        print(f"PostgreSQL version: {cursor.fetchone()}")
        cursor.close()

    # Check pool status
    print(f"Pool status: {pg_pool.get_pool_status()}")

    # Example 2: MySQL pool with connection validation
    mysql_pool = create_pool(
        DatabaseType.MYSQL,
        min_size=3,
        max_size=15,
        host='localhost',
        port=3306,
        database='testdb',
        user='root',
        password='password'
    )

    # Simulate concurrent usage
    import concurrent.futures


    def worker(pool, worker_id):
        """Worker function to test concurrent access."""
        for i in range(5):
            try:
                with pool.get_connection(timeout=10) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT SLEEP(0.1)")
                    cursor.close()
                    print(f"Worker {worker_id} - Query {i} completed")
            except Exception as e:
                print(f"Worker {worker_id} - Error: {e}")
            time.sleep(0.1)


    # Test with multiple threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for i in range(10):
            futures.append(executor.submit(worker, mysql_pool, i))

        # Wait for all workers to complete
        concurrent.futures.wait(futures)

    # Check final statistics
    print(f"Final pool statistics: {mysql_pool.get_pool_status()}")

    # Example 3: SQLite pool (useful for testing)
    sqlite_config = PoolConfig(
        min_idle=1,
        max_idle=5,
        max_total=10,
        test_on_borrow=False,  # SQLite doesn't need connection testing
        test_while_idle=False
    )

    sqlite_pool = DatabaseConnectionPool(
        DatabaseType.SQLITE,
        config=sqlite_config,
        database=':memory:'  # In-memory database
    )

    # Create test table
    with sqlite_pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT
            )
        ''')
        conn.commit()
        cursor.close()

    # Example 4: Oracle pool with abandoned connection removal
    if HAS_ORACLE:
        oracle_config = PoolConfig(
            min_idle=2,
            max_idle=8,
            max_total=15,
            abandoned_remove=True,
            abandoned_timeout=120,
            log_abandoned=True
        )

        oracle_pool = DatabaseConnectionPool(
            DatabaseType.ORACLE,
            config=oracle_config,
            user='scott',
            password='tiger',
            dsn='localhost:1521/XE'
        )

        # Test abandoned connection handling
        wrapper = oracle_pool.borrow_connection()
        # Simulate abandoned connection by not returning it
        time.sleep(130)  # Wait longer than abandoned_timeout

        # The eviction thread should detect and reclaim the connection
        print(f"Oracle pool status: {oracle_pool.get_pool_status()}")

    # Clean up
    pg_pool.close()
    mysql_pool.close()
    sqlite_pool.close()