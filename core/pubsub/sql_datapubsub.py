import json
import logging
import time
from datetime import datetime
from core.pubsub.datapubsub import DataPublisher, DataSubscriber
import queue
logger = logging.getLogger(__name__)


class SQLDataPublisher(DataPublisher):
    """Publisher that inserts data into SQL database"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        self.sql_dest_name = destination.replace('sql://', '')
        self.sql_config_file = config.get('sql_config_file')

        # Load SQL configuration
        with open(self.sql_config_file, 'r') as f:
            self.sql_config = json.load(f)

        self.db_type = self.sql_config.get('db_type', 'mysql')
        self.insert_statement = self.sql_config.get('insert_statement')

        # Create database connection
        self.connection = self._create_connection()

        logger.info(f"SQL publisher created for {self.sql_dest_name}")

    def _create_connection(self):
        """Create database connection based on type"""
        if self.db_type == 'mysql':
            import pymysql
            return pymysql.connect(
                host=self.sql_config.get('host', 'localhost'),
                port=self.sql_config.get('port', 3306),
                user=self.sql_config.get('user'),
                password=self.sql_config.get('password'),
                database=self.sql_config.get('database'),
                autocommit=True
            )
        elif self.db_type == 'postgres':
            import psycopg2
            return psycopg2.connect(
                host=self.sql_config.get('host', 'localhost'),
                port=self.sql_config.get('port', 5432),
                user=self.sql_config.get('user'),
                password=self.sql_config.get('password'),
                database=self.sql_config.get('database')
            )
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def _do_publish(self, data):
        """Insert data into database"""
        try:
            cursor = self.connection.cursor()

            # Replace placeholders in insert statement with data values
            # Assumes insert statement uses named placeholders like %(key)s
            cursor.execute(self.insert_statement, data)

            if self.db_type == 'postgres':
                self.connection.commit()

            cursor.close()

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to SQL {self.sql_dest_name}")
        except Exception as e:
            logger.error(f"Error publishing to SQL: {str(e)}")
            # Try to reconnect
            try:
                self.connection = self._create_connection()
            except:
                pass
            raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        if self.connection:
            self.connection.close()


class SQLDataSubscriber(DataSubscriber):
    """Subscriber that reads data from SQL database"""

    def __init__(self, name, source, config, given_queue: queue.Queue = None):
        super().__init__(name, source, config, given_queue)

        self.sql_source_name = source.replace('sql://', '')
        self.sql_config_file = config.get('sql_config_file')
        self.poll_interval = config.get('poll_interval', 5)

        # Load SQL configuration
        with open(self.sql_config_file, 'r') as f:
            self.sql_config = json.load(f)

        self.db_type = self.sql_config.get('db_type', 'mysql')
        self.select_statement = self.sql_config.get('select_statement')

        # Create database connection
        self.connection = self._create_connection()

        logger.info(f"SQL subscriber created for {self.sql_source_name}")

    def _create_connection(self):
        """Create database connection based on type"""
        if self.db_type == 'mysql':
            import pymysql
            return pymysql.connect(
                host=self.sql_config.get('host', 'localhost'),
                port=self.sql_config.get('port', 3306),
                user=self.sql_config.get('user'),
                password=self.sql_config.get('password'),
                database=self.sql_config.get('database'),
                cursorclass=pymysql.cursors.DictCursor
            )
        elif self.db_type == 'postgres':
            import psycopg2
            import psycopg2.extras
            conn = psycopg2.connect(
                host=self.sql_config.get('host', 'localhost'),
                port=self.sql_config.get('port', 5432),
                user=self.sql_config.get('user'),
                password=self.sql_config.get('password'),
                database=self.sql_config.get('database')
            )
            return conn
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def _do_subscribe(self):
        """Read data from database"""
        try:
            if self.db_type == 'postgres':
                import psycopg2.extras
                cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            else:
                cursor = self.connection.cursor()

            cursor.execute(self.select_statement)
            row = cursor.fetchone()
            cursor.close()

            if row:
                return dict(row)
            else:
                time.sleep(self.poll_interval)
                return None
        except Exception as e:
            logger.error(f"Error reading from SQL: {str(e)}")
            # Try to reconnect
            try:
                self.connection = self._create_connection()
            except:
                pass
            time.sleep(self.poll_interval)
            return None

    def stop(self):
        """Stop the subscriber"""
        super().stop()
        if self.connection:
            self.connection.close()