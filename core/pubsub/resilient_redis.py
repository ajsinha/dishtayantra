import logging
import time
import threading
import functools
from queue import Queue, Empty
from typing import Any, Dict, Optional, List, Union, Callable, Tuple
import redis
from redis import Redis, ConnectionPool, RedisError, ConnectionError, TimeoutError, ResponseError
from redis.client import Pipeline
from redis.connection import Connection

logger = logging.getLogger(__name__)


class BufferedCommand:
    """Represents a buffered Redis command."""

    def __init__(self, method_name: str, args: tuple, kwargs: dict,
                 callback: Optional[Callable] = None):
        self.method_name = method_name
        self.args = args
        self.kwargs = kwargs
        self.callback = callback
        self.result = None
        self.exception = None
        self.event = threading.Event()


class ResilientPipeline(Pipeline):
    """A resilient pipeline that can recover from connection failures."""

    def __init__(self, client, *args, **kwargs):
        self.client = client
        self._commands_buffer = []
        super().__init__(client.connection_pool, *args, **kwargs)

    def execute(self, raise_on_error: bool = True):
        """Execute pipeline commands with retry logic."""
        # Store commands before execution for potential retry
        commands_snapshot = list(self.command_stack)

        for attempt in range(1, self.client.reconnect_tries + 1):
            try:
                result = super().execute(raise_on_error=raise_on_error)
                return result

            except (ConnectionError, TimeoutError, ConnectionResetError) as e:
                logger.warning(f"Pipeline execution failed (attempt {attempt}/{self.client.reconnect_tries}): {e}")

                if attempt < self.client.reconnect_tries:
                    # Restore command stack for retry
                    self.command_stack = list(commands_snapshot)

                    logger.info(f"Retrying pipeline in {self.client.reconnect_interval_seconds} seconds...")
                    time.sleep(self.client.reconnect_interval_seconds)

                    # Trigger reconnection
                    self.client._ensure_connection()
                else:
                    logger.error(f"Pipeline execution failed after {self.client.reconnect_tries} attempts")
                    raise


class ResilientRedisClient(Redis):
    """
    A redis.Redis subclass that automatically handles reconnection on failures
    and optionally buffers commands during reconnection to prevent data loss.

    This class extends redis.Redis to provide automatic reconnection capabilities
    when connection failures occur during initialization or command execution.
    Commands sent during reconnection attempts can be buffered and retried once
    connection is restored.

    Args:
        host: Redis server hostname (default: 'localhost')
        port: Redis server port (default: 6379)
        db: Database number (default: 0)
        password: Password for authentication
        reconnect_tries: Maximum number of reconnection attempts (default: 10)
        reconnect_interval_seconds: Sleep interval between reconnection attempts (default: 60)
        buffer_commands: Whether to buffer commands during reconnection (default: True)
        buffer_max_commands: Maximum commands to buffer during reconnection (default: 10000)
        **kwargs: All other redis.Redis parameters
    """

    def __init__(self,
                 host: str = 'localhost',
                 port: int = 6379,
                 db: int = 0,
                 password: Optional[str] = None,
                 reconnect_tries: int = 10,
                 reconnect_interval_seconds: int = 60,
                 buffer_commands: bool = True,
                 buffer_max_commands: int = 10000,
                 **kwargs):
        """Initialize the ResilientRedisClient with reconnection and buffering capabilities."""

        # Store connection parameters for reconnection
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.redis_kwargs = kwargs

        # Reconnection settings
        self.reconnect_tries = reconnect_tries
        self.reconnect_interval_seconds = reconnect_interval_seconds
        self.buffer_commands = buffer_commands
        self.buffer_max_commands = buffer_max_commands

        # Connection state
        self._current_retry = 0
        self._connected = False
        self._reconnecting = False
        self._lock = threading.Lock()

        # Command buffer for storing commands during reconnection
        self._command_buffer = Queue(maxsize=buffer_max_commands) if buffer_commands else None
        self._failed_commands = []

        # Subscription state for pub/sub
        self._pubsub_patterns = {}  # pattern -> callback mapping
        self._pubsub_channels = {}  # channel -> callback mapping
        self._active_pubsubs = []

        # Initialize parent Redis client with retry logic
        self._connect_with_retry()

        # Start background thread for processing buffered commands if buffering is enabled
        if buffer_commands:
            self._stop_buffer_processor = threading.Event()
            self._buffer_processor_thread = threading.Thread(
                target=self._process_buffered_commands,
                daemon=True
            )
            self._buffer_processor_thread.start()

    def _connect_with_retry(self):
        """Establish connection to Redis with retry logic."""
        last_exception = None

        for attempt in range(1, self.reconnect_tries + 1):
            try:
                logger.info(f"Attempting to connect to Redis (attempt {attempt}/{self.reconnect_tries})")

                # Initialize the parent Redis client
                super().__init__(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    password=self.password,
                    **self.redis_kwargs
                )

                # Test the connection
                self.ping()

                self._connected = True
                self._reconnecting = False
                self._current_retry = 0
                logger.info("Successfully connected to Redis")

                # Restore pub/sub subscriptions if any
                self._restore_subscriptions()

                return

            except (ConnectionError, TimeoutError, RedisError) as e:
                last_exception = e
                logger.warning(f"Failed to connect to Redis (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    logger.info(f"Retrying in {self.reconnect_interval_seconds} seconds...")
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    logger.error(f"Failed to connect to Redis after {self.reconnect_tries} attempts")
                    raise last_exception

    def _ensure_connection(self):
        """Ensure Redis connection is active, reconnect if necessary."""
        try:
            self.ping()
            return True
        except (ConnectionError, TimeoutError):
            logger.warning("Connection lost, attempting to reconnect...")
            self._reconnect()
            return self._connected

    def _reconnect(self):
        """Attempt to reconnect to Redis."""
        with self._lock:
            if self._reconnecting:
                return
            self._reconnecting = True

        logger.info("Starting reconnection process...")

        try:
            # Reset connection state
            self._connected = False

            # Close existing connection pool
            try:
                self.connection_pool.disconnect()
            except Exception as e:
                logger.debug(f"Error closing connection pool: {e}")

            # Reconnect with retry logic
            self._connect_with_retry()

            # Flush buffered commands if buffering is enabled
            if self.buffer_commands:
                self._flush_buffer()

        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            raise
        finally:
            with self._lock:
                self._reconnecting = False

    def _restore_subscriptions(self):
        """Restore pub/sub subscriptions after reconnection."""
        if not self._pubsub_channels and not self._pubsub_patterns:
            return

        logger.info(f"Restoring {len(self._pubsub_channels)} channel and "
                    f"{len(self._pubsub_patterns)} pattern subscriptions")

        for pubsub in self._active_pubsubs:
            try:
                # Re-subscribe to channels
                for channel in list(self._pubsub_channels.keys()):
                    pubsub.subscribe(channel)
                    logger.debug(f"Restored subscription to channel: {channel}")

                # Re-subscribe to patterns
                for pattern in list(self._pubsub_patterns.keys()):
                    pubsub.psubscribe(pattern)
                    logger.debug(f"Restored subscription to pattern: {pattern}")

            except Exception as e:
                logger.error(f"Failed to restore subscriptions: {e}")

    def execute_command(self, *args, **kwargs):
        """Execute a Redis command with automatic retry and optional buffering."""
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                # Check connection before executing
                if not self._connected and not self._ensure_connection():
                    raise ConnectionError("Not connected to Redis")

                result = super().execute_command(*args, **kwargs)
                self._current_retry = 0
                return result

            except (ConnectionError, TimeoutError, ConnectionResetError, BrokenPipeError) as e:
                logger.warning(f"Command execution failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                # Buffer command on first failure if buffering is enabled
                if attempt == 1 and self.buffer_commands:
                    self._buffer_command(args, kwargs)

                if attempt < self.reconnect_tries:
                    logger.info(f"Retrying in {self.reconnect_interval_seconds} seconds...")
                    time.sleep(self.reconnect_interval_seconds)

                    # Trigger reconnection
                    self._ensure_connection()
                else:
                    logger.error(f"Command execution failed after {self.reconnect_tries} attempts")
                    raise

    def _buffer_command(self, args: tuple, kwargs: dict):
        """Buffer a command for later execution when connection is restored."""
        if not self.buffer_commands or not self._command_buffer:
            return

        try:
            # Parse the command name from args
            command_name = args[0] if args else 'UNKNOWN'

            command_data = BufferedCommand(
                method_name=command_name,
                args=args,
                kwargs=kwargs
            )

            if self._command_buffer.full():
                # Remove oldest command to make room
                old_cmd = self._command_buffer.get_nowait()
                logger.warning(f"Buffer full, dropping oldest command: {old_cmd.method_name}")

            self._command_buffer.put_nowait(command_data)
            logger.debug(f"Buffered command: {command_name}, buffer size: {self._command_buffer.qsize()}")

        except Exception as e:
            logger.error(f"Failed to buffer command: {e}")

    def _process_buffered_commands(self):
        """Background thread to process buffered commands when connection is restored."""
        while not self._stop_buffer_processor.is_set():
            try:
                if self._connected and not self._reconnecting and not self._command_buffer.empty():
                    try:
                        command = self._command_buffer.get(timeout=1)

                        # Execute the buffered command
                        result = super().execute_command(*command.args, **command.kwargs)
                        command.result = result

                        logger.debug(f"Successfully executed buffered command: {command.method_name}")

                        # Signal completion if there's a callback
                        if command.callback:
                            command.callback(result)

                    except Empty:
                        continue
                    except Exception as e:
                        logger.error(f"Failed to execute buffered command: {e}")
                        command.exception = e
                        self._failed_commands.append(command)
                else:
                    time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in buffer processor thread: {e}")
                time.sleep(1)

    def _flush_buffer(self):
        """Flush all buffered commands after reconnection."""
        if not self.buffer_commands or not self._command_buffer:
            return

        logger.info(f"Flushing {self._command_buffer.qsize()} buffered commands")

        flushed_count = 0
        failed_count = 0

        while not self._command_buffer.empty():
            try:
                command = self._command_buffer.get_nowait()

                result = super().execute_command(*command.args, **command.kwargs)
                command.result = result
                flushed_count += 1

                if command.callback:
                    command.callback(result)

            except Empty:
                break
            except Exception as e:
                logger.error(f"Failed to flush buffered command: {e}")
                command.exception = e
                self._failed_commands.append(command)
                failed_count += 1

        logger.info(f"Flushed {flushed_count} commands, {failed_count} failed")

    def pipeline(self, transaction: bool = True, shard_hint: Optional[str] = None) -> ResilientPipeline:
        """Create a resilient pipeline for batch operations."""
        return ResilientPipeline(self, transaction=transaction, shard_hint=shard_hint)

    def pubsub(self, **kwargs):
        """Create a resilient pub/sub connection."""
        pubsub = super().pubsub(**kwargs)
        self._active_pubsubs.append(pubsub)

        # Wrap subscribe methods to track subscriptions
        original_subscribe = pubsub.subscribe
        original_psubscribe = pubsub.psubscribe

        def tracked_subscribe(*channels, **kwargs):
            for channel in channels:
                self._pubsub_channels[channel] = kwargs.get('callback')
            return original_subscribe(*channels, **kwargs)

        def tracked_psubscribe(*patterns, **kwargs):
            for pattern in patterns:
                self._pubsub_patterns[pattern] = kwargs.get('callback')
            return original_psubscribe(*patterns, **kwargs)

        pubsub.subscribe = tracked_subscribe
        pubsub.psubscribe = tracked_psubscribe

        return pubsub

    def multi_exec(self, commands: List[Tuple[str, tuple]]) -> List[Any]:
        """
        Execute multiple commands atomically with retry logic.

        Args:
            commands: List of (command_name, args) tuples

        Returns:
            List of command results
        """
        with self.pipeline() as pipe:
            for cmd_name, cmd_args in commands:
                getattr(pipe, cmd_name)(*cmd_args)
            return pipe.execute()

    def close(self):
        """Close the Redis connection."""
        try:
            # Stop buffer processor if enabled
            if self.buffer_commands:
                self._stop_buffer_processor.set()

                # Try to flush remaining commands
                if self._connected and not self._command_buffer.empty():
                    logger.info(f"Flushing {self._command_buffer.qsize()} commands before closing")
                    self._flush_buffer()

                # Log unsent commands
                if not self._command_buffer.empty():
                    logger.warning(f"Closing with {self._command_buffer.qsize()} unsent commands")

            if self._failed_commands:
                logger.warning(f"Failed to execute {len(self._failed_commands)} commands")

            # Close pub/sub connections
            for pubsub in self._active_pubsubs:
                try:
                    pubsub.close()
                except:
                    pass

            # Close connection pool
            self.connection_pool.disconnect()
            self._connected = False
            logger.info("Redis connection closed successfully")

        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")

    def get_buffer_size(self) -> int:
        """Get the current number of commands in the buffer."""
        return self._command_buffer.qsize() if self._command_buffer else 0

    def get_failed_commands(self) -> List[BufferedCommand]:
        """Get the list of commands that failed to execute."""
        return self._failed_commands.copy()

    def is_connected(self) -> bool:
        """Check if the client is connected to Redis."""
        try:
            self.ping()
            return True
        except:
            return False

    # Ensure all Redis methods use the resilient execute_command
    def __getattr__(self, name):
        """Intercept method calls to add resilience to all Redis commands."""
        # Get the original method
        try:
            attr = super().__getattribute__(name)
        except AttributeError:
            # Try to get from parent class
            attr = getattr(super(), name)

        # If it's a method that executes Redis commands, wrap it
        if callable(attr) and not name.startswith('_'):
            @functools.wraps(attr)
            def wrapper(*args, **kwargs):
                for attempt in range(1, self.reconnect_tries + 1):
                    try:
                        return attr(*args, **kwargs)
                    except (ConnectionError, TimeoutError, ConnectionResetError) as e:
                        logger.warning(f"{name} failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                        if attempt < self.reconnect_tries:
                            time.sleep(self.reconnect_interval_seconds)
                            self._ensure_connection()
                        else:
                            raise

            return wrapper
        return attr


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Example 1: Basic usage as a drop-in replacement
    client = ResilientRedisClient(
        host='localhost',
        port=6379,
        db=0,
        reconnect_tries=5,
        reconnect_interval_seconds=30,
        buffer_commands=True,
        buffer_max_commands=5000,
        decode_responses=True
    )

    # Example 2: Basic operations with automatic reconnection
    try:
        # Set/Get operations
        client.set('key1', 'value1')
        print(f"key1: {client.get('key1')}")

        # Hash operations
        client.hset('hash1', 'field1', 'value1')
        client.hset('hash1', 'field2', 'value2')
        print(f"hash1: {client.hgetall('hash1')}")

        # List operations
        client.lpush('list1', 'item1', 'item2', 'item3')
        print(f"list1: {client.lrange('list1', 0, -1)}")

        # Set operations
        client.sadd('set1', 'member1', 'member2', 'member3')
        print(f"set1: {client.smembers('set1')}")

    except Exception as e:
        print(f"Operation failed: {e}")

    # Example 3: Using pipeline with automatic retry
    try:
        with client.pipeline() as pipe:
            pipe.set('pipe_key1', 'value1')
            pipe.set('pipe_key2', 'value2')
            pipe.incr('counter')
            pipe.hset('pipe_hash', 'field', 'value')
            results = pipe.execute()
            print(f"Pipeline results: {results}")
    except Exception as e:
        print(f"Pipeline failed: {e}")


    # Example 4: Pub/Sub with automatic reconnection
    def message_handler(message):
        print(f"Received: {message}")


    pubsub = client.pubsub()
    pubsub.subscribe('channel1')

    # Start listening in a thread
    pubsub_thread = threading.Thread(
        target=lambda: pubsub.listen(),
        daemon=True
    )
    pubsub_thread.start()

    # Publish messages
    for i in range(10):
        client.publish('channel1', f'Message {i}')
        time.sleep(1)

        # Check buffer status
        buffer_size = client.get_buffer_size()
        if buffer_size > 0:
            print(f"Commands in buffer: {buffer_size}")

    # Example 5: Atomic multi-command execution
    commands = [
        ('set', ('atomic_key1', 'value1')),
        ('set', ('atomic_key2', 'value2')),
        ('incr', ('atomic_counter',)),
        ('lpush', ('atomic_list', 'item1', 'item2'))
    ]

    try:
        results = client.multi_exec(commands)
        print(f"Multi-exec results: {results}")
    except Exception as e:
        print(f"Multi-exec failed: {e}")

    # Example 6: Monitor connection and buffer status
    print(f"\nConnection status: {'Connected' if client.is_connected() else 'Disconnected'}")
    print(f"Buffered commands: {client.get_buffer_size()}")

    failed_commands = client.get_failed_commands()
    if failed_commands:
        print(f"Failed commands: {len(failed_commands)}")
        for cmd in failed_commands[:5]:  # Show first 5 failed commands
            print(f"  - {cmd.method_name}: {cmd.exception}")


    # Example 7: Using with context manager pattern
    class RedisContext:
        def __init__(self, **kwargs):
            self.client = None
            self.kwargs = kwargs

        def __enter__(self):
            self.client = ResilientRedisClient(**self.kwargs)
            return self.client

        def __exit__(self, exc_type, exc_val, exc_tb):
            if self.client:
                self.client.close()


    with RedisContext(host='localhost', port=6379) as redis_client:
        redis_client.set('context_key', 'context_value')
        print(f"Context value: {redis_client.get('context_key')}")

    # Clean up
    client.close()

    # Example 8: Working with transactions
    client2 = ResilientRedisClient(host='localhost', port=6379, decode_responses=True)


    def transfer_funds(from_account, to_account, amount):
        """Example of using WATCH for optimistic locking."""
        with client2.pipeline() as pipe:
            while True:
                try:
                    # Watch the from_account for changes
                    pipe.watch(from_account)

                    # Get current balance
                    balance = pipe.get(from_account)
                    if balance is None or int(balance) < amount:
                        pipe.unwatch()
                        return False

                    # Start transaction
                    pipe.multi()
                    pipe.decrby(from_account, amount)
                    pipe.incrby(to_account, amount)
                    pipe.execute()
                    return True

                except redis.WatchError:
                    # Another client changed the watched key
                    continue


    # Initialize accounts
    client2.set('account1', 1000)
    client2.set('account2', 500)

    # Transfer funds
    if transfer_funds('account1', 'account2', 100):
        print(f"Transfer successful!")
        print(f"Account1: {client2.get('account1')}")
        print(f"Account2: {client2.get('account2')}")

    client2.close()