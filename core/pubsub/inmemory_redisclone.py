"""
In-Memory Redis Clone
A thread-safe, in-memory implementation of Redis cache supporting all major operations.
No network connectivity required - pure Python implementation.
With periodic JSON dump and auto-reload functionality.
"""

import threading
import time
import copy
import json
import os
from typing import Any, List, Dict, Set, Tuple, Optional, Union
from collections import defaultdict, deque
from datetime import datetime, timedelta
import heapq
import logging
from core.core_utils import SingletonMeta
import queue
logger = logging.getLogger(__name__)


# v2.2 module split: the command groups live in sibling mixin modules and
# the Pipeline in redisclone_pipeline.py - all state stays on this class,
# and every public name is still importable from this module.
from core.pubsub.redisclone_strings import StringKeyCommandsMixin
from core.pubsub.redisclone_collections import ListSetCommandsMixin
from core.pubsub.redisclone_sorted_hash import SortedSetHashCommandsMixin



class InMemoryRedisClone(StringKeyCommandsMixin, ListSetCommandsMixin,
                         SortedSetHashCommandsMixin,
                         metaclass=SingletonMeta):
    """
    A complete in-memory Redis implementation with thread-safe operations.
    Supports: Strings, Lists, Sets, Sorted Sets, Hashes, Expiration, Transactions, Pub/Sub
    Features: Periodic JSON dump and auto-reload on restart
    """

    def __init__(self, dump_file: Optional[str] = None, dump_interval: int = 300):
        """
        Initialize InMemoryRedisClone with periodic dump support

        Args:
            dump_file: Path to JSON file for periodic dumps (default: data/inmemory_redisclone.json)
            dump_interval: Interval in seconds between dumps (default: 300 = 5 minutes)
        """
        # Main data store
        self._data: Dict[str, Any] = {}

        # Type tracking for each key
        self._types: Dict[str, str] = {}

        # Expiration times (key -> timestamp)
        self._expiry: Dict[str, float] = {}

        # Thread safety
        self._lock = threading.RLock()

        # Transaction support
        self._transaction_queue: Dict[int, List] = {}
        self._watch_keys: Dict[int, Set[str]] = {}
        self._key_versions: Dict[str, int] = defaultdict(int)

        # Pub/Sub support
        self._subscribers: Dict[str, List] = defaultdict(list)
        self._pattern_subscribers: Dict[str, List] = defaultdict(list)
        self._pubsub_lock = threading.RLock()

        # Periodic dump configuration
        self._dump_file = dump_file if dump_file else 'data/inmemory_redisclone.json'
        self._dump_interval = dump_interval
        self._dump_enabled = True
        self._last_dump_time = None

        # Ensure dump directory exists
        dump_dir = os.path.dirname(self._dump_file)
        if dump_dir and not os.path.exists(dump_dir):
            try:
                os.makedirs(dump_dir, exist_ok=True)
                logger.info(f"Created dump directory: {dump_dir}")
            except Exception as e:
                logger.error(f"Failed to create dump directory {dump_dir}: {e}")

        # Load existing data if dump file exists
        self._load_from_dump()

        # Background expiration cleanup
        self._cleanup_thread = threading.Thread(target=self._cleanup_expired, daemon=True)
        self._cleanup_thread.start()

        # Background periodic dump
        self._dump_thread = threading.Thread(target=self._periodic_dump, daemon=True)
        self._dump_thread.start()

        logger.info(f"InMemoryRedisClone initialized: dump_file={self._dump_file}, dump_interval={self._dump_interval}s")

    # ==================== UTILITY METHODS ====================

    def _cleanup_expired(self):
        """Background thread to cleanup expired keys"""
        while True:
            time.sleep(1)  # Check every second
            with self._lock:
                current_time = time.time()
                expired_keys = [k for k, v in self._expiry.items() if v <= current_time]
                for key in expired_keys:
                    self._delete_key(key)

    def _check_expired(self, key: str) -> bool:
        """Check if key has expired and delete if necessary"""
        if key in self._expiry:
            if self._expiry[key] <= time.time():
                self._delete_key(key)
                return True
        return False

    def _delete_key(self, key: str):
        """Internal method to delete a key"""
        if key in self._data:
            del self._data[key]
        if key in self._types:
            del self._types[key]
        if key in self._expiry:
            del self._expiry[key]
        self._key_versions[key] += 1

    def _check_type(self, key: str, expected_type: str) -> bool:
        """Verify key is of expected type"""
        if key not in self._types:
            return True
        return self._types[key] == expected_type

    # ==================== PERIODIC DUMP METHODS ====================

    def _periodic_dump(self):
        """Background thread to periodically dump cache to JSON file"""
        while self._dump_enabled:
            try:
                time.sleep(self._dump_interval)
                if self._dump_enabled:
                    self.dump_to_file()
            except Exception as e:
                logger.error(f"Error in periodic dump: {e}")

    def dump_to_file(self, file_path: Optional[str] = None) -> bool:
        """
        Dump current cache state to JSON file

        Args:
            file_path: Optional custom file path (uses configured dump_file if None)

        Returns:
            True if successful, False otherwise
        """
        target_file = file_path if file_path else self._dump_file

        try:
            with self._lock:
                dump_data = {
                    'metadata': {
                        'dump_time': datetime.now().isoformat(),
                        'total_keys': len(self._data),
                        'version': '1.0'
                    },
                    'data': {}
                }

                # Serialize each key with its metadata
                for key in list(self._data.keys()):
                    try:
                        value = self._data.get(key)
                        key_type = self._types.get(key, 'string')

                        # Calculate TTL (time to live in seconds)
                        ttl = None
                        if key in self._expiry:
                            remaining = self._expiry[key] - time.time()
                            ttl = max(0, int(remaining)) if remaining > 0 else None

                        # Serialize complex types
                        serialized_value = value
                        if key_type == 'list' and isinstance(value, deque):
                            serialized_value = list(value)
                        elif key_type == 'set' and isinstance(value, set):
                            serialized_value = list(value)
                        elif key_type == 'zset' and isinstance(value, list):
                            # Convert zset to serializable format
                            serialized_value = [[member, score] for member, score in value]
                        elif key_type == 'hash' and isinstance(value, dict):
                            serialized_value = value

                        dump_data['data'][key] = {
                            'value': serialized_value,
                            'type': key_type,
                            'ttl': ttl
                        }
                    except Exception as e:
                        logger.warning(f"Failed to serialize key {key}: {e}")

                # Write to temporary file first (atomic operation)
                temp_file = target_file + '.tmp'
                with open(temp_file, 'w') as f:
                    json.dump(dump_data, f, indent=2)

                # Atomic rename
                os.replace(temp_file, target_file)

                self._last_dump_time = datetime.now()
                logger.info(f"Successfully dumped {len(self._data)} keys to {target_file}")
                return True

        except Exception as e:
            logger.error(f"Failed to dump cache to {target_file}: {e}")
            return False

    def _load_from_dump(self, file_path: Optional[str] = None) -> bool:
        """
        Load cache state from JSON file

        Args:
            file_path: Optional custom file path (uses configured dump_file if None)

        Returns:
            True if successful, False otherwise
        """
        source_file = file_path if file_path else self._dump_file

        if not os.path.exists(source_file):
            logger.info(f"No dump file found at {source_file}, starting with empty cache")
            return False

        try:
            with open(source_file, 'r') as f:
                dump_data = json.load(f)

            with self._lock:
                loaded_count = 0
                skipped_count = 0

                for key, key_data in dump_data.get('data', {}).items():
                    try:
                        value = key_data['value']
                        key_type = key_data.get('type', 'string')
                        ttl = key_data.get('ttl')

                        # Restore complex types
                        if key_type == 'list':
                            value = deque(value)
                        elif key_type == 'set':
                            value = set(value)
                        elif key_type == 'zset':
                            # Restore zset from list of lists
                            value = [(member, score) for member, score in value]
                        elif key_type == 'hash':
                            value = dict(value)

                        # Set the value
                        self._data[key] = value
                        self._types[key] = key_type

                        # Set TTL if present and valid
                        if ttl and ttl > 0:
                            self._expiry[key] = time.time() + ttl

                        loaded_count += 1

                    except Exception as e:
                        logger.warning(f"Failed to load key {key}: {e}")
                        skipped_count += 1

                metadata = dump_data.get('metadata', {})
                dump_time = metadata.get('dump_time', 'unknown')
                logger.info(f"Loaded {loaded_count} keys from {source_file} (dump from {dump_time}, skipped {skipped_count})")
                return True

        except Exception as e:
            logger.error(f"Failed to load cache from {source_file}: {e}")
            return False

    def set_dump_config(self, dump_file: Optional[str] = None, dump_interval: Optional[int] = None):
        """
        Update dump configuration

        Args:
            dump_file: New dump file path
            dump_interval: New dump interval in seconds
        """
        if dump_file:
            self._dump_file = dump_file
            dump_dir = os.path.dirname(dump_file)
            if dump_dir and not os.path.exists(dump_dir):
                os.makedirs(dump_dir, exist_ok=True)
            logger.info(f"Updated dump file to: {dump_file}")

        if dump_interval is not None:
            self._dump_interval = dump_interval
            logger.info(f"Updated dump interval to: {dump_interval}s")

    def get_dump_info(self) -> Dict[str, Any]:
        """
        Get information about dump configuration and status

        Returns:
            Dictionary with dump configuration and status
        """
        return {
            'dump_file': self._dump_file,
            'dump_interval': self._dump_interval,
            'dump_enabled': self._dump_enabled,
            'last_dump_time': self._last_dump_time.isoformat() if self._last_dump_time else None,
            'file_exists': os.path.exists(self._dump_file),
            'file_size': os.path.getsize(self._dump_file) if os.path.exists(self._dump_file) else 0
        }

    def stop_dump(self):
        """Stop the periodic dump thread"""
        self._dump_enabled = False
        logger.info("Periodic dump stopped")

    # ==================== TRANSACTION OPERATIONS ====================

    def pipeline(self):
        """Create a pipeline for batching commands"""
        return Pipeline(self)

    def multi(self):
        """MULTI - Start transaction (for pipeline compatibility)"""
        return "OK"

    def exec(self):
        """EXEC - Execute transaction (for pipeline compatibility)"""
        return []

    def discard(self):
        """DISCARD - Discard transaction (for pipeline compatibility)"""
        return "OK"

    def watch(self, *keys: str) -> str:
        """WATCH key [key ...] - Watch keys for changes"""
        thread_id = threading.get_ident()

        if thread_id not in self._watch_keys:
            self._watch_keys[thread_id] = {}

        with self._lock:
            for key in keys:
                self._watch_keys[thread_id][key] = self._key_versions.get(key, 0)

        return "OK"

    def unwatch(self) -> str:
        """UNWATCH - Unwatch all keys"""
        thread_id = threading.get_ident()

        if thread_id in self._watch_keys:
            del self._watch_keys[thread_id]

        return "OK"

    # ==================== PUB/SUB OPERATIONS ====================

    def publish(self, channel: str, message: Any) -> int:
        """PUBLISH channel message - Publish message to channel"""
        with self._pubsub_lock:
            count = 0
            message = str(message)

            # Direct subscribers
            if channel in self._subscribers:
                for callback in self._subscribers[channel]:
                    try:
                        callback(channel, message)
                        count += 1
                    except:
                        pass

            # Pattern subscribers
            import fnmatch
            for pattern, callbacks in self._pattern_subscribers.items():
                if fnmatch.fnmatch(channel, pattern):
                    for callback in callbacks:
                        try:
                            callback(pattern, channel, message)
                            count += 1
                        except:
                            pass

            return count

    def subscribe(self, channel: str, callback) -> None:
        """SUBSCRIBE channel - Subscribe to channel"""
        with self._pubsub_lock:
            self._subscribers[channel].append(callback)

    def unsubscribe(self, channel: str, callback=None) -> None:
        """UNSUBSCRIBE channel - Unsubscribe from channel"""
        with self._pubsub_lock:
            if channel in self._subscribers:
                if callback:
                    try:
                        self._subscribers[channel].remove(callback)
                    except ValueError:
                        pass
                else:
                    self._subscribers[channel].clear()

    def psubscribe(self, pattern: str, callback) -> None:
        """PSUBSCRIBE pattern - Subscribe to pattern"""
        with self._pubsub_lock:
            self._pattern_subscribers[pattern].append(callback)

    def punsubscribe(self, pattern: str, callback=None) -> None:
        """PUNSUBSCRIBE pattern - Unsubscribe from pattern"""
        with self._pubsub_lock:
            if pattern in self._pattern_subscribers:
                if callback:
                    try:
                        self._pattern_subscribers[pattern].remove(callback)
                    except ValueError:
                        pass
                else:
                    self._pattern_subscribers[pattern].clear()

    # ==================== UTILITY OPERATIONS ====================

    def flushdb(self) -> str:
        """FLUSHDB - Remove all keys"""
        with self._lock:
            self._data.clear()
            self._types.clear()
            self._expiry.clear()
            self._key_versions.clear()
            return "OK"

    def flushall(self) -> str:
        """FLUSHALL - Remove all keys (same as flushdb in single-db mode)"""
        return self.flushdb()

    def dbsize(self) -> int:
        """DBSIZE - Return number of keys"""
        with self._lock:
            # Clean expired keys first
            for key in list(self._data.keys()):
                self._check_expired(key)
            return len(self._data)

    def ping(self, message: Optional[str] = None) -> str:
        """PING [message] - Ping server"""
        return message if message else "PONG"

    def echo(self, message: str) -> str:
        """ECHO message - Echo message"""
        return message

    def info(self, section: str = "all") -> str:
        """INFO [section] - Get server information"""
        with self._lock:
            info = []
            info.append("# Server")
            info.append("redis_mode:standalone")
            info.append("os:In-Memory Clone")
            info.append("")
            info.append("# Clients")
            info.append("connected_clients:1")
            info.append("")
            info.append("# Memory")
            info.append(f"used_memory:{len(str(self._data))}")
            info.append("")
            info.append("# Stats")
            info.append(f"total_commands_processed:{self.dbsize()}")
            info.append("")
            info.append("# Keyspace")
            info.append(f"db0:keys={self.dbsize()}")
            return "\r\n".join(info)

    def save(self) -> str:
        """SAVE - Synchronous save (no-op in memory)"""
        return "OK"

    def bgsave(self) -> str:
        """BGSAVE - Background save (no-op in memory)"""
        return "Background saving started"

    def lastsave(self) -> int:
        """LASTSAVE - Get last save timestamp"""
        return int(time.time())

    def time(self) -> Tuple[int, int]:
        """TIME - Get server time"""
        t = time.time()
        return (int(t), int((t % 1) * 1000000))


# v2.2 module split: Pipeline lives in redisclone_pipeline.py (imported
# after the class because transactions construct Pipeline at call time).
from core.pubsub.redisclone_pipeline import Pipeline  # noqa: E402,F401

# v2.2: the usage demo moved to examples/demo_inmemory_redisclone.py
