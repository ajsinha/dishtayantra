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


class InMemoryRedisClone(metaclass=SingletonMeta):
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

    # ==================== STRING OPERATIONS ====================

    def set(self, key: str, value: Any, ex: Optional[int] = None, px: Optional[int] = None,
            nx: bool = False, xx: bool = False) -> bool:
        """
        SET key value [EX seconds] [PX milliseconds] [NX|XX]
        Set key to hold the string value
        """
        with self._lock:
            self._check_expired(key)

            # NX: Only set if key does not exist
            if nx and key in self._data:
                return False

            # XX: Only set if key exists
            if xx and key not in self._data:
                return False

            self._data[key] = str(value)
            self._types[key] = 'string'

            # Set expiration
            if ex:
                self._expiry[key] = time.time() + ex
            elif px:
                self._expiry[key] = time.time() + (px / 1000.0)
            elif key in self._expiry:
                del self._expiry[key]

            self._key_versions[key] += 1
            return True

    def get(self, key: str) -> Optional[str]:
        """GET key - Get the value of key"""
        with self._lock:
            if self._check_expired(key):
                return None

            if key not in self._data:
                return None

            if not self._check_type(key, 'string'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return self._data[key]

    def getset(self, key: str, value: Any) -> Optional[str]:
        """GETSET key value - Set key to value and return old value"""
        with self._lock:
            old_value = self.get(key)
            self.set(key, value)
            return old_value

    def mget(self, *keys: str) -> List[Optional[str]]:
        """MGET key [key ...] - Get values of all given keys"""
        with self._lock:
            return [self.get(key) for key in keys]

    def mset(self, mapping: Dict[str, Any]) -> bool:
        """MSET key value [key value ...] - Set multiple keys"""
        with self._lock:
            for key, value in mapping.items():
                self.set(key, value)
            return True

    def incr(self, key: str) -> int:
        """INCR key - Increment integer value by 1"""
        return self.incrby(key, 1)

    def incrby(self, key: str, increment: int) -> int:
        """INCRBY key increment - Increment integer value"""
        with self._lock:
            self._check_expired(key)

            if key not in self._data:
                self._data[key] = '0'
                self._types[key] = 'string'

            try:
                value = int(self._data[key])
                value += increment
                self._data[key] = str(value)
                self._key_versions[key] += 1
                return value
            except ValueError:
                raise ValueError("ERR value is not an integer or out of range")

    def decr(self, key: str) -> int:
        """DECR key - Decrement integer value by 1"""
        return self.incrby(key, -1)

    def decrby(self, key: str, decrement: int) -> int:
        """DECRBY key decrement - Decrement integer value"""
        return self.incrby(key, -decrement)

    def incrbyfloat(self, key: str, increment: float) -> float:
        """INCRBYFLOAT key increment - Increment float value"""
        with self._lock:
            self._check_expired(key)

            if key not in self._data:
                self._data[key] = '0'
                self._types[key] = 'string'

            try:
                value = float(self._data[key])
                value += increment
                self._data[key] = str(value)
                self._key_versions[key] += 1
                return value
            except ValueError:
                raise ValueError("ERR value is not a valid float")

    def append(self, key: str, value: str) -> int:
        """APPEND key value - Append value to key"""
        with self._lock:
            self._check_expired(key)

            if key not in self._data:
                self._data[key] = ''
                self._types[key] = 'string'

            if not self._check_type(key, 'string'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            self._data[key] += value
            self._key_versions[key] += 1
            return len(self._data[key])

    def strlen(self, key: str) -> int:
        """STRLEN key - Get length of value"""
        with self._lock:
            value = self.get(key)
            return len(value) if value else 0

    def getrange(self, key: str, start: int, end: int) -> str:
        """GETRANGE key start end - Get substring"""
        with self._lock:
            value = self.get(key)
            if not value:
                return ""

            # Handle negative indices
            if start < 0:
                start = len(value) + start
            if end < 0:
                end = len(value) + end

            return value[start:end + 1]

    def setrange(self, key: str, offset: int, value: str) -> int:
        """SETRANGE key offset value - Overwrite part of string"""
        with self._lock:
            self._check_expired(key)

            if key not in self._data:
                self._data[key] = ''
                self._types[key] = 'string'

            current = self._data[key]

            # Pad with zeros if needed
            if offset > len(current):
                current += '\x00' * (offset - len(current))

            # Replace substring
            self._data[key] = current[:offset] + value + current[offset + len(value):]
            self._key_versions[key] += 1
            return len(self._data[key])

    # ==================== KEY OPERATIONS ====================

    def delete(self, *keys: str) -> int:
        """DEL key [key ...] - Delete keys"""
        with self._lock:
            count = 0
            for key in keys:
                if key in self._data:
                    self._delete_key(key)
                    count += 1
            return count

    def exists(self, *keys: str) -> int:
        """EXISTS key [key ...] - Check if keys exist"""
        with self._lock:
            count = 0
            for key in keys:
                if not self._check_expired(key) and key in self._data:
                    count += 1
            return count

    def keys(self, pattern: str = '*') -> List[str]:
        """KEYS pattern - Find all keys matching pattern"""
        with self._lock:
            import fnmatch
            all_keys = list(self._data.keys())

            # Remove expired keys
            for key in all_keys[:]:
                if self._check_expired(key):
                    all_keys.remove(key)

            if pattern == '*':
                return all_keys

            return [key for key in all_keys if fnmatch.fnmatch(key, pattern)]

    def randomkey(self) -> Optional[str]:
        """RANDOMKEY - Return random key"""
        with self._lock:
            keys = self.keys()
            if not keys:
                return None
            import random
            return random.choice(keys)

    def rename(self, key: str, newkey: str) -> bool:
        """RENAME key newkey - Rename key"""
        with self._lock:
            if not self.exists(key):
                raise KeyError("ERR no such key")

            self._data[newkey] = self._data[key]
            self._types[newkey] = self._types[key]

            if key in self._expiry:
                self._expiry[newkey] = self._expiry[key]

            self._delete_key(key)
            self._key_versions[newkey] += 1
            return True

    def renamenx(self, key: str, newkey: str) -> bool:
        """RENAMENX key newkey - Rename key only if newkey doesn't exist"""
        with self._lock:
            if self.exists(newkey):
                return False
            return self.rename(key, newkey)

    def type(self, key: str) -> str:
        """TYPE key - Determine type of key"""
        with self._lock:
            if self._check_expired(key):
                return 'none'

            if key not in self._types:
                return 'none'

            return self._types[key]

    # ==================== EXPIRATION OPERATIONS ====================

    def expire(self, key: str, seconds: int) -> bool:
        """EXPIRE key seconds - Set timeout in seconds"""
        with self._lock:
            if not self.exists(key):
                return False

            self._expiry[key] = time.time() + seconds
            return True

    def expireat(self, key: str, timestamp: int) -> bool:
        """EXPIREAT key timestamp - Set timeout at timestamp"""
        with self._lock:
            if not self.exists(key):
                return False

            self._expiry[key] = float(timestamp)
            return True

    def pexpire(self, key: str, milliseconds: int) -> bool:
        """PEXPIRE key milliseconds - Set timeout in milliseconds"""
        with self._lock:
            if not self.exists(key):
                return False

            self._expiry[key] = time.time() + (milliseconds / 1000.0)
            return True

    def ttl(self, key: str) -> int:
        """TTL key - Get time to live in seconds"""
        with self._lock:
            if not self.exists(key):
                return -2

            if key not in self._expiry:
                return -1

            ttl = int(self._expiry[key] - time.time())
            return ttl if ttl > 0 else -2

    def pttl(self, key: str) -> int:
        """PTTL key - Get time to live in milliseconds"""
        with self._lock:
            if not self.exists(key):
                return -2

            if key not in self._expiry:
                return -1

            ttl = int((self._expiry[key] - time.time()) * 1000)
            return ttl if ttl > 0 else -2

    def persist(self, key: str) -> bool:
        """PERSIST key - Remove expiration"""
        with self._lock:
            if key not in self._expiry:
                return False

            del self._expiry[key]
            return True

    # ==================== LIST OPERATIONS ====================

    def lpush(self, key: str, *values: Any) -> int:
        """LPUSH key value [value ...] - Prepend values to list"""
        with self._lock:
            self._check_expired(key)

            if key not in self._data:
                self._data[key] = deque()
                self._types[key] = 'list'

            if not self._check_type(key, 'list'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            for value in values:
                self._data[key].appendleft(str(value))

            self._key_versions[key] += 1
            return len(self._data[key])

    def rpush(self, key: str, *values: Any) -> int:
        """RPUSH key value [value ...] - Append values to list"""
        with self._lock:
            self._check_expired(key)

            if key not in self._data:
                self._data[key] = deque()
                self._types[key] = 'list'

            if not self._check_type(key, 'list'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            for value in values:
                self._data[key].append(str(value))

            self._key_versions[key] += 1
            return len(self._data[key])

    def lpop(self, key: str) -> Optional[str]:
        """LPOP key - Remove and return first element"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return None

            if not self._check_type(key, 'list'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            if len(self._data[key]) == 0:
                return None

            self._key_versions[key] += 1
            return self._data[key].popleft()

    def rpop(self, key: str) -> Optional[str]:
        """RPOP key - Remove and return last element"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return None

            if not self._check_type(key, 'list'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            if len(self._data[key]) == 0:
                return None

            self._key_versions[key] += 1
            return self._data[key].pop()

    def lrange(self, key: str, start: int, stop: int) -> List[str]:
        """LRANGE key start stop - Get range of elements"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return []

            if not self._check_type(key, 'list'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            lst = list(self._data[key])

            # Handle negative indices
            if start < 0:
                start = len(lst) + start
            if stop < 0:
                stop = len(lst) + stop

            return lst[start:stop + 1]

    def llen(self, key: str) -> int:
        """LLEN key - Get length of list"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return 0

            if not self._check_type(key, 'list'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return len(self._data[key])

    def lindex(self, key: str, index: int) -> Optional[str]:
        """LINDEX key index - Get element by index"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return None

            if not self._check_type(key, 'list'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            lst = list(self._data[key])
            try:
                return lst[index]
            except IndexError:
                return None

    def lset(self, key: str, index: int, value: Any) -> bool:
        """LSET key index value - Set element at index"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                raise KeyError("ERR no such key")

            if not self._check_type(key, 'list'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            lst = list(self._data[key])
            try:
                lst[index] = str(value)
                self._data[key] = deque(lst)
                self._key_versions[key] += 1
                return True
            except IndexError:
                raise IndexError("ERR index out of range")

    def lrem(self, key: str, count: int, value: Any) -> int:
        """LREM key count value - Remove elements equal to value"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return 0

            if not self._check_type(key, 'list'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            value = str(value)
            removed = 0
            lst = list(self._data[key])

            if count > 0:
                # Remove from head
                new_lst = []
                for item in lst:
                    if item == value and removed < count:
                        removed += 1
                    else:
                        new_lst.append(item)
                lst = new_lst
            elif count < 0:
                # Remove from tail
                lst.reverse()
                new_lst = []
                count = abs(count)
                for item in lst:
                    if item == value and removed < count:
                        removed += 1
                    else:
                        new_lst.append(item)
                lst = list(reversed(new_lst))
            else:
                # Remove all
                new_lst = []
                for item in lst:
                    if item == value:
                        removed += 1
                    else:
                        new_lst.append(item)
                lst = new_lst

            self._data[key] = deque(lst)
            if removed > 0:
                self._key_versions[key] += 1
            return removed

    def ltrim(self, key: str, start: int, stop: int) -> bool:
        """LTRIM key start stop - Trim list to range"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return True

            if not self._check_type(key, 'list'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            lst = list(self._data[key])

            # Handle negative indices
            if start < 0:
                start = len(lst) + start
            if stop < 0:
                stop = len(lst) + stop

            self._data[key] = deque(lst[start:stop + 1])
            self._key_versions[key] += 1
            return True

    def rpoplpush(self, source: str, destination: str) -> Optional[str]:
        """RPOPLPUSH source destination - Pop from source and push to destination"""
        with self._lock:
            value = self.rpop(source)
            if value:
                self.lpush(destination, value)
            return value

    # ==================== SET OPERATIONS ====================

    def sadd(self, key: str, *members: Any) -> int:
        """SADD key member [member ...] - Add members to set"""
        with self._lock:
            self._check_expired(key)

            if key not in self._data:
                self._data[key] = set()
                self._types[key] = 'set'

            if not self._check_type(key, 'set'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            initial_size = len(self._data[key])
            for member in members:
                self._data[key].add(str(member))

            added = len(self._data[key]) - initial_size
            if added > 0:
                self._key_versions[key] += 1
            return added

    def srem(self, key: str, *members: Any) -> int:
        """SREM key member [member ...] - Remove members from set"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return 0

            if not self._check_type(key, 'set'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            removed = 0
            for member in members:
                if str(member) in self._data[key]:
                    self._data[key].remove(str(member))
                    removed += 1

            if removed > 0:
                self._key_versions[key] += 1
            return removed

    def smembers(self, key: str) -> Set[str]:
        """SMEMBERS key - Get all members"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return set()

            if not self._check_type(key, 'set'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return self._data[key].copy()

    def sismember(self, key: str, member: Any) -> bool:
        """SISMEMBER key member - Check if member exists"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return False

            if not self._check_type(key, 'set'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return str(member) in self._data[key]

    def scard(self, key: str) -> int:
        """SCARD key - Get set size"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return 0

            if not self._check_type(key, 'set'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return len(self._data[key])

    def spop(self, key: str, count: int = 1) -> Union[Optional[str], List[str]]:
        """SPOP key [count] - Remove and return random members"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return None if count == 1 else []

            if not self._check_type(key, 'set'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            import random
            result = []
            for _ in range(min(count, len(self._data[key]))):
                member = random.choice(list(self._data[key]))
                self._data[key].remove(member)
                result.append(member)

            if result:
                self._key_versions[key] += 1

            return result[0] if count == 1 and result else result

    def srandmember(self, key: str, count: int = 1) -> Union[Optional[str], List[str]]:
        """SRANDMEMBER key [count] - Get random members"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return None if count == 1 else []

            if not self._check_type(key, 'set'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            import random
            members = list(self._data[key])

            if count == 1:
                return random.choice(members) if members else None

            if count > 0:
                # Return unique elements
                return random.sample(members, min(count, len(members)))
            else:
                # Allow duplicates
                return [random.choice(members) for _ in range(abs(count))] if members else []

    def sinter(self, *keys: str) -> Set[str]:
        """SINTER key [key ...] - Intersect sets"""
        with self._lock:
            if not keys:
                return set()

            sets = []
            for key in keys:
                if self._check_expired(key) or key not in self._data:
                    return set()

                if not self._check_type(key, 'set'):
                    raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

                sets.append(self._data[key])

            result = sets[0].copy()
            for s in sets[1:]:
                result &= s

            return result

    def sunion(self, *keys: str) -> Set[str]:
        """SUNION key [key ...] - Union sets"""
        with self._lock:
            result = set()

            for key in keys:
                if not self._check_expired(key) and key in self._data:
                    if not self._check_type(key, 'set'):
                        raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

                    result |= self._data[key]

            return result

    def sdiff(self, *keys: str) -> Set[str]:
        """SDIFF key [key ...] - Difference of sets"""
        with self._lock:
            if not keys:
                return set()

            first_key = keys[0]
            if self._check_expired(first_key) or first_key not in self._data:
                return set()

            if not self._check_type(first_key, 'set'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            result = self._data[first_key].copy()

            for key in keys[1:]:
                if not self._check_expired(key) and key in self._data:
                    if not self._check_type(key, 'set'):
                        raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

                    result -= self._data[key]

            return result

    def sinterstore(self, destination: str, *keys: str) -> int:
        """SINTERSTORE destination key [key ...] - Store intersection"""
        with self._lock:
            result = self.sinter(*keys)
            self._data[destination] = result
            self._types[destination] = 'set'
            self._key_versions[destination] += 1
            return len(result)

    def sunionstore(self, destination: str, *keys: str) -> int:
        """SUNIONSTORE destination key [key ...] - Store union"""
        with self._lock:
            result = self.sunion(*keys)
            self._data[destination] = result
            self._types[destination] = 'set'
            self._key_versions[destination] += 1
            return len(result)

    def sdiffstore(self, destination: str, *keys: str) -> int:
        """SDIFFSTORE destination key [key ...] - Store difference"""
        with self._lock:
            result = self.sdiff(*keys)
            self._data[destination] = result
            self._types[destination] = 'set'
            self._key_versions[destination] += 1
            return len(result)

    # ==================== SORTED SET OPERATIONS ====================

    def zadd(self, key: str, mapping: Dict[Any, float], nx: bool = False, xx: bool = False) -> int:
        """ZADD key [NX|XX] score member [score member ...] - Add members with scores"""
        with self._lock:
            self._check_expired(key)

            if key not in self._data:
                self._data[key] = {}  # member -> score
                self._types[key] = 'zset'

            if not self._check_type(key, 'zset'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            added = 0
            for member, score in mapping.items():
                member = str(member)

                if nx and member in self._data[key]:
                    continue
                if xx and member not in self._data[key]:
                    continue

                if member not in self._data[key]:
                    added += 1

                self._data[key][member] = float(score)

            if added > 0:
                self._key_versions[key] += 1
            return added

    def zrem(self, key: str, *members: Any) -> int:
        """ZREM key member [member ...] - Remove members"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return 0

            if not self._check_type(key, 'zset'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            removed = 0
            for member in members:
                member = str(member)
                if member in self._data[key]:
                    del self._data[key][member]
                    removed += 1

            if removed > 0:
                self._key_versions[key] += 1
            return removed

    def zscore(self, key: str, member: Any) -> Optional[float]:
        """ZSCORE key member - Get score of member"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return None

            if not self._check_type(key, 'zset'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return self._data[key].get(str(member))

    def zincrby(self, key: str, increment: float, member: Any) -> float:
        """ZINCRBY key increment member - Increment score"""
        with self._lock:
            self._check_expired(key)

            if key not in self._data:
                self._data[key] = {}
                self._types[key] = 'zset'

            if not self._check_type(key, 'zset'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            member = str(member)
            current_score = self._data[key].get(member, 0.0)
            new_score = current_score + float(increment)
            self._data[key][member] = new_score
            self._key_versions[key] += 1
            return new_score

    def zcard(self, key: str) -> int:
        """ZCARD key - Get number of members"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return 0

            if not self._check_type(key, 'zset'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return len(self._data[key])

    def zcount(self, key: str, min_score: float, max_score: float) -> int:
        """ZCOUNT key min max - Count members in score range"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return 0

            if not self._check_type(key, 'zset'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            count = 0
            for score in self._data[key].values():
                if min_score <= score <= max_score:
                    count += 1
            return count

    def zrange(self, key: str, start: int, stop: int, withscores: bool = False) -> List:
        """ZRANGE key start stop [WITHSCORES] - Get range by index"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return []

            if not self._check_type(key, 'zset'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            # Sort by score, then by member
            sorted_items = sorted(self._data[key].items(), key=lambda x: (x[1], x[0]))

            # Handle negative indices
            if start < 0:
                start = len(sorted_items) + start
            if stop < 0:
                stop = len(sorted_items) + stop

            result = sorted_items[start:stop + 1]

            if withscores:
                return [(member, score) for member, score in result]
            else:
                return [member for member, score in result]

    def zrevrange(self, key: str, start: int, stop: int, withscores: bool = False) -> List:
        """ZREVRANGE key start stop [WITHSCORES] - Get range by index (reversed)"""
        with self._lock:
            result = self.zrange(key, start, stop, withscores)
            return list(reversed(result))

    def zrangebyscore(self, key: str, min_score: float, max_score: float,
                      withscores: bool = False, offset: int = 0, count: int = -1) -> List:
        """ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return []

            if not self._check_type(key, 'zset'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            # Filter by score
            filtered = [(m, s) for m, s in self._data[key].items()
                        if min_score <= s <= max_score]

            # Sort by score
            sorted_items = sorted(filtered, key=lambda x: (x[1], x[0]))

            # Apply offset and count
            if count < 0:
                result = sorted_items[offset:]
            else:
                result = sorted_items[offset:offset + count]

            if withscores:
                return result
            else:
                return [member for member, score in result]

    def zrank(self, key: str, member: Any) -> Optional[int]:
        """ZRANK key member - Get rank (index) of member"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return None

            if not self._check_type(key, 'zset'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            member = str(member)
            if member not in self._data[key]:
                return None

            sorted_items = sorted(self._data[key].items(), key=lambda x: (x[1], x[0]))
            for rank, (m, s) in enumerate(sorted_items):
                if m == member:
                    return rank

            return None

    def zrevrank(self, key: str, member: Any) -> Optional[int]:
        """ZREVRANK key member - Get reverse rank of member"""
        with self._lock:
            rank = self.zrank(key, member)
            if rank is None:
                return None

            return self.zcard(key) - rank - 1

    # ==================== HASH OPERATIONS ====================

    def hset(self, key: str, field: str, value: Any) -> int:
        """HSET key field value - Set hash field"""
        with self._lock:
            self._check_expired(key)

            if key not in self._data:
                self._data[key] = {}
                self._types[key] = 'hash'

            if not self._check_type(key, 'hash'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            is_new = field not in self._data[key]
            self._data[key][field] = str(value)

            if is_new:
                self._key_versions[key] += 1

            return 1 if is_new else 0

    def hget(self, key: str, field: str) -> Optional[str]:
        """HGET key field - Get hash field"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return None

            if not self._check_type(key, 'hash'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return self._data[key].get(field)

    def hmset(self, key: str, mapping: Dict[str, Any]) -> bool:
        """HMSET key field value [field value ...] - Set multiple fields"""
        with self._lock:
            for field, value in mapping.items():
                self.hset(key, field, value)
            return True

    def hmget(self, key: str, *fields: str) -> List[Optional[str]]:
        """HMGET key field [field ...] - Get multiple fields"""
        with self._lock:
            return [self.hget(key, field) for field in fields]

    def hgetall(self, key: str) -> Dict[str, str]:
        """HGETALL key - Get all fields and values"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return {}

            if not self._check_type(key, 'hash'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return self._data[key].copy()

    def hdel(self, key: str, *fields: str) -> int:
        """HDEL key field [field ...] - Delete fields"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return 0

            if not self._check_type(key, 'hash'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            deleted = 0
            for field in fields:
                if field in self._data[key]:
                    del self._data[key][field]
                    deleted += 1

            if deleted > 0:
                self._key_versions[key] += 1
            return deleted

    def hexists(self, key: str, field: str) -> bool:
        """HEXISTS key field - Check if field exists"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return False

            if not self._check_type(key, 'hash'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return field in self._data[key]

    def hkeys(self, key: str) -> List[str]:
        """HKEYS key - Get all field names"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return []

            if not self._check_type(key, 'hash'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return list(self._data[key].keys())

    def hvals(self, key: str) -> List[str]:
        """HVALS key - Get all values"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return []

            if not self._check_type(key, 'hash'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return list(self._data[key].values())

    def hlen(self, key: str) -> int:
        """HLEN key - Get number of fields"""
        with self._lock:
            if self._check_expired(key) or key not in self._data:
                return 0

            if not self._check_type(key, 'hash'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return len(self._data[key])

    def hincrby(self, key: str, field: str, increment: int) -> int:
        """HINCRBY key field increment - Increment field"""
        with self._lock:
            self._check_expired(key)

            if key not in self._data:
                self._data[key] = {}
                self._types[key] = 'hash'

            if not self._check_type(key, 'hash'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            current = int(self._data[key].get(field, '0'))
            new_value = current + increment
            self._data[key][field] = str(new_value)
            self._key_versions[key] += 1
            return new_value

    def hincrbyfloat(self, key: str, field: str, increment: float) -> float:
        """HINCRBYFLOAT key field increment - Increment field by float"""
        with self._lock:
            self._check_expired(key)

            if key not in self._data:
                self._data[key] = {}
                self._types[key] = 'hash'

            if not self._check_type(key, 'hash'):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            current = float(self._data[key].get(field, '0'))
            new_value = current + increment
            self._data[key][field] = str(new_value)
            self._key_versions[key] += 1
            return new_value

    def hsetnx(self, key: str, field: str, value: Any) -> int:
        """HSETNX key field value - Set field if not exists"""
        with self._lock:
            if self.hexists(key, field):
                return 0
            return self.hset(key, field, value)

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


class Pipeline:
    """Pipeline for batching multiple commands"""

    def __init__(self, redis_instance):
        self.redis = redis_instance
        self.command_queue = []
        self.watched_keys = {}
        self.in_transaction = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None and not self.in_transaction:
            # Auto-execute if not in explicit transaction
            return self.execute()
        return False

    def multi(self):
        """Start transaction mode"""
        self.in_transaction = True
        return self

    def watch(self, *keys):
        """Watch keys for optimistic locking"""
        with self.redis._lock:
            for key in keys:
                self.watched_keys[key] = self.redis._key_versions.get(key, 0)
        return self

    def execute(self):
        """Execute all queued commands"""
        with self.redis._lock:
            # Check watched keys
            for key, version in self.watched_keys.items():
                if self.redis._key_versions.get(key, 0) != version:
                    # Key was modified, abort
                    self.command_queue.clear()
                    return None

            # Execute all commands
            results = []
            for method_name, args, kwargs in self.command_queue:
                try:
                    method = getattr(self.redis, method_name)
                    result = method(*args, **kwargs)
                    results.append(result)
                except Exception as e:
                    results.append(e)

            self.command_queue.clear()
            return results

    def discard(self):
        """Discard all queued commands"""
        self.command_queue.clear()
        return self

    def _queue_command(self, method_name, *args, **kwargs):
        """Queue a command for later execution"""
        self.command_queue.append((method_name, args, kwargs))
        return self

    # Queue all Redis commands
    def set(self, *args, **kwargs):
        return self._queue_command('set', *args, **kwargs)

    def get(self, *args, **kwargs):
        return self._queue_command('get', *args, **kwargs)

    def incr(self, *args, **kwargs):
        return self._queue_command('incr', *args, **kwargs)

    def incrby(self, *args, **kwargs):
        return self._queue_command('incrby', *args, **kwargs)

    def decr(self, *args, **kwargs):
        return self._queue_command('decr', *args, **kwargs)

    def decrby(self, *args, **kwargs):
        return self._queue_command('decrby', *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self._queue_command('delete', *args, **kwargs)

    def lpush(self, *args, **kwargs):
        return self._queue_command('lpush', *args, **kwargs)

    def rpush(self, *args, **kwargs):
        return self._queue_command('rpush', *args, **kwargs)

    def sadd(self, *args, **kwargs):
        return self._queue_command('sadd', *args, **kwargs)

    def zadd(self, *args, **kwargs):
        return self._queue_command('zadd', *args, **kwargs)

    def hset(self, *args, **kwargs):
        return self._queue_command('hset', *args, **kwargs)


# ==================== EXAMPLE USAGE ====================

if __name__ == "__main__":
    # Create Redis instance
    redis = InMemoryRedisClone()

    print("=== In-Memory Redis Clone Demo ===\n")

    # String operations
    print("--- String Operations ---")
    redis.set("name", "Redis Clone")
    print(f"GET name: {redis.get('name')}")
    redis.set("counter", "0")
    redis.incr("counter")
    redis.incrby("counter", 5)
    print(f"Counter after incr and incrby(5): {redis.get('counter')}")

    # List operations
    print("\n--- List Operations ---")
    redis.rpush("mylist", "one", "two", "three")
    print(f"LRANGE mylist 0 -1: {redis.lrange('mylist', 0, -1)}")
    print(f"LPOP mylist: {redis.lpop('mylist')}")
    print(f"LLEN mylist: {redis.llen('mylist')}")

    # Set operations
    print("\n--- Set Operations ---")
    redis.sadd("myset", "a", "b", "c", "d")
    print(f"SMEMBERS myset: {redis.smembers('myset')}")
    redis.sadd("myset2", "c", "d", "e", "f")
    print(f"SINTER myset myset2: {redis.sinter('myset', 'myset2')}")
    print(f"SUNION myset myset2: {redis.sunion('myset', 'myset2')}")

    # Sorted Set operations
    print("\n--- Sorted Set Operations ---")
    redis.zadd("leaderboard", {"Alice": 100, "Bob": 150, "Charlie": 120})
    print(f"ZRANGE leaderboard 0 -1 WITHSCORES: {redis.zrange('leaderboard', 0, -1, withscores=True)}")
    redis.zincrby("leaderboard", 30, "Alice")
    print(f"Alice's new score: {redis.zscore('leaderboard', 'Alice')}")
    print(f"Bob's rank: {redis.zrank('leaderboard', 'Bob')}")

    # Hash operations
    print("\n--- Hash Operations ---")
    redis.hset("user:1000", "name", "John Doe")
    redis.hset("user:1000", "email", "john@example.com")
    redis.hset("user:1000", "age", "30")
    print(f"HGETALL user:1000: {redis.hgetall('user:1000')}")
    redis.hincrby("user:1000", "age", 1)
    print(f"Age after increment: {redis.hget('user:1000', 'age')}")

    # Expiration
    print("\n--- Expiration Operations ---")
    redis.set("temp_key", "temporary")
    redis.expire("temp_key", 2)
    print(f"TTL temp_key: {redis.ttl('temp_key')} seconds")
    print(f"GET temp_key: {redis.get('temp_key')}")
    time.sleep(3)
    print(f"GET temp_key after expiry: {redis.get('temp_key')}")

    # Transactions
    print("\n--- Transaction Operations ---")
    redis.set("balance", "100")
    with redis.pipeline() as pipe:
        pipe.multi()
        pipe.incrby("balance", 50)
        pipe.incrby("balance", 25)
        results = pipe.execute()
    print(f"Transaction results: {results}")
    print(f"Final balance: {redis.get('balance')}")

    # Pub/Sub
    print("\n--- Pub/Sub Operations ---")
    messages_received = []


    def message_handler(channel, message):
        messages_received.append(f"Received on {channel}: {message}")


    redis.subscribe("news", message_handler)
    redis.publish("news", "Breaking: Redis Clone Released!")
    redis.publish("news", "It's amazing!")
    time.sleep(0.1)  # Give time for callbacks
    for msg in messages_received:
        print(msg)

    # Database operations
    print("\n--- Database Operations ---")
    print(f"DBSIZE: {redis.dbsize()}")
    print(f"KEYS *: {redis.keys('*')}")
    print(f"KEYS my*: {redis.keys('my*')}")

    print("\n=== Demo Complete ===")
    print(f"\nTotal keys in database: {redis.dbsize()}")