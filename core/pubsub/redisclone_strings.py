"""
Redis Clone - String / Key / Expiration Commands (v2.2 module split)
====================================================================

Extracted verbatim from inmemory_redisclone.py to respect the 500-line
architecture limit. All state lives on InMemoryRedisClone.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import threading
import time
import copy
import json
import os
import random
from typing import Any, List, Dict, Set, Tuple, Optional, Union
from collections import defaultdict, deque
from datetime import datetime, timedelta
import heapq
import logging

logger = logging.getLogger(__name__)


class StringKeyCommandsMixin:
    """SET/GET/INCR..., DEL/EXISTS/KEYS/RENAME..., EXPIRE/TTL/PERSIST."""

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

