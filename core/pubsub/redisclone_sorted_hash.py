"""
Redis Clone - Sorted Set / Hash Commands (v2.2 module split)
============================================================

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


class SortedSetHashCommandsMixin:
    """ZADD/ZRANGE/ZRANK..., HSET/HGET/HGETALL/HINCRBY...."""

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

