"""
Redis Clone - List / Set Commands (v2.2 module split)
=====================================================

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


class ListSetCommandsMixin:
    """LPUSH/RPOP/LRANGE..., SADD/SMEMBERS/SINTER/SUNION/SDIFF...."""

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

