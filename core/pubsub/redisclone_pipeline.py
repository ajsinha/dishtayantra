"""
Redis Clone - Pipeline (v2.2 module split)
==========================================

Extracted verbatim from inmemory_redisclone.py to respect the 500-line
architecture limit. Re-exported from core.pubsub.inmemory_redisclone.

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


