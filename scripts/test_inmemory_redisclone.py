"""
Comprehensive Test Suite for In-Memory Redis Clone
Tests all major Redis operations and demonstrates usage patterns
"""

import time
import threading
from inmemory_redisclone import InMemoryRedisClone


def test_string_operations():
    """Test all string operations"""
    print("=" * 60)
    print("TESTING STRING OPERATIONS")
    print("=" * 60)

    redis = InMemoryRedisClone()

    # Basic SET/GET
    redis.set("key1", "value1")
    assert redis.get("key1") == "value1", "Basic SET/GET failed"
    print("✓ SET/GET works")

    # SET with options
    redis.set("key2", "value2", ex=10)
    assert redis.ttl("key2") > 0, "SET with EX failed"
    print("✓ SET with expiration works")

    # NX and XX options
    assert redis.set("key3", "value3", nx=True) == True, "SET NX failed"
    assert redis.set("key3", "value4", nx=True) == False, "SET NX should fail on existing key"
    assert redis.set("key3", "value5", xx=True) == True, "SET XX failed"
    assert redis.set("nonexistent", "val", xx=True) == False, "SET XX should fail on non-existing key"
    print("✓ SET with NX/XX options works")

    # MGET/MSET
    redis.mset({"k1": "v1", "k2": "v2", "k3": "v3"})
    result = redis.mget("k1", "k2", "k3", "nonexistent")
    assert result == ["v1", "v2", "v3", None], "MGET/MSET failed"
    print("✓ MGET/MSET works")

    # INCR/DECR
    redis.set("counter", "10")
    assert redis.incr("counter") == 11, "INCR failed"
    assert redis.incrby("counter", 5) == 16, "INCRBY failed"
    assert redis.decr("counter") == 15, "DECR failed"
    assert redis.decrby("counter", 3) == 12, "DECRBY failed"
    print("✓ INCR/DECR operations work")

    # INCRBYFLOAT
    redis.set("float_counter", "10.5")
    assert redis.incrbyfloat("float_counter", 2.3) == 12.8, "INCRBYFLOAT failed"
    print("✓ INCRBYFLOAT works")

    # APPEND
    redis.set("msg", "Hello")
    redis.append("msg", " World")
    assert redis.get("msg") == "Hello World", "APPEND failed"
    print("✓ APPEND works")

    # STRLEN
    assert redis.strlen("msg") == 11, "STRLEN failed"
    print("✓ STRLEN works")

    # GETRANGE/SETRANGE
    assert redis.getrange("msg", 0, 4) == "Hello", "GETRANGE failed"
    redis.setrange("msg", 6, "Redis")
    assert redis.get("msg") == "Hello Redis", "SETRANGE failed"
    print("✓ GETRANGE/SETRANGE works")

    # GETSET
    old = redis.getset("key1", "newvalue")
    assert old == "value1", "GETSET failed"
    assert redis.get("key1") == "newvalue", "GETSET failed"
    print("✓ GETSET works")

    print("\n✓ ALL STRING OPERATIONS PASSED\n")


def test_list_operations():
    """Test all list operations"""
    print("=" * 60)
    print("TESTING LIST OPERATIONS")
    print("=" * 60)

    redis = InMemoryRedisClone()

    # LPUSH/RPUSH
    redis.lpush("list1", "a", "b", "c")
    redis.rpush("list1", "d", "e")
    result = redis.lrange("list1", 0, -1)
    assert result == ["c", "b", "a", "d", "e"], f"LPUSH/RPUSH failed: {result}"
    print("✓ LPUSH/RPUSH works")

    # LPOP/RPOP
    assert redis.lpop("list1") == "c", "LPOP failed"
    assert redis.rpop("list1") == "e", "RPOP failed"
    print("✓ LPOP/RPOP works")

    # LLEN
    assert redis.llen("list1") == 3, "LLEN failed"
    print("✓ LLEN works")

    # LINDEX
    assert redis.lindex("list1", 0) == "b", "LINDEX failed"
    assert redis.lindex("list1", -1) == "d", "LINDEX with negative index failed"
    print("✓ LINDEX works")

    # LSET
    redis.lset("list1", 0, "x")
    assert redis.lindex("list1", 0) == "x", "LSET failed"
    print("✓ LSET works")

    # LREM
    redis.rpush("list2", "a", "b", "a", "c", "a")
    redis.lrem("list2", 2, "a")  # Remove first 2 "a"s
    result = redis.lrange("list2", 0, -1)
    assert result == ["b", "c", "a"], f"LREM failed: {result}"
    print("✓ LREM works")

    # LTRIM
    redis.rpush("list3", "1", "2", "3", "4", "5")
    redis.ltrim("list3", 1, 3)
    result = redis.lrange("list3", 0, -1)
    assert result == ["2", "3", "4"], f"LTRIM failed: {result}"
    print("✓ LTRIM works")

    # RPOPLPUSH
    redis.rpush("src", "a", "b", "c")
    redis.rpush("dst", "x")
    popped = redis.rpoplpush("src", "dst")
    assert popped == "c", "RPOPLPUSH failed"
    assert redis.lrange("dst", 0, -1) == ["c", "x"], "RPOPLPUSH failed"
    print("✓ RPOPLPUSH works")

    print("\n✓ ALL LIST OPERATIONS PASSED\n")


def test_set_operations():
    """Test all set operations"""
    print("=" * 60)
    print("TESTING SET OPERATIONS")
    print("=" * 60)

    redis = InMemoryRedisClone()

    # SADD/SMEMBERS
    redis.sadd("set1", "a", "b", "c")
    members = redis.smembers("set1")
    assert members == {"a", "b", "c"}, "SADD/SMEMBERS failed"
    print("✓ SADD/SMEMBERS works")

    # SREM
    redis.srem("set1", "b")
    assert redis.smembers("set1") == {"a", "c"}, "SREM failed"
    print("✓ SREM works")

    # SISMEMBER
    assert redis.sismember("set1", "a") == True, "SISMEMBER failed"
    assert redis.sismember("set1", "z") == False, "SISMEMBER failed"
    print("✓ SISMEMBER works")

    # SCARD
    assert redis.scard("set1") == 2, "SCARD failed"
    print("✓ SCARD works")

    # SPOP
    redis.sadd("set2", "1", "2", "3", "4", "5")
    popped = redis.spop("set2", 2)
    assert len(popped) == 2, "SPOP failed"
    assert redis.scard("set2") == 3, "SPOP failed"
    print("✓ SPOP works")

    # SRANDMEMBER
    redis.sadd("set3", "x", "y", "z")
    member = redis.srandmember("set3")
    assert member in {"x", "y", "z"}, "SRANDMEMBER failed"
    assert redis.scard("set3") == 3, "SRANDMEMBER should not remove"
    print("✓ SRANDMEMBER works")

    # SINTER
    redis.sadd("setA", "a", "b", "c", "d")
    redis.sadd("setB", "c", "d", "e", "f")
    intersection = redis.sinter("setA", "setB")
    assert intersection == {"c", "d"}, f"SINTER failed: {intersection}"
    print("✓ SINTER works")

    # SUNION
    union = redis.sunion("setA", "setB")
    assert union == {"a", "b", "c", "d", "e", "f"}, f"SUNION failed: {union}"
    print("✓ SUNION works")

    # SDIFF
    diff = redis.sdiff("setA", "setB")
    assert diff == {"a", "b"}, f"SDIFF failed: {diff}"
    print("✓ SDIFF works")

    # SINTERSTORE/SUNIONSTORE/SDIFFSTORE
    redis.sinterstore("result", "setA", "setB")
    assert redis.smembers("result") == {"c", "d"}, "SINTERSTORE failed"
    redis.sunionstore("result", "setA", "setB")
    assert redis.smembers("result") == {"a", "b", "c", "d", "e", "f"}, "SUNIONSTORE failed"
    redis.sdiffstore("result", "setA", "setB")
    assert redis.smembers("result") == {"a", "b"}, "SDIFFSTORE failed"
    print("✓ SINTERSTORE/SUNIONSTORE/SDIFFSTORE works")

    print("\n✓ ALL SET OPERATIONS PASSED\n")


def test_sorted_set_operations():
    """Test all sorted set operations"""
    print("=" * 60)
    print("TESTING SORTED SET OPERATIONS")
    print("=" * 60)

    redis = InMemoryRedisClone()

    # ZADD
    redis.zadd("zset1", {"a": 1, "b": 2, "c": 3})
    assert redis.zcard("zset1") == 3, "ZADD failed"
    print("✓ ZADD works")

    # ZSCORE
    assert redis.zscore("zset1", "b") == 2.0, "ZSCORE failed"
    print("✓ ZSCORE works")

    # ZINCRBY
    new_score = redis.zincrby("zset1", 2.5, "a")
    assert new_score == 3.5, "ZINCRBY failed"
    print("✓ ZINCRBY works")

    # ZCARD
    assert redis.zcard("zset1") == 3, "ZCARD failed"
    print("✓ ZCARD works")

    # ZCOUNT
    assert redis.zcount("zset1", 2, 3.5) == 3, "ZCOUNT failed"  # b=2.0, c=3.0, a=3.5
    print("✓ ZCOUNT works")

    # ZRANGE
    result = redis.zrange("zset1", 0, -1)
    assert result == ["b", "c", "a"], f"ZRANGE failed: {result}"
    result_with_scores = redis.zrange("zset1", 0, -1, withscores=True)
    assert result_with_scores == [("b", 2.0), ("c", 3.0), ("a", 3.5)], "ZRANGE with scores failed"
    print("✓ ZRANGE works")

    # ZREVRANGE
    result = redis.zrevrange("zset1", 0, -1)
    assert result == ["a", "c", "b"], f"ZREVRANGE failed: {result}"
    print("✓ ZREVRANGE works")

    # ZRANGEBYSCORE
    redis.zadd("scores", {"Alice": 100, "Bob": 150, "Charlie": 120, "David": 180})
    result = redis.zrangebyscore("scores", 100, 150)
    assert set(result) == {"Alice", "Bob", "Charlie"}, f"ZRANGEBYSCORE failed: {result}"
    print("✓ ZRANGEBYSCORE works")

    # ZRANK
    assert redis.zrank("scores", "Bob") == 2, "ZRANK failed"
    print("✓ ZRANK works")

    # ZREVRANK
    assert redis.zrevrank("scores", "Bob") == 1, "ZREVRANK failed"
    print("✓ ZREVRANK works")

    # ZREM
    redis.zrem("zset1", "b")
    assert redis.zcard("zset1") == 2, "ZREM failed"
    print("✓ ZREM works")

    # ZADD with NX/XX
    redis.zadd("zset2", {"x": 1}, nx=True)
    assert redis.zadd("zset2", {"x": 2}, nx=True) == 0, "ZADD NX should not update"
    assert redis.zadd("zset2", {"x": 3}, xx=True) == 0, "ZADD XX should update"
    assert redis.zscore("zset2", "x") == 3.0, "ZADD XX failed to update"
    print("✓ ZADD with NX/XX works")

    print("\n✓ ALL SORTED SET OPERATIONS PASSED\n")


def test_hash_operations():
    """Test all hash operations"""
    print("=" * 60)
    print("TESTING HASH OPERATIONS")
    print("=" * 60)

    redis = InMemoryRedisClone()

    # HSET/HGET
    redis.hset("user:1", "name", "John")
    assert redis.hget("user:1", "name") == "John", "HSET/HGET failed"
    print("✓ HSET/HGET works")

    # HMSET/HMGET
    redis.hmset("user:2", {"name": "Jane", "age": "25", "city": "NYC"})
    result = redis.hmget("user:2", "name", "age", "nonexistent")
    assert result == ["Jane", "25", None], f"HMSET/HMGET failed: {result}"
    print("✓ HMSET/HMGET works")

    # HGETALL
    all_fields = redis.hgetall("user:2")
    assert all_fields == {"name": "Jane", "age": "25", "city": "NYC"}, "HGETALL failed"
    print("✓ HGETALL works")

    # HDEL
    redis.hdel("user:2", "city")
    assert redis.hexists("user:2", "city") == False, "HDEL failed"
    print("✓ HDEL works")

    # HEXISTS
    assert redis.hexists("user:2", "name") == True, "HEXISTS failed"
    assert redis.hexists("user:2", "nonexistent") == False, "HEXISTS failed"
    print("✓ HEXISTS works")

    # HKEYS/HVALS
    keys = redis.hkeys("user:2")
    vals = redis.hvals("user:2")
    assert set(keys) == {"name", "age"}, "HKEYS failed"
    assert set(vals) == {"Jane", "25"}, "HVALS failed"
    print("✓ HKEYS/HVALS works")

    # HLEN
    assert redis.hlen("user:2") == 2, "HLEN failed"
    print("✓ HLEN works")

    # HINCRBY
    redis.hset("user:3", "score", "100")
    new_score = redis.hincrby("user:3", "score", 50)
    assert new_score == 150, "HINCRBY failed"
    print("✓ HINCRBY works")

    # HINCRBYFLOAT
    new_score = redis.hincrbyfloat("user:3", "score", 2.5)
    assert new_score == 152.5, "HINCRBYFLOAT failed"
    print("✓ HINCRBYFLOAT works")

    # HSETNX
    assert redis.hsetnx("user:3", "name", "Bob") == 1, "HSETNX should set new field"
    assert redis.hsetnx("user:3", "name", "Alice") == 0, "HSETNX should not overwrite"
    assert redis.hget("user:3", "name") == "Bob", "HSETNX failed"
    print("✓ HSETNX works")

    print("\n✓ ALL HASH OPERATIONS PASSED\n")


def test_key_operations():
    """Test all key operations"""
    print("=" * 60)
    print("TESTING KEY OPERATIONS")
    print("=" * 60)

    redis = InMemoryRedisClone()

    # SET/GET/DELETE
    redis.set("key1", "value1")
    redis.set("key2", "value2")
    assert redis.delete("key1", "key2", "nonexistent") == 2, "DELETE failed"
    print("✓ DELETE works")

    # EXISTS
    redis.set("a", "1")
    redis.set("b", "2")
    assert redis.exists("a", "b", "c") == 2, "EXISTS failed"
    print("✓ EXISTS works")

    # KEYS
    redis.set("name", "John")
    redis.set("age", "30")
    redis.set("nation", "USA")
    keys = set(redis.keys("na*"))
    assert keys == {"name", "nation"}, f"KEYS pattern matching failed: {keys}"
    print("✓ KEYS works")

    # RANDOMKEY
    random_key = redis.randomkey()
    assert random_key in redis.keys(), "RANDOMKEY failed"
    print("✓ RANDOMKEY works")

    # RENAME
    redis.set("old", "value")
    redis.rename("old", "new")
    assert redis.exists("old") == 0, "RENAME failed - old key still exists"
    assert redis.get("new") == "value", "RENAME failed - new key not found"
    print("✓ RENAME works")

    # RENAMENX
    redis.set("key_a", "val_a")
    redis.set("key_b", "val_b")
    assert redis.renamenx("key_a", "key_b") == False, "RENAMENX should fail on existing key"
    redis.set("key_c", "val_c")
    assert redis.renamenx("key_c", "key_d") == True, "RENAMENX failed"
    print("✓ RENAMENX works")

    # TYPE
    redis.set("str", "value")
    redis.lpush("list", "item")
    redis.sadd("set", "member")
    redis.zadd("zset", {"member": 1})
    redis.hset("hash", "field", "value")

    assert redis.type("str") == "string", "TYPE failed for string"
    assert redis.type("list") == "list", "TYPE failed for list"
    assert redis.type("set") == "set", "TYPE failed for set"
    assert redis.type("zset") == "zset", "TYPE failed for sorted set"
    assert redis.type("hash") == "hash", "TYPE failed for hash"
    assert redis.type("nonexistent") == "none", "TYPE failed for nonexistent"
    print("✓ TYPE works")

    print("\n✓ ALL KEY OPERATIONS PASSED\n")


def test_expiration():
    """Test expiration operations"""
    print("=" * 60)
    print("TESTING EXPIRATION OPERATIONS")
    print("=" * 60)

    redis = InMemoryRedisClone()

    # EXPIRE
    redis.set("temp", "value")
    redis.expire("temp", 2)
    assert redis.ttl("temp") in [1, 2], "EXPIRE/TTL failed"
    print("✓ EXPIRE/TTL works")

    # PEXPIRE
    redis.set("temp2", "value")
    redis.pexpire("temp2", 2000)
    ttl_ms = redis.pttl("temp2")
    assert 1900 <= ttl_ms <= 2000, f"PEXPIRE/PTTL failed: {ttl_ms}"
    print("✓ PEXPIRE/PTTL works")

    # EXPIREAT
    future_timestamp = int(time.time()) + 3
    redis.set("temp3", "value")
    redis.expireat("temp3", future_timestamp)
    assert redis.ttl("temp3") in [2, 3], "EXPIREAT failed"
    print("✓ EXPIREAT works")

    # PERSIST
    redis.set("temp4", "value")
    redis.expire("temp4", 100)
    redis.persist("temp4")
    assert redis.ttl("temp4") == -1, "PERSIST failed"
    print("✓ PERSIST works")

    # Check actual expiration
    redis.set("expires_soon", "value")
    redis.expire("expires_soon", 1)
    time.sleep(2)
    assert redis.get("expires_soon") is None, "Key did not expire"
    print("✓ Key expiration works")

    print("\n✓ ALL EXPIRATION OPERATIONS PASSED\n")


def test_transactions():
    """Test transaction operations"""
    print("=" * 60)
    print("TESTING TRANSACTION OPERATIONS")
    print("=" * 60)

    redis = InMemoryRedisClone()

    # Basic transaction
    redis.set("balance", "100")
    with redis.pipeline() as pipe:
        pipe.multi()
        pipe.incrby("balance", 50)
        pipe.incrby("balance", 25)
        results = pipe.execute()
    assert results == [150, 175], f"Transaction failed: {results}"
    assert redis.get("balance") == "175", "Transaction result incorrect"
    print("✓ Pipeline with MULTI/EXEC works")

    # DISCARD
    redis.set("counter", "0")
    with redis.pipeline() as pipe:
        pipe.multi()
        pipe.incr("counter")
        pipe.incr("counter")
        pipe.discard()
        results = pipe.execute()
    assert redis.get("counter") == "0", "DISCARD failed"
    print("✓ DISCARD works")

    # WATCH (successful transaction)
    redis.set("watched", "10")
    with redis.pipeline() as pipe:
        pipe.watch("watched")
        pipe.multi()
        pipe.incr("watched")
        results = pipe.execute()
    assert results == [11], f"WATCH failed on unmodified key: {results}"
    print("✓ WATCH works (unmodified key)")

    # WATCH (failed transaction - key modified)
    redis.set("watched2", "5")
    with redis.pipeline() as pipe:
        pipe.watch("watched2")
        # Modify the key outside the transaction
        redis.set("watched2", "10")
        pipe.multi()
        pipe.incr("watched2")
        results = pipe.execute()
    assert results is None, "WATCH should abort transaction on modified key"
    assert redis.get("watched2") == "10", "Key should not be modified after aborted transaction"
    print("✓ WATCH works (modified key aborts transaction)")

    # Auto-execute pipeline (without explicit execute)
    redis.set("auto", "0")
    with redis.pipeline() as pipe:
        pipe.incr("auto")
        pipe.incr("auto")
        # Auto-executes on context exit
    assert redis.get("auto") == "2", "Auto-execute pipeline failed"
    print("✓ Auto-execute pipeline works")

    print("\n✓ ALL TRANSACTION OPERATIONS PASSED\n")


def test_pubsub():
    """Test pub/sub operations"""
    print("=" * 60)
    print("TESTING PUB/SUB OPERATIONS")
    print("=" * 60)

    redis = InMemoryRedisClone()

    messages = []

    def handler(channel, message):
        messages.append((channel, message))

    # Basic pub/sub
    redis.subscribe("news", handler)
    count = redis.publish("news", "Breaking News!")
    time.sleep(0.1)
    assert count == 1, "PUBLISH should return subscriber count"
    assert len(messages) == 1, "Message not received"
    assert messages[0] == ("news", "Breaking News!"), "Message content incorrect"
    print("✓ SUBSCRIBE/PUBLISH works")

    # Pattern subscription
    pattern_messages = []

    def pattern_handler(pattern, channel, message):
        pattern_messages.append((pattern, channel, message))

    redis.psubscribe("news:*", pattern_handler)
    redis.publish("news:sports", "Team wins!")
    redis.publish("news:tech", "New gadget released!")
    time.sleep(0.1)
    assert len(pattern_messages) == 2, "Pattern messages not received"
    print("✓ PSUBSCRIBE works")

    # Unsubscribe
    redis.unsubscribe("news", handler)
    redis.publish("news", "Should not receive")
    time.sleep(0.1)
    assert len(messages) == 1, "Unsubscribe failed"
    print("✓ UNSUBSCRIBE works")

    print("\n✓ ALL PUB/SUB OPERATIONS PASSED\n")


def test_thread_safety():
    """Test thread safety"""
    print("=" * 60)
    print("TESTING THREAD SAFETY")
    print("=" * 60)

    redis = InMemoryRedisClone()
    redis.set("counter", "0")

    def increment_counter():
        for _ in range(100):
            redis.incr("counter")

    threads = [threading.Thread(target=increment_counter) for _ in range(10)]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    final_count = int(redis.get("counter"))
    assert final_count == 1000, f"Thread safety failed: expected 1000, got {final_count}"
    print("✓ Thread-safe operations work")

    print("\n✓ THREAD SAFETY TEST PASSED\n")


def test_database_operations():
    """Test database operations"""
    print("=" * 60)
    print("TESTING DATABASE OPERATIONS")
    print("=" * 60)

    redis = InMemoryRedisClone()

    # DBSIZE
    redis.flushdb()
    redis.set("a", "1")
    redis.set("b", "2")
    redis.set("c", "3")
    assert redis.dbsize() == 3, "DBSIZE failed"
    print("✓ DBSIZE works")

    # FLUSHDB
    redis.flushdb()
    assert redis.dbsize() == 0, "FLUSHDB failed"
    print("✓ FLUSHDB works")

    # PING
    assert redis.ping() == "PONG", "PING failed"
    assert redis.ping("hello") == "hello", "PING with message failed"
    print("✓ PING works")

    # ECHO
    assert redis.echo("test message") == "test message", "ECHO failed"
    print("✓ ECHO works")

    # INFO
    info = redis.info()
    assert "redis_mode" in info, "INFO failed"
    print("✓ INFO works")

    # TIME
    server_time = redis.time()
    assert len(server_time) == 2, "TIME failed"
    assert isinstance(server_time[0], int), "TIME failed"
    print("✓ TIME works")

    print("\n✓ ALL DATABASE OPERATIONS PASSED\n")


def run_all_tests():
    """Run all test suites"""
    print("\n" + "=" * 60)
    print("REDIS CLONE COMPREHENSIVE TEST SUITE")
    print("=" * 60 + "\n")

    try:
        test_string_operations()
        test_list_operations()
        test_set_operations()
        test_sorted_set_operations()
        test_hash_operations()
        test_key_operations()
        test_expiration()
        test_transactions()
        test_pubsub()
        test_thread_safety()
        test_database_operations()

        print("\n" + "=" * 60)
        print("✓✓✓ ALL TESTS PASSED SUCCESSFULLY ✓✓✓")
        print("=" * 60 + "\n")

    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}\n")
        raise
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}\n")
        raise


if __name__ == "__main__":
    run_all_tests()