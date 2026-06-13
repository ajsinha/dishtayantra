"""Demo of core.pubsub.inmemory_redisclone (moved out of the module in
v2.2 so the production module stays within the 500-line limit). Run:

    python examples/demo_inmemory_redisclone.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from core.pubsub.inmemory_redisclone import InMemoryRedisClone  # noqa: E402

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