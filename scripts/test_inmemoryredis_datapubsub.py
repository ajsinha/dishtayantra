"""
Comprehensive tests for InMemoryRedis DataPublisher and DataSubscriber
"""

import unittest
import time
import threading
from datetime import datetime
from inmemory_redisclone import InMemoryRedisClone
from inmemoryredis_datapubsub import (
    InMemoryRedisDataPublisher,
    InMemoryRedisChannelDataPublisher,
    InMemoryRedisChannelDataSubscriber,
    InMemoryRedisDataSubscriber
)


class TestInMemoryRedisDataPublisher(unittest.TestCase):
    """Test InMemoryRedisDataPublisher"""

    def setUp(self):
        """Set up test fixtures"""
        self.redis = InMemoryRedisClone()
        self.publisher = InMemoryRedisDataPublisher(
            name="test_publisher",
            destination="inmemoryredis://",
            config={
                'redis_instance': self.redis,
                'key_prefix': 'test:'
            }
        )

    def tearDown(self):
        """Clean up"""
        self.publisher.stop()
        self.redis.flushdb()

    def test_basic_publish(self):
        """Test basic publishing"""
        data = {
            '__dagserver_key': 'mykey',
            'value': 'test_data',
            'count': 42
        }

        self.publisher.publish(data)

        # Verify data was stored
        stored_value = self.redis.get('test:mykey')
        self.assertIsNotNone(stored_value)

        import json
        stored_data = json.loads(stored_value)
        self.assertEqual(stored_data['value'], 'test_data')
        self.assertEqual(stored_data['count'], 42)
        self.assertNotIn('__dagserver_key', stored_data)

    def test_publish_with_ttl(self):
        """Test publishing with TTL"""
        data = {
            '__dagserver_key': 'expiring_key',
            '__ttl_seconds': 2,
            'value': 'expires_soon'
        }

        self.publisher.publish(data)

        # Check key exists
        self.assertEqual(self.redis.exists('test:expiring_key'), 1)

        # Check TTL is set
        ttl = self.redis.ttl('test:expiring_key')
        self.assertGreater(ttl, 0)
        self.assertLessEqual(ttl, 2)

    def test_publish_stats(self):
        """Test publisher statistics"""
        for i in range(5):
            self.publisher.publish({
                '__dagserver_key': f'key_{i}',
                'index': i
            })

        stats = self.publisher.details()
        self.assertEqual(stats['publish_count'], 5)
        self.assertIsNotNone(stats['last_publish'])

    def test_missing_key(self):
        """Test publishing without __dagserver_key"""
        data = {'value': 'no_key'}

        # Should not raise exception, just log error
        self.publisher.publish(data)

        # No data should be stored
        keys = self.redis.keys('test:*')
        self.assertEqual(len(keys), 0)


class TestInMemoryRedisChannelPublisher(unittest.TestCase):
    """Test InMemoryRedisChannelDataPublisher"""

    def setUp(self):
        """Set up test fixtures"""
        self.redis = InMemoryRedisClone()
        self.publisher = InMemoryRedisChannelDataPublisher(
            name="channel_publisher",
            destination="inmemoryredischannel://test_channel",
            config={'redis_instance': self.redis}
        )

    def tearDown(self):
        """Clean up"""
        self.publisher.stop()
        self.redis.flushdb()

    def test_channel_publish(self):
        """Test publishing to channel"""
        received_messages = []

        def callback(channel, message):
            received_messages.append(message)

        # Subscribe before publishing
        self.redis.subscribe('test_channel', callback)

        # Publish message
        data = {'event': 'test', 'message': 'hello'}
        self.publisher.publish(data)

        # Give time for callback
        time.sleep(0.1)

        # Verify message received
        self.assertEqual(len(received_messages), 1)

        import json
        received_data = json.loads(received_messages[0])
        self.assertEqual(received_data['event'], 'test')
        self.assertEqual(received_data['message'], 'hello')

    def test_multiple_subscribers(self):
        """Test publishing to multiple subscribers"""
        received_1 = []
        received_2 = []

        def callback_1(channel, message):
            received_1.append(message)

        def callback_2(channel, message):
            received_2.append(message)

        # Two subscribers
        self.redis.subscribe('test_channel', callback_1)
        self.redis.subscribe('test_channel', callback_2)

        # Publish one message
        self.publisher.publish({'message': 'broadcast'})

        time.sleep(0.1)

        # Both should receive
        self.assertEqual(len(received_1), 1)
        self.assertEqual(len(received_2), 1)


class TestInMemoryRedisChannelSubscriber(unittest.TestCase):
    """Test InMemoryRedisChannelDataSubscriber"""

    def setUp(self):
        """Set up test fixtures"""
        self.redis = InMemoryRedisClone()
        self.subscriber = InMemoryRedisChannelDataSubscriber(
            name="channel_subscriber",
            source="inmemoryredischannel://test_channel",
            config={'redis_instance': self.redis}
        )

    def tearDown(self):
        """Clean up"""
        self.subscriber.stop()
        self.redis.flushdb()

    def test_receive_message(self):
        """Test receiving messages from channel"""
        # Publish a message
        import json
        self.redis.publish('test_channel', json.dumps({'data': 'test'}))

        time.sleep(0.1)

        # Receive the message
        data = self.subscriber._do_subscribe()
        self.assertIsNotNone(data)
        self.assertEqual(data['data'], 'test')

    def test_multiple_messages(self):
        """Test receiving multiple messages"""
        import json

        # Publish multiple messages
        for i in range(3):
            self.redis.publish('test_channel', json.dumps({'index': i}))

        time.sleep(0.1)

        # Receive all messages
        messages = []
        for _ in range(3):
            data = self.subscriber._do_subscribe()
            if data:
                messages.append(data)

        self.assertEqual(len(messages), 3)
        self.assertEqual(messages[0]['index'], 0)
        self.assertEqual(messages[1]['index'], 1)
        self.assertEqual(messages[2]['index'], 2)

    def test_subscriber_stats(self):
        """Test subscriber statistics"""
        import json

        # Send some messages
        for i in range(5):
            self.redis.publish('test_channel', json.dumps({'count': i}))

        time.sleep(0.1)

        # Receive them
        for _ in range(5):
            self.subscriber._do_subscribe()

        stats = self.subscriber.details()
        self.assertEqual(stats['receive_count'], 5)
        self.assertIsNotNone(stats['last_receive'])


class TestInMemoryRedisDataSubscriber(unittest.TestCase):
    """Test InMemoryRedisDataSubscriber (key polling)"""

    def setUp(self):
        """Set up test fixtures"""
        self.redis = InMemoryRedisClone()
        self.subscriber = InMemoryRedisDataSubscriber(
            name="key_subscriber",
            source="inmemoryredis://",
            config={
                'redis_instance': self.redis,
                'key_pattern': 'queue:*',
                'delete_on_read': False
            }
        )

    def tearDown(self):
        """Clean up"""
        self.subscriber.stop()
        self.redis.flushdb()

    def test_poll_keys(self):
        """Test polling for new keys"""
        import json

        # Add a key
        self.redis.set('queue:msg1', json.dumps({'data': 'message1'}))

        # Poll for it
        data = self.subscriber._do_subscribe()
        self.assertIsNotNone(data)
        self.assertEqual(data['data'], 'message1')
        self.assertEqual(data['__dagserver_key'], 'queue:msg1')

    def test_delete_on_read(self):
        """Test delete_on_read option"""
        import json

        # Create subscriber with delete_on_read
        subscriber = InMemoryRedisDataSubscriber(
            name="delete_subscriber",
            source="inmemoryredis://",
            config={
                'redis_instance': self.redis,
                'key_pattern': 'temp:*',
                'delete_on_read': True
            }
        )

        # Add a key
        self.redis.set('temp:msg', json.dumps({'data': 'test'}))

        # Key should exist
        self.assertEqual(self.redis.exists('temp:msg'), 1)

        # Read it
        data = subscriber._do_subscribe()
        self.assertIsNotNone(data)

        # Key should be deleted
        self.assertEqual(self.redis.exists('temp:msg'), 0)

        subscriber.stop()

    def test_no_duplicate_reads(self):
        """Test that keys are only read once"""
        import json

        # Add one key
        self.redis.set('queue:msg1', json.dumps({'data': 'once'}))

        # Read it first time
        data1 = self.subscriber._do_subscribe()
        self.assertIsNotNone(data1)

        # Try to read again - should get None (no new keys)
        data2 = self.subscriber._do_subscribe()
        self.assertIsNone(data2)

    def test_reset_seen_keys(self):
        """Test resetting seen keys"""
        import json

        # Add and read a key
        self.redis.set('queue:msg1', json.dumps({'data': 'test'}))
        data1 = self.subscriber._do_subscribe()
        self.assertIsNotNone(data1)

        # Try to read again - should get None
        data2 = self.subscriber._do_subscribe()
        self.assertIsNone(data2)

        # Reset seen keys
        self.subscriber.reset_seen_keys()

        # Now should be able to read the same key again
        data3 = self.subscriber._do_subscribe()
        self.assertIsNotNone(data3)
        self.assertEqual(data3['data'], 'test')


class TestIntegration(unittest.TestCase):
    """Integration tests"""

    def setUp(self):
        """Set up test fixtures"""
        self.redis = InMemoryRedisClone()

    def tearDown(self):
        """Clean up"""
        self.redis.flushdb()

    def test_publisher_subscriber_flow(self):
        """Test complete publisher to subscriber flow"""
        # Create publisher and subscriber
        publisher = InMemoryRedisDataPublisher(
            name="pub",
            destination="inmemoryredis://",
            config={
                'redis_instance': self.redis,
                'key_prefix': 'flow:'
            }
        )

        subscriber = InMemoryRedisDataSubscriber(
            name="sub",
            source="inmemoryredis://",
            config={
                'redis_instance': self.redis,
                'key_pattern': 'flow:*',
                'delete_on_read': True
            }
        )

        # Publish messages
        for i in range(3):
            publisher.publish({
                '__dagserver_key': f'msg_{i}',
                'index': i,
                'content': f'Message {i}'
            })

        time.sleep(0.1)

        # Subscribe and receive all
        received = []
        for _ in range(3):
            data = subscriber._do_subscribe()
            if data:
                received.append(data)
            time.sleep(0.05)

        self.assertEqual(len(received), 3)
        self.assertEqual(received[0]['index'], 0)
        self.assertEqual(received[1]['index'], 1)
        self.assertEqual(received[2]['index'], 2)

        # All keys should be deleted
        self.assertEqual(len(self.redis.keys('flow:*')), 0)

        publisher.stop()
        subscriber.stop()

    def test_channel_broadcast(self):
        """Test channel broadcasting to multiple subscribers"""
        # Create publisher
        publisher = InMemoryRedisChannelDataPublisher(
            name="broadcaster",
            destination="inmemoryredischannel://broadcast",
            config={'redis_instance': self.redis}
        )

        # Create multiple subscribers
        sub1 = InMemoryRedisChannelDataSubscriber(
            name="sub1",
            source="inmemoryredischannel://broadcast",
            config={'redis_instance': self.redis}
        )

        sub2 = InMemoryRedisChannelDataSubscriber(
            name="sub2",
            source="inmemoryredischannel://broadcast",
            config={'redis_instance': self.redis}
        )

        # Publish one message
        publisher.publish({
            'event': 'notification',
            'message': 'Important update'
        })

        time.sleep(0.1)

        # Both subscribers should receive
        data1 = sub1._do_subscribe()
        data2 = sub2._do_subscribe()

        self.assertIsNotNone(data1)
        self.assertIsNotNone(data2)
        self.assertEqual(data1['message'], 'Important update')
        self.assertEqual(data2['message'], 'Important update')

        publisher.stop()
        sub1.stop()
        sub2.stop()


def run_tests():
    """Run all tests"""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestInMemoryRedisDataPublisher))
    suite.addTests(loader.loadTestsFromTestCase(TestInMemoryRedisChannelPublisher))
    suite.addTests(loader.loadTestsFromTestCase(TestInMemoryRedisChannelSubscriber))
    suite.addTests(loader.loadTestsFromTestCase(TestInMemoryRedisDataSubscriber))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegration))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()


if __name__ == '__main__':
    print("\n" + "=" * 70)
    print("InMemoryRedis DataPublisher/DataSubscriber Test Suite")
    print("=" * 70 + "\n")

    success = run_tests()

    print("\n" + "=" * 70)
    if success:
        print("✓✓✓ ALL TESTS PASSED ✓✓✓")
    else:
        print("❌ SOME TESTS FAILED")
    print("=" * 70 + "\n")

    exit(0 if success else 1)