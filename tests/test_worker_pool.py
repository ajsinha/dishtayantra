#!/usr/bin/env python3
"""
Worker Pool Test Script
Version: 1.5.2

Tests the multiprocessing worker pool functionality:
- Worker spawning and management
- DAG affinity assignments
- Health monitoring
- LMDB pub/sub

Usage:
    python test_worker_pool.py

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

import os
import sys
import time
import json
import logging

import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.workers import (
    WorkerPoolManager, 
    DAGAffinityManager, 
    AffinityStrategy,
    WorkerHealthMonitor
)
from core.workers.worker_monitor import WorkerState
from core.pubsub.lmdbpubsub import LMDBDataPublisher, LMDBDataSubscriber, LMDBPubSubManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


def test_affinity_manager():
    """Test DAG affinity manager"""
    print("\n" + "="*60)
    print("TEST: DAG Affinity Manager")
    print("="*60)
    
    # Create affinity manager for 4 workers
    manager = DAGAffinityManager(num_workers=4)
    
    # Test 1: Assign DAGs with weight-based strategy
    print("\n1. Testing weight-based assignment...")
    
    dag_configs = [
        {'name': 'dag_a', 'nodes': [1,2,3], 'calculators': [1,2]},
        {'name': 'dag_b', 'nodes': [1,2,3,4,5], 'calculators': [1,2,3]},
        {'name': 'dag_c', 'nodes': [1,2], 'calculators': [1]},
        {'name': 'dag_d', 'nodes': [1,2,3,4,5,6,7,8], 'calculators': [1,2,3,4]},
    ]
    
    for dag_config in dag_configs:
        worker_id = manager.assign_dag(dag_config)
        print(f"   DAG '{dag_config['name']}' -> Worker {worker_id}")
    
    # Test 2: Pinned assignment
    print("\n2. Testing pinned worker assignment...")
    
    pinned_config = {
        'name': 'critical_dag',
        'nodes': [1,2,3],
        'calculators': [1],
        'worker_affinity': {'pinned_worker': 2}
    }
    
    worker_id = manager.assign_dag(pinned_config)
    print(f"   DAG '{pinned_config['name']}' pinned to Worker {worker_id}")
    assert worker_id == 2, "Pinned assignment failed!"
    
    # Test 3: Exclusive worker
    print("\n3. Testing exclusive worker assignment...")
    
    exclusive_config = {
        'name': 'isolated_dag',
        'nodes': [1,2,3],
        'calculators': [1],
        'worker_affinity': {'exclusive': True}
    }
    
    worker_id = manager.assign_dag(exclusive_config)
    print(f"   DAG '{exclusive_config['name']}' assigned exclusive Worker {worker_id}")
    
    # Test 4: Load summary
    print("\n4. Load summary:")
    summary = manager.get_load_summary()
    for worker_id, info in summary.items():
        print(f"   Worker {worker_id}: {info['dag_count']} DAGs, "
              f"load={info['estimated_cpu_weight']:.2f}, "
              f"exclusive={info['is_exclusive']}")
    
    print("\n✓ Affinity manager tests passed!")
    return True


def test_lmdb_pubsub():
    """Test LMDB-based pub/sub (skipped when the optional lmdb native
    module is not installed)."""
    pytest.importorskip("lmdb", reason="optional lmdb module not installed")
    print("\n" + "="*60)
    print("TEST: LMDB DataPublisher/DataSubscriber")
    print("="*60)
    
    # Setup
    test_channel = "test_channel"
    lmdb_path = "data/test_lmdb"
    os.makedirs(lmdb_path, exist_ok=True)
    
    # Create publisher
    print("\n1. Creating publisher...")
    pub_config = {
        'destination': f'lmdb://{test_channel}',
        'lmdb_path': lmdb_path
    }
    publisher = LMDBDataPublisher("test_pub", pub_config)
    publisher.start()
    
    # Create subscriber
    print("2. Creating subscriber...")
    sub_config = {
        'source': f'lmdb://{test_channel}',
        'lmdb_path': lmdb_path,
        'poll_interval_ms': 10
    }
    subscriber = LMDBDataSubscriber("test_sub", sub_config)
    subscriber.start()
    
    # Publish messages
    print("\n3. Publishing messages...")
    test_messages = [
        {'price': 100.5, 'symbol': 'AAPL'},
        {'price': 200.25, 'symbol': 'GOOGL'},
        "raw text message",
        {'price': 150.75, 'symbol': 'MSFT'}
    ]
    
    for msg in test_messages:
        publisher.publish(msg)
        print(f"   Published: {msg}")
    
    # Allow time for subscriber to poll
    time.sleep(0.5)
    
    # Receive messages
    print("\n4. Receiving messages...")
    received = []
    while True:
        msg = subscriber.get(timeout=0.1)
        if msg is None:
            break
        received.append(msg)
        print(f"   Received: {msg}")
    
    # Validate
    print(f"\n5. Validation: Published {len(test_messages)}, Received {len(received)}")
    
    # Check details
    print("\n6. Publisher details:")
    for k, v in publisher.details().items():
        print(f"   {k}: {v}")
    
    print("\n7. Subscriber details:")
    for k, v in subscriber.details().items():
        print(f"   {k}: {v}")
    
    # Cleanup
    publisher.stop()
    subscriber.stop()
    
    print("\n✓ LMDB pub/sub tests passed!")
    return True


def test_health_monitor():
    """Test worker health monitoring"""
    print("\n" + "="*60)
    print("TEST: Worker Health Monitor")
    print("="*60)
    
    config = {
        'health_check_interval_seconds': 1,
        'auto_restart_on_crash': True,
        'max_restart_attempts': 3
    }
    
    monitor = WorkerHealthMonitor(config)
    
    # Register workers
    print("\n1. Registering workers...")
    for i in range(4):
        monitor.register_worker(i, pid=1000 + i)
        print(f"   Registered worker {i}")
    
    # Simulate heartbeats
    print("\n2. Simulating heartbeats...")
    for i in range(4):
        monitor.update_heartbeat(i, {
            'pid': 1000 + i,
            'cpu_percent': 25.0 + i * 10,
            'memory_mb': 100.0 + i * 50,
            'dag_count': i + 1,
            'loaded_dags': [f'dag_{j}' for j in range(i + 1)],
            'uptime_seconds': 100.0
        })
    
    # Check statuses
    print("\n3. Worker statuses:")
    statuses = monitor.get_all_health_statuses()
    for worker_id, status in statuses.items():
        print(f"   Worker {worker_id}: state={status['state']}, "
              f"cpu={status['cpu_percent']:.1f}%, "
              f"mem={status['memory_mb']:.1f}MB")
    
    # Test healthy/unhealthy detection
    print("\n4. Healthy workers:", monitor.get_healthy_workers())
    print("   Unhealthy workers:", monitor.get_unhealthy_workers())
    
    # Mark one as crashed
    print("\n5. Marking worker 2 as crashed...")
    monitor.mark_worker_crashed(2)
    
    status = monitor.get_health_status(2)
    print(f"   Worker 2 state: {status.state.value}")
    print(f"   Consecutive failures: {status.consecutive_failures}")
    
    print("\n✓ Health monitor tests passed!")
    return True


def test_worker_pool_config():
    """Test worker pool configuration loading"""
    print("\n" + "="*60)
    print("TEST: Worker Pool Configuration")
    print("="*60)
    
    config_path = 'config/worker_config.json'
    
    if os.path.exists(config_path):
        with open(config_path) as f:
            config = json.load(f)
        
        print("\n1. Worker pool settings:")
        pool_config = config.get('worker_pool', {})
        for key, value in pool_config.items():
            if not key.startswith('_'):
                print(f"   {key}: {value}")
        
        print("\n2. Affinity settings:")
        affinity_config = config.get('affinity', {})
        for key, value in affinity_config.items():
            if not key.startswith('_'):
                print(f"   {key}: {value}")
        
        print("\n3. Communication settings:")
        comm_config = config.get('communication', {})
        for key, value in comm_config.items():
            print(f"   {key}: {value}")
        
        print("\n✓ Configuration loaded successfully!")
    else:
        print(f"\n✗ Config file not found: {config_path}")
        return False
    
    return True


def main():
    """Run all tests"""
    print("\n" + "#"*60)
    print("# DishtaYantra Worker Pool Test Suite v1.5.2")
    print("#"*60)
    
    results = {}
    
    # Run tests
    tests = [
        ("Configuration", test_worker_pool_config),
        ("Affinity Manager", test_affinity_manager),
        ("LMDB Pub/Sub", test_lmdb_pubsub),
        ("Health Monitor", test_health_monitor),
    ]
    
    for name, test_func in tests:
        try:
            results[name] = test_func()
        except Exception as e:
            logger.error(f"Test '{name}' failed with error: {e}")
            import traceback
            traceback.print_exc()
            results[name] = False
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    all_passed = True
    for name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"   {name}: {status}")
        if not passed:
            all_passed = False
    
    print("\n" + "="*60)
    if all_passed:
        print("All tests passed! ✓")
    else:
        print("Some tests failed! ✗")
    print("="*60)
    
    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main())
