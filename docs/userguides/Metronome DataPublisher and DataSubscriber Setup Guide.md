# Metronome DataPublisher and DataSubscriber Setup Guide

## Overview

This guide provides instructions for setting up and using the Metronome DataPublisher and DataSubscriber implementations for the DAG Server framework. These components generate messages at regular intervals, like a metronome keeping time, useful for testing, scheduling, heartbeats, and time-based workflows.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Concepts](#concepts)
3. [Components](#components)
4. [Configuration](#configuration)
5. [Usage Examples](#usage-examples)
6. [Use Cases](#use-cases)
7. [Advanced Patterns](#advanced-patterns)
8. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Dependencies

- Python 3.7 or higher
- DAG Server framework with `core.pubsub.datapubsub` module

### Python Packages

No additional packages required - uses only Python standard library.

---

## Concepts

### What is a Metronome Publisher/Subscriber?

Unlike traditional publishers and subscribers that respond to external data:

**Traditional Pattern:**
```
External Data → Publisher → Downstream Systems
External Data → Subscriber → Processing
```

**Metronome Pattern:**
```
Internal Timer → Metronome → Self-Generated Messages
```

### Key Characteristics

1. **Self-Generating**: Creates its own messages internally
2. **Time-Based**: Triggers at regular intervals
3. **No External Source**: Doesn't require external data
4. **Predictable**: Messages arrive at consistent intervals
5. **Stateless**: Each message is independent

### When to Use

- **Testing**: Generate test data at regular intervals
- **Heartbeats**: Send keep-alive signals
- **Scheduling**: Trigger periodic tasks
- **Monitoring**: Poll systems regularly
- **Demo/Development**: Simulate data streams
- **Orchestration**: Coordinate timing of workflows

---

## Components

### Available Classes

#### 1. **MetronomeDataPublisher**

Automatically publishes messages at regular intervals without external trigger.

**Key Features:**
- Runs in background thread
- Configurable interval
- Customizable message content
- Automatic lifecycle management
- Thread-safe operations
- Self-starting

**Message Format:**
```python
{
    'message': 'tick',  # Configurable
    'current_timestamp': '20251022103045'  # YYYYMMDDHHmmss
}
```

#### 2. **MetronomeDataSubscriber**

Generates messages at regular intervals when polled.

**Key Features:**
- On-demand generation
- Configurable interval
- Customizable message content
- No background threads
- Lightweight

**Message Format:**
```python
{
    'message': 'tick',  # Configurable
    'current_timestamp': '20251022103045'  # YYYYMMDDHHmmss
}
```

### Comparison

| Feature | Publisher | Subscriber |
|---------|-----------|------------|
| Background Thread | Yes | No |
| Auto-Start | Yes | Manual poll |
| Resource Usage | Moderate | Minimal |
| Use Case | Continuous generation | On-demand generation |

---

## Configuration

### MetronomeDataPublisher Configuration

```python
config = {
    'interval': 1,          # Seconds between messages (default: 1)
    'message': 'tick'       # Message content (default: 'tick')
}
```

**Note:** `interval` overrides the default `publish_interval`.

### MetronomeDataSubscriber Configuration

```python
config = {
    'interval': 1,          # Seconds between messages (default: 1)
    'message': 'tick'       # Message content (default: 'tick')
}
```

### Destination/Source Format

```
metronome://identifier_name
```

The identifier is used for logging and identification purposes.

---

## Usage Examples

### Example 1: Basic Metronome Publisher

```python
from metronome_datapubsub import MetronomeDataPublisher
import time

# Create metronome that ticks every second
config = {
    'interval': 1,
    'message': 'tick'
}

publisher = MetronomeDataPublisher(
    name='basic_metronome',
    destination='metronome://timer',
    config=config
)

# Metronome starts automatically
print("Metronome started, generating messages...")

# Let it run for 10 seconds
time.sleep(10)

# Stop metronome
publisher.stop()

print(f"Generated {publisher._publish_count} messages")
```

### Example 2: Fast Metronome (High Frequency)

```python
from metronome_datapubsub import MetronomeDataPublisher
import time

# Fast metronome - 10 times per second
config = {
    'interval': 0.1,  # 100ms
    'message': 'rapid_tick'
}

publisher = MetronomeDataPublisher(
    name='fast_metronome',
    destination='metronome://fast_timer',
    config=config
)

print("Fast metronome started...")
time.sleep(5)
publisher.stop()

print(f"Generated {publisher._publish_count} messages in 5 seconds")
print(f"Rate: {publisher._publish_count / 5:.1f} messages/second")
```

### Example 3: Slow Metronome (Low Frequency)

```python
from metronome_datapubsub import MetronomeDataPublisher
import time

# Slow metronome - once every 5 seconds
config = {
    'interval': 5,
    'message': 'slow_tick'
}

publisher = MetronomeDataPublisher(
    name='slow_metronome',
    destination='metronome://slow_timer',
    config=config
)

print("Slow metronome started...")
print("Waiting for ticks (every 5 seconds)...")

# Monitor for 30 seconds
for i in range(6):
    time.sleep(5)
    print(f"Tick #{publisher._publish_count}")

publisher.stop()
```

### Example 4: Heartbeat Monitor

```python
from metronome_datapubsub import MetronomeDataPublisher
import time
from datetime import datetime

# Heartbeat every 30 seconds
config = {
    'interval': 30,
    'message': 'heartbeat'
}

publisher = MetronomeDataPublisher(
    name='heartbeat',
    destination='metronome://system_heartbeat',
    config=config
)

print("System heartbeat started...")
print("Heartbeat will occur every 30 seconds")

# Monitor heartbeats
try:
    while True:
        time.sleep(1)
        # Your application logic here
        
except KeyboardInterrupt:
    print("\nStopping heartbeat...")
    publisher.stop()
    print(f"Total heartbeats sent: {publisher._publish_count}")
```

### Example 5: Custom Message Content

```python
from metronome_datapubsub import MetronomeDataPublisher
import time

# Metronome with custom message
config = {
    'interval': 2,
    'message': 'custom_event_trigger'
}

publisher = MetronomeDataPublisher(
    name='custom_metronome',
    destination='metronome://custom',
    config=config
)

print("Custom metronome started...")
time.sleep(20)
publisher.stop()

print(f"Sent {publisher._publish_count} custom event triggers")
```

### Example 6: Metronome Subscriber

```python
from metronome_datapubsub import MetronomeDataSubscriber
import time

# Create metronome subscriber
config = {
    'interval': 1,
    'message': 'subscriber_tick'
}

subscriber = MetronomeDataSubscriber(
    name='tick_subscriber',
    source='metronome://subscriber_timer',
    config=config
)

# Start subscriber
subscriber.start()

print("Subscriber started, receiving ticks...")

# Run for 10 seconds
time.sleep(10)

# Stop subscriber
subscriber.stop()

print(f"Received {subscriber.get_message_count()} ticks")
```

### Example 7: Multiple Metronomes

```python
from metronome_datapubsub import MetronomeDataPublisher
import time

# Create multiple metronomes with different intervals
metronomes = {
    'fast': MetronomeDataPublisher(
        'fast_metro',
        'metronome://fast',
        {'interval': 0.5, 'message': 'fast_tick'}
    ),
    'medium': MetronomeDataPublisher(
        'medium_metro',
        'metronome://medium',
        {'interval': 1, 'message': 'medium_tick'}
    ),
    'slow': MetronomeDataPublisher(
        'slow_metro',
        'metronome://slow',
        {'interval': 2, 'message': 'slow_tick'}
    )
}

print("Started multiple metronomes...")
time.sleep(10)

# Stop all and show results
for name, metro in metronomes.items():
    metro.stop()
    print(f"{name}: {metro._publish_count} ticks")
```

### Example 8: Scheduled Task Trigger

```python
from metronome_datapubsub import MetronomeDataPublisher
import time
from datetime import datetime

# Metronome to trigger tasks every minute
config = {
    'interval': 60,  # 1 minute
    'message': 'task_trigger'
}

publisher = MetronomeDataPublisher(
    name='task_scheduler',
    destination='metronome://task_scheduler',
    config=config
)

print("Task scheduler started (triggers every minute)...")
print("Press Ctrl+C to stop")

try:
    while True:
        time.sleep(1)
        
        # Your monitoring or other logic
        if publisher._publish_count > 0:
            print(f"Tasks triggered: {publisher._publish_count}")
            
except KeyboardInterrupt:
    print("\nStopping scheduler...")
    publisher.stop()
```

---

## Use Cases

### Use Case 1: Testing Data Pipeline

```python
from metronome_datapubsub import MetronomeDataPublisher
import time

# Generate test data every second
config = {
    'interval': 1,
    'message': 'test_data'
}

publisher = MetronomeDataPublisher(
    name='test_generator',
    destination='metronome://test',
    config=config
)

print("Generating test data for pipeline...")
print("Downstream systems should receive data every second")

# Run test for 1 minute
time.sleep(60)
publisher.stop()

print(f"Test complete: {publisher._publish_count} messages sent")
```

### Use Case 2: Keep-Alive / Health Check

```python
from metronome_datapubsub import MetronomeDataPublisher
import time

# Send keep-alive every 10 seconds
config = {
    'interval': 10,
    'message': 'keep_alive'
}

publisher = MetronomeDataPublisher(
    name='keepalive',
    destination='metronome://health',
    config=config
)

print("Keep-alive service started...")

# Your application runs here
try:
    while True:
        # Do your work
        time.sleep(1)
except KeyboardInterrupt:
    publisher.stop()
```

### Use Case 3: Periodic Monitoring

```python
from metronome_datapubsub import MetronomeDataSubscriber
import time
import psutil

# Check system metrics every 5 seconds
config = {
    'interval': 5,
    'message': 'check_system'
}

subscriber = MetronomeDataSubscriber(
    name='system_monitor',
    source='metronome://monitor',
    config=config
)

subscriber.start()

print("System monitoring started...")

# Custom monitoring loop
last_check = time.time()

try:
    while True:
        current_time = time.time()
        
        # Check if it's time to monitor
        if current_time - last_check >= 5:
            cpu = psutil.cpu_percent()
            memory = psutil.virtual_memory().percent
            
            print(f"CPU: {cpu}%, Memory: {memory}%")
            last_check = current_time
        
        time.sleep(0.1)
        
except KeyboardInterrupt:
    subscriber.stop()
```

### Use Case 4: Rate-Limited Operations

```python
from metronome_datapubsub import MetronomeDataPublisher
import time

# Limit operations to once per 2 seconds
config = {
    'interval': 2,
    'message': 'rate_limit_token'
}

publisher = MetronomeDataPublisher(
    name='rate_limiter',
    destination='metronome://rate_limit',
    config=config
)

# Simulated work queue
work_queue = list(range(100))

print("Processing work with rate limiting...")

processed = 0
while work_queue:
    # Wait for next "token"
    time.sleep(2)
    
    # Process one item
    item = work_queue.pop(0)
    print(f"Processing item {item}")
    processed += 1

publisher.stop()
print(f"Processed {processed} items with rate limiting")
```

### Use Case 5: Demo Data Generation

```python
from metronome_datapubsub import MetronomeDataPublisher
import time
import random

# Generate demo data for presentation
config = {
    'interval': 3,
    'message': 'demo_data'
}

publisher = MetronomeDataPublisher(
    name='demo_generator',
    destination='metronome://demo',
    config=config
)

print("Generating demo data for presentation...")
print("Simulating sensor readings...")

# Simulate for 30 seconds
for _ in range(10):
    time.sleep(3)
    
    # Generate fake sensor reading
    temperature = 20 + random.uniform(-5, 5)
    humidity = 60 + random.uniform(-10, 10)
    
    print(f"Sensor Reading - Temp: {temperature:.1f}°C, Humidity: {humidity:.1f}%")

publisher.stop()
```

### Use Case 6: Workflow Orchestration

```python
from metronome_datapubsub import MetronomeDataPublisher
import time

# Orchestrate multi-step workflow
config = {
    'interval': 5,
    'message': 'workflow_step'
}

publisher = MetronomeDataPublisher(
    name='workflow_orchestrator',
    destination='metronome://workflow',
    config=config
)

steps = [
    'Initialize',
    'Fetch Data',
    'Process Data',
    'Validate Results',
    'Store Results',
    'Cleanup'
]

print("Workflow orchestration started...")

for i, step in enumerate(steps):
    time.sleep(5)
    print(f"Step {i+1}: {step}")
    
    # Simulate step execution
    time.sleep(2)
    print(f"  ✓ {step} completed")

publisher.stop()
print("Workflow complete!")
```

### Use Case 7: Load Testing

```python
from metronome_datapubsub import MetronomeDataPublisher
import time

# Generate consistent load for testing
config = {
    'interval': 0.01,  # 100 messages per second
    'message': 'load_test'
}

publisher = MetronomeDataPublisher(
    name='load_tester',
    destination='metronome://load_test',
    config=config
)

print("Load test started: 100 messages/second")
print("Testing system under load...")

# Run load test for 60 seconds
time.sleep(60)

publisher.stop()

total_messages = publisher._publish_count
rate = total_messages / 60

print(f"\n=== Load Test Results ===")
print(f"Total messages: {total_messages}")
print(f"Duration: 60 seconds")
print(f"Actual rate: {rate:.2f} messages/second")
```

---

## Advanced Patterns

### Pattern 1: Cascading Metronomes

Create dependent timers:

```python
from metronome_datapubsub import MetronomeDataPublisher
import time

# Primary metronome (fast)
primary = MetronomeDataPublisher(
    'primary',
    'metronome://primary',
    {'interval': 1, 'message': 'primary_tick'}
)

# Secondary metronome (slower)
secondary = MetronomeDataPublisher(
    'secondary',
    'metronome://secondary',
    {'interval': 5, 'message': 'secondary_tick'}
)

# Tertiary metronome (slowest)
tertiary = MetronomeDataPublisher(
    'tertiary',
    'metronome://tertiary',
    {'interval': 15, 'message': 'tertiary_tick'}
)

print("Cascading timers started...")
time.sleep(30)

primary.stop()
secondary.stop()
tertiary.stop()

print(f"Primary: {primary._publish_count} ticks")
print(f"Secondary: {secondary._publish_count} ticks")
print(f"Tertiary: {tertiary._publish_count} ticks")
```

### Pattern 2: Conditional Metronome

Start/stop based on conditions:

```python
from metronome_datapubsub import MetronomeDataPublisher
import time
import random

config = {'interval': 1, 'message': 'conditional_tick'}

publisher = None
running = False

print("Conditional metronome demo...")

for i in range(30):
    # Random condition
    should_run = random.random() > 0.5
    
    if should_run and not running:
        publisher = MetronomeDataPublisher(
            'conditional',
            'metronome://conditional',
            config
        )
        running = True
        print("▶ Metronome started")
    
    elif not should_run and running:
        publisher.stop()
        running = False
        print("■ Metronome stopped")
    
    time.sleep(1)

if running:
    publisher.stop()
```

### Pattern 3: Metronome with Side Effects

Trigger actions on each tick:

```python
from metronome_datapubsub import MetronomeDataSubscriber
import time

class ActionMetronome:
    """Metronome that performs actions"""
    
    def __init__(self, interval, action_func):
        config = {'interval': interval, 'message': 'action_tick'}
        
        self.subscriber = MetronomeDataSubscriber(
            'action_metro',
            'metronome://actions',
            config
        )
        
        self.action_func = action_func
        self.subscriber.start()
    
    def run(self, duration):
        """Run for specified duration"""
        start = time.time()
        
        while time.time() - start < duration:
            time.sleep(0.1)
            
            # Check for tick and perform action
            if self.subscriber.get_message_count() > self.last_count:
                self.action_func()
                self.last_count = self.subscriber.get_message_count()
        
        self.subscriber.stop()

# Usage
def my_action():
    print(f"Action performed at {time.time()}")

metro = ActionMetronome(2, my_action)
metro.run(10)
```

### Pattern 4: Adaptive Interval

Change interval dynamically:

```python
from metronome_datapubsub import MetronomeDataPublisher
import time

# Start with 1 second interval
publishers = []

intervals = [1, 0.5, 0.25, 0.5, 1]  # Speed up then slow down

for interval in intervals:
    config = {'interval': interval, 'message': f'tick_{interval}'}
    
    pub = MetronomeDataPublisher(
        f'metro_{interval}',
        f'metronome://adaptive_{interval}',
        config
    )
    
    publishers.append(pub)
    print(f"Running at {interval}s interval...")
    
    time.sleep(5)
    pub.stop()
    
    print(f"  Generated {pub._publish_count} ticks in 5 seconds")
```

### Pattern 5: Synchronization Point

Use metronome to synchronize operations:

```python
from metronome_datapubsub import MetronomeDataPublisher
import time
import threading

# Sync metronome
config = {'interval': 2, 'message': 'sync_point'}

publisher = MetronomeDataPublisher(
    'sync',
    'metronome://sync',
    config
)

sync_counter = 0

def worker(worker_id):
    """Worker that syncs with metronome"""
    global sync_counter
    local_counter = 0
    
    while local_counter < 5:
        # Wait for next sync point
        while sync_counter == local_counter:
            time.sleep(0.1)
        
        local_counter = sync_counter
        print(f"Worker {worker_id} at sync point {local_counter}")
        time.sleep(0.5)  # Do work

# Start workers
threads = [
    threading.Thread(target=worker, args=(i,))
    for i in range(3)
]

for t in threads:
    t.start()

# Update sync counter with metronome
for i in range(5):
    time.sleep(2)
    sync_counter += 1

for t in threads:
    t.join()

publisher.stop()
```

---

## Troubleshooting

### Common Issues and Solutions

#### Issue 1: Messages Not Generated

**Symptoms:**
- Publisher created but no messages
- Count remains 0

**Solutions:**

1. **Verify publisher started:**
```python
publisher = MetronomeDataPublisher(...)
time.sleep(2)  # Give it time to generate
print(f"Count: {publisher._publish_count}")
```

2. **Check interval is reasonable:**
```python
# Too long?
config = {'interval': 1000}  # 16+ minutes!

# Better
config = {'interval': 1}
```

3. **Ensure not stopped immediately:**
```python
# Wrong
publisher = MetronomeDataPublisher(...)
publisher.stop()  # Stopped immediately!

# Correct
publisher = MetronomeDataPublisher(...)
time.sleep(10)  # Let it run
publisher.stop()
```

#### Issue 2: Too Many Messages

**Symptoms:**
- System overloaded
- Too frequent generation

**Solutions:**

1. **Increase interval:**
```python
# Was too fast
config = {'interval': 0.001}  # 1000/second!

# Better
config = {'interval': 1}  # 1/second
```

2. **Stop when done:**
```python
publisher.stop()  # Always stop when finished
```

#### Issue 3: Subscriber Not Receiving

**Symptoms:**
- Subscriber started but no messages

**Solutions:**

1. **Verify timing:**
```python
subscriber = MetronomeDataSubscriber(
    'sub',
    'metronome://test',
    {'interval': 5}  # Every 5 seconds
)

subscriber.start()
time.sleep(1)  # Not enough time!
# Should wait at least 5 seconds
```

2. **Check subscriber is started:**
```python
subscriber.start()  # Must call start()
```

#### Issue 4: Timestamp Format Issues

**Issue:**
Need different timestamp format

**Solution:**
Modify the message in your processing:
```python
import time
from datetime import datetime

# Metronome gives: '20251022103045'
# Convert to ISO format
timestamp = '20251022103045'
dt = datetime.strptime(timestamp, '%Y%m%d%H%M%S')
iso_format = dt.isoformat()
```

### Debugging Tips

**Monitor generation rate:**
```python
publisher = MetronomeDataPublisher(...)

start_count = publisher._publish_count
time.sleep(10)
end_count = publisher._publish_count

rate = (end_count - start_count) / 10
print(f"Actual rate: {rate} messages/second")

publisher.stop()
```

**Check last publish time:**
```python
print(f"Last publish: {publisher._last_publish}")
```

**Verify thread is running:**
```python
import threading

print(f"Active threads: {threading.active_count()}")
```

---

## Best Practices

### 1. Choose Appropriate Intervals

```python
# Testing: Fast
config = {'interval': 0.1}  # 10/second

# Heartbeat: Moderate
config = {'interval': 30}  # Every 30 seconds

# Scheduled: Slow
config = {'interval': 300}  # Every 5 minutes
```

### 2. Always Stop Metronomes

```python
try:
    publisher = MetronomeDataPublisher(...)
    time.sleep(60)
finally:
    publisher.stop()  # Always cleanup
```

### 3. Use Meaningful Messages

```python
# Good
config = {'message': 'heartbeat'}
config = {'message': 'schedule_trigger'}
config = {'message': 'health_check'}

# Avoid
config = {'message': 'tick'}  # Too generic
```

### 4. Monitor Performance

```python
publisher = MetronomeDataPublisher(...)

# Periodically check
time.sleep(10)
print(f"Messages: {publisher._publish_count}")
print(f"Rate: {publisher._publish_count / 10}/s")
```

### 5. Use for Intended Purpose

**Good Uses:**
- Testing and development
- Heartbeats and health checks
- Scheduled tasks
- Rate limiting
- Demo/presentations

**Avoid:**
- Production data pipelines (use real publishers)
- High-precision timing (system clocks vary)
- Critical operations (not designed for reliability)

---

## API Reference

### MetronomeDataPublisher

```python
MetronomeDataPublisher(name, destination, config)
```

**Parameters:**
- `name` (str): Publisher name
- `destination` (str): Format 'metronome://identifier'
- `config` (dict): Configuration

**Config Options:**
- `interval` (float): Seconds between messages (default: 1)
- `message` (str): Message content (default: 'tick')

**Methods:**
- `stop()`: Stop the metronome
- `is_running()`: Check if running

**Attributes:**
- `_publish_count`: Number of messages generated
- `_last_publish`: ISO timestamp of last message

**Generated Message:**
```python
{
    'message': '<configured_message>',
    'current_timestamp': 'YYYYMMDDHHmmss'
}
```

### MetronomeDataSubscriber

```python
MetronomeDataSubscriber(name, source, config)
```

**Parameters:**
- `name` (str): Subscriber name
- `source` (str): Format 'metronome://identifier'
- `config` (dict): Configuration

**Config Options:**
- `interval` (float): Seconds between messages (default: 1)
- `message` (str): Message content (default: 'tick')

**Methods:**
- `start()`: Start subscriber
- `stop()`: Stop subscriber
- `get_message_count()`: Get message count

---

## Performance Characteristics

### Timing Accuracy

- **Precision**: ±10-100ms (depends on system load)
- **Not suitable for**: High-precision timing
- **Good for**: Regular polling, heartbeats

### Resource Usage

- **Publisher**: One background thread per instance
- **Subscriber**: No background threads
- **Memory**: Minimal (<1MB per instance)
- **CPU**: Negligible when idle

### Scalability

- **Instances**: Can run dozens simultaneously
- **Frequency**: Up to ~100/second reliably
- **Duration**: Can run indefinitely

---

## Additional Resources

- Python Threading: https://docs.python.org/3/library/threading.html
- Time Module: https://docs.python.org/3/library/time.html
- Scheduling: https://schedule.readthedocs.io/

---

**Version:** 1.0  
**Last Updated:** October 22, 2025  
**Author:** DAG Server Development Team
