# DishtaYantra Compute Server - Quick Start Guide

## Version 1.1.1

## © 2025-2030 Ashutosh Sinha

---

## What's New

**v1.1.1**: Admin System Monitoring, Log Viewer, Admin Dropdown Menu

**v1.1.0**: Multi-language calculators (Java, C++, Rust, REST), Free-threading support

---


## Prerequisites

- Python 3.8 or higher (3.13+ recommended for free-threading)
- pip (Python package manager)
- (Optional) Docker and Docker Compose for containerized deployment
- (Optional) Java 8+ for Java calculators
- (Optional) Rust toolchain for Rust calculators

## Quick Installation

### Option 1: Local Installation

```bash
# 1. Clone or download the project
cd dagcomputeserver

# 2. Run setup script
chmod +x setup.sh
./setup.sh

# 3. Activate virtual environment
source venv/bin/activate

# 4. Start the server
python run_server.py
```

### Option 2: Docker Installation

```bash
# 1. Build and start all services
docker-compose up -d

# 2. View logs
docker-compose logs -f dagserver

# 3. Stop services
docker-compose down
```

## First Steps

### 1. Access Web UI

Open your browser and navigate to:
```
http://localhost:5000
```

### 2. Login

Use default credentials:
- **Username**: `admin`
- **Password**: `admin123`

### 3. View Dashboard

You should see the dashboard with pre-loaded sample DAGs:
- `sample_data_pipeline`
- `kafka_data_pipeline` (if Kafka is available)
- `heartbeat_monitor`

## Running Your First Pipeline

### Test In-Memory Pipeline

```bash
# Activate virtual environment (if not already activated)
source venv/bin/activate

# Run test script
python test_inmemory_pipeline.py
```

This will:
1. Create a simple in-memory pipeline
2. Publish test messages
3. Process them through calculators
4. Output results
5. Verify calculations

Expected output:
```
✓ Test 1: Expected sum=30, Got sum=30
✓ Test 2: Expected sum=40, Got sum=40
✓ Test 3: Expected sum=70, Got sum=70
```

## Creating Your First DAG

### Step 1: Create Configuration File

Create `config/dags/my_first_dag.json`:

```json
{
  "name": "my_first_dag",
  "start_time": null,
  "end_time": null,
  "subscribers": [
    {
      "name": "my_input",
      "config": {
        "source": "mem://queue/my_input_queue",
        "max_depth": 1000
      }
    }
  ],
  "publishers": [
    {
      "name": "my_output",
      "config": {
        "destination": "mem://queue/my_output_queue"
      }
    }
  ],
  "calculators": [
    {
      "name": "my_calculator",
      "type": "ApplyDefaultsCalculator",
      "config": {
        "defaults": {
          "processed": true,
          "timestamp": null
        }
      }
    }
  ],
  "transformers": [],
  "nodes": [
    {
      "name": "input_node",
      "type": "SubscriptionNode",
      "config": {},
      "subscriber": "my_input"
    },
    {
      "name": "processing_node",
      "type": "CalculationNode",
      "config": {},
      "calculator": "my_calculator"
    },
    {
      "name": "output_node",
      "type": "PublicationNode",
      "config": {},
      "publishers": ["my_output"]
    }
  ],
  "edges": [
    {
      "from_node": "input_node",
      "to_node": "processing_node"
    },
    {
      "from_node": "processing_node",
      "to_node": "output_node"
    }
  ]
}
```

### Step 2: Upload via Web UI

1. Go to Dashboard
2. Click "Create DAG" button (admin only)
3. Select your JSON file
4. Click "Create"

### Step 3: Start the DAG

1. Find your DAG in the table
2. Click "Start" button
3. DAG status should change to "Running"

### Step 4: Test Your DAG

From the DAG Details page:
1. Find your subscriber in the Subscribers table
2. Click "Publish" button (admin only)
3. Enter test JSON:
```json
{
  "id": 1,
  "name": "Test Message",
  "value": 100
}
```
4. Click "Submit"

### Step 5: Monitor Execution

1. Click "View State" to see:
   - Input/Output of each node
   - Node processing status
   - Any errors

## Common Patterns

### Pattern 1: Simple Transformation Pipeline

```
Subscriber → Transformer → Publisher
```

### Pattern 2: Multi-step Processing

```
Subscriber → Calculator1 → Calculator2 → Publisher
```

### Pattern 3: Fan-out

```
Subscriber → Calculator → Publisher1
                       → Publisher2
                       → Publisher3
```

### Pattern 4: Fan-in

```
Subscriber1 →
Subscriber2 → Aggregator → Publisher
Subscriber3 →
```

### Pattern 5: Scheduled Jobs

```
Metronome → Calculator → Publisher
```

## Testing with Different Sources

### Kafka Pipeline

1. Start Kafka: `docker-compose up -d kafka zookeeper`
2. Create topic: 
```bash
docker exec -it dagserver-kafka kafka-topics.sh \
  --create --topic input_topic \
  --bootstrap-server localhost:9092
```
3. Start `kafka_data_pipeline` DAG
4. Publish test message:
```bash
echo '{"price": 10, "quantity": 5}' | \
  docker exec -i dagserver-kafka kafka-console-producer.sh \
  --topic input_topic \
  --bootstrap-server localhost:9092
```

### Redis Pipeline

1. Start Redis: `docker-compose up -d redis`
2. Create DAG with Redis channel subscriber
3. Publish via Redis CLI:
```bash
docker exec -it dagserver-redis redis-cli
PUBLISH my_channel '{"key": "value"}'
```

### File Pipeline

1. Create DAG with file subscriber pointing to `/var/log/input.jsonl`
2. Append messages to file:
```bash
echo '{"id": 1, "data": "test"}' >> /var/log/input.jsonl
```
3. DAG will read and process new lines

## Monitoring and Troubleshooting

### Check Logs

```bash
# Application logs
tail -f logs/dagserver.log

# Docker logs
docker-compose logs -f dagserver
```

### Common Issues

**Issue**: DAG won't start
- **Solution**: Check if server is PRIMARY (not STANDBY)
- Check logs for configuration errors

**Issue**: Messages not flowing
- **Solution**: Verify subscriber source is correct
- Check queue depths in DAG Details
- Ensure external services (Kafka, Redis) are running

**Issue**: Permission denied
- **Solution**: Login as admin user
- Non-admin users can only view, not modify

### Health Checks

1. **Server Status**: Dashboard shows PRIMARY/STANDBY
2. **DAG Status**: Green = Running, Yellow = Suspended, Gray = Stopped
3. **Node Status**: Green = Clean, Yellow = Dirty
4. **Queue Depths**: Monitor in DAG Details page

## Performance Tuning

### Optimize Queue Sizes

```json
{
  "config": {
    "max_depth": 10000  // Increase for high throughput
  }
}
```

### Enable Batch Publishing

```json
{
  "config": {
    "publish_interval": 5,  // Publish every 5 seconds
    "batch_size": 100       // Or when 100 messages accumulated
  }
}
```

### Adjust Time Windows

```json
{
  "start_time": "0900",  // Start at 9 AM
  "end_time": "1700"      // Stop at 5 PM
}
```

## Next Steps

1. **Read Full Documentation**: See `README.md`
2. **Explore Examples**: Check `config/dags/` directory
3. **Create Custom Calculators**: Extend with your business logic
4. **Setup High Availability**: Configure multiple instances with Zookeeper
5. **Monitor Production**: Setup logging and alerting

## Getting Help

- Check application logs in `logs/` directory
- Review DAG State page for node errors
- Verify configuration JSON syntax
- Ensure external services are accessible

## Security Reminder

⚠️ **Important for Production**:
- Change default passwords in `config/users.json`
- Use password hashing (bcrypt, argon2)
- Enable HTTPS/SSL
- Configure firewall rules
- Use proper authentication (OAuth2, LDAP)
- Setup audit logging

## Useful Commands

```bash
# Start server
python run_server.py

# Test in-memory pipeline
python test_inmemory_pipeline.py

# View logs
tail -f logs/dagserver.log

# Check Python version
python --version

# List installed packages
pip list

# Docker commands
docker-compose up -d        # Start all services
docker-compose down         # Stop all services
docker-compose ps           # Check status
docker-compose logs -f      # Follow logs
```

## New in v1.1.x

### Multi-Language Calculators (v1.1.0)

Use high-performance calculators written in different languages:

```json
{
  "calculators": [
    {
      "name": "java_calc",
      "type": "com.example.Calculator",
      "config": { "calculator": "java" }
    },
    {
      "name": "cpp_calc", 
      "type": "FastCalculator",
      "config": { "calculator": "cpp" }
    },
    {
      "name": "rust_calc",
      "type": "SafeCalculator", 
      "config": { "calculator": "rust" }
    },
    {
      "name": "rest_calc",
      "config": {
        "calculator": "rest",
        "endpoint": "https://api.example.com/calculate",
        "auth_type": "api_key",
        "api_key": "${API_KEY}"
      }
    }
  ]
}
```

See the [Multi-Language Calculator Integration Guide](Multi-Language%20Calculator%20Integration%20Guide.md) for details.

### Admin System Monitoring (v1.1.1)

Admin users can access system monitoring via **Admin → System Monitoring**:

- Real-time CPU, memory, disk metrics
- DAG server statistics
- Service health checks
- Calculator integration status
- Auto-refresh every 5 seconds

### System Logs Viewer (v1.1.1)

View and search application logs via **Admin → System Logs**:

- Filter by log level (Info, Warning, Error)
- Search log content
- Download logs

See the [Admin and System Monitoring Guide](Admin%20and%20System%20Monitoring%20Guide.md) for details.

### Free-Threading (Python 3.13+)

For true parallel execution without the GIL:

```bash
# Install Python 3.13+ with free-threading
# DishtaYantra automatically detects and uses it
python run_server.py
```

---

## Resources

- Main Documentation: `README.md`
- Architecture Document: `docs/ARCHITECTURE.md`
- Sample Configurations: `config/dags/`
- Test Scripts: `test_inmemory_pipeline.py`
- Docker Setup: `docker-compose.yml`

### User Guides
- [Multi-Language Calculator Integration Guide](Multi-Language%20Calculator%20Integration%20Guide.md)
- [Admin and System Monitoring Guide](Admin%20and%20System%20Monitoring%20Guide.md)
- [User Management Architecture](User%20Management%20Architecture.md)

### Help Center (Web UI)
- Getting Started
- DAG Configuration
- Calculators & Transformers
- Pub/Sub Setup
- API Reference
- Glossary

---

**Congratulations!** You're now ready to build data pipelines with DishtaYantra Compute Server! 🚀


## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.
