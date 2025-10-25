# DishtaYantra Compute Server

## © 2025-2030 Ashutosh Sinha

A high-performance, multi-threaded, and thread-safe DAG (Directed Acyclic Graph) compute server with support for multiple message brokers and data sources.

## Features

- **Multi-threaded DAG Execution**: Efficient parallel processing with topologically sorted node execution
- **Multiple Message Brokers**: Support for Kafka, ActiveMQ, Redis, In-Memory queues/topics
- **Various Data Sources**: File, SQL databases, Aerospike, Redis, custom implementations
- **Web UI**: Flask-based dashboard for monitoring and management
- **High Availability**: Zookeeper-based leader election for primary/failover setup
- **Time-windowed Execution**: Configure DAGs to run only during specific time windows
- **Dynamic Cloning**: Clone DAGs with different configurations at runtime

## Project Structure

```
dishtayantra/
├── core/
│   ├── core_utils.py
│   ├── pubsub/
│   │   ├── datapubsub.py          # Abstract base classes
│   │   ├── pubsubfactory.py       # Factory methods
│   │   ├── inmemorypubsub.py      # Singleton in-memory pub/sub
│   │   ├── inmemory_datapubsub.py # In-memory publishers/subscribers
│   │   ├── file_datapubsub.py     # File-based publishers/subscribers
│   │   ├── kafka_datapubsub.py    # Kafka publishers/subscribers
│   │   ├── activemq_datapubsub.py # ActiveMQ publishers/subscribers
│   │   ├── sql_datapubsub.py      # SQL publishers/subscribers
│   │   ├── redis_datapubsub.py    # Redis publishers/subscribers
│   │   ├── aerospike_datapubsub.py # Aerospike publisher
│   │   ├── custom_datapubsub.py   # Custom delegate publishers/subscribers
│   │   └── metronome_datapubsub.py # Metronome publishers/subscribers
│   ├── calculator/
│   │   └── core_calculator.py     # Calculator implementations
│   ├── transformer/
│   │   └── core_transformer.py    # Transformer implementations
│   └── dag/
│       ├── graph_elements.py      # Node and Edge classes
│       ├── node_implementations.py # Concrete node types
│       ├── compute_graph.py       # ComputeGraph class
│       └── dag_server.py          # DAGComputeServer class
├── web/
│   ├── app.py                     # Flask application
│   └── templates/
│       ├── base.html
│       ├── login.html
│       ├── dashboard.html
│       ├── dag_details.html
│       └── dag_state.html
├── config/
│   ├── users.json                 # User credentials
│   └── dags/                      # DAG configuration files
│       └── sample_dag.json
├── requirements.txt
└── README.md
```

## Installation

1. **Install Python 3.8+**

2. **Install dependencies**:
```bash
pip install -r requirements.txt
```

3. **Setup Zookeeper** (optional, for HA):
```bash
# Download and start Zookeeper
# https://zookeeper.apache.org/releases.html
```

4. **Create necessary directories**:
```bash
mkdir -p config/dags
mkdir -p logs
```

## Configuration

### Users Configuration

Edit `config/users.json`:
```json
{
  "admin": {
    "password": "admin123",
    "role": "admin"
  },
  "user1": {
    "password": "user123",
    "role": "user"
  }
}
```

### DAG Configuration

Create JSON files in `config/dags/` directory. Example:

```json
{
  "name": "my_pipeline",
  "start_time": "0900",
  "end_time": "1700",
  "subscribers": [...],
  "publishers": [...],
  "calculators": [...],
  "transformers": [...],
  "nodes": [...],
  "edges": [...]
}
```

## Running the Application

### Start the DAG Server and Web UI:

```bash
python web/app.py
```

### Environment Variables:

- `DAG_CONFIG_FOLDER`: Path to DAG configuration folder (default: `./config/dags`)
- `ZOOKEEPER_HOSTS`: Zookeeper connection string (default: `localhost:2181`)
- `USERS_FILE`: Path to users configuration file (default: `./config/users.json`)
- `SECRET_KEY`: Flask secret key for sessions

Example:
```bash
export DAG_CONFIG_FOLDER=/path/to/dags
export ZOOKEEPER_HOSTS=zk1:2181,zk2:2181,zk3:2181
python web/app.py
```

## Usage

### Web Interface

1. **Login**: Navigate to `http://localhost:5000` and login with credentials from `users.json`

2. **Dashboard**: View all DAGs, their status, and perform operations:
   - Start/Stop DAGs
   - Suspend/Resume DAGs
   - Clone DAGs with new time windows
   - Delete DAGs
   - View Details and State

3. **DAG Details**: View comprehensive information about:
   - Nodes in topological order
   - Subscribers and Publishers
   - Calculators and Transformers
   - Full JSON configuration

4. **DAG State**: Real-time view of:
   - Input/Output state of each node
   - Error logs
   - Node dirty status

### Publishing Messages (Admin Only)

For in-memory, Kafka, Redis channel, and ActiveMQ subscribers, admins can publish test messages directly from the UI:

1. Go to DAG Details page
2. Find the subscriber in the Subscribers table
3. Click "Publish" button
4. Enter JSON message
5. Submit

## Components

### Publishers and Subscribers

- **InMemory**: `mem://queue/name` or `mem://topic/name`
- **File**: `file:///path/to/file`
- **Kafka**: `kafka://topic/topic_name`
- **ActiveMQ**: `activemq://queue/name` or `activemq://topic/name`
- **SQL**: `sql://source_name`
- **Redis**: `redis://keys` or `redischannel://channel_name`
- **Aerospike**: `aerospike://namespace/set`
- **Custom**: `custom://custom_name`
- **Metronome**: `metronome`

### Calculators

- **NullCalculator**: Returns deep copy
- **PassthruCalculator**: Returns input as-is
- **AttributeFilterAwayCalculator**: Removes specified attributes
- **AttributeFilterCalculator**: Keeps only specified attributes
- **ApplyDefaultsCalculator**: Applies default values
- **AdditionCalculator**: Adds numeric attributes
- **MultiplicationCalculator**: Multiplies numeric attributes
- **AttributeNameChangeCalculator**: Renames attributes

### Transformers

- **NullDataTransformer**: Returns deep copy
- **PassthruDataTransformer**: Returns input as-is
- **AttributeFilterAwayDataTransformer**: Removes specified attributes
- **AttributeFilterDataTransformer**: Keeps only specified attributes
- **ApplyDefaultsDataTransformer**: Applies default values

### Node Types

- **SubscriptionNode**: Pulls data from DataSubscriber
- **PublicationNode**: Publishes data to DataPublishers
- **MetronomeNode**: Executes at regular intervals
- **CalculationNode**: Performs calculations on input

## High Availability

The system uses Zookeeper for leader election:

- **Primary Instance**: Actively runs DAGs
- **Standby Instance**: Keeps DAGs suspended, ready for failover

When primary fails, standby automatically becomes primary and resumes all DAGs.

## Naming Conventions

All names in the system must:
- Contain only alphanumeric characters and underscores
- Have at least one alphabetic character
- Examples: `my_dag`, `queue1`, `subscriber_A`

## Logging

All components use Python's logging module. Configure logging level:

```python
import logging
logging.basicConfig(level=logging.INFO)
```

## Error Handling

- Each node maintains up to 10 most recent errors with timestamps
- Errors are visible in the DAG State view
- Failed operations don't crash the entire system
- Automatic reconnection for database and message broker connections

## Performance Considerations

- **Thread-safe**: All components use proper locking mechanisms
- **Backpressure Management**: Internal queues with configurable max depths
- **Efficient Computation**: Only dirty nodes are recomputed
- **Batch Publishing**: Optional batching for high-throughput scenarios

## Advanced Features

### Periodic Publishing

Publishers can accumulate messages and publish periodically:

```json
{
  "name": "batch_publisher",
  "config": {
    "destination": "kafka://topic/output",
    "publish_interval": 5,
    "batch_size": 100
  }
}
```

### Time-windowed DAGs

DAGs can be configured to run only during specific hours:

```json
{
  "name": "business_hours_dag",
  "start_time": "0900",
  "end_time": "1700"
}
```

### Custom Implementations

Extend the system with custom calculators, transformers, and pub/sub:

```python
# my_custom_calculator.py
from core.calculator.core_calculator import DataCalculator

class MyCustomCalculator(DataCalculator):
    def calculate(self, data):
        # Your custom logic here
        return processed_data
```

Reference in configuration:
```json
{
  "name": "custom_calc",
  "type": "my_module.my_custom_calculator.MyCustomCalculator",
  "config": {}
}
```

## Example Use Cases

### 1. Real-time Data Pipeline

```
Kafka Subscriber → Filter Transformer → Calculation → Redis Publisher
```

### 2. Batch Processing

```
File Subscriber → Apply Defaults → Aggregation → SQL Publisher
```

### 3. Multi-source Aggregation

```
                  → Node A →
Subscriber 1 →              → Aggregator → Publisher
Subscriber 2 →  → Node B →
```

### 4. Scheduled Data Generation

```
Metronome → Calculation → File Publisher
```

## Troubleshooting

### DAG won't start
- Check if instance is PRIMARY (not STANDBY)
- Verify all configuration files are valid JSON
- Check logs for error messages

### Messages not flowing
- Verify subscribers are connected to correct sources
- Check queue depths in DAG Details
- Ensure nodes are marked as dirty (check DAG State)

### High memory usage
- Reduce max_depth for subscribers
- Check for cycles in DAG (will be rejected at build time)
- Monitor node error logs

### Connection failures
- Verify Kafka/ActiveMQ/Redis/SQL connection details
- Check network connectivity
- Review firewall rules

## API Reference

### DAGComputeServer Methods

```python
# Add a new DAG
dag_name = server.add_dag(config_dict, filename)

# Start a DAG
server.start(dag_name)

# Stop a DAG
server.stop(dag_name)

# Suspend a DAG
server.suspend(dag_name)

# Resume a DAG
server.resume(dag_name)

# Delete a DAG
server.delete(dag_name)

# Clone a DAG
new_dag_name = server.clone_dag(dag_name, start_time, end_time)

# Get DAG details
details = server.details(dag_name)

# List all DAGs
dags = server.list_dags()

# Get server status
status = server.get_server_status()
```

### ComputeGraph Methods

```python
# Build the DAG
graph.build_dag()

# Get topologically sorted nodes
sorted_nodes = graph.topological_sort()

# Start execution
graph.start()

# Suspend execution
graph.suspend()

# Resume execution
graph.resume()

# Stop execution
graph.stop()

# Clone with new time window
cloned_graph = graph.clone(start_time='0800', end_time='1800')

# Get JSON configuration
json_str = graph.show_json()

# Get details
details = graph.details()
```

## Security Considerations

⚠️ **Important**: This implementation uses cleartext passwords for demonstration purposes. For production use:

1. **Hash passwords**: Use bcrypt, argon2, or similar
2. **Use HTTPS**: Enable SSL/TLS for web interface
3. **Implement proper authentication**: Consider OAuth2, LDAP, or similar
4. **Secure Zookeeper**: Use ACLs and authentication
5. **Network security**: Use firewalls and network segmentation
6. **Audit logging**: Log all administrative actions
7. **Input validation**: Already implemented for names, extend as needed

## Testing

### Unit Tests

```python
# test_calculator.py
import unittest
from core.calculator.core_calculator import AdditionCalculator

class TestCalculators(unittest.TestCase):
    def test_addition_calculator(self):
        calc = AdditionCalculator('test', {
            'arguments': ['a', 'b'],
            'output_attribute': 'result'
        })
        result = calc.calculate({'a': 5, 'b': 3})
        self.assertEqual(result['result'], 8)
```

### Integration Tests

Test with actual message brokers:

```bash
# Start Kafka locally
docker run -d --name kafka -p 9092:9092 apache/kafka

# Run tests
python -m pytest tests/
```

## Contributing

1. Follow PEP 8 style guidelines
2. Add logging to all major operations
3. Write unit tests for new components
4. Update documentation
5. Ensure thread safety

## License

[Your License Here]

## Support

For issues and questions:
- Check logs in the application
- Review this documentation
- Check DAG configuration syntax
- Verify external service connectivity

## Changelog

### Version 1.0.0
- Initial release
- Support for Kafka, ActiveMQ, Redis, SQL, File, Aerospike
- Flask web UI
- Zookeeper-based HA
- Time-windowed execution
- DAG cloning
- Message publishing from UI

For issues, questions, or contributions, please contact:
- Email: ajsinha@gmail.com
- GitHub: https://github.com/ajsinha/dishtayantra

## License

© 2025-2030 Ashutosh Sinha. All rights reserved.

This software is proprietary and confidential. Unauthorized copying, distribution, or use is strictly prohibited.