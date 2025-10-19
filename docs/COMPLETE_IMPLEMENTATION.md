# DAG Compute Server - Complete Implementation

This document provides the complete implementation structure for the DAG Compute Server system.

## Project Structure

```
dagcomputeserver/
├── core/
│   ├── __init__.py
│   ├── core_utils.py
│   ├── pubsub/
│   │   ├── __init__.py
│   │   ├── datapubsub.py
│   │   ├── pubsubfactory.py
│   │   ├── inmemorypubsub.py
│   │   ├── inmemory_datapubsub.py
│   │   ├── file_datapubsub.py
│   │   ├── kafka_datapubsub.py
│   │   ├── activemq_datapubsub.py
│   │   ├── sql_datapubsub.py
│   │   ├── aerospike_datapubsub.py
│   │   ├── redis_datapubsub.py
│   │   ├── custom_datapubsub.py
│   │   └── metronome_datapubsub.py
│   ├── calculator/
│   │   ├── __init__.py
│   │   └── core_calculator.py
│   ├── transformer/
│   │   ├── __init__.py
│   │   └── core_transformer.py
│   └── dag/
│       ├── __init__.py
│       ├── graph_elements.py
│       ├── compute_graph.py
│       └── dag_server.py
├── web/
│   ├── __init__.py
│   ├── app.py
│   ├── templates/
│   │   ├── base.html
│   │   ├── login.html
│   │   ├── dashboard.html
│   │   ├── dag_details.html
│   │   └── dag_state.html
│   └── static/
│       ├── css/
│       └── js/
├── config/
│   ├── users.json
│   └── dags/
├── requirements.txt
└── README.md
```

## Requirements.txt

```
flask==3.0.0
kafka-python==2.0.2
stomp.py==8.1.0
pymysql==1.1.0
psycopg2-binary==2.9.9
aerospike==13.0.0
redis==5.0.1
kazoo==2.9.0
```

## Key Implementation Files

### 1. core/pubsub/inmemory_datapubsub.py

```python
import json
import logging
from datetime import datetime
from core.pubsub.datapubsub import DataPublisher, DataSubscriber
from core.pubsub.inmemorypubsub import InMemoryPubSub

logger = logging.getLogger(__name__)

class InMemoryDataPublisher(DataPublisher):
    """Publisher for in-memory queues and topics"""
    
    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)
        self.pubsub = InMemoryPubSub()
        self.is_queue = 'queue' in destination
        self.target_name = destination.split('/')[-1]
        
        if self.is_queue:
            self.pubsub.create_queue(self.target_name, config.get('max_size', 100000))
    
    def _do_publish(self, data):
        """Publish to in-memory queue or topic"""
        json_data = json.dumps(data)
        
        if self.is_queue:
            self.pubsub.publish_to_queue(self.target_name, json_data, block=False)
        else:
            self.pubsub.publish_to_topic(self.target_name, json_data)
        
        with self._lock:
            self._last_publish = datetime.now().isoformat()
            self._publish_count += 1
        
        logger.debug(f"Published to {self.destination}")


class InMemoryDataSubscriber(DataSubscriber):
    """Subscriber for in-memory queues and topics"""
    
    def __init__(self, name, source, config):
        super().__init__(name, source, config)
        self.pubsub = InMemoryPubSub()
        self.is_queue = 'queue' in source
        self.target_name = source.split('/')[-1]
        
        if self.is_queue:
            self.pubsub.create_queue(self.target_name, config.get('max_size', 100000))
        else:
            self._subscription_queue = self.pubsub.subscribe_to_topic(self.target_name, self.max_depth)
    
    def _do_subscribe(self):
        """Subscribe from in-memory queue or topic"""
        if self.is_queue:
            json_data = self.pubsub.consume_from_queue(self.target_name, block=False)
        else:
            try:
                json_data = self._subscription_queue.get(block=False)
            except:
                json_data = None
        
        if json_data:
            return json.loads(json_data)
        return None
```

### 2. core/pubsub/kafka_datapubsub.py

```python
import json
import logging
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from core.pubsub.datapubsub import DataPublisher, DataSubscriber

logger = logging.getLogger(__name__)

class KafkaDataPublisher(DataPublisher):
    """Publisher for Kafka topics"""
    
    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)
        self.topic = destination.split('/')[-1]
        self.bootstrap_servers = config.get('bootstrap_servers', ['localhost:9092'])
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            **config.get('producer_config', {})
        )
        logger.info(f"Kafka producer created for topic {self.topic}")
    
    def _do_publish(self, data):
        """Publish to Kafka topic"""
        self.producer.send(self.topic, value=data)
        self.producer.flush()
        
        with self._lock:
            self._last_publish = datetime.now().isoformat()
            self._publish_count += 1
        
        logger.debug(f"Published to Kafka topic {self.topic}")
    
    def stop(self):
        """Stop the publisher"""
        super().stop()
        if self.producer:
            self.producer.close()


class KafkaDataSubscriber(DataSubscriber):
    """Subscriber for Kafka topics"""
    
    def __init__(self, name, source, config):
        super().__init__(name, source, config)
        self.topic = source.split('/')[-1]
        self.bootstrap_servers = config.get('bootstrap_servers', ['localhost:9092'])
        self.group_id = config.get('group_id', f'{name}_group')
        
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=100,
            **config.get('consumer_config', {})
        )
        logger.info(f"Kafka consumer created for topic {self.topic}")
    
    def _do_subscribe(self):
        """Subscribe from Kafka topic"""
        for message in self.consumer:
            return message.value
        return None
    
    def stop(self):
        """Stop the subscriber"""
        super().stop()
        if self.consumer:
            self.consumer.close()
```

### 3. core/calculator/core_calculator.py

```python
import copy
import logging
from abc import ABC, abstractmethod
from datetime import datetime

logger = logging.getLogger(__name__)

class DataCalculator(ABC):
    """Abstract base class for data calculators"""
    
    def __init__(self, name, config):
        self.name = name
        self.config = config
        self._calculation_count = 0
        self._last_calculation = None
    
    @abstractmethod
    def calculate(self, data):
        """Calculate and return result"""
        pass
    
    def details(self):
        """Return details in JSON format"""
        return {
            'name': self.name,
            'type': self.__class__.__name__,
            'calculation_count': self._calculation_count,
            'last_calculation': self._last_calculation
        }


class NullCalculator(DataCalculator):
    """Returns deep copy of supplied dictionary"""
    
    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()
        return copy.deepcopy(data)


class PassthruCalculator(DataCalculator):
    """Returns supplied dictionary as-is"""
    
    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()
        return data


class AttributeFilterAwayCalculator(DataCalculator):
    """Filters out specified attributes"""
    
    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()
        
        filter_attrs = self.config.get('filter_attributes', [])
        result = copy.deepcopy(data)
        
        for attr in filter_attrs:
            result.pop(attr, None)
        
        return result


class AttributeFilterCalculator(DataCalculator):
    """Keeps only specified attributes"""
    
    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()
        
        keep_attrs = self.config.get('keep_attributes', [])
        result = {k: copy.deepcopy(v) for k, v in data.items() if k in keep_attrs}
        
        return result


class ApplyDefaultsCalculator(DataCalculator):
    """Applies default values for missing or None attributes"""
    
    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()
        
        defaults = self.config.get('defaults', {})
        result = copy.deepcopy(data)
        
        for key, default_value in defaults.items():
            if key not in result or result[key] is None:
                result[key] = default_value
        
        return result


class AdditionCalculator(DataCalculator):
    """Adds values of specified attributes"""
    
    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()
        
        arguments = self.config.get('arguments', [])
        output_attr = self.config.get('output_attribute', 'result')
        
        total = 0
        for arg in arguments:
            if arg in data:
                try:
                    total += float(data[arg])
                except (ValueError, TypeError):
                    logger.warning(f"Cannot convert {arg} value to number")
        
        return {output_attr: total}


class MultiplicationCalculator(DataCalculator):
    """Multiplies values of specified attributes"""
    
    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()
        
        arguments = self.config.get('arguments', [])
        output_attr = self.config.get('output_attribute', 'result')
        
        product = 1
        for arg in arguments:
            if arg in data:
                try:
                    product *= float(data[arg])
                except (ValueError, TypeError):
                    logger.warning(f"Cannot convert {arg} value to number")
        
        return {output_attr: product}


class AttributeNameChangeCalculator(DataCalculator):
    """Changes attribute names based on mapping"""
    
    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()
        
        name_mapping = self.config.get('name_mapping', {})
        result = copy.deepcopy(data)
        
        for old_name, new_name in name_mapping.items():
            if old_name in result:
                result[new_name] = result.pop(old_name)
        
        return result
```

### 4. core/dag/graph_elements.py

```python
import copy
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from collections import deque

logger = logging.getLogger(__name__)

class Node(ABC):
    """Abstract base class for nodes in compute graph"""
    
    def __init__(self, name, config):
        self.name = name
        self.config = config
        self._input = {}
        self._output = {}
        self._isdirty = True
        self._calculator = None
        self._input_transformers = []
        self._output_transformers = []
        self._last_compute = None
        self._compute_count = 0
        self._errors = deque(maxlen=10)
        self._outgoing_edges = []
        self._incoming_edges = []
    
    def input(self):
        return copy.deepcopy(self._input)
    
    def output(self):
        return copy.deepcopy(self._output)
    
    def isdirty(self):
        return self._isdirty
    
    def set_dirty(self):
        self._isdirty = True
    
    def set_calculator(self, calculator):
        self._calculator = calculator
    
    def add_input_transformer(self, transformer):
        self._input_transformers.append(transformer)
    
    def add_output_transformer(self, transformer):
        self._output_transformers.append(transformer)
    
    def add_outgoing_edge(self, edge):
        self._outgoing_edges.append(edge)
    
    def add_incoming_edge(self, edge):
        self._incoming_edges.append(edge)
    
    def pre_compute(self):
        """Pre-compute hook - can be overridden by subclasses"""
        pass
    
    def compute(self):
        """Compute node output based on inputs"""
        if not self._isdirty:
            return
        
        try:
            # Gather inputs from incoming edges
            merged_input = {}
            for edge in self._incoming_edges:
                edge_data = edge.get_data()
                if edge_data:
                    merged_input.update(edge_data)
            
            # Apply input transformers
            transformed_input = merged_input
            for transformer in self._input_transformers:
                transformed_input = transformer.transform(transformed_input)
            
            # Check if input has changed
            if transformed_input == self._input:
                self._isdirty = False
                return
            
            self._input = copy.deepcopy(transformed_input)
            
            # Calculate if calculator is available
            if self._calculator:
                calculated_output = self._calculator.calculate(self._input)
            else:
                calculated_output = self._input
            
            # Apply output transformers
            transformed_output = calculated_output
            for transformer in self._output_transformers:
                transformed_output = transformer.transform(transformed_output)
            
            # Check if output has changed
            if transformed_output != self._output:
                self._output = copy.deepcopy(transformed_output)
                
                # Mark children as dirty
                for edge in self._outgoing_edges:
                    edge.to_node.set_dirty()
            
            self._isdirty = False
            self._last_compute = datetime.now().isoformat()
            self._compute_count += 1
            
        except Exception as e:
            error_info = {
                'time': datetime.now().isoformat(),
                'error': str(e)
            }
            self._errors.append(error_info)
            logger.error(f"Error in node {self.name}: {str(e)}")
    
    def post_compute(self):
        """Post-compute hook - can be overridden by subclasses"""
        pass
    
    def details(self):
        """Return node details in JSON format"""
        return {
            'name': self.name,
            'type': self.__class__.__name__,
            'isdirty': self._isdirty,
            'last_compute': self._last_compute,
            'compute_count': self._compute_count,
            'input': self._input,
            'output': self._output,
            'errors': list(self._errors),
            'incoming_edges': [e.name for e in self._incoming_edges],
            'outgoing_edges': [e.name for e in self._outgoing_edges]
        }


class Edge:
    """Edge connecting two nodes in compute graph"""
    
    def __init__(self, from_node, to_node, data_transformer=None):
        self.from_node = from_node
        self.to_node = to_node
        self.data_transformer = data_transformer
        self.name = f"{from_node.name}_to_{to_node.name}"
        
        from_node.add_outgoing_edge(self)
        to_node.add_incoming_edge(self)
    
    def get_data(self):
        """Get data from source node through transformer"""
        output = self.from_node.output()
        
        if self.data_transformer:
            return self.data_transformer.transform(output)
        
        return output
    
    def details(self):
        """Return edge details in JSON format"""
        return {
            'name': self.name,
            'from_node': self.from_node.name,
            'to_node': self.to_node.name,
            'transformer': self.data_transformer.name if self.data_transformer else None
        }
```

## Continued in Next Parts...

Due to the extensive size of this project (20+ files with full implementation), I recommend:

1. **Download Structure**: Use the above as a template
2. **Key Patterns**: Follow the established patterns for remaining implementations
3. **Testing**: Create unit tests for each module
4. **Configuration**: Use JSON files for DAG configurations

Would you like me to:
1. Continue with specific modules (ActiveMQ, SQL, Flask app)?
2. Provide deployment and testing guidelines?
3. Create sample DAG configurations?
4. Focus on the Flask web UI implementation?


## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.