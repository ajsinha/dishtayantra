"""DAG Designer routes module"""
import json
import logging
from flask import render_template, session, flash, redirect, url_for, request, jsonify

logger = logging.getLogger(__name__)


class DAGDesignerRoutes:
    """Handles DAG Designer-related routes"""

    def __init__(self, app, dag_server, user_registry, login_required):
        self.app = app
        self.dag_server = dag_server
        self.user_registry = user_registry
        self.login_required = login_required
        self._register_routes()

    def _register_routes(self):
        """Register all DAG Designer routes"""
        self.app.add_url_rule('/dag-designer', 'dag_designer',
                             self.login_required(self.dag_designer))
        self.app.add_url_rule('/dag-designer/load/<dag_name>', 'dag_designer_load',
                             self.login_required(self.dag_designer_load))
        self.app.add_url_rule('/api/dag-designer/validate', 'dag_designer_validate',
                             self.login_required(self.validate_dag), methods=['POST'])
        self.app.add_url_rule('/api/dag-designer/components', 'dag_designer_components',
                             self.login_required(self.get_available_components))

    def dag_designer(self):
        """Main DAG Designer view"""
        try:
            # Get list of existing DAGs for the "Load Existing" dropdown
            dags = self.dag_server.list_dags()
            dag_names = [dag['name'] for dag in dags]

            return render_template('dag_designer.html',
                                   existing_dags=dag_names,
                                   is_admin=self.user_registry.has_role(session.get('username'), 'admin'))
        except Exception as e:
            logger.error(f"Error loading DAG Designer: {str(e)}")
            flash(f'Error loading DAG Designer: {str(e)}', 'error')
            return render_template('dag_designer.html', existing_dags=[], is_admin=False)

    def dag_designer_load(self, dag_name):
        """Load an existing DAG into the designer"""
        try:
            if dag_name not in self.dag_server.dags:
                flash(f'DAG "{dag_name}" not found', 'error')
                return redirect(url_for('dag_designer'))

            dag = self.dag_server.dags[dag_name]
            dag_config = dag.config

            # Get list of existing DAGs
            dags = self.dag_server.list_dags()
            dag_names = [d['name'] for d in dags]

            return render_template('dag_designer.html',
                                   existing_dags=dag_names,
                                   loaded_dag=json.dumps(dag_config, indent=2),
                                   loaded_dag_name=dag_name,
                                   is_admin=self.user_registry.has_role(session.get('username'), 'admin'))
        except Exception as e:
            logger.error(f"Error loading DAG into designer: {str(e)}")
            flash(f'Error loading DAG: {str(e)}', 'error')
            return redirect(url_for('dag_designer'))

    def validate_dag(self):
        """Validate a DAG configuration (API endpoint)"""
        try:
            dag_config = request.get_json()

            errors = []
            warnings = []

            # Basic validation
            if not dag_config.get('name'):
                errors.append("DAG name is required")

            if not dag_config.get('nodes') or len(dag_config.get('nodes', [])) == 0:
                errors.append("At least one node is required")

            # Validate edges reference existing nodes
            node_names = {n['name'] for n in dag_config.get('nodes', [])}
            transformer_names = {t['name'] for t in dag_config.get('transformers', [])}

            for edge in dag_config.get('edges', []):
                if edge.get('from_node') not in node_names:
                    errors.append(f"Edge references non-existent source node: {edge.get('from_node')}")
                if edge.get('to_node') not in node_names:
                    errors.append(f"Edge references non-existent target node: {edge.get('to_node')}")

                # Validate edge transformer if specified
                if edge.get('data_transformer') and edge.get('data_transformer') not in transformer_names:
                    warnings.append(f"Edge from '{edge.get('from_node')}' to '{edge.get('to_node')}' references transformer '{edge.get('data_transformer')}' which may be a custom type")

            # Cycle detection using DFS
            cycles = self._detect_cycles(dag_config.get('nodes', []), dag_config.get('edges', []))
            for cycle in cycles:
                cycle_path = ' -> '.join(cycle)
                errors.append(f"Cycle detected: {cycle_path}")

            # Validate node references
            subscriber_names = {s['name'] for s in dag_config.get('subscribers', [])}
            publisher_names = {p['name'] for p in dag_config.get('publishers', [])}
            calculator_names = {c['name'] for c in dag_config.get('calculators', [])}
            transformer_names = {t['name'] for t in dag_config.get('transformers', [])}

            for node in dag_config.get('nodes', []):
                # Check subscriber reference
                if node.get('subscriber') and node.get('subscriber') not in subscriber_names:
                    errors.append(f"Node '{node['name']}' references non-existent subscriber: {node.get('subscriber')}")

                # Check publisher references
                for pub in node.get('publishers', []):
                    if pub not in publisher_names:
                        errors.append(f"Node '{node['name']}' references non-existent publisher: {pub}")

                # Check calculator reference
                if node.get('calculator') and node.get('calculator') not in calculator_names:
                    errors.append(f"Node '{node['name']}' references non-existent calculator: {node.get('calculator')}")

                # Check transformer references
                for trans in node.get('input_transformers', []):
                    if trans not in transformer_names:
                        errors.append(f"Node '{node['name']}' references non-existent input transformer: {trans}")
                for trans in node.get('output_transformers', []):
                    if trans not in transformer_names:
                        errors.append(f"Node '{node['name']}' references non-existent output transformer: {trans}")

            # Warnings
            if dag_config.get('start_time') and not dag_config.get('duration'):
                warnings.append("start_time is set but duration is not specified. Default duration (-5 minutes) will be used.")

            if dag_config.get('autoclone'):
                autoclone = dag_config['autoclone']
                if not autoclone.get('ramp_up_time'):
                    errors.append("autoclone.ramp_up_time is required when autoclone is configured")
                if not autoclone.get('ramp_count'):
                    errors.append("autoclone.ramp_count is required when autoclone is configured")

            return jsonify({
                'valid': len(errors) == 0,
                'errors': errors,
                'warnings': warnings
            })

        except Exception as e:
            logger.error(f"Error validating DAG: {str(e)}")
            return jsonify({
                'valid': False,
                'errors': [f"Validation error: {str(e)}"],
                'warnings': []
            }), 500

    def _detect_cycles(self, nodes, edges):
        """
        Detect cycles in the DAG using depth-first search.
        Returns a list of cycles found, where each cycle is a list of node names.
        """
        # Build adjacency list
        graph = {}
        node_names = {n['name'] for n in nodes}

        for name in node_names:
            graph[name] = []

        for edge in edges:
            from_node = edge.get('from_node')
            to_node = edge.get('to_node')
            if from_node in graph and to_node in node_names:
                graph[from_node].append(to_node)

        cycles = []
        visited = set()
        rec_stack = set()
        path = []

        def dfs(node):
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    cycle = dfs(neighbor)
                    if cycle:
                        return cycle
                elif neighbor in rec_stack:
                    # Found a cycle - extract the cycle path
                    cycle_start_idx = path.index(neighbor)
                    cycle_path = path[cycle_start_idx:] + [neighbor]
                    return cycle_path

            path.pop()
            rec_stack.remove(node)
            return None

        # Run DFS from each unvisited node
        for node_name in node_names:
            if node_name not in visited:
                cycle = dfs(node_name)
                if cycle:
                    cycles.append(cycle)
                    # Reset for finding additional cycles
                    visited = set()
                    rec_stack = set()
                    path = []
                    # Mark nodes in found cycle as visited to avoid re-reporting
                    for n in cycle[:-1]:
                        visited.add(n)

        return cycles

    def get_available_components(self):
        """Get available component types (API endpoint)"""
        components = {
            'node_types': [
                {'type': 'SubscriptionNode', 'description': 'Pulls data from a subscriber', 'requires': ['subscriber']},
                {'type': 'PublicationNode', 'description': 'Publishes data to publishers', 'requires': ['publishers']},
                {'type': 'CalculationNode', 'description': 'Performs calculations on data', 'optional': ['calculator']},
                {'type': 'MetronomeNode', 'description': 'Executes at regular intervals', 'config': ['interval']},
                {'type': 'SinkNode', 'description': 'Terminal node that consumes data'},
                {'type': 'PublisherSinkNode', 'description': 'Sink node that publishes to multiple destinations'}
            ],
            'calculator_types': [
                {'type': 'NullCalculator', 'description': 'Returns deep copy of input'},
                {'type': 'PassthruCalculator', 'description': 'Returns input as-is'},
                {'type': 'ApplyDefaultsCalculator', 'description': 'Applies default values', 'config': ['defaults']},
                {'type': 'AdditionCalculator', 'description': 'Adds specified attributes', 'config': ['arguments', 'output_attribute']},
                {'type': 'MultiplicationCalculator', 'description': 'Multiplies specified attributes', 'config': ['arguments', 'output_attribute']},
                {'type': 'AttributeFilterCalculator', 'description': 'Keeps only specified attributes', 'config': ['keep_attributes']},
                {'type': 'AttributeFilterAwayCalculator', 'description': 'Removes specified attributes', 'config': ['filter_attributes']},
                {'type': 'AttributeNameChangeCalculator', 'description': 'Renames attributes', 'config': ['name_mapping']},
                {'type': 'RandomCalculator', 'description': 'Adds random value to data'}
            ],
            'transformer_types': [
                {'type': 'NullDataTransformer', 'description': 'Returns deep copy of input'},
                {'type': 'PassthruDataTransformer', 'description': 'Returns input as-is'},
                {'type': 'ApplyDefaultsDataTransformer', 'description': 'Applies default values', 'config': ['defaults']},
                {'type': 'AttributeFilterDataTransformer', 'description': 'Keeps only specified attributes', 'config': ['keep_attributes']},
                {'type': 'AttributeFilterAwayDataTransformer', 'description': 'Removes specified attributes', 'config': ['filter_attributes']}
            ],
            'subscriber_types': [
                {'type': 'KafkaDataSubscriber', 'module': 'core.pubsub.kafka_datapubsub', 'description': 'Subscribe from Kafka topics', 'config': ['source', 'bootstrap_servers', 'group_id', 'max_depth']},
                {'type': 'RedisChannelDataSubscriber', 'module': 'core.pubsub.redis_datapubsub', 'description': 'Subscribe from Redis pub/sub channels', 'config': ['source', 'host', 'port', 'db']},
                {'type': 'RabbitMQDataSubscriber', 'module': 'core.pubsub.rabbitmq_datapubsub', 'description': 'Subscribe from RabbitMQ queues/topics', 'config': ['source', 'host', 'port', 'username', 'password']},
                {'type': 'ActiveMQDataSubscriber', 'module': 'core.pubsub.activemq_datapubsub', 'description': 'Subscribe from ActiveMQ queues/topics', 'config': ['source', 'host', 'port', 'username', 'password']},
                {'type': 'GRPCDataSubscriber', 'module': 'core.pubsub.grpc_datapubsub', 'description': 'Subscribe from gRPC streams', 'config': ['source', 'use_ssl', 'subscriber_id']},
                {'type': 'FileDataSubscriber', 'module': 'core.pubsub.file_datapubsub', 'description': 'Subscribe from files (JSONL)', 'config': ['source', 'read_interval']},
                {'type': 'InMemoryDataSubscriber', 'module': 'core.pubsub.inmemory_datapubsub', 'description': 'Subscribe from in-memory queues/topics', 'config': ['source', 'max_size']},
                {'type': 'InMemoryRedisDataSubscriber', 'module': 'core.pubsub.inmemoryredis_datapubsub', 'description': 'Poll keys from InMemory Redis', 'config': ['source', 'key_pattern', 'poll_interval', 'delete_on_read']},
                {'type': 'InMemoryRedisChannelDataSubscriber', 'module': 'core.pubsub.inmemoryredis_datapubsub', 'description': 'Subscribe from InMemory Redis channels', 'config': ['source']},
                {'type': 'AshRedisChannelDataSubscriber', 'module': 'core.pubsub.ashredis_datapubsub', 'description': 'Subscribe from AshRedis channels', 'config': ['source', 'host', 'port', 'region']},
                {'type': 'MetronomeDataSubscriber', 'module': 'core.pubsub.metronome_datapubsub', 'description': 'Generate messages at regular intervals', 'config': ['source', 'interval', 'message']},
                {'type': 'FaninDataSubscriber', 'module': 'core.pubsub.fanin_datapubsub', 'description': 'Composite subscriber aggregating multiple sources', 'config': ['source', 'max_depth']},
                {'type': 'FanoutDataSubscriber', 'module': 'core.pubsub.fanout_datapubsub', 'description': 'Router subscriber with routing logic', 'config': ['source_subscriber', 'resolver_class', 'child_subscribers']},
                {'type': 'CustomDataSubscriber', 'module': 'core.pubsub.custom_datapubsub', 'description': 'Custom subscriber with delegate class', 'config': ['source', 'delegate_module', 'delegate_class', 'delegate_config']}
            ],
            'publisher_types': [
                {'type': 'KafkaDataPublisher', 'module': 'core.pubsub.kafka_datapubsub', 'description': 'Publish to Kafka topics', 'config': ['destination', 'bootstrap_servers']},
                {'type': 'RedisDataPublisher', 'module': 'core.pubsub.redis_datapubsub', 'description': 'Set data in Redis keys', 'config': ['destination', 'host', 'port', 'db']},
                {'type': 'RedisChannelDataPublisher', 'module': 'core.pubsub.redis_datapubsub', 'description': 'Publish to Redis pub/sub channels', 'config': ['destination', 'host', 'port', 'db']},
                {'type': 'RabbitMQDataPublisher', 'module': 'core.pubsub.rabbitmq_datapubsub', 'description': 'Publish to RabbitMQ queues/topics', 'config': ['destination', 'host', 'port', 'username', 'password']},
                {'type': 'ActiveMQDataPublisher', 'module': 'core.pubsub.activemq_datapubsub', 'description': 'Publish to ActiveMQ queues/topics', 'config': ['destination', 'host', 'port', 'username', 'password']},
                {'type': 'GRPCDataPublisher', 'module': 'core.pubsub.grpc_datapubsub', 'description': 'Publish to gRPC streams', 'config': ['destination', 'use_ssl', 'max_retries', 'timeout']},
                {'type': 'FileDataPublisher', 'module': 'core.pubsub.file_datapubsub', 'description': 'Append data to files (JSONL)', 'config': ['destination', 'publish_interval', 'batch_size']},
                {'type': 'InMemoryDataPublisher', 'module': 'core.pubsub.inmemory_datapubsub', 'description': 'Publish to in-memory queues/topics', 'config': ['destination', 'max_size']},
                {'type': 'InMemoryRedisDataPublisher', 'module': 'core.pubsub.inmemoryredis_datapubsub', 'description': 'Set data in InMemory Redis keys', 'config': ['destination', 'key_prefix', 'ttl_seconds']},
                {'type': 'InMemoryRedisChannelDataPublisher', 'module': 'core.pubsub.inmemoryredis_datapubsub', 'description': 'Publish to InMemory Redis channels', 'config': ['destination']},
                {'type': 'AshRedisDataPublisher', 'module': 'core.pubsub.ashredis_datapubsub', 'description': 'Set data in AshRedis keys', 'config': ['destination', 'host', 'port', 'region', 'ttl_seconds']},
                {'type': 'AshRedisChannelDataPublisher', 'module': 'core.pubsub.ashredis_datapubsub', 'description': 'Publish to AshRedis channels', 'config': ['destination', 'host', 'port', 'region']},
                {'type': 'AerospikeDataPublisher', 'module': 'core.pubsub.aerospike_datapubsub', 'description': 'Write data to Aerospike', 'config': ['destination', 'hosts']},
                {'type': 'MetronomeDataPublisher', 'module': 'core.pubsub.metronome_datapubsub', 'description': 'Emit messages at regular intervals', 'config': ['destination', 'interval', 'message']},
                {'type': 'FaninDataPublisher', 'module': 'core.pubsub.fanin_datapubsub', 'description': 'Composite publisher broadcasting to multiple destinations', 'config': ['destination', 'publish_interval']},
                {'type': 'FanoutDataPublisher', 'module': 'core.pubsub.fanout_datapubsub', 'description': 'Router publisher with routing logic', 'config': ['destination', 'resolver_class', 'child_publishers']},
                {'type': 'CustomDataPublisher', 'module': 'core.pubsub.custom_datapubsub', 'description': 'Custom publisher with delegate class', 'config': ['destination', 'delegate_module', 'delegate_class', 'delegate_config']}
            ],
            'subscriber_sources': [
                {'prefix': 'kafka://', 'description': 'Kafka topic subscription'},
                {'prefix': 'redischannel://', 'description': 'Redis pub/sub channel'},
                {'prefix': 'rabbitmq://', 'description': 'RabbitMQ queue or topic'},
                {'prefix': 'activemq://', 'description': 'ActiveMQ queue or topic'},
                {'prefix': 'grpc://', 'description': 'gRPC stream'},
                {'prefix': 'file://', 'description': 'File input (JSONL)'},
                {'prefix': 'inmemory://', 'description': 'In-memory queue/topic'},
                {'prefix': 'inmemoryredis://', 'description': 'InMemory Redis keys'},
                {'prefix': 'inmemoryredischannel://', 'description': 'InMemory Redis channel'},
                {'prefix': 'ashredischannel://', 'description': 'AshRedis channel'},
                {'prefix': 'metronome://', 'description': 'Timer-based source'},
                {'prefix': 'fanin://', 'description': 'Composite fan-in'},
                {'prefix': 'router://', 'description': 'Message router'},
                {'prefix': 'custom://', 'description': 'Custom subscriber'}
            ],
            'publisher_destinations': [
                {'prefix': 'kafka://', 'description': 'Kafka topic publishing'},
                {'prefix': 'redis://', 'description': 'Redis key-value storage'},
                {'prefix': 'redischannel://', 'description': 'Redis pub/sub channel'},
                {'prefix': 'rabbitmq://', 'description': 'RabbitMQ queue or topic'},
                {'prefix': 'activemq://', 'description': 'ActiveMQ queue or topic'},
                {'prefix': 'grpc://', 'description': 'gRPC stream'},
                {'prefix': 'file://', 'description': 'File output (JSONL)'},
                {'prefix': 'inmemory://', 'description': 'In-memory queue/topic'},
                {'prefix': 'inmemoryredis://', 'description': 'InMemory Redis keys'},
                {'prefix': 'inmemoryredischannel://', 'description': 'InMemory Redis channel'},
                {'prefix': 'ashredis://', 'description': 'AshRedis key-value storage'},
                {'prefix': 'ashredischannel://', 'description': 'AshRedis channel'},
                {'prefix': 'aerospike://', 'description': 'Aerospike database'},
                {'prefix': 'metronome://', 'description': 'Timer-based publisher'},
                {'prefix': 'fanin://', 'description': 'Composite fan-in broadcast'},
                {'prefix': 'router://', 'description': 'Message router'},
                {'prefix': 'custom://', 'description': 'Custom publisher'}
            ]
        }
        return jsonify(components)