"""
DAG Designer component catalogue (single source of truth)
=========================================================

The authoritative list of node / calculator / transformer / subscriber /
publisher types (and source/destination URI prefixes) offered by the visual
DAG Designer.  Both the JSON API (``GET /api/dag-designer/components`` in
``routes.dagdesigner_routes``) and the Designer palette (rendered dynamically
in ``web/templates/dag/designer.html``) read from this one place, so the
palette can never again silently drift from what the backend supports.

This module is pure data with no web/FastAPI dependency, so it is trivially
importable from tests (see ``tests/test_designer_catalogue.py``) and from any
tooling that needs to know the component catalogue.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""


def build_component_catalogue() -> dict:
    """Return the component catalogue used by the Designer palette + API.

    A fresh dict is built on each call so callers may mutate the result
    without affecting anyone else.
    """
    catalogue = {
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
            {'type': 'S3DataSubscriber', 'module': 'core.pubsub.s3_datapubsub', 'description': 'Poll messages from an S3 prefix (v2.0.0)', 'config': ['source', 'region', 'poll_interval', 'delete_on_read']},
            {'type': 'AzureBlobDataSubscriber', 'module': 'core.pubsub.azureblob_datapubsub', 'description': 'Poll messages from an Azure Blob prefix (v2.0.0)', 'config': ['source', 'connection_string', 'poll_interval', 'delete_on_read']},
            {'type': 'GCSDataSubscriber', 'module': 'core.pubsub.gcs_datapubsub', 'description': 'Poll messages from a GCS prefix (v2.0.0)', 'config': ['source', 'credentials_file', 'poll_interval', 'delete_on_read']},
            {'type': 'SQSDataSubscriber', 'module': 'core.pubsub.sqs_datapubsub', 'description': 'Receive messages from an AWS SQS queue', 'config': ['source', 'region', 'wait_time_seconds', 'max_messages']},
            {'type': 'KinesisDataSubscriber', 'module': 'core.pubsub.kinesis_datapubsub', 'description': 'Read records from an AWS Kinesis stream', 'config': ['source', 'region', 'shard_iterator_type', 'poll_interval']},
            {'type': 'ServiceBusDataSubscriber', 'module': 'core.pubsub.servicebus_datapubsub', 'description': 'Receive from an Azure Service Bus queue/topic', 'config': ['source', 'connection_string', 'max_messages']},
            {'type': 'EventHubsDataSubscriber', 'module': 'core.pubsub.eventhubs_datapubsub', 'description': 'Consume events from Azure Event Hubs', 'config': ['source', 'connection_string', 'consumer_group']},
            {'type': 'LMDBDataSubscriber', 'module': 'core.pubsub.lmdbpubsub_endpoints', 'description': 'Read messages from an LMDB store (zero-copy IPC)', 'config': ['source', 'path', 'poll_interval']},
            {'type': 'SQLDataSubscriber', 'module': 'core.pubsub.sql_datapubsub', 'description': 'Poll rows from a SQL database table', 'config': ['source', 'connection_string', 'query', 'poll_interval']},
            {'type': 'RESTDataSubscriber', 'module': 'core.pubsub.rest_datapubsub', 'description': 'Poll a REST/HTTP endpoint for messages', 'config': ['source', 'url', 'method', 'headers', 'poll_interval']},
            {'type': 'TibcoEMSDataSubscriber', 'module': 'core.pubsub.tibcoems_datapubsub', 'description': 'Subscribe from TIBCO EMS queues/topics', 'config': ['source', 'server_url', 'username', 'password']},
            {'type': 'WebSphereMQDataSubscriber', 'module': 'core.pubsub.websphere_datapubsub', 'description': 'Subscribe from IBM WebSphere MQ', 'config': ['source', 'queue_manager', 'channel', 'host', 'port']},
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
            {'type': 'S3DataPublisher', 'module': 'core.pubsub.s3_datapubsub', 'description': 'Publish messages as S3 objects (v2.0.0)', 'config': ['destination', 'region']},
            {'type': 'AzureBlobDataPublisher', 'module': 'core.pubsub.azureblob_datapubsub', 'description': 'Publish messages as Azure blobs (v2.0.0)', 'config': ['destination', 'connection_string']},
            {'type': 'GCSDataPublisher', 'module': 'core.pubsub.gcs_datapubsub', 'description': 'Publish messages as GCS objects (v2.0.0)', 'config': ['destination', 'credentials_file']},
            {'type': 'SQSDataPublisher', 'module': 'core.pubsub.sqs_datapubsub', 'description': 'Send messages to an AWS SQS queue', 'config': ['destination', 'region']},
            {'type': 'SNSDataPublisher', 'module': 'core.pubsub.sns_datapubsub', 'description': 'Publish messages to an AWS SNS topic', 'config': ['destination', 'region']},
            {'type': 'KinesisDataPublisher', 'module': 'core.pubsub.kinesis_datapubsub', 'description': 'Put records to an AWS Kinesis stream', 'config': ['destination', 'region', 'partition_key']},
            {'type': 'ServiceBusDataPublisher', 'module': 'core.pubsub.servicebus_datapubsub', 'description': 'Send to an Azure Service Bus queue/topic', 'config': ['destination', 'connection_string']},
            {'type': 'EventHubsDataPublisher', 'module': 'core.pubsub.eventhubs_datapubsub', 'description': 'Send events to Azure Event Hubs', 'config': ['destination', 'connection_string']},
            {'type': 'LMDBDataPublisher', 'module': 'core.pubsub.lmdbpubsub_endpoints', 'description': 'Write messages to an LMDB store (zero-copy IPC)', 'config': ['destination', 'path', 'map_size']},
            {'type': 'SQLDataPublisher', 'module': 'core.pubsub.sql_datapubsub', 'description': 'Insert rows into a SQL database table', 'config': ['destination', 'connection_string', 'table']},
            {'type': 'RESTDataPublisher', 'module': 'core.pubsub.rest_datapubsub', 'description': 'POST messages to a REST/HTTP endpoint', 'config': ['destination', 'url', 'method', 'headers']},
            {'type': 'TibcoEMSDataPublisher', 'module': 'core.pubsub.tibcoems_datapubsub', 'description': 'Publish to TIBCO EMS queues/topics', 'config': ['destination', 'server_url', 'username', 'password']},
            {'type': 'WebSphereMQDataPublisher', 'module': 'core.pubsub.websphere_datapubsub', 'description': 'Publish to IBM WebSphere MQ', 'config': ['destination', 'queue_manager', 'channel', 'host', 'port']},
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
            {'prefix': 's3://', 'description': 'AWS S3 object prefix (v2.0.0)'},
            {'prefix': 'azureblob://', 'description': 'Azure Blob prefix (v2.0.0)'},
            {'prefix': 'gcs://', 'description': 'Google Cloud Storage prefix (v2.0.0)'},
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
            {'prefix': 's3://', 'description': 'AWS S3 object prefix (v2.0.0)'},
            {'prefix': 'azureblob://', 'description': 'Azure Blob prefix (v2.0.0)'},
            {'prefix': 'gcs://', 'description': 'Google Cloud Storage prefix (v2.0.0)'},
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

    return catalogue


# Palette-bearing catalogue sections: (catalogue key, palette drag category).
# URI-prefix sections (subscriber_sources / publisher_destinations) are
# intentionally excluded - they are reference lists, not draggable items.
PALETTE_SECTIONS = (
    ("node_types", "node"),
    ("subscriber_types", "subscriber"),
    ("publisher_types", "publisher"),
    ("calculator_types", "calculator"),
    ("transformer_types", "transformer"),
)


def palette_component_types() -> set:
    """Every component ``type`` that should appear as a draggable palette item."""
    cat = build_component_catalogue()
    types = set()
    for key, _category in PALETTE_SECTIONS:
        for entry in cat.get(key, []):
            t = entry.get("type")
            if t:
                types.add(t)
    return types
