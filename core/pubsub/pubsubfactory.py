import logging

from core.core_utils import validate_name

logger = logging.getLogger(__name__)

def create_publisher(name, config):
    """
    Factory method to create appropriate DataPublisher based on destination

    Args:
        name: Name of the publisher
        config: Configuration dictionary containing 'destination' and other params

    Returns:
        Instance of appropriate DataPublisher

    Supported destinations:
        - mem://queue/queue_name - In-memory queue
        - mem://topic/topic_name - In-memory topic
        - mem://name - Simple in-memory (v1.5.2)
        - inmemory://name - Alternative in-memory format (v1.5.2)
        - memory://name - Alternative in-memory format (v1.5.2)
        - lmdb://channel_name - LMDB cross-process pub/sub (v1.5.2)
        - file://path/to/file - File-based
        - kafka://topic/topic_name - Apache Kafka
        - activemq://queue/queue_name - ActiveMQ queue
        - activemq://topic/topic_name - ActiveMQ topic
        - grpc://host:port/topic_name - gRPC
        - rabbitmq://queue/queue_name - RabbitMQ queue
        - rabbitmq://topic/topic_name - RabbitMQ topic
        - tibcoems://queue/queue_name - TIBCO EMS queue
        - tibcoems://topic/topic_name - TIBCO EMS topic
        - websphere://queue/QUEUE_NAME - WebSphere MQ queue
        - websphere://topic/TOPIC_NAME - WebSphere MQ topic
        - sql://table_name - SQL database
        - rest://endpoint - REST API
        - redis://key - Redis key
        - redischannel://channel - Redis pub/sub
        - aerospike://namespace:set - Aerospike
        - custom://destination - Custom implementation
        - metronome - Metronome (scheduled)
    """
    validate_name(name)

    destination = config.get('destination')
    if not destination:
        raise ValueError("Destination not specified in config")

    logger.info(f"Creating publisher {name} for destination {destination}")

    # In-Memory (queues and topics)
    # Supports: mem://queue/, mem://topic/, mem://, inmemory://, memory://
    if destination.startswith('mem://queue/'):
        from core.pubsub.inmemory_datapubsub import InMemoryDataPublisher
        return InMemoryDataPublisher(name, destination, config)
    elif destination.startswith('mem://topic/'):
        from core.pubsub.inmemory_datapubsub import InMemoryDataPublisher
        return InMemoryDataPublisher(name, destination, config)
    elif destination.startswith('mem://'):
        # Simple mem:// format (v1.5.2) - e.g., mem://my_queue
        from core.pubsub.inmemory_datapubsub import InMemoryDataPublisher
        return InMemoryDataPublisher(name, destination, config)
    elif destination.startswith('inmemory://'):
        # inmemory:// format (v1.5.2) - e.g., inmemory://batch_data_queue
        from core.pubsub.inmemory_datapubsub import InMemoryDataPublisher
        normalized_dest = 'mem://' + destination[len('inmemory://'):]
        return InMemoryDataPublisher(name, normalized_dest, config)
    elif destination.startswith('memory://'):
        # memory:// format (v1.5.2) - e.g., memory://market_data
        from core.pubsub.inmemory_datapubsub import InMemoryDataPublisher
        normalized_dest = 'mem://' + destination[len('memory://'):]
        return InMemoryDataPublisher(name, normalized_dest, config)

    # File-based
    elif destination.startswith('file://'):
        from core.pubsub.file_datapubsub import FileDataPublisher
        return FileDataPublisher(name, destination, config)

    # Apache Kafka
    elif destination.startswith('kafka://topic/'):
        from core.pubsub.kafka_datapubsub import KafkaDataPublisher
        return KafkaDataPublisher(name, destination, config)

    # ActiveMQ (queues and topics)
    elif destination.startswith('activemq://queue/'):
        from core.pubsub.activemq_datapubsub import ActiveMQDataPublisher
        return ActiveMQDataPublisher(name, destination, config)
    elif destination.startswith('activemq://topic/'):
        from core.pubsub.activemq_datapubsub import ActiveMQDataPublisher
        return ActiveMQDataPublisher(name, destination, config)

    # gRPC (set "resilient": true in config for retry/buffer/reconnect)
    elif destination.startswith('grpc://'):
        if config.get('resilient', False):
            from core.pubsub.resilient_grpc import ResilientGRPCDataPublisher
            return ResilientGRPCDataPublisher(name, destination, config)
        from core.pubsub.grpc_datapubsub import GRPCDataPublisher
        return GRPCDataPublisher(name, destination, config)

    # RabbitMQ (queues and topics)
    elif destination.startswith('rabbitmq://queue/'):
        from core.pubsub.rabbitmq_datapubsub import RabbitMQDataPublisher
        return RabbitMQDataPublisher(name, destination, config)
    elif destination.startswith('rabbitmq://topic/'):
        from core.pubsub.rabbitmq_datapubsub import RabbitMQDataPublisher
        return RabbitMQDataPublisher(name, destination, config)

    # TIBCO EMS (queues and topics)
    elif destination.startswith('tibcoems://queue/'):
        from core.pubsub.tibcoems_datapubsub import TibcoEMSDataPublisher
        return TibcoEMSDataPublisher(name, destination, config)
    elif destination.startswith('tibcoems://topic/'):
        from core.pubsub.tibcoems_datapubsub import TibcoEMSDataPublisher
        return TibcoEMSDataPublisher(name, destination, config)

    # WebSphere MQ / IBM MQ (queues and topics)
    elif destination.startswith('websphere://queue/'):
        from core.pubsub.websphere_datapubsub import WebSphereMQDataPublisher
        return WebSphereMQDataPublisher(name, destination, config)
    elif destination.startswith('websphere://topic/'):
        from core.pubsub.websphere_datapubsub import WebSphereMQDataPublisher
        return WebSphereMQDataPublisher(name, destination, config)

    # SQL Database (set "resilient": true for retry/buffer/reconnect)
    elif destination.startswith('sql://'):
        if config.get('resilient', False):
            from core.pubsub.resilient_sql import ResilientSQLDataPublisher
            return ResilientSQLDataPublisher(name, destination, config)
        from core.pubsub.sql_datapubsub import SQLDataPublisher
        return SQLDataPublisher(name, destination, config)

    # REST API (set "resilient": true for retry/buffer/recovery)
    elif destination.startswith('rest://'):
        if config.get('resilient', False):
            from core.pubsub.resilient_rest import ResilientRESTDataPublisher
            return ResilientRESTDataPublisher(name, destination, config)
        from core.pubsub.rest_datapubsub import RESTDataPublisher
        return RESTDataPublisher(name, destination, config)

    # Redis
    elif destination.startswith('redis://'):
        from core.pubsub.redis_datapubsub import RedisDataPublisher
        return RedisDataPublisher(name, destination, config)
    elif destination.startswith('redischannel://'):
        from core.pubsub.redis_datapubsub import RedisChannelDataPublisher
        return RedisChannelDataPublisher(name, destination, config)

    # Aerospike (set "resilient": true for retry/buffer/reconnect)
    elif destination.startswith('aerospike://'):
        if config.get('resilient', False):
            from core.pubsub.resilient_aerospike import ResilientAerospikeDataPublisher
            return ResilientAerospikeDataPublisher(name, destination, config)
        from core.pubsub.aerospike_datapubsub import AerospikeDataPublisher
        return AerospikeDataPublisher(name, destination, config)

    # Custom
    elif destination.startswith('custom://'):
        from core.pubsub.custom_datapubsub import CustomDataPublisher
        return CustomDataPublisher(name, destination, config)

    # Metronome
    elif destination == 'metronome':
        from core.pubsub.metronome_datapubsub import MetronomeDataPublisher
        return MetronomeDataPublisher(name, destination, config)

    # Cloud object stores (v2.0.0)
    elif destination.startswith('s3://'):
        from core.pubsub.s3_datapubsub import S3DataPublisher
        return S3DataPublisher(name, destination, config)
    elif destination.startswith('azureblob://'):
        from core.pubsub.azureblob_datapubsub import AzureBlobDataPublisher
        return AzureBlobDataPublisher(name, destination, config)
    elif destination.startswith('gcs://'):
        from core.pubsub.gcs_datapubsub import GCSDataPublisher
        return GCSDataPublisher(name, destination, config)

    # AWS managed messaging (v2.2; "resilient": true wraps with retry/buffer)
    elif destination.startswith('sqs://'):
        from core.pubsub.sqs_datapubsub import SQSDataPublisher
        inner = lambda: SQSDataPublisher(name, destination, config)
        if config.get('resilient', False):
            from core.pubsub.resilient_base import ResilientDataPublisherWrapper
            return ResilientDataPublisherWrapper(name, destination, config, inner)
        return inner()
    elif destination.startswith('kinesis://'):
        from core.pubsub.kinesis_datapubsub import KinesisDataPublisher
        inner = lambda: KinesisDataPublisher(name, destination, config)
        if config.get('resilient', False):
            from core.pubsub.resilient_base import ResilientDataPublisherWrapper
            return ResilientDataPublisherWrapper(name, destination, config, inner)
        return inner()
    elif destination.startswith('sns://'):
        from core.pubsub.sns_datapubsub import SNSDataPublisher
        inner = lambda: SNSDataPublisher(name, destination, config)
        if config.get('resilient', False):
            from core.pubsub.resilient_base import ResilientDataPublisherWrapper
            return ResilientDataPublisherWrapper(name, destination, config, inner)
        return inner()

    # Azure managed messaging (v2.2)
    elif destination.startswith('servicebus://'):
        from core.pubsub.servicebus_datapubsub import ServiceBusDataPublisher
        inner = lambda: ServiceBusDataPublisher(name, destination, config)
        if config.get('resilient', False):
            from core.pubsub.resilient_base import ResilientDataPublisherWrapper
            return ResilientDataPublisherWrapper(name, destination, config, inner)
        return inner()
    elif destination.startswith('eventhubs://'):
        from core.pubsub.eventhubs_datapubsub import EventHubsDataPublisher
        inner = lambda: EventHubsDataPublisher(name, destination, config)
        if config.get('resilient', False):
            from core.pubsub.resilient_base import ResilientDataPublisherWrapper
            return ResilientDataPublisherWrapper(name, destination, config, inner)
        return inner()

    # LMDB (v1.5.2) - for cross-process communication
    elif destination.startswith('lmdb://'):
        from core.pubsub.lmdbpubsub import create_lmdb_publisher
        return create_lmdb_publisher(name, config)

    else:
        raise ValueError(f"Unknown destination type: {destination}")


def create_subscriber(name, config):
    """
    Factory method to create appropriate DataSubscriber based on source

    Args:
        name: Name of the subscriber
        config: Configuration dictionary containing 'source' and other params

    Returns:
        Instance of appropriate DataSubscriber

    Supported sources:
        - mem://queue/queue_name - In-memory queue
        - mem://topic/topic_name - In-memory topic
        - mem://name - Simple in-memory (v1.5.2)
        - inmemory://name - Alternative in-memory format (v1.5.2)
        - memory://name - Alternative in-memory format (v1.5.2)
        - lmdb://channel_name - LMDB cross-process pub/sub (v1.5.2)
        - file://path/to/file - File-based
        - kafka://topic/topic_name - Apache Kafka
        - activemq://queue/queue_name - ActiveMQ queue
        - activemq://topic/topic_name - ActiveMQ topic
        - grpc://host:port/topic_name - gRPC
        - rabbitmq://queue/queue_name - RabbitMQ queue
        - rabbitmq://topic/topic_name - RabbitMQ topic
        - tibcoems://queue/queue_name - TIBCO EMS queue
        - tibcoems://topic/topic_name - TIBCO EMS topic
        - websphere://queue/QUEUE_NAME - WebSphere MQ queue
        - websphere://topic/TOPIC_NAME - WebSphere MQ topic
        - sql://table_name - SQL database
        - rest://endpoint - REST API
        - redischannel://channel - Redis pub/sub
        - custom://source - Custom implementation
        - metronome - Metronome (scheduled)
    """
    validate_name(name)

    source = config.get('source')
    if not source:
        raise ValueError("Source not specified in config")

    logger.info(f"Creating subscriber {name} for source {source}")

    if source.startswith('fanin://'):
        from core.pubsub.fanin_datapubsub import FaninDataSubscriber
        source = source[len('fanin://'):]
        return FaninDataSubscriber(name, source, config)
    elif source.startswith('composite://'):
        from core.pubsub.fanin_datapubsub import FaninDataSubscriber
        source = source[len('composite://'):]
        return FaninDataSubscriber(name, source, config)

    # In-Memory (queues and topics)
    # Supports: mem://queue/, mem://topic/, mem://, inmemory://, memory://
    elif source.startswith('mem://queue/'):
        from core.pubsub.inmemory_datapubsub import InMemoryDataSubscriber
        return InMemoryDataSubscriber(name, source, config)
    elif source.startswith('mem://topic/'):
        from core.pubsub.inmemory_datapubsub import InMemoryDataSubscriber
        return InMemoryDataSubscriber(name, source, config)
    elif source.startswith('mem://'):
        # Simple mem:// format (v1.5.2) - e.g., mem://my_queue
        from core.pubsub.inmemory_datapubsub import InMemoryDataSubscriber
        return InMemoryDataSubscriber(name, source, config)
    elif source.startswith('inmemory://'):
        # inmemory:// format (v1.5.2) - e.g., inmemory://batch_data_queue
        from core.pubsub.inmemory_datapubsub import InMemoryDataSubscriber
        # Normalize to mem:// format for consistency
        normalized_source = 'mem://' + source[len('inmemory://'):]
        return InMemoryDataSubscriber(name, normalized_source, config)
    elif source.startswith('memory://'):
        # memory:// format (v1.5.2) - e.g., memory://market_data
        from core.pubsub.inmemory_datapubsub import InMemoryDataSubscriber
        # Normalize to mem:// format for consistency
        normalized_source = 'mem://' + source[len('memory://'):]
        return InMemoryDataSubscriber(name, normalized_source, config)

    # File-based
    elif source.startswith('file://'):
        from core.pubsub.file_datapubsub import FileDataSubscriber
        return FileDataSubscriber(name, source, config)

    # Apache Kafka
    elif source.startswith('kafka://topic/'):
        from core.pubsub.kafka_datapubsub import KafkaDataSubscriber
        return KafkaDataSubscriber(name, source, config)

    # ActiveMQ (queues and topics)
    elif source.startswith('activemq://queue/'):
        from core.pubsub.activemq_datapubsub import ActiveMQDataSubscriber
        return ActiveMQDataSubscriber(name, source, config)
    elif source.startswith('activemq://topic/'):
        from core.pubsub.activemq_datapubsub import ActiveMQDataSubscriber
        return ActiveMQDataSubscriber(name, source, config)

    # gRPC (set "resilient": true in config for backoff/reconnect)
    elif source.startswith('grpc://'):
        if config.get('resilient', False):
            from core.pubsub.resilient_grpc import ResilientGRPCDataSubscriber
            return ResilientGRPCDataSubscriber(name, source, config)
        from core.pubsub.grpc_datapubsub import GRPCDataSubscriber
        return GRPCDataSubscriber(name, source, config)

    # RabbitMQ (queues and topics)
    elif source.startswith('rabbitmq://queue/'):
        from core.pubsub.rabbitmq_datapubsub import RabbitMQDataSubscriber
        return RabbitMQDataSubscriber(name, source, config)
    elif source.startswith('rabbitmq://topic/'):
        from core.pubsub.rabbitmq_datapubsub import RabbitMQDataSubscriber
        return RabbitMQDataSubscriber(name, source, config)

    # TIBCO EMS (queues and topics)
    elif source.startswith('tibcoems://queue/'):
        from core.pubsub.tibcoems_datapubsub import TibcoEMSDataSubscriber
        return TibcoEMSDataSubscriber(name, source, config)
    elif source.startswith('tibcoems://topic/'):
        from core.pubsub.tibcoems_datapubsub import TibcoEMSDataSubscriber
        return TibcoEMSDataSubscriber(name, source, config)

    # WebSphere MQ / IBM MQ (queues and topics)
    elif source.startswith('websphere://queue/'):
        from core.pubsub.websphere_datapubsub import WebSphereMQDataSubscriber
        return WebSphereMQDataSubscriber(name, source, config)
    elif source.startswith('websphere://topic/'):
        from core.pubsub.websphere_datapubsub import WebSphereMQDataSubscriber
        return WebSphereMQDataSubscriber(name, source, config)

    # SQL Database (set "resilient": true for backoff/reconnect)
    elif source.startswith('sql://'):
        if config.get('resilient', False):
            from core.pubsub.resilient_sql import ResilientSQLDataSubscriber
            return ResilientSQLDataSubscriber(name, source, config)
        from core.pubsub.sql_datapubsub import SQLDataSubscriber
        return SQLDataSubscriber(name, source, config)

    # REST API (set "resilient": true for backoff/recovery)
    elif source.startswith('rest://'):
        if config.get('resilient', False):
            from core.pubsub.resilient_rest import ResilientRESTDataSubscriber
            return ResilientRESTDataSubscriber(name, source, config)
        from core.pubsub.rest_datapubsub import RESTDataSubscriber
        return RESTDataSubscriber(name, source, config)

    # Redis (channel only for subscriber)
    elif source.startswith('redischannel://'):
        from core.pubsub.redis_datapubsub import RedisChannelDataSubscriber
        return RedisChannelDataSubscriber(name, source, config)

    # Custom
    elif source.startswith('custom://'):
        from core.pubsub.custom_datapubsub import CustomDataSubscriber
        return CustomDataSubscriber(name, source, config)

    # Metronome
    elif source == 'metronome':
        from core.pubsub.metronome_datapubsub import MetronomeDataSubscriber
        return MetronomeDataSubscriber(name, source, config)

    # Cloud object stores (v2.0.0)
    elif source.startswith('s3://'):
        from core.pubsub.s3_datapubsub import S3DataSubscriber
        return S3DataSubscriber(name, source, config)
    elif source.startswith('azureblob://'):
        from core.pubsub.azureblob_datapubsub import AzureBlobDataSubscriber
        return AzureBlobDataSubscriber(name, source, config)
    elif source.startswith('gcs://'):
        from core.pubsub.gcs_datapubsub import GCSDataSubscriber
        return GCSDataSubscriber(name, source, config)

    # AWS managed messaging (v2.2; "resilient": true wraps with backoff/reconnect).
    # Note: SNS is publish-only (fan-out); consume via an SQS queue subscribed
    # to the topic, using an sqs:// source.
    elif source.startswith('sqs://'):
        from core.pubsub.sqs_datapubsub import SQSDataSubscriber
        inner = lambda: SQSDataSubscriber(name, source, config)
        if config.get('resilient', False):
            from core.pubsub.resilient_base import ResilientDataSubscriberWrapper
            return ResilientDataSubscriberWrapper(name, source, config, inner)
        return inner()
    elif source.startswith('kinesis://'):
        from core.pubsub.kinesis_datapubsub import KinesisDataSubscriber
        inner = lambda: KinesisDataSubscriber(name, source, config)
        if config.get('resilient', False):
            from core.pubsub.resilient_base import ResilientDataSubscriberWrapper
            return ResilientDataSubscriberWrapper(name, source, config, inner)
        return inner()

    # Azure managed messaging (v2.2)
    elif source.startswith('servicebus://'):
        from core.pubsub.servicebus_datapubsub import ServiceBusDataSubscriber
        inner = lambda: ServiceBusDataSubscriber(name, source, config)
        if config.get('resilient', False):
            from core.pubsub.resilient_base import ResilientDataSubscriberWrapper
            return ResilientDataSubscriberWrapper(name, source, config, inner)
        return inner()
    elif source.startswith('eventhubs://'):
        from core.pubsub.eventhubs_datapubsub import EventHubsDataSubscriber
        inner = lambda: EventHubsDataSubscriber(name, source, config)
        if config.get('resilient', False):
            from core.pubsub.resilient_base import ResilientDataSubscriberWrapper
            return ResilientDataSubscriberWrapper(name, source, config, inner)
        return inner()

    # LMDB (v1.5.2) - for cross-process communication
    elif source.startswith('lmdb://'):
        from core.pubsub.lmdbpubsub import create_lmdb_subscriber
        return create_lmdb_subscriber(name, config)

    else:
        raise ValueError(f"Unknown source type: {source}")