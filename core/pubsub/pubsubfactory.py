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
    if destination.startswith('mem://queue/'):
        from core.pubsub.inmemory_datapubsub import InMemoryDataPublisher
        return InMemoryDataPublisher(name, destination, config)
    elif destination.startswith('mem://topic/'):
        from core.pubsub.inmemory_datapubsub import InMemoryDataPublisher
        return InMemoryDataPublisher(name, destination, config)

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

    # gRPC
    elif destination.startswith('grpc://'):
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

    # SQL Database
    elif destination.startswith('sql://'):
        from core.pubsub.sql_datapubsub import SQLDataPublisher
        return SQLDataPublisher(name, destination, config)

    # REST API
    elif destination.startswith('rest://'):
        from core.pubsub.rest_datapubsub import RESTDataPublisher
        return RESTDataPublisher(name, destination, config)

    # Redis
    elif destination.startswith('redis://'):
        from core.pubsub.redis_datapubsub import RedisDataPublisher
        return RedisDataPublisher(name, destination, config)
    elif destination.startswith('redischannel://'):
        from core.pubsub.redis_datapubsub import RedisChannelDataPublisher
        return RedisChannelDataPublisher(name, destination, config)

    # Aerospike
    elif destination.startswith('aerospike://'):
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

    if source.startswith('composite://'):
        from core.pubsub.composite_datapubsub import CompositeDataSubscriber
        source = source[len('composite://'):]
        return CompositeDataSubscriber(name, source, config)

    # In-Memory (queues and topics)
    elif source.startswith('mem://queue/'):
        from core.pubsub.inmemory_datapubsub import InMemoryDataSubscriber
        return InMemoryDataSubscriber(name, source, config)
    elif source.startswith('mem://topic/'):
        from core.pubsub.inmemory_datapubsub import InMemoryDataSubscriber
        return InMemoryDataSubscriber(name, source, config)

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

    # gRPC
    elif source.startswith('grpc://'):
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

    # SQL Database
    elif source.startswith('sql://'):
        from core.pubsub.sql_datapubsub import SQLDataSubscriber
        return SQLDataSubscriber(name, source, config)

    # REST API
    elif source.startswith('rest://'):
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

    else:
        raise ValueError(f"Unknown source type: {source}")