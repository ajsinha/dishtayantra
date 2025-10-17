import logging
import re

logger = logging.getLogger(__name__)


def validate_name(name):
    """Validate that name contains only alphanumeric and underscore with at least one alphabetic character"""
    if not re.match(r'^(?=.*[a-zA-Z])[a-zA-Z0-9_]+$', name):
        raise ValueError(
            f"Invalid name: {name}. Must contain only alphanumeric and underscore with at least one alphabetic character")
    return True


def create_publisher(name, config):
    """
    Factory method to create appropriate DataPublisher based on destination

    Args:
        name: Name of the publisher
        config: Configuration dictionary containing 'destination' and other params

    Returns:
        Instance of appropriate DataPublisher
    """
    validate_name(name)

    destination = config.get('destination')
    if not destination:
        raise ValueError("Destination not specified in config")

    logger.info(f"Creating publisher {name} for destination {destination}")

    if destination.startswith('mem://queue/'):
        from core.pubsub.inmemory_datapubsub import InMemoryDataPublisher
        return InMemoryDataPublisher(name, destination, config)
    elif destination.startswith('mem://topic/'):
        from core.pubsub.inmemory_datapubsub import InMemoryDataPublisher
        return InMemoryDataPublisher(name, destination, config)
    elif destination.startswith('file://'):
        from core.pubsub.file_datapubsub import FileDataPublisher
        return FileDataPublisher(name, destination, config)
    elif destination.startswith('kafka://topic/'):
        from core.pubsub.kafka_datapubsub import KafkaDataPublisher
        return KafkaDataPublisher(name, destination, config)
    elif destination.startswith('grpc://'):
        from core.pubsub.grpc_datapubsub import GRPCDataPublisher
        return GRPCDataPublisher(name, destination, config)
    elif destination.startswith('activemq://'):
        from core.pubsub.activemq_datapubsub import ActiveMQDataPublisher
        return ActiveMQDataPublisher(name, destination, config)
    elif destination.startswith('sql://'):
        from core.pubsub.sql_datapubsub import SQLDataPublisher
        return SQLDataPublisher(name, destination, config)
    elif destination.startswith('aerospike://'):
        from core.pubsub.aerospike_datapubsub import AerospikeDataPublisher
        return AerospikeDataPublisher(name, destination, config)
    elif destination.startswith('redis://'):
        from core.pubsub.redis_datapubsub import RedisDataPublisher
        return RedisDataPublisher(name, destination, config)
    elif destination.startswith('redischannel://'):
        from core.pubsub.redis_datapubsub import RedisChannelDataPublisher
        return RedisChannelDataPublisher(name, destination, config)
    elif destination.startswith('custom://'):
        from core.pubsub.custom_datapubsub import CustomDataPublisher
        return CustomDataPublisher(name, destination, config)
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
    """
    validate_name(name)

    source = config.get('source')
    if not source:
        raise ValueError("Source not specified in config")

    logger.info(f"Creating subscriber {name} for source {source}")

    if source.startswith('mem://queue/'):
        from core.pubsub.inmemory_datapubsub import InMemoryDataSubscriber
        return InMemoryDataSubscriber(name, source, config)
    elif source.startswith('mem://topic/'):
        from core.pubsub.inmemory_datapubsub import InMemoryDataSubscriber
        return InMemoryDataSubscriber(name, source, config)
    elif source.startswith('file://'):
        from core.pubsub.file_datapubsub import FileDataSubscriber
        return FileDataSubscriber(name, source, config)
    elif source.startswith('kafka://topic/'):
        from core.pubsub.kafka_datapubsub import KafkaDataSubscriber
        return KafkaDataSubscriber(name, source, config)
    elif source.startswith('grpc://'):
        from core.pubsub.grpc_datapubsub import GRPCDataSubscriber
        return GRPCDataSubscriber(name, source, config)
    elif source.startswith('activemq://'):
        from core.pubsub.activemq_datapubsub import ActiveMQDataSubscriber
        return ActiveMQDataSubscriber(name, source, config)
    elif source.startswith('sql://'):
        from core.pubsub.sql_datapubsub import SQLDataSubscriber
        return SQLDataSubscriber(name, source, config)
    elif source.startswith('redischannel://'):
        from core.pubsub.redis_datapubsub import RedisChannelDataSubscriber
        return RedisChannelDataSubscriber(name, source, config)
    elif source.startswith('custom://'):
        from core.pubsub.custom_datapubsub import CustomDataSubscriber
        return CustomDataSubscriber(name, source, config)
    elif source == 'metronome':
        from core.pubsub.metronome_datapubsub import MetronomeDataSubscriber
        return MetronomeDataSubscriber(name, source, config)
    else:
        raise ValueError(f"Unknown source type: {source}")