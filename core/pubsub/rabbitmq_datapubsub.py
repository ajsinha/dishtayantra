"""
RabbitMQ DataPublisher and DataSubscriber implementation

Supports both queue and topic messaging patterns similar to ActiveMQ.
"""

import json
import logging
import time
from datetime import datetime
from core.pubsub.datapubsub import DataPublisher, DataSubscriber
import queue
try:
    import pika
    from pika.exceptions import AMQPConnectionError, AMQPChannelError
except ImportError:
    logging.warning("pika library not found. Install with: pip install pika")
    pika = None
    AMQPConnectionError = Exception
    AMQPChannelError = Exception

logger = logging.getLogger(__name__)

class RabbitMQConnectionError(Exception):
    """Custom exception for RabbitMQ connection issues"""
    pass

class RabbitMQDataPublisher(DataPublisher):
    """Publisher for RabbitMQ queues and topics"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        if not pika:
            raise ImportError("pika library required for RabbitMQ. Install with: pip install pika")

        # Parse destination: rabbitmq://queue/queue_name or rabbitmq://topic/topic_name
        parts = destination.replace('rabbitmq://', '').split('/')
        self.dest_type = parts[0]  # 'queue' or 'topic'
        self.dest_name = parts[1] if len(parts) > 1 else 'default'

        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 5672)
        self.username = config.get('username', 'guest')
        self.password = config.get('password', 'guest')
        self.virtual_host = config.get('virtual_host', '/')

        # Connection settings
        self.max_retries = 5
        self.retry_delay = 10
        self.connection = None
        self.channel = None
        self._connect()

        # For topics, we use a topic exchange
        if self.dest_type == 'topic':
            self.exchange = config.get('exchange', 'amq.topic')
            self.routing_key = self.dest_name
            # Declare exchange (using built-in topic exchange)
            self.channel.exchange_declare(
                exchange=self.exchange,
                exchange_type='topic',
                durable=True
            )
        else:
            # For queues, use default exchange
            self.exchange = ''
            self.routing_key = self.dest_name
            # Declare queue
            self.channel.queue_declare(queue=self.dest_name, durable=True)

        logger.info(f"RabbitMQ publisher connected to {self.host}:{self.port}, "
                    f"dest_type={self.dest_type}, dest_name={self.dest_name}")

    def _do_connect(self):
        """Establish connection to RabbitMQ"""
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )

            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            logger.info(f"Connected to RabbitMQ at {self.host}:{self.port}")

        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ ({self.name}): {str(e)}")
            raise

    def _connect(self):
        """Establish connection to RabbitMQ with retry logic"""
        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                self._do_connect()
                if self.channel and not self.channel.is_closed:
                    return  # Success
            except Exception as e:
                last_error = e
                logger.warning(
                    f"Connection attempt {attempt}/{self.max_retries} failed: {str(e)}"
                )
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay * attempt)  # Exponential backoff

        # All retries failed
        error_msg = f"Failed to connect to RabbitMQ after {self.max_retries} attempts"
        if last_error:
            error_msg += f": {str(last_error)}"
        raise RabbitMQConnectionError(error_msg)

    def _ensure_connection(self):
        """Ensure connection is alive, reconnect if needed"""
        try:
            if not self.connection or self.connection.is_closed:
                self._connect()
            elif not self.channel or self.channel.is_closed:
                self.channel = self.connection.channel()
        except Exception as e:
            logger.error(f"Error ensuring connection: {str(e)}")
            raise

    def _do_publish(self, data):
        """Publish to RabbitMQ queue or topic"""
        try:
            self._ensure_connection()

            # Serialize data to JSON
            json_data = json.dumps(data)

            # Publish message
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=self.routing_key,
                body=json_data,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Persistent
                    content_type='application/json'
                )
            )

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to RabbitMQ {self.dest_type}/{self.dest_name}")

        except Exception as e:
            logger.error(f"Error publishing to RabbitMQ: {str(e)}")
            # Try to reconnect
            try:
                self._connect()
            except:
                pass
            raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            logger.info(f"RabbitMQ publisher {self.name} stopped")
        except Exception as e:
            logger.warning(f"Error closing RabbitMQ connection: {str(e)}")


class RabbitMQDataSubscriber(DataSubscriber):
    """Subscriber for RabbitMQ queues and topics"""

    def __init__(self, name, source, config, given_queue: queue.Queue = None):
        super().__init__(name, source, config, given_queue)

        if not pika:
            raise ImportError("pika library required for RabbitMQ. Install with: pip install pika")

        # Parse source: rabbitmq://queue/queue_name or rabbitmq://topic/topic_name
        parts = source.replace('rabbitmq://', '').split('/')
        self.dest_type = parts[0]  # 'queue' or 'topic'
        self.dest_name = parts[1] if len(parts) > 1 else 'default'

        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 5672)
        self.username = config.get('username', 'guest')
        self.password = config.get('password', 'guest')
        self.virtual_host = config.get('virtual_host', '/')

        # Connection settings
        self.max_retries = 5
        self.retry_delay = 10
        self.connection = None
        self.channel = None
        self._connect()

        # Setup queue based on type
        if self.dest_type == 'topic':
            # For topics, create a temporary queue and bind to topic exchange
            self.exchange = config.get('exchange', 'amq.topic')
            self.queue_name = config.get('queue', f'{name}_queue')

            # Declare exchange
            self.channel.exchange_declare(
                exchange=self.exchange,
                exchange_type='topic',
                durable=True
            )

            # Declare queue
            result = self.channel.queue_declare(
                queue=self.queue_name,
                durable=config.get('queue_durable', False),
                exclusive=config.get('queue_exclusive', False),
                auto_delete=config.get('queue_auto_delete', True)
            )
            self.queue_name = result.method.queue

            # Bind queue to topic
            # Support wildcards: * (one word), # (zero or more words)
            binding_key = config.get('binding_key', self.dest_name)
            self.channel.queue_bind(
                exchange=self.exchange,
                queue=self.queue_name,
                routing_key=binding_key
            )

            logger.info(f"RabbitMQ subscriber bound queue {self.queue_name} to topic {binding_key}")
        else:
            # For queues, use the queue directly
            self.queue_name = self.dest_name
            self.channel.queue_declare(queue=self.queue_name, durable=True)

        logger.info(f"RabbitMQ subscriber connected to {self.host}:{self.port}, "
                    f"dest_type={self.dest_type}, queue={self.queue_name}")

    def _do_connect(self):
        """Establish connection to RabbitMQ"""
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )

            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=1)

            logger.info(f"Connected to RabbitMQ at {self.host}:{self.port}")

        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            raise

    def _connect(self):
        """Establish connection to RabbitMQ with retry logic"""
        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                self._do_connect()
                if self.channel and not self.channel.is_closed:
                    return  # Success
            except Exception as e:
                last_error = e
                logger.warning(
                    f"Connection attempt {attempt}/{self.max_retries} failed: {str(e)}"
                )
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay * attempt)  # Exponential backoff

        # All retries failed
        error_msg = f"Failed to connect to RabbitMQ after {self.max_retries} attempts"
        if last_error:
            error_msg += f": {str(last_error)}"
        raise RabbitMQConnectionError(error_msg)

    def _do_subscribe(self):
        """Subscribe from RabbitMQ"""
        try:
            # Reconnect if connection is closed
            if not self.connection or self.connection.is_closed:
                self._connect()

            # Get a message with basic_get (polling mode)
            method_frame, properties, body = self.channel.basic_get(
                queue=self.queue_name,
                auto_ack=True
            )

            if method_frame:
                # Deserialize JSON data
                data = json.loads(body.decode('utf-8'))
                logger.debug(f"Received message from RabbitMQ {self.dest_type}/{self.dest_name}")
                return data
            else:
                # No messages available
                return None

        except (AMQPConnectionError, AMQPChannelError) as e:
            logger.error(f"RabbitMQ connection error: {str(e)}")
            self.connection = None
            self.channel = None
            time.sleep(1)
            return None

        except Exception as e:
            logger.error(f"Error subscribing from RabbitMQ: {str(e)}")
            return None

    def stop(self):
        """Stop the subscriber"""
        super().stop()
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            logger.info(f"RabbitMQ subscriber {self.name} stopped")
        except Exception as e:
            logger.warning(f"Error closing RabbitMQ connection: {str(e)}")