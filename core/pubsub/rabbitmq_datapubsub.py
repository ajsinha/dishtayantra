"""
RabbitMQ DataPublisher and DataSubscriber implementation

Supports both queue and topic messaging patterns similar to ActiveMQ.
v1.7.6: Enhanced connection retry and auto-recovery support.
"""

import json
import logging
import time
import traceback
import threading
from datetime import datetime
from core.pubsub.datapubsub import DataPublisher, DataSubscriber, smart_deserialize
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


def _build_rabbitmq_parameters(host, port, virtual_host, username, password,
                               config):
    """Build pika ConnectionParameters with optional TLS.

    TLS is enabled with ``"use_ssl": true``. The standard AMQPS port is 5671;
    set ``port`` accordingly. Optional TLS keys (all paths to PEM files):

        ssl_ca_certs   : CA bundle to verify the broker (server auth)
        ssl_certfile   : client certificate (for mutual TLS)
        ssl_keyfile    : client private key (for mutual TLS)
        ssl_server_hostname : SNI / hostname to verify (defaults to ``host``)
        ssl_no_verify  : when true, disable cert verification (NOT for prod)
    """
    credentials = pika.PlainCredentials(username, password)
    ssl_options = None
    if config.get('use_ssl', False):
        import ssl as _ssl
        context = _ssl.create_default_context(cafile=config.get('ssl_ca_certs'))
        certfile = config.get('ssl_certfile')
        keyfile = config.get('ssl_keyfile')
        if certfile:
            context.load_cert_chain(certfile=certfile, keyfile=keyfile)
        if config.get('ssl_no_verify', False):
            context.check_hostname = False
            context.verify_mode = _ssl.CERT_NONE
        server_hostname = config.get('ssl_server_hostname', host)
        ssl_options = pika.SSLOptions(context, server_hostname=server_hostname)

    return pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host=virtual_host,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
        ssl_options=ssl_options,
    )

class RabbitMQDataPublisher(DataPublisher):
    """Publisher for RabbitMQ queues and topics with v1.7.6 connection resilience."""

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

        # v1.7.6: Connection retry configuration
        self.max_retries = config.get('max_retries', 5)
        self.retry_delay = config.get('retry_delay', 3)
        self._auto_reconnect = config.get('auto_reconnect', True)
        self._connected = False
        self.connection = None
        self.channel = None
        
        # v1.7.6: Connect with retry logic
        self._connect_with_retry()

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
        parameters = _build_rabbitmq_parameters(
            self.host, self.port, self.virtual_host,
            self.username, self.password, self.config)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self._connected = True

    def _connect_with_retry(self):
        """v1.7.6: Establish connection to RabbitMQ with retry logic"""
        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"RabbitMQ publisher connection attempt {attempt}/{self.max_retries} to {self.host}:{self.port}")
                self._do_connect()
                if self.channel and not self.channel.is_closed:
                    logger.info(f"RabbitMQ publisher connected successfully (attempt={attempt})")
                    return  # Success
            except Exception as e:
                last_error = e
                logger.warning(f"RabbitMQ publisher connection attempt {attempt}/{self.max_retries} failed: {e}")
                if attempt < self.max_retries:
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)

        # All retries failed
        error_msg = f"Failed to connect to RabbitMQ after {self.max_retries} attempts"
        if last_error:
            error_msg += f": {str(last_error)}"
        raise RabbitMQConnectionError(error_msg)

    # Keep old method name for compatibility
    def _connect(self):
        """Alias for _connect_with_retry"""
        self._connect_with_retry()

    def _reconnect_if_needed(self):
        """v1.7.6: Ensure connection is alive, reconnect if needed"""
        if not self._auto_reconnect:
            return
            
        try:
            if self.connection and not self.connection.is_closed:
                if self.channel and not self.channel.is_closed:
                    return  # Connection is healthy
                else:
                    # Channel closed, reopen it
                    self.channel = self.connection.channel()
                    return
        except Exception:
            pass
        
        self._connected = False
        logger.info("RabbitMQ publisher connection lost, attempting reconnection...")
        
        try:
            self._close_connections()
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"RabbitMQ publisher reconnection failed: {e}")
            raise

    def _close_connections(self):
        """Safely close existing connections"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
        except:
            pass
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
        except:
            pass

    def _do_publish(self, data):
        """Publish to RabbitMQ queue or topic with auto-reconnection"""
        try:
            # v1.7.6: Check and reconnect if needed
            self._reconnect_if_needed()

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
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            self._connected = False
            
            if self._auto_reconnect:
                try:
                    self._reconnect_if_needed()
                    # Retry the publish
                    json_data = json.dumps(data)
                    self.channel.basic_publish(
                        exchange=self.exchange,
                        routing_key=self.routing_key,
                        body=json_data,
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                            content_type='application/json'
                        )
                    )
                    logger.info("Retry publish succeeded after reconnection")
                except Exception as retry_error:
                    logger.error(f"Retry publish failed: {retry_error}")
                    raise
            else:
                raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        self._close_connections()
        self._connected = False
        logger.info(f"RabbitMQ publisher {self.name} stopped")


class RabbitMQDataSubscriber(DataSubscriber):
    """Subscriber for RabbitMQ queues and topics with v1.7.6 connection resilience."""

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

        # v1.7.6: Connection retry configuration
        self.max_retries = config.get('max_retries', 5)
        self.retry_delay = config.get('retry_delay', 3)
        self._auto_reconnect = config.get('auto_reconnect', True)
        self._connected = False
        self._stopping = False
        self.connection = None
        self.channel = None
        
        # v1.7.6: Connect with retry logic
        self._connect_with_retry()

        # Setup queue based on type
        self._setup_queue(config)

        logger.info(f"RabbitMQ subscriber connected to {self.host}:{self.port}, "
                    f"dest_type={self.dest_type}, queue={self.queue_name}")

    def _setup_queue(self, config):
        """Setup queue based on destination type"""
        if self.dest_type == 'topic':
            # For topics, create a temporary queue and bind to topic exchange
            self.exchange = config.get('exchange', 'amq.topic')
            self.queue_name = config.get('queue', f'{self.name}_queue')

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

    def _do_connect(self):
        """Establish connection to RabbitMQ"""
        parameters = _build_rabbitmq_parameters(
            self.host, self.port, self.virtual_host,
            self.username, self.password, self.config)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)
        self._connected = True

    def _connect_with_retry(self):
        """v1.7.6: Establish connection to RabbitMQ with retry logic"""
        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"RabbitMQ subscriber connection attempt {attempt}/{self.max_retries} to {self.host}:{self.port}")
                self._do_connect()
                if self.channel and not self.channel.is_closed:
                    logger.info(f"RabbitMQ subscriber connected successfully (attempt={attempt})")
                    return  # Success
            except Exception as e:
                last_error = e
                logger.warning(f"RabbitMQ subscriber connection attempt {attempt}/{self.max_retries} failed: {e}")
                if attempt < self.max_retries:
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)

        # All retries failed
        error_msg = f"Failed to connect to RabbitMQ after {self.max_retries} attempts"
        if last_error:
            error_msg += f": {str(last_error)}"
        raise RabbitMQConnectionError(error_msg)

    # Keep old method name for compatibility
    def _connect(self):
        """Alias for _connect_with_retry"""
        self._connect_with_retry()

    def _reconnect_if_needed(self):
        """v1.7.6: Ensure connection is alive, reconnect if needed"""
        if not self._auto_reconnect or self._stopping:
            return
            
        try:
            if self.connection and not self.connection.is_closed:
                if self.channel and not self.channel.is_closed:
                    return  # Connection is healthy
        except Exception:
            pass
        
        self._connected = False
        logger.info("RabbitMQ subscriber connection lost, attempting reconnection...")
        
        try:
            self._close_connections()
            self._connect_with_retry()
            self._setup_queue(self.config)
        except Exception as e:
            logger.error(f"RabbitMQ subscriber reconnection failed: {e}")

    def _close_connections(self):
        """Safely close existing connections"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
        except:
            pass
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
        except:
            pass

    def _do_subscribe(self):
        """Subscribe from RabbitMQ with auto-reconnection"""
        try:
            # v1.7.6: Check and reconnect if needed
            self._reconnect_if_needed()

            # Get a message with basic_get (polling mode)
            method_frame, properties, body = self.channel.basic_get(
                queue=self.queue_name,
                auto_ack=True
            )

            if method_frame:
                # v1.7.2: Use smart deserializer for non-JSON message handling
                data = smart_deserialize(body, f"rabbitmq:{self.name}")
                logger.debug(f"Received message from RabbitMQ {self.dest_type}/{self.dest_name}")
                return data
            else:
                # No messages available
                return None

        except (AMQPConnectionError, AMQPChannelError) as e:
            logger.error(f"RabbitMQ connection error: {str(e)}")
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            self._connected = False
            
            if self._auto_reconnect and not self._stopping:
                try:
                    self._reconnect_if_needed()
                except:
                    pass
            time.sleep(1)
            return None

        except Exception as e:
            # v1.7.2 Policy: Full stack trace for all exceptions
            logger.error(f"Error subscribing from RabbitMQ: {str(e)}")
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            self._connected = False
            return None

    def stop(self):
        """Stop the subscriber"""
        self._stopping = True
        super().stop()
        self._close_connections()
        self._connected = False
        logger.info(f"RabbitMQ subscriber {self.name} stopped")