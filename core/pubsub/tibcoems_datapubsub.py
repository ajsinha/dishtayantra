"""
TIBCO EMS DataPublisher and DataSubscriber implementation

Supports both queue and topic messaging patterns.
"""

import json
import logging
import time
from datetime import datetime
from core.pubsub.datapubsub import DataPublisher, DataSubscriber

try:
    import tibcoems
except ImportError:
    logging.warning("tibcoems library not found. Install TIBCO EMS Python client")
    tibcoems = None

logger = logging.getLogger(__name__)


class TibcoEMSDataPublisher(DataPublisher):
    """Publisher for TIBCO EMS queues and topics"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        if not tibcoems:
            raise ImportError("tibcoems library required for TIBCO EMS. Install TIBCO EMS Python client")

        # Parse destination: tibcoems://queue/queue_name or tibcoems://topic/topic_name
        parts = destination.replace('tibcoems://', '').split('/')
        self.dest_type = parts[0]  # 'queue' or 'topic'
        self.dest_name = parts[1] if len(parts) > 1 else 'default'

        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 7222)
        self.username = config.get('username', 'admin')
        self.password = config.get('password', 'admin')

        # Connection URL for TIBCO EMS
        self.server_url = config.get('server_url', f'tcp://{self.host}:{self.port}')

        # SSL settings
        self.use_ssl = config.get('use_ssl', False)
        if self.use_ssl:
            self.server_url = config.get('server_url', f'ssl://{self.host}:{self.port}')

        # Connect to TIBCO EMS
        self.connection = None
        self.session = None
        self.producer = None
        self.destination_obj = None
        self._connect()

        logger.info(f"TIBCO EMS publisher connected to {self.server_url}, "
                    f"dest_type={self.dest_type}, dest_name={self.dest_name}")

    def _connect(self):
        """Establish connection to TIBCO EMS"""
        try:
            # Create connection factory
            factory = tibcoems.ConnectionFactory()
            factory.setServerUrl(self.server_url)

            # Create connection
            self.connection = factory.createConnection(self.username, self.password)

            # Create session
            self.session = self.connection.createSession(
                False,  # Not transacted
                tibcoems.Session.AUTO_ACKNOWLEDGE
            )

            # Create destination
            if self.dest_type == 'queue':
                self.destination_obj = self.session.createQueue(self.dest_name)
            else:  # topic
                self.destination_obj = self.session.createTopic(self.dest_name)

            # Create producer
            self.producer = self.session.createProducer(self.destination_obj)

            # Start connection
            self.connection.start()

            logger.info(f"Connected to TIBCO EMS at {self.server_url}")

        except Exception as e:
            logger.error(f"Failed to connect to TIBCO EMS: {str(e)}")
            raise

    def _do_publish(self, data):
        """Publish to TIBCO EMS queue or topic"""
        try:
            # Serialize data to JSON
            json_data = json.dumps(data)

            # Create text message
            message = self.session.createTextMessage(json_data)

            # Send message
            self.producer.send(message)

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to TIBCO EMS {self.dest_type}/{self.dest_name}")

        except Exception as e:
            logger.error(f"Error publishing to TIBCO EMS: {str(e)}")
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
            if self.producer:
                self.producer.close()
            if self.session:
                self.session.close()
            if self.connection:
                self.connection.close()
            logger.info(f"TIBCO EMS publisher {self.name} stopped")
        except Exception as e:
            logger.warning(f"Error closing TIBCO EMS connection: {str(e)}")


class TibcoEMSDataSubscriber(DataSubscriber):
    """Subscriber for TIBCO EMS queues and topics"""

    def __init__(self, name, source, config):
        super().__init__(name, source, config)

        if not tibcoems:
            raise ImportError("tibcoems library required for TIBCO EMS. Install TIBCO EMS Python client")

        # Parse source: tibcoems://queue/queue_name or tibcoems://topic/topic_name
        parts = source.replace('tibcoems://', '').split('/')
        self.dest_type = parts[0]  # 'queue' or 'topic'
        self.dest_name = parts[1] if len(parts) > 1 else 'default'

        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 7222)
        self.username = config.get('username', 'admin')
        self.password = config.get('password', 'admin')

        # Connection URL for TIBCO EMS
        self.server_url = config.get('server_url', f'tcp://{self.host}:{self.port}')

        # SSL settings
        self.use_ssl = config.get('use_ssl', False)
        if self.use_ssl:
            self.server_url = config.get('server_url', f'ssl://{self.host}:{self.port}')

        # Message selector for filtering
        self.message_selector = config.get('message_selector', None)

        # Durable subscriber settings (for topics only)
        self.durable = config.get('durable', False)
        self.client_id = config.get('client_id', f'{name}_client')
        self.durable_name = config.get('durable_name', f'{name}_durable')

        # Connect to TIBCO EMS
        self.connection = None
        self.session = None
        self.consumer = None
        self.destination_obj = None
        self._connect()

        logger.info(f"TIBCO EMS subscriber connected to {self.server_url}, "
                    f"dest_type={self.dest_type}, dest_name={self.dest_name}")

    def _connect(self):
        """Establish connection to TIBCO EMS"""
        try:
            # Create connection factory
            factory = tibcoems.ConnectionFactory()
            factory.setServerUrl(self.server_url)

            # Create connection
            self.connection = factory.createConnection(self.username, self.password)

            # Set client ID if using durable subscriber
            if self.durable and self.dest_type == 'topic':
                self.connection.setClientID(self.client_id)

            # Create session
            self.session = self.connection.createSession(
                False,  # Not transacted
                tibcoems.Session.AUTO_ACKNOWLEDGE
            )

            # Create destination
            if self.dest_type == 'queue':
                self.destination_obj = self.session.createQueue(self.dest_name)
                # Create consumer for queue
                if self.message_selector:
                    self.consumer = self.session.createConsumer(
                        self.destination_obj,
                        self.message_selector
                    )
                else:
                    self.consumer = self.session.createConsumer(self.destination_obj)
            else:  # topic
                self.destination_obj = self.session.createTopic(self.dest_name)
                # Create consumer for topic (durable or non-durable)
                if self.durable:
                    if self.message_selector:
                        self.consumer = self.session.createDurableSubscriber(
                            self.destination_obj,
                            self.durable_name,
                            self.message_selector,
                            False  # noLocal
                        )
                    else:
                        self.consumer = self.session.createDurableSubscriber(
                            self.destination_obj,
                            self.durable_name
                        )
                else:
                    if self.message_selector:
                        self.consumer = self.session.createConsumer(
                            self.destination_obj,
                            self.message_selector
                        )
                    else:
                        self.consumer = self.session.createConsumer(self.destination_obj)

            # Start connection
            self.connection.start()

            logger.info(f"Connected to TIBCO EMS at {self.server_url}")

        except Exception as e:
            logger.error(f"Failed to connect to TIBCO EMS: {str(e)}")
            raise

    def _do_subscribe(self):
        """Subscribe from TIBCO EMS"""
        try:
            # Reconnect if connection is closed
            if not self.connection:
                self._connect()

            # Receive message with timeout (100ms)
            message = self.consumer.receive(100)

            if message:
                # Check if it's a text message
                if isinstance(message, tibcoems.TextMessage):
                    text = message.getText()
                    # Deserialize JSON data
                    data = json.loads(text)
                    logger.debug(f"Received message from TIBCO EMS {self.dest_type}/{self.dest_name}")
                    return data
                else:
                    logger.warning(f"Received non-text message, skipping")
                    return None
            else:
                # No messages available
                return None

        except tibcoems.EMSException as e:
            logger.error(f"TIBCO EMS error: {str(e)}")
            self.connection = None
            self.session = None
            self.consumer = None
            time.sleep(1)
            return None

        except Exception as e:
            logger.error(f"Error subscribing from TIBCO EMS: {str(e)}")
            return None

    def stop(self):
        """Stop the subscriber"""
        super().stop()
        try:
            if self.consumer:
                self.consumer.close()
            if self.session:
                # Unsubscribe durable subscriber if needed
                if self.durable and self.dest_type == 'topic':
                    try:
                        self.session.unsubscribe(self.durable_name)
                    except:
                        pass
                self.session.close()
            if self.connection:
                self.connection.close()
            logger.info(f"TIBCO EMS subscriber {self.name} stopped")
        except Exception as e:
            logger.warning(f"Error closing TIBCO EMS connection: {str(e)}")