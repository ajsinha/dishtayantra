import json
import logging
import time
from datetime import datetime
import stomp
from core.pubsub.datapubsub import DataPublisher, DataSubscriber, DestinationAwarePayload

logger = logging.getLogger(__name__)


class ActiveMQDataPublisher(DataPublisher):
    """Publisher for ActiveMQ queues and topics"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        # Parse destination: activemq://queue/queue_name or activemq://topic/topic_name
        parts = destination.replace('activemq://', '').split('/')
        self.dest_type = parts[0]  # 'queue' or 'topic'
        self.dest_name = parts[1]

        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 61613)

        self.connection = stomp.Connection([(self.host, self.port)])
        self.connection.connect(
            config.get('username', 'admin'),
            config.get('password', 'admin'),
            wait=True
        )

        logger.info(f"ActiveMQ publisher connected to {self.host}:{self.port}")

    def _do_publish(self, data):
        """Publish to ActiveMQ queue or topic"""
        try:
            destination = f'/queue/{self.dest_name}' if self.dest_type == 'queue' else f'/topic/{self.dest_name}'

            local_destination = destination
            local_data = data
            if isinstance(data, DestinationAwarePayload):
                if data.destination is not None:
                    local_destination = data.destination
                    local_data = data.payload

            self.connection.send(local_destination, json.dumps(local_data))

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to ActiveMQ {self.name}/{local_destination}")
        except Exception as e:
            logger.error(f"Error publishing to ActiveMQ: {str(e)}")
            raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        if self.connection and self.connection.is_connected():
            self.connection.disconnect()


class ActiveMQListener(stomp.ConnectionListener):
    """Listener for ActiveMQ messages"""

    def __init__(self, subscriber):
        self.subscriber = subscriber

    def on_message(self, frame):
        try:
            data = json.loads(frame.body)
            self.subscriber._internal_queue.put(data, timeout=1)
            with self.subscriber._lock:
                self.subscriber._last_receive = datetime.now().isoformat()
                self.subscriber._receive_count += 1
        except Exception as e:
            logger.error(f"Error processing ActiveMQ message: {str(e)}")

    def on_error(self, frame):
        logger.error(f"ActiveMQ error: {frame.body}")


class ActiveMQDataSubscriber(DataSubscriber):
    """Subscriber for ActiveMQ queues and topics"""

    def __init__(self, name, source, config):
        # Don't call super().__init__ yet as we need to override the subscription loop
        self.name = name
        self.source = source
        self.config = config
        self.max_depth = config.get('max_depth', 100000)

        import queue
        self._internal_queue = queue.Queue(maxsize=self.max_depth)
        self._stop_event = None
        self._suspend_event = None
        self._last_receive = None
        self._receive_count = 0

        import threading
        self._lock = threading.Lock()

        # Parse source: activemq://queue/queue_name or activemq://topic/topic_name
        parts = source.replace('activemq://', '').split('/')
        self.dest_type = parts[0]
        self.dest_name = parts[1]

        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 61613)

        self.connection = stomp.Connection([(self.host, self.port)])
        self.connection.set_listener('', ActiveMQListener(self))
        self.connection.connect(
            config.get('username', 'admin'),
            config.get('password', 'admin'),
            wait=True
        )

        destination = f'/queue/{self.dest_name}' if self.dest_type == 'queue' else f'/topic/{self.dest_name}'
        self.connection.subscribe(destination=destination, id=1, ack='auto')

        logger.info(f"ActiveMQ subscriber connected to {self.host}:{self.port}, subscribed to {destination}")

    def _do_subscribe(self):
        """Messages are received via listener, this is not used"""
        time.sleep(0.1)
        return None

    def start(self):
        """ActiveMQ uses push model, no need for polling thread"""
        logger.info(f"Subscriber {self.name} is active (push model)")

    def suspend(self):
        """Suspend subscription"""
        logger.info(f"Subscriber {self.name} suspended")

    def resume(self):
        """Resume subscription"""
        logger.info(f"Subscriber {self.name} resumed")

    def stop(self):
        """Stop the subscriber"""
        if self.connection and self.connection.is_connected():
            self.connection.disconnect()
        logger.info(f"Subscriber {self.name} stopped")