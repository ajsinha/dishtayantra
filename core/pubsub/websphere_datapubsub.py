"""
WebSphere MQ (IBM MQ) DataPublisher and DataSubscriber implementation

Supports both queue and topic messaging patterns.
"""

import json
import logging
import time
from datetime import datetime
from core.pubsub.datapubsub import DataPublisher, DataSubscriber

try:
    import pymqi
except ImportError:
    logging.warning("pymqi library not found. Install with: pip install pymqi")
    pymqi = None

logger = logging.getLogger(__name__)


class WebSphereMQDataPublisher(DataPublisher):
    """Publisher for WebSphere MQ (IBM MQ) queues and topics"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        if not pymqi:
            raise ImportError("pymqi library required for WebSphere MQ. Install with: pip install pymqi")

        # Parse destination: websphere://queue/queue_name or websphere://topic/topic_name
        parts = destination.replace('websphere://', '').split('/')
        self.dest_type = parts[0]  # 'queue' or 'topic'
        self.dest_name = parts[1] if len(parts) > 1 else 'default'

        # Connection parameters
        self.queue_manager = config.get('queue_manager', 'QM1')
        self.channel = config.get('channel', 'SYSTEM.DEF.SVRCONN')
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 1414)
        self.conn_info = config.get('conn_info', f'{self.host}({self.port})')

        # Authentication
        self.username = config.get('username', None)
        self.password = config.get('password', None)

        # SSL/TLS settings
        self.use_ssl = config.get('use_ssl', False)
        self.ssl_cipher_spec = config.get('ssl_cipher_spec', None)
        self.ssl_key_repository = config.get('ssl_key_repository', None)

        # Message options
        self.persistence = config.get('persistence', pymqi.CMQC.MQPER_PERSISTENT)

        # Connect to WebSphere MQ
        self.qmgr = None
        self.queue_obj = None
        self.topic_obj = None
        self._connect()

        logger.info(f"WebSphere MQ publisher connected to {self.queue_manager}, "
                    f"dest_type={self.dest_type}, dest_name={self.dest_name}")

    def _connect(self):
        """Establish connection to WebSphere MQ"""
        try:
            # Build connection options
            cd = pymqi.CD()
            cd.ChannelName = self.channel
            cd.ConnectionName = self.conn_info
            cd.ChannelType = pymqi.CMQC.MQCHT_CLNTCONN
            cd.TransportType = pymqi.CMQC.MQXPT_TCP

            # SSL configuration
            if self.use_ssl and self.ssl_cipher_spec:
                sco = pymqi.SCO()
                sco.KeyRepository = self.ssl_key_repository
                cd.SSLCipherSpec = self.ssl_cipher_spec
            else:
                sco = None

            # Connection options
            opts = pymqi.CMQC.MQCNO_HANDLE_SHARE_BLOCK

            # Authentication
            if self.username and self.password:
                opts |= pymqi.CMQC.MQCNO_NONE
                user = self.username
                password = self.password
            else:
                user = ''
                password = ''

            # Connect to queue manager
            self.qmgr = pymqi.QueueManager(None)
            self.qmgr.connect_with_options(
                self.queue_manager,
                user=user,
                password=password,
                cd=cd,
                sco=sco,
                opts=opts
            )

            # Open destination
            if self.dest_type == 'queue':
                self.queue_obj = pymqi.Queue(self.qmgr, self.dest_name,
                                             pymqi.CMQC.MQOO_OUTPUT)
            else:  # topic
                # For topics, we need to create a topic string and open for publish
                od = pymqi.OD()
                od.ObjectType = pymqi.CMQC.MQOT_TOPIC
                od.ObjectString = self.dest_name

                self.topic_obj = pymqi.Topic(self.qmgr, topic_string=self.dest_name)

            logger.info(f"Connected to WebSphere MQ at {self.conn_info}")

        except pymqi.MQMIError as e:
            logger.error(f"Failed to connect to WebSphere MQ: {e.comp}, {e.reason}")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to WebSphere MQ: {str(e)}")
            raise

    def _do_publish(self, data):
        """Publish to WebSphere MQ queue or topic"""
        try:
            # Serialize data to JSON
            json_data = json.dumps(data)

            # Create message descriptor
            md = pymqi.MD()
            md.Format = pymqi.CMQC.MQFMT_STRING
            md.Persistence = self.persistence

            # Create put message options
            pmo = pymqi.PMO()
            pmo.Options = pymqi.CMQC.MQPMO_NO_SYNCPOINT

            # Send message
            if self.dest_type == 'queue':
                self.queue_obj.put(json_data.encode('utf-8'), md, pmo)
            else:  # topic
                self.topic_obj.put(json_data.encode('utf-8'), md, pmo)

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to WebSphere MQ {self.dest_type}/{self.dest_name}")

        except pymqi.MQMIError as e:
            logger.error(f"Error publishing to WebSphere MQ: {e.comp}, {e.reason}")
            # Try to reconnect
            try:
                self._connect()
            except:
                pass
            raise
        except Exception as e:
            logger.error(f"Error publishing to WebSphere MQ: {str(e)}")
            raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        try:
            if self.queue_obj:
                self.queue_obj.close()
            if self.topic_obj:
                self.topic_obj.close()
            if self.qmgr:
                self.qmgr.disconnect()
            logger.info(f"WebSphere MQ publisher {self.name} stopped")
        except Exception as e:
            logger.warning(f"Error closing WebSphere MQ connection: {str(e)}")


class WebSphereMQDataSubscriber(DataSubscriber):
    """Subscriber for WebSphere MQ (IBM MQ) queues and topics"""

    def __init__(self, name, source, config):
        super().__init__(name, source, config)

        if not pymqi:
            raise ImportError("pymqi library required for WebSphere MQ. Install with: pip install pymqi")

        # Parse source: websphere://queue/queue_name or websphere://topic/topic_name
        parts = source.replace('websphere://', '').split('/')
        self.dest_type = parts[0]  # 'queue' or 'topic'
        self.dest_name = parts[1] if len(parts) > 1 else 'default'

        # Connection parameters
        self.queue_manager = config.get('queue_manager', 'QM1')
        self.channel = config.get('channel', 'SYSTEM.DEF.SVRCONN')
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 1414)
        self.conn_info = config.get('conn_info', f'{self.host}({self.port})')

        # Authentication
        self.username = config.get('username', None)
        self.password = config.get('password', None)

        # SSL/TLS settings
        self.use_ssl = config.get('use_ssl', False)
        self.ssl_cipher_spec = config.get('ssl_cipher_spec', None)
        self.ssl_key_repository = config.get('ssl_key_repository', None)

        # Get options
        self.wait_interval = config.get('wait_interval', 100)  # milliseconds

        # For topics, we need to create a managed subscription queue
        self.subscription_name = config.get('subscription_name', f'{name}_sub')
        self.durable = config.get('durable', False)

        # Connect to WebSphere MQ
        self.qmgr = None
        self.queue_obj = None
        self.sub_desc = None
        self._connect()

        logger.info(f"WebSphere MQ subscriber connected to {self.queue_manager}, "
                    f"dest_type={self.dest_type}, dest_name={self.dest_name}")

    def _connect(self):
        """Establish connection to WebSphere MQ"""
        try:
            # Build connection options
            cd = pymqi.CD()
            cd.ChannelName = self.channel
            cd.ConnectionName = self.conn_info
            cd.ChannelType = pymqi.CMQC.MQCHT_CLNTCONN
            cd.TransportType = pymqi.CMQC.MQXPT_TCP

            # SSL configuration
            if self.use_ssl and self.ssl_cipher_spec:
                sco = pymqi.SCO()
                sco.KeyRepository = self.ssl_key_repository
                cd.SSLCipherSpec = self.ssl_cipher_spec
            else:
                sco = None

            # Connection options
            opts = pymqi.CMQC.MQCNO_HANDLE_SHARE_BLOCK

            # Authentication
            if self.username and self.password:
                opts |= pymqi.CMQC.MQCNO_NONE
                user = self.username
                password = self.password
            else:
                user = ''
                password = ''

            # Connect to queue manager
            self.qmgr = pymqi.QueueManager(None)
            self.qmgr.connect_with_options(
                self.queue_manager,
                user=user,
                password=password,
                cd=cd,
                sco=sco,
                opts=opts
            )

            # Open destination
            if self.dest_type == 'queue':
                # Open queue for input
                self.queue_obj = pymqi.Queue(self.qmgr, self.dest_name,
                                             pymqi.CMQC.MQOO_INPUT_SHARED |
                                             pymqi.CMQC.MQOO_FAIL_IF_QUIESCING)
            else:  # topic
                # For topics, create a subscription
                sub = pymqi.SD()
                sub.ObjectString = self.dest_name
                sub.Options = pymqi.CMQC.MQSO_CREATE | \
                              pymqi.CMQC.MQSO_RESUME | \
                              pymqi.CMQC.MQSO_MANAGED

                if self.durable:
                    sub.Options |= pymqi.CMQC.MQSO_DURABLE
                    sub.SubName = self.subscription_name
                else:
                    sub.Options |= pymqi.CMQC.MQSO_NON_DURABLE

                self.queue_obj = pymqi.Queue(self.qmgr)
                self.sub_desc = pymqi.Subscription(self.qmgr)
                self.sub_desc.sub(sub_name=self.subscription_name if self.durable else None,
                                  sub_desc=sub,
                                  queue=self.queue_obj)

            logger.info(f"Connected to WebSphere MQ at {self.conn_info}")

        except pymqi.MQMIError as e:
            logger.error(f"Failed to connect to WebSphere MQ: {e.comp}, {e.reason}")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to WebSphere MQ: {str(e)}")
            raise

    def _do_subscribe(self):
        """Subscribe from WebSphere MQ"""
        try:
            # Reconnect if connection is closed
            if not self.qmgr:
                self._connect()

            # Create message descriptor
            md = pymqi.MD()

            # Create get message options
            gmo = pymqi.GMO()
            gmo.Options = pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_NO_SYNCPOINT
            gmo.WaitInterval = self.wait_interval

            # Get message
            try:
                message = self.queue_obj.get(None, md, gmo)

                # Deserialize JSON data
                data = json.loads(message.decode('utf-8'))
                logger.debug(f"Received message from WebSphere MQ {self.dest_type}/{self.dest_name}")
                return data

            except pymqi.MQMIError as e:
                if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    # No messages available (timeout)
                    return None
                else:
                    raise

        except pymqi.MQMIError as e:
            if e.reason in (pymqi.CMQC.MQRC_CONNECTION_BROKEN,
                            pymqi.CMQC.MQRC_Q_MGR_QUIESCING,
                            pymqi.CMQC.MQRC_CONNECTION_QUIESCING):
                logger.error(f"WebSphere MQ connection error: {e.comp}, {e.reason}")
                self.qmgr = None
                self.queue_obj = None
                time.sleep(1)
            else:
                logger.error(f"WebSphere MQ error: {e.comp}, {e.reason}")
            return None

        except Exception as e:
            logger.error(f"Error subscribing from WebSphere MQ: {str(e)}")
            return None

    def stop(self):
        """Stop the subscriber"""
        super().stop()
        try:
            if self.sub_desc:
                # Close subscription
                self.sub_desc.close()
            if self.queue_obj:
                self.queue_obj.close()
            if self.qmgr:
                self.qmgr.disconnect()
            logger.info(f"WebSphere MQ subscriber {self.name} stopped")
        except Exception as e:
            logger.warning(f"Error closing WebSphere MQ connection: {str(e)}")