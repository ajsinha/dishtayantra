import logging
import time
import threading
from queue import Queue, Empty
from typing import Dict, Optional, Any, List, Callable, Tuple, Union
from dataclasses import dataclass
from enum import Enum
import struct

# IBM MQ Python client imports
import pymqi
from pymqi import QueueManager, Queue as MQQueue, PCFExecute
from pymqi import MQMIError, MQMIWarning
from pymqi.CMQC import *
from pymqi.CMQXC import *

logger = logging.getLogger(__name__)


class PersistenceMode(Enum):
    """MQ Persistence Modes."""
    NOT_PERSISTENT = MQPER_NOT_PERSISTENT
    PERSISTENT = MQPER_PERSISTENT
    PERSISTENCE_AS_Q_DEF = MQPER_PERSISTENCE_AS_Q_DEF


class MessageType(Enum):
    """MQ Message Types."""
    DATAGRAM = MQMT_DATAGRAM
    REQUEST = MQMT_REQUEST
    REPLY = MQMT_REPLY
    REPORT = MQMT_REPORT


@dataclass
class BufferedMessage:
    """Represents a buffered MQ message."""
    queue_name: str
    message_body: Union[str, bytes]
    message_descriptor: Optional[pymqi.MD]
    put_options: Optional[pymqi.PMO]
    get_options: Optional[pymqi.GMO]
    correlation_id: Optional[bytes]
    message_id: Optional[bytes]
    reply_to_queue: Optional[str]
    reply_to_queue_manager: Optional[str]
    expiry: Optional[int]
    priority: Optional[int]
    persistence: Optional[int]
    callback: Optional[Callable]


@dataclass
class OpenQueueInfo:
    """Information about an open queue."""
    queue_name: str
    open_options: int
    dynamic_queue_name: Optional[str]
    alternate_user_id: Optional[str]


class ResilientQueue:
    """A resilient wrapper around pymqi.Queue."""

    def __init__(self, queue_manager, queue_name: str, open_options: int = None):
        self.queue_manager = queue_manager
        self.queue_name = queue_name
        self.open_options = open_options or (MQOO_INPUT_AS_Q_DEF | MQOO_OUTPUT)
        self._queue = None
        self._is_open = False

    def _ensure_open(self):
        """Ensure the queue is open."""
        if not self._queue or not self._is_open:
            self._open()
        return self._queue

    def _open(self):
        """Open the queue with retry logic."""
        for attempt in range(1, self.queue_manager.reconnect_tries + 1):
            try:
                self._queue = MQQueue(
                    self.queue_manager._qmgr,
                    self.queue_name,
                    self.open_options
                )
                self._is_open = True
                return

            except MQMIError as e:
                logger.warning(f"Failed to open queue {self.queue_name} (attempt {attempt}): {e}")

                if attempt < self.queue_manager.reconnect_tries:
                    time.sleep(self.queue_manager.reconnect_interval_seconds)
                    self.queue_manager._ensure_connection()
                else:
                    raise

    def put(self, message: Union[str, bytes], md: Optional[pymqi.MD] = None,
            pmo: Optional[pymqi.PMO] = None):
        """Put a message on the queue with retry logic."""
        return self.queue_manager._put_with_retry(self, message, md, pmo)

    def put1(self, message: Union[str, bytes], md: Optional[pymqi.MD] = None,
             pmo: Optional[pymqi.PMO] = None):
        """Put a single message on the queue without opening it permanently."""
        return self.queue_manager.put1(self.queue_name, message, md, pmo)

    def get(self, max_length: Optional[int] = None, md: Optional[pymqi.MD] = None,
            gmo: Optional[pymqi.GMO] = None):
        """Get a message from the queue with retry logic."""
        queue = self._ensure_open()

        for attempt in range(1, self.queue_manager.reconnect_tries + 1):
            try:
                return queue.get(max_length, md, gmo)

            except MQMIError as e:
                if e.reason == MQRC_NO_MSG_AVAILABLE:
                    raise  # No message available is not a connection error

                logger.warning(f"Get failed (attempt {attempt}): {e}")

                if attempt < self.queue_manager.reconnect_tries:
                    time.sleep(self.queue_manager.reconnect_interval_seconds)
                    self.queue_manager._ensure_connection()
                    self._is_open = False
                    queue = self._ensure_open()
                else:
                    raise

    def browse(self, max_length: Optional[int] = None, md: Optional[pymqi.MD] = None,
               gmo: Optional[pymqi.GMO] = None):
        """Browse messages on the queue without removing them."""
        if gmo is None:
            gmo = pymqi.GMO()
        gmo.Options = MQGMO_BROWSE_NEXT
        return self.get(max_length, md, gmo)

    def get_all(self):
        """Get all messages from the queue."""
        messages = []
        while True:
            try:
                msg = self.get()
                messages.append(msg)
            except MQMIError as e:
                if e.reason == MQRC_NO_MSG_AVAILABLE:
                    break
                raise
        return messages

    def close(self):
        """Close the queue."""
        if self._queue and self._is_open:
            try:
                self._queue.close()
            except:
                pass
            self._is_open = False
            self._queue = None

    def inquire(self, attribute: int):
        """Inquire queue attributes."""
        queue = self._ensure_open()
        return queue.inquire(attribute)

    def set(self, attribute: int, value: Any):
        """Set queue attributes."""
        queue = self._ensure_open()
        return queue.set(attribute, value)


class ResilientWebSphereMQQueueManager(QueueManager):
    """
    A resilient IBM MQ (WebSphere MQ) Queue Manager that automatically handles
    reconnection on failures and buffers messages during reconnection to prevent message loss.

    This class extends pymqi.QueueManager to provide automatic reconnection capabilities
    when connection failures occur during initialization, queue operations, or message sending/receiving.
    Messages sent during reconnection attempts are buffered and retried once connection is restored.

    Args:
        queue_manager_name: Name of the queue manager
        channel: Channel name for client connection
        connection_info: Connection info string (host(port))
        user: Username for authentication
        password: Password for authentication
        reconnect_tries: Maximum number of reconnection attempts (default: 10)
        reconnect_interval_seconds: Sleep interval between reconnection attempts (default: 60)
        buffer_max_messages: Maximum messages to buffer during reconnection (default: 10000)
        use_ssl: Whether to use SSL/TLS connection
        ssl_cipher_spec: SSL cipher specification
        key_repo_location: SSL key repository location
        **kwargs: Additional connection parameters
    """

    def __init__(self,
                 queue_manager_name: Optional[str] = None,
                 channel: Optional[str] = None,
                 connection_info: Optional[str] = None,
                 user: Optional[str] = None,
                 password: Optional[str] = None,
                 reconnect_tries: int = 10,
                 reconnect_interval_seconds: int = 60,
                 buffer_max_messages: int = 10000,
                 use_ssl: bool = False,
                 ssl_cipher_spec: Optional[str] = None,
                 key_repo_location: Optional[str] = None,
                 **kwargs):
        """Initialize the ResilientWebSphereMQQueueManager with reconnection and buffering."""

        # Connection parameters
        self.queue_manager_name = queue_manager_name
        self.channel = channel
        self.connection_info = connection_info
        self.user = user
        self.password = password
        self.use_ssl = use_ssl
        self.ssl_cipher_spec = ssl_cipher_spec
        self.key_repo_location = key_repo_location
        self.connection_kwargs = kwargs

        # Reconnection settings
        self.reconnect_tries = reconnect_tries
        self.reconnect_interval_seconds = reconnect_interval_seconds
        self.buffer_max_messages = buffer_max_messages

        # Connection state
        self._qmgr = None
        self._current_retry = 0
        self._connected = False
        self._reconnecting = False
        self._lock = threading.Lock()

        # Queue management
        self._open_queues = {}  # queue_name -> ResilientQueue
        self._queue_info = {}  # queue_name -> OpenQueueInfo

        # Topic management
        self._open_topics = {}  # topic_string -> topic_handle
        self._subscriptions = {}  # sub_name -> subscription_info

        # Message buffer for storing messages during reconnection
        self._message_buffer = Queue(maxsize=buffer_max_messages)
        self._failed_messages = []

        # PCF command executor
        self._pcf = None

        # Initialize connection with retry logic
        self._connect_with_retry()

        # Start background thread for processing buffered messages
        self._stop_buffer_processor = threading.Event()
        self._buffer_processor_thread = threading.Thread(
            target=self._process_buffered_messages,
            daemon=True
        )
        self._buffer_processor_thread.start()

    def _build_connection_info(self):
        """Build the connection information dictionary."""
        conn_info = {}

        if self.channel and self.connection_info:
            conn_info['channel'] = self.channel
            conn_info['conn_info'] = self.connection_info

            # Build CD (Channel Definition) if needed
            cd = pymqi.CD()
            cd.ChannelName = self.channel.encode()
            cd.ConnectionName = self.connection_info.encode()
            cd.ChannelType = MQCHT_CLNTCONN
            cd.TransportType = MQXPT_TCP

            if self.use_ssl and self.ssl_cipher_spec:
                cd.SSLCipherSpec = self.ssl_cipher_spec.encode()

            conn_info['cd'] = cd

            # Build SCO (SSL Configuration Options) if using SSL
            if self.use_ssl:
                sco = pymqi.SCO()
                sco.KeyRepository = self.key_repo_location.encode() if self.key_repo_location else b''
                conn_info['sco'] = sco

        if self.user and self.password:
            conn_info['user'] = self.user
            conn_info['password'] = self.password

        # Add any additional connection parameters
        conn_info.update(self.connection_kwargs)

        return conn_info

    def _connect_with_retry(self):
        """Establish connection to WebSphere MQ with retry logic."""
        last_exception = None

        for attempt in range(1, self.reconnect_tries + 1):
            try:
                logger.info(f"Attempting to connect to MQ Queue Manager (attempt {attempt}/{self.reconnect_tries})")

                # Build connection info
                conn_info = self._build_connection_info()

                # Connect to queue manager
                if conn_info:
                    # Client connection
                    super().__init__(None)
                    self.connect_tcp_client(
                        self.queue_manager_name or '',
                        cd=conn_info.get('cd'),
                        channel=conn_info.get('channel'),
                        conn_info=conn_info.get('conn_info'),
                        user=conn_info.get('user'),
                        password=conn_info.get('password'),
                        sco=conn_info.get('sco')
                    )
                else:
                    # Local connection
                    super().__init__(self.queue_manager_name)

                self._qmgr = self
                self._connected = True
                self._reconnecting = False
                self._current_retry = 0

                # Initialize PCF executor
                self._pcf = PCFExecute(self)

                logger.info("Successfully connected to MQ Queue Manager")

                # Restore open queues
                self._restore_queues()

                return

            except (MQMIError, Exception) as e:
                last_exception = e
                logger.warning(f"Failed to connect to MQ (attempt {attempt}/{self.reconnect_tries}): {e}")

                if attempt < self.reconnect_tries:
                    logger.info(f"Retrying in {self.reconnect_interval_seconds} seconds...")
                    time.sleep(self.reconnect_interval_seconds)
                else:
                    logger.error(f"Failed to connect to MQ after {self.reconnect_tries} attempts")
                    raise last_exception

    def _ensure_connection(self):
        """Ensure MQ connection is active, reconnect if necessary."""
        try:
            # Test connection by inquiring queue manager status
            if self._connected:
                self.inquire(MQCA_Q_MGR_NAME)
                return True
        except:
            pass

        logger.warning("Connection lost, attempting to reconnect...")
        self._reconnect()
        return self._connected

    def _reconnect(self):
        """Attempt to reconnect to WebSphere MQ."""
        with self._lock:
            if self._reconnecting:
                return
            self._reconnecting = True

        logger.info("Starting reconnection process...")

        try:
            # Reset connection state
            self._connected = False

            # Disconnect if connected
            if self._qmgr:
                try:
                    self.disconnect()
                except:
                    pass

            # Reconnect with retry logic
            self._connect_with_retry()

            # Flush buffered messages
            self._flush_buffer()

        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            raise
        finally:
            with self._lock:
                self._reconnecting = False

    def _restore_queues(self):
        """Restore all open queues after reconnection."""
        logger.info(f"Restoring {len(self._queue_info)} open queues")

        for queue_name, queue_info in self._queue_info.items():
            try:
                # Re-open the queue
                resilient_queue = ResilientQueue(self, queue_name, queue_info.open_options)
                resilient_queue._open()
                self._open_queues[queue_name] = resilient_queue
                logger.debug(f"Restored queue: {queue_name}")
            except Exception as e:
                logger.error(f"Failed to restore queue {queue_name}: {e}")

    def open_queue(self, queue_name: str, open_options: Optional[int] = None,
                   dynamic_queue_name: Optional[str] = None,
                   alternate_user_id: Optional[str] = None) -> ResilientQueue:
        """Open a queue with resilience."""
        if queue_name in self._open_queues:
            return self._open_queues[queue_name]

        open_opts = open_options or (MQOO_INPUT_AS_Q_DEF | MQOO_OUTPUT)

        # Store queue info for restoration
        self._queue_info[queue_name] = OpenQueueInfo(
            queue_name=queue_name,
            open_options=open_opts,
            dynamic_queue_name=dynamic_queue_name,
            alternate_user_id=alternate_user_id
        )

        # Create and open resilient queue
        resilient_queue = ResilientQueue(self, queue_name, open_opts)
        resilient_queue._open()
        self._open_queues[queue_name] = resilient_queue

        return resilient_queue

    def put1(self, queue_name: str, message: Union[str, bytes],
             md: Optional[pymqi.MD] = None, pmo: Optional[pymqi.PMO] = None):
        """Put a single message to a queue without opening it permanently."""
        # Buffer message if reconnecting
        if self._reconnecting or not self._connected:
            logger.info(f"Connection unavailable, buffering message for queue: {queue_name}")
            self._buffer_message(queue_name, message, md, pmo)
            return

        # Try to put with retry logic
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                super().put1(queue_name, message, md, pmo)
                self._current_retry = 0
                return

            except MQMIError as e:
                logger.warning(f"Put1 failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                # Buffer message on first failure
                if attempt == 1:
                    self._buffer_message(queue_name, message, md, pmo)

                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                    self._ensure_connection()
                else:
                    logger.error(f"Put1 failed after {self.reconnect_tries} attempts")
                    raise

    def _put_with_retry(self, resilient_queue: ResilientQueue, message: Union[str, bytes],
                        md: Optional[pymqi.MD], pmo: Optional[pymqi.PMO]):
        """Put a message with automatic buffering on failure."""
        # Buffer message if reconnecting
        if self._reconnecting or not self._connected:
            logger.info(f"Connection unavailable, buffering message for queue: {resilient_queue.queue_name}")
            self._buffer_message(resilient_queue.queue_name, message, md, pmo)
            return

        # Try to put with retry logic
        queue = resilient_queue._ensure_open()

        for attempt in range(1, self.reconnect_tries + 1):
            try:
                queue.put(message, md, pmo)
                self._current_retry = 0
                return

            except MQMIError as e:
                logger.warning(f"Put failed (attempt {attempt}/{self.reconnect_tries}): {e}")

                # Buffer message on first failure
                if attempt == 1:
                    self._buffer_message(resilient_queue.queue_name, message, md, pmo)

                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                    self._ensure_connection()
                    resilient_queue._is_open = False
                    queue = resilient_queue._ensure_open()
                else:
                    logger.error(f"Put failed after {self.reconnect_tries} attempts")
                    raise

    def _buffer_message(self, queue_name: str, message: Union[str, bytes],
                        md: Optional[pymqi.MD], pmo: Optional[pymqi.PMO]):
        """Buffer a message for later sending when connection is restored."""
        try:
            # Create copies of MD and PMO to avoid reference issues
            md_copy = None
            pmo_copy = None

            if md:
                md_copy = pymqi.MD()
                md_copy.Format = md.Format
                md_copy.Persistence = md.Persistence
                md_copy.Priority = md.Priority
                md_copy.Expiry = md.Expiry
                md_copy.CorrelId = md.CorrelId
                md_copy.MsgId = md.MsgId
                md_copy.ReplyToQ = md.ReplyToQ
                md_copy.ReplyToQMgr = md.ReplyToQMgr
                md_copy.MsgType = md.MsgType

            if pmo:
                pmo_copy = pymqi.PMO()
                pmo_copy.Options = pmo.Options
                pmo_copy.Context = pmo.Context

            buffered_msg = BufferedMessage(
                queue_name=queue_name,
                message_body=message,
                message_descriptor=md_copy,
                put_options=pmo_copy,
                get_options=None,
                correlation_id=md_copy.CorrelId if md_copy else None,
                message_id=md_copy.MsgId if md_copy else None,
                reply_to_queue=md_copy.ReplyToQ.decode().strip() if md_copy and md_copy.ReplyToQ else None,
                reply_to_queue_manager=md_copy.ReplyToQMgr.decode().strip() if md_copy and md_copy.ReplyToQMgr else None,
                expiry=md_copy.Expiry if md_copy else None,
                priority=md_copy.Priority if md_copy else None,
                persistence=md_copy.Persistence if md_copy else None,
                callback=None
            )

            if self._message_buffer.full():
                # Remove oldest message to make room
                old_msg = self._message_buffer.get_nowait()
                logger.warning(f"Buffer full, dropping oldest message for queue: {old_msg.queue_name}")

            self._message_buffer.put_nowait(buffered_msg)
            logger.debug(f"Buffered message for queue: {queue_name}, buffer size: {self._message_buffer.qsize()}")

        except Exception as e:
            logger.error(f"Failed to buffer message: {e}")

    def _process_buffered_messages(self):
        """Background thread to process buffered messages when connection is restored."""
        while not self._stop_buffer_processor.is_set():
            try:
                if self._connected and not self._reconnecting and not self._message_buffer.empty():
                    try:
                        buffered_msg = self._message_buffer.get(timeout=1)

                        # Send the buffered message
                        self.put1(
                            buffered_msg.queue_name,
                            buffered_msg.message_body,
                            buffered_msg.message_descriptor,
                            buffered_msg.put_options
                        )

                        logger.debug(f"Successfully sent buffered message to queue: {buffered_msg.queue_name}")

                    except Empty:
                        continue
                    except Exception as e:
                        logger.error(f"Failed to send buffered message: {e}")
                        # Re-queue the message for retry
                        self._message_buffer.put(buffered_msg)
                        time.sleep(0.1)
                else:
                    time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in buffer processor thread: {e}")
                time.sleep(1)

    def _flush_buffer(self):
        """Flush all buffered messages after reconnection."""
        logger.info(f"Flushing {self._message_buffer.qsize()} buffered messages")

        flushed_count = 0
        failed_count = 0

        while not self._message_buffer.empty():
            try:
                buffered_msg = self._message_buffer.get_nowait()

                self.put1(
                    buffered_msg.queue_name,
                    buffered_msg.message_body,
                    buffered_msg.message_descriptor,
                    buffered_msg.put_options
                )

                flushed_count += 1

            except Empty:
                break
            except Exception as e:
                logger.error(f"Failed to flush buffered message: {e}")
                self._failed_messages.append(buffered_msg)
                failed_count += 1

        logger.info(f"Flushed {flushed_count} messages, {failed_count} failed")

    def begin(self):
        """Begin a global transaction."""
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                super().begin()
                return
            except MQMIError as e:
                logger.warning(f"Begin transaction failed (attempt {attempt}): {e}")
                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                    self._ensure_connection()
                else:
                    raise

    def commit(self):
        """Commit the current transaction."""
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                super().commit()
                return
            except MQMIError as e:
                logger.warning(f"Commit failed (attempt {attempt}): {e}")
                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                    self._ensure_connection()
                else:
                    raise

    def backout(self):
        """Backout (rollback) the current transaction."""
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                super().backout()
                return
            except MQMIError as e:
                logger.warning(f"Backout failed (attempt {attempt}): {e}")
                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                    self._ensure_connection()
                else:
                    raise

    def get_handle(self):
        """Get the queue manager handle."""
        return self._qmgr.get_handle() if self._qmgr else None

    def inquire(self, attribute: int):
        """Inquire queue manager attributes."""
        for attempt in range(1, self.reconnect_tries + 1):
            try:
                return super().inquire(attribute)
            except MQMIError as e:
                logger.warning(f"Inquire failed (attempt {attempt}): {e}")
                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                    self._ensure_connection()
                else:
                    raise

    def pcf_execute(self, command: int, args: Optional[List] = None):
        """Execute a PCF command with retry logic."""
        if not self._pcf:
            self._pcf = PCFExecute(self)

        for attempt in range(1, self.reconnect_tries + 1):
            try:
                return self._pcf(command, args or [])
            except MQMIError as e:
                logger.warning(f"PCF command failed (attempt {attempt}): {e}")
                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                    self._ensure_connection()
                    self._pcf = PCFExecute(self)
                else:
                    raise

    def put_batch(self, queue_name: str, messages: List[Tuple[Union[str, bytes], Optional[pymqi.MD]]]) -> List[bool]:
        """
        Put multiple messages in batch with automatic reconnection.

        Args:
            queue_name: Target queue name
            messages: List of (message, md) tuples

        Returns:
            List of boolean values indicating success for each message
        """
        results = []
        queue = self.open_queue(queue_name)

        self.begin()  # Start transaction
        try:
            for message, md in messages:
                try:
                    queue.put(message, md)
                    results.append(True)
                except Exception as e:
                    logger.error(f"Failed to put message in batch: {e}")
                    results.append(False)

            self.commit()  # Commit all messages
        except:
            self.backout()  # Rollback on error
            raise

        return results

    def subscribe(self, topic_string: str, sub_name: Optional[str] = None,
                  sub_desc: Optional[pymqi.SD] = None, sub_queue: Optional[str] = None):
        """Subscribe to a topic with automatic restoration after reconnection."""
        if not sub_desc:
            sub_desc = pymqi.SD()
            sub_desc.Options = MQSO_CREATE | MQSO_RESUME | MQSO_MANAGED | MQSO_DURABLE
            if sub_name:
                sub_desc.SubName = sub_name.encode()

        # Store subscription info for restoration
        self._subscriptions[sub_name or topic_string] = {
            'topic_string': topic_string,
            'sub_name': sub_name,
            'sub_desc': sub_desc,
            'sub_queue': sub_queue
        }

        for attempt in range(1, self.reconnect_tries + 1):
            try:
                sub = self.sub(topic_string, sub_desc)
                return sub
            except MQMIError as e:
                logger.warning(f"Subscribe failed (attempt {attempt}): {e}")
                if attempt < self.reconnect_tries:
                    time.sleep(self.reconnect_interval_seconds)
                    self._ensure_connection()
                else:
                    raise

    def close_all_queues(self):
        """Close all open queues."""
        for queue in self._open_queues.values():
            queue.close()
        self._open_queues.clear()

    def disconnect(self):
        """Disconnect from the queue manager."""
        try:
            # Stop buffer processor
            self._stop_buffer_processor.set()

            # Try to flush remaining messages
            if self._connected and not self._message_buffer.empty():
                logger.info(f"Flushing {self._message_buffer.qsize()} messages before disconnect")
                self._flush_buffer()

            # Log unsent messages
            if not self._message_buffer.empty():
                logger.warning(f"Disconnecting with {self._message_buffer.qsize()} unsent messages")

            if self._failed_messages:
                logger.warning(f"Failed to send {len(self._failed_messages)} messages")

            # Close all open queues
            self.close_all_queues()

            # Disconnect from queue manager
            if self._qmgr:
                super().disconnect()

            self._connected = False
            logger.info("Disconnected from MQ Queue Manager")

        except Exception as e:
            logger.error(f"Error during disconnect: {e}")

    def get_buffer_size(self) -> int:
        """Get the current number of messages in the buffer."""
        return self._message_buffer.qsize()

    def get_failed_messages(self) -> List[BufferedMessage]:
        """Get the list of messages that failed to send."""
        return self._failed_messages.copy()

    def is_connected(self) -> bool:
        """Check if the queue manager is connected."""
        return self._connected


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Example 1: Basic usage as a drop-in replacement for client connection
    qmgr = ResilientWebSphereMQQueueManager(
        queue_manager_name='QM1',
        channel='SYSTEM.DEF.SVRCONN',
        connection_info='localhost(1414)',
        user='mquser',
        password='mqpass',
        reconnect_tries=5,
        reconnect_interval_seconds=30,
        buffer_max_messages=5000
    )

    # Example 2: Sending messages with automatic buffering
    try:
        # Open a queue for output
        queue = qmgr.open_queue('TEST.QUEUE', MQOO_OUTPUT)

        for i in range(100):
            # Create message descriptor
            md = pymqi.MD()
            md.Format = MQFMT_STRING
            md.Persistence = MQPER_PERSISTENT
            md.Priority = 5

            # Messages will be buffered if connection is lost
            queue.put(f'Message {i}', md)
            print(f"Sent message {i}")

            # Check buffer status
            buffer_size = qmgr.get_buffer_size()
            if buffer_size > 0:
                print(f"Messages in buffer: {buffer_size}")

            time.sleep(1)

    except KeyboardInterrupt:
        pass
    finally:
        queue.close()
        qmgr.disconnect()

    # Example 3: Receiving messages with automatic reconnection
    qmgr2 = ResilientWebSphereMQQueueManager(
        queue_manager_name='QM1',
        channel='SYSTEM.DEF.SVRCONN',
        connection_info='localhost(1414)'
    )

    input_queue = qmgr2.open_queue('TEST.QUEUE', MQOO_INPUT_AS_Q_DEF)

    print("Receiving messages... Press CTRL+C to stop")
    try:
        while True:
            try:
                # Get message with wait
                gmo = pymqi.GMO()
                gmo.Options = MQGMO_WAIT | MQGMO_FAIL_IF_QUIESCING
                gmo.WaitInterval = 5000  # 5 seconds

                message = input_queue.get(None, None, gmo)
                print(f"Received: {message}")

            except MQMIError as e:
                if e.reason == MQRC_NO_MSG_AVAILABLE:
                    continue
                raise

    except KeyboardInterrupt:
        pass
    finally:
        input_queue.close()
        qmgr2.disconnect()

    # Example 4: Transactional batch processing
    qmgr3 = ResilientWebSphereMQQueueManager(
        queue_manager_name='QM1',
        channel='SYSTEM.DEF.SVRCONN',
        connection_info='localhost(1414)'
    )

    messages = [
        (f'Batch message {i}'.encode(), None)
        for i in range(50)
    ]

    results = qmgr3.put_batch('BATCH.QUEUE', messages)
    print(f"Successfully sent {sum(results)} out of {len(results)} messages")

    qmgr3.disconnect()

    # Example 5: Request-Reply pattern with correlation ID
    qmgr4 = ResilientWebSphereMQQueueManager(
        queue_manager_name='QM1',
        channel='SYSTEM.DEF.SVRCONN',
        connection_info='localhost(1414)'
    )

    # Send request
    request_queue = qmgr4.open_queue('REQUEST.QUEUE', MQOO_OUTPUT)
    reply_queue = qmgr4.open_queue('REPLY.QUEUE', MQOO_INPUT_AS_Q_DEF)

    # Prepare request message
    request_md = pymqi.MD()
    request_md.ReplyToQ = b'REPLY.QUEUE'
    request_md.MsgType = MQMT_REQUEST
    request_md.Format = MQFMT_STRING
    request_md.Persistence = MQPER_NOT_PERSISTENT

    request_queue.put('What is the status?', request_md)
    correlation_id = request_md.MsgId

    print(f"Sent request with MsgId: {correlation_id.hex()}")

    # Wait for reply
    reply_md = pymqi.MD()
    reply_md.CorrelId = correlation_id

    reply_gmo = pymqi.GMO()
    reply_gmo.Options = MQGMO_WAIT | MQGMO_FAIL_IF_QUIESCING
    reply_gmo.WaitInterval = 10000  # 10 seconds
    reply_gmo.MatchOptions = MQMO_MATCH_CORREL_ID

    try:
        reply = reply_queue.get(None, reply_md, reply_gmo)
        print(f"Received reply: {reply}")
    except MQMIError as e:
        if e.reason == MQRC_NO_MSG_AVAILABLE:
            print("No reply received within timeout")

    request_queue.close()
    reply_queue.close()
    qmgr4.disconnect()

    # Example 6: Using PCF commands for administration
    qmgr5 = ResilientWebSphereMQQueueManager(
        queue_manager_name='QM1',
        channel='SYSTEM.DEF.SVRCONN',
        connection_info='localhost(1414)'
    )

    # Inquire queue status using PCF
    try:
        from pymqi.CMQCFC import *

        args = [
            {MQCA_Q_NAME: b'SYSTEM.DEFAULT.LOCAL.QUEUE'},
            {MQIA_Q_TYPE: MQQT_LOCAL}
        ]

        response = qmgr5.pcf_execute(MQCMD_INQUIRE_Q, args)
        for queue_info in response:
            print(f"Queue: {queue_info.get(MQCA_Q_NAME, b'').decode().strip()}")
            print(f"Current depth: {queue_info.get(MQIA_CURRENT_Q_DEPTH, 0)}")

    except Exception as e:
        print(f"PCF command failed: {e}")

    qmgr5.disconnect()