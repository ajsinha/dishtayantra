"""
gRPC DataPublisher and DataSubscriber implementation

v1.7.6: Enhanced connection retry and auto-recovery support.

This implementation assumes a gRPC service defined as:

syntax = "proto3";

service PubSubService {
    rpc Publish(PublishRequest) returns (PublishResponse);
    rpc Subscribe(SubscribeRequest) returns (stream Message);
}

message PublishRequest {
    string topic = 1;
    bytes data = 2;
}

message PublishResponse {
    bool success = 1;
    string message = 2;
}

message SubscribeRequest {
    string topic = 1;
    string subscriber_id = 2;
}

message Message {
    string topic = 1;
    bytes data = 2;
    int64 timestamp = 3;
}
"""

import json
import logging
import time
import traceback
import grpc
from datetime import datetime
from core.pubsub.datapubsub import DataPublisher, DataSubscriber, smart_deserialize
import queue

# Import generated gRPC stubs (these would be generated from the proto file)
try:
    from core.pubsub.grpc_generated import pubsub_pb2, pubsub_pb2_grpc
except ImportError:
    # Fallback for when proto files aren't generated yet
    logging.warning("gRPC stubs not found. Please generate from proto files.")
    pubsub_pb2 = None
    pubsub_pb2_grpc = None

logger = logging.getLogger(__name__)


def _create_grpc_channel(host_port, config):
    """Create a gRPC channel, secure when ``use_ssl`` is set.

    With ``use_ssl`` and no certs supplied, the system trust roots are used.
    Optional keys enable custom CAs and mutual TLS (paths to PEM files):

        ssl_root_certs : root CA(s) to verify the server
        ssl_certfile   : client certificate (mutual TLS)
        ssl_keyfile    : client private key (mutual TLS)
    """
    if not config.get('use_ssl', False):
        return grpc.insecure_channel(host_port)

    def _read(path):
        if not path:
            return None
        with open(path, 'rb') as fh:
            return fh.read()

    credentials = grpc.ssl_channel_credentials(
        root_certificates=_read(config.get('ssl_root_certs')),
        private_key=_read(config.get('ssl_keyfile')),
        certificate_chain=_read(config.get('ssl_certfile')),
    )
    return grpc.secure_channel(host_port, credentials)



class GRPCDataPublisher(DataPublisher):
    """Publisher for gRPC streams with v1.7.6 connection resilience."""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        # Parse destination: grpc://host:port/topic
        self.topic = destination.split('/')[-1]
        host_port = destination.replace('grpc://', '').split('/')[0]

        # gRPC connection settings
        self.host_port = config.get('host_port', host_port)
        self.use_ssl = config.get('use_ssl', False)
        
        # v1.7.6: Connection retry configuration
        self._max_retries = config.get('max_retries', 5)
        self._retry_delay = config.get('retry_delay', 3)
        self._auto_reconnect = config.get('auto_reconnect', True)
        self._connected = False

        # Set gRPC options
        self.timeout = config.get('timeout', 30)

        self.channel = None
        self.stub = None

        # v1.7.6: Connect with retry logic
        self._connect_with_retry()

        logger.info(f"gRPC publisher created for topic {self.topic} at {self.host_port}")

    def _create_channel(self):
        """Create gRPC channel (secure when use_ssl is set)."""
        return _create_grpc_channel(self.host_port, self.config)

    def _connect_with_retry(self):
        """v1.7.6: Establish gRPC connection with retry logic."""
        last_error = None

        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(f"gRPC publisher connection attempt {attempt}/{self._max_retries} to {self.host_port}")

                self.channel = self._create_channel()

                if pubsub_pb2_grpc:
                    self.stub = pubsub_pb2_grpc.PubSubServiceStub(self.channel)
                else:
                    self.stub = None

                # Test connection by waiting for channel to be ready
                grpc.channel_ready_future(self.channel).result(timeout=5)

                self._connected = True
                logger.info(f"gRPC publisher connected successfully (attempt={attempt})")
                return

            except Exception as e:
                last_error = e
                logger.warning(f"gRPC publisher connection attempt {attempt}/{self._max_retries} failed: {e}")

                if attempt < self._max_retries:
                    logger.info(f"Retrying in {self._retry_delay} seconds...")
                    time.sleep(self._retry_delay)

        logger.error(f"Failed to connect gRPC publisher after {self._max_retries} attempts: {last_error}")
        raise ConnectionError(f"Could not connect to gRPC server {self.host_port}: {last_error}")

    def _reconnect_if_needed(self):
        """v1.7.6: Reconnect if connection is broken."""
        if not self._auto_reconnect:
            return

        try:
            if self._connected and self.channel:
                # Check channel state
                state = self.channel._channel.check_connectivity_state(True)
                if state in [grpc.ChannelConnectivity.READY, grpc.ChannelConnectivity.IDLE]:
                    return  # Connection is healthy
        except:
            pass

        self._connected = False
        logger.info("gRPC publisher connection lost, attempting reconnection...")

        try:
            self._close_connections()
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"gRPC publisher reconnection failed: {e}")
            raise

    def _close_connections(self):
        """Safely close existing connections."""
        try:
            if self.channel:
                self.channel.close()
        except:
            pass
        self.channel = None
        self.stub = None

    def _do_publish(self, data):
        """Publish to gRPC service with auto-reconnection."""
        if not pubsub_pb2:
            logger.error(f"gRPC stubs not available for publisher {self.name}")
            return

        try:
            # v1.7.6: Check and reconnect if needed
            self._reconnect_if_needed()

            if not self.stub:
                logger.error(f"gRPC stub not available for publisher {self.name}")
                return

            # Serialize data to JSON bytes
            json_data = json.dumps(data).encode('utf-8')

            # Create publish request
            request = pubsub_pb2.PublishRequest(
                topic=self.topic,
                data=json_data
            )

            # Send with retry logic
            for attempt in range(self._max_retries):
                try:
                    response = self.stub.Publish(request, timeout=self.timeout)

                    if response.success:
                        with self._lock:
                            self._last_publish = datetime.now().isoformat()
                            self._publish_count += 1
                        logger.debug(f"Published to gRPC topic {self.topic}")
                        return
                    else:
                        logger.warning(f"Publish failed: {response.message}")

                except grpc.RpcError as e:
                    logger.warning(f"gRPC error on attempt {attempt + 1}: {e.code()}")
                    self._connected = False
                    
                    if attempt < self._max_retries - 1:
                        if self._auto_reconnect:
                            try:
                                self._reconnect_if_needed()
                            except:
                                pass
                    else:
                        raise

        except Exception as e:
            logger.error(f"Error publishing to gRPC topic {self.topic}: {str(e)}")
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            self._connected = False
            raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        self._close_connections()
        self._connected = False
        logger.info(f"gRPC channel closed for publisher {self.name}")


class GRPCDataSubscriber(DataSubscriber):
    """Subscriber for gRPC streams with v1.7.6 connection resilience."""

    def __init__(self, name, source, config, given_queue: queue.Queue = None):
        super().__init__(name, source, config, given_queue)

        # Parse source: grpc://host:port/topic
        self.topic = source.split('/')[-1]
        host_port = source.replace('grpc://', '').split('/')[0]

        # gRPC connection settings
        self.host_port = config.get('host_port', host_port)
        self.use_ssl = config.get('use_ssl', False)
        self.subscriber_id = config.get('subscriber_id', f'{name}_sub')

        # v1.7.6: Connection retry configuration
        self._max_retries = config.get('max_retries', 5)
        self._retry_delay = config.get('retry_delay', 3)
        self._auto_reconnect = config.get('auto_reconnect', True)
        self._connected = False
        self._stopping = False

        # Stream handling
        self.channel = None
        self.stub = None
        self.stream = None
        self.reconnect_delay = config.get('reconnect_delay', 5)

        # v1.7.6: Connect with retry logic
        self._connect_with_retry()

        logger.info(f"gRPC subscriber created for topic {self.topic} at {self.host_port}")

    def _create_channel(self):
        """Create gRPC channel (secure when use_ssl is set)."""
        return _create_grpc_channel(self.host_port, self.config)

    def _connect_with_retry(self):
        """v1.7.6: Establish gRPC connection with retry logic."""
        last_error = None

        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(f"gRPC subscriber connection attempt {attempt}/{self._max_retries} to {self.host_port}")

                self.channel = self._create_channel()

                if pubsub_pb2_grpc:
                    self.stub = pubsub_pb2_grpc.PubSubServiceStub(self.channel)
                else:
                    self.stub = None

                # Test connection
                grpc.channel_ready_future(self.channel).result(timeout=5)

                self._connected = True
                logger.info(f"gRPC subscriber connected successfully (attempt={attempt})")
                return

            except Exception as e:
                last_error = e
                logger.warning(f"gRPC subscriber connection attempt {attempt}/{self._max_retries} failed: {e}")

                if attempt < self._max_retries:
                    logger.info(f"Retrying in {self._retry_delay} seconds...")
                    time.sleep(self._retry_delay)

        logger.error(f"Failed to connect gRPC subscriber after {self._max_retries} attempts: {last_error}")
        raise ConnectionError(f"Could not connect to gRPC server {self.host_port}: {last_error}")

    def _reconnect_if_needed(self):
        """v1.7.6: Reconnect if connection is broken."""
        if not self._auto_reconnect or self._stopping:
            return

        if self._connected and self.channel and self.stub:
            try:
                state = self.channel._channel.check_connectivity_state(True)
                if state in [grpc.ChannelConnectivity.READY, grpc.ChannelConnectivity.IDLE]:
                    return
            except:
                pass

        self._connected = False
        self.stream = None
        logger.info("gRPC subscriber connection lost, attempting reconnection...")

        try:
            self._close_connections()
            self._connect_with_retry()
        except Exception as e:
            logger.error(f"gRPC subscriber reconnection failed: {e}")

    def _close_connections(self):
        """Safely close existing connections."""
        try:
            if self.stream:
                self.stream.cancel()
        except:
            pass
        try:
            if self.channel:
                self.channel.close()
        except:
            pass
        self.stream = None
        self.channel = None
        self.stub = None

    def _connect_stream(self):
        """Connect to gRPC stream with auto-reconnection."""
        if not self.stub or not pubsub_pb2:
            logger.error(f"gRPC stubs not available for subscriber {self.name}")
            return None

        try:
            # v1.7.6: Check and reconnect if needed
            self._reconnect_if_needed()

            request = pubsub_pb2.SubscribeRequest(
                topic=self.topic,
                subscriber_id=self.subscriber_id
            )

            self.stream = self.stub.Subscribe(request)
            logger.info(f"Connected to gRPC stream for topic {self.topic}")
            return self.stream

        except grpc.RpcError as e:
            logger.error(f"Failed to connect to gRPC stream: {e.code()}")
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            self._connected = False
            return None

    def _do_subscribe(self):
        """Subscribe from gRPC stream with auto-reconnection."""
        if not pubsub_pb2:
            return None

        try:
            # v1.7.6: Check and reconnect if needed
            self._reconnect_if_needed()

            # Connect to stream if not already connected
            if self.stream is None:
                self.stream = self._connect_stream()
                if self.stream is None:
                    return None

            # Try to get next message from stream
            try:
                message = next(self.stream)

                # v1.7.2: Use smart deserializer for non-JSON message handling
                data = smart_deserialize(message.data, f"grpc:{self.name}")
                logger.debug(f"Received message from gRPC topic {self.topic}")
                return data

            except StopIteration:
                logger.warning(f"gRPC stream ended for topic {self.topic}")
                self.stream = None
                self._connected = False
                
                if self._auto_reconnect and not self._stopping:
                    try:
                        self._reconnect_if_needed()
                    except:
                        pass
                return None

            except grpc.RpcError as e:
                logger.error(f"gRPC stream error: {e.code()}")
                logger.error(f"Full stack trace:\n{traceback.format_exc()}")
                self.stream = None
                self._connected = False
                
                if self._auto_reconnect and not self._stopping:
                    try:
                        self._reconnect_if_needed()
                    except:
                        pass
                return None

        except Exception as e:
            # v1.7.2 Policy: Full stack trace for all exceptions
            logger.error(f"Error subscribing from gRPC topic {self.topic}: {str(e)}")
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            self.stream = None
            self._connected = False
            return None

    def stop(self):
        """Stop the subscriber"""
        self._stopping = True
        super().stop()
        self._close_connections()
        self._connected = False
        logger.info(f"gRPC channel closed for subscriber {self.name}")