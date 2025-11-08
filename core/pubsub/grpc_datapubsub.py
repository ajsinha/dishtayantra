"""
gRPC DataPublisher and DataSubscriber implementation

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
import grpc
from datetime import datetime
from core.pubsub.datapubsub import DataPublisher, DataSubscriber
import queue
# Import generated gRPC stubs (these would be generated from the proto file)
# For this implementation, we'll assume they exist
try:
    from core.pubsub.grpc_generated import pubsub_pb2, pubsub_pb2_grpc
except ImportError:
    # Fallback for when proto files aren't generated yet
    logging.warning("gRPC stubs not found. Please generate from proto files.")
    pubsub_pb2 = None
    pubsub_pb2_grpc = None

logger = logging.getLogger(__name__)


class GRPCDataPublisher(DataPublisher):
    """Publisher for gRPC streams"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        # Parse destination: grpc://host:port/topic
        self.topic = destination.split('/')[-1]
        host_port = destination.replace('grpc://', '').split('/')[0]

        # gRPC connection settings
        self.host_port = config.get('host_port', host_port)
        self.use_ssl = config.get('use_ssl', False)
        self.max_retries = config.get('max_retries', 3)

        # Create gRPC channel
        if self.use_ssl:
            credentials = grpc.ssl_channel_credentials()
            self.channel = grpc.secure_channel(self.host_port, credentials)
        else:
            self.channel = grpc.insecure_channel(self.host_port)

        # Create stub
        if pubsub_pb2_grpc:
            self.stub = pubsub_pb2_grpc.PubSubServiceStub(self.channel)
        else:
            self.stub = None

        # Set gRPC options
        self.timeout = config.get('timeout', 30)

        logger.info(f"gRPC publisher created for topic {self.topic} at {self.host_port}")

    def _do_publish(self, data):
        """Publish to gRPC service"""
        if not self.stub or not pubsub_pb2:
            logger.error(f"gRPC stubs not available for publisher {self.name}")
            return

        try:
            # Serialize data to JSON bytes
            json_data = json.dumps(data).encode('utf-8')

            # Create publish request
            request = pubsub_pb2.PublishRequest(
                topic=self.topic,
                data=json_data
            )

            # Send with retry logic
            for attempt in range(self.max_retries):
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
                    if attempt == self.max_retries - 1:
                        raise

        except Exception as e:
            logger.error(f"Error publishing to gRPC topic {self.topic}: {str(e)}")
            raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        if self.channel:
            self.channel.close()
            logger.info(f"gRPC channel closed for publisher {self.name}")


class GRPCDataSubscriber(DataSubscriber):
    """Subscriber for gRPC streams"""

    def __init__(self, name, source, config, given_queue: queue.Queue = None):
        super().__init__(name, source, config, given_queue)

        # Parse source: grpc://host:port/topic
        self.topic = source.split('/')[-1]
        host_port = source.replace('grpc://', '').split('/')[0]

        # gRPC connection settings
        self.host_port = config.get('host_port', host_port)
        self.use_ssl = config.get('use_ssl', False)
        self.subscriber_id = config.get('subscriber_id', f'{name}_sub')

        # Create gRPC channel
        if self.use_ssl:
            credentials = grpc.ssl_channel_credentials()
            self.channel = grpc.secure_channel(self.host_port, credentials)
        else:
            self.channel = grpc.insecure_channel(self.host_port)

        # Create stub
        if pubsub_pb2_grpc:
            self.stub = pubsub_pb2_grpc.PubSubServiceStub(self.channel)
        else:
            self.stub = None

        # Stream handling
        self.stream = None
        self.reconnect_delay = config.get('reconnect_delay', 5)

        logger.info(f"gRPC subscriber created for topic {self.topic} at {self.host_port}")

    def _connect_stream(self):
        """Connect to gRPC stream"""
        if not self.stub or not pubsub_pb2:
            logger.error(f"gRPC stubs not available for subscriber {self.name}")
            return None

        try:
            request = pubsub_pb2.SubscribeRequest(
                topic=self.topic,
                subscriber_id=self.subscriber_id
            )

            self.stream = self.stub.Subscribe(request)
            logger.info(f"Connected to gRPC stream for topic {self.topic}")
            return self.stream

        except grpc.RpcError as e:
            logger.error(f"Failed to connect to gRPC stream: {e.code()}")
            return None

    def _do_subscribe(self):
        """Subscribe from gRPC stream"""
        if not self.stub or not pubsub_pb2:
            return None

        try:
            # Connect to stream if not already connected
            if self.stream is None:
                self.stream = self._connect_stream()
                if self.stream is None:
                    return None

            # Try to get next message from stream
            try:
                message = next(self.stream)

                # Deserialize JSON data
                data = json.loads(message.data.decode('utf-8'))
                logger.debug(f"Received message from gRPC topic {self.topic}")
                return data

            except StopIteration:
                logger.warning(f"gRPC stream ended for topic {self.topic}")
                self.stream = None
                return None

            except grpc.RpcError as e:
                logger.error(f"gRPC stream error: {e.code()}")
                self.stream = None
                return None

        except Exception as e:
            logger.error(f"Error subscribing from gRPC topic {self.topic}: {str(e)}")
            self.stream = None
            return None

    def stop(self):
        """Stop the subscriber"""
        super().stop()

        # Cancel the stream
        if self.stream:
            try:
                self.stream.cancel()
            except Exception as e:
                logger.warning(f"Error cancelling stream: {str(e)}")

        # Close the channel
        if self.channel:
            self.channel.close()
            logger.info(f"gRPC channel closed for subscriber {self.name}")