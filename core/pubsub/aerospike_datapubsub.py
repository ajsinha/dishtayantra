import json
import logging
from datetime import datetime
import aerospike
from core.pubsub.datapubsub import DataPublisher
import queue

logger = logging.getLogger(__name__)


class AerospikeDataPublisher(DataPublisher):
    """Publisher that writes data to Aerospike"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        # Parse destination: aerospike://namespace/set_name
        parts = destination.replace('aerospike://', '').split('/')
        self.namespace = parts[0]
        self.set_name = parts[1]

        self.hosts = config.get('hosts', [('localhost', 3000)])

        # Configure Aerospike client
        aerospike_config = {
            'hosts': self.hosts
        }

        self.client = aerospike.client(aerospike_config).connect()

        logger.info(f"Aerospike publisher created for {self.namespace}/{self.set_name}")

    def _do_publish(self, data):
        """Write data to Aerospike"""
        try:
            key_value = data.get('__dagserver_key')
            if not key_value:
                logger.error("No __dagserver_key found in data")
                return

            # Create Aerospike key
            key = (self.namespace, self.set_name, key_value)

            # Remove the key from data before storing
            data_copy = data.copy()
            data_copy.pop('__dagserver_key', None)

            # Put record
            self.client.put(key, data_copy)

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to Aerospike key {key_value}")
        except Exception as e:
            logger.error(f"Error publishing to Aerospike: {str(e)}")
            raise

    def stop(self):
        """Stop the publisher"""
        super().stop()
        if self.client:
            self.client.close()