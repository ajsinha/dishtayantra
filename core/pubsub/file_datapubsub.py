import json
import logging
import os
import time
from datetime import datetime
from core.pubsub.datapubsub import DataPublisher, DataSubscriber,DataAwarePayload
import queue
logger = logging.getLogger(__name__)


class FileDataPublisher(DataPublisher):
    """Publisher that appends data to a file"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)
        self.filepath = destination.replace('file://', '')

        # Create directory if it doesn't exist
        directory = os.path.dirname(self.filepath)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)

        logger.info(f"File publisher created for {self.filepath}")

    def _do_publish(self, data):
        """Append data to file"""
        try:
            with open(self.filepath, 'a') as f:
                if hasattr(data, 'to_dict'):
                    to_json = json.dumps(data.to_dict(), separators=(',', ':'))
                    f.write(to_json + '\n')
                else:
                    to_json = json.dumps(data, separators=(',', ':'))
                    f.write( to_json + '\n')

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published to file {self.filepath}")
        except Exception as e:
            logger.error(f"Error writing to file {self.filepath}: {str(e)}")
            raise


class FileDataSubscriber(DataSubscriber):
    """Subscriber that reads data from a file"""

    def __init__(self, name, source, config, given_queue: queue.Queue = None):
        super().__init__(name, source, config, given_queue)
        self.filepath = source.replace('file://', '')
        self.read_interval = config.get('read_interval', 1)
        self.file_handle = None
        self.last_position = 0

        if not os.path.exists(self.filepath):
            logger.warning(f"File {self.filepath} does not exist yet")

        logger.info(f"File subscriber created for {self.filepath}")

    def _do_subscribe(self):
        """Read data from file"""
        try:
            if not os.path.exists(self.filepath):
                time.sleep(self.read_interval)
                return None

            if self.file_handle is None:
                self.file_handle = open(self.filepath, 'r')
                self.file_handle.seek(self.last_position)

            line = self.file_handle.readline()
            if line:
                self.last_position = self.file_handle.tell()
                return json.loads(line.strip())
            else:
                time.sleep(self.read_interval)
                return None
        except Exception as e:
            logger.error(f"Error reading from file {self.filepath}: {str(e)}")
            time.sleep(self.read_interval)
            return None

    def stop(self):
        """Stop the subscriber"""
        super().stop()
        if self.file_handle:
            self.file_handle.close()