import logging
import time
from datetime import datetime
from core.pubsub.datapubsub import DataPublisher, DataSubscriber

logger = logging.getLogger(__name__)


class MetronomeDataPublisher(DataPublisher):
    """Publisher that emits messages at regular intervals"""

    def __init__(self, name, destination, config):
        # Override publish_interval to use metronome interval
        config['publish_interval'] = config.get('interval', 1)
        super().__init__(name, destination, config)

        self.message = config.get('message', 'tick')
        self._start_metronome()

    def _start_metronome(self):
        """Start metronome publishing"""
        import threading
        self._metronome_thread = threading.Thread(target=self._metronome_loop, daemon=True)
        self._metronome_thread.start()

    def _metronome_loop(self):
        """Metronome loop"""
        while not self._stop_event.is_set():
            data = {
                'message': self.message,
                'current_timestamp': datetime.now().strftime('%Y%m%d%H%M%S')
            }
            self.publish(data)
            time.sleep(self.publish_interval)

    def _do_publish(self, data):
        """No-op for metronome, messages are generated internally"""
        with self._lock:
            self._last_publish = datetime.now().isoformat()
            self._publish_count += 1


class MetronomeDataSubscriber(DataSubscriber):
    """Subscriber that generates messages at regular intervals"""

    def __init__(self, name, source, config):
        super().__init__(name, source, config)

        self.message = config.get('message', 'tick')
        self.interval = config.get('interval', 1)
        self.last_emit = time.time()

    def _do_subscribe(self):
        """Generate metronome message"""
        current_time = time.time()

        if current_time - self.last_emit >= self.interval:
            self.last_emit = current_time
            return {
                'message': self.message,
                'current_timestamp': datetime.now().strftime('%Y%m%d%H%M%S')
            }

        time.sleep(0.1)
        return None