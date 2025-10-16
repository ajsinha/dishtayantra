import logging
import time
from datetime import datetime
from core.pubsub.datapubsub import DataPublisher, DataSubscriber
from core.core_utils import instantiate_module

logger = logging.getLogger(__name__)


class CustomDataPublisher(DataPublisher):
    """Publisher that delegates to a custom class"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        self.custom_name = destination.replace('custom://', '')
        delegate_module = config.get('delegate_module')
        delegate_class = config.get('delegate_class')

        if not delegate_module or not delegate_class:
            raise ValueError("delegate_module and delegate_class must be specified in config")

        # Instantiate delegate
        delegate_config = config.get('delegate_config', {})
        self.delegate = instantiate_module(
            delegate_module,
            delegate_class,
            {'name': self.custom_name, 'config': delegate_config}
        )

        logger.info(f"Custom publisher created with delegate {delegate_class}")

    def _do_publish(self, data):
        """Delegate publishing to custom class"""
        try:
            self.delegate.publish(data)

            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1

            logger.debug(f"Published via custom delegate")
        except Exception as e:
            logger.error(f"Error in custom publisher: {str(e)}")
            raise


class CustomDataSubscriber(DataSubscriber):
    """Subscriber that delegates to a custom class"""

    def __init__(self, name, source, config):
        super().__init__(name, source, config)

        self.custom_name = source.replace('custom://', '')
        delegate_module = config.get('delegate_module')
        delegate_class = config.get('delegate_class')
        self.poll_interval = config.get('poll_interval', 1)

        if not delegate_module or not delegate_class:
            raise ValueError("delegate_module and delegate_class must be specified in config")

        # Instantiate delegate
        delegate_config = config.get('delegate_config', {})
        self.delegate = instantiate_module(
            delegate_module,
            delegate_class,
            {'name': self.custom_name, 'config': delegate_config}
        )

        logger.info(f"Custom subscriber created with delegate {delegate_class}")

    def _do_subscribe(self):
        """Delegate subscription to custom class"""
        try:
            data = self.delegate.subscribe()
            if data:
                return data
            else:
                time.sleep(self.poll_interval)
                return None
        except Exception as e:
            logger.error(f"Error in custom subscriber: {str(e)}")
            time.sleep(self.poll_interval)
            return None