import logging
import threading
import time
from datetime import datetime
from core.dag.graph_elements import Node

logger = logging.getLogger(__name__)

class PublicationGMTCTradeDiscriminatorNode(Node):
    """Node that publishes gmtc trades to product specific kafka topics"""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.publishers = []
        self._last_published_output = None

    def add_publisher(self, publisher):
        """Add a data publisher"""
        self.publishers.append(publisher)

    def compute(self):
        """Compute and publish if output changed"""
        if not self._isdirty:
            return

        try:
            # Run parent compute logic
            super().compute()

            # Publish if output changed
            if self._output != self._last_published_output:
                for publisher in self.publishers:
                    try:
                        publisher.publish(self._output)
                    except Exception as e:
                        logger.error(f"Error publishing from node {self.name}: {str(e)}")

                self._last_published_output = self._output.copy() if self._output else None

        except Exception as e:
            error_info = {
                'time': datetime.now().isoformat(),
                'error': str(e)
            }
            self._errors.append(error_info)
            logger.error(f"Error in publication node {self.name}: {str(e)}")