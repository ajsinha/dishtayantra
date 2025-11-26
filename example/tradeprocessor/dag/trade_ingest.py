import logging
import threading
import time
from datetime import datetime
from core.dag.node_implementations import CalculationNode
from core.pubsub.datapubsub import DataAwarePayload
from copy import deepcopy

logger = logging.getLogger(__name__)

class TradeDiscriminatorNode(CalculationNode):
    """Node that publishes trades to product specific kafka topics"""

    def __init__(self, name, config: dict):
        super().__init__(name, config)
        self.publishers = config.get("publishers", [])

        self._last_published_output = None


    def __build_event_id(self):
        import uuid
        # Generate a random UUID (version 4)
        random_uuid = uuid.uuid4()
        return str(random_uuid)

    def compute(self):
        """Compute and publish if output changed"""
        if not self.isdirty():
            return

        try:
            # Run parent compute logic
            super().compute()

            # Publish if output changed
            if self._output != self._last_published_output:
                # construct payload
                product_type = self._output ['product_type']
                subproduct = self._output ['subproduct']
                k_topic = f'{product_type}_{subproduct}'.replace(' ', '_').lower()
                cde = {'target_topic': k_topic, 'event_id': self.__build_event_id(), 'processing_agent':self.name}
                payload = deepcopy(self._output)
                d_payload = DataAwarePayload(destination=k_topic, cde=cde, payload=payload)
                for publisher_name in self.publishers:
                    try:
                        logger.debug(f"publishing: {d_payload}")
                        self_graph = self._graph
                        local_publisher = self_graph.get_publiisher_by_name(publisher_name)
                        local_publisher.publish(d_payload)
                        #self_graph.publish(d_payload)
                        #publisher.publish(d_payload)
                    except Exception as e:
                        logger.error(f"Error publishing from node {self.name}: {str(e)}")
                x, p_output = d_payload.get_data_for_publication()
                self._output = p_output
                self._last_published_output = self._output.copy() if self._output else None

        except Exception as e:
            import traceback
            error_info = {
                'time': datetime.now().isoformat(),
                'error': str(e)
            }
            self._errors.append(error_info)
            logger.error(f"Error in publication node {self.name}: {str(e)}")
            traceback.print_exc()