#!/usr/bin/env python
"""
Test script to demonstrate in-memory pipeline functionality
"""

import json
import time
import logging
from core.dag.compute_graph import ComputeGraph
from core.pubsub.inmemorypubsub import InMemoryPubSub

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def main():
    """Test in-memory pipeline"""

    # Configuration for a simple in-memory pipeline
    config = {
        "name": "test_inmemory_pipeline",
        "start_time": None,
        "end_time": None,
        "subscribers": [
            {
                "name": "test_input_subscriber",
                "config": {
                    "source": "mem://queue/test_input",
                    "max_depth": 100
                }
            }
        ],
        "publishers": [
            {
                "name": "test_output_publisher",
                "config": {
                    "destination": "mem://queue/test_output"
                }
            }
        ],
        "calculators": [
            {
                "name": "test_addition",
                "type": "AdditionCalculator",
                "config": {
                    "arguments": ["value1", "value2"],
                    "output_attribute": "sum"
                }
            }
        ],
        "transformers": [],
        "nodes": [
            {
                "name": "input_node",
                "type": "SubscriptionNode",
                "config": {},
                "subscriber": "test_input_subscriber"
            },
            {
                "name": "calc_node",
                "type": "CalculationNode",
                "config": {},
                "calculator": "test_addition"
            },
            {
                "name": "output_node",
                "type": "PublicationNode",
                "config": {},
                "publishers": ["test_output_publisher"]
            }
        ],
        "edges": [
            {
                "from_node": "input_node",
                "to_node": "calc_node"
            },
            {
                "from_node": "calc_node",
                "to_node": "output_node"
            }
        ]
    }

    # Create and start compute graph
    logger.info("Creating compute graph...")
    graph = ComputeGraph(config)

    logger.info("Starting compute graph...")
    graph.start()

    # Give it time to start
    time.sleep(1)

    # Get in-memory pub/sub instance
    pubsub = InMemoryPubSub()

    # Publish test messages
    logger.info("Publishing test messages...")
    test_messages = [
        {"value1": 10, "value2": 20, "id": 1},
        {"value1": 15, "value2": 25, "id": 2},
        {"value1": 30, "value2": 40, "id": 3}
    ]

    for msg in test_messages:
        pubsub.publish_to_queue("test_input", json.dumps(msg))
        logger.info(f"Published: {msg}")

    # Wait for processing
    logger.info("Waiting for processing...")
    time.sleep(3)

    # Read results
    logger.info("Reading results...")
    results = []
    for _ in range(3):
        result = pubsub.consume_from_queue("test_output", block=False)
        if result:
            result_data = json.loads(result)
            results.append(result_data)
            logger.info(f"Result: {result_data}")

    # Verify results
    logger.info("\n=== Verification ===")
    for i, result in enumerate(results, 1):
        expected_sum = test_messages[i - 1]["value1"] + test_messages[i - 1]["value2"]
        actual_sum = result.get("sum")
        status = "✓" if actual_sum == expected_sum else "✗"
        logger.info(f"{status} Test {i}: Expected sum={expected_sum}, Got sum={actual_sum}")

    # Get graph details
    logger.info("\n=== Graph Details ===")
    details = graph.details()
    logger.info(f"Graph name: {details['name']}")
    logger.info(f"Is running: {details['is_running']}")
    logger.info(f"Is suspended: {details['is_suspended']}")

    for node_name, node_details in details['nodes'].items():
        logger.info(f"\nNode: {node_name}")
        logger.info(f"  Type: {node_details['type']}")
        logger.info(f"  Compute count: {node_details['compute_count']}")
        logger.info(f"  Is dirty: {node_details['isdirty']}")

    # Stop the graph
    logger.info("\n=== Stopping graph ===")
    graph.stop()
    logger.info("Test completed successfully!")


if __name__ == '__main__':
    main()