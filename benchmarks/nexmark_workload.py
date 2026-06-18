"""
Nexmark benchmark workload for DishtaYantra (auction/bid stream).

Generates a synthetic bid stream and builds an in-memory linear DAG
(ingest -> Q1 currency convert -> Q2 auction select -> sink) from the
``benchmarks.nexmark_calculators``. Driven by the same harness as the finance
trade-ETL workload, so the two are directly comparable. Representative subset of
Nexmark (Q1 + Q2), not the full suite.
"""

from __future__ import annotations

import random
from typing import Any, Dict

_CHANNELS = ["Google", "Apple", "Amazon", "Baidu", "Facebook"]


def make_bid(i: int, rng: random.Random) -> Dict[str, Any]:
    """Synthetic Nexmark bid keyed by bid id = i."""
    return {
        "trade_id": i,            # harness correlates per message by this key
        "bid_id": i,
        "auction": rng.randint(1000, 1000 + 9999),
        "bidder": rng.randint(1, 50000),
        "price": rng.randint(1, 100_000),   # USD cents
        "channel": rng.choice(_CHANNELS),
    }


def bid_generator(seed: int = 4321):
    rng = random.Random(seed)

    def _gen(i: int) -> Dict[str, Any]:
        return make_bid(i, rng)

    return _gen


def nexmark_linear_config(input_queue: str = "nex_in",
                          output_queue: str = "nex_out") -> Dict[str, Any]:
    """In-memory linear Nexmark DAG: ingest -> q1 -> q2 -> sink."""
    calculators = [
        {"name": "q1", "type": "benchmarks.nexmark_calculators.Q1CurrencyConvertCalculator", "config": {}},
        {"name": "q2", "type": "benchmarks.nexmark_calculators.Q2AuctionFilterCalculator", "config": {}},
    ]
    nodes = [
        {"name": "ingest", "type": "SubscriptionNode", "config": {}, "subscriber": "nex_sub"},
        {"name": "q1_node", "type": "CalculationNode", "config": {}, "calculator": "q1"},
        {"name": "q2_node", "type": "CalculationNode", "config": {}, "calculator": "q2"},
        {"name": "sink", "type": "PublicationNode", "config": {}, "publishers": ["nex_pub"]},
    ]
    order = ["ingest", "q1_node", "q2_node", "sink"]
    edges = [{"from_node": order[i], "to_node": order[i + 1]} for i in range(len(order) - 1)]

    return {
        "name": "bench_nexmark_linear",
        "start_time": None,
        "end_time": None,
        "subscribers": [
            {"name": "nex_sub",
             "config": {"source": f"mem://queue/{input_queue}", "max_depth": 1_000_000}}
        ],
        "publishers": [
            {"name": "nex_pub", "config": {"destination": f"mem://queue/{output_queue}"}}
        ],
        "calculators": calculators,
        "transformers": [],
        "nodes": nodes,
        "edges": edges,
    }
