"""Reference benchmark workloads for DishtaYantra.

The first reference workload is a linear trade-ETL pipeline built from the real
``perftest.etl_calculators`` (validate -> normalize -> FX -> notional -> fees
-> risk), wired to in-memory queues so it runs anywhere with no Kafka. This
exercises genuine engine + calculator code paths, mirroring the Kafka-fed
``perftest/`` workload the team already cares about.

Point the harness at any other DAG config (e.g. the full branching
``perftest/perftest_trade_etl.json`` with brokers) to benchmark it the same way.
"""

from __future__ import annotations

import random
from typing import Any, Dict


_SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "JPM", "BAC", "XOM"]
_CCYS = ["USD", "EUR", "GBP", "JPY", "CHF", "INR", "AUD", "CAD"]
_SIDES = ["BUY", "SELL"]

# Linear chain of real trade-ETL calculators (dotted paths resolved by the DAG).
_STAGES = [
    ("validate", "perftest.etl_calculators.ValidateTradeCalculator"),
    ("normalize", "perftest.etl_calculators.NormalizeTradeCalculator"),
    ("fx", "perftest.etl_calculators.FxConvertCalculator"),
    ("notional", "perftest.etl_calculators.NotionalCalculator"),
    ("fees", "perftest.etl_calculators.FeeCalculator"),
    ("risk", "perftest.etl_calculators.RiskScoreCalculator"),
]


def make_trade(i: int, rng: random.Random) -> Dict[str, Any]:
    """Deterministic-ish synthetic trade keyed by trade_id=i."""
    return {
        "trade_id": i,
        "symbol": rng.choice(_SYMBOLS),
        "side": rng.choice(_SIDES),
        "quantity": rng.randint(1, 5000),
        "price": round(rng.uniform(1.0, 950.0), 4),
        "currency": rng.choice(_CCYS),
    }


def trade_generator(seed: int = 1234):
    """Return a gen_fn(i)->trade closure with a private RNG (thread-safe-ish)."""
    rng = random.Random(seed)

    def _gen(i: int) -> Dict[str, Any]:
        return make_trade(i, rng)

    return _gen


def trade_etl_linear_config(
    input_queue: str = "bench_in",
    output_queue: str = "bench_out",
    stages: int = len(_STAGES),
) -> Dict[str, Any]:
    """Build an in-memory linear trade-ETL DAG config with `stages` calculators."""
    stages = max(1, min(stages, len(_STAGES)))
    chosen = _STAGES[:stages]

    calculators = [
        {"name": name, "type": dotted, "config": {}} for name, dotted in chosen
    ]
    nodes = [
        {
            "name": "ingest",
            "type": "SubscriptionNode",
            "config": {},
            "subscriber": "bench_sub",
        }
    ]
    for name, _ in chosen:
        nodes.append(
            {
                "name": f"{name}_node",
                "type": "CalculationNode",
                "config": {},
                "calculator": name,
            }
        )
    nodes.append(
        {
            "name": "sink",
            "type": "PublicationNode",
            "config": {},
            "publishers": ["bench_pub"],
        }
    )

    # Linear edges: ingest -> stage1 -> ... -> stageN -> sink
    order = ["ingest"] + [f"{n}_node" for n, _ in chosen] + ["sink"]
    edges = [
        {"from_node": order[i], "to_node": order[i + 1]}
        for i in range(len(order) - 1)
    ]

    return {
        "name": "bench_trade_etl_linear",
        "start_time": None,
        "end_time": None,
        "subscribers": [
            {
                "name": "bench_sub",
                "config": {"source": f"mem://queue/{input_queue}", "max_depth": 1_000_000},
            }
        ],
        "publishers": [
            {
                "name": "bench_pub",
                "config": {"destination": f"mem://queue/{output_queue}"},
            }
        ],
        "calculators": calculators,
        "transformers": [],
        "nodes": nodes,
        "edges": edges,
    }
