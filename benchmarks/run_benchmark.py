#!/usr/bin/env python3
"""Run the DishtaYantra reference benchmark.

Examples:
    python -m benchmarks.run_benchmark --messages 5000
    python -m benchmarks.run_benchmark --messages 20000 --stages 6 \
        --output benchmarks/results/baseline.json

Drives a linear trade-ETL DAG over the in-memory broker (real engine code, no
Kafka) and reports end-to-end latency percentiles, throughput, and peak memory.
This is roadmap Phase 0: establish a reproducible baseline before the engine
work begins.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

# Allow running as a plain script from the repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from benchmarks.harness import run_inmemory_dag_benchmark  # noqa: E402
from benchmarks.workloads import (  # noqa: E402
    trade_etl_linear_config,
    trade_generator,
)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="DishtaYantra benchmark harness")
    parser.add_argument("--messages", type=int, default=5000,
                        help="number of measured messages (default 5000)")
    parser.add_argument("--stages", type=int, default=6,
                        help="number of linear ETL calculator stages (1-6)")
    parser.add_argument("--pace-hz", type=float, default=None,
                        help="publish rate cap (default: as fast as possible)")
    parser.add_argument("--warmup", type=int, default=50,
                        help="warmup messages (not measured)")
    parser.add_argument("--drain-timeout", type=float, default=60.0)
    parser.add_argument("--output", type=str, default=None,
                        help="optional path to write a JSON result")
    parser.add_argument("--markdown", type=str, default=None,
                        help="optional path to write a markdown result")
    parser.add_argument("--quiet", action="store_true",
                        help="silence engine logs (default on)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING)
    if args.quiet or True:  # benchmarks are noisy; keep engine logs quiet
        logging.disable(logging.INFO)

    in_q, out_q = "bench_in", "bench_out"
    config = trade_etl_linear_config(in_q, out_q, stages=args.stages)

    result = run_inmemory_dag_benchmark(
        name=f"trade_etl_linear[{args.stages} stages]",
        config=config,
        input_queue=in_q,
        output_queue=out_q,
        n_messages=args.messages,
        gen_fn=trade_generator(),
        warmup_messages=args.warmup,
        pace_hz=args.pace_hz,
        drain_timeout_s=args.drain_timeout,
    )

    print("\n" + result.summary())
    if result.notes:
        print(f"  notes: {result.notes}")

    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, "w") as fh:
            json.dump(result.to_dict(), fh, indent=2)
        print(f"  wrote JSON -> {args.output}")
    if args.markdown:
        os.makedirs(os.path.dirname(os.path.abspath(args.markdown)), exist_ok=True)
        with open(args.markdown, "w") as fh:
            fh.write(result.to_markdown())
        print(f"  wrote markdown -> {args.markdown}")

    # Non-zero exit if we failed to deliver everything (useful as a CI gate).
    return 0 if result.delivered == result.n_messages else 1


if __name__ == "__main__":
    sys.exit(main())
