#!/usr/bin/env python3
"""
Trade-data load generator for the DishtaYantra performance test.

Generates a configurable number of realistic trade records and publishes them
to a Kafka topic on localhost. The 'perftest_trade_etl' DAG consumes that topic,
runs the 9-stage ETL, and writes enriched output to a file sink.

Usage:
    python3 perftest/generate_trades.py                 # 10000 trades, defaults
    python3 perftest/generate_trades.py --count 50000
    python3 perftest/generate_trades.py --count 1000 --rate 500   # 500 msg/sec
    python3 perftest/generate_trades.py --topic perftest_trades \
        --bootstrap localhost:9092 --anomaly-pct 2 --seed 42

After it finishes it prints how many were sent and the elapsed time; pair that
with the line count of the DAG's output file to measure end-to-end throughput:

    wc -l /tmp/perftest/trade_etl_output.jsonl
"""

import argparse
import json
import random
import sys
import time

DEFAULT_TOPIC = "perftest_trades"
DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_COUNT = 10000

SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "JPM", "GS", "BAC",
    "XOM", "CVX", "PFE", "KO", "PEP", "WMT", "DIS", "NFLX", "INTC", "AMD",
    "BRK.B", "V", "MA", "HD", "MCD", "NKE", "BA", "CAT", "IBM", "ORCL",
]
CURRENCIES = ["USD", "USD", "USD", "EUR", "GBP", "JPY", "CHF", "INR", "AUD", "CAD"]
SIDES = ["BUY", "SELL"]
DESKS = ["EQ-CASH", "EQ-DERIV", "FX-SPOT", "RATES", "CREDIT"]
TRADERS = ["tgupta", "jsmith", "mwang", "akhan", "lpatel", "rgarcia", "dmuller"]


def make_trade(i, rng, anomaly_pct):
    """Build one realistic trade record. A small percentage are anomalous.

    Every record is guaranteed UNIQUE even if a trade_id repeats: the
    monotonic `seq`, a high-resolution `event_ts`, and a per-record price
    jitter ensure no two emitted dicts are identical (important when the DAG
    runs with change-detection rather than streaming mode).
    """
    symbol = rng.choice(SYMBOLS)
    side = rng.choice(SIDES)
    currency = rng.choice(CURRENCIES)
    quantity = rng.choice([100, 200, 250, 500, 1000, 2500, 5000, 10000])
    # base price plus a tiny unique jitter so identical (symbol, qty) draws
    # still differ from one another
    price = round(rng.uniform(5.0, 950.0) + (i % 1000) * 0.000001, 6)

    trade = {
        "trade_id": f"T{1_000_000 + i}",
        "seq": i,                       # monotonic, guarantees uniqueness
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "price": price,
        "currency": currency,
        # client_id lets the SAME trade stream drive client-centric pipelines
        # (e.g. the EOD running-exposure example) without a separate generator.
        "client_id": f"C{(i % 5) + 1}",
        "desk": rng.choice(DESKS),
        "trader": rng.choice(TRADERS),
        "venue": rng.choice(["NYSE", "NASDAQ", "LSE", "XETRA", "TSE"]),
        "client_order_id": f"OID{rng.randint(10**8, 10**9)}",
        "ts_event": time.time(),
        "event_ts": time.perf_counter_ns(),   # high-res, unique per record
        # a field the ETL deliberately drops, to exercise projection work
        "internal_only": "scratch",
    }

    # Inject occasional anomalies so the ETL's anomaly stage has work to flag.
    if rng.random() * 100 < anomaly_pct:
        kind = rng.choice(["fat_finger", "price_outlier", "bad_side", "neg_price"])
        if kind == "fat_finger":
            trade["quantity"] = rng.choice([2_000_000, 5_000_000])
        elif kind == "price_outlier":
            trade["price"] = round(rng.uniform(1_500_000, 5_000_000), 2)
        elif kind == "bad_side":
            trade["side"] = "XFER"
        elif kind == "neg_price":
            trade["price"] = -abs(trade["price"])

    return trade


def build_producer(bootstrap):
    """Construct a kafka-python producer, failing clearly if Kafka is absent."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        sys.exit("kafka-python is not installed. Run: pip install kafka-python")

    try:
        return KafkaProducer(
            bootstrap_servers=bootstrap.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=1,
            linger_ms=5,          # small batching window for throughput
            retries=3,
        )
    except Exception as exc:  # noqa: BLE001
        sys.exit(f"Could not connect to Kafka at {bootstrap}: {exc}\n"
                 f"Start a broker (e.g. docker run -p 9092:9092 ...) and retry.")


def main():
    ap = argparse.ArgumentParser(description="Generate trades and publish to Kafka.")
    ap.add_argument("--count", type=int, default=DEFAULT_COUNT,
                    help=f"number of trades to send (default {DEFAULT_COUNT})")
    ap.add_argument("--topic", default=DEFAULT_TOPIC,
                    help=f"Kafka topic (default {DEFAULT_TOPIC})")
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP,
                    help=f"bootstrap servers, comma-separated (default {DEFAULT_BOOTSTRAP})")
    ap.add_argument("--rate", type=float, default=0.0,
                    help="max messages/sec (0 = as fast as possible)")
    ap.add_argument("--anomaly-pct", type=float, default=1.0,
                    help="percentage of anomalous trades (default 1.0)")
    ap.add_argument("--seed", type=int, default=None,
                    help="RNG seed for reproducible data")
    ap.add_argument("--dry-run", action="store_true",
                    help="print a few sample trades instead of sending to Kafka")
    args = ap.parse_args()

    rng = random.Random(args.seed)

    if args.dry_run:
        print(f"# Dry run: {min(args.count, 5)} sample trades "
              f"(of {args.count} requested)\n")
        for i in range(min(args.count, 5)):
            print(json.dumps(make_trade(i, rng, args.anomaly_pct)))
        return

    producer = build_producer(args.bootstrap)
    print(f"Publishing {args.count} trades to topic '{args.topic}' "
          f"at {args.bootstrap}"
          + (f" (rate-limited to {args.rate}/s)" if args.rate else "") + " ...")

    interval = (1.0 / args.rate) if args.rate > 0 else 0.0
    sent = 0
    start = time.perf_counter()
    next_report = 10000

    for i in range(args.count):
        producer.send(args.topic, make_trade(i, rng, args.anomaly_pct))
        sent += 1
        if interval:
            time.sleep(interval)
        if sent >= next_report:
            elapsed = time.perf_counter() - start
            print(f"  ... {sent} sent ({sent/elapsed:,.0f} msg/s)")
            next_report += 10000

    producer.flush()
    producer.close()
    elapsed = time.perf_counter() - start
    print(f"\nDone. Sent {sent} trades in {elapsed:.2f}s "
          f"({sent/elapsed:,.0f} msg/s produced).")
    print("Now check the DAG output file once the pipeline drains, e.g.:")
    print("  wc -l /tmp/perftest/trade_etl_output.jsonl")


if __name__ == "__main__":
    main()
