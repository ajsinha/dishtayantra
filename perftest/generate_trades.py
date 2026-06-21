#!/usr/bin/env python3
"""
Trade-data load generator for the DishtaYantra performance test.

Generates a configurable number of HETEROGENEOUS trade records and publishes
them to a Kafka topic on localhost. The 'perftest_trade_etl' (row) DAG and the
'perftest_trade_etl_arrow' (columnar RecordBatch) DAG both consume that topic,
so the same stream exercises - and lets you compare - both paths.

Heterogeneity model
-------------------
A small set of CORE attributes is ALWAYS present, because both pipelines rely on
them as a contract: trade_id, seq, symbol, side, quantity, price, currency. The
row ETL requires them; the Arrow ingest maps them to typed columns. Everything
else varies wildly so the stream is genuinely ragged:

  * different trades carry different OPTIONAL key sets (shape differs per record),
  * nested dicts (settlement, counterparty, algo_params with dict-in-dict),
  * lists and lists-of-dicts (tags, allocations, option legs, audit_trail),
  * a few keys deliberately change TYPE across trades (notes str|list|dict,
    ref int|str, rate float|str) to stress the extras path,
  * several archetypes (simple / block / multi_leg / algo / cross_ccy /
    minimal / kitchen_sink) so overall record shape differs structurally.

In the Arrow path all non-core attributes (including the nested/list ones) are
preserved losslessly in the JSON ``extras_json`` column; in the row path they
ride through deepcopy and are projected away at summarize. Use ``--uniform`` to
fall back to the old flat single-shape records for an apples-to-apples baseline.

Usage:
    python3 perftest/generate_trades.py                 # 10000 heterogeneous trades
    python3 perftest/generate_trades.py --count 50000
    python3 perftest/generate_trades.py --count 1000 --rate 500     # 500 msg/sec
    python3 perftest/generate_trades.py --dry-run --count 8         # print samples
    python3 perftest/generate_trades.py --uniform                   # old flat shape
    python3 perftest/generate_trades.py --hetero-level high --seed 42

After it finishes, compare the DAG output line counts to measure throughput:
    wc -l /tmp/perftest/trade_etl_output.jsonl
    wc -l /tmp/perftest/trade_etl_arrow_output.jsonl
"""

import argparse
import json
import random
import sys
import time

DEFAULT_TOPIC = "perftest_trades"
DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_COUNT = 10000
DEFAULT_PARTITIONS = 5
DEFAULT_PARTITION_KEY = "symbol"

SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "JPM", "GS", "BAC",
    "XOM", "CVX", "PFE", "KO", "PEP", "WMT", "DIS", "NFLX", "INTC", "AMD",
    "BRK.B", "V", "MA", "HD", "MCD", "NKE", "BA", "CAT", "IBM", "ORCL",
]
CURRENCIES = ["USD", "USD", "USD", "EUR", "GBP", "JPY", "CHF", "INR", "AUD", "CAD"]
SIDES = ["BUY", "SELL"]
DESKS = ["EQ-CASH", "EQ-DERIV", "FX-SPOT", "RATES", "CREDIT"]
TRADERS = ["tgupta", "jsmith", "mwang", "akhan", "lpatel", "rgarcia", "dmuller"]
VENUES = ["NYSE", "NASDAQ", "LSE", "XETRA", "TSE"]
ARCHETYPES = ["simple", "block", "multi_leg", "algo", "cross_ccy",
              "minimal", "kitchen_sink"]


def _core(i, rng):
    """The always-present contract fields. Both pipelines depend on these."""
    return {
        "trade_id": f"T{1_000_000 + i}",
        "seq": i,                                   # monotonic -> unique
        "symbol": rng.choice(SYMBOLS),
        "side": rng.choice(SIDES),
        "quantity": rng.choice([100, 200, 250, 500, 1000, 2500, 5000, 10000]),
        # base price + tiny unique jitter so identical draws still differ
        "price": round(rng.uniform(5.0, 950.0) + (i % 1000) * 0.000001, 6),
        "currency": rng.choice(CURRENCIES),
    }


# ---- nested / list optional blocks (each returns a {key: value}) ------------ #
def _settlement(rng):
    return {"settlement": {
        "date": f"2026-0{rng.randint(1, 9)}-{rng.randint(10, 28)}",
        "method": rng.choice(["DVP", "FOP", "RVP"]),
        "account": f"ACC{rng.randint(1000, 9999)}",
        "custodian": rng.choice(["BNY", "STT", "JPM", "CITI"]),
    }}


def _counterparty(rng):
    return {"counterparty": {
        "id": f"CP{rng.randint(100, 999)}",
        "name": rng.choice(["Acme Cap", "Globex", "Initech", "Soylent", "Hooli"]),
        "lei": "".join(rng.choice("0123456789ABCDEFGHJKLMNPQRSTUVWXYZ") for _ in range(20)),
        "country": rng.choice(["US", "GB", "DE", "JP", "SG"]),
    }}


def _algo_params(rng):
    # dict-in-dict (deeper nesting)
    return {"algo_params": {
        "type": rng.choice(["VWAP", "TWAP", "POV", "IS"]),
        "start": "09:30", "end": "16:00",
        "participation_pct": round(rng.uniform(2, 25), 1),
        "limits": {"min_clip": rng.randint(100, 500),
                   "max_clip": rng.randint(1000, 5000),
                   "dark_ok": rng.choice([True, False])},
    }}


def _compliance(rng):
    # dict containing a list
    return {"compliance": {
        "restricted": rng.choice([True, False]),
        "checks": rng.sample(["AML", "KYC", "BEST_EX", "WASH", "POSITION_LIMIT"],
                             k=rng.randint(1, 4)),
    }}


def _allocations(rng):
    # list of dicts (block trade split across accounts)
    n = rng.randint(2, 5)
    return {"allocations": [
        {"account": f"FUND{rng.randint(1, 20)}",
         "quantity": rng.choice([100, 250, 500, 1000]),
         "pct": round(rng.uniform(5, 60), 1)} for _ in range(n)]}


def _legs(rng):
    # list of dicts (multi-leg option strategy)
    n = rng.randint(2, 4)
    return {"legs": [
        {"leg_id": k + 1, "symbol": rng.choice(SYMBOLS),
         "side": rng.choice(SIDES), "ratio": rng.choice([1, 1, 2, 3]),
         "strike": round(rng.uniform(50, 500), 2),
         "expiry": f"2026-{rng.randint(1, 12):02d}-15"} for k in range(n)]}


def _audit_trail(rng):
    actions = ["CREATED", "ROUTED", "AMENDED", "PARTIAL_FILL", "FILLED"]
    n = rng.randint(1, 4)
    return {"audit_trail": [
        {"ts": time.time() - rng.uniform(0, 60),
         "action": rng.choice(actions),
         "user": rng.choice(TRADERS)} for _ in range(n)]}


def _tags(rng):
    pool = ["urgent", "client", "hedge", "rebalance", "tax-lot", "program",
            "crossing", "facilitation", "risk", "sweep"]
    return {"tags": rng.sample(pool, k=rng.randint(1, 4))}


# flat optional fields (simple scalars)
def _flat_optionals(rng):
    out = {}
    pool = {
        "client_id": lambda: f"C{rng.randint(1, 5)}",
        "desk": lambda: rng.choice(DESKS),
        "trader": lambda: rng.choice(TRADERS),
        "venue": lambda: rng.choice(VENUES),
        "client_order_id": lambda: f"OID{rng.randint(10**8, 10**9)}",
        "portfolio": lambda: f"PF{rng.randint(1, 50)}",
        "region": lambda: rng.choice(["AMER", "EMEA", "APAC"]),
        "broker": lambda: rng.choice(["GS", "MS", "JPM", "UBS", "BARC"]),
        "ts_event": lambda: time.time(),
        "event_ts": lambda: time.perf_counter_ns(),
        "internal_only": lambda: "scratch",
        "urgency": lambda: rng.choice(["LOW", "NORMAL", "HIGH"]),
    }
    for key in rng.sample(list(pool), k=rng.randint(1, len(pool))):
        out[key] = pool[key]()
    return out


# keys whose TYPE varies across trades (stress the extras path)
def _type_varying(rng):
    out = {}
    if rng.random() < 0.5:
        out["notes"] = rng.choice([
            "manual booking",
            ["reviewed", "approved"],
            {"text": "see ticket", "author": rng.choice(TRADERS)},
        ])
    if rng.random() < 0.4:
        out["ref"] = rng.choice([rng.randint(1, 10**6), f"REF-{rng.randint(1, 999)}"])
    if rng.random() < 0.3:
        out["rate"] = rng.choice([round(rng.uniform(0.5, 2.0), 4),
                                  str(round(rng.uniform(0.5, 2.0), 4))])
    return out


_NESTED_BLOCKS = [_settlement, _counterparty, _algo_params, _compliance,
                  _audit_trail, _tags]
_LIST_HEAVY = [_allocations, _legs, _audit_trail]

# how many optional blocks to attach, by heterogeneity level
_LEVELS = {"low": (0, 2), "med": (1, 4), "high": (3, 7)}


def make_trade(i, rng, anomaly_pct, uniform=False, level="med"):
    """Build one trade. With uniform=True, emit the legacy flat single shape;
    otherwise pick an archetype and attach a random mix of nested/list/scalar
    optional fields so records differ in shape and depth."""
    trade = _core(i, rng)

    if uniform:
        trade.update({
            "client_id": f"C{(i % 5) + 1}", "desk": rng.choice(DESKS),
            "trader": rng.choice(TRADERS), "venue": rng.choice(VENUES),
            "client_order_id": f"OID{rng.randint(10**8, 10**9)}",
            "ts_event": time.time(), "event_ts": time.perf_counter_ns(),
            "internal_only": "scratch",
        })
    else:
        archetype = rng.choice(ARCHETYPES)
        trade["archetype"] = archetype          # so you can see the shape mix
        if archetype != "minimal":
            trade.update(_flat_optionals(rng))
            trade.update(_type_varying(rng))

        if archetype == "block":
            trade.update(_allocations(rng)); trade.update(_counterparty(rng))
        elif archetype == "multi_leg":
            trade.update(_legs(rng)); trade.update(_algo_params(rng))
        elif archetype == "algo":
            trade.update(_algo_params(rng)); trade.update(_tags(rng))
        elif archetype == "cross_ccy":
            trade.update(_settlement(rng)); trade.update(_counterparty(rng))
            trade["fx"] = {"pair": f"{trade['currency']}USD",
                           "tenor": rng.choice(["SPOT", "TN", "SN"])}
        elif archetype == "kitchen_sink":
            for blk in _NESTED_BLOCKS + _LIST_HEAVY:
                trade.update(blk(rng))
        elif archetype == "simple":
            lo, hi = _LEVELS.get(level, _LEVELS["med"])
            for blk in rng.sample(_NESTED_BLOCKS,
                                  k=min(rng.randint(lo, hi), len(_NESTED_BLOCKS))):
                trade.update(blk(rng))
        # 'minimal' stays core-only (exercises the no-extras path)

    # Anomaly injection (on the core, so both pipelines flag the same records).
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


def ensure_topic(bootstrap, topic, partitions):
    """Best-effort: create ``topic`` with ``partitions`` partitions so Kafka has
    that many partitions to spread keyed messages across. No-ops if the topic
    already exists. If it exists with fewer partitions, key-based routing still
    works - the spread is simply limited to the partitions that exist.
    """
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError
    except ImportError:
        return
    admin = None
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap.split(","))
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions,
                                      replication_factor=1)])
        print(f"  created topic '{topic}' with {partitions} partitions")
    except TopicAlreadyExistsError:
        try:
            md = admin.describe_topics([topic])
            existing = len(md[0]["partitions"]) if md else None
            if existing is not None and existing < partitions:
                print(f"  note: topic '{topic}' already exists with {existing} "
                      f"partition(s) (< {partitions} requested). Key-based routing "
                      f"still works; messages spread across {existing} partition(s). "
                      f"Recreate the topic with more partitions for wider spread.")
            else:
                print(f"  topic '{topic}' already exists with {existing} partition(s)")
        except Exception:  # noqa: BLE001
            print(f"  topic '{topic}' already exists (partition count unknown)")
    except Exception as exc:  # noqa: BLE001
        print(f"  (note: could not ensure {partitions} partitions on '{topic}': {exc}; "
              f"relying on the existing/auto-created topic)")
    finally:
        if admin is not None:
            try:
                admin.close()
            except Exception:  # noqa: BLE001
                pass


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
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            acks=1,
            linger_ms=5,          # small batching window for throughput
            retries=3,
        )
    except Exception as exc:  # noqa: BLE001
        sys.exit(f"Could not connect to Kafka at {bootstrap}: {exc}\n"
                 f"Start a broker (e.g. docker run -p 9092:9092 ...) and retry.")


def main():
    ap = argparse.ArgumentParser(description="Generate heterogeneous trades and publish to Kafka.")
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
    ap.add_argument("--hetero-level", choices=["low", "med", "high"], default="med",
                    help="how many optional blocks 'simple' trades carry (default med)")
    ap.add_argument("--uniform", action="store_true",
                    help="emit the legacy flat single-shape records (baseline)")
    ap.add_argument("--seed", type=int, default=None,
                    help="RNG seed for reproducible data")
    ap.add_argument("--dry-run", action="store_true",
                    help="print a few sample trades instead of sending to Kafka")
    ap.add_argument("--partitions", type=int, default=DEFAULT_PARTITIONS,
                    help=f"how many partitions to create the topic with (default "
                         f"{DEFAULT_PARTITIONS}), giving Kafka that many partitions "
                         f"to spread keyed messages across. Routing is by key hash, "
                         f"not an explicit partition number; same value -> same "
                         f"partition. Use 1 for a single-partition topic.")
    ap.add_argument("--partition-key", default=DEFAULT_PARTITION_KEY,
                    help=f"trade field that identifies the 'product type' for "
                         f"partition routing (default '{DEFAULT_PARTITION_KEY}'). "
                         f"All trades sharing this field value go to one partition.")
    args = ap.parse_args()

    if args.partitions < 1:
        sys.exit("--partitions must be >= 1")

    rng = random.Random(args.seed)

    if args.dry_run:
        n = min(args.count, 8)
        print(f"# Dry run: {n} sample trades (of {args.count} requested), "
              f"{'uniform' if args.uniform else 'heterogeneous'}\n")
        for i in range(n):
            t = make_trade(i, rng, args.anomaly_pct, args.uniform, args.hetero_level)
            key_val = str(t.get(args.partition_key, ""))
            print(f"# key {args.partition_key}={key_val}: {json.dumps(t)}")
        prods = sorted(set(SYMBOLS))
        print(f"\n# {len(prods)} distinct '{args.partition_key}' value(s). Each is "
              f"sent as the Kafka message key, so all trades for one value share a "
              f"partition (Kafka hashes the key over the topic's partitions; the "
              f"producer does not choose the partition number):")
        print("#   " + ", ".join(prods))
        return

    if args.partitions > 1:
        ensure_topic(args.bootstrap, args.topic, args.partitions)
    producer = build_producer(args.bootstrap)
    print(f"Publishing {args.count} "
          f"{'uniform' if args.uniform else 'heterogeneous'} trades to topic "
          f"'{args.topic}' at {args.bootstrap}"
          + (f" (rate-limited to {args.rate}/s)" if args.rate else "")
          + f"; keyed by '{args.partition_key}' so Kafka groups every trade "
            f"with the same value onto one partition ...")

    interval = (1.0 / args.rate) if args.rate > 0 else 0.0
    sent = 0
    start = time.perf_counter()
    next_report = 10000
    key_field = args.partition_key
    distinct_keys = set()

    for i in range(args.count):
        trade = make_trade(i, rng, args.anomaly_pct, args.uniform, args.hetero_level)
        key_val = str(trade.get(key_field, ""))
        # Provide ONLY the message key; Kafka's partitioner hashes it to one of
        # the topic's partitions (same key -> same partition). The producer must
        # not choose the partition number itself - that would assume a partition
        # count the topic may not have, and fail if it has fewer.
        producer.send(args.topic, value=trade, key=key_val)
        distinct_keys.add(key_val)
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
    print(f"Keyed by '{key_field}': {len(distinct_keys)} distinct value(s); Kafka "
          f"routes each value to one of the topic's partitions by key hash "
          f"(same value -> same partition).")
    print("Now check the DAG output files once the pipeline drains, e.g.:")
    print("  wc -l /tmp/perftest/trade_etl_output.jsonl")
    print("  wc -l /tmp/perftest/trade_etl_arrow_output.jsonl")


if __name__ == "__main__":
    main()
