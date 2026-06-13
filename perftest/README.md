# DishtaYantra Performance Test Harness

A realistic, Kafka-fed trade-ETL workload for benchmarking end-to-end
**throughput** and **latency** of the DAG compute server.

## What's here

| File | Purpose |
|------|---------|
| `perftest_trade_etl.json` | The 11-node branching DAG: Kafka source -> common prep -> 3 parallel branches -> merge -> file + Kafka sinks. |
| `etl_calculators.py` | Custom trade-processing calculators (validate, normalize, FX, notional, fees, risk/VaR, classify, anomaly, summarize). |
| `generate_trades.py` | Configurable load generator that publishes trades to Kafka (default 10,000). |

## The pipeline (11 nodes, branching topology)

```
Kafka topic "perftest_trades"
        |
  trade_ingest   (SubscriptionNode + ValidateTradeCalculator)
        v
  normalize_node (types/casing canonicalized)
        v
  fx_node        (convert price to USD)
        v
  notional_node  (quantity x price_usd, signed by side)
        |
        |  ---- FAN-OUT into 3 parallel branches ----
        |
   +----+--------------------+--------------------+
   v                         v                    v
 fees_branch            risk_branch         anomaly_branch
 (commission,           (per-symbol vol     (fat-finger /
  exchange fee,          -> 1d VaR,          price-outlier /
  net notional)          risk score)         validation flags)
   |                         |                    |
   +----+--------------------+--------------------+
        |  ---- FAN-IN: classify merges all three ----
        v
  classify_node  (size bucket + risk tier + requires_review,
                  computed from the merged branch outputs)
        v
  summarize_node (project the clean output record)
        |
        |  ---- FAN-OUT to two sinks ----
        +-----------------------+
        v                       v
   file_sink                kafka_sink
 file://.../trade_etl    kafka://topic/
   _output.jsonl          perftest_trades_enriched
```

This exercises **fan-out** (one node feeding several), **parallel branches**
that compute different attributes, and **fan-in** (the platform merges multiple
incoming edges into a single dict at `classify_node`) - a far more realistic and
demanding shape than a straight line. The enriched output is written to **both**
a file and a Kafka topic.

## Prerequisites

- A Kafka broker reachable at `localhost:9092` (override with `--bootstrap`).
  A quick local broker, for example:
  ```bash
  docker run -p 9092:9092 apache/kafka:latest
  ```
- `pip install -r requirements.txt` (kafka-python is included).

## Running the test

**1. Install the DAG and start the server.** Copy the DAG into the live config
and start DishtaYantra so it begins consuming the topic:
```bash
cp perftest/perftest_trade_etl.json config/dags/
mkdir -p /tmp/perftest
python run_server.py        # or however you start the server
```
Then **Start** the `perftest_trade_etl` DAG from the dashboard (or it will start
per its schedule; this DAG is perpetual / no schedule).

**2. Generate load.** In another shell:
```bash
python perftest/generate_trades.py --count 10000
# faster/larger:
python perftest/generate_trades.py --count 100000
# rate-limited (e.g. to measure steady-state latency):
python perftest/generate_trades.py --count 20000 --rate 2000
# reproducible data:
python perftest/generate_trades.py --count 10000 --seed 42
```

**3. Measure.** The generator prints produced throughput. For the end-to-end
result, watch the sink file fill and time how long after the last send it takes
to drain:
```bash
wc -l /tmp/perftest/trade_etl_output.jsonl
tail -1 /tmp/perftest/trade_etl_output.jsonl | python -m json.tool
```

### Generator options

```
--count N          number of trades (default 10000)
--topic NAME       Kafka topic (default perftest_trades)
--bootstrap HOST   bootstrap servers, comma-separated (default localhost:9092)
--rate R           cap at R msg/sec (0 = as fast as possible)
--anomaly-pct P    percentage of anomalous trades (default 1.0)
--seed S           RNG seed for reproducible data
--dry-run          print a few sample trades instead of sending
```

## Reading the output

The enriched trades are written to **two places**: the file sink
(`/tmp/perftest/trade_etl_output.jsonl`) and the Kafka topic
`perftest_trades_enriched`. To inspect the file output, each line is one
enriched trade, e.g.:
```json
{"trade_id":"T1000048","symbol":"ORCL","side":"SELL","notional_usd":3127000.0,
 "total_fees_usd":1876.2,"var_1d_usd":1444674.0,"risk_score":100.0,
 "size_bucket":"BLOCK","risk_tier":"HIGH","requires_review":true,
 "is_anomalous":false,"anomaly_flags":[],"processed_at":"...","pipeline":"perftest_trade_etl"}
```

Useful slices:
```bash
# how many large/risky trades were flagged for review
grep -c '"requires_review": true' /tmp/perftest/trade_etl_output.jsonl
# anomalies caught
grep -c '"is_anomalous": true' /tmp/perftest/trade_etl_output.jsonl
```

## Notes

- **First output line may be an empty `{}`** - an artifact of the node
  change-detection emitting once at startup before real data flows. Skip line 1
  when counting processed trades, or filter `grep -v '^{}$'`.
- **Latency tuning:** the subscriber's `idle_poll_interval` is set low (2 ms) in
  the DAG config for a latency-sensitive feed; raise it to reduce idle CPU if you
  only care about throughput.
- **No broker in your environment?** The harness logic can be exercised without
  Kafka by pointing the DAG source at `mem://queue/perftest_trades` and the
  generator's records onto that in-memory queue - useful for validating the ETL
  itself separately from broker throughput.
