# Benchmarking and Performance Harness Guide

DishtaYantra ships a benchmark harness (`benchmarks/`) that drives the **real
engine** over representative workloads and reports throughput, latency
percentiles, and peak memory. The goal is the roadmap's "measure before you
claim" principle: every performance number should be reproducible and
CI-checkable rather than quoted from memory.

## Running a benchmark

```bash
# Finance trade-ETL workload (default), 20k messages, 6 calculator stages
python -m benchmarks.run_benchmark --messages 20000 --stages 6

# Nexmark-subset workload (Q1 currency convert + Q2 auction select)
python -m benchmarks.run_benchmark --workload nexmark --messages 20000

# Write machine-readable + human-readable results
python -m benchmarks.run_benchmark --workload nexmark --messages 20000 \
    --output benchmarks/results/nexmark.json \
    --markdown benchmarks/results/nexmark.md
```

A run prints a one-line summary, for example:

```
[nexmark_linear[Q1+Q2]] 20000/20000 msgs in 2.61s -> 7,653 msg/s | latency ms p50=... p95=... p99=... | peak RSS 71 MB
```

The process exits non-zero if not every message was delivered, which makes it a
usable CI gate.

## Workloads

| Workload | `--workload` | What it exercises |
|---|---|---|
| Finance trade-ETL | `trade_etl` (default) | A linear DAG of real trade calculators (validate → normalize → FX → notional → fees → risk) over the in-memory broker. |
| Nexmark (subset) | `nexmark` | The canonical auction/bid stream: Q1 (currency conversion, a stateless map) and Q2 (auction selection, a stateless filter). A representative subset, not the full 8-query suite. |

Both run over the genuine `ComputeGraph` engine via the in-memory broker, so no
Kafka or external infrastructure is required. To benchmark a different DAG, point
the harness at any DAG config the same way.

## Metrics reported

- **Throughput** — delivered messages per second.
- **Latency percentiles** — end-to-end p50/p95/p99/max in milliseconds, correlated
  per message.
- **Peak memory** — peak RSS over the run.
- Environment — Python version and free-threading (GIL) status.

Recovery-time (failover) benchmarking is not yet part of the harness; it is
tracked on the roadmap and needs the HA test rig.

## Comparing the native Arrow path

The harness pairs naturally with the zero-copy native calculator (see the
*Native Arrow Calculators* guide): run a numeric workload through the pyarrow path
and the native C-Data-Interface path and compare throughput on identical output.
This is how claims like "the columnar path is ~2.3× faster, byte-identical" stay
reproducible rather than anecdotal.

## CI

The harness has smoke tests (`tests/test_benchmark_harness.py`,
`tests/test_nexmark_workload.py`) that run each workload at tiny scale and assert
that all messages are delivered and that metrics are produced and ordered. Keep
these green so the harness itself never silently rots.

## Spikes

Alongside the workloads, `benchmarks/` includes exploratory spikes used to
de-risk roadmap items — Arrow vectorization, the A1 vertical slice, and
free-threading readiness. These are research scripts, not CI gates.
