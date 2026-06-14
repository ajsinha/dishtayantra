# DishtaYantra Benchmark Harness

Roadmap **Phase 0 — Foundations & instrumentation** (see `docs/ROADMAP.md`).
The goal of this directory is simple: **establish a reproducible baseline
before the engine work begins**, so every later change (Arrow data plane,
no-GIL Python, embedded SQL engine, …) can be measured rather than asserted.

Everything here runs against the **in-memory broker** and the **real engine**
(`core.dag.compute_graph.ComputeGraph` + real `perftest` calculators), so it
needs no Kafka or external systems and is safe to run in CI.

## What's here

| File | Purpose |
|------|---------|
| `harness.py` | Reusable measurement framework: latency percentiles, throughput, peak memory, structured results. Engine-agnostic core + an in-memory DAG runner. |
| `workloads.py` | Reference workload: a linear trade-ETL DAG (validate → normalize → FX → notional → fees → risk) built from the real `perftest` calculators. |
| `run_benchmark.py` | CLI to run the reference workload and emit JSON / markdown. |
| `freethreading_spike.py` | Phase-0 de-risking spike for step **A3** (no-GIL Python): measures CPU-bound calculator scaling across threads and inventories extension GIL-readiness. |

## Running the throughput / latency benchmark

```bash
# from the repo root
python -m benchmarks.run_benchmark --messages 5000 --stages 6
python -m benchmarks.run_benchmark --messages 20000 --output benchmarks/results/baseline.json
```

Reports: messages delivered, wall-clock duration, throughput (msg/s),
end-to-end latency `p50/p95/p99/max` (ms, correlated per message by
`trade_id`), and peak RSS.

**Methodology note.** By default the publisher emits as fast as possible and a
single consumer drains concurrently, so the latency figures are
*latency under maximum load* (messages queue while the pipeline catches up).
For steady-state latency at a fixed arrival rate, pass `--pace-hz` (e.g.
`--pace-hz 2000`). Throughput is always delivered ÷ wall-clock. The point of a
baseline is a *consistent* methodology to compare against, not a single
"realistic" number.

Point the harness at any DAG config (including the full branching, Kafka-fed
`perftest/perftest_trade_etl.json`) to benchmark it the same way — see
`run_inmemory_dag_benchmark` in `harness.py`.

## Running the free-threading readiness spike

```bash
# on a standard build: shows the GIL ceiling we want to remove
python -m benchmarks.freethreading_spike --calls 120000 --threads 1,2,4,8

# on a free-threaded build: shows the no-GIL upside
python3.14t -m benchmarks.freethreading_spike --output benchmarks/results/ft.json
```

On a standard interpreter, CPU-bound calculator threads do **not** speed up
(efficiency collapses as threads increase) — that flat line is the ceiling
roadmap step A3 targets. On a `python3.14t` build, wall time should drop
roughly linearly. The spike also flags any native extension (pyarrow, lmdb,
polars, …) that silently re-enables the GIL at import.

This complements `scripts/check_free_threading.py` (which only does the
import-time GIL-reenable check) by adding the actual scaling measurement.

## Notes

- Results are **not** written unless you pass `--output` / `--markdown`; the
  suggested location is `benchmarks/results/` (kept out of the shipped package).
- A fast smoke version of each runs in the test suite
  (`tests/test_benchmark_harness.py`, `tests/test_freethreading_spike.py`).
