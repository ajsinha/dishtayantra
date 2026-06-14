"""DishtaYantra benchmark harness (roadmap Phase 0).

A small, dependency-light framework for measuring end-to-end latency
percentiles, throughput, and peak memory of DishtaYantra pipelines, plus a
free-threading readiness spike. CI-runnable; drives the real engine over the
in-memory broker so no external systems are required.

See benchmarks/README.md and docs/ROADMAP.md (Phase 0).
"""

from .harness import (
    BenchmarkResult,
    LatencyStats,
    percentile,
    run_inmemory_dag_benchmark,
)

__all__ = [
    "BenchmarkResult",
    "LatencyStats",
    "percentile",
    "run_inmemory_dag_benchmark",
]
