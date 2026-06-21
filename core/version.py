"""
DishtaYantra Version Information
================================

Single source of truth for the application version. Every module, template,
banner, and document must reference this module rather than hard-coding a
version string.

Per-release highlights are kept in docs/CHANGELOG.md (not here, to keep this
file small). Most recent release:

Version 5.19.0 highlights (new JSON-array trade-ETL lane: list-of-dicts batching, no Arrow):
    - NEW perftest/perftest_trade_etl_array.json + perftest/array_trade_calculators.py: a third
      trade-ETL lane whose unit of work on each edge is a plain Python list of dicts (a JSON
      array), processed a whole batch per calculate() call. Uses stock BatchingSubscriptionNode
      (drains N messages into {batch:[...]}) + array calculators + stock FlatteningPublicationNode
      sinks; no Arrow dependency. The speedup is pure batch amortization of per-message node/
      queue/gate overhead (the arithmetic stays pure-Python per element), so it sits between the
      row lane and the Arrow lane - useful when data is too irregular to normalize into Arrow
      columns, and as a baseline that isolates batching gains from vectorization gains.
    - Verified BIT-FOR-BIT output parity with the canonical row lane (constants/logic imported
      from perftest/etl_calculators.py so they cannot drift); DAG assembles correctly in the
      engine (12 nodes, correct types/topology). Indicative calculator-chain throughput on 50k
      heterogeneous trades (~9 stages, single thread, CPython 3.12, excludes Kafka I/O): row
      ~7.4k rec/s, array ~17k (~2.3x), arrow ~55k (~7.4x). Documented in the Benchmarking guide.
    - Additive/opt-in (referenced by dotted path); the engine is untouched. 235 passed.
      Full history: docs/CHANGELOG.md.
"""

VERSION = "5.19.0"
BUILD_DATE = "2026-06-18"
APP_NAME = "DishtaYantra"


def get_version_banner() -> str:
    """Return a one-line human readable version banner."""
    return f"{APP_NAME} Compute Server v{VERSION} (build {BUILD_DATE})"
