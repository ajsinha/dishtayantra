"""
DishtaYantra Version Information
================================

Single source of truth for the application version. Every module, template,
banner, and document must reference this module rather than hard-coding a
version string.

Version 4.5.0 highlights (A1 automatic source-batching - additive, opt-in):
    - Two new opt-in node types make Arrow batching automatic while preserving the
      external per-message contract: BatchingSubscriptionNode drains incoming
      messages into one columnar envelope per cycle (load-adaptive), and
      FlatteningPublicationNode republishes each record on output.
    - The existing SubscriptionNode / PublicationNode classes are byte-for-byte
      unchanged (verified by diff); the new types are appended subclasses, so
      every existing DAG behaves exactly as before.
    - Worked example perftest/perftest_arrow_autobatch.json +
      perftest/run_autobatch_example.py: ordinary per-message trades in and out,
      batched internally, output identical to the all-row pipeline (CI:
      tests/test_autobatch.py). Throughput gain is currently modest (the dataflow
      deep-copies each envelope per stage); carrying Arrow RecordBatches on edges
      is the next A1 increment.
    - Docs updated: ROADMAP, TUTORIAL, ARCHITECTURE, README, A1 design docs, and
      the architecture help page.

Version 4.4.0 highlights (Arrow tutorial + adapter ergonomics):
    - Hands-on tutorial at docs/TUTORIAL_arrow_dag.md: write an ArrowCalculator,
      use it as a drop-in, vectorize batches, mix row + Arrow in one graph, bridge
      legacy calculators with RowCalculatorBatchAdapter, and measure the result.
    - RowCalculatorBatchAdapter now resolves config["wrapped"] using the same
      {"type": <builtin|dotted.path>, "config": {...}} shape as a DAG calculator
      entry (with CalculatorFactory fallback), so it is configurable directly in
      DAG JSON. Backward compatible (passing a wrapped instance still works).
    - README linked to the tutorial. No engine behavior change.

Version 4.3.0 highlights (A1 worked example + old/new coexistence):
    - Worked example hosted in perftest/: arrow_etl_calculators.py (vectorized
      FX/notional/fee/risk, each output-identical to the row versions),
      perftest_arrow_mixed.json (a SINGLE graph mixing row and Arrow calculators),
      and run_arrow_example.py (a runnable demonstration).
    - CONFIRMED by execution + CI (tests/test_arrow_coexistence.py): old-style and
      new-style DAGs/calculators coexist in the same instance, AND a single graph
      can contain both row and Arrow nodes (mixed graph output is bit-identical to
      the all-row equivalent over 20,000 trades).
    - Detailed design doc + decision tree at
      docs/design/A1-worked-example-and-coexistence.md; README, ARCHITECTURE.md,
      and the Calculators/Architecture help pages updated accordingly.
    - Engine (core/dag/*) and core_calculator.py remain unchanged.

Version 4.2.0 highlights (Phase 1 / A1 vertical slice - additive, opt-in):
    - ArrowCalculator contract (core/calculator/arrow_calculator.py): an opt-in
      columnar calculator that is ALSO a drop-in row DataCalculator, so adopting
      it requires NO engine change. The engine (core/dag/*) and core_calculator.py
      are byte-for-byte unchanged; existing DAGs and stored data are unaffected.
    - Vectorized ArrowFxConvert / ArrowNotional calculators that are output-
      identical to the perftest row calculators (exact parity, validated in CI).
    - End-to-end vertical slice through the real ComputeGraph
      (benchmarks/a1_vertical.py): the same two-stage pipeline run row-at-a-time
      vs Arrow batch-envelopes, verified IDENTICAL per-trade and measured ~1.8x
      faster end-to-end (the pure kernel is ~11.8x; the node-boundary dict<->Arrow
      conversion accounts for the rest - the documented next increment is Arrow
      transport to remove that bridging).
    - RowCalculatorBatchAdapter lets legacy row calculators run in a batched path
      unchanged (mixed-DAG compatibility).

Version 4.1.0 highlights (Phase 1 / A1 kickoff - design + de-risking spike):
    - A1 Arrow columnar data-plane design RFC at docs/design/A1-arrow-data-plane.md
      (Arrow RecordBatch edges, an additive ArrowCalculator batch contract,
      micro-batching with a linger cap, zero-copy polyglot handoff, a one-path
      vertical slice, and a backward-compatible migration plan)
    - Runnable A1 spike (benchmarks/arrow_vectorization_spike.py) proving the
      core claim on this codebase's own trade-ETL numerics: vectorized Arrow
      kernels ran ~11.8x faster than the current row-at-a-time model with
      bit-identical output (validated row-vs-Arrow in the test suite)
    - Still no production engine behavior change; this de-risks A1 and gives the
      RFC measured evidence before the core refactor begins

Version 4.0.0 highlights (start of the "best-in-class" roadmap):
    - Strategic feature roadmap published at docs/ROADMAP.md (Arrow data plane,
      no-GIL Python, embedded streaming SQL, event-time/exactly-once,
      incremental materialized views, WASM calculators, real-time AI, and a
      graceful single-node-first scale path)
    - Phase 0 foundations shipped: a reproducible benchmark harness
      (benchmarks/) that drives the real engine over the in-memory broker and
      reports latency percentiles, throughput, and peak memory
    - Free-threading readiness spike (benchmarks/freethreading_spike.py) that
      measures CPU-bound calculator scaling across threads and inventories
      native-extension GIL readiness, de-risking the no-GIL work (step A3)
    - No engine behavior change in this release; it establishes the
      measurement baseline that all later roadmap work is compared against

Version 3.1.0 highlights:
    - Multiple DAG folders: 'storage.dags.prefixes' lists extra, logically
      grouped folders (config/dags is always included); each scanned for its
      DIRECT .json children only (never sub-folders)
    - Globally-unique DAG names enforced: a name collision across folders is
      FATAL at startup (server refuses to boot, listing every collision) and
      REJECTED at reload (incumbent keeps running, newcomer not booted)
    - Persistent red dashboard banner for reload-time collisions, with a
      one-click button to delete the offending DAG file
    - External library paths (external.module.path.*) documented: add custom
      calculator/transformer module directories to the import path at startup
    - Zero-message-loss ingestion: every subscriber applies backpressure
      (block-retry) instead of dropping on a full queue; Kafka consumer now
      buffers the full polled batch and delivers one message at a time

Version 3.0.0 highlights:
    - Event-driven compute loop: DAGs react to data immediately instead of
      polling on a fixed interval, collapsing per-hop latency from tens/hundreds
      of milliseconds to sub-millisecond on the in-process path
    - Subscribers wake the compute loop on message arrival (notify hook); the
      idle poll interval is configurable (idle_poll_interval) and far lower by
      default, cutting ingress latency ~180x in benchmarks
    - Suitable for near-real-time, low-latency pipelines (low-single-digit-ms
      hops) in addition to throughput workloads
    - Free-threading (Python 3.13 no-GIL) enablement + testing guide and a
      dependency compatibility checker
    - Interactive Cytoscape.js graph view (zoom/pan/drag, live node-state panel)
    - Expanded tutorials: two-subgraph showcase, parallel-DAG coordination,
      multi-worker-pool execution, and the JVM gateway pool

Version 2.2 highlights:
    - FastAPI web layer (replacing Flask) served by uvicorn, with full
      light/dark theming and locally-vendored front-end assets
    - Pluggable storage abstraction (FileSystem / S3 / Azure Blob / GCS)
    - Database-backed users, roles, and API keys (SQLAlchemy DAO layer,
      SQLite default with PostgreSQL switchable via configuration)
    - Configurable HA Manager (Zookeeper / S3 / Redis / Socket providers),
      with the active role surfaced in the navbar on every page
    - Format-agnostic configuration (YAML or .properties) with
      ${VAR:default} resolution and env / command-line overrides
    - AWS (SQS / Kinesis / SNS) and Azure (Service Bus / Event Hubs)
      managed-messaging pub/sub, plus S3 / Azure Blob / GCS object stores
    - Market-aware scheduling: time windows with duration syntax,
      day-of-week allow/deny lists, and USA / Canada holiday calendars
    - Resilient wrappers for REST, SQL, Aerospike, gRPC, and cloud messaging
    - Greatly expanded automated test coverage

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

VERSION = "4.5.0"
BUILD_DATE = "2026-06-14"
APP_NAME = "DishtaYantra"


def get_version_banner() -> str:
    """Return a one-line human readable version banner."""
    return f"{APP_NAME} Compute Server v{VERSION} (build {BUILD_DATE})"
