"""
DishtaYantra Version Information
================================

Single source of truth for the application version. Every module, template,
banner, and document must reference this module rather than hard-coding a
version string.

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

VERSION = "3.0.0"
BUILD_DATE = "2026-06-13"
APP_NAME = "DishtaYantra"


def get_version_banner() -> str:
    """Return a one-line human readable version banner."""
    return f"{APP_NAME} Compute Server v{VERSION} (build {BUILD_DATE})"
