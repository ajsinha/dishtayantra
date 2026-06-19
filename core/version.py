"""
DishtaYantra Version Information
================================

Single source of truth for the application version. Every module, template,
banner, and document must reference this module rather than hard-coding a
version string.

Per-release highlights are kept in docs/CHANGELOG.md (not here, to keep this
file small). Most recent release:

Version 5.13.0 highlights (DAG Details: true per-minute throughput histogram):
    - The Details page throughput panel now shows ACTUAL message counts in 1-minute
      tumbling windows for the last 10 minutes (plus the in-progress partial minute) as a
      dependency-free inline SVG bar chart - replacing the misleading EWMA "msg/min"
      headline and the page-load-sampled sparkline. Small-font captions explain, in plain
      language, that each bar is an exact count and that the lighter last bar is the current
      (still-filling) minute; hovering a bar shows its count and how many minutes ago it was.
    - core/metrics/rate_meter.py: RateMeter gained a bounded ring of per-minute counts
      (minute_buckets()), keyed by a monotonic-minute index so it is immune to wall-clock/NTP
      jumps. O(1) memory (window+1 small ints) - it preserves the meter's no-growing-buffer
      property. Idle minutes read as a true 0; a long-idle meter reports all zeros.
    - build_dag_view aggregates these buckets across all subscribers into
      dag_stats['ingest_buckets'] and a dag_stats['last_full_minute'] headline. Because the
      view is built by the worker for worker-run DAGs (v5.12.0), the histogram works
      identically in worker-pool mode. The EWMA rate is kept but relabeled as a secondary
      "smoothed ~N/min" estimate, not the headline.
    - Removed the old per-DAG rate_history deque from the dashboard route - it was sampled
      only when a human opened the page and never worked for worker-run DAGs. 235 passed
      (234 + 1 skipped without lmdb).
      Full history: docs/CHANGELOG.md.
"""

VERSION = "5.13.0"
BUILD_DATE = "2026-06-18"
APP_NAME = "DishtaYantra"


def get_version_banner() -> str:
    """Return a one-line human readable version banner."""
    return f"{APP_NAME} Compute Server v{VERSION} (build {BUILD_DATE})"
