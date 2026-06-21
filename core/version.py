"""
DishtaYantra Version Information
================================

Single source of truth for the application version. Every module, template,
banner, and document must reference this module rather than hard-coding a
version string.

Per-release highlights are kept in docs/CHANGELOG.md (not here, to keep this
file small). Most recent release:

Version 5.25.1 highlights (fix: trade generator must not choose Kafka partitions):
    - perftest/generate_trades.py computed an explicit partition number itself
      (zlib.crc32(product) % N) and passed partition=<n> to producer.send(...). On a topic with
      fewer partitions than requested (e.g. an existing 1-partition topic) this raised
      "AssertionError: Unrecognized partition" because partitions 1..N-1 did not exist.
    - Fix: the generator now provides ONLY the message key (the --partition-key field value) and
      lets Kafka's partitioner hash it over whatever partitions the topic actually has - same value
      -> same partition, and it works for any partition count. Removed partition_for()/zlib and the
      client-side per-partition counting (the producer no longer decides partitions); the summary
      now reports distinct key values. --partitions now only controls how many partitions the topic
      is CREATED with (topic provisioning), not per-message routing; the "sends will fail" warning
      became an informational note. --dry-run shows the key grouping instead of computed partition
      numbers. This mirrors the v5.20.0 publisher design (set the key, let Kafka route).
    - Docs: Message Broker Connectors Guide note clarified. No server code changed; 247 passed.
      Full history: docs/CHANGELOG.md.
"""

VERSION = "5.25.1"
BUILD_DATE = "2026-06-18"
APP_NAME = "DishtaYantra"


def get_version_banner() -> str:
    """Return a one-line human readable version banner."""
    return f"{APP_NAME} Compute Server v{VERSION} (build {BUILD_DATE})"
