"""
DishtaYantra Version Information
================================

Single source of truth for the application version. Every module, template,
banner, and document must reference this module rather than hard-coding a
version string.

Per-release highlights are kept in docs/CHANGELOG.md (not here, to keep this
file small). Most recent release:

Version 5.18.1 highlights (fix: DAG state page crashed for Arrow DAGs - RecordBatch not JSON serializable):
    - The DAG "Details"/state page (/dag/<name>/state) passed each node's raw _input/_output to
      the template, which renders them with the Jinja `| tojson` filter. Arrow nodes hold a
      pyarrow.RecordBatch as their input/output, which is NOT JSON-serializable, so the page
      failed with "Error loading DAG state: Object of type RecordBatch is not JSON serializable".
    - Fix (routes/dashboard_routes.py): new _state_value_safe() summarizes a RecordBatch
      (schema + row count + a small row preview) into a JSON-safe dict; dicts, lists, scalars,
      and None pass through UNCHANGED, so row (non-Arrow) node state still renders as proper
      JSON. Applied to every node's input/output before rendering (covers in-process and worker
      paths). The sibling details/graph view (build_dag_view) was verified unaffected.
    - 235 passed. Full history: docs/CHANGELOG.md.
"""

VERSION = "5.18.1"
BUILD_DATE = "2026-06-18"
APP_NAME = "DishtaYantra"


def get_version_banner() -> str:
    """Return a one-line human readable version banner."""
    return f"{APP_NAME} Compute Server v{VERSION} (build {BUILD_DATE})"
