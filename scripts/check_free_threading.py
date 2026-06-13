#!/usr/bin/env python3
"""
Free-threading compatibility check for DishtaYantra.

Run this on a free-threaded Python 3.13+ interpreter (a "cp313t" build) to find
out which dependencies are safe and which ones silently re-enable the GIL.

What it does:
  1. Reports whether the interpreter supports free-threading and whether the GIL
     is currently disabled.
  2. Imports each native/important dependency one at a time, and after EACH
     import checks whether the GIL got re-enabled (a C extension that is not
     marked free-threading-ready will flip it back on at import time).
  3. Prints a clear PASS / RE-ENABLED-GIL / MISSING table and a final verdict.

Usage:
    python3.13t scripts/check_free_threading.py
    # or, on a normal build, it will tell you it is not a free-threaded interpreter

This script only imports modules; it does not start the server or open sockets.
"""

import importlib
import sys


# (import_name, pip_name, why_it_matters)
DEPENDENCIES = [
    ("fastapi", "fastapi", "web framework"),
    ("uvicorn", "uvicorn", "ASGI server (has optional C speedups)"),
    ("jinja2", "jinja2", "templating"),
    ("sqlalchemy", "SQLAlchemy", "user/role/api-key store"),
    ("kafka", "kafka-python", "Kafka pub/sub (pure-python)"),
    ("confluent_kafka", "confluent-kafka", "Kafka pub/sub (C/librdkafka)"),
    ("stomp", "stomp.py", "ActiveMQ pub/sub"),
    ("pymysql", "pymysql", "MySQL SQL pub/sub"),
    ("psycopg2", "psycopg2-binary", "Postgres SQL pub/sub (C)"),
    ("aerospike", "aerospike", "Aerospike pub/sub (C)"),
    ("redis", "redis", "Redis pub/sub / cache"),
    ("kazoo", "kazoo", "ZooKeeper HA provider"),
    ("polars", "polars", "data processing (Rust)"),
    ("pandas", "pandas", "data processing (C/Cython)"),
    ("pyarrow", "pyarrow", "Arrow columnar (C++)"),
    ("py4j", "py4j", "JVM gateway bridge (pure-python)"),
    ("lmdb", "lmdb", "cross-worker zero-copy store (C)"),
    ("psutil", "psutil", "worker CPU/memory metrics (C)"),
    ("prometheus_client", "prometheus-client", "metrics export"),
    ("msgpack", "msgpack", "message packing (C, has pure-py fallback)"),
    ("yaml", "PyYAML", "configuration"),
]


def gil_enabled():
    """Return True/False if we can tell whether the GIL is enabled, else None."""
    # sys._is_gil_enabled() exists on free-threading-capable builds (3.13+).
    fn = getattr(sys, "_is_gil_enabled", None)
    if fn is None:
        return None
    try:
        return bool(fn())
    except Exception:
        return None


def main():
    print("=" * 70)
    print("DishtaYantra free-threading compatibility check")
    print("=" * 70)
    print(f"Python: {sys.version}")

    # Detect free-threading capability.
    has_check = hasattr(sys, "_is_gil_enabled")
    py_ver = sys.version_info
    if py_ver < (3, 13) or not has_check:
        print(
            "\nThis interpreter is NOT a free-threaded build (no "
            "sys._is_gil_enabled).\nInstall a 3.13+ free-threaded build "
            "(e.g. 'python3.13t') and re-run this script there.\n"
            "This run will still import the dependencies and report failures,\n"
            "but it cannot detect GIL re-enabling."
        )

    start_state = gil_enabled()
    if start_state is None:
        print("GIL state at start: <cannot determine on this build>")
    else:
        print(f"GIL enabled at start: {start_state} "
              f"({'free-threading ACTIVE' if not start_state else 'GIL ON'})")

    print("\n" + "-" * 70)
    print(f"{'DEPENDENCY':<22}{'RESULT':<22}{'NOTES'}")
    print("-" * 70)

    reenabled_by = []
    missing = []
    ok = []

    for import_name, pip_name, why in DEPENDENCIES:
        before = gil_enabled()
        try:
            importlib.import_module(import_name)
        except ImportError:
            missing.append((pip_name, why))
            print(f"{pip_name:<22}{'MISSING (skipped)':<22}{why}")
            continue
        except Exception as exc:  # noqa: BLE001 - report any import-time failure
            print(f"{pip_name:<22}{'IMPORT ERROR':<22}{type(exc).__name__}: {exc}")
            continue

        after = gil_enabled()
        if before is False and after is True:
            # The GIL was off, this import turned it back on.
            reenabled_by.append((pip_name, why))
            print(f"{pip_name:<22}{'*** RE-ENABLED GIL':<22}{why}")
        else:
            ok.append(pip_name)
            print(f"{pip_name:<22}{'ok':<22}{why}")

    print("-" * 70)
    print("\nSUMMARY")
    print(f"  Imported cleanly : {len(ok)}")
    print(f"  Missing          : {len(missing)} (not installed - install if you use them)")
    print(f"  Re-enabled GIL   : {len(reenabled_by)}")

    if reenabled_by:
        print("\n  The following dependencies RE-ENABLED the GIL (no free-threaded")
        print("  wheel for this interpreter). Free-threading will NOT be active")
        print("  while these are imported. Upgrade to a cp313t wheel or remove them:")
        for pip_name, why in reenabled_by:
            print(f"    - {pip_name}  ({why})")

    end_state = gil_enabled()
    print()
    if end_state is None:
        print("VERDICT: cannot determine GIL state on this interpreter "
              "(not a free-threaded build).")
        return 2
    if end_state is True and start_state is False:
        print("VERDICT: FAIL - the GIL was re-enabled after importing dependencies.")
        print("         Free-threading is effectively OFF for this app right now.")
        return 1
    if end_state is False:
        print("VERDICT: PASS - GIL remained disabled after importing all "
              "available dependencies.")
        print("         Proceed to functional + stress testing (see the guide).")
        return 0
    print("VERDICT: GIL is ON (this is a standard build, or it was never off).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
