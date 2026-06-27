#!/usr/bin/env python3
"""Generate two per-node configs from the canonical config/application.yaml.

Each node config is the canonical config with only the values that MUST differ
between co-located instances changed: server port, service instance name, and
the SQLite + flow-store paths (so the two processes never share a database).
Everything else (DAG definitions, storage root, messaging, etc.) is inherited
verbatim, and the node configs are regenerated from canonical on every setup so
they can never drift.

Run from the repository root:  python3 twonode/generate_configs.py
"""
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CANONICAL = os.path.join(REPO, "config", "application.yaml")
OUT_DIR = os.path.join(REPO, "twonode", "configs")

# (exact canonical line, replacement) per node. Exact-line matching keeps this
# safe and obvious; if canonical changes shape, the assert below fails loudly.
NODES = {
    "nodeA": {
        "  port: 5002": "  port: 18080",
        "  instance_name: DishtaYantra": "  instance_name: UI-Plane-A",
        "    path: data/dishtayantra.db":
            "    path: data/twonode/nodeA/dishtayantra.db",
        "  store_path: data/flow_history.db":
            "  store_path: data/twonode/nodeA/flow_history.db",
    },
    "nodeB": {
        "  port: 5002": "  port: 18090",
        "  instance_name: DishtaYantra": "  instance_name: Service-Plane-B",
        "    path: data/dishtayantra.db":
            "    path: data/twonode/nodeB/dishtayantra.db",
        "  store_path: data/flow_history.db":
            "  store_path: data/twonode/nodeB/flow_history.db",
    },
}


def main():
    if not os.path.exists(CANONICAL):
        sys.exit(f"canonical config not found: {CANONICAL}")
    base = open(CANONICAL, encoding="utf-8").read()
    os.makedirs(OUT_DIR, exist_ok=True)
    for node, repls in NODES.items():
        text = base
        for old, new in repls.items():
            if old not in text:
                sys.exit(f"[{node}] expected line not found in canonical "
                         f"config (it may have changed shape): {old!r}")
            text = text.replace(old, new, 1)
        header = (f"# GENERATED for the two-node dev setup ({node}). Do not edit "
                  f"by hand;\n# regenerate with: python3 twonode/generate_configs.py\n")
        out = os.path.join(OUT_DIR, node + ".yaml")
        open(out, "w", encoding="utf-8").write(header + text)
        print(f"wrote {os.path.relpath(out, REPO)}")


if __name__ == "__main__":
    main()
