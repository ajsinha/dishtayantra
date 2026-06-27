#!/bin/sh
# One-time setup for the two-node dev environment.
# Generates per-node configs from canonical config/application.yaml and creates
# isolated data directories. Re-run any time to refresh configs.
set -e
cd "$(dirname "$0")/.."          # repo root
python3 twonode/generate_configs.py
mkdir -p data/twonode/nodeA data/twonode/nodeB twonode/logs
echo "Setup complete."
echo "  Node A (UI plane):      ./twonode/run-node-a.sh   -> http://127.0.0.1:18080"
echo "  Node B (service plane): ./twonode/run-node-b.sh   -> http://127.0.0.1:18090"
echo "Run each in its own terminal, then: python3 twonode/bootstrap.py"
