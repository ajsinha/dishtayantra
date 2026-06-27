#!/bin/sh
# Node B = a managed service plane (port 18090). No secret needed (it holds no
# trusted servers itself), but harmless if set.
cd "$(dirname "$0")/.."          # repo root
export DY_CONFIG_FILE="twonode/configs/nodeB.yaml"
mkdir -p data/twonode/nodeB twonode/logs
echo "Starting Node B (service plane / Service-Plane-B) on http://127.0.0.1:18090"
exec python3 run_server.py
