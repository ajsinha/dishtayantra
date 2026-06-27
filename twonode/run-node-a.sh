#!/bin/sh
# Node A = UI plane (port 18080). Sets DY_SECRET_KEY (needed to encrypt
# trusted-server keys at rest). Override DY_SECRET_KEY in your environment for
# anything beyond local testing.
cd "$(dirname "$0")/.."          # repo root
if [ -z "$DY_SECRET_KEY" ]; then
    DY_SECRET_KEY="twonode-demo-secret-change-me"
fi
export DY_SECRET_KEY
export DY_CONFIG_FILE="twonode/configs/nodeA.yaml"
mkdir -p data/twonode/nodeA twonode/logs
echo "Starting Node A (UI plane / UI-Plane-A) on http://127.0.0.1:18080"
exec python3 run_server.py
