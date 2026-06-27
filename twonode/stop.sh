#!/bin/sh
# Stop both nodes started from this kit (matches on their DY_CONFIG_FILE).
for n in nodeA nodeB; do
    pids=$(pgrep -f "twonode/configs/$n.yaml" 2>/dev/null)
    if [ -n "$pids" ]; then
        echo "Stopping $n (pids: $pids)"
        kill $pids 2>/dev/null
    else
        echo "$n not running"
    fi
done
