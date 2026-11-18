#!/usr/bin/bash
# Starts Ray on a worker node. Runs as a YellowDog task, as the Agent's user, for as
# long as the node lives. RayDog sets RAY_HEAD_IP to the head node's private
# address, and OBSERVABILITY_HOST to the observability node's when the cluster has
# one.

set -euo pipefail

source /opt/raydog/venv/bin/activate

RAY_ARGS=(--address="$RAY_HEAD_IP:6379" --disable-usage-stats --block)
ALLOY_PID=""

if [[ -n "${OBSERVABILITY_HOST:-}" ]]; then
    # Ray's metrics on a fixed port, where Alloy collects them. Ray's worker
    # processes would otherwise take ports from 10002 up, so they start above it.
    RAY_ARGS+=(--metrics-export-port=10002 --min-worker-port=10003 --max-worker-port=19999)
    if command -v alloy > /dev/null; then
        # Sends this node's metrics and logs to the observability node
        alloy run --storage.path="$PWD/alloy-data" /etc/alloy/config.alloy \
            > alloy.log 2>&1 &
        ALLOY_PID=$!
    else
        echo "raydog: Grafana Alloy is not installed; no data will go to the" \
             "observability node from this node" >&2
    fi
fi

stop() {
    if [[ -n "$ALLOY_PID" ]]; then
        kill "$ALLOY_PID" 2> /dev/null || true
    fi
    ray stop
    echo "raydog: Ray stopped"
}
trap stop EXIT
ray stop > /dev/null 2>&1 || true

ray start "${RAY_ARGS[@]}"
