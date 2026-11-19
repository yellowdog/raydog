#!/usr/bin/bash
# Starts Ray on the head node. Runs as a YellowDog task, as the Agent's user, for as
# long as the cluster lives. RayDog sets OBSERVABILITY_HOST to the observability
# node's private address when the cluster has one; without one, the node setup may
# have put Prometheus and Grafana on this node instead.

set -euo pipefail

source /opt/raydog/venv/bin/activate

# The dashboard listens on localhost only: reach it through an SSH tunnel
RAY_ARGS=(--head --port=6379 --num-cpus=0 --memory=0 --disable-usage-stats --block)
ALLOY_PID=""

if [[ -n "${OBSERVABILITY_HOST:-}" ]]; then
    # The dashboard draws its graphs from Prometheus and Grafana on the
    # observability node; your browser fetches Grafana's through a tunnel to
    # localhost:3000
    export RAY_GRAFANA_HOST="http://$OBSERVABILITY_HOST:3000"
    export RAY_GRAFANA_IFRAME_HOST="http://localhost:3000"
    export RAY_PROMETHEUS_HOST="http://$OBSERVABILITY_HOST:9090"
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
elif [[ -f /etc/raydog-observability ]]; then
    # Prometheus and Grafana on this node (RAYDOG_OBSERVABILITY=head): Prometheus
    # finds every node through the list of metrics endpoints Ray keeps here. Your
    # browser fetches Grafana's graphs through a tunnel to localhost:3000.
    export RAY_GRAFANA_HOST="http://localhost:3000"
    export RAY_GRAFANA_IFRAME_HOST="http://localhost:3000"
    export RAY_PROMETHEUS_HOST="http://localhost:9090"
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
