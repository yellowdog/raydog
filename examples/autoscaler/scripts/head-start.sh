#!/usr/bin/bash
# Starts Ray on the head node. Runs as a YellowDog task, as the Agent's user, for
# as long as the cluster lives: when this script ends, so does the cluster's head.

set -euo pipefail

source /opt/raydog/venv/bin/activate

# `ray up` copies these to the head node over SSH once it can reach it: the cluster
# config (with which Ray's autoscaler runs here), the YellowDog credentials the
# autoscaler needs, and last of all the key.
CONFIG="$HOME/ray_bootstrap_config.yaml"
KEY="$HOME/ray_bootstrap_key.pem"

for _ in $(seq 1 150); do
    [[ -f "$KEY" && -f "$CONFIG" ]] && break
    sleep 2
done
if [[ ! -f "$KEY" || ! -f "$CONFIG" ]]; then
    echo "raydog: the files from 'ray up' did not arrive within 5 minutes" >&2
    exit 1
fi

# With RAYDOG_OBSERVABILITY, the node setup put Prometheus and Grafana on this node:
# Ray's dashboard shows their graphs. Your browser fetches Grafana's at
# localhost:3000, so forward that port too (raydog tunnel -p 3000; see README.md).
if [[ -f /etc/raydog-observability ]]; then
    export RAY_GRAFANA_HOST="http://localhost:3000"
    export RAY_GRAFANA_IFRAME_HOST="http://localhost:3000"
    export RAY_PROMETHEUS_HOST="http://localhost:9090"
fi

trap 'ray stop; echo "raydog: Ray stopped"' EXIT
ray stop > /dev/null 2>&1 || true

# The dashboard and job server listen on localhost only: reach them through an SSH
# tunnel (see README.md) rather than opening them to the network.
ray start --head --port=6379 --num-cpus=0 --disable-usage-stats \
    --autoscaling-config="$CONFIG" --block
