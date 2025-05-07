#!/usr/bin/bash
trap "ray stop; echo Ray stopped" EXIT

set -euo pipefail

RAY_ARGS="--address=$RAY_HEAD_NODE_PRIVATE_IP:6379 --block"

if [[ -n "$OBSERVABILITY_HOST" ]]; then
    RAY_ARGS="$RAY_ARGS -metrics-export-port=10002 --min-worker-port=10003 --max-worker-port=19999"
    alloy run /etc/alloy/config.alloy &
fi

VENV=/opt/yellowdog/agent/venv
source $VENV/bin/activate
ray start $RAY_ARGS