#!/usr/bin/bash
    
trap "ray stop; echo Ray stopped" EXIT

set -eo pipefail

RAY_ARGS="--head --port=6379 --num-cpus=0 --memory=0 --dashboard-host=0.0.0.0 --block"

if [[ -n "$OBSERVABILITY_HOST" ]]; then
    export RAY_GRAFANA_HOST="http://$OBSERVABILITY_HOST:3000"
    export RAY_GRAFANA_IFRAME_HOST="http://localhost:3000"
    export RAY_PROMETHEUS_HOST="http://$OBSERVABILITY_HOST:9090"
    RAY_ARGS="$RAY_ARGS --metrics-export-port=10002 --min-worker-port=10003 --max-worker-port=19999"
        
    alloy run /etc/alloy/config.alloy &
fi

VENV=/opt/yellowdog/agent/venv
source $VENV/bin/activate
ray start $RAY_ARGS