#!/usr/bin/env bash
# Run the example job on the cluster. Opens an SSH tunnel to the head node's job
# server for as long as the job takes, so nothing has to be open to the network.
#   ./submit-job.sh [template.yaml]
set -euo pipefail
cd "$(dirname "$0")"
TEMPLATE="${1:-raydog.yaml}"
python render.py "$TEMPLATE" ".rendered/$TEMPLATE"
CONFIG=".rendered/$TEMPLATE"

ray dashboard "$CONFIG" > /dev/null 2>&1 &   # forwards localhost:8265 to the head
TUNNEL=$!
trap 'kill "$TUNNEL" 2> /dev/null || true' EXIT
for _ in $(seq 1 30); do
    curl --silent --output /dev/null http://localhost:8265 && break
    sleep 1
done
if ! curl --silent --output /dev/null http://localhost:8265; then
    echo "submit-job.sh: cannot reach the head node's job server through an SSH" \
         "tunnel; is the cluster up? ('ray dashboard $CONFIG' shows why not)" >&2
    exit 1
fi

# Only the job's own directory is uploaded to the cluster
ray job submit --address http://localhost:8265 --working-dir job -- python sleep_job.py
