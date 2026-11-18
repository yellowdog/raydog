#!/usr/bin/bash
# Starts Ray on a worker node. Runs as a YellowDog task for as long as the node
# lives. RayDog sets RAY_HEAD_IP to the head node's private address.

set -euo pipefail

source /opt/raydog/venv/bin/activate

trap 'ray stop; echo "raydog: Ray stopped"' EXIT
ray stop > /dev/null 2>&1 || true

ray start --address="$RAY_HEAD_IP:6379" --disable-usage-stats --block
