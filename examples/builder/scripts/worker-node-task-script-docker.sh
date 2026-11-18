#!/usr/bin/bash
# Starts Ray on a worker node, in a container. Runs as a YellowDog task for as long
# as the node lives. RayDog sets RAY_HEAD_IP to the head node's private address.
# Used when RAYDOG_USE_CONTAINERS is set (see settings.py).

set -euo pipefail

[[ -f /etc/raydog-node.env ]] && source /etc/raydog-node.env
RAY_IMAGE="${RAY_IMAGE:-rayproject/ray:2.60.0-py312}"
CONTAINER="raydog-worker-$$"

trap 'docker stop "$CONTAINER" > /dev/null 2>&1 || true; echo "raydog: Ray stopped"' EXIT

docker run --rm --init --name "$CONTAINER" \
    --network=host --shm-size=2gb --ulimit nofile=65536 \
    -e RAY_HEAD_IP="$RAY_HEAD_IP" \
    "$RAY_IMAGE" \
    bash -c 'exec ray start --address="$RAY_HEAD_IP:6379" --disable-usage-stats --block'
