#!/usr/bin/bash
# Starts Ray on the head node, in a container. Runs as a YellowDog task, as the
# Agent's user, for as long as the cluster lives. Used when RAYDOG_USE_CONTAINERS
# is set (see settings.py). Excludes the observability option.

set -euo pipefail

# The image to use, as recorded by the node setup script
[[ -f /etc/raydog-node.env ]] && source /etc/raydog-node.env
RAY_IMAGE="${RAY_IMAGE:-rayproject/ray:2.60.0-py312}"
CONTAINER="raydog-head-$$"

trap 'docker stop "$CONTAINER" > /dev/null 2>&1 || true; echo "raydog: Ray stopped"' EXIT

# Host networking lets the workers reach Ray, and SSH tunnels reach the Ray client
# port and the dashboard (which listens on localhost only), exactly as without a
# container.
docker run --rm --init --name "$CONTAINER" \
    --network=host --shm-size=2gb --ulimit nofile=65536 \
    "$RAY_IMAGE" \
    bash -c 'exec ray start --head --port=6379 --num-cpus=0 --memory=0 \
                  --disable-usage-stats --block'
