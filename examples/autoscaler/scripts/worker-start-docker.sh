#!/usr/bin/bash
# Starts Ray on a worker node, in a container. Runs as a YellowDog task for as long
# as the node lives. RayDog sets RAY_HEAD_IP to the head node's private address.

set -euo pipefail

[[ -f /etc/raydog-node.env ]] && source /etc/raydog-node.env
RAY_IMAGE="${RAY_IMAGE:-rayproject/ray:2.60.0-py312}"
CONTAINER="raydog-worker-$$"

trap 'docker stop "$CONTAINER" > /dev/null 2>&1 || true; echo "raydog: Ray stopped"' EXIT

# RayDog sets the RAY_ variables: the three that Ray's autoscaler v2 needs, and the
# resources and labels the node type declares, which `ray start` then uses in place
# of what it finds. Each is passed on only when set.
docker run --rm --init --name "$CONTAINER" \
    --network=host --shm-size=2gb --ulimit nofile=65536 \
    -e RAY_enable_autoscaler_v2 -e RAY_CLOUD_INSTANCE_ID -e RAY_NODE_TYPE_NAME \
    -e RAY_OVERRIDE_RESOURCES -e RAY_OVERRIDE_LABELS \
    -e RAY_HEAD_IP="$RAY_HEAD_IP" \
    "$RAY_IMAGE" \
    bash -c 'exec ray start --address="$RAY_HEAD_IP:6379" --disable-usage-stats --block'
