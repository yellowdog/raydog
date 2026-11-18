#!/usr/bin/bash
# Starts Ray on the head node, in a container. Runs as a YellowDog task, as the
# Agent's user, for as long as the cluster lives.

set -euo pipefail

# The image and the RayDog package to use, as recorded by node-setup-docker.sh
[[ -f /etc/raydog-node.env ]] && source /etc/raydog-node.env
RAY_IMAGE="${RAY_IMAGE:-rayproject/ray:2.60.0-py312}"
RAYDOG_PIP_SPEC="${RAYDOG_PIP_SPEC:-yellowdog-ray}"
CONTAINER="raydog-head-$$"

# `ray up` copies these to the head node (the host, not the container) over SSH:
# the cluster config, the YellowDog credentials the autoscaler needs, and last of
# all the key.
CONFIG="$HOME/ray_bootstrap_config.yaml"
KEY="$HOME/ray_bootstrap_key.pem"
for _ in $(seq 1 150); do
    [[ -f "$KEY" && -f "$CONFIG" && -d "$HOME/.raydog/credentials" ]] && break
    sleep 2
done
if [[ ! -f "$KEY" || ! -f "$CONFIG" || ! -d "$HOME/.raydog/credentials" ]]; then
    echo "raydog: the files from 'ray up' did not arrive within 5 minutes" >&2
    exit 1
fi

trap 'docker stop "$CONTAINER" > /dev/null 2>&1 || true; echo "raydog: Ray stopped"' EXIT

# - The files from `ray up` are readable only by this user, so the container runs
#   as root, which can read them through the read-only mounts.
# - Ray's autoscaler looks for the key and the credentials under its own home
#   directory, at the same relative paths as on the host: hence the mounts at
#   /root/ray_bootstrap_key.pem and /root/.raydog. Without the key it starts no
#   workers. The same goes for anything you add to `file_mounts` in the cluster
#   config: the autoscaler stops if a mounted path is missing, so mount each one
#   here, at the same place under /root as it has under this user's home directory.
# - Host networking lets the workers reach Ray, and SSH tunnels reach the
#   dashboard, exactly as without a container.
# - Nothing secret is on a command line.
# - RayDog sets the RAY_ variables: the three that Ray's autoscaler v2 needs, and
#   the resources and labels the head node type declares, which `ray start` then
#   uses in place of its own options. Each is passed on only when set.
docker run --rm --init --name "$CONTAINER" --user 0 -e HOME=/root \
    --network=host --shm-size=2gb --ulimit nofile=65536 \
    -e RAY_enable_autoscaler_v2 -e RAY_CLOUD_INSTANCE_ID -e RAY_NODE_TYPE_NAME \
    -e RAY_OVERRIDE_RESOURCES -e RAY_OVERRIDE_LABELS \
    -v "$CONFIG:/raydog/cluster.yaml:ro" \
    -v "$KEY:/root/ray_bootstrap_key.pem:ro" \
    -v "$HOME/.raydog:/root/.raydog:ro" \
    -e RAYDOG_PIP_SPEC="$RAYDOG_PIP_SPEC" \
    "$RAY_IMAGE" \
    bash -c 'pip install --quiet "$RAYDOG_PIP_SPEC" &&
             exec ray start --head --port=6379 --num-cpus=0 --disable-usage-stats \
                  --autoscaling-config=/raydog/cluster.yaml --block'
