#!/usr/bin/bash

set -eo pipefail

################################################################################

# Set container name using the process ID
CONTAINER_NAME=yd-container-$$

cleanup_docker() {
  CONTAINER_ID=$(docker ps -aq --filter name=$CONTAINER_NAME)
  if [ -n "$CONTAINER_ID" ]
  then
    echo
    echo "Abort received at: $(date -u "+%Y-%m-%d_%H%M%S_UTC")"
    echo "Stopping container: $CONTAINER_NAME ($CONTAINER_ID)"
    docker stop "$CONTAINER_ID" > /dev/null
    echo "Done"
  fi
}

# Trap EXIT signal and run the container cleanup function
trap cleanup_docker EXIT

################################################################################

# Note: excludes observability support

# Run the container
docker run --rm --name $CONTAINER_NAME \
           --network=host \
           --shm-size=1gb \
           --ulimit nofile=65536 \
           ray-container \
           bash -c 'ray start --disable-usage-stats --head --port=6379 \
                              --dashboard-host=0.0.0.0 \
                              --num-cpus=0 --memory=0 --block'

################################################################################
