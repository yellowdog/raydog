#!/usr/bin/env python3

# Dimensioning the cluster: total workers is the product of the vars below
# Each compute requirement is split across eu-west-2{a, b, c}
# Note: EBS limit of 500 per provisioning request, so max WORKER_NODES_PER_POOL
#       should be 1,500 (500 instances per AZ)

WORKER_NODES_PER_POOL = 5  # Must be <= 1500, assuming split across 3 AZs
NUM_WORKER_POOLS = 1

# Sleep duration for each Ray task in the test job
TASK_SLEEP_TIME_SECONDS = 30

import logging
import time
from datetime import datetime, timedelta
from os import getenv

import dotenv
import ray

from raydog.raydog import RayDogCluster

# This is needed temporarily because the preconfigured AMI doesn't
# have the Ray dashboard installed

RAY_HEAD_NODE_START_SCRIPT = r"""#!/usr/bin/bash
trap "ray stop; echo Ray stopped" EXIT
set -euo pipefail
VENV=/opt/yellowdog/agent/venv
source $VENV/bin/activate
/opt/yellowdog/agent/.local/bin/uv pip install -U ray[default]
ray start --head --port=6379 --num-cpus=0 --memory=0 --dashboard-host=0.0.0.0 --block
"""


def main():
    timestamp = str(datetime.timestamp(datetime.now())).replace(".", "-")

    try:
        # Read any extra environment variables from a file
        dotenv.load_dotenv(verbose=True, override=True)

        # Configure the Ray cluster
        raydog_cluster = RayDogCluster(
            yd_application_key_id=getenv("YD_API_KEY_ID"),
            yd_application_key_secret=getenv("YD_API_KEY_SECRET"),
            cluster_name=f"raytest-{timestamp}",  # Names the WP, WR and worker tag
            cluster_namespace="pwt-ray",
            head_node_compute_requirement_template_id="yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead",
            head_node_images_id="ami-00befc97a86859589",  # 'ray-test' AMI eu-west-2
            cluster_tag="my-ray-tag",
            head_node_metrics_enabled=True,
            head_node_ray_start_script=RAY_HEAD_NODE_START_SCRIPT,
        )

        # Add the worker pools
        for _ in range(NUM_WORKER_POOLS):
            raydog_cluster.add_worker_pool(
                worker_node_compute_requirement_template_id="yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayworker",
                worker_pool_node_count=WORKER_NODES_PER_POOL,
                worker_node_images_id="ami-00befc97a86859589",  # 'ray test' AMI eu-west-2
                worker_node_metrics_enabled=True,
            )

        # Build the Ray cluster
        print("Building Ray cluster")
        private_ip, public_ip = raydog_cluster.build(
            head_node_build_timeout=timedelta(seconds=600)
        )

        cluster_address = f"ray://{public_ip}:10001"
        print(f"Head node started: {cluster_address}")

        input(
            "Wait for worker nodes to join the cluster ... then hit enter to run the sample job: "
        )

        # Run a simple application on the cluster
        print("Starting simple Ray test application")
        ray_test_job(cluster_address)
        print("Finished")

        input("Hit enter to shut down cluster: ")

    finally:
        # Make sure the Ray cluster gets shut down
        print("Shutting down Ray cluster")
        raydog_cluster.shut_down()


# Define a remote task that uses 1 CPU
@ray.remote(num_cpus=1)
def ray_worker_task(task_id):
    print(f"Task {task_id} running on worker with 1 CPU")
    time.sleep(TASK_SLEEP_TIME_SECONDS)  # Simulate work (e.g., computation)
    return f"Task {task_id} completed"


def ray_test_job(cluster_address):
    print("Connecting Ray to", cluster_address)

    # Initialize Ray
    ray.init(address=cluster_address, logging_level=logging.ERROR)

    start_time = time.time()
    task_refs = [
        ray_worker_task.remote(i)
        for i in range(NUM_WORKER_POOLS * WORKER_NODES_PER_POOL)
    ]
    results = ray.get(task_refs)  # Wait for all tasks to complete

    # Print results and duration
    print(f"Results: {results[:5]} ...")  # Show first 5 results
    print(f"Total duration: {time.time() - start_time} seconds")


# Entry point
if __name__ == "__main__":
    main()
