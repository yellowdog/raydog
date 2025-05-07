#!/usr/bin/env python3

MY_USERNAME = "pwt"  # Note; Match YD naming rules, lower case, etc.

# Dimensioning the cluster: total workers is the product of the vars below
# Each compute requirement is split across eu-west-2{a, b, c}
# Note: EBS limit of 500 per provisioning request, so max WORKER_NODES_PER_POOL
#       should be 1,500 (500 instances per AZ)

WORKER_NODES_PER_POOL = 2  # Must be <= 1500, assuming split across 3 AZs
NUM_WORKER_POOLS = 2
TOTAL_WORKER_NODES = WORKER_NODES_PER_POOL * NUM_WORKER_POOLS

# Sleep duration for each Ray task in the test job
TASK_SLEEP_TIME_SECONDS = 10

import logging
import time
from datetime import datetime, timedelta
from os import getenv

import dotenv
import ray
from yellowdog_client.model.range import T
from yellowdog_client.model.worker import Worker

from raydog.raydog import RayDogCluster

# This is needed temporarily because the preconfigured AMI doesn't
# have the Ray dashboard installed

from utils.ray_ssh_tunnels import RayTunnels

def main():
    timestamp = str(datetime.timestamp(datetime.now())).replace(".", "-")
    raydog_cluster: RayDogCluster | None = None
    ssh_tunnels: RayTunnels | None = None
    try:
        # Read any extra environment variables from a file
        dotenv.load_dotenv(verbose=True, override=True)

        # Configure the Ray cluster
        raydog_cluster = RayDogCluster(
            yd_application_key_id=getenv("YD_API_KEY_ID"),
            yd_application_key_secret=getenv("YD_API_KEY_SECRET"),
            cluster_name=f"raytest-{timestamp}",  # Names the WP, WR and worker tag
            cluster_tag=f"{MY_USERNAME}-ray-testing",
            cluster_namespace=f"{MY_USERNAME}-ray",
            head_node_compute_requirement_template_id=(
                "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead-big"
                if TOTAL_WORKER_NODES > 1000
                else "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead"
            ),
            head_node_images_id="ami-01d201b7824bcda1c",  # 'ray-test-8gb' AMI eu-west-2
            head_node_metrics_enabled=True,
            enable_observability=True,
            observability_node_compute_requirement_template_id="yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayworker",
            observability_node_images_id="ami-01d201b7824bcda1c",
            observability_node_metrics_enabled=True
        )

        # Add the worker pools
        for _ in range(NUM_WORKER_POOLS):
            raydog_cluster.add_worker_pool(
                worker_node_compute_requirement_template_id=(
                    "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayworker"
                ),
                worker_pool_node_count=WORKER_NODES_PER_POOL,
                worker_node_images_id="ami-01d201b7824bcda1c",  # 'ray-test-8gb' AMI eu-west-2
                worker_node_metrics_enabled=True,
            )

        # Build the Ray cluster
        print("Building Ray cluster")
        private_ip, public_ip = raydog_cluster.build(
            head_node_build_timeout=timedelta(seconds=600)
        )

        # Allow time for the API and Dashboard to start before creating
        # the SSH tunnels
        time.sleep(10)
        ssh_tunnels = RayTunnels(
            ray_head_ip_address=public_ip,
            ssh_user="yd-agent",
            private_key_file="private-key",
        )
        ssh_tunnels.start_tunnels()

        cluster_address = "ray://localhost:10001"
        print(
            f"Ray head node and SSH tunnels started; using client at: {cluster_address}"
        )
        print("Ray dashboard is available at: http://localhost:8265")

        input(
            "Wait for worker nodes to join the cluster ... then hit enter to run the sample job: "
        )

        # Run a simple application on the cluster
        print("Starting simple Ray test application")
        ray_test_job(cluster_address)
        print("Finished")

        input("Hit enter to shut down cluster: ")

    finally:
        # Make sure the Ray cluster gets shut down, and the SSH tunnels stopped
        if raydog_cluster is not None:
            print("Shutting down Ray cluster")
            raydog_cluster.shut_down()
        if ssh_tunnels is not None:
            ssh_tunnels.stop_tunnels()


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
    task_refs = [ray_worker_task.remote(i) for i in range(TOTAL_WORKER_NODES)]
    results = ray.get(task_refs)  # Wait for all tasks to complete

    # Print results and duration
    print(f"Results: {results[:5]} ...")  # Show first 5 results
    print(f"Total duration: {time.time() - start_time} seconds")

    ray.shutdown()  # Shut down Ray

# Entry point
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopped.")
