#!/usr/bin/env python3

# Dimensioning the cluster: total workers is the product of the vars below
# Each compute requirement is split across eu-west-2{a, b, c}
# Note: EBS limit of 500 per provisioning request, so max WORKER_NODES_PER_POOL
#       should be 1,500 (500 instances per AZ)

WORKER_NODES_PER_POOL = 2  # Must be <= 1500, assuming split across 3 AZs
NUM_WORKER_POOLS = 2
TOTAL_WORKER_NODES = WORKER_NODES_PER_POOL * NUM_WORKER_POOLS

# Sleep duration for each Ray task in the test job
TASK_SLEEP_TIME_SECONDS = 10

ENABLE_OBSERVABILITY = False

import logging
import time
from datetime import datetime, timedelta
from getpass import getuser
from os import getenv, path

import dotenv
import ray

from raydog.raydog import RayDogCluster
from utils.ray_ssh_tunnels import RayTunnels, SSHTunnelSpec

try:
    USERNAME = getuser().replace(" ", "_").lower()
except:
    USERNAME = "default"

# Load the example userdata and task scripts
CURRENT_DIR = path.dirname(path.abspath(__file__))
SCRIPT_PATHS = {
    "node-setup-userdata": "scripts/node-setup-userdata.sh",
    "head-node-task-script": "scripts/head-node-task-script.sh",
    "worker-node-task-script": "scripts/worker-node-task-script.sh",
    "observability-node-task-script": "scripts/observability-node-task-script.sh",
}
DEFAULT_SCRIPTS = {}
for name, script_path in SCRIPT_PATHS.items():
    with open(path.join(CURRENT_DIR, script_path), "r") as file:
        DEFAULT_SCRIPTS[name] = file.read()

# Node boot setup
AMI = "ami-0c6175878f7e01e70"  # ray-246-observability-docker-8GB, eu-west-2
USERDATA = None


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
            cluster_tag=f"{USERNAME}-ray-testing",
            cluster_namespace=f"{USERNAME}-ray",
            head_node_compute_requirement_template_id=(
                "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead-big"
                if TOTAL_WORKER_NODES > 1000
                else "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead"
            ),
            head_node_images_id=AMI,
            head_node_metrics_enabled=True,
            head_node_userdata=USERDATA,
            head_node_ray_start_script=DEFAULT_SCRIPTS["head-node-task-script"],
            enable_observability=ENABLE_OBSERVABILITY,
            observability_node_compute_requirement_template_id=(
                "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead-big"
                if TOTAL_WORKER_NODES > 1000
                else "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead"
            ),
            observability_node_images_id=AMI,
            observability_node_metrics_enabled=True,
            observability_node_userdata=USERDATA,
            head_node_capture_taskoutput=True,
            observability_node_capture_taskoutput=True,
            observability_node_start_script=DEFAULT_SCRIPTS[
                "observability-node-task-script"
            ],
        )

        # Add the worker pools
        for _ in range(NUM_WORKER_POOLS):
            raydog_cluster.add_worker_pool(
                worker_node_compute_requirement_template_id=(
                    "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayworker"
                ),
                worker_pool_node_count=WORKER_NODES_PER_POOL,
                worker_node_images_id=AMI,
                worker_node_metrics_enabled=True,
                worker_node_task_script=DEFAULT_SCRIPTS["worker-node-task-script"],
                worker_node_userdata=USERDATA,
                worker_node_capture_taskoutput=True,
            )

        # Build the Ray cluster
        print("Building Ray cluster...")
        private_ip, public_ip = raydog_cluster.build(
            head_node_build_timeout=timedelta(seconds=600)
        )
        print(f"Ray head node started at public IP address: '{public_ip}'")

        tunnels = [
            RayTunnels.basic_port_forward(10001),
            RayTunnels.basic_port_forward(8265),
        ]

        if ENABLE_OBSERVABILITY:
            tunnels.append(
                SSHTunnelSpec(
                    "localhost",
                    3000,
                    str(raydog_cluster.observability_node_private_ip),
                    3000,
                )
            )
            tunnels.append(
                SSHTunnelSpec(
                    "localhost",
                    9090,
                    str(raydog_cluster.observability_node_private_ip),
                    9090,
                )
            )

        # Allow time for the API and Dashboard to start before creating
        # the SSH tunnels
        print("Waiting for Ray services to start...")
        time.sleep(20)
        ssh_tunnels = RayTunnels(
            ray_head_ip_address=public_ip,
            ssh_user="yd-agent",
            private_key_file="private-key",
            ray_tunnel_specs=tunnels,
        )
        ssh_tunnels.start_tunnels()

        cluster_address = "ray://localhost:10001"
        print(
            f"Ray head node and SSH tunnels started; using client at: {cluster_address}"
        )
        print("Ray dashboard: http://localhost:8265")

        if ENABLE_OBSERVABILITY:
            print("Grafana:       http://localhost:3000")
            print("Prometheus:    http://localhost:9090")

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
            print("Shutting down SSH tunnels")
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
