#!/usr/bin/env python3

import logging
import time
from datetime import datetime, timedelta
from getpass import getuser
from os import getenv, path
from pathlib import Path

import dotenv
import ray

from raydog.raydog import RayDogCluster, RayDogWorkSpecification

# Dimensioning the cluster: total workers is the product of the vars below
# Each compute requirement is split across eu-west-2{a, b, c}
# Note: EBS limit of 500 per provisioning request, so max WORKER_NODES_PER_POOL
#       should be 1,500 (500 instances per AZ)

WORKER_NODES_PER_POOL = 10  # Must be <= 1500, assuming split across 3 AZs
NUM_WORKER_POOLS = 1
TOTAL_WORKER_NODES = WORKER_NODES_PER_POOL * NUM_WORKER_POOLS

COMMON_ENV_VARS={}
AMI="ami-067058dcad9a25efa"
USERDATA = None

# k3s stuff
# WORKERS_PER_NODE is just used as an inticator for node size. 
# Ray worker deployment requests 0.1 CPU and 1G RAM per pod, so that should be
# taken into account when selecting node size. Pods in the deployment are spread
# evenly across nodes. There's also a soft limit of about 100 pods per node.
# With k3s enabled the worker and head node start scripts are replaced by an
# installation of k3s where the nodes are labeled and tainted according to their
# role (head|worker). The deployment of the ray head node, redis and workers is
# coordinated by kubernetes on the k3s server. The worker deployment is not
# currently dynamically sized according to TOTAL_WORKERS, because starting too
# many pods at a time kills the k3s server. Therefore, gradual scaling is 
# required. Log on to the k3s node and run:
# `sudo kubectl scale deployment/ray-workers --replicas=n`
# to increase or decrease the workers in the cluster.
ENABLE_K3S = False
WORKERS_PER_NODE = 1
TOTAL_WORKERS=TOTAL_WORKER_NODES * WORKERS_PER_NODE 
RCLONE_REMOTE=":s3,region=eu-west-2,env_auth:tech.yellowdog.devsandbox.dev-platform/raydog/"

# Mutually exclusive with k3s, so ignored if k3s is enabled
ENABLE_OBSERVABILITY = False

# Sleep duration for each Ray task in the test job
TASK_SLEEP_TIME_SECONDS = 10

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

try:
    USERNAME = getuser().replace(" ", "_").lower()
except:
    USERNAME = "default"

# Use a larger head node / observability node instance if number of worker nodes
# meets or exceeds this threshold
LARGE_HEAD_NODE_THRESHOLD = 1000
CONTROL_NODE_TEMPLATE_ID = (
    "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead"
    if TOTAL_WORKER_NODES < LARGE_HEAD_NODE_THRESHOLD
    else "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead-big"
)


def mirror_folder_on_agent(local_path, bucket_dir, base) -> dict[str, str]:
    return {
        f"rclone:{bucket_dir}/{f.relative_to(base)}": f"local:{f.relative_to(base)}"
        for f in list(Path(local_path).rglob("*.*"))
    }

def main():
    timestamp = str(datetime.timestamp(datetime.now())).replace(".", "-")
    raydog_cluster: RayDogCluster | None = None
    try:
        # Read any extra environment variables from a file
        dotenv.load_dotenv(verbose=True, override=True)

        task_inputs = mirror_folder_on_agent(f"{CURRENT_DIR}/scripts/k3s", f"{RCLONE_REMOTE}setup", f"{CURRENT_DIR}/scripts/k3s")
        task_outputs = {"local:task_*": f"rclone:{RCLONE_REMOTE}raytest-{timestamp}/task-output/"}
        
        k3s = None
        if ENABLE_K3S:
            COMMON_ENV_VARS.update({
                "K3S_TOKEN": "pd0i3okrm9pyfr73",
                "OBSERVABILITY_PROMETHEUS_PORT": "30090",
                "OBSERVABILITY_GRAFANA_PORT": "30030",
            })
            k3s = RayDogWorkSpecification(
                images_id=AMI,
                capture_taskoutput=True,
                environment_variables=COMMON_ENV_VARS,
                task_script="chmod +x ./k3s-install.sh && ./k3s-install.sh server",
                task_inputs=task_inputs,
                compute_requirement_template_id=(
                    "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead-big"
                    if TOTAL_WORKERS >= 1000
                    else "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead"
                ),
            )
            
        head_node = RayDogWorkSpecification(
            compute_requirement_template_id=(
                "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead-big"
                if TOTAL_WORKERS > 1000
                else "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead"
            ),
            images_id="ami-0716f015de8fad689",
            metrics_enabled=True,
            userdata=USERDATA,
            task_script=DEFAULT_SCRIPTS['head-node-task-script'],
            capture_taskoutput=True,
            environment_variables=COMMON_ENV_VARS,
            task_inputs=task_inputs,
        )
        
        observability = None
        if ENABLE_OBSERVABILITY and not ENABLE_K3S:
            observability = RayDogWorkSpecification(
                compute_requirement_template_id=(
                    "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead-big"
                    if TOTAL_WORKER_NODES > 1000
                    else "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead"
                ),
                images_id=AMI,
                metrics_enabled=True,
                userdata=USERDATA,
                capture_taskoutput=True,
                task_script=DEFAULT_SCRIPTS[
                    "observability-node-task-script"
                ],
            ) 
        
        # Configure the Ray cluster
        raydog_cluster = RayDogCluster(
            yd_application_key_id=getenv("YD_API_KEY_ID"),
            yd_application_key_secret=getenv("YD_API_KEY_SECRET"),
            cluster_name=f"raytest-{timestamp}",  # Names the WP, WR and worker tag
            cluster_tag=f"{USERNAME}-ray-testing",
            cluster_namespace=f"{USERNAME}-ray",
            head_node_work_specification=head_node,
            observability_work_specification=observability,
            k3s_work_specification=k3s
        )

        # Add the worker pools
        for _ in range(NUM_WORKER_POOLS):
            raydog_cluster.add_worker_pool(
                worker_node_compute_requirement_template_id=(
                    "yd-demo/yd-demo-aws-eu-west-2-split-spot-rayworker"
                ),
                worker_pool_node_count=WORKER_NODES_PER_POOL,
                worker_node_images_id="ami-0716f015de8fad689",
                worker_node_metrics_enabled=True,
                worker_node_userdata=USERDATA,
                worker_node_capture_taskoutput=True,
                worker_node_task_script=DEFAULT_SCRIPTS['worker-node-task-script'],
                worker_node_task_inputs=task_inputs,
                worker_node_task_outputs=task_outputs,
                workers_per_node=WORKERS_PER_NODE,
                worker_node_environment_variables=COMMON_ENV_VARS
            )

        # Build the Ray cluster
        print("Building Ray cluster...")
        private_ip, public_ip = raydog_cluster.build(
            head_node_build_timeout=timedelta(seconds=600)
        )
        print(f"Ray head node started at public IP address: '{public_ip}'")

        # Allow time for the API and Dashboard to start before creating
        # the SSH tunnels
        print("Waiting for Ray services to start...")
        time.sleep(20)
        raydog_cluster.start_tunnels()

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
            raydog_cluster.stop_tunnels()


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
