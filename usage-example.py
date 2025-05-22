#!/usr/bin/env python3

import logging
import random
from datetime import datetime, timedelta
from fractions import Fraction
from os import getenv, path
from time import sleep

import dotenv
import ray

from raydog.raydog import RayDogCluster
from utils.ray_ssh_tunnels import RayTunnels

NUM_WORKER_POOLS = 2
NODES_PER_WORKER_POOL = 2
TOTAL_WORKER_NODES = NUM_WORKER_POOLS * NODES_PER_WORKER_POOL

# Load the example userdata and task scripts
CURRENT_DIR = path.dirname(path.abspath(__file__))
SCRIPT_PATHS = {
    "node-setup-userdata": "scripts/node-setup-userdata.sh",
    "head-node-task-script": "scripts/head-node-task-script.sh",
    "worker-node-task-script": "scripts/worker-node-task-script.sh",
}
DEFAULT_SCRIPTS = {}
for name, script_path in SCRIPT_PATHS.items():
    with open(path.join(CURRENT_DIR, script_path), "r") as file:
        DEFAULT_SCRIPTS[name] = file.read()

IMAGES_ID = "ami-0fef583e486727263"  # Ubuntu 22.04, AMD64, eu-west-2

MY_USERNAME = "jsm"  # Note; Match YD naming rules, lower case, etc.
WORKER_NODES_PER_POOL = 500  # Must be <= 1500, assuming split across 3 AZs
NUM_WORKER_POOLS = 2
TOTAL_WORKER_NODES = WORKER_NODES_PER_POOL * NUM_WORKER_POOLS
BATCHES_PER_NODE=10

def main():
    timestamp = str(datetime.timestamp(datetime.now())).replace(".", "-")

    raydog_cluster: RayDogCluster | None = None
    raydog_tunnels: RayTunnels | None = None

    try:
        # Read any extra environment variables from a file
        dotenv.load_dotenv(verbose=True, override=True)

        # Configure the RayDog cluster
        raydog_cluster = RayDogCluster(
            yd_application_key_id=getenv("YD_API_KEY_ID"),
            yd_application_key_secret=getenv("YD_API_KEY_SECRET"),
            cluster_name=f"raytest-{timestamp}",  # Names the WP, WR and worker tag
            cluster_namespace="pwt-ray",
            head_node_compute_requirement_template_id=(
                "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead-big"
                if TOTAL_WORKER_NODES > 1000
                else "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead"
            ),
            head_node_images_id=IMAGES_ID,
            cluster_tag="my-ray-tag",
            cluster_lifetime=timedelta(seconds=600),
            head_node_metrics_enabled=True,
            head_node_ray_start_script=DEFAULT_SCRIPTS["head-node-task-script"],
            head_node_userdata=DEFAULT_SCRIPTS["node-setup-userdata"],
        )

        # Add worker node worker pools
        for _ in range(NUM_WORKER_POOLS):
            raydog_cluster.add_worker_pool(
                worker_node_compute_requirement_template_id="yd-demo/yd-demo-aws-eu-west-2-split-ondemand",
                worker_pool_node_count=NODES_PER_WORKER_POOL,
                worker_node_images_id=IMAGES_ID,
                worker_node_metrics_enabled=True,
                worker_node_task_script=DEFAULT_SCRIPTS["worker-node-task-script"],
                worker_node_userdata=DEFAULT_SCRIPTS["node-setup-userdata"],
            )

        # Build the Ray cluster
        print("Building Ray cluster")
        private_ip, public_ip = raydog_cluster.build(
            head_node_build_timeout=timedelta(seconds=300)
        )
        print(f"Head node started at public IP: '{public_ip}'")

        cluster_address = f"ray://localhost:10001"
        print(f"Head node started: {cluster_address}")
        
        print("Waiting for Ray services to start...")
        sleep(20)
        raydog_cluster.start_tunnels()

        print("Pausing to allow all worker nodes to start")
        sleep(90)

        # Run a simple application on the cluster
        print("Starting simple Ray application")
        estimate_pi(cluster_address, TOTAL_WORKER_NODES)
        print("Finished")

        input("Hit enter to shut down cluster ")

    finally:
        # Make sure the Ray cluster gets shut down, and the SSH tunnels stopped
        if raydog_cluster is not None:
            print("Shutting down Ray cluster")
            raydog_cluster.shut_down()
            raydog_cluster.stop_tunnels()


# simple Ray example
@ray.remote
def pi4_sample(sample_count):
    """pi4_sample runs sample_count experiments, and returns the
    fraction of random coordinates that are inside a unit circle.
    """
    in_count = 0
    for i in range(sample_count):
        x = random.random()
        y = random.random()
        if x * x + y * y <= 1:
            in_count += 1
    return Fraction(in_count, sample_count)


def estimate_pi(cluster_address, num_worker_nodes):
    # Initialize Ray
    print("Connecting Ray to", cluster_address)
    ray.init(address=cluster_address, logging_level=logging.ERROR)

    # How long do we want to spend working?
    idealtime = timedelta(seconds=60)
    mintime = 0.75 * idealtime

    # Get several estimates of pi
    batches = 100 * num_worker_nodes
    while True:
        print(f"Getting {batches} estimates of pi")
        start = datetime.now()

        results = []
        for _ in range(batches):
            results.append(pi4_sample.remote(1000 * 1000))

        output = ray.get(results)
        mypi = sum(output) * 4 / len(output)

        dur = datetime.now() - start

        print(f"Estimation took {dur} seconds")
        print(f"The average estimate is {mypi} =", float(mypi))

        if dur >= mintime:
            break
        else:
            print("That was too quick. Increasing the number of estimates")
            batches = int(batches * (idealtime / dur))

    # Shutdown Ray
    ray.shutdown()


# Entry point
if __name__ == "__main__":
    main()
