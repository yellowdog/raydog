#!/usr/bin/env python3

import logging
import random
from datetime import datetime, timedelta
from fractions import Fraction
from os import getenv
from time import sleep

import dotenv
import ray

from raydog.raydog import RayDogCluster


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
            head_node_compute_requirement_template_id="yd-demo/yd-demo-aws-eu-west-2-split-ondemand",
            head_node_images_id="ami-0fef583e486727263",  # Ubuntu 22.04, AMD64, eu-west-2
            cluster_tag="my-ray-tag",
            cluster_lifetime=timedelta(seconds=600),
            head_node_metrics_enabled=True,
        )

        # Add the worker pools
        for _ in range(2):
            raydog_cluster.add_worker_pool(
                worker_node_compute_requirement_template_id="yd-demo/yd-demo-aws-eu-west-2-split-ondemand",
                worker_pool_node_count=2,
                worker_node_images_id="ami-0fef583e486727263",
                worker_node_metrics_enabled=True,
            )

        # Build the Ray cluster
        print("Building Ray cluster")
        private_ip, public_ip = raydog_cluster.build(
            head_node_build_timeout=timedelta(seconds=300)
        )

        cluster_address = f"ray://{public_ip}:10001"
        print(f"Head node started: {cluster_address}")

        print("Pausing to allow all worker nodes to start")
        sleep(90)

        # Run a simple application on the cluster
        print("Starting simple Ray application")
        estimate_pi(cluster_address)
        print("Finished")

        input("Hit enter to shut down cluster ")

    finally:
        # Make sure the Ray cluster gets shut down
        raydog_cluster.shut_down()


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


def estimate_pi(cluster_address):
    # Initialize Ray
    print("Connecting Ray to", cluster_address)
    ray.init(address=cluster_address, logging_level=logging.ERROR)

    # How long do we want to spend working?
    idealtime = timedelta(seconds=60)
    mintime = 0.75 * idealtime

    # Get several estimates of pi
    batches = 100
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

# 
# Entry point
if __name__ == "__main__":
    main()
