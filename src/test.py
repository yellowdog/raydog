#!/usr/bin/env python3

import ray
import time
import random

from raydogclient import *
from raydoghead import *

def main():
    raydog = RayDogClient()
    try:
        raydog.create_worker_pool()
        raycluster = raydog.start_head_node()

        headnode = RayDogHead()
        headnode.add_workers(2)

        print("Starting simple Ray application")
        hello_ray(raycluster)
        print("Finished")

        input("Hit enter to shut down cluster ")

    finally:
        raydog.shutdown()

def hello_ray(raycluster):
    print("Connecting Ray to", raycluster)

    # Initialize Ray
    ray.init(address=raycluster)

    # Simulated dataset
    DATA_SIZE = 10
    DATA = [random.randint(1, 100) for _ in range(DATA_SIZE)]

    @ray.remote
    def process_data(data_chunk):
        time.sleep(random.uniform(0.1, 0.5))  # Simulate processing time
        return sum(data_chunk)

    # Split data into chunks
    CHUNK_SIZE = 2
    chunks = [DATA[i:i + CHUNK_SIZE] for i in range(0, len(DATA), CHUNK_SIZE)]

    # Process data in parallel
    futures = [process_data.remote(chunk) for chunk in chunks]
    results = ray.get(futures)

    total_sum = sum(results)
    print(f"Processed dataset: {DATA}")
    print(f"Chunk results: {results}")
    print(f"Total sum: {total_sum}")

    # Shutdown Ray
    ray.shutdown()


# call the main function
if __name__ == "__main__":
    main()



