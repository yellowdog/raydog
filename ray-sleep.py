#!/usr/bin/env python3

import time

import ray

NUM_TASKS = 10
TASK_SLEEP_SECS = 1
RAY_IP = "localhost"  # Assumes a Ray client tunnel is established

# Connect to the Ray cluster
ray.init(address=f"ray://{RAY_IP}:10001")


# Define a remote task that uses 1 CPU
@ray.remote(num_cpus=1)
def worker_task(task_id):
    print(f"Task {task_id} running on worker with 1 CPU")
    time.sleep(TASK_SLEEP_SECS)  # Simulate work (e.g., computation)
    return f"Task {task_id} completed"


start_time = time.time()
task_refs = [worker_task.remote(i) for i in range(NUM_TASKS)]
results = ray.get(task_refs)  # Wait for all tasks to complete

# Print results and duration
if NUM_TASKS > 5:
    print(f"Results: {results[:5]} ...")  # Show first 5 results
else:
    print(f"Results: {results}")
print(f"Total duration: {time.time() - start_time} seconds")

# Disconnect
ray.shutdown()
