"""
A small Ray job that needs more CPUs than the head node offers, so that the
autoscaler has to add worker nodes to run it, and removes them again afterwards.
"""

import time

import ray

# Eight one-CPU tasks fill the example's four two-CPU workers exactly. More would
# wait for a free slot.
TASKS = 8
CPUS_PER_TASK = 1
SLEEP_SECONDS = 30


@ray.remote(num_cpus=CPUS_PER_TASK)
def sleep_task(number: int) -> str:
    time.sleep(SLEEP_SECONDS)
    return f"task {number} ran on {ray.util.get_node_ip_address()}"


ray.init()
for line in ray.get([sleep_task.remote(n) for n in range(TASKS)]):
    print(line)
