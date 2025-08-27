import time

import ray

TASK_SLEEP_SECONDS = 30
PARALLEL_TASK_COUNT = 2
CPUS_PER_TASK = 1


@ray.remote(num_cpus=CPUS_PER_TASK)
def sleep_task(task_id):
    print(f"Task {task_id} starting with {CPUS_PER_TASK} CPU(s)")
    time.sleep(TASK_SLEEP_SECONDS)
    print(f"Task {task_id} finished sleeping for {TASK_SLEEP_SECONDS} seconds")
    return f"Task {task_id} completed"


# Initialize Ray and run parallel tasks
ray.init()
tasks = [sleep_task.remote(i) for i in range(PARALLEL_TASK_COUNT)]
results = ray.get(tasks)
for result in results:
    print(result)
