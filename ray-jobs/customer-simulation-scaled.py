#!/usr/bin/env python3

import time

import numpy as np
import ray

# Scale the workload
TASK_COUNT_SCALING_FACTOR = 100  # Factor by which to reduce the number of tasks
TASK_DURATION_REDUCTION_FACTOR = 10000  # Factor by which to reduce the task durations
TASK_NUM_CPUS = 1  # Set the required CPU count per task


def scaled_count(task_count: int) -> int:
    """
    Reduce the number of tasks by a scaling factor.
    """
    # Minimum task count is 20
    return max(20, task_count // TASK_COUNT_SCALING_FACTOR)


def create_array_with_mean_max(mean_value, max_value, size):
    # Size must be >= 20

    while True:  # Loop ensures that 'mean_value * size - n_max * max_value' is > 0

        # Choose a random number of elements to be max_value
        n_max = np.random.randint(
            1, size // 10
        )  # Ensure at least 1 but not more than 10%.

        # Generate random numbers between 0 and 1 for the remaining elements
        random_numbers = np.random.rand(size - n_max)

        # Calculate the target sum for the remaining elements
        # The sum of all elements should be: mean * size
        # Since n_max elements are fixed as max_value, the sum of the remaining
        # elements should be: mean * size - n_max * max_value
        target_sum = mean_value * size - n_max * max_value

        if target_sum > 0:
            break

        size += 20  # Bump the size and try again

    # Scale the random numbers to match the target sum
    current_sum = np.sum(random_numbers)
    scaled_numbers = random_numbers * (target_sum / current_sum)

    # Combine the scaled numbers with the n_max occurrences of max_value
    final_array = np.append(scaled_numbers, [max_value] * n_max)

    # Shuffle the array to ensure the max values are not always at the end
    np.random.shuffle(final_array)

    return final_array


@ray.remote(num_cpus=TASK_NUM_CPUS)
def task(minutes):
    time.sleep(minutes * 60 / TASK_DURATION_REDUCTION_FACTOR)


def run_group_a1():
    return [
        task.remote(m)
        for m in create_array_with_mean_max(10.2, 30, scaled_count(45200))
    ]


def run_group_a2():
    return [
        task.remote(m)
        for m in create_array_with_mean_max(0.72, 10, scaled_count(14000))
    ]


def run_group_a3():
    return [
        task.remote(m) for m in create_array_with_mean_max(0.67, 2, scaled_count(1080))
    ]


def run_group_b1():
    return [
        task.remote(m)
        for m in create_array_with_mean_max(17.2, 60, scaled_count(30100))
    ]


def run_group_b2():
    return [
        task.remote(m) for m in create_array_with_mean_max(0.74, 20, scaled_count(4680))
    ]


def run_group_c():
    return [
        task.remote(m)
        for m in create_array_with_mean_max(1.08, 30, scaled_count(31700))
    ]


def run_group_e():
    return [
        task.remote(m)
        for m in create_array_with_mean_max(3.59, 360, scaled_count(9360))
    ]


def run_group_f():
    return [
        task.remote(m)
        for m in create_array_with_mean_max(3.08, 120, scaled_count(4680))
    ]


def run_group_g():
    return [
        task.remote(m) for m in create_array_with_mean_max(0.5, 5, scaled_count(4140))
    ]


def run_group_h():
    return [
        task.remote(m) for m in create_array_with_mean_max(3.14, 45, scaled_count(1610))
    ]


# Connect to the Ray cluster
ray.init(address=f"ray://localhost:10001")

print("Task group dependencies: {a1, b1, c, e, f, g, h}; a1 -> a2 -> a3 | b1 -> b2")

all_tasks = []
a1_tasks = set(run_group_a1())
len_a1_tasks = len(a1_tasks)
all_tasks.extend(a1_tasks)
b1_tasks = set(run_group_b1())
len_b1_tasks = len(b1_tasks)
all_tasks.extend(b1_tasks)
all_tasks.extend(run_group_c())
all_tasks.extend(run_group_e())
all_tasks.extend(run_group_f())
all_tasks.extend(run_group_g())
all_tasks.extend(run_group_h())
a2_tasks = set()

total_tasks = len(all_tasks)
print(f"a1, b1, c, e, f, g, h task groups added ({total_tasks} tasks)")

try:
    while all_tasks:
        ready_tasks, all_tasks = ray.wait(all_tasks, num_returns=1)

        # print(
        #     f"Ready task: {ready_tasks[0]} | len_ready_tasks = {len(ready_tasks)} |"
        #     f" len_all_tasks = {len(all_tasks)}"
        # )

        if ready_tasks[0] in a1_tasks:
            a1_tasks.remove(ready_tasks[0])
            # print("a1 remove")
            if not a1_tasks:
                print(f"a1 tasks complete ({len_a1_tasks} tasks)")
                a2_tasks = run_group_a2()
                print(f"Added a2 task group ({len(a2_tasks)} tasks)")
                all_tasks.extend(a2_tasks)
                total_tasks += len(a2_tasks)
            continue

        if ready_tasks[0] in b1_tasks:
            b1_tasks.remove(ready_tasks[0])
            # print("b1 remove")
            if not b1_tasks:
                print(f"b1 tasks complete ({len_b1_tasks} tasks)")
                b2_tasks = run_group_b2()
                print(f"Added b2 task group ({len(b2_tasks)} tasks)")
                all_tasks.extend(b2_tasks)
                total_tasks += len(b2_tasks)
            continue

        if ready_tasks[0] in a2_tasks:
            a2_tasks.remove(ready_tasks[0])
            # print("a2 remove")
            if not a2_tasks:
                print("a2 tasks complete")
                a3_tasks = run_group_a3()
                all_tasks.extend(a3_tasks)
                print(f"Added a3 task group ({len(a3_tasks)} tasks)")
                total_tasks += len(a3_tasks)

    print(f"all_tasks complete ({total_tasks} tasks)")

except Exception as e:
    print(f"Exception: ({e})")

finally:
    print("Shutting down Ray")
    ray.shutdown()

print("All Done!")
