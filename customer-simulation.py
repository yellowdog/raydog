#!/usr/bin/env python3

import time

import numpy as np
import ray

# Connect to the Ray cluster
ray.init(address=f"ray://localhost:10001")


def create_array_with_mean_max(mean_value, max_value, size):
    # Choose a random number of elements to be max_value
    n_max = np.random.randint(1, size // 10)  # Ensure at least 1 but not more than 10%.

    # Generate random numbers between 0 and 1 for the remaining elements
    random_numbers = np.random.rand(size - n_max)

    # Calculate the target sum for the remaining elements
    # The sum of all elements should be: mean * size
    # Since n_max elements are fixed as max_value, the sum of the remaining elements should be: mean * size - n_max * max_value
    target_sum = mean_value * size - n_max * max_value

    # Scale the random numbers to match the target sum
    current_sum = np.sum(random_numbers)
    scaled_numbers = random_numbers * (target_sum / current_sum)

    # Combine the scaled numbers with the n_max occurrences of max_value
    final_array = np.append(scaled_numbers, [max_value] * n_max)

    # Shuffle the array to ensure the max values are not always at the end
    np.random.shuffle(final_array)

    return final_array


@ray.remote(num_cpus=1)
def task(minutes):
    time.sleep(minutes * 60)


def run_group_a1():
    return [task.remote(m) for m in create_array_with_mean_max(10.2, 30, 45200)]


def run_group_a2():
    return [task.remote(m) for m in create_array_with_mean_max(0.72, 10, 14000)]


def run_group_a3():
    return [task.remote(m) for m in create_array_with_mean_max(0.67, 2, 1080)]


def run_group_b1():
    return [task.remote(m) for m in create_array_with_mean_max(17.2, 60, 30100)]


def run_group_b2():
    return [task.remote(m) for m in create_array_with_mean_max(0.74, 20, 4680)]


def run_group_c():
    return [task.remote(m) for m in create_array_with_mean_max(1.08, 30, 31700)]


def run_group_e():
    return [task.remote(m) for m in create_array_with_mean_max(3.59, 360, 9360)]


def run_group_f():
    return [task.remote(m) for m in create_array_with_mean_max(3.08, 120, 4680)]


def run_group_g():
    return [task.remote(m) for m in create_array_with_mean_max(0.5, 5, 4140)]


def run_group_h():
    return [task.remote(m) for m in create_array_with_mean_max(3.14, 45, 1610)]


all_tasks = []
print("Adding run group a1")
a1_tasks = set(run_group_a1())
all_tasks.extend(a1_tasks)
print("Adding run group b1")
b1_tasks = set(run_group_b1())
all_tasks.extend(b1_tasks)
print("Adding run group c")
all_tasks.extend(run_group_c())
print("Adding run group e")
all_tasks.extend(run_group_e())
print("Adding run group f")
all_tasks.extend(run_group_f())
print("Adding run group g")
all_tasks.extend(run_group_g())
print("Adding run group h")
all_tasks.extend(run_group_h())
print("Converting a2 group to set")
a2_tasks = set()

try:
    while all_tasks:
        ready_tasks, all_tasks = ray.wait(all_tasks, num_returns=1)
        if ready_tasks[0] in a1_tasks:
            print(f"Removing a1 task {ready_tasks[0]}")
            a1_tasks.remove(ready_tasks[0])
            if not a1_tasks:
                print("Adding run group a2")
                a2_tasks = run_group_a2()
                all_tasks.extend(a2_tasks)
            continue
        if ready_tasks[0] in b1_tasks:
            print(f"Removing b1 task {ready_tasks[0]}")
            b1_tasks.remove(ready_tasks[0])
            if not b1_tasks:
                print("Adding run group b2")
                all_tasks.extend(run_group_b2())
            continue
        if ready_tasks[0] in a2_tasks:
            print(f"Removing a2 task {ready_tasks[0]}")
            a2_tasks.remove(ready_tasks[0])
            if not a2_tasks:
                print("Adding run group a3")
                all_tasks.extend(run_group_a3())

except:
    pass

# finally:
    # ray.shutdown()

print("All Done!")
