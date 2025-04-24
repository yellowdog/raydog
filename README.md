# RayDog: Building Ray Clusters with YellowDog

## Introduction

This is an initial proof of concept implementation that uses YellowDog to provision Ray clusters. It operates by modelling the Ray cluster as a YellowDog work requirement, and provisioning YellowDog worker pools to provide Ray cluster nodes. In its current form it creates statically dimensioned clusters, with varying types of node.

The implementation and its interface are subject to change as we gather feedback and evolve the functionality.

## Usage

The [raydog.py](raydog/raydog.py) Python module defines a `RayDogCluster` class.

1. The constructor for the `RayDogCluster` class sets up general cluster properties, and the properties needed to provision a single Ray head node.


2. The `add_worker_pool()` method sets the properties and node count for a worker pool supplying Ray worker nodes. This method can be called multiple times to add heterogeneous Ray worker nodes, with different numbers of nodes of each type.


3. The `build()` method creates the cluster, and returns the private and (if applicable) public IP addresses of the head node. The method blocks until the head node is running, but note that after it returns, Ray worker nodes will still be in the process of configuring and joining the cluster. A timeout can be set that cancels cluster creation if exceeded while the head node is still being configured.


4. The `shut_down()` method shuts down the Ray cluster by cancelling its work requirement, aborting all its tasks representing the head node and worker nodes, and shutting down its worker pools.

## Usage Example

An example of usage can be found in [usage-example.py](usage-example.py).

## Requirements

Requires Python v3.10 or later. The [raydog.py](raydog/raydog.py) module requires the [yellowdog-sdk](https://pypi.org/project/yellowdog-sdk) package, which can be installed from PyPI.

Provisioned nodes must advertise the `bash` task type, in order to run the startup scripts for setting up the Ray head node and worker nodes.

## Ray Head Node and Worker Node Start-Up Scripts

### Default Scripts

Default Bash scripts are provided for the `head_node_ray_start_script` and `worker_node_ray_start_script` arguments. The default scripts are found within [raydog.py](raydog/raydog.py).

If these scripts are used, the Ray virtual environment to be activated must be situated in the YellowDog agent home directory at `/opt/yellowdog/agent/venv`.

### User-Supplied Scripts

To use your own Bash scripts, please ensure that an EXIT trap is set that stops Ray gracefully, e.g.:
```bash
trap "ray stop; echo Ray stopped" EXIT
```

When Ray is started on either the head node or worker nodes, use the `--block` option with the `ray start` command, to ensure the YellowDog task lifecycle matches that of the Ray processes.

For the script used to set up Ray worker nodes, the private IP address of the head node to connect to will be found in the environment variable `RAY_HEAD_NODE_PRIVATE_IP`.
