# RayDog: Building Ray Clusters with YellowDog

## Introduction

This is an initial proof of concept implementation that uses YellowDog to provision Ray clusters. It operates by modelling the Ray cluster as a YellowDog work requirement, and provisioning YellowDog worker pools to provide Ray cluster nodes.

The implementation is subject to change as we gather feedback and extend its capabilities.

## Usage

The [raydog.py](raydog/raydog.py) Python module defines a `RayDogCluster` class.

1. The constructor for this class sets up general cluster properties, and the properties needed to provision the single Ray head node.


2. The `add_worker_pool()` method sets the properties and node count for a worker pool supplying Ray worker nodes. This method can be called multiple times to add heterogeneous Ray worker nodes.


3. The `build()` method creates the cluster. The method blocks until the head node is running, but note that Ray worker nodes will still be in the process of configuring and joining the cluster.


4. The `shut_down()` method shuts down the Ray cluster and deprovisions all Ray nodes.

An example of usage can be found in [usage-example.py](usage-example.py).

## Requirements

The [raydog.py](raydog/raydog.py) module requires the [yellowdog-sdk](https://pypi.org/project/yellowdog-sdk) package, which can be installed from PyPI.

Provisioned nodes must support the `bash` task type, in order to run the startup scripts for setting up the Ray head node and worker nodes.

## Ray Head and Worker Start Scripts

Default Bash scripts are provided for the `head_node_ray_start_script` and `worker_node_ray_start_script` arguments. The default scripts are found within [raydog.py](raydog/raydog.py).

If these scripts are used, the Ray virtual environment to be activated must be situated in the YellowDog agent home directory at `/opt/yellowdog/agent/venv`.

To use your own Bash scripts, please ensure that an EXIT trap is set that stops Ray, e.g.:
```bash
trap "ray stop; echo Ray stopped" EXIT
```

When Ray is started, use the `--block` option, to ensure the YellowDog task lifecycle matches that of the Ray processes.

For the script to set up Ray worker nodes, the private IP address of the head node will be found in the environment variable `RAY_HEAD_NODE_PRIVATE_IP`.
