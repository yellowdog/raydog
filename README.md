# RayDog: Building Ray Clusters with YellowDog

## Introduction

This is an initial proof of concept implementation that uses YellowDog to provision Ray clusters. It operates by modelling the Ray cluster as a YellowDog work requirement, and provisioning YellowDog worker pools to provide Ray cluster nodes. In its current form it creates manually dimensioned clusters, containing varying node types. Worker pools containing configurable numbers of Ray worker nodes can be added and removed dynamically to expand and contract the cluster during its lifetime.

The implementation and its interface are subject to change as we gather feedback and evolve the functionality.

## Usage

The [raydog.py](raydog/raydog.py) Python module defines a `RayDogCluster` class.

1. The constructor for the `RayDogCluster` class establishes the YellowDog application credentials for accessing the platform, sets up general cluster properties, and sets the properties required to provision a single Ray head node. The task script to run the Ray head node process is specified. It optionally also sets the properties for provisioning an observability node.


2. The `add_worker_pool()` method sets the properties, node count and worker node task script for a worker pool supplying Ray worker nodes. This method can be called multiple times to add heterogeneous Ray worker nodes, with different numbers of nodes of each type. This method can be called one or more times before calling the `build()` method, or after it to allow worker pools to be added dynamically to the cluster. Each worker pool can optionally be created with a user-defined `worker_pool_internal_name`, which can subsequently be used to remove the worker pool using `remove_worker_pool_by_internal_name()`.


3. The `build()` method creates the cluster, and returns the private and (if applicable) public IP addresses of the head node. It also provisions any worker pools defined by the `add_worker_pool()` method prior to `build()` being invoked. The method blocks until the head node is running, but note that after it returns, Ray worker nodes will still be in the process of configuring and joining the cluster. A timeout can be set that cancels cluster creation if exceeded while the head node is still being configured.


4. The `remove_worker_pool()` method will remove a Ray worker node worker pool by its ID. This causes the compute requirement associated with the worker pool to be terminated immediately, and the worker node tasks running on the nodes will fail. The nodes should be gracefully removed from the Ray cluster, which will keep running.


5. The `shut_down()` method shuts down the Ray cluster by cancelling its work requirement, aborting all the tasks representing the head node and worker nodes, and shutting down all worker pools.

## Usage Example

An example of usage can be found in [usage-example.py](usage-example.py).

## Requirements

Requires Python v3.10 or later. The [raydog.py](raydog/raydog.py) module requires the [yellowdog-sdk](https://pypi.org/project/yellowdog-sdk) package, which can be installed from PyPI.

Provisioned nodes must advertise the `bash` task type, in order to run the startup scripts for setting up the Ray head node and worker nodes.

## Ray Head Node and Worker Node Start-Up Scripts

### Default Scripts

Default Bash scripts are provided for the following arguments in the constructor, and are found in the [scripts](scripts) directory:

- [`head_node_ray_start_script`](scripts/head-node-task-script.sh) in the `RayDogCluster()` constructor (required)
- [`worker_node_ray_start_script`](scripts/worker-node-task-script.sh) in the `add_worker_pool()` method (required)
- [`observability_node_start_script`](scripts/observability-node-task-script.sh) in the `RayDogCluster()` constructor (required if observability is enabled)

- If these scripts are used 'as-is', the Ray virtual environment to be activated must be situated in the YellowDog agent home directory at `/opt/yellowdog/agent/venv`.

A default [userdata script](scripts/node-setup-userdata.sh) is also supplied that is suitable for configuring base Ubuntu 22.04 nodes by:

- Installing the YellowDog Agent
- Installing Python and Ray
- Installing the observability packages

### User-Supplied Scripts

To use your own Bash task scripts, please ensure that an EXIT trap is set that stops Ray gracefully, e.g.:
```bash
trap "ray stop; echo Ray stopped" EXIT
```

When Ray is started on either the head node or worker nodes, use the `--block` option with the `ray start` command, to ensure the YellowDog task lifecycle matches that of the Ray processes.

For the script used to set up Ray worker nodes, the private IP address of the head node to connect to will be found in the environment variable `RAY_HEAD_NODE_PRIVATE_IP`.

## Creating SSH Tunnels for the Ray client, dashboard, etc.

The utility class [`RayTunnels`](utils/ray_ssh_tunnels.py) allows SSH tunnels to be created using a local private key to SSH to the public IP of the Ray head node. The class is also used for establishing the required tunnels if observability is used. The client, etc., can then be accessed using, e.g., `localhost:10001`.

The `RayTunnels` class depends on the `sshtunnel` package.

## Observability

An **observability node** can optionally be added to the RayDog cluster, hosting Prometheus and Grafana to supply information to the Ray dashboard. The node is added by setting the `RayDogCluster` constructor argument `enable_observability` to `True`, supplying the required values for instance provisioning, and for the task script that starts up the required observability processes.

The private IP address of the observability node is available in the `OBSERVABILITY_HOST` environment variable supplied to the head node task and the worker node tasks, and this can be used to set up the required observability connections.
