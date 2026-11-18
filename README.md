# RayDog: Ray clusters on YellowDog

RayDog runs [Ray](https://ray.io) clusters on the [YellowDog Platform](https://yellowdog.ai). YellowDog finds and provisions the compute, across clouds, regions and instance types, according to templates you define; RayDog turns that compute into a Ray cluster and keeps the two in step.

- **Autoscaler**: YellowDog as a cloud for Ray's own cluster launcher. Write a cluster YAML, run `ray up`, and Ray adds and removes workers as its workload demands. This is the usual way to use RayDog.
- **Builder**: a Python API for creating a cluster of a size you choose and controlling it from your own code. Use it when a cluster is part of a larger program, or when your machine cannot reach the head node over SSH.
- **`raydog` command**: see what clusters exist in a namespace and shut them down from anywhere, including what a failed run left behind.

Documentation: <https://docs.yellowdog.ai/raydog/index.html>. To start a cluster, run a job on it and shut it down, follow the [quick start](https://docs.yellowdog.ai/raydog/quickstart.html).

## Install

```
pip install yellowdog-ray
```

Python 3.10 or later; Ray 2.54 or later (installed with it).

## A first look

With the Autoscaler:

```
ray up cluster.yaml       # create the cluster: a head node, and workers on demand
ray down cluster.yaml     # shut it down
```

With the Builder:

```python
from yellowdog_ray import ClusterSpec, NodeSpec, RayDogCluster

head = NodeSpec(
    compute_requirement_template="my-namespace/head", start_script=HEAD_SCRIPT
)
worker = NodeSpec(
    compute_requirement_template="my-namespace/worker", start_script=WORKER_SCRIPT
)

with RayDogCluster(
    ClusterSpec(name="demo", namespace="my-namespace", head=head)
) as cluster:
    cluster.add_worker_pool("workers", worker, count=4)
    head_node = cluster.build()  # returns when the head node is running
    ...  # use Ray at head_node.private_ip
# the cluster is shut down here, whatever happened inside the block
```

And from anywhere:

```
raydog list -n my-namespace
raydog down demo -n my-namespace --wait
```

Credentials are read from `YD_API_KEY_ID` and `YD_API_KEY_SECRET`, in the environment or in a `.env` file.

## How it works

A Ray cluster is one YellowDog work requirement. Each kind of node (the head, each pool of workers) is a task group fed by its own worker pool, and each node is a task: the script that starts Ray on it. Ending the task ends the node, and YellowDog releases the instance. Every cluster has a lifetime, 24 hours by default, after which it is stopped whatever else has happened.

## In this repository

- [`examples/autoscaler`](examples/autoscaler) and [`examples/builder`](examples/builder): complete, runnable examples, with the scripts that set nodes up and start Ray.
- [`packer`](packer): a Packer template for an AWS machine image with the YellowDog Agent, Python and Ray installed, which makes nodes start much faster than setting them up at boot.
- [`docs`](docs): the documentation source.
- [`tests`](tests): the test suite; [`tests/README.md`](tests/README.md) explains its tiers, including the tests that run against a live YellowDog account.

## Development

```
uv venv && source .venv/bin/activate
make install      # uv pip install -e ".[dev,docs]"
make lint typecheck test
```

[`DEVELOPMENT.md`](DEVELOPMENT.md) is the guide for working on RayDog itself: the architecture, the rules that must stay true, what RayDog relies on YellowDog and Ray to do, testing and debugging. See [`RELEASING.md`](RELEASING.md) for how releases are made.

## Licence

Apache 2.0. See [`LICENSE`](LICENSE).
