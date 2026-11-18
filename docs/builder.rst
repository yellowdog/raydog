The Builder
===========

The Builder is a Python API for creating a Ray cluster and controlling it directly: you decide when it is built, how many workers of which kinds it has, and when it ends. Most clusters are better served by :doc:`autoscaler`; the Builder is for a cluster that is part of a larger program, has a size you choose, must be created without SSH access to the head node, or must not hold YellowDog credentials. It also reaches its full size sooner, because the workers' instances are requested together with the head's.

Describing a cluster
--------------------

A :class:`~yellowdog_ray.builder.spec.ClusterSpec` names the cluster and describes its head node. A :class:`~yellowdog_ray.core.cluster.NodeSpec` describes one kind of node: where it runs, and what it runs.

.. code-block:: python

   from datetime import timedelta
   from yellowdog_ray import ClusterSpec, NodeSpec

   head = NodeSpec(
       compute_requirement_template="my-namespace/ray-head",
       start_script=open("scripts/head-node-task-script.sh").read(),
       images_id="my-namespace/ubuntu-26-04",   # optional: an image family (or an image ID)
       userdata=open("scripts/node-setup-userdata.sh").read(),   # optional: run at first boot
   )

   spec = ClusterSpec(
       name="demo",
       namespace="my-namespace",
       head=head,
       lifetime=timedelta(hours=8),             # optional: 24 hours by default
   )

``NodeSpec`` fields:

``compute_requirement_template``
   The YellowDog compute requirement template for the node's instances (``namespace/name``, or an ID).

``start_script``
   A Bash script that starts Ray on the node. It runs as a YellowDog task for as long as the node is part of the cluster, so it must not return while Ray is running: use ``ray start --block``. Worker scripts are given the head node's private address in ``RAY_HEAD_IP``.

``images_id``, ``userdata``, ``instance_tags``, ``metrics_enabled``
   Optional: the image (a YellowDog image family as ``namespace/name`` or by ID, or a cloud image ID: see :doc:`nodes`); a script to run when an instance first boots; tags for the cloud instances; and whether YellowDog collects node metrics.

``ClusterSpec`` also accepts ``tag`` (applied to the YellowDog work requirement and compute requirements), ``build_timeout``, ``idle_shutdown`` and ``observability`` (see below).

Building and shutting down
--------------------------

.. code-block:: python

   from yellowdog_ray import RayDogCluster

   with RayDogCluster(spec) as cluster:
       cluster.add_worker_pool("workers", worker, count=4)
       head_node = cluster.build()
       print(head_node.private_ip, head_node.public_ip)
       ...

Creating the ``RayDogCluster`` object does nothing. :meth:`~yellowdog_ray.builder.cluster.RayDogCluster.build` creates the cluster and returns once the head node is running, with its addresses. If anything goes wrong, including the head node failing to start within the build timeout, everything created so far is removed and the error is raised.

A cluster name can be in use by one cluster at a time in a namespace. If a cluster of the same name is already running, ``build()`` raises :class:`~yellowdog_ray.errors.ClusterExistsError`. If one is there but no longer works, because its head node has ended, or it had none within its build timeout, or its lifetime has passed, ``build()`` removes it with a warning and goes on, as ``ray up`` does.

Leaving the ``with`` block shuts the cluster down, whether the block finished or raised. To keep the cluster running after the block, call :meth:`~yellowdog_ray.builder.cluster.RayDogCluster.detach`. Without ``with``, call :meth:`~yellowdog_ray.builder.cluster.RayDogCluster.shut_down` yourself, ideally in a ``finally``.

``shut_down()`` returns as soon as the shutdown has been requested. ``shut_down(wait=True)`` returns only when no instance is left. It is safe to call more than once; if it raises :class:`~yellowdog_ray.errors.TeardownError`, something may still be running and it should be called again.

Worker pools
------------

A worker pool is a named group of identical worker nodes. A cluster can have any number of them, each with its own kind of node, and they can be added, resized and removed **at any time**, before the cluster is built or while it is running.

.. code-block:: python

   cluster.add_worker_pool("cpu", cpu_node, count=8)
   cluster.add_worker_pool("gpu", gpu_node, count=0, max_count=4)   # none yet, room for four
   cluster.build()

   cluster.resize_worker_pool("gpu", 2)       # grow
   cluster.resize_worker_pool("cpu", 4)       # shrink: nodes not yet started go first

   for name, pool in cluster.worker_pools.items():
       print(name, pool.count, pool.max_count)

Pools added before ``build()`` are created by it, and their instances boot while the head node's does. A pool can be resized between zero and its ``max_count``, which is fixed when the pool is added and defaults to its initial count.

Changing a running cluster
++++++++++++++++++++++++++

Nothing about a cluster's pools is fixed by ``build()``. On a running cluster:

.. code-block:: python

   # A new kind of node, when the work turns out to need it
   cluster.add_worker_pool("highmem", highmem_node, count=2, max_count=6)

   cluster.resize_worker_pool("highmem", 6)   # more of them
   cluster.resize_worker_pool("highmem", 1)   # fewer

   # And gone again, when that part of the work is done
   cluster.remove_worker_pool("highmem")

- :meth:`~yellowdog_ray.builder.cluster.RayDogCluster.add_worker_pool` creates the pool at once and starts its nodes, which join the cluster as they come up. It needs the head node to be running, since the new workers are told where it is.
- :meth:`~yellowdog_ray.builder.cluster.RayDogCluster.resize_worker_pool` adds nodes, or stops them. Nodes are counted as they are now, so resizing to the size a pool is meant to have also replaces any that have been lost.
- :meth:`~yellowdog_ray.builder.cluster.RayDogCluster.remove_worker_pool` stops the pool's nodes and releases its instances. Work running on them is lost to Ray in the same way as if the nodes had failed, so remove a pool when it is idle. The head node and the other pools are not affected.
- All three work from any process that has attached to the cluster, not only the one that built it: see below.

A removed pool's name cannot be used again in the same cluster for 24 hours, because the platform keeps the names of ended pools that long.

Using the cluster
-----------------

``build()`` returns when the head node's start script is running; worker nodes join shortly afterwards. Nodes usually have private addresses only reachable from inside their network, so from elsewhere use an SSH tunnel:

.. code-block:: python

   import ray
   from yellowdog_ray import RayTunnels

   with RayTunnels(head_node.public_ip, user="yd-agent", private_key="~/.ssh/id_ed25519"):
       ray.init("ray://localhost:10001")          # the Ray client
       ...                                        # the dashboard is at http://localhost:8265

:meth:`~yellowdog_ray.builder.cluster.RayDogCluster.status` reports the state of the head node and of each pool. The ``head`` property gives the head node's addresses if it is running now, and ``None`` if it is not.

Picking a cluster up again
--------------------------

A cluster does not belong to the object that built it. Any process with credentials can attach to it by name:

.. code-block:: python

   cluster = RayDogCluster.attach("demo", "my-namespace")
   print(cluster.status().head_alive, dict(cluster.worker_pools))
   cluster.resize_worker_pool("cpu", 2)
   cluster.shut_down(wait=True)

An attached cluster can do everything the original could, with one exception: to *grow* a pool that another process added, pass ``start_script=`` to ``resize_worker_pool``, since this process has never seen it.

:meth:`~yellowdog_ray.builder.cluster.RayDogCluster.save_state` writes a small JSON description of the cluster (its name and identifiers, never credentials), and :meth:`~yellowdog_ray.builder.cluster.RayDogCluster.from_state` reads one back. Unlike ``attach``, ``from_state`` also finds a cluster that has already ended, so that anything it left behind can be shut down.

An observability node
---------------------

Give ``ClusterSpec`` an ``observability`` node to have one extra node started before the head, for Prometheus and Grafana for example. Its private address is given to the head and worker start scripts in ``OBSERVABILITY_HOST``.

The Builder example has a complete one: ``RAYDOG_OBSERVABILITY=1 python usage-example.py`` adds a node running Prometheus, Loki and Grafana, with Ray's dashboards, to which every other node sends its metrics and logs; Ray's dashboard then shows Grafana's graphs. The example's README describes how it is set up and how to view it.

Errors
------

Everything RayDog raises deliberately derives from :class:`~yellowdog_ray.errors.RayDogError`: :class:`~yellowdog_ray.errors.ConfigError` for a specification that cannot work, :class:`~yellowdog_ray.errors.ClusterExistsError` when a cluster of that name is already running (one that no longer works is replaced instead), :class:`~yellowdog_ray.errors.NodeStartError` and :class:`~yellowdog_ray.errors.BuildTimeoutError` when a node fails to start, :class:`~yellowdog_ray.errors.ClusterStateError` for an operation that makes no sense now, :class:`~yellowdog_ray.errors.TeardownError` when a shutdown could not be completed, and :class:`~yellowdog_ray.errors.PlatformError` when a call to YellowDog fails.

Examples
--------

Complete, runnable examples are in `examples/builder <https://github.com/yellowdog/raydog/tree/main/examples/builder>`_: ``usage-example.py`` builds a cluster, runs a small Ray job through a tunnel and shuts down (with an observability node too, given ``RAYDOG_OBSERVABILITY=1``); ``manage-example.py`` adds, resizes and removes pools and hands the cluster to "another process" through its saved state.
