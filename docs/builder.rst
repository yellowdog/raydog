RayDog Cluster Builder
======================

The RayDog cluster builder uses YellowDog to provision Ray clusters manually. In its initial form it creates manually dimensioned clusters, containing varying node types. Worker pools containing configurable numbers of Ray worker nodes can be added and removed dynamically to expand and contract the cluster during its lifetime.

Usage
-----

The ``yellowdog_ray.raydog.builder`` module defines a ``RayDogCluster`` class.

1. The constructor for the ``RayDogCluster`` class establishes the YellowDog application credentials for accessing the platform, sets up general cluster properties, and sets the properties required to provision a single Ray head node. The task script to run the Ray head node process is specified. It optionally also sets the properties for provisioning an observability node running Prometheus and Grafana.


2. The ``add_worker_pool()`` method sets the properties, node count and worker node task script for a worker pool supplying Ray worker nodes. This method can be called multiple times to add heterogeneous Ray worker nodes, with different numbers of nodes of each type. This method can be called one or more times before calling the ``build()`` method, or after it to allow worker pools to be added dynamically to the cluster. Each worker pool can optionally be created with a user-defined ``worker_pool_internal_name``, which can subsequently be used to remove the worker pool using ``remove_worker_pool_by_internal_name()``.


3. The ``build()`` method creates the cluster, and returns the private and (if applicable) public IP addresses of the head node. It also provisions any worker pools defined by the ``add_worker_pool()`` method prior to ``build()`` being invoked. The method blocks until the head node is running. After it returns, Ray worker nodes will still be in the process of configuring and joining the cluster. A timeout can be set that cancels cluster creation if exceeded while the head node is still being configured.


4. The ``remove_worker_pool()`` method will remove a Ray worker node worker pool by its ID. This causes the compute requirement associated with the worker pool to be terminated immediately, and the worker node tasks running on the nodes will fail (this is by design). The nodes should be gracefully removed from the Ray cluster, which will remain operational with a reduced node count.


5. The ``shut_down()`` method shuts down the Ray cluster by cancelling its associated work requirement, aborting all the tasks representing the head node and worker nodes, and shutting down all worker pools. The cancellation of the work requirement is by design.

Requirements
------------

All provisioned nodes (head, worker, and optionally observability) must advertise the ``bash`` task type, in order to run the startup scripts for setting up and running the Ray head node and worker nodes.

Ray Head Node and Worker Node Start-Up Scripts
----------------------------------------------

Default Scripts
^^^^^^^^^^^^^^^

Default Bash scripts are provided for the script arguments in the constructor, and are found in the ``examples/builder/scripts`` directory on GitHub: https://github.com/yellowdog/raydog/tree/main/examples/builder/scripts.

If these scripts are used 'as-is', the Ray virtual environment to be activated must be situated in the YellowDog agent home directory at ``/opt/yellowdog/agent/venv``.

A default userdata node setup script is also supplied that is suitable for configuring base Ubuntu 22.04 nodes by:

- Installing the YellowDog Agent
- Installing Python and Ray
- Installing the observability packages

User-Supplied Scripts
^^^^^^^^^^^^^^^^^^^^^

To use your own Bash task scripts, there are three requirements:

1. ensure that an EXIT trap is set that stops Ray gracefully, e.g.::

    trap "ray stop; echo Ray stopped" EXIT

2. When Ray is started on either the head node or worker nodes, use the ``--block`` option with the ``ray start`` command, to ensure the YellowDog task lifecycle matches that of the Ray processes.


3. For the script used to set up Ray worker nodes, the private IP address of the head node to connect to will be found in the environment variable ``RAY_HEAD_NODE_PRIVATE_IP``. For example, use the command line option ``--address=$RAY_HEAD_NODE_PRIVATE_IP:6379`` when starting the Ray processes.

Creating SSH Tunnels for the Ray client, dashboard, etc.
--------------------------------------------------------

The utility class ``RayTunnels`` in module ``yellowdog_ray.utils.ray_ssh_tunnels`` allows SSH tunnels to be created using a local private key to SSH to the public IP of the Ray head node. The class is also used for establishing the required tunnels if observability is used. The client, etc., can then be accessed using, e.g., ``localhost:10001``.

By default, tunnels are set up for the client on port ``10001``, and the Ray dashboard on port ``8265``.

Observability
-------------

An **observability node** can optionally be added to the RayDog cluster, hosting Prometheus and Grafana to supply information to the Ray dashboard. The node is added by setting the ``RayDogCluster`` constructor argument ``enable_observability`` to ``True``, supplying the required values for instance provisioning, and for the task script that starts up the required observability processes.

The private IP address of the observability node is available in the ``OBSERVABILITY_HOST`` environment variable supplied to the head node task and the worker node tasks, and this can be used to set up the required observability connections.

Saving Cluster State to Allow Later Cluster Shutdown
----------------------------------------------------

When a running RayDog cluster has been built, it's possible to save the essential state required to shut down the cluster later.

Saving the Cluster State
^^^^^^^^^^^^^^^^^^^^^^^^

The ``RayDogCluster`` object has methods ``save_state_to_json()``, which returns a JSON string, and ``save_state_to_json_file(file_name)`` which writes the JSON state to the nominated file. These should be used only after the ``build()`` method has been invoked, when the required IDs are known.

The contents of the saved state are (e.g.)::

    {
      "cluster_name": "ray-prod-1748357565-702532",
      "cluster_namespace": "my-ray-namespace",
      "cluster_tag": "my-ray-production",
      "work_requirement_id": "ydid:workreq:000000:9d12fec4-faf2-4e8f-a179-80e5d8729276",
      "worker_pool_ids": [
        "ydid:wrkrpool:000000:d66bb53e-b3da-4df4-961c-204a0195e981",
        "ydid:wrkrpool:000000:b957728f-0b5d-49b7-a45d-f97aa450bb5e",
        "ydid:wrkrpool:000000:572fd46d-9c4c-4274-90c7-dcd5d95a52ba",
        "ydid:wrkrpool:000000:a5317856-182f-4d46-a64a-95549cbdf30a"
      ]
    }

Only the ``work_requirement_id`` and ``worker_pool_ids`` properties are used; the other properties are for information only.

Using Saved Cluster State
^^^^^^^^^^^^^^^^^^^^^^^^^

To use the saved state:

1. Instantiate an object of class ``RayDogClusterProxy``, supplying the YellowDog Application Key ID and Secret to authenticate with the platform
2. Load the saved state using one of the methods ``load_saved_state_from_json()``, supplying a JSON string as the argument, or ``load_saved_state_from_json_file()``, supplying the name of a file containing JSON content as the argument
3. Invoke the ``shut_down()`` method; this will cancel the work requirement (aborting executing tasks) and shut down all worker pools

Caveats
^^^^^^^

This feature must be used with caution:

1. Only save the state for a RayDog cluster that has already been built, and note that any subsequent changes (adding or removing worker pools) will not be reflected in existing saved state
2. When loading state into a ``RayDogClusterProxy`` object the cluster state may now be invalid and the ``shut_down()`` method will throw exceptions if asked to operate on a stale work requirement or worker pools.

Usage Examples
--------------

Usage examples can be found on GitHub: https://github.com/yellowdog/raydog/tree/main/examples/builder
