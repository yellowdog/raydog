Moving from version 2
=====================

Version 3 is a rewrite, and nothing is compatible with version 2: not the Python API, not the cluster YAML, not the saved state. This page maps the old to the new.

What changed, and why
---------------------

- **One core.** The Builder and the Autoscaler now share the code that creates, finds and removes clusters, and it is tested against a simulation of the platform as well as the real thing.
- **Safer by construction.** A failed build removes what it created. Shutting down tries every step and can verify that no instance is left. No command can remove a cluster as a side effect, and clusters with similar names can no longer be confused.
- **No Valkey, no tunnel to it.** The Autoscaler no longer runs a key-value server on the head node; the state of the cluster is read from YellowDog itself.
- **Credentials stay out of YellowDog tasks.** The Autoscaler's head node now gets its credentials over SSH, not through a task's environment.
- **Fewer dependencies.** ``redis``, ``sshtunnel`` and the pinned ``paramiko`` are gone; tunnels use the OpenSSH client.
- **A command-line tool**, ``raydog``, to list, inspect, shut down and clean up clusters.
- **Lifetimes by default.** Every cluster has a 24-hour lifetime unless told otherwise.

Clusters created by version 2 are invisible to version 3: shut them down with version 2, or in the YellowDog portal, before upgrading.

The Builder
-----------

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - Version 2
     - Version 3
   * - ``from yellowdog_ray.raydog.builder import RayDogCluster``
     - ``from yellowdog_ray import RayDogCluster, ClusterSpec, NodeSpec``
   * - ``RayDogCluster(yd_application_key_id=..., cluster_name=..., head_node_compute_requirement_template_id=..., ...)``
     - ``RayDogCluster(ClusterSpec(name=..., namespace=..., head=NodeSpec(...)))``; credentials from the environment, or ``credentials=load_credentials(...)``
   * - ``head_node_ray_start_script``, ``head_node_images_id``, ``head_node_userdata``, ...
     - fields of the head's ``NodeSpec``
   * - ``enable_observability`` and the ``observability_node_*`` arguments
     - ``ClusterSpec(observability=NodeSpec(...))``
   * - ``cluster_lifetime`` (no limit by default)
     - ``ClusterSpec(lifetime=...)`` (24 hours by default; ``None`` for no limit)
   * - ``build(head_node_build_timeout=...)`` returning ``(private_ip, public_ip)``
     - ``ClusterSpec(build_timeout=...)``; ``build()`` returns a ``HeadNode``
   * - ``add_worker_pool(worker_node_compute_requirement_template_id=..., worker_pool_node_count=..., ...)`` returning an ID
     - ``add_worker_pool(name, NodeSpec(...), count, max_count=None)``
   * - ``remove_worker_pool(id)``, ``remove_worker_pool_by_internal_name(name)``
     - ``remove_worker_pool(name)``; new: ``resize_worker_pool(name, count)``
   * - ``worker_pool_ids``, ``worker_pool_internal_names``, ``worker_node_worker_pools``
     - ``worker_pools``
   * - ``save_state_to_json()``, ``save_state_to_json_file(path)``
     - ``save_state(path=None)``
   * - ``RayDogClusterProxy`` with ``load_saved_state_from_json*`` and ``shut_down()``
     - ``RayDogCluster.from_state(...)`` or ``RayDogCluster.attach(name, namespace)``: a full cluster object
   * - ``RAY_HEAD_NODE_PRIVATE_IP`` in worker scripts
     - ``RAY_HEAD_IP``
   * - ``yellowdog_ray.utils.ray_ssh_tunnels.RayTunnels(ray_head_ip_address=..., private_key_file=...)``, ``start_tunnels()``, ``stop_tunnels()``
     - ``yellowdog_ray.RayTunnels(host, user, private_key=...)``, ``start()``, ``stop()``, or ``with``; no password or passphrase arguments (use ``ssh-agent``)

The Autoscaler
--------------

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - Version 2
     - Version 3
   * - ``module: yellowdog_ray.raydog.autoscaler.RayDogNodeProvider``
     - ``module: yellowdog_ray.autoscaler.RayDogNodeProvider``
   * - ``cluster_namespace``, ``cluster_tag``
     - ``namespace``, ``tag``
   * - ``cluster_lifetime_hours`` (default 1)
     - ``lifetime_hours`` (default 24)
   * - ``head_node_build_timeout_minutes``
     - ``build_timeout_minutes``
   * - ``ray_head_node_task_script``, ``ray_worker_node_task_script``
     - ``head_start_script``, ``worker_start_script``
   * - ``head_node_tag_server_port``; Valkey on the head node; ``install-valkey.sh``
     - removed
   * - ``files_to_upload``
     - removed: scripts named with ``file:`` are included in the configuration automatically; for other files use Ray's ``file_mounts``
   * - ``auth`` repeated inside ``provider``
     - removed
   * - ``userdata`` and ``extra_userdata``
     - ``userdata``, which may be a list
   * - credentials passed to the head node's task
     - copied to the head node by ``ray up`` over SSH; nothing to configure
   * - workers needed SSH access from the head node
     - not needed

The head start script no longer starts Valkey. The node setup script should install the YellowDog Agent last (see :doc:`nodes`).
