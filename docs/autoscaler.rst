The Autoscaler
==============

The Autoscaler makes YellowDog a cloud that `Ray's cluster launcher <https://docs.ray.io/en/latest/cluster/vms/index.html>`_ can use. You describe the cluster in a YAML file and use Ray's own commands; Ray's autoscaler, running on the head node, asks for worker nodes when its workload needs them and gives them back when it does not.

.. code-block:: console

   $ ray up cluster.yaml                      # create the cluster, or update it
   $ ray exec cluster.yaml 'ray status'       # run a command on the head node
   $ ray dashboard cluster.yaml               # forward the dashboard to localhost:8265
   $ ray down cluster.yaml                    # shut it down

A cluster configuration
-----------------------

Only two parts of a Ray cluster YAML are RayDog's: the ``provider`` section, and the ``node_config`` of each node type. Everything else is Ray's and is described in the `Ray documentation <https://docs.ray.io/en/latest/cluster/vms/references/ray-cluster-configuration.html>`_.

.. code-block:: yaml

   cluster_name: demo                  # lowercase letters, digits, hyphens; max 24

   provider:
     type: external
     module: yellowdog_ray.autoscaler.RayDogNodeProvider
     namespace: my-namespace
     head_start_script: "file:scripts/head-start.sh"
     worker_start_script: "file:scripts/worker-start.sh"

   auth:
     ssh_user: yd-agent
     ssh_private_key: ~/.ssh/id_ed25519

   head_node_type: head
   max_workers: 10

   available_node_types:
     head:
       resources: {"CPU": 0}
       node_config:
         compute_requirement_template: my-namespace/ray-head
     worker:
       resources: {"CPU": 4}
       min_workers: 0
       max_workers: 10
       node_config:
         compute_requirement_template: my-namespace/ray-worker

   setup_commands: []
   head_setup_commands: []
   worker_setup_commands: []

A complete example, with every setting explained in place and the scripts it uses, is in `examples/autoscaler <https://github.com/yellowdog/raydog/tree/main/examples/autoscaler>`_.

The ``provider`` section
++++++++++++++++++++++++

.. list-table::
   :header-rows: 1
   :widths: 30 12 58

   * - Key
     - Default
     - Meaning
   * - ``type``, ``module``
     - required
     - Always ``external`` and ``yellowdog_ray.autoscaler.RayDogNodeProvider``.
   * - ``namespace``
     - required
     - The YellowDog namespace to create the cluster in.
   * - ``head_start_script``
     - required
     - A Bash script that starts Ray on the head node, with Ray's autoscaler. The text itself, or ``file:<path>``.
   * - ``worker_start_script``
     - required
     - A Bash script that starts Ray on a worker node. It is given the head node's private address in ``RAY_HEAD_IP``.
   * - ``tag``
     - none
     - A tag for the YellowDog work requirement and compute requirements.
   * - ``lifetime_hours``
     - 24
     - The cluster is stopped this long after it was created, whatever state it is in. ``none`` for no limit.
   * - ``build_timeout_minutes``
     - 10
     - How long ``ray up`` waits for the head node to start before giving up and removing what it created.
   * - ``node_idle_shutdown_seconds``
     - 60
     - How long an instance is kept after Ray has finished with its node.
   * - ``worker_start_grace_seconds``
     - 0
     - With the original autoscaler only (``autoscaler_v2: false``): how long a worker's start script may run before Ray expects to hear from the node. That autoscaler drops a worker it has not heard from within about 30 seconds of being told it is ready; raise this if the start script does something slow first, such as pulling a container image. Autoscaler v2 waits for each stage of a node's start separately, by default up to an hour for Ray to start on it, and ignores this setting.
   * - ``refresh_interval_seconds``
     - 2
     - How fresh the provider's view of the cluster's nodes must be: Ray asks for the list of nodes often, and RayDog asks YellowDog again only if its last answer is older than this. Raise it for very large clusters to reduce the load on the platform, at the price of Ray noticing changes that much later.
   * - ``autoscaler_v2``
     - true
     - Which of Ray's autoscalers runs the cluster: ``true`` for autoscaler v2, ``false`` for the original one (see `Autoscaler versions`_).
   * - ``use_internal_ips``
     - false
     - Ray's own setting: connect to the head node by its private address. Needed if the head has no public address.
   * - ``disable_node_updaters``
     - true
     - Ray's own setting, which RayDog sets and requires. (Turned off, Ray on the head node would log in to every worker over SSH and copy the cluster's files to it, the credentials among them.)

A ``file:`` path may be absolute, or relative to the directory ``ray up`` is run from or to the directory of the YAML file.

The ``node_config`` of a node type
++++++++++++++++++++++++++++++++++

.. list-table::
   :header-rows: 1
   :widths: 30 12 58

   * - Key
     - Default
     - Meaning
   * - ``compute_requirement_template``
     - required
     - The YellowDog compute requirement template for this type of node.
   * - ``images_id``
     - none
     - The image, if the template does not fix one: a YellowDog image family as ``namespace/name`` or by ID, or a cloud image ID such as an AMI. See :doc:`nodes`.
   * - ``userdata``
     - none
     - What to run when an instance first boots: text, ``file:<path>``, or a list of either, joined in order.
   * - ``instance_tags``
     - none
     - Tags for the cloud instances.
   * - ``metrics_enabled``
     - false
     - Whether YellowDog collects metrics from the nodes.

Each node type's ``max_workers`` is also the limit of its YellowDog worker pool.

What Ray requires of the file
+++++++++++++++++++++++++++++

Ray reads ``max_workers`` and the three lists ``setup_commands``, ``head_setup_commands`` and ``worker_setup_commands`` before any provider is involved, so they must be present. The command lists, and ``initialization_commands``, ``head_start_ray_commands`` and ``worker_start_ray_commands`` if given, must be empty: RayDog starts Ray from the two start scripts, and nodes are prepared by ``userdata`` or by their machine image. (RayDog itself puts one command in ``head_start_ray_commands``: an ``echo`` that says where Ray is started. Without it, ``ray up`` warns that Ray will not be started and that the head node will launch no workers, because it looks for ``ray start`` among those commands.) A mistake in the RayDog parts of the file is reported, naming the setting at fault, before anything is created.

The start scripts
-----------------

Each node runs its start script as a YellowDog task, as the YellowDog Agent's user, for as long as it is part of the cluster. The script must therefore keep running while Ray does: use ``ray start --block``.

The head script has one more job. ``ray up`` copies three files to the head node over SSH once it can reach it: the cluster configuration (``~/ray_bootstrap_config.yaml``), the credentials file described in :doc:`concepts`, and last of all an SSH key (``~/ray_bootstrap_key.pem``). The head script must wait for them, and then start Ray with ``--autoscaling-config`` pointing at the configuration. In outline:

.. code-block:: bash

   until [[ -f ~/ray_bootstrap_key.pem ]]; do sleep 2; done
   ray start --head --port=6379 --num-cpus=0 \
       --autoscaling-config=~/ray_bootstrap_config.yaml --block

The scripts in the example do this with a time limit and proper error handling; start from them.

What happens when
-----------------

``ray up`` on a new cluster creates the YellowDog work requirement and a worker pool with one instance, starts the head script on it, waits for it to be running (up to the build timeout), and then copies the files across. It returns when Ray is starting on the head node. Run again on a running cluster, it finds the cluster and leaves it as it is; if the head node's configuration has changed, the cluster is replaced.

When Ray decides it needs more workers of some type, the autoscaler on the head node creates a worker pool for that type if there is none yet, and one task per worker. A worker is reported to Ray as ready once its start script is running. When Ray has no more use for a worker (``idle_timeout_minutes``), its task is cancelled and its instance released.

If a worker's script fails or its instance is lost, the node simply disappears from Ray's view at its next look, and Ray replaces it if it is still needed.

``ray down`` stops the workers and then the head node, removes every worker pool, and waits until no instance is left. It can be run more than once, and from any machine with the same YAML and credentials.

If ``ray up`` is run for a cluster name whose previous cluster has lost its head node, the remains of the old cluster are removed first.

Changing a running cluster
++++++++++++++++++++++++++

Two things about re-running ``ray up`` are worth knowing.

Ray caches the prepared configuration, keyed on the text of the YAML file. If you change a script that the YAML names with ``file:`` but not the YAML itself, Ray will not notice: run ``ray up --no-config-cache``. The example's ``up.sh`` always does.

A change to the head node's ``node_config`` makes Ray ask to replace the head node, and with RayDog the head node is the cluster: confirming replaces the whole cluster. A change to a worker node type's ``node_config``, or to the worker start script, does not reach a running cluster, because the worker pool for that type already exists; shut the cluster down and bring it up again.

The Ray dashboard and jobs
--------------------------

The dashboard and job server listen on the head node itself. ``ray dashboard cluster.yaml`` forwards port 8265 to your machine, after which ``ray job submit --address http://localhost:8265 ...`` works; ``raydog tunnel`` does the same for any cluster by name. Submit a job with ``--working-dir`` pointing at a directory that contains only the job, since the whole directory is uploaded.

Graphs in the dashboard
+++++++++++++++++++++++

Ray's dashboard shows graphs of the cluster's metrics when it can find Prometheus and Grafana. Ray's own guide puts both on the head node, and the example does this when ``RAYDOG_OBSERVABILITY=1`` is set: the head node's setup installs them, Prometheus collects from every node through the list of metrics endpoints that Ray keeps up to date, Grafana shows the dashboards that Ray writes, and the head's start script points Ray's dashboard at them. Nothing changes on the workers, and nothing in RayDog. Your browser fetches the graphs from Grafana at ``localhost:3000``, so forward that port as well as the dashboard's: ``raydog tunnel -p 10001 -p 8265 -p 3000 <cluster>``. The head node needs room for both services, and the container variant does not offer them. The example's README has the details.

Running Ray in containers
-------------------------

Nothing in RayDog itself changes: the start scripts run ``docker run`` instead of ``ray start``. The example includes a container variant (``raydog-docker.yaml``). The points to get right are:

- Run the containers with host networking, so that Ray's ports are where the rest of the cluster expects them.
- On the head node, Ray's autoscaler inside the container must find the credentials directory (``~/.raydog``) and the key Ray copied over (``~/ray_bootstrap_key.pem``) at the same places under its own home directory as on the host: mount them there, read-only. Without the key the autoscaler starts no workers, and says so only in its own log (``monitor.err``). The configuration file can be mounted anywhere and named with ``--autoscaling-config``. Pass nothing secret on the command line.
- The same goes for every path under ``file_mounts`` in the cluster config: Ray's autoscaler examines each of them when it starts, and stops, taking the head node's Ray with it, if one is missing. Mount each into the head's container at the same place under the container user's home directory as it has on the host.
- The files Ray copies to the head node are readable only by the Agent's user, so the container must run as a user that can read them. The example runs the head's container as root for that reason.
- With the original autoscaler, give workers time to start (``worker_start_grace_seconds``; autoscaler v2 needs no such setting). Either way, pull the image when the instance boots rather than when the node starts.
- Give the instances room for the image. ``rayproject/ray`` is over 2 GB unpacked, and the GPU and ML images are several times that; an instance whose disk fills during the pull never becomes a node, and ``ray up`` reports that the head node did not start.
- Ray's own images work as they are. Workers need only Ray. The head node's container also needs RayDog, because Ray's autoscaler runs there and uses it to start and stop nodes; the example installs it with ``pip`` as the container starts, which needs PyPI to be reachable and adds a little to the head's start. An image of your own, built ``FROM rayproject/ray`` with RayDog added, avoids both. Whichever image you use, its Python and Ray must match those on the machines that connect to the cluster.

Autoscaler versions
-------------------

RayDog works with both of Ray's autoscaler implementations, and uses autoscaler v2 unless the provider section says ``autoscaler_v2: false``. Ray's own ``ray up`` also uses v2 by default, from Ray 2.50; RayDog starts Ray from its own start scripts, so the choice is made in RayDog's settings.

The two differ in how they keep track of nodes. The original autoscaler keeps its own picture, built from tags it writes on each node and matched to Ray's nodes by address, and treats a node it has not heard from within about 30 seconds of being ready as dead. Autoscaler v2 follows each node through explicit stages (requested, allocated, Ray installing, Ray running, stopping, terminated), matches nodes by an identity each one declares, gives each stage its own timeout, and asks Ray to drain an idle node before removing it, which Ray refuses if the node has become busy. ``ray status`` shows more with v2, including nodes that are pending or failed to start, and why.

With autoscaler v2, RayDog gives every node what autoscaler v2 needs: ``RAY_enable_autoscaler_v2=1`` and the node's type in ``RAY_NODE_TYPE_NAME``, in the task's environment, and its identity, which for RayDog is the node's YellowDog task ID, in ``RAY_CLOUD_INSTANCE_ID``. The task ID exists only once the task does, so that last one is set by a line RayDog adds at the top of the start script (after any ``#!`` line), from the ``YD_TASK_ID`` the YellowDog Agent gives every task. The start scripts need nothing extra, except that a script running Ray in a container must pass these three variables on, as the example's container scripts do.

Both have been run with RayDog's real-platform tests, on plain nodes and in containers. ``autoscaler_v2: false`` is the way back to the original autoscaler, for a cluster that depends on how it behaves.