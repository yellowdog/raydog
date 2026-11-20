Quick start
===========

This page takes you from a YellowDog account to a running Ray cluster, runs a job on it, and shuts it down, using the examples in the repository. After a common set-up there are two paths; follow either or both:

- :ref:`quickstart-autoscaler`: a cluster that grows when a job needs more workers and shrinks when it is done, with Ray's ``ray up``. About fifteen minutes.
- :ref:`quickstart-builder`: a cluster of a size you choose, built, used and shut down by a Python program. About five minutes.

Much of the time is spent waiting for instances to start.

Before you start
----------------

On YellowDog:

- A **namespace** to work in.
- An **application key** (an ID and a secret) that can create and manage work requirements and worker pools in that namespace, and that can use the keyring holding the cloud credentials its templates need. :ref:`application-key` lists exactly what it needs.
- Two **compute requirement templates**, one for the head node and one for workers. Any instance type with 2 or more CPUs will do. The head node needs a public address that accepts SSH (port 22) from your machine, and every node must be able to reach the YellowDog platform over HTTPS.
- A plain **Ubuntu image** for those templates, unless the templates already name one: best as a YellowDog image family, which covers every region. The example sets each node up when it first boots.

On your own machine (Linux or macOS):

- Python 3.12, ``git``, the OpenSSH client and ``rsync``. The Builder example connects to the cluster with the Ray client, which needs the same version of Python on your machine as on the nodes, and the examples' setup scripts give the nodes Python 3.12.
- An **SSH key pair** for the cluster. A new one is best:

  .. code-block:: console

     $ ssh-keygen -t ed25519 -N "" -f ~/.ssh/raydog

Get the examples
----------------

.. code-block:: console

   $ git clone https://github.com/yellowdog/raydog.git
   $ cd raydog
   $ python3.12 -m venv .venv && source .venv/bin/activate
   $ pip install yellowdog-ray "ray[default]==2.60.0"

The Ray on your machine must be the same version as the Ray on the cluster's nodes, which the examples' setup scripts install. That is why Ray is named with its version here; it is ``RAY_VERSION`` in each example's setup script. Keep this virtual environment active for the rest of the page.

Settings
--------

Create a file called ``.env`` in ``examples/autoscaler``, with your own values. Both examples read the same settings.

.. code-block:: bash

   YD_API_KEY_ID=...
   YD_API_KEY_SECRET=...
   RAYDOG_NAMESPACE=my-namespace
   RAYDOG_HEAD_TEMPLATE=my-namespace/ray-head
   RAYDOG_WORKER_TEMPLATE=my-namespace/ray-worker
   RAYDOG_IMAGES_ID=my-namespace/ubuntu-26-04
   RAYDOG_SSH_PRIVATE_KEY=~/.ssh/raydog
   RAYDOG_TAG=my-tag

``RAYDOG_IMAGES_ID`` is an image family's ``namespace/name``, or an image ID such as an AMI; leave it out if the templates name an image. ``RAYDOG_TAG`` is optional: the Autoscaler puts it on the cluster's work requirement and compute requirements, so that you can find them in the YellowDog portal. Add ``RAYDOG_OBSERVABILITY=1`` too if you would like graphs in Ray's dashboard: the Autoscaler example then also runs Prometheus and Grafana on the head node (see :ref:`quickstart-run-a-job`). On a dedicated YellowDog platform, add ``YD_API_URL``. The ``.env`` file holds your application key: keep it to yourself.

.. _quickstart-autoscaler:

With the Autoscaler
-------------------

Start the cluster
~~~~~~~~~~~~~~~~~

.. code-block:: console

   $ cd examples/autoscaler
   $ ./up.sh

Ray asks before it creates anything; answer ``y``. It then waits for the head node, which takes two or three minutes: YellowDog provisions an instance, the setup script installs Python, Ray and the YellowDog Agent, and the Agent registers. Meanwhile, the YellowDog portal shows a work requirement named after the cluster in your namespace, tagged ``my-tag``, with a worker pool for the head node. When ``up.sh`` returns, the head node is running Ray.

The example's cluster is called ``raydog-example``. See it from anywhere with the ``raydog`` command:

.. code-block:: console

   $ raydog status raydog-example -n my-namespace

There are no workers yet. Ray adds them only when there is work for them.

.. _quickstart-run-a-job:

Run a job
~~~~~~~~~

The example job runs eight tasks of 30 seconds each, needing one CPU apiece, which exactly fills the four two-CPU workers the example allows. The head node offers Ray no CPUs, so the autoscaler must add workers to run them.

To watch it happen, first open the Ray dashboard in a second terminal, with the virtual environment active:

.. code-block:: console

   $ raydog tunnel -n my-namespace -i ~/.ssh/raydog raydog-example

and browse to http://localhost:8265. With ``RAYDOG_OBSERVABILITY=1``, forward Grafana's port as well, so that the dashboard's graphs reach your browser:

.. code-block:: console

   $ raydog tunnel -n my-namespace -i ~/.ssh/raydog -p 10001 -p 8265 -p 3000 raydog-example

The graphs are on the dashboard's Metrics tab, and fill in as the cluster works; Grafana itself is at http://localhost:3000. Then, back in the first terminal:

.. code-block:: console

   $ ./submit-job.sh

Within a minute or so the autoscaler asks for four workers, and a worker pool for them appears in the portal. The workers take a minute or two to start, because each is set up when it boots, as the head was; the job's tasks run as workers join, and it prints where each one ran. The whole job takes about three minutes. Two or three minutes after it ends, Ray removes the idle workers, and their instances are released within five minutes or so of the job's end. ``raydog status`` shows each stage.

Shut it down
~~~~~~~~~~~~

Stop the tunnel with Ctrl-C, then:

.. code-block:: console

   $ ./down.sh

This ends every node, cancels the work requirement, removes the worker pools, and waits until no instance remains. Ray begins by running ``ray stop`` on the head node, which answers ``raydog: this node stops when its task ends; 'ray stop' ignored``: that is expected, as the node is ended through YellowDog instead. To check that nothing is left:

.. code-block:: console

   $ raydog list -n my-namespace

If you forget, the example's cluster stops itself after two hours (``lifetime_hours`` in ``raydog.yaml``).

.. _quickstart-builder:

With the Builder
----------------

The Builder example is a Python program that builds a cluster of a head node and two workers, connects to it through an SSH tunnel, runs a small Ray job, and shuts the cluster down. From the top of the repository:

.. code-block:: console

   $ cd examples/builder
   $ cp ../autoscaler/.env .
   $ python usage-example.py

It takes about three minutes. The example turns on RayDog's logging, so you see each step as it happens:

.. code-block:: text

   INFO:yellowdog_ray.builder.cluster:building cluster raydog-usage with 1 worker pool(s)
   INFO:yellowdog_ray.core.cluster:created cluster raydog-usage in namespace my-namespace (work requirement raydog-usage_3mbvmkxx)
   INFO:yellowdog_ray.core.cluster:added worker pool raydog-usage_3mbvmkxx_head for 1 node(s), at most 1
   INFO:yellowdog_ray.core.cluster:added worker pool raydog-usage_3mbvmkxx_w-workers for 2 node(s), at most 2
   INFO:yellowdog_ray.core.cluster:started 1 head node(s) in cluster raydog-usage
   INFO:yellowdog_ray.core.cluster:node head of cluster raydog-usage is running at 10.0.158.111 (public address 3.8.3.40)
   INFO:yellowdog_ray.core.cluster:started 2 w-workers node(s) in cluster raydog-usage
   INFO:yellowdog_ray.builder.cluster:cluster raydog-usage is built
   Head node: private IP 10.0.158.111, public IP 3.8.3.40
   Result: 328350
   INFO:yellowdog_ray.core.cluster:shutting down cluster raydog-usage
   INFO:yellowdog_ray.core.cluster:cluster raydog-usage is shut down
   Cluster shut down

The instances for the head node and both workers are requested together, so they start at the same time. The program waits for the head node, connects to it, waits for the workers to join, runs the job (the sum of the squares of 0 to 99), and shuts the cluster down. Ray may warn that the Python patch versions differ between your machine and the cluster; that is harmless, as long as the minor versions (3.12) match. Ray's client also prints a line or two of its own that are not shown above, such as ``SIGTERM handler is not set because current thread is not the main thread``; they are harmless too.

The heart of ``usage-example.py`` is this:

.. code-block:: python

   spec = ClusterSpec(name=settings.cluster_name, namespace=settings.namespace,
                      head=settings.head, lifetime=timedelta(hours=1))
   with RayDogCluster(spec) as cluster:
       cluster.add_worker_pool("workers", settings.worker, count=2)
       head = cluster.build()                  # returns when the head node is running
       with RayTunnels(head.public_ip, user=settings.ssh_user,
                       private_key=settings.ssh_private_key):
           ray.init(address="ray://localhost:10001")
           ...                                 # use Ray as usual
   # leaving the block shuts the cluster down, whatever happened inside it

``settings.head`` and ``settings.worker`` are :class:`~yellowdog_ray.NodeSpec` objects, made by ``settings.py`` from your ``.env``: each names a template, the setup script to run when an instance boots, and the script that starts Ray on the node.

The program shuts its cluster down when it ends, and also if it fails or is interrupted with Ctrl-C. To check that nothing is left:

.. code-block:: console

   $ raydog list -n my-namespace

``manage-example.py``, beside it, goes further: it adds worker pools to a running cluster, resizes and removes them, and hands the cluster to another process to shut down.

Where next
----------

- :doc:`concepts` explains what you just saw: how a cluster, its nodes and its job map to YellowDog's work requirements, worker pools and tasks.
- :doc:`autoscaler` describes the cluster configuration, which in the Autoscaler example is ``raydog.yaml``, with every setting explained in place.
- :doc:`nodes` covers images and setup scripts. Setting nodes up at boot is slow; an image with everything installed starts a node within a minute or so.
- :doc:`builder` describes the Python API in full.
- :doc:`troubleshooting` helps when a step above does not go as described.
