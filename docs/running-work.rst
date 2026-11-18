Running work on a cluster
=========================

RayDog provides the cluster; using it is ordinary Ray. This page sets out the ways of getting a program onto a cluster and when each fits. They work the same for a cluster made by the Autoscaler or by the Builder, except where it says otherwise.

Reaching the head node
----------------------

Everything below talks to the head node, on one of two ports: 8265 for the dashboard and job server, and 10001 for the Ray client. In the example start scripts the dashboard and job server listen on the head node's own loopback interface only, so they are reached through an SSH tunnel, which is also the only way in when the head node is on a private network:

.. code-block:: console

   $ raydog tunnel -n my-namespace -i ~/.ssh/cluster-key my-cluster
   localhost:10001 -> my-cluster head node, port 10001  (Ray client: ray.init("ray://localhost:10001"))
   localhost:8265 -> my-cluster head node, port 8265  (dashboard and job server: http://localhost:8265)
   Press Ctrl-C to close.

``raydog tunnel`` works for any cluster by name. With the Autoscaler, ``ray dashboard cluster.yaml`` forwards port 8265 in the same way; from Python, :class:`~yellowdog_ray.tunnels.RayTunnels` does both. The tunnel needs the head node to accept SSH from your machine with the key its setup authorised. See :doc:`cli`.

Submitting a job
----------------

This is the way to run most work. A job is a command, usually ``python some_script.py``, that Ray runs on the head node on your behalf, keeping its output and its final state.

.. code-block:: console

   $ ray job submit --address http://localhost:8265 --working-dir my_job -- python main.py
   $ ray job list --address http://localhost:8265
   $ ray job logs --address http://localhost:8265 JOB_ID
   $ ray job stop --address http://localhost:8265 JOB_ID

- The directory named by ``--working-dir`` is uploaded to the cluster and becomes the job's working directory. Point it at a directory holding only what the job needs, since all of it is uploaded.
- The job's own program (the driver) runs on the head node. In the examples the head node offers Ray no CPUs, so the tasks and actors the driver starts run on worker nodes; with the Autoscaler, that is what makes workers appear.
- The job carries on if your machine disconnects, and several jobs can run at once.
- Packages the job needs must be on the nodes: install them in the node setup script or the machine image, or name them in the job's runtime environment (see Ray's documentation).

The Autoscaler example's ``submit-job.sh`` does exactly this, tunnel included.

Connecting a program on your own machine
----------------------------------------

The Ray client lets a program running on your machine use the cluster as if it were local, which suits interactive work and notebooks.

.. code-block:: python

   import ray

   ray.init("ray://localhost:10001")    # through the tunnel

   @ray.remote
   def square(x):
       return x * x

   print(ray.get([square.remote(i) for i in range(100)]))

- The driver is your own process, so its version of Ray and its minor version of Python (3.12, say) must match the cluster's: see :doc:`nodes`. A different patch release of Python only produces a warning.
- The work ends if the connection does. For anything long, submit a job instead.
- Code and data pass through the tunnel, so large inputs are better read by the tasks themselves, from storage the nodes can reach.

Running a command on the head node
----------------------------------

With the Autoscaler, Ray's cluster launcher can run commands on the head node over SSH, using the cluster YAML to find it:

.. code-block:: console

   $ ray exec cluster.yaml 'ray status'                  # what the autoscaler is doing
   $ ray rsync-up cluster.yaml data.csv '~/data.csv'     # copy a file to the head node
   $ ray exec cluster.yaml 'python ~/script.py'          # run something there

These run as the YellowDog Agent's user. The example node setup puts Ray on that user's ``PATH``. A command started this way belongs to your SSH session, so it stops if the session does; a submitted job does not.

One command is deliberately without effect: ``ray stop`` in such a session does nothing, because a RayDog node is stopped by ending its YellowDog task. Use ``ray down``, ``raydog down`` or the Builder's ``shut_down()``.

Which to use
------------

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - You want to
     - Use
   * - Run a program to completion, unattended
     - ``ray job submit``
   * - Explore, or drive the cluster from a notebook
     - The Ray client, ``ray.init("ray://localhost:10001")``
   * - Look at the cluster, or do something small on the head node
     - ``ray exec`` (Autoscaler), or the dashboard at ``http://localhost:8265``
   * - Build a cluster, run work and remove the cluster in one program
     - The Builder, with a job submitted or a client connected inside its ``with`` block

What YellowDog sees
-------------------

None of this appears in YellowDog. The platform sees the cluster's nodes, as tasks; jobs, and their success or failure, are Ray's business and are shown in Ray's dashboard. :doc:`concepts` explains why.
