Troubleshooting
===============

Getting more detail
-------------------

RayDog logs through Python's standard ``logging`` module, under the logger ``yellowdog_ray``. Warnings and errors are shown on the terminal unless your program has set logging up otherwise. Two further levels are there when you need them:

- **INFO** tells a cluster's story: created, pools added and removed, nodes started and stopped, where the head node is running, shutting down and whether it finished.
- **DEBUG** adds every call RayDog makes to YellowDog, with how long it took and whether it failed. That is the place to look when something is slow.

Names and IDs are logged; scripts, task environments, userdata and credentials never are, at any level.

How to see them:

- **The** ``raydog`` **command:** ``-v`` for INFO, ``-vv`` for DEBUG, before or after the command (``raydog -v down demo``).
- **Your own Python program** (the Builder):

  .. code-block:: python

     import logging

     logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s: %(message)s")
     logging.getLogger("yellowdog_ray").setLevel(logging.INFO)    # or logging.DEBUG

- **The Autoscaler on your machine:** Ray's command line sets up only Ray's own logging, so set ``RAYDOG_LOG_LEVEL`` to ``INFO`` or ``DEBUG`` for RayDog's lines to appear among Ray's. For more of Ray's own detail, give ``ray up`` its ``-v`` option; the example's ``up.sh`` and ``down.sh`` pass any options after the template on to Ray:

  .. code-block:: console

     $ RAYDOG_LOG_LEVEL=DEBUG ray up cluster.yaml -v
     $ RAYDOG_LOG_LEVEL=INFO ./up.sh raydog.yaml -v

- **The Autoscaler on the head node:** Ray's autoscaler runs there, and RayDog's lines go to Ray's monitor log, among Ray's. This is where to look when workers are not being added or removed as expected:

  .. code-block:: console

     $ ray exec cluster.yaml 'tail -n 200 /tmp/ray/session_latest/logs/monitor.log'
     $ ray exec cluster.yaml 'tail -n 50 /tmp/ray/session_latest/logs/monitor.err'

  An error inside the autoscaler, RayDog's or Ray's, usually ends up in ``monitor.err``.

Interrupting ``ray up``
-----------------------

Ctrl-C while ``ray up`` waits for the head node removes the cluster, and says so. Once the head node has started, Ray is setting it up, and Ctrl-C leaves the cluster running: ``raydog list`` shows it, and ``ray down`` (or the example's ``down.sh``) removes it. If the removal is itself interrupted, ``raydog down <name>`` finishes it.

The head node does not start
----------------------------

``build()`` or ``ray up`` waits for the head node up to the build timeout, then removes what it created and reports the failure, saying how far the node got: no instance provisioned, an instance whose YellowDog Agent never registered, or a node that registered but did not start its task. The commonest causes:

- **The node never registers with YellowDog.** In the YellowDog portal the worker pool shows instances but no nodes. Either the instance's YellowDog Agent is not running, or is too old to be accepted by the platform: check the image, or install the current Agent at boot as the example setup script does. Or the Agent cannot reach the platform: it registers by making an outbound HTTPS (SSL/TLS) connection to the platform's address (``https://api.yellowdog.ai``, or your own for a dedicated platform), and an instance that cannot make that connection never registers. Check that the instance's subnet has a route out (a public address or a NAT gateway), that its security group or firewall allows outbound traffic on port 443, and that any proxy in between does not block or re-sign the connection.
- **The setup script failed.** The example scripts stop at the first error, before the Agent is installed, and write ``raydog: node setup FAILED at line N`` to the instance's console output; the detail is in ``/var/log/raydog-node-setup.log`` on the instance. Two causes seen in practice: a machine image with a broken third-party package repository (the scripts tolerate a failing ``apt-get update``, but not a package that cannot then be installed), and, for the container variant, a disk too small for the Ray image (over 2 GB unpacked).
- **The start script failed.** The task's output is in the YellowDog portal, under the cluster's work requirement. A script that refers to a Python environment that is not there yet (because the Agent was installed before Ray) fails at once.
- **No instances could be provisioned.** The compute requirement template has no capacity, or its image or network settings are wrong. The compute requirement in the portal says why.

Raise ``build_timeout`` (``build_timeout_minutes`` for the Autoscaler) if nodes are simply slow to set up.

``ray up`` cannot reach the head node
-------------------------------------

After the head node starts, Ray connects to it over SSH. It needs the head's address to be reachable from your machine on port 22, the user named in ``auth.ssh_user`` to accept the key named in ``auth.ssh_private_key``, and ``rsync`` on both ends. If the head node has only a private address, set ``use_internal_ips: true`` in the ``provider`` section and run ``ray up`` from inside the network.

Workers are added and then removed again
----------------------------------------

Ray drops a worker that it has been told is ready but has not heard from within about 30 seconds. If the worker's start script does slow work before starting Ray, set ``worker_start_grace_seconds`` to cover it. If the worker's Ray cannot reach the head node at all, check that the nodes can reach each other on Ray's ports (6379 and the worker port range).

A cluster name is "already running"
-----------------------------------

Only one running cluster may have a given name in a namespace. ``raydog list`` shows it; ``raydog down NAME`` ends it. If the earlier cluster's head node has died, ``ray up`` replaces it by itself; the Builder does not, so that it can never remove a cluster by accident.

Something is still running
--------------------------

.. code-block:: console

   $ raydog list -n my-namespace            # every cluster, and its state
   $ raydog down NAME -n my-namespace --wait
   $ raydog cleanup -n my-namespace         # anything nothing accounts for
   $ raydog cleanup -n my-namespace --delete --wait

A shutdown that reports it could not complete (:class:`~yellowdog_ray.errors.TeardownError`, or exit code 4 from ``raydog``) may have left instances running: run it again. As a last resort every RayDog instance carries the tags ``raydog-cluster`` and ``raydog-work-requirement``, by which it can be found in the cloud provider's own console.

Every cluster also has a lifetime, after which its nodes are stopped whatever else has happened.

Credentials are not found
-------------------------

RayDog looks for ``YD_API_KEY_ID`` and ``YD_API_KEY_SECRET`` in the environment and then in a ``.env`` file in or above the working directory. The error message names what is missing. On the head node of an Autoscaler cluster the credentials come from a file that ``ray up`` copies there; if the head's autoscaler reports missing credentials, the head start script started Ray before that file had arrived.

Ray warns about ``head_node`` and ``worker_nodes``
--------------------------------------------------

Every ``ray`` command that reads the cluster YAML prints ``Dropping the empty legacy field head_node`` and the same for ``worker_nodes``. They are harmless and nothing in your YAML causes them: Ray adds those two empty settings itself for any provider that is not built into Ray, then warns that it is removing them. The example's ``up.sh`` and ``down.sh`` filter the two lines out.

Version mismatches
------------------

Errors about protocol or pickle versions when connecting a Ray client, or jobs that fail to deserialise, mean that Ray or Python differs between your machine and the nodes. See :doc:`nodes`.
