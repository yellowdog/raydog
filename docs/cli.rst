The ``raydog`` command
======================

``raydog`` shows the RayDog clusters in a namespace and ends them. It finds clusters on the platform by name, so it works from any machine with YellowDog credentials, whoever created the cluster and whether with the Builder or with ``ray up``.

Every command takes ``-n NAMESPACE``; without it, the namespace is read from the environment variable ``RAYDOG_NAMESPACE``. Credentials are found as described in :doc:`concepts`.

``raydog list``
---------------

.. code-block:: console

   $ raydog list -n my-namespace
   NAME      STATE     AGE     NODES  INSTANCES
   demo      running   2h 14m  5      5
   nightly   no head   9h 3m   2      2

Lists the running clusters. The state is ``starting`` while the head node is coming up, ``running``, ``no head`` if the head node has failed or never started, or ``expired`` if the cluster's lifetime has passed. A cluster that is ``no head`` or ``expired`` no longer works, but its work requirement stays until something removes it, and the listing ends with a reminder of how: ``raydog down`` for one cluster, ``raydog cleanup --delete`` for all of them. ``--all`` includes clusters that have ended within the last day. ``--json`` prints the same information as JSON.

``raydog status CLUSTER``
-------------------------

Shows one cluster in detail: its work requirement, how long it has left, the head node's state and addresses, and for each pool its nodes by state and the number of instances alive. For a cluster that no longer works, it ends with the ``raydog down`` command that removes it. A cluster that ended recently can still be shown. ``--json`` is available.

``raydog down CLUSTER``
-----------------------

Shuts a cluster down: stops every node and removes every pool. It also removes anything an earlier cluster of the same name left behind.

It shows what it found and asks before acting; ``--yes`` skips the question and ``--dry-run`` stops after showing. With ``--wait`` it returns only when no instance is left, which is the only real proof that a cluster has gone.

``raydog cleanup``
------------------

Finds what failed runs have left behind in a namespace:

- worker pools whose cluster has ended;
- clusters that are still running but whose head node has ended, at once, or that have had no running head node for longer than their build timeout (before that, such a cluster may still be being built);
- clusters still running well after their lifetime has passed.

Without options it only lists them, with the reason for each. ``--delete`` removes them, and ``--wait`` waits until their instances have gone. Only resources carrying RayDog's own markers are ever considered; anything else in the namespace is ignored.

``raydog tunnel CLUSTER``
-------------------------

Forwards the Ray client port (10001) and the dashboard and job server port (8265) of a cluster's head node to the same ports on this machine, over SSH, until interrupted.

.. code-block:: console

   $ raydog tunnel -n my-namespace -i ~/.ssh/cluster-key my-cluster
   localhost:10001 -> my-cluster head node, port 10001  (Ray client: ray.init("ray://localhost:10001"))
   localhost:8265 -> my-cluster head node, port 8265  (dashboard and job server: http://localhost:8265)
   Press Ctrl-C to close.

``-i KEY`` gives the private key and ``-u USER`` the user (``yd-agent`` by default). The command knows a cluster only by its name, not the key its nodes were set up with: without ``-i``, ``ssh`` offers your default keys, and if the head node refuses them (``Permission denied (publickey)``) the error says to pass the cluster's key. ``-p LOCAL[:REMOTE]`` forwards a chosen port instead of the defaults and may be repeated. ``--internal`` connects to the head node's private address. ``--for SECONDS`` closes the tunnels after a time. ``-o NAME=VALUE`` passes an option to ``ssh`` and may be repeated.

If a default port is already in use on this machine, by another tunnel or a Ray running locally, the command uses the next free port instead and says so first; the lines it prints give the ports actually used:

.. code-block:: console

   $ raydog tunnel -n my-namespace -i ~/.ssh/cluster-key my-cluster
   local port 8265 is in use; using 8266
   localhost:10001 -> my-cluster head node, port 10001  (Ray client: ray.init("ray://localhost:10001"))
   localhost:8266 -> my-cluster head node, port 8265  (dashboard and job server: http://localhost:8266)
   Press Ctrl-C to close.

A port given with ``-p`` is used as given: if it is in use, the command fails at once, rather than mistake whatever is using it for the tunnel, and says to choose another. It closes its ``ssh`` process when interrupted (Ctrl-C) or terminated.

**Host keys are not checked by default**, as with Ray's own SSH: instances are short-lived and their addresses are reused, so a key remembered from last time says little. On a network you do not trust, record the head node's host key yourself and pass ``-o StrictHostKeyChecking=yes -o UserKnownHostsFile=FILE``. :class:`~yellowdog_ray.tunnels.RayTunnels` takes the same options as ``ssh_options``.

Seeing more
-----------

Every command takes ``-v`` to show what it does, and ``-vv`` to show as well each call it makes to YellowDog and how long it took. The flag can go before or after the command. See :doc:`troubleshooting` for logging from your own programs and on an Autoscaler's head node.

Exit codes
----------

.. list-table::
   :widths: 10 90

   * - 0
     - Success.
   * - 1
     - The operation failed.
   * - 2
     - A usage or configuration error, including missing credentials.
   * - 3
     - No such cluster.
   * - 4
     - A shutdown or cleanup could not be completed: something may still be running. Run the command again.
   * - 130
     - Interrupted. The operation may be incomplete.
