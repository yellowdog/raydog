When something fails
====================

This page describes what happens to a cluster when part of it is lost, and what is left to you. For finding out *why* something failed, see :doc:`troubleshooting`.

Two rules cover most of it. A cluster cannot outlive its **lifetime**, whatever else has gone wrong, because YellowDog itself ends every node at that deadline. And nothing of a cluster is ever removed as a side effect of something else failing: RayDog removes things when asked to, or to undo what the same call had just created.

A worker node is lost
---------------------

A worker is lost when its instance is taken back (a spot instance being reclaimed, for instance), when the instance fails, or when Ray on it stops. YellowDog shows its task as failed.

- **Ray's part.** Ray notices that the node has gone. Tasks that were running there are run again elsewhere, by default up to three times; actors are restarted only if they were created with ``max_restarts``; results held only on that node are recomputed if they can be. This is Ray's own fault tolerance and is described in Ray's documentation.
- **With the Autoscaler,** the node disappears from RayDog's list at Ray's next look. If the work still needs it, Ray asks for another and RayDog provides one. Nothing is needed from you.
- **With the Builder,** the pool simply has one node fewer: nothing replaces it. ``status()`` shows how many nodes each pool has running, and ``resize_worker_pool(name, count)`` with the count you want brings it back up, since it counts the nodes that are alive.

Worker nodes are a good use of spot instances with the Autoscaler, for this reason.

The head node is lost
---------------------

The head node holds the cluster's state, so losing it loses the cluster: running jobs end, and the workers have nothing to connect to.

- **The workers stop by themselves.** Ray on each worker gives up about a minute after losing the head, which ends the worker's task as failed. Their instances then have nothing to run and are released after the idle shutdown time. In our tests every instance had gone within four to six minutes of the head being lost, with nothing done by anyone.
- **The cluster's record stays.** The work requirement remains, still shown as running, with its worker pools empty, until it is removed or its lifetime passes. It holds no instances and costs nothing, but its name cannot be used for a new Builder cluster until it is removed.
- **How it is shown.** In YellowDog the head's task is failed, with the reason "Task process exited with non-zero error code" if Ray stopped on a working instance, or "Task node allocation lost" if the instance itself went. ``raydog status`` reports the cluster as having no head. ``raydog cleanup`` lists it straight away, as a cluster whose head node has ended.
- **With the Autoscaler,** running ``ray up`` again for the same cluster removes what is left of the old one and starts a new one. ``ray down`` or ``raydog down`` removes it without replacing it.
- **With the Builder,** the cluster is not replaced, so that no program can ever remove a cluster by accident. ``status().head_alive`` is false and the ``head`` property is ``None``. Shut the cluster down and build another.

Because everything depends on it, give the head node an on-demand instance, not a spot one.

A node does not start
---------------------

- **While a cluster is being created,** the head node is given the build timeout to start. If it does not, ``ray up`` or ``build()`` removes everything it had created and reports the failure.
- **A worker that does not start** leaves its task waiting, which ``raydog status`` and the YellowDog portal both show. It costs no more than the instance it may be holding, and goes when the pool is resized, the cluster is shut down or the lifetime passes. :doc:`troubleshooting` lists the usual causes.

The program that created the cluster stops
------------------------------------------

- **With the Autoscaler,** nothing changes. Once ``ray up`` has returned, the cluster is run from its head node, and your machine can be switched off.
- **With the Builder,** the cluster carries on at its current size, since nothing but your program resizes it. If the program ended normally, or with an error or Ctrl-C inside a ``with`` block, the cluster is shut down on the way out. If the process was killed outright, or the machine lost power, the cluster stays up: attach to it by name from another process (see :doc:`builder`), or end it with ``raydog down``.
- **A program killed in the middle of creating a cluster** may leave a work requirement and pools with no head. ``raydog cleanup`` finds and removes them.

A shutdown does not complete
----------------------------

Shutting down attempts every step even if some fail, then reports what it could not do: :class:`~yellowdog_ray.errors.TeardownError` from Python, exit code 4 from ``raydog``. Something may still be running, so run it again; it is safe to repeat. ``raydog down --wait`` and ``shut_down(wait=True)`` return only once no instance is left.

YellowDog cannot be reached
---------------------------

It depends on who has lost contact.

- **Your machine, or the Autoscaler's head node, cannot reach the platform's API.** The cluster is not affected, but nothing about it can be changed: the Autoscaler cannot add or remove nodes, the Builder cannot resize, and nothing can be shut down, until the platform can be reached again. Calls from your own machine fail with :class:`~yellowdog_ray.errors.PlatformError`.
- **A node's Agent cannot reach the platform.** Ray on the node does not need YellowDog in order to do its work, so a brief loss of contact changes nothing. If the Agent stays out of touch for longer than the platform allows, though, YellowDog marks the node as lost, marks its task as failed, and terminates the instance. From then on it is a lost worker or a lost head, as described above. A network change that cuts the nodes off from the platform therefore ends the cluster, even if the nodes can still reach each other.

The lifetime passes
-------------------

Every node's task is ended by YellowDog, whether or not work is in progress, and the instances are then released as idle. In our tests the tasks ended about a minute after the deadline and the last instance had gone about five minutes after it.

YellowDog records a node ended this way as failed (the start script is terminated, and reports exit code 143), which is the platform's way of saying that a time limit was reached and not a sign that anything went wrong on the node. The work requirement remains, with empty pools; ``raydog status`` shows the cluster as expired, and ``raydog down`` or ``raydog cleanup --delete`` removes it.

This is the backstop for everything above, so set the lifetime longer than the work needs and not much longer. See :doc:`concepts`.

In short
--------

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - What is lost
     - Autoscaler
     - Builder
   * - A worker node
     - Replaced if still needed
     - Not replaced; resize the pool to restore it
   * - The head node
     - Cluster lost; workers stop and instances are released; ``ray up`` again replaces it
     - Cluster lost; workers stop and instances are released; shut down and rebuild
   * - Your program or machine
     - No effect
     - Cluster stays at its size; attach or ``raydog down``
   * - Everything, and nobody notices
     - Ends at its lifetime
     - Ends at its lifetime
