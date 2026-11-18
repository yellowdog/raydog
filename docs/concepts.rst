How RayDog works
================

A cluster is a work requirement
-------------------------------

RayDog represents a Ray cluster with ordinary YellowDog objects, so everything about a cluster can be seen in the YellowDog portal and controlled through the platform. If the YellowDog or Ray terms used here are unfamiliar, :doc:`background` explains them.

.. code-block:: text

   Ray cluster "demo"
   └── work requirement  demo_k3x9q2ab
       ├── task group  head      ── worker pool  demo_k3x9q2ab_head      ── 1 instance   the head node
       ├── task group  w-small   ── worker pool  demo_k3x9q2ab_w-small   ── N instances  workers
       └── task group  w-gpu     ── worker pool  demo_k3x9q2ab_w-gpu     ── M instances  workers

- The **cluster** is one work requirement.
- Each **kind of node** has a task group and, feeding it, a worker pool of its own: one for the head node, one for each pool of workers (Builder) or each node type (Autoscaler), and optionally one for an observability node.
- Each **node** is one task. The task is the script that starts Ray on the node, and it runs for as long as the node is part of the cluster. Ending the task ends the node; when a node's instance has nothing left to run, YellowDog releases it.

The life of the work requirement is the life of the cluster: cancelling it stops every node.

Each of those choices is there for what the YellowDog object already does:

- A **work requirement** is something YellowDog can list, show and cancel as a whole. Making the cluster one work requirement means the cluster can be found by name from anywhere, has one status, and can be stopped with one call that reaches every node.
- A **worker pool** provides instances from one compute requirement template, so each kind of node, which may need a different instance type, has its own. The pool is given a worker tag that only its own task group asks for, so a head node's instance is never handed a worker's script.
- A **task group** carries the settings its nodes share, and a tag that marks it as RayDog's and says which cluster and kind of node it belongs to. The head's also records the cluster's settings, which is how another process can pick the cluster up knowing nothing but its name.
- A **task** is the one thing YellowDog runs on an instance and keeps track of. Making the node's Ray process the task means that YellowDog knows whether each node is running, shows its output, can stop it, and releases the instance when it ends.

The life of a node
------------------

Following one worker node from start to finish shows how the two systems fit together.

1. RayDog creates a task in the node type's task group. The task is the worker start script, with the head node's address in its environment.
2. RayDog raises the target size of the type's worker pool, and YellowDog provisions an instance from the pool's template. (The Builder does these two steps the other way round: it asks for every pool's instances at the start, and adds the workers' tasks once the head node's address is known.)
3. The instance boots. If it is set up at boot, its setup script installs Python and Ray and, last, the YellowDog Agent.
4. The Agent registers the instance with YellowDog as a node. The task is waiting for exactly such a node, and is given to it.
5. The Agent runs the script, which starts Ray and tells it where the head is. Ray on the new node connects to the head, and the cluster has another node. The script does not return: it stays running for as long as the node is part of the cluster.
6. From then on Ray sends the node work directly. YellowDog takes no part in that and does not see it.
7. When the node is no longer wanted, RayDog cancels the task. The Agent stops the script, which stops Ray on its way out.
8. The instance now has nothing to run. After the idle shutdown time YellowDog releases it, or at once if the whole cluster is being shut down, when the pool itself is removed.

The head node is the same, except that it comes first, there is exactly one, and its ending ends the cluster.

Why a Ray job is not a YellowDog task
-------------------------------------

YellowDog runs tasks, and Ray runs jobs made of tasks, so it is natural to expect a Ray job, or a Ray task, to appear in YellowDog as a task. It does not, and the reasons are worth knowing, because they explain what you will and will not see in the YellowDog portal.

**The two kinds of task are different things.** A YellowDog task is a self-contained piece of work: a program that is given an instance, runs, and finishes, with no connection to any other task. It takes seconds to be placed, which does not matter for work that runs for minutes or hours. A Ray task is a function call inside a running program. It is placed in milliseconds, there may be millions of them, and they are anything but self-contained: they hand each other data through a store that all the nodes share, call actors that hold state on other nodes, and report back to the driver that started them. None of that can happen between YellowDog tasks, and no scheduler built for the first kind of task could keep up with the second.

**A Ray job has no single place to run.** A job is a driver and whatever tasks and actors it starts, spread over the cluster's nodes by Ray from moment to moment, sharing those nodes with other jobs. There is no instance that a job occupies and no moment at which YellowDog could be told to start or stop it.

**So the two meet at the node.** What Ray needs from outside is machines that are running Ray and can reach each other, and that is a kind of work YellowDog understands: a long-running program on an instance, which is to say a task. RayDog therefore asks YellowDog for nodes, not for work, and leaves everything that happens inside the cluster to Ray.

This has some consequences:

- A cluster's tasks in YellowDog are its **nodes**. A task that has been executing for many hours is a healthy node, not a stuck job.
- Ray **jobs do not appear in YellowDog** at all. To see them, use Ray's dashboard or ``ray job list``: see :doc:`autoscaler` and :doc:`builder` for how to reach them.
- How a YellowDog task ended says how a *node* ended, not whether any job succeeded.
- YellowDog's accounting for a cluster is the accounting for its instances. It cannot be divided between the jobs that ran on them.

If your work is a large number of independent programs with no need to talk to each other while they run, it may suit YellowDog's own tasks directly, with no Ray cluster at all. RayDog is for work written for Ray.

What you see in the YellowDog portal
------------------------------------

A running cluster appears as one work requirement, named after the cluster with a random suffix, and one worker pool for each kind of node, named after the work requirement and the kind (``demo_k3x9q2ab_head``, ``demo_k3x9q2ab_w-small``). Inside the work requirement there is a task group for each kind of node, and in each task group a task for each node that kind has had.

The state of a node's task says where the node is in its life:

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - Task state
     - What it means for the node
   * - Ready
     - The node has been asked for and its instance is not yet available: it is being provisioned, is booting, or is being set up.
   * - Allocated
     - A registered instance has been given the task and is about to run it.
   * - Executing
     - The start script is running: the node is part of the cluster, or is in the last moments of joining it.
   * - Aborted
     - The node was stopped on purpose while it was running, by scaling down or by shutting the cluster down. This is how a healthy node normally ends.
   * - Cancelled
     - The node was no longer wanted before it had started.
   * - Failed
     - One of three things, which the task's error message tells apart. The start script ended with an error, in which case the task's output, shown in the portal, usually says why. The node's instance was lost ("Task node allocation lost"). Or the cluster reached its lifetime and the platform ended the node (exit code 143), which is the lifetime doing its job. Workers also end this way, about a minute later, when their head node is lost.

When a cluster is shut down its work requirement ends as cancelled, which is likewise the normal ending and not a sign of trouble.

Names
-----

A cluster's name may contain lowercase letters, digits and hyphens, must start with a letter and end with a letter or digit, and may be up to 24 characters long.

The work requirement is named after the cluster with a random suffix (``demo_k3x9q2ab``). The suffix is there because YellowDog keeps the names of ended work requirements and worker pools for 24 hours: without it, a cluster name could not be used twice in a day. Only one *running* cluster may have a given name in a namespace, and RayDog enforces that itself.

Worker pools and node types are named by you. RayDog keeps simple names as they are and makes others safe for the platform (``ray.worker.default`` becomes the role ``w-ray-worker-defa-0d413455``: the name cut to fit, plus a short hash of the whole name so that two long names cannot collide).

Lifetime
--------

Every cluster has a **lifetime**, 24 hours unless you say otherwise. It is a deadline fixed when the cluster is created: when it passes, every node's task is ended by the platform and the instances are released. It exists so that a cluster that has been forgotten, or whose controlling program has died, does not run for ever. Set it to suit the work; it can be set to "no limit", in which case shutting the cluster down is entirely up to you.

Two shorter timers also apply. A node is given a **build timeout** (10 minutes by default) to start. An instance whose node has no task is kept for the **idle shutdown** time (3 minutes for the Builder, 1 minute for the Autoscaler) and then released.

Shutting down, and what can be left behind
------------------------------------------

Shutting a cluster down cancels its work requirement and removes every one of its worker pools, terminating their instances. RayDog tries every step even if some fail, and can wait until it has confirmed that no instance is left. It can be done from the object that built the cluster, from another process that names it, by ``ray down``, or with ``raydog down``.

If building a cluster fails part-way, RayDog removes what it had created before reporting the error.

Things can still be left behind: a process killed outright, a network failure in the middle of a teardown, a head node that dies while its workers run on. Three things guard against that. Every node has the cluster's lifetime as a backstop. The ``raydog list`` command shows every cluster in a namespace and its state, and ``raydog down`` ends one by name. And ``raydog cleanup`` finds what nothing healthy accounts for (pools whose cluster has ended, clusters running without a head) and removes it on request. See :doc:`cli`.

Only resources that carry RayDog's own markers are ever touched. A cluster's task groups are tagged ``raydog:1:<cluster>:<role>``, and its instances carry the tags ``raydog-cluster`` and ``raydog-work-requirement``, which also makes them easy to find in a cloud provider's console.

Credentials and security
------------------------

RayDog uses your YellowDog application key to talk to the platform. It is read from, in order: arguments you pass; the environment (``YD_API_KEY_ID``, ``YD_API_KEY_SECRET``, ``YD_API_URL``); a ``.env`` file in or above the working directory. The API URL is only ever taken from the same place as the secret or one ahead of it, so that a key given explicitly is never sent to an address found in some stray file.

With the Builder, the key stays on your machine.

With the Autoscaler, the head node also needs a key, because Ray's autoscaler runs there and creates the worker nodes. RayDog writes the key to a file readable only by you (``~/.raydog/credentials/``), and Ray copies that file to the head node over SSH along with the cluster configuration. The key is never placed in a YellowDog task, tag or log. Anyone who can log in to the head node as the Agent's user can read it there, so use a key with no more permissions than managing clusters needs.

.. _application-key:

What the application key must be allowed to do
++++++++++++++++++++++++++++++++++++++++++++++

RayDog uses the key for the following, all within the one namespace a cluster lives in:

- **Work requirements:** create, read (by name, by ID and as a list), and cancel; add tasks to their task groups; read and cancel tasks.
- **Worker pools:** provision, read, resize and shut down; read the nodes registered in a pool.
- **Compute requirements:** read a pool's compute requirement and its instances, and terminate it.
- **Compute requirement templates,** and any machine images they or your node types name: use.
- **Keyrings:** access to each keyring that holds the cloud credentials named by the compute source templates behind those templates. Without it the platform cannot provision instances on the key's behalf.

It needs nothing else: no access to other namespaces or keyrings, to users, groups or applications, or to compute allowances or usage. A key that can do more than this can do it from anywhere the key is.

That matters most for the Autoscaler, whose head node holds the key for the life of the cluster and does all of the above itself, since it is where workers are added and removed. Two recommendations follow. Give RayDog an application of its own, with access to the namespace it works in and no other, in place of a personal or administrative key. And treat login access to an Autoscaler head node as access to that key.

Node start scripts run as the YellowDog Agent's user. In the example start scripts, Ray's dashboard and job server are left listening on the head node's local interface only; reach them through an SSH tunnel (``raydog tunnel``, or :class:`~yellowdog_ray.tunnels.RayTunnels`) rather than opening them to a network. The tunnels do not check the head node's host key unless you ask them to: see :doc:`cli`.
