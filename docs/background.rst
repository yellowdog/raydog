YellowDog and Ray in brief
==========================

RayDog joins two systems that each have their own vocabulary. This page gives the parts of each that the rest of the documentation relies on. If you know YellowDog, read the Ray half, and the other way round; then read the two short sections at the end, which are about where the two meet.

YellowDog in brief
------------------

YellowDog finds compute and runs work on it. RayDog uses both halves.

**Getting compute**

- A **namespace** groups the things you create. Everything RayDog makes for a cluster goes in the one namespace you name.
- An **application key** (an ID and a secret) is what a program uses to act on your account.
- A **compute requirement template** says what instances may be used and where: which cloud providers, regions and instance types, on-demand or spot, in what order of preference. Templates are written once, usually by whoever manages your YellowDog account, and RayDog is given their names.
- A **compute requirement** is one request for a number of instances, made from a template. YellowDog provisions the instances and keeps the requirement at its target count.
- A **worker pool** is a compute requirement whose instances are meant to run work. Each instance runs the **YellowDog Agent**, which connects out to the platform and **registers** the instance as a **node**. A node offers one or more **workers**, each of which runs one task at a time. RayDog asks for one worker per node.
- A pool can release a node that has had nothing to run for a set time (**idle node shutdown**), and its size can be changed while it runs.

**Running work**

- A **work requirement** is a piece of work you submit, made up of one or more **task groups**.
- A **task group** holds tasks that share how they are to be run: the type of task, a time limit, and a **worker tag** that says which workers may take them. A worker takes a task only if its own tag matches.
- A **task** is one thing to run: for RayDog, a ``bash`` script. A task waits until a matching worker is free, runs there, and ends as completed, failed, or aborted if it was cancelled while running. Its output can be read in the YellowDog portal.

Ray in brief
------------

Ray runs Python functions and classes across many machines as if they were one.

**A cluster**

- A Ray **cluster** is one **head node** and any number of **worker nodes**. Every node runs Ray's processes and connects to the head; the nodes also talk to each other directly.
- The **head node** holds the cluster's state, decides where work runs, and serves the **dashboard** and the **job server** (port 8265) and the **Ray client** server (port 10001). If the head is lost, the cluster is lost.
- Each node declares **resources**, such as a number of CPUs. They are a bookkeeping device for scheduling: Ray trusts the numbers it is given.
- Ray on every node, and on any machine that connects to the cluster, must be the same version, with the same version of Python.

**Work**

- A **task** is one call of a function marked ``@ray.remote``. Ray picks a node with the resources the task asks for and runs it there, typically within milliseconds. A program may start thousands or millions of them.
- An **actor** is an instance of a class marked ``@ray.remote``: a process that stays alive on some node, keeps state, and answers method calls.
- Tasks and actors pass data to each other through Ray's **object store**, which is shared across the cluster.
- A **job** is one run of a program (the **driver**) that uses the cluster. The driver starts tasks and actors, which spread over the nodes as Ray sees fit. Many jobs can use one cluster, at once or one after another.

**Growing and shrinking**

- Ray's **cluster launcher** (``ray up``, ``ray down``) creates clusters on a cloud through a **node provider**, a plug-in that knows how to create and remove nodes there. RayDog's Autoscaler is a node provider for YellowDog.
- Ray's **autoscaler** runs on the head node. It looks at the resources that waiting tasks and actors are asking for, asks the node provider for more worker nodes when there are not enough, and gives back nodes that have been idle.

Words that mean two things
--------------------------

Several words are used by both systems for different things. This documentation says which is meant wherever it is not obvious.

.. list-table::
   :header-rows: 1
   :widths: 14 43 43

   * - Word
     - In YellowDog
     - In Ray
   * - node
     - An instance whose Agent has registered with the platform.
     - A machine running Ray as part of a cluster. With RayDog, every Ray node is also a YellowDog node.
   * - worker
     - A slot on a node that runs one task at a time.
     - A **worker node** is any node but the head. A **worker process** is one of the processes on a node that run tasks.
   * - worker pool
     - A set of instances provisioned to run work.
     - Not a Ray term. In RayDog's Builder, a named group of Ray worker nodes of one kind, which is backed by one YellowDog worker pool.
   * - task
     - A script run once on a worker. For RayDog, the script that runs Ray on a node for as long as the node lasts.
     - One call of a remote function, usually short.
   * - job
     - Not a YellowDog term.
     - One run of a program that uses the cluster.

Who decides what
----------------

The two systems make decisions at different levels, and RayDog keeps them apart.

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - Question
     - Answered by
   * - Which clouds, regions and instance types may be used, and at what price?
     - YellowDog, from your compute requirement templates.
   * - How many nodes should the cluster have?
     - You, with the Builder. Ray's autoscaler, with the Autoscaler.
   * - Which instance does a node run on, and what happens if it cannot be provided?
     - YellowDog.
   * - Which node does a piece of a Ray program run on?
     - Ray.
   * - When does the cluster end?
     - You; or the cluster's lifetime, enforced by YellowDog.

:doc:`concepts` describes how a Ray cluster is represented in YellowDog, and why.
