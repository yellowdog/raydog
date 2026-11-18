RayDog: Ray clusters on YellowDog
=================================

**RayDog** runs `Ray <https://ray.io>`_ clusters on the `YellowDog Platform <https://yellowdog.ai>`_. YellowDog finds and provisions the compute, across clouds, regions and instance types, according to templates you define; RayDog turns that compute into a Ray cluster and keeps the two in step.

The usual way to use it is the **Autoscaler**, which plugs YellowDog into Ray's own cluster launcher. You write a cluster YAML, run ``ray up``, and Ray adds and removes worker nodes as its workload demands. Start here unless one of the reasons below applies.

The **Builder** is a Python API for the cases the Autoscaler does not suit. Your code says what the cluster should look like, and when: build it, add a pool of workers, resize it, shut it down. Choose it when:

- the cluster is part of a larger program, such as a pipeline that builds a cluster, runs its work and removes it;
- you want a cluster of a size you decide, not one that follows the workload;
- your machine cannot reach the head node over SSH, which ``ray up`` requires and the Builder does not;
- you do not want YellowDog credentials on the cluster: the Autoscaler's head node holds an application key so that it can add workers, and a Builder cluster holds none;
- you want a cluster of a known size as soon as possible. The Builder asks for the workers' instances at the same moment as the head's, so they start up together; the Autoscaler asks for workers once the head is running. In our own test, a head and three workers that were set up at boot were all running 20 to 50 seconds sooner with the Builder, in a total of about three minutes.

Both come with the ``raydog`` command, for seeing what clusters exist and shutting them down from anywhere.

A first look
------------

To run a cluster now, follow the :doc:`quickstart`: it starts one from the examples in the repository, with the Autoscaler or the Builder, runs a job on it and shuts it down.

With the Autoscaler:

.. code-block:: console

   $ ray up cluster.yaml        # create the cluster: a head node, and workers on demand
   $ ray down cluster.yaml      # shut it down

With the Builder:

.. code-block:: python

   from yellowdog_ray import ClusterSpec, NodeSpec, RayDogCluster

   head = NodeSpec(compute_requirement_template="my-namespace/head", start_script=HEAD_SCRIPT)
   worker = NodeSpec(compute_requirement_template="my-namespace/worker", start_script=WORKER_SCRIPT)

   with RayDogCluster(ClusterSpec(name="demo", namespace="my-namespace", head=head)) as cluster:
       cluster.add_worker_pool("workers", worker, count=4)
       head_node = cluster.build()          # returns when the head node is running
       ...                                  # use Ray at head_node.private_ip
   # the cluster is shut down here, whatever happened inside the block

Installation
------------

.. code-block:: console

   $ pip install yellowdog-ray          # or: uv pip install yellowdog-ray

RayDog needs Python 3.10 or later and Ray 2.54 or later, which it installs. The Ray on your own machine must be the same version as the Ray on the cluster's nodes. The Autoscaler and the ``raydog tunnel`` command also use the OpenSSH client (``ssh``), and the Autoscaler uses ``rsync``; both are part of any Linux or macOS system.

What you need from YellowDog
----------------------------

- A YellowDog account with a **namespace** to work in.
- An **application key** (an ID and a secret) allowed to create and manage work requirements and worker pools in that namespace. RayDog reads it from the environment variables ``YD_API_KEY_ID`` and ``YD_API_KEY_SECRET``, or from a ``.env`` file in or above the working directory. On a dedicated YellowDog platform, set ``YD_API_URL`` as well.
- One or more **compute requirement templates**, which say where and on what instances nodes may run. A small cluster typically uses one template for the head node and one for workers.
- Instances that can become Ray nodes: see :doc:`nodes`.

.. toctree::
   :hidden:

   Overview <self>

.. toctree::
   :maxdepth: 2
   :caption: Using RayDog

   quickstart
   background
   concepts
   autoscaler
   builder
   running-work
   cli
   nodes
   failures
   troubleshooting

.. toctree::
   :maxdepth: 2
   :caption: Reference

   migration
   testing
   yellowdog_ray
