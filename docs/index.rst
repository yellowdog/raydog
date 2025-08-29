RayDog: YellowDog Integration with Ray
======================================


Welcome to the documentation for YellowDog's integration with Ray: **RayDog**. This package adds Ray support to the `YellowDog Platform <https://yellowdog.ai>`_.

There are two usage models for RayDog:

1. A manual **Builder** library that offers manual control of the scale and node types within the Ray cluster.

2. A Ray **Autoscaler** provider that uses Ray autoscaling to control the scale and node types within the Ray cluster.

How RayDog Works
----------------

RayDog operates by modelling a Ray cluster as a YellowDog work requirement, and provisioning YellowDog worker pools to provide the Ray head node and worker nodes. This model applies both to the Builder and to the Autoscaler.

Behind the scenes, RayDog creates work requirements containing tasks that represent the Ray processes on a head node and multiple sets of worker nodes of varying types. The tasks are organised into task groups that match worker pools, also created by RayDog to supply nodes to run the Ray process tasks.

The lifecycle of the work requirement matches the lifecycle of the Ray cluster. As tasks are added and terminated by RayDog, the relevant worker pools will be scaled up and down by YellowDog.

This allows the power and flexibility of YellowDog resource provisioning and scaling to be applied to Ray.

Installation
------------

Install or upgrade from PyPI, e.g.::

   pip install --upgrade yellowdog-ray

RayDog requires Python 3.10 or later.

The RayDog ``yellowdog-ray`` package includes both the Builder and Autoscaler variants.

YellowDog Prequisites
---------------------

Use of RayDog requires a YellowDog account populated with cloud provider credentials, one or more compute source templates, one or more compute requirement templates, and an application key/secret with the permissions to create and manage worker pools and work requirements.

.. toctree::
   :maxdepth: 2
   :caption: The RayDog Builder

   builder

.. toctree::
   :maxdepth: 2
   :caption: The RayDog Autoscaler

   autoscaler

.. toctree::
   :maxdepth: 2
   :caption: Module Details

   yellowdog_ray

