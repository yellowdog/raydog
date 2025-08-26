RayDog: YellowDog Integration with Ray
======================================


Welcome to the documentation for YellowDog's integration with Ray: **RayDog**. This package adds Ray support to the `YellowDog Platform <https://yellowdog.ai>`_.

There are two usage models for RayDog:

1. A manual Cluster Builder library that allows manual control of the scale and node types within the Ray cluster.

2. A Ray Autoscaler provider, that uses Ray autoscaling to control the scale and node types with the Ray cluster.

These options are discussed in the sections below.

.. toctree::
   :maxdepth: 2
   :caption: YellowDog Prerequisites

   prerequisites


Installation
------------

Install or upgrade from PyPI, e.g.::

   pip install --upgrade yellowdog-ray

RayDog requires Python 3.10 or later.

.. toctree::
   :maxdepth: 2
   :caption: The RayDog Cluster Builder

   builder

.. toctree::
   :maxdepth: 2
   :caption: The RayDog Autoscaler

   autoscaler

.. toctree::
   :maxdepth: 2
   :caption: Module Details

   yellowdog_ray

