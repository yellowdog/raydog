"""
The Autoscaler: Ray's own autoscaler, with YellowDog supplying the nodes.

Name the provider in a cluster YAML and use ``ray up`` and ``ray down`` as usual::

    provider:
      type: external
      module: yellowdog_ray.autoscaler.RayDogNodeProvider
"""

from yellowdog_ray.autoscaler.provider import RayDogNodeProvider

__all__ = ["RayDogNodeProvider"]
