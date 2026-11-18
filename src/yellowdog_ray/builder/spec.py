"""What a Builder cluster is made of."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from yellowdog_ray.core import naming
from yellowdog_ray.core.cluster import ClusterSettings, NodeSpec, validate_settings
from yellowdog_ray.errors import ConfigError

__all__ = ["ClusterSpec", "NodeSpec"]


@dataclass(frozen=True)
class ClusterSpec:
    """
    A cluster: its name, where it lives, and its head node.

    ``name``: lowercase letters, digits and hyphens, at most 24 characters.
    ``lifetime``: how long the cluster may live from the moment it is built, after
    which every node's task times out; ``None`` for no limit.
    ``build_timeout``: how long a node is given to start.
    ``idle_shutdown``: how long a node with nothing to run is kept.
    ``observability``: an optional node started before the head, whose address is
    given to every other node as ``OBSERVABILITY_HOST``.
    """

    name: str
    namespace: str
    head: NodeSpec
    tag: str | None = None
    lifetime: timedelta | None = timedelta(hours=24)
    build_timeout: timedelta = timedelta(minutes=10)
    idle_shutdown: timedelta = timedelta(minutes=3)
    observability: NodeSpec | None = None

    def settings(self) -> ClusterSettings:
        return ClusterSettings(
            name=self.name,
            namespace=self.namespace,
            tag=self.tag,
            lifetime=self.lifetime,
            idle_shutdown=self.idle_shutdown,
            build_timeout=self.build_timeout,
        )

    def validate(self) -> None:
        """Raise ``ConfigError`` for anything that could not work."""
        naming.validate_cluster_name(self.name)
        validate_settings(self.settings())
        validate_node("head", self.head)
        if self.observability is not None:
            validate_node("observability", self.observability)


def validate_node(what: str, node: NodeSpec) -> None:
    if not node.compute_requirement_template.strip():
        raise ConfigError(f"the {what} node has no compute requirement template")
    if not node.start_script.strip():
        raise ConfigError(f"the {what} node has no start script")
