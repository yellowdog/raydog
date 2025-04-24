
import logging
from typing import Any, Dict, List, Optional

from ray.autoscaler.node_provider import NodeProvider

class RayDogNodeProvider(NodeProvider):

    def __init__(self, provider_config: Dict[str, Any], cluster_name: str) -> None:
        self.provider_config = provider_config
        self.cluster_name = cluster_name
        self._internal_ip_cache: Dict[str, str] = {}
        self._external_ip_cache: Dict[str, str] = {}

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        """Return a list of node ids filtered by the specified tags dict.

        This list must not include terminated nodes. For performance reasons,
        providers are allowed to cache the result of a call to
        non_terminated_nodes() to serve single-node queries
        (e.g. is_running(node_id)). This means that non_terminate_nodes() must
        be called again to refresh results.

        Examples:
            >>> from ray.autoscaler.node_provider import NodeProvider
            >>> from ray.autoscaler.tags import TAG_RAY_NODE_KIND
            >>> provider = NodeProvider(...) # doctest: +SKIP
            >>> provider.non_terminated_nodes( # doctest: +SKIP
            ...     {TAG_RAY_NODE_KIND: "worker"})
            ["node-1", "node-2"]

        """
        print("non_terminated_nodes")

    def is_running(self, node_id: str) -> bool:
        """Return whether the specified node is running."""
        print("is_running", node_id)

    def is_terminated(self, node_id: str) -> bool:
        """Return whether the specified node is terminated."""
        print("is_terminated", node_id)

    def node_tags(self, node_id: str) -> Dict[str, str]:
        """Returns the tags of the given node (string dict)."""
        print("node_tags", node_id)

    def external_ip(self, node_id: str) -> str:
        """Returns the external ip of the given node."""
        print("external_ip", node_id)

    def internal_ip(self, node_id: str) -> str:
        """Returns the internal ip (Ray ip) of the given node."""
        print("internal_ip", node_id)

    def create_node(
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Optional[Dict[str, Any]]:
        """Creates a number of nodes within the namespace.

        Optionally returns a mapping from created node ids to node metadata.

        Optionally may throw a
        ray.autoscaler.node_launch_exception.NodeLaunchException which the
        autoscaler may use to provide additional functionality such as
        observability.

        """
        print("create_node", node_config)

    def set_node_tags(self, node_id: str, tags: Dict[str, str]) -> None:
        """Sets the tag values (string dict) for the specified node."""
        print("set_node_tags", node_id)

    def terminate_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Terminates the specified node.

        Optionally return a mapping from deleted node ids to node
        metadata.
        """
        print("terminate_node", node_id)

    @staticmethod
    def bootstrap_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the cluster config by adding env defaults if needed."""
        print("bootstrap_config", cluster_config)
        return cluster_config

    def prepare_for_head_node(self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Returns a new cluster config with custom configs for head node."""
        print("prepare_for_head_node", cluster_config)
        return cluster_config

    @staticmethod
    def fillout_available_node_types_resources(
        cluster_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fills out missing "resources" field for available_node_types."""
        print("fillout_available_node_types_resources", cluster_config)
        return cluster_config
