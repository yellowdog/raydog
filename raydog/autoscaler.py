import logging
import dotenv
import os
import shortuuid

from typing import Any, Dict, List, Optional
from datetime import timedelta

from ray.autoscaler.node_provider import NodeProvider

from yellowdog_client.model import ApiKey, ServicesSchema
from yellowdog_client.platform_client import PlatformClient

from raydog.raydog import RayDogCluster

logger = logging.getLogger(__name__)

class RayDogNodeProvider(NodeProvider):

    def __init__(self, provider_config: Dict[str, Any], cluster_name: str) -> None:
        print("RayDogNodeProvider", cluster_name, provider_config)
        self.provider_config = provider_config
        self.cluster_name = cluster_name

        self._internal_ip_cache: Dict[str, str] = {}
        self._external_ip_cache: Dict[str, str] = {}

        # Read any extra environment variables from a file
        dotenv.load_dotenv(verbose=True, override=True)

        # Connect to YellowDog
        self._api_url = os.getenv("YD_API_URL")
        self._api_key_id = os.getenv("YD_API_KEY_ID")
        self._api_key_secret = os.getenv("YD_API_KEY_SECRET")

        self._ydclient = PlatformClient.create(
            ServicesSchema(defaultUrl=self._api_url),
            ApiKey(
                os.getenv(self._api_key_id),
                os.getenv(self._api_key_secret),
            ),
        )

    @staticmethod
    def _read_file(filename):
        """Read the contents of a file"""
        with open(filename) as f:
            return f.read()

    def _get_lifetime(self):
        """Get the cluster lifetime frmo the config. If the lifetime ends with 'd' its in days, 
           'm' is minutes, 's' (or nothing) is seconds
        """
        lifetime = self.provider_config.get('lifetime')
        if not lifetime:
            return None
 
        lastchar = lifetime[-1].lower()
        if lastchar == 'd':
            return timedelta(days=int(lifetime[0:-1]))
        elif lastchar == 'm':
            return timedelta(minutes=int(lifetime[0:-1]))
        elif lastchar == 's':
            return timedelta(seconds=int(lifetime[0:-1]))
        else:
            return timedelta(seconds=int(lifetime))

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
        return []

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
        """
        node_type = tags['ray-node-type']
        node_name = tags['ray-node-name']

        userdata = self._read_file(node_config['userdata'])
        cmptreqt = node_config['compute_requirement_template']
        imagesid = node_config['images_id']

        if node_type == 'head':
            namespace = self.provider_config['namespace']

            self._raydog_cluster = RayDogCluster(
                yd_application_key_id=self._api_key_id,
                yd_application_key_secret=self._api_key_secret,
                yd_platform_api_url=self._api_url,

                cluster_name=self.cluster_name,
                cluster_namespace=namespace,
                cluster_tag="my-ray-tag",
                cluster_lifetime=self._get_lifetime(),

                head_node_compute_requirement_template_id=cmptreqt,
                head_node_images_id=imagesid,
                head_node_userdata=userdata
            )

            self._raydog_cluster.build()
        else:
            pass


        print("create_node", node_config, tags, count)

    def set_node_tags(self, node_id: str, tags: Dict[str, str]) -> None:
        """Sets the tag values (string dict) for the specified node."""
        print("set_node_tags", node_id)

    def terminate_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Terminates the specified node.

        Optionally return a mapping from deleted node ids to node
        metadata.
        """
        print("terminate_node", node_id)

    def prepare_for_head_node(self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Returns a new cluster config with custom configs for head node."""
        print("prepare_for_head_node", cluster_config)
        return cluster_config


    @staticmethod
    def bootstrap_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the cluster config by adding env defaults if needed."""
        print("bootstrap_config", cluster_config)
        return cluster_config
