import logging
import dotenv
import os
import shortuuid

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from datetime import timedelta

from ray.autoscaler.node_provider import NodeProvider

from yellowdog_client.model import (
    ApiKey, 
    Node,
    ServicesSchema,
    Task,
    TaskGroup,
    TaskStatus
)
from yellowdog_client.platform_client import PlatformClient

from raydog.raydog import RayDogCluster

logger = logging.getLogger(__name__)

@dataclass
class NodeInfo:
    task_id: str
    tags: Dict[str, str] = None
    task: Task = None
    node: Node = None
    ip: [str, str] = None
    terminated: bool = False


class RayDogNodeProvider(NodeProvider):

    def __init__(self, provider_config: Dict[str, Any], cluster_name: str) -> None:
        print("RayDogNodeProvider", cluster_name, provider_config)
        self.provider_config = provider_config
        self.cluster_name = cluster_name
    
        shortuuid.set_alphabet("0123456789abcdefghijklmnopqrstuvwxyz")
        self.cluster_id = cluster_name + '-' + shortuuid.uuid()

        self._node_info: Dict[str, NodeInfo] = {}
        self._raydog = None

        self._ip_cache: Dict[str, (str, str)] = {}
     
        self._worker_pools: Dict[str, Any] = {}

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

    def __del__(self):
        """Shutdown the cluster"""
        print("RayDogNodeProvider destructor")
        if self._raydog:
            self._raydog.shut_down()

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
        print("non_terminated_nodes", tag_filters)

        candidates = filter(lambda x: not x.terminated, self._node_info.values())
        for k, v in tag_filters.items():
            candidates = filter(lambda x: (v == x.tags.get(k)), candidates)

        result = [ x.task_id for x in candidates ]
        print("matching nodes:", result)

        return result

    def is_running(self, node_id: str) -> bool:
        """Return whether the specified node is running."""
        print("is_running", node_id)

    def is_terminated(self, node_id: str) -> bool:
        """Return whether the specified node is terminated."""
        print("is_terminated", node_id)
        self._get_node_info(node_id).terminated

    def node_tags(self, node_id: str) -> Dict[str, str]:
        """Returns the tags of the given node (string dict)."""
        print("node_tags", node_id)
        self._get_node_info(node_id).tags

    def set_node_tags(self, node_id: str, tags: Dict[str, str]) -> None:
        """Sets the tag values (string dict) for the specified node."""
        print("set_node_tags", node_id, tags)
        self._get_node_info(node_id).tags.update(tags)

    def _get_ip_addresses(self, node_id: str) -> (str, str):
        info = self._get_node_info(node_id)
        if info.ip:
            return info.ip
        
        info.task = self._read_task_info(node_id)
        if info.task.status != TaskStatus.EXECUTING:
            return ( None, None ) 

        ydnodeid = info.task.workerId.replace("wrkr", "node")[:-2]
        ydnode = self.yd_client.worker_pool_client.get_node_by_id(ydnodeid)

        info.ip = ( ydnode.details.publicIpAddress, ydnode.details.privateIpAddress )
        return info.ip
        
    def external_ip(self, node_id: str) -> str:
        """Returns the external ip of the given node."""
        print("external_ip", node_id)
        return self._get_ip_addresses(node_id)[0]
    
    def internal_ip(self, node_id: str) -> str:
        """Returns the internal ip (Ray ip) of the given node."""
        print("internal_ip", node_id)
        return self._get_ip_addresses(node_id)[1]

    def create_node(
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Optional[Dict[str, Any]]:
        """Creates a number of nodes within the namespace.
        """
        print("create_node", node_config, tags, count)

        node_type = tags['ray-node-type']
        node_name = tags['ray-node-name']

        if node_type == 'head':
            namespace = self.provider_config['namespace']

            self._raydog = RayDogCluster(
                yd_application_key_id=self._api_key_id,
                yd_application_key_secret=self._api_key_secret,
                yd_platform_api_url=self._api_url,

                cluster_name=self.cluster_id,
                cluster_namespace=namespace,
                cluster_tag=self.cluster_name,
                cluster_lifetime=self._get_cluster_lifetime(),

                head_node_compute_requirement_template_id=node_config['compute_requirement_template'],
                head_node_images_id=node_config['images_id'],
                head_node_userdata=self._read_file(node_config['userdata']),
            )

            self._raydog.build()

            node_id = self._raydog.head_node_task_id
            info = self._new_node_info(node_id, tags)
            info.ip = ( self._raydog.head_node_public_ip, self._raydog.head_node_private_ip )

        else:
            # is there a worker pool for this worker type?
            pool = self._raydog.worker_node_worker_pools.get(node_name)
            if not pool:
                self._raydog.add_worker_pool(
                    worker_pool_internal_name=node_name,
                    worker_pool_node_count=count,

                    worker_node_compute_requirement_template_id=node_config['compute_requirement_template'],
                    worker_node_images_id=node_config['images_id'],
                    worker_node_userdata=self._read_file(node_config['userdata'])
                )
                pool = self._raydog.worker_node_worker_pools.get(node_name)

            # tell Yellowdog to create workers 
            task = pool.task_prototype
            task.environment.update(
                { "RAY_HEAD_IP" : self._raydog.head_node_private_ip }
            )

            newtasks = self._raydog.yd_client.work_client.add_tasks_to_task_group_by_id(
                pool.task_group_id, [ task for _ in range(count) ])

            # add to the list of workers
            for newtask in newtasks:
                self._new_node_info(newtask.id, tags, newtask)

            print("Trying to build a worker node")

    def terminate_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Terminates the specified node."""
        print("terminate_node", node_id)
        self._raydog.yd_client.cancel_task_by_id(node_id, True)
        self._get_node_info(node_id).terminated = True 

    def terminate_nodes(self, node_ids: List[str]) -> Optional[Dict[str, Any]]:
        """Terminates a set of nodes."""
        print("terminate_nodes", node_ids)
        for node_id in node_ids:
            self.terminate_node(node_id)
        return None

    def prepare_for_head_node(self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Returns a new cluster config with custom configs for head node."""
        print("prepare_for_head_node", cluster_config)
        return cluster_config

    @staticmethod
    def bootstrap_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the cluster config by adding env defaults if needed."""
        print("bootstrap_config", cluster_config)
        return cluster_config

    # manage the list of information about nodes and the related YellowDog tasks
    def _new_node_info(self, node_id, tags, task=None):
        if not task:
            task = self._read_task_info(node_id)

        info = NodeInfo(task_id=node_id, tags=tags, task=task)
        self._node_info[node_id] = info
        return info

    def _del_node_info(self, node_id):
        if node_id in self._node_info:
            del self._node_info[node_id]

    def _get_node_info(self, node_id):
        info = self._node_info.get(node_id)
        if not info:
            raise Exception(f"Unknown node id {node_id}")
        return info
    
    def _read_task_info(self, node_id):    
        return self._raydog.yd_client.work_client.get_task_by_id(node_id)

    def _get_cluster_lifetime(self):
        """Get the cluster lifetime from the config. If the lifetime ends with 'd' its in days, 
           'h' is hours, 'm' is minutes, 's' (or nothing) is seconds
        """
        lifetime = self.provider_config.get('lifetime')
        if not lifetime:
            return None
 
        lastchar = lifetime[-1].lower()
        if lastchar in "dhms":
            duration = float(lifetime[0:-1])
            if lastchar == 'd':
                return timedelta(days=duration)
            elif lastchar == 'h':
                return timedelta(hours=duration)
            elif lastchar == 'm':
                return timedelta(minutes=duration)
            elif lastchar == 's':
                return timedelta(seconds=duration)
        else:
            return timedelta(seconds=float(lifetime))

    @staticmethod
    def _read_file(filename):
        """Read the contents of a file"""
        with open(filename) as f:
            return f.read()

