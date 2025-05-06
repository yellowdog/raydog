import json
import logging
import os
import random
import subprocess
import sys
import time

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from datetime import timedelta

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.command_runner import CommandRunnerInterface

from ray.autoscaler._private.cli_logger import cli_logger

from yellowdog_client.common import (
    SearchClient
)

from yellowdog_client.model import (
    ApiKey, 
    ComputeRequirement,
    ComputeRequirementStatus,
    ComputeRequirementSummary,
    ComputeRequirementSummarySearch,
    ComputeRequirementTemplateUsage,
    Node,
    ServicesSchema,
    Task,
    TaskGroup,
    TaskSearch,
    TaskStatus,
    WorkerPool,
    WorkerPoolSearch,
    WorkerPoolStatus,
    WorkerPoolSummary,
    WorkRequirement,
    WorkRequirementSearch,
    WorkRequirementStatus
)
from yellowdog_client.platform_client import PlatformClient

from raydog.raydog import RayDogCluster

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.FileHandler('/tmp/autoscaler.log'))

@dataclass
class NodeInfo:
    task_id: str
    task: Task = None
    node: Node = None
    tags: Dict[str, str] = None
    ip: [str, str] = None
    terminated: bool = False

class RayDogNodeProvider(NodeProvider):
    def __init__(self, provider_config: Dict[str, Any], cluster_name: str) -> None:   
        logger.setLevel(logging.DEBUG)
        cli_logger.configure(verbosity=2)

        logger.debug(f"RayDogNodeProvider {cluster_name} {provider_config}")

        # force the cluster name to be lower case, to match naming requirements in YellowDog
        cluster_name = cluster_name.lower()

        # initialise the standard base class, in case there's anything useful in there        
        super().__init__(provider_config, cluster_name)

        # remember the YellowDog m=namespace
        self.namespace = self.provider_config['namespace']

        # Pick an ID for this run, to avoid name clashes
        self.uniqueid = ''.join(random.choices("0123456789abcdefghijklmnopqrstuvwxyz", k=8))

        # If we're running client-side, the bootstrap function puts the auth info into the provider_config
        self._auth_config = self.provider_config.get('auth')
 
         # Make sure various other variables exist. Values come later
        self._transfer_pending = None
        self._cmd_runner = None
        self._tag_store = TagStore()
        self._scripts = {}

        # Initialise the connection to YellowDog
        self._connect_to_yellowdog()

        # Work out whether this is the head node (ie. autoscaling config provided & running as the YD agent) 
        configfile = self._get_autoscaling_config_option()
        if configfile:
            self._basepath = os.path.dirname(configfile) 
            self._on_head_node = self._is_running_as_yd_agent()
        else:
            self._basepath = '.'
            self._on_head_node = False

        # Try to find an existing setup in YellowDog
        workreq = self._find_work_requirement()

        # Decide how to boot up, depending on the situation
        if self._on_head_node:
            # Running on the head node
            self._raydog = RayDogClusterConnection(self.yd_client, workreq, self._tag_store)
            self._headid = self._raydog.head_node_task_id

            # Wait for the client to upload initial tag data
            self._load_tags_sent_from_client()
        else:
            # Running on a client node
            if workreq:
                # Connect to an existing head node
                self._raydog = RayDogClusterConnection(self.yd_client, workreq, self._tag_store)
                self._headid = self._raydog.head_node_task_id

                # Will get the tags from the head node later
                self._pull_tags_from_head_node()
            else:
                # Ray will create a new head node ... later
                self._raydog = None
                #self._init_node_info()
    
    def _is_running_as_yd_agent(self) -> bool:
        """Detect when autoscaler is running under the YellowDog agent"""
        return ((os.environ.get("USER") == "yd-agent") or (os.environ.get("LOGNAME") == "yd-agent"))

    def _get_autoscaling_config_option(self) -> str:
        """Get the path for the autoscaling config file, if set"""
        for arg in sys.argv:
            if arg.startswith("--autoscaling-config="):
                return arg.split('=')[1]
        return None
    
    def _connect_to_yellowdog(self) -> None:
        """Connect to the YellowDog API, using creds from environment variables and/or a .env file"""

        # Read extra environment vars from a file ... a minimalist dotenv
        env_file = ".env"
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if (not line) or line.startswith('#'):
                        continue
                    name, equals, value = line.partition('=')
                    if equals == '=':
                        value = value.removeprefix('"').removesuffix('"') 
                        os.environ[name] = value

        # Get API login info from environment variables 
        self._api_url = os.getenv("YD_API_URL", "https://api.yellowdog.ai")
        self._api_key_id = os.getenv("YD_API_KEY_ID")
        self._api_key_secret = os.getenv("YD_API_KEY_SECRET")

        # Make the connection
        self.yd_client = PlatformClient.create(
            ServicesSchema(defaultUrl=self._api_url),
            ApiKey(
                self._api_key_id,
                self._api_key_secret,
            ),
        )

    def _find_work_requirement(self) -> str:
        """Try to find an existing cluster in YellowDog""" 

        # is there a live work requirement with the right name?
        candidates = self.yd_client.work_client.get_work_requirements(
            WorkRequirementSearch(
                statuses=[WorkRequirementStatus.RUNNING],
                namespace=self.namespace 
            )
        )
 
        for x in candidates.iterate():
            if x.name.startswith(self.cluster_name):
                return x.id
                 
        return None
    
    def _find_head_node(self):
        """Try to find an existing head node for the cluster through YellowDog""" 

        # is there a live compute requirement with the right name?
        candidates = self.yd_client.work_client.get_work_requirements(
            WorkRequirementSearch(
                statuses=[WorkRequirementStatus.RUNNING],
                namespace=self.namespace 
            )
        )
 
        for x in candidates.iterate():
            if x.name.startswith(self.cluster_name):
                return x.id
                 
        return None


    def __del__(self):
        """Shutdown the cluster"""
        logger.debug("RayDogNodeProvider destructor")
        # if self._raydog:
        #     self._raydog.shut_down()

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
        logger.debug(f"non_terminated_nodes {tag_filters}")


        candidates = filter(lambda x: not x.terminated, self._tag_store.node_info.values())
        for k, v in tag_filters.items():
            candidates = filter(lambda x: (v == x.tags.get(k)), candidates)

        result = [ x.task_id for x in candidates ]
        logger.debug(f"matching nodes: {result}")

        return result

    def is_running(self, node_id: str) -> bool:
        """Return whether the specified node is running."""
        logger.debug(f"is_running {node_id}")

    def is_terminated(self, node_id: str) -> bool:
        """Return whether the specified node is terminated."""
        logger.debug(f"is_terminated {node_id}")
        return self._tag_store.get_node_info(node_id).terminated

    def node_tags(self, node_id: str) -> Dict[str, str]:
        """Returns the tags of the given node (string dict)."""
        logger.debug(f"node_tags {node_id}")
        return self._tag_store.ray_tags.get(node_id, {})

    def set_node_tags(self, node_id: str, tags: Dict) -> None:
        """Sets the tag values (string dict) for the specified node."""
        logger.debug(f"set_node_tags {node_id} {tags}")

        if node_id in self._tag_store.ray_tags:
            self._tag_store.ray_tags[node_id].update(tags)
        else:
            raise Exception(f"Unknown node id {node_id}")

    def _get_ip_addresses(self, node_id: str) -> (str, str):
        info = self._tag_store.get_node_info(node_id)
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
        logger.debug(f"external_ip {node_id}")
        return self._get_ip_addresses(node_id)[0]
    
    def internal_ip(self, node_id: str) -> str:
        """Returns the internal ip (Ray ip) of the given node."""
        logger.debug(f"internal_ip {node_id}")
        return self._get_ip_addresses(node_id)[1]

    def create_node(
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Optional[Dict[str, Any]]:
        """Creates a number of nodes within the namespace.
        """
        logger.debug(f"create_node {node_config} {tags} {count}")

        node_type = tags['ray-node-type']
        node_name = tags['ray-node-name']

        if node_type == 'head':
            namespace = self.provider_config['namespace']

            self._raydog = RayDogCluster(
                yd_application_key_id=self._api_key_id,
                yd_application_key_secret=self._api_key_secret,
                yd_platform_api_url=self._api_url,

                cluster_name=self.cluster_name + '-' + self.uniqueid,
                cluster_namespace=namespace,
                cluster_tag=self.cluster_name,
                cluster_lifetime=self._get_cluster_lifetime(),

                head_node_compute_requirement_template_id=node_config['compute_requirement_template'],
                head_node_images_id=node_config['images_id'],
                head_node_userdata=self._get_script('initialization_script'),
                head_node_ray_start_script=self._get_script('head_start_ray_script')
            )

            self._raydog.build()

            # create the initial set of tags 
            self._headid = self._raydog.head_node_task_id
            info = self._tag_store.new_node_info(self._headid, tags, self._read_task_info(self._headid))
            info.ip = ( self._raydog.head_node_public_ip, self._raydog.head_node_private_ip )

            # need to send tags to the head node
            self._push_tags_to_head_node()
        else:
            # is there a worker pool for this worker type?
            pool = self._raydog.worker_node_worker_pools.get(node_name)
            if not pool:
                self._raydog.add_worker_pool(
                    worker_pool_internal_name=node_name,
                    worker_pool_node_count=count,

                    worker_node_compute_requirement_template_id=node_config['compute_requirement_template'],
                    worker_node_images_id=node_config['images_id'],
                    worker_node_userdata=self._get_script('initialization_script'),
                    worker_node_task_script=self._get_script('worker_start_ray_script')
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
                self._tag_store.new_node_info(newtask.id, tags, newtask)

            logger.debug("Trying to build a worker node")

    def terminate_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Terminates the specified node."""
        logger.debug(f"terminate_node {node_id}")
        self._raydog.yd_client.work_client.cancel_task_by_id(node_id, True)
        self._tag_store.get_node_info(node_id).terminated = True 

    def terminate_nodes(self, node_ids: List[str]) -> Optional[Dict[str, Any]]:
        """Terminates a set of nodes."""
        logger.debug(f"terminate_nodes {node_ids}")
        for node_id in node_ids:
            self.terminate_node(node_id)
        return None

    def prepare_for_head_node(self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Returns a new cluster config with custom configs for head node."""
        logger.info(f"prepare_for_head_node {cluster_config}")

        # new head node started, send it the tags we have created so far
        #self._push_tags_to_head_node()

        return cluster_config

    @staticmethod
    def bootstrap_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the cluster config by adding env defaults if needed."""
        # thisdir = os.path.dirname(__file__)
        # cluster_config['file_mounts'].update({
        #     "/opt/yellowdog/agent/raydog" : thisdir + "/",
        #     "/opt/yellowdog/agent/rayx" : thisdir + "/../rayx"
        # })
        logger.info(f"bootstrap_config {cluster_config}")

        # copy the global auth info to the provider, so the constructor sees it
        cluster_config["provider"]["auth"] = cluster_config["auth"].copy()
        return cluster_config


    def _get_head_node_command_runner(self) -> CommandRunnerInterface :
        """Create a CommandRunner object for the head node"""
        if not self._cmd_runner:
            self._cmd_runner = self.get_command_runner(
                "Head Node",
                self._headid,
                self._auth_config,
                self.cluster_name,
                subprocess,
                False,
            )
        return self._cmd_runner

    def _get_tag_file_name(self, on_head:bool, client_tags:bool=False):
        dirname = "/opt/yellowdog/agent/tags" if on_head else ".tags" 
        suffix = "-client" if client_tags else "" 
        return f"{dirname}/{self.cluster_name}{suffix}.json"

    def _load_tags_sent_from_client(self):
        """Get tags uploaded from the client, with initial values for the head node"""

        # wait for the file to arrive
        filename = self._get_tag_file_name(True, True)
        while not os.path.exists(filename):
            logger.info(f"Waiting for {filename}")
            time.sleep(5)

        # initialise the tag store   
        self._tag_store.read_tags_from_file(filename)

    def _pull_tags_from_head_node(self) -> None:
        """Read tags from the head node"""
        remotefile = self._get_tag_file_name(True)
        localfile = self._get_tag_file_name(False)

        runner = self._get_head_node_command_runner()
        runner.run_rsync_down(remotefile, localfile, {})

        #TODO: what if the file doesn't exist yet?
        self._tag_store.read_tags_from_file(localfile)

    def _push_tags_to_head_node(self) -> None:
        """Upload tag data to the head node"""
        remotefile = self._get_tag_file_name(True, True)
        localfile = self._get_tag_file_name(False, True)

        self._write_tags_to_file(localfile)
        runner = self._get_head_node_command_runner()
        runner.run_rsync_up(localfile, remotefile, {})

    def _write_tags_to_file(self, filename:str=None) -> None:
        """Store all the Ray tags in a file"""

        # Act like we're on the head node, unless told otherwise
        if not filename:
            filename = self._get_tag_file_name(True)

        # Make sure the directory exists
        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        # Save as JSON
        self._tag_store.write_tags_to_file(filename)


    # 
    def _read_task_info(self, node_id):    
        return self.yd_client.work_client.get_task_by_id(node_id)

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

    def _get_script(self, config_name: str):
        """Either read a script from a file or create it from a list of lines"""

        script = self._scripts.get(config_name)
        if not script:
            script = self.provider_config.get(config_name)
            if not script:
                return ""

            if isinstance(script, str) and script.startswith("file:"):
                filename = script[5:]        
                with open(os.path.join(self._basepath, filename)) as f:
                    script = f.read()

            elif isinstance(script, List):
                script = '\n'.join(script)

            self._scripts[config_name] = script

        return script



class TagStore():
    """manage the list of information about nodes and the related YellowDog tasks"""

    def __init__(self):
        self.node_info: Dict[str, NodeInfo] = {}
        self.ray_tags: Dict[str, str] = {}

    # NB: the node_id parameter for all these functions is really a Yellowdog task id
    def get_node_info(self, node_id:str) -> NodeInfo:
        info = self.node_info.get(node_id)
        if not info:
            raise Exception(f"Unknown node id {node_id}")
        return info

    def new_node_info(self, node_id:str, ray_tags:Dict, task:Task) -> NodeInfo:
        info = NodeInfo(task_id=node_id, task=task, tags=ray_tags)
        self.node_info[node_id] = info
        self.ray_tags[node_id] = info.tags
        return info

    def del_node_info(self, node_id:str) -> None:
        if node_id in self.node_info:
            del self.node_info[node_id]
            del self.ray_tags[node_id]

    def write_tags_to_file(self, filename):
        """Write Ray tags to a JSON file"""
        with open(filename, 'w') as f:
            json.dump(self.ray_tags, f)

    def read_tags_from_file(self, filename:str) -> Dict[str, Any]:
        """Read the Ray tags from a file"""

        # Read the JSON file
        with open(filename) as f:
            contents = f.read()
        newtags = json.loads(contents)

        # Merge into the info we  already have
        oldinfo = self.node_info

        self.node_info = {}
        self.ray_tags = newtags
        for node_id, tags in newtags.items():
            node = oldinfo.get(node_id)
            if node:
                node.tags = tags
                self.node_info[node_id] = node
            else:
                self.new_node_info(node_id, tags)



class RayDogClusterConnection(RayDogCluster):
    """Connect to a RayDogCluster that is already running"""

    def _get_tasks_in_task_group(self, task_group_id:str) -> SearchClient[Task]:
        """Helper method to do a search to find all the Tasks in a TaskGroup"""
        return self.yd_client.work_client.get_tasks(TaskSearch(
            workRequirementId=self.work_requirement_id,
            taskGroupId=task_group_id,
            statuses=[TaskStatus.EXECUTING])
        )

    def _get_ip_addresses(self, yd_node_id: str) -> ( str, str ):
        """Get the public and private IP addresses for a given node"""
        node: Node = self.yd_client.worker_pool_client.get_node_by_id(yd_node_id)
        return ( node.details.privateIpAddress, node.details.publicIpAddress )

    @staticmethod
    def _get_node_id_for_task(task: Task) -> str:
        """Get the YellowDog id for the node running a particular task. 
           Because of the way YD IDs are constructed, this is just string manipulation
        """
        return task.workerId.replace("wrkr", "node")[:-2]    

    def __init__(self, yd_client:PlatformClient, work_req:str, tag_store:TagStore):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        self.yd_client: PlatformClient = yd_client

        # Get the work requirement from YellowDog
        self.work_requirement_id: str = work_req

        self._work_requirement: WorkRequirement = self.yd_client.work_client.get_work_requirement_by_id(self.work_requirement_id)

        self._is_shut_down: bool = (self._work_requirement.status != WorkRequirementStatus.RUNNING)

        self._cluster_namespace: str = self._work_requirement.namespace
        self._cluster_name: str = self._work_requirement.name
        self._cluster_tag: str = self._work_requirement.tag

        # Task group for the head node
        head_task_group: TaskGroup = self._work_requirement.taskGroups[0]
        self._cluster_lifetime = head_task_group.runSpecification.taskTimeout 

        head_task: Task = self._get_tasks_in_task_group(head_task_group.id).list_all()[0]
        self.head_node_task_id = head_task.id

        # remember info about the head node
        tag_store.new_node_info(head_task.id, {}, head_task)

        # Get the node details for the head node
        self.head_node_node_id = self._get_node_id_for_task(head_task)
        self.head_node_private_ip, self.head_node_public_ip = self._get_ip_addresses(self.head_node_node_id)

        logger.info(f"worker task groups")

        # Look through the Task groups for worker nodes
        for worker_task_group in self._work_requirement.taskGroups[1:]:
            worker_task: Task
            for worker_task in self._get_tasks_in_task_group(worker_task_group.id).iterate():
                logger.info(f"Worker Task: {worker_task}")
                tag_store.new_node_info(worker_task.id, {}, worker_task)

        # Compute requirement template
        cr_summaries = self.yd_client.compute_client.get_compute_requirement_summaries(
            ComputeRequirementSummarySearch(
                namespace=self._cluster_namespace,
                tag=self._cluster_tag,         
                statuses=[ComputeRequirementStatus.NEW,
                          ComputeRequirementStatus.PROVISIONING,
                          ComputeRequirementStatus.RUNNING]
            )
        )
        
        logger.info(f"get_compute_requirement_summaries")

        head_node_naming = f"{self._cluster_name}-00"

        cr_summary: ComputeRequirementSummary
        for cr_summary in cr_summaries.iterate():
            if not cr_summary.name.startswith(self._cluster_name):
                continue

            cr: ComputeRequirement = self.yd_client.compute_client.get_compute_requirement_by_id(cr_summary.id)
            #logger.info(f"Compute Req: {cr}")
            logger.info(f"Compute Req Summary: {cr_summary}")

        # Provisioned worker pools
        worker_pools = self.yd_client.worker_pool_client.get_worker_pools(
            WorkerPoolSearch(
                namespace=self._cluster_namespace,
                statuses=[WorkerPoolStatus.CONFIGURING,
                          WorkerPoolStatus.EMPTY,
                          WorkerPoolStatus.IDLE, 
                          WorkerPoolStatus.PENDING, 
                          WorkerPoolStatus.RUNNING]
            )
        )
       
        logger.info(f"get_worker_pools")

        pool_summary: WorkerPoolSummary
        for pool_summary in worker_pools.iterate():
            if not pool_summary.name.startswith(self._cluster_name):
                continue

            pool: WorkerPool = self.yd_client.worker_pool_client.get_worker_pool_by_id(pool_summary.id)
            logger.info(f"Worker Pool: {pool}")

