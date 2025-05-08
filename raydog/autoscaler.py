import json
import logging
import os
import random
import socket
import socketserver
import struct
import subprocess
import sys
import threading
import time

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from sshtunnel import SSHTunnelForwarder
from time import sleep
from typing import Any, Dict, List, Optional

from ray.autoscaler._private.cli_logger import cli_logger
from ray.autoscaler.command_runner import CommandRunnerInterface
from ray.autoscaler.node_provider import NodeProvider
from yellowdog_client.common import SearchClient
from yellowdog_client.model import (
    ApiKey,
    AutoShutdown,
    ComputeRequirement,
    ComputeRequirementStatus,
    ComputeRequirementSummary,
    ComputeRequirementSummarySearch,
    ComputeRequirementTemplateUsage,
    Node,
    NodeWorkerTarget,
    ProvisionedWorkerPoolProperties,
    RunSpecification,
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
    WorkRequirementStatus,
)
from yellowdog_client.platform_client import PlatformClient

TASK_TYPE = "bash"

HEAD_NODE_TASK_POLLING_INTERVAL_SECONDS = 10.0
IDLE_NODE_AND_POOL_SHUTDOWN_MINUTES = 10.0

TAG_SERVER_PORT = 16667

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.FileHandler('/tmp/autoscaler.log'))

@dataclass
class NodeInfo:
    task_id: str
    task: Task = None
    node: Node = None
    tags: Dict[str, str] = None
    ip: list[str] = None
    terminated: bool = False

class RayDogNodeProvider(NodeProvider):
    def __init__(self, provider_config: Dict[str, Any], cluster_name: str) -> None:   
        logger.setLevel(logging.DEBUG)
        cli_logger.configure(verbosity=2)

        logger.debug(f"RayDogNodeProvider {cluster_name} {provider_config}")

        # Force the cluster name to be lower case, to match the naming requirements for YellowDog
        cluster_name = cluster_name.lower()

        # Initialise the standard base class, in case there's anything useful in there        
        super().__init__(provider_config, cluster_name)

        # If we're running client-side, our bootstrap function puts the auth info into the provider_config
        self._auth_config = self.provider_config.get('auth')
 
        # Initialise various instance variables
        self._tag_store = TagStore()
        self._cmd_runner = None
        self._scripts = {}

        # Work out whether this is the head node (ie. autoscaling config provided & running as the YD agent) 
        configfile = self._get_autoscaling_config_option()
        if configfile:
            self._basepath = os.path.dirname(configfile) 
            self._on_head_node = self._is_running_as_yd_agent()
        else:
            self._basepath = '.'
            self._on_head_node = False

        # Decide how to boot up, depending on the situation
        if self._on_head_node:
            # Get the tag server running
            start_tag_server(self._tag_store)

            # Running on the head node
            self._raydog = RayDogHead(provider_config, cluster_name, self._tag_store)

            # Make sure that YellowDog knows about this cluster
            if not self._raydog.find_raydog_cluster():
                raise Exception(f"Failed to find info in YellowDog for {cluster_name}")
            
        else:
            # Running on a client node
            self._raydog = RayDogClient(provider_config, cluster_name, self._tag_store)

            if self._raydog.find_raydog_cluster():
                # Get the tags an existing head node
                self._connect_to_tag_server()
            else:
                # Ray will create a new head node ... later
                pass
    
    def _is_running_as_yd_agent(self) -> bool:
        """Detect when autoscaler is running under the YellowDog agent"""
        return ((os.environ.get("USER") == "yd-agent") or (os.environ.get("LOGNAME") == "yd-agent"))

    def _get_autoscaling_config_option(self) -> str:
        """Get the path for the autoscaling config file, if set"""
        for arg in sys.argv:
            if arg.startswith("--autoscaling-config="):
                return arg.split('=')[1]
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
            self._send_tags_to_head_node(tags)
        else:
            raise Exception(f"Unknown node id {node_id}")
        
    def external_ip(self, node_id: str) -> str:
        """Returns the external ip of the given node."""
        logger.debug(f"external_ip {node_id}")
        return self._raydog.get_ip_addresses(node_id)[0]
    
    def internal_ip(self, node_id: str) -> str:
        """Returns the internal ip (Ray ip) of the given node."""
        logger.debug(f"internal_ip {node_id}")
        return self._raydog.get_ip_addresses(node_id)[1]

    def create_node(
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Optional[Dict[str, Any]]:
        """Creates a number of nodes within the namespace."""
        logger.debug(f"create_node {node_config} {tags} {count}")

        # Start creating instances 
        flavour = tags['ray-user-node-type'].lower()
        self._raydog.provision_instances(
            flavour=flavour,
            node_config=node_config, 
            count=count, 
            userdata=self._get_script('initialization_script'), 
            metrics_enabled= True) 

        # Start the required tasks
        node_type = tags['ray-node-type']
        if node_type == 'head':
            # Create a head node
            self._raydog.create_head_node(
                flavour=flavour,
                tags=tags,
                ray_start_script=self._get_script('head_start_ray_script')
            )

            # Send initial tag values to the head node
            self._connect_to_tag_server()
            self._send_tags_to_head_node(self._tag_store.ray_tags)
        else:
            # Create worker nodes
            self._raydog.create_worker_nodes(
                flavour=flavour,
                tags=tags,
                ray_start_script=self._get_script('worker_start_ray_script'),
                count=count
            )

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
        assert self._auth_config

        if not self._cmd_runner:
            self._cmd_runner = self.get_command_runner(
                "Head Node",
                self._raydog.head_node_task_id,
                self._auth_config,
                self.cluster_name,
                subprocess,
                False,
            )
        return self._cmd_runner

    def _connect_to_tag_server(self):
        assert not self._on_head_node
        assert self._auth_config

        # setup SSH tunnel
        ip = self._raydog.head_node_public_ip
        logger.debug(f"Setting up SSH tunnel to tag server on {ip}")
        tunnel = SSHTunnelForwarder(
            ip,
            ssh_username=self._auth_config['ssh_user'],
            ssh_pkey=self._auth_config['ssh_private_key'],
            remote_bind_address=('127.0.0.1', TAG_SERVER_PORT)
        )
        tunnel.start()

        logger.debug(f"SSH tunnel local port {tunnel.local_bind_port}")

        # Open a local socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        sock.connect(('127.0.0.1', tunnel.local_bind_port))
        self._tag_server = sock

    def _message_to_tag_server(self, operation, data_in):
        assert not self._on_head_node          
        _send_message(self._tag_server, operation, data_in)
        reply, data_out = _receive_message(self._tag_server)
        assert reply == b'K'
        return data_out

    def _read_tags_from_head_node(self) -> None:
        """Read tags from the head node"""
        data = self._message_to_tag_server(b'G', b'')
 
        newtags = json.loads(data)
        self._tag_store.ray_tags.update(newtags)

    def _send_tags_to_head_node(self, thetags) -> None:
        """Upload tag data to the head node"""
        self._message_to_tag_server(b'U', _encode_tags(thetags))

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
        assert node_id.startswith("ydid:task:")

        info = self.node_info.get(node_id)
        if not info:
            raise Exception(f"Unknown node id {node_id}")
        return info

    def new_node_info(self, node_id:str, ray_tags:Dict, task:Task) -> NodeInfo:
        assert node_id.startswith("ydid:task:")

        info = NodeInfo(task_id=node_id, task=task, tags=ray_tags)
        self.node_info[node_id] = info
        self.ray_tags[node_id] = info.tags
        return info

    def del_node_info(self, node_id:str) -> None:
        assert node_id.startswith("ydid:task:")

        if node_id in self.node_info:
            del self.node_info[node_id]
            del self.ray_tags[node_id]


#----------------------------------------------------------------------------------------
# Simple TCP server to share tags stored on the head node
def _recv_bytes(req, n):
    thebytes = bytearray()
    while n > 0:
        newdata = req.recv(n)
        thebytes.extend(newdata)
        n -= len(newdata)
    return thebytes

def _encode_tags(tags):
    return bytes(json.dumps(tags), 'ascii')

def _receive_message(sock):
    header = _recv_bytes(sock, 5)
    operation, extra = struct.unpack("ci", header)
    if extra:
        payload = _recv_bytes(self.request, extra)
    else:
        payload = b''
    return operation, payload

def _send_message(sock, operation, payload):
    extra = len(payload) if payload else 0
    header = struct.pack("ci", operation, extra)
    sock.sendall(header + payload)
  
class ThreadedTagRequestHandler(socketserver.BaseRequestHandler):
    thetags: TagStore = None

    def handle(self):
        operation, payload = _receive_message(self.request)
        if operation == b'G':
            # get all the tags
            response = _encode_tags(self.thetags.ray_tags)
        elif operation == b'U':
            # update tags 
            newtags = json.loads(str(payload, 'ascii'))
            self.thetags.ray_tags.update(newtags)
            response = b''
        else: 
            response = b'RayDog Tag Server'

        _send_message(self.request, b'K', response)

class ThreadedTagServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

def start_tag_server(thetags: TagStore, port: int=16667):
    ThreadedTagRequestHandler.thetags = thetags

    server = ThreadedTagServer(("127.0.0.1", port), ThreadedTagRequestHandler)
    with server:
        ip, port = server.server_address

        # Start a thread with the server -- that thread will then start one
        # more thread for each request
        server_thread = threading.Thread(target=server.serve_forever)
		
        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()
        logger.debug(f"Tag server started on {ip}:{port}")


#----------------------------------------------------------------------------------------

class RayDogBase():
    def __init__(self, provider_config: Dict[str, Any], cluster_name: str, tag_store:TagStore):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        self._namespace = provider_config['namespace']
        self._cluster_name = cluster_name
        self._cluster_tag = cluster_name
        
        self._tag_store = tag_store

        self._worker_pools = {}
        self._work_requirement_id: str = None

        # Pick an ID for this run, to avoid name clashes
        self._uniqueid = ''.join(random.choices("0123456789abcdefghijklmnopqrstuvwxyz", k=8))

        # Determine how long to wait for things
        self._cluster_lifetime = self._parse_timespan(provider_config.get('lifetime'))
        self._build_timeout = self._parse_timespan(provider_config.get('build_timeout'))

        # Connect to YellowDog
        self._connect_to_yellowdog_api() 

    def provision_instances(self,
        flavour: str,
        node_config: Dict[str, Any], 
        count: int, 
        userdata:str, 
        metrics_enabled:bool = True) -> None:
        """Make sure there is a worker pool for this type of node"""

        compute_requirement_template_id = node_config['compute_requirement_template']
        images_id = node_config['images_id']

        #TODO: handle the on-prem case
        if flavour in self._worker_pools:
            pass
            #TODO: enlarge the worker pool, if needed
        else:
            worker_pool_name = f"{self._cluster_name}-{self._uniqueid}-{flavour}"

            compute_requirement_template_usage = ComputeRequirementTemplateUsage(
                templateId=compute_requirement_template_id,
                requirementName=worker_pool_name,
                requirementNamespace=self._namespace,
                requirementTag=self._cluster_tag,
                targetInstanceCount=count,
                imagesId=images_id,
                userData=userdata,
                instanceTags=None)

            auto_shut_down = AutoShutdown(
                enabled=True,
                timeout=timedelta(minutes=IDLE_NODE_AND_POOL_SHUTDOWN_MINUTES))
            
            provisioned_worker_pool_properties = ProvisionedWorkerPoolProperties(
                createNodeWorkers=NodeWorkerTarget.per_node(1),
                minNodes=1,
                maxNodes=node_config.get("max_nodes", 1000),
                workerTag=flavour,
                metricsEnabled=metrics_enabled,
                idleNodeShutdown=auto_shut_down,
                idlePoolShutdown=auto_shut_down)
            
            worker_pool: WorkerPool = self.yd_client.worker_pool_client.provision_worker_pool(                
                compute_requirement_template_usage,                
                provisioned_worker_pool_properties)
   
            self._worker_pools[flavour] = worker_pool.id

    def _connect_to_yellowdog_api(self) -> None:
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

    def find_raydog_cluster(self) -> bool:
        """Try to find an existing RayDog cluster in YellowDog""" 

        # Is there a live work requirement with the right name?
        candidates = self.yd_client.work_client.get_work_requirements(
            WorkRequirementSearch(
                statuses=[WorkRequirementStatus.RUNNING],
                namespace=self._namespace 
            )
        )
 
        work_req_id: str = None
        work_req: WorkRequirement
        for work_req in candidates.iterate():
            if work_req.name.startswith(self._cluster_name):
                work_req_id = work_req.id
                break

        # Not found         
        if work_req_id is None:
            return False

        # Found - fill in the details
        self._work_requirement_id = work_req_id
        work_requirement = self.get_work_requirement()

        self._is_shut_down: bool = (work_requirement.status != WorkRequirementStatus.RUNNING)

        self._uniqueid: str = work_requirement.name[-8:]
        self._cluster_name: str = work_requirement.name[:-9]
        self._cluster_tag: str = work_requirement.tag

        # Task group for the head node
        head_task_group: TaskGroup = work_requirement.taskGroups[0]
        self._cluster_lifetime = head_task_group.runSpecification.taskTimeout 

        head_task: Task = self.get_tasks_in_task_group(head_task_group.id).list_all()[0]
        self.head_node_task_id = head_task.id

        # Remember info about the head node
        self._tag_store.new_node_info(head_task.id, {}, head_task)

        # Create some dummy Ray tags 
        self._tag_store.new_node_info(
            node_id=head_task.id,
            ray_tags= {
                'ray-node-type' : 'head',
                'ray-node-status': 'up-to-date'
            },
            task=head_task)

        # Get the node details for the head node
        self.head_node_private_ip, self.head_node_public_ip = self.get_ip_addresses(head_task.id)

        return True
        
    def read_task_info(self, node_id):    
        return self.yd_client.work_client.get_task_by_id(node_id)

    def get_work_requirement(self) -> WorkRequirement:
        """Get the latest state of the YellowDog work requirement for this cluster"""
        return self.yd_client.work_client.get_work_requirement_by_id(self._work_requirement_id)

    def get_tasks_in_task_group(self, task_group_id:str) -> SearchClient[Task]:
        """Helper method to do a search to find all the Tasks in a TaskGroup"""
        return self.yd_client.work_client.get_tasks(TaskSearch(
            workRequirementId=self._work_requirement_id,
            taskGroupId=task_group_id,
            statuses=[TaskStatus.EXECUTING])
        )

    def get_ip_addresses(self, node_id: str) -> (str, str):
        info = self._tag_store.get_node_info(node_id)
        if info.ip:
            return info.ip
        
        info.task = self.read_task_info(node_id)
        if info.task.status != TaskStatus.EXECUTING:
            return ( None, None ) 

        ydnodeid = self._get_node_id_for_task(info.task)
        ydnode = self.yd_client.worker_pool_client.get_node_by_id(ydnodeid)

        info.ip = ( ydnode.details.publicIpAddress, ydnode.details.privateIpAddress )
        return info.ip

    def shut_down(self):
        #TODO: shutdown cleanly
        pass
    
    @staticmethod
    def _get_node_id_for_task(task: Task) -> str:
        """Get the YellowDog id for the node running a particular task. 
           Because of the way YD IDs are constructed, this is just string manipulation
        """
        return task.workerId.replace("wrkr", "node")[:-2]    

    @staticmethod
    def _parse_timespan(timespan: str) -> timedelta:
        """Parse a string representing a time span. If it ends with 'd' its in days, 
           'h' is hours, 'm' is minutes, 's' (or nothing) is seconds
        """
        if not timespan:
            return None
 
        lastchar = timespan[-1].lower()
        if lastchar in "dhms":
            duration = float(timespan[0:-1])
            if lastchar == 'd':
                return timedelta(days=duration)
            elif lastchar == 'h':
                return timedelta(hours=duration)
            elif lastchar == 'm':
                return timedelta(minutes=duration)
            elif lastchar == 's':
                return timedelta(seconds=duration)
        else:
            return timedelta(seconds=float(timespan))


class RayDogClient(RayDogBase):
    def __init__(self, provider_config: Dict[str, Any], cluster_name: str, tag_store:TagStore):
        super().__init__(provider_config, cluster_name, tag_store)

    def create_head_node(self, 
        flavour: str,
        tags: Dict[str, str],
        ray_start_script: str) :

        start_time = datetime.now()

        # Create the work requirement in YellowDog, if it isn't already there 
        if self._work_requirement_id:
            work_requirement = self.get_work_requirement()
        else:
            work_requirement = WorkRequirement(
                namespace=self._namespace,
                name=f"{self._cluster_name}-{self._uniqueid}",
                tag=self._cluster_tag,
                taskGroups=[
                    TaskGroup(
                        name="head-node",
                        tag=flavour,
                        finishIfAnyTaskFailed=True,
                        runSpecification=RunSpecification(
                            taskTypes=[TASK_TYPE],
                            workerTags=[flavour],
                            namespaces=[self._namespace],
                            exclusiveWorkers=True,
                            taskTimeout=self._cluster_lifetime,
                        ),
                    )
                ],
            )

            work_requirement = self.yd_client.work_client.add_work_requirement(work_requirement)
            self._work_requirement_id = work_requirement.id

        # Create a task to run the head node
        head_node_task = Task(
            taskType=TASK_TYPE,
            taskData=ray_start_script,
            arguments=["taskdata.txt"],
            environment={
                "YD_API_KEY_ID" : self._api_key_id,
                "YD_API_KEY_SECRET" : self._api_key_secret,
                "YD_API_URL" : self._api_url
            }
        )

        self.head_node_task_id = self.yd_client.work_client.add_tasks_to_task_group_by_id(
            work_requirement.taskGroups[0].id,
            [head_node_task])[0].id

        # Wait for the head node to start
        if self._build_timeout:
            endtime = start_time + self._build_timeout
            timed_out = lambda: (datetime.now() >= endtime)
        else:
            timed_out = lambda: False
             
        while True:  
            head_task = self.yd_client.work_client.get_task_by_id(self.head_node_task_id)
            if head_task.status == TaskStatus.EXECUTING:
                break

            if timed_out():
                self.shut_down()
                raise TimeoutError("Timeout waiting for Ray head node task to enter EXECUTING state")
            
            sleep(HEAD_NODE_TASK_POLLING_INTERVAL_SECONDS)

        # Remember the Ray tags 
        self._tag_store.new_node_info(
            node_id=head_task.id,
            ray_tags=tags,
            task=head_task)

        # Extract the head node's IP addresses
        self.head_node_private_ip, self.head_node_public_ip = self.get_ip_addresses(head_task.id)

    def create_worker_nodes(self, 
        flavour: str,
        tags: Dict[str, str],
        ray_start_script: str,
        count: int) :
        raise Exception(f"create_worker_nodes called on the client node")


class RayDogHead(RayDogBase):
    def __init__(self, provider_config: Dict[str, Any], cluster_name: str, tag_store:TagStore):
        super().__init__(provider_config, cluster_name, tag_store)

    def create_worker_nodes(self, 
        flavour: str,
        tags: Dict[str, str],
        ray_start_script: str,
        count: int) :
            
        # Get the latest state of the work requirement from YellowDog
        work_requirement: WorkRequirement = self.yd_client.work_client.get_work_requirement_by_id(self._work_requirement_id)

        # Look for a task group for this node flavour
        task_group: TaskGroup = None
        for tg in work_requirement.taskGroups:
            if tg.tag == flavour:
                task_group = tg
                break

        # If there isn't one, create it
        if task_group is None:
            index = len(work_requirement.taskGroups)

            work_requirement.taskGroups.append(TaskGroup(
                name=f"worker-nodes-{flavour}",
                tag=flavour,
                finishIfAnyTaskFailed=False,
                runSpecification=RunSpecification(
                    taskTypes=[TASK_TYPE],
                    workerTags=[flavour],
                    namespaces=[self._namespace],
                    exclusiveWorkers=True,
                    taskTimeout=self._cluster_lifetime,
                )
            ))
            work_requirement.taskGroups.append(task_group)

            work_requirement = self.yd_client.work_client.update_work_requirement(work_requirement)
            task_group = work_requirement.taskGroups[index]

        # Add tasks to create worker nodes
        worker_node_task = Task(
            taskType=TASK_TYPE,
            taskData=ray_start_script,
            arguments=["taskdata.txt"],
            environment={
                "RAY_HEAD_IP" : self.head_node_private_ip
            },
        )

        newtasks = self.yd_client.work_client.add_tasks_to_task_group_by_id(
            task_group.id,
            [ worker_node_task for _ in range(count) ],
        )

        # Store the Ray tags and task info
        for newtask in newtasks:
            self._tag_store.new_node_info(newtask.id, tags, newtask)

    def create_head_node(self, 
        flavour: str,
        tags: Dict[str, str],
        ray_start_script: str) :
        raise Exception(f"create_head_node called on the head node")
