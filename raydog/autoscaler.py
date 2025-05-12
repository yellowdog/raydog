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
#logger = logging.getLogger("RayDog")
logger.setLevel(logging.DEBUG)

loghandler = logging.FileHandler('/tmp/raydog.log')
loghandler.setFormatter(logging.Formatter(fmt="%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"))
logger.addHandler(loghandler)

@dataclass
class NodeInfo:
    task_id: str
    task: Task = None
    node: Node = None
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
            self._tag_server = TagServer(self._tag_store, TAG_SERVER_PORT)

            # Running on the head node
            self._raydog = RayDogHead(provider_config, cluster_name, self._tag_store)
            self._need_to_sync_tags = False

            # Make sure that YellowDog knows about this cluster
            if not self._raydog.find_raydog_cluster():
                raise Exception(f"Failed to find info in YellowDog for {cluster_name}")
            
        else:
            # Running on a client node
            self._raydog = RayDogClient(provider_config, cluster_name, self._tag_store)

            # Don't have a conenctino to the tag store yet
            self._tunnel = None
            self._remote_tag_server = None

            if self._raydog.find_raydog_cluster():
                # Get the tags from an existing head node
                logger.debug(f"Found an existimg head node")
                self._need_to_sync_tags = True
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

        ray_tags = self._tag_store.ray_tags
        for k, v in tag_filters.items():
            candidates = filter(lambda x: (v == ray_tags.get(x.task_id, {}).get(k)), candidates)
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

        if node_id not in self._tag_store.ray_tags:
            raise Exception(f"Unknown node id {node_id}")

        info = { node_id : tags }
        self._tag_store.update_ray_tags(info)

        if not self._on_head_node:
            # the last thing ray up does on the client is set the head node status  
            # the tags needs to be sync-ed before ray up exits
            lastchance = tags.get('ray-node-status') == 'up-to-date'

            if self._connect_to_tag_server(lastchance):
                if self._need_to_sync_tags:
                    self._send_tags_to_head_node(self._tag_store.ray_tags)
                    self._read_tags_from_head_node()
                    self._need_to_sync_tags = False
                else:
                   self._send_tags_to_head_node(tags)

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
        logger.info(f"create_node {node_config} {tags} {count}")

        # Tell YellowDog to start creating instances 
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

            # Sync tag values with the head node, when we can
            self._need_to_sync_tags = True
        else:
            # Create worker nodes
            self._raydog.create_worker_nodes(
                flavour=flavour,
                tags=tags,
                ray_start_script=self._get_script('worker_start_ray_script'),
                count=count
            )

    def create_node_with_resources_and_labels(
        self,
        node_config: Dict[str, Any],
        tags: Dict[str, str],
        count: int,
        resources: Dict[str, float],
        labels: Dict[str, str],
    ) -> Optional[Dict[str, Any]]:
        """Create nodes with a given resource and label config.
        This is the method actually called by the autoscaler. Prefer to
        implement this when possible directly, otherwise it delegates to the
        create_node() implementation.

        Optionally may throw a ray.autoscaler.node_launch_exception.NodeLaunchException.
        """
        logger.info(f"create_node_with_resources_and_labels {node_config} {tags} {count}")

        return self.create_node(node_config, tags, count)


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
        return cluster_config

    @staticmethod
    def bootstrap_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the cluster config by adding env defaults if needed."""
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

    # def _connect_to_tag_server(self, mustconnect: bool=False):
    #     """Open a connection to the tag server, via an SSH tunnel"""
    #     assert not self._on_head_node
    #     assert self._auth_config

    #     self.tunnel = None

    #     retry = 10
    #     while retry > 0:
    #         try:
    #             # Setup an SSH tunnel to the tag server on the head node
    #             self._connect_ssh_tunnel()

    #             # Make a connection
    #             logger.info("Connecting to tag server")
    #             sock = socket.create_connection(('127.0.0.1', self.tunnel.local_bind_port))
    #             self._remote_tag_server = sock

    #             # Try a wake-up message
    #             hello = self._message_to_tag_server('?', '')
    #             logger.debug(f"Tag server response: {hello}")

    #             # Do we need 
    #             if self._need_to_sync_tags:
    #                 self._send_tags_to_head_node(self._tag_store.ray_tags)
    #                 self._read_tags_from_head_node()
    #                 self._need_to_sync_tags = False

    #             # If we made this far without an exception, we're connected
    #             break
    #         except:
    #             # Retry if something goes wrong
    #             logger.info(f"Connection failed. Will retry {retry} more times")
    #             sleep(15.0)
    #             retry -= 1    

    # def _connect_ssh_tunnel(self):
    #     assert not self._on_head_node
    #     assert self._auth_config

    #     # Skip if tunnel is ok
    #     if self.tunnel:
    #         self.tunnel.check_tunnels()
    #         if self.tunnel.tunnel_is_up:
    #             return

    #     # Setup an SSH tunnel to the tag server on the head node
    #     ip = self._raydog.head_node_public_ip

    #     logger.debug(f"Setting up SSH tunnel to tag server on {ip}")
    #     self.tunnel = SSHTunnelForwarder(
    #         ip,
    #         ssh_username=self._auth_config['ssh_user'],
    #         ssh_pkey=self._auth_config['ssh_private_key'],
    #         remote_bind_address=('127.0.0.1', TAG_SERVER_PORT)
    #     )
    #     self.tunnel.start()

    #     logger.debug(f"SSH tunnel local port {self.tunnel.local_bind_port}")

    def _connect_to_tag_server(self, mustconnect:bool):
        assert not self._on_head_node
        assert self._auth_config

        # Setup an SSH tunnel to the tag server on the head node
        # if self._tunnel:
        #     self._tunnel.check_tunnels()
        #     if not self._tunnel.tunnel_is_up:
        #         self._tunnel = None

        if not self._tunnel:
            ip = self._raydog.head_node_public_ip

            logger.debug(f"Setting up SSH tunnel to tag server on {ip}")
            self._tunnel = SSHTunnelForwarder(
                ip,
                ssh_username=self._auth_config['ssh_user'],
                ssh_pkey=self._auth_config['ssh_private_key'],
                remote_bind_address=('127.0.0.1', TAG_SERVER_PORT)
            )
            self._tunnel.start()

            logger.debug(f"SSH tunnel local port {self._tunnel.local_bind_port}")
    
        # Connect to the tag server
        if self._remote_tag_server:
            return True
        else:
            retry = 10
            while retry > 0:
                try:
                    logger.info("Connecting to tag server")
                    sock = socket.create_connection(('127.0.0.1', self._tunnel.local_bind_port))
                    self._remote_tag_server = sock

                    # Try a wake-up message
                    hello = self._message_to_tag_server('?', '')
                    logger.debug(f"Tag server response: {hello}")

                    # If we made it this far without an exception, we're connected
                    return True
                except:
                    # Retry if something goes wrong
                    self._remote_tag_server = None
                    if mustconnect:
                        retry -= 1
                        logger.info(f"Connection failed. Will retry {retry} more times")
                        sleep(15.0)
                    else:
                        logger.info(f"Connection failed. Try again later")
                        break

        return False

    def _message_to_tag_server(self, operation: str, data_in: str) -> str:
        assert not self._on_head_node 

        TagProtocol.send_message(self._remote_tag_server, operation, data_in)
        reply, data_out = TagProtocol.receive_message(self._remote_tag_server)
        assert reply == '!'
        return data_out

    def _read_tags_from_head_node(self) -> None:
        """Read tags from the head node"""
        data = self._message_to_tag_server('G', '')
 
        newtags = json.loads(data)
        logger.debug(f"Tags received from head node {newtags}")
        self._tag_store.ray_tags.update(newtags)

    def _send_tags_to_head_node(self, thetags: Dict[str, Dict]) -> None:
        """Upload tag data to the head node"""
        self._message_to_tag_server('U', json.dumps(thetags))

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

        self.writelock = threading.Lock()

    # NB: the node_id parameter for all these functions is really a Yellowdog task id
    def get_node_info(self, node_id:str) -> NodeInfo:
        assert node_id.startswith("ydid:task:")

        info = self.node_info.get(node_id)
        if not info:
            raise Exception(f"Unknown node id {node_id}")
        return info

    def new_node_info(self, node_id:str, ray_tags:Dict, task:Task) -> NodeInfo:
        assert node_id.startswith("ydid:task:")

        info = NodeInfo(task_id=node_id, task=task)
        self.node_info[node_id] = info
        self.ray_tags[node_id] = ray_tags
        return info

    def del_node_info(self, node_id:str) -> None:
        assert node_id.startswith("ydid:task:")

        if node_id in self.node_info:
            del self.node_info[node_id]
            del self.ray_tags[node_id]

    def update_ray_tags(self, newtags: Dict) -> None:
        for node_id, node_tags in newtags.items():
            self.writelock.acquire()
            if node_id in self.ray_tags:
                self.ray_tags[node_id].update(node_tags)
            else:
                self.ray_tags[node_id] = node_tags.copy()
            self.writelock.release()

#----------------------------------------------------------------------------------------
# Simple TCP server to share tags stored on the head node

class TagProtocol:
    """ Class to encapsulate the details of the custom low-level comms protocol for 
        sharing tags.

        All messages start with a 1 digit opcode, followed by an 8 digit ASCII string 
        giving the length of the data and then the actual data, as bytes.

        If there is no data to send, messages are shortened to the opcode plus ';'
    """

    @staticmethod
    def _recv_bytes(sock: socket.socket, n: int) -> bytes:
        thebytes = bytearray()
        while n > 0:
            newdata = sock.recv(n)
            thebytes.extend(newdata)
            n -= len(newdata)
        return thebytes

    @staticmethod
    def send_message(sock: socket.socket, opcode: str, content: str) -> None:
        #logger.debug(f"tx opcode:{opcode} content:{content}")
        message = bytes(opcode, 'ascii')
        if content:
            extra_data = bytes(content, 'utf8')
            extra_len = len(extra_data)

            length_field = bytes(str(extra_len).zfill(8), 'ascii')
            message += length_field + extra_data
        else:
            message += b';'

        sock.sendall(message)

    @staticmethod
    def receive_message(sock: socket.socket) -> tuple[str, str]:
        first2 = TagProtocol._recv_bytes(sock, 2)
        opcode = chr(first2[0])
        if first2[1] in [ ord(';'),  ord('\n') ]:
            content = '' 
        else:
            next7 = TagProtocol._recv_bytes(sock, 7)
            extra_len = int(first2[1:2] + next7)
            extra_data = TagProtocol._recv_bytes(sock, extra_len)
            content = str(extra_data, 'utf8')

        #logger.debug(f"rx opcode:{opcode} content:{content}")
        return opcode, content
  
class TagRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            while True:
                opcode, content = TagProtocol.receive_message(self.request)

                response = ''
                if opcode == 'G':
                    # Get all the tags
                    response = json.dumps(self.server.tagstore.ray_tags)
                elif opcode == 'U':
                    # Update tags
                    newtags = json.loads(content)
                    self.server.tagstore.update_ray_tags(newtags)
                elif opcode == '?':
                    response = 'RayDog Tag Server'

                # Acknowledge with a !
                TagProtocol.send_message(self.request, '!', response)
        except Exception as e: 
            logger.error(f"Tag server failed\n{repr(e)}")
            return

class TagServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    def __init__(self, tagstore: TagStore, port: int):
        self.tagstore: TagStore = tagstore
        
        super().__init__(("127.0.0.1", port), TagRequestHandler)

        # Start a thread with the server, which will start another thread for each request
        self.server_thread = threading.Thread(target=self._run_server)
        
        # Exit the server thread when the main thread terminates
        self.server_thread.daemon = True
        self.server_thread.start()

    def _run_server(self):
        ip, port = self.server_address

        logger.debug(f"{datetime.now()} Tag server starting on {ip}:{port}")
        self.serve_forever()
        logger.debug(f"Tag server stopped")

#----------------------------------------------------------------------------------------
# Connect to YellowDog and use it to setup clusters

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

        logger.debug(f"provision_instances {flavour}")

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
        self.head_node_public_ip, self.head_node_private_ip = self.get_ip_addresses(head_task.id)

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
        self.head_node_public_ip, self.head_node_private_ip = self.get_ip_addresses(head_task.id)
 
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
            
        logger.debug(f"create_worker_nodes {flavour} {count}")
        
        # Get the latest state of the work requirement from YellowDog
        work_requirement: WorkRequirement = self.yd_client.work_client.get_work_requirement_by_id(self._work_requirement_id)

        #logger.debug(f"work_requirement {work_requirement}")

        # Look for a task group for this node flavour
        task_group: TaskGroup = None
        for tg in work_requirement.taskGroups:
            if tg.tag == flavour:
                task_group = tg
                break

        #logger.debug(f"task_group {task_group}")

        # If there isn't one, create it
        if not task_group:
            index = len(work_requirement.taskGroups)
            #logger.debug(f"index {index}")

            work_requirement.taskGroups.append(TaskGroup(
                name=f"worker-nodes-{flavour}",
                tag=flavour,
                finishIfAnyTaskFailed=False,
                runSpecification=RunSpecification(
                    taskTypes=[TASK_TYPE],
                    workerTags=[flavour],
                    namespaces=[self._namespace],
                    exclusiveWorkers=True,
                    taskTimeout=self._cluster_lifetime
                )
            ))

            work_requirement = self.yd_client.work_client.update_work_requirement(work_requirement)
            task_group = work_requirement.taskGroups[index]
            #logger.debug(f"work_requirement2 {work_requirement}")
            #logger.debug(f"task_group2 {task_group}")

        # Add tasks to create worker nodes
        worker_node_task = Task(
            taskType=TASK_TYPE,
            taskData=ray_start_script,
            arguments=["taskdata.txt"],
            environment={
                "RAY_HEAD_IP" : self.head_node_private_ip
            },
        )
        #logger.debug(f"worker_node_task {worker_node_task}")

        newtasks = self.yd_client.work_client.add_tasks_to_task_group_by_id(
            task_group.id,
            [ worker_node_task for _ in range(count) ],
        )

        #logger.debug(f"newtasks {newtasks}")
        # Store the Ray tags and task info
        for newtask in newtasks:
            self._tag_store.new_node_info(newtask.id, tags, newtask)

    def create_head_node(self, 
        flavour: str,
        tags: Dict[str, str],
        ray_start_script: str) :
        raise Exception(f"create_head_node called on the head node")
