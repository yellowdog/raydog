"""
RayDog Autoscaler: Ray cluster creation and autoscaling using YellowDog.
"""

import logging
import os
import random
import re
import subprocess
import sys
from datetime import datetime, timedelta
from time import sleep
from typing import Any

import redis
from dotenv import load_dotenv
from ray.autoscaler._private.cli_logger import cli_logger
from ray.autoscaler.command_runner import CommandRunnerInterface
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    TAG_RAY_NODE_KIND,
    TAG_RAY_USER_NODE_TYPE,
)
from requests import HTTPError
from sshtunnel import SSHTunnelForwarder
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
    TaskGroupStatus,
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
HEAD_NODE_TASK_POLLING_INTERVAL = timedelta(seconds=10.0)
TAG_SERVER_PORT = 16667
LOCALHOST = "127.0.0.1"

# Shut down nodes immediately, because the Ray autoscaler will
# already have waited before terminating
IDLE_NODE_YD_SHUTDOWN = timedelta(seconds=0)

# The 'max_workers' property in the autoscaler YAML will determine
# the actual maximum size of the worker pool; this prevents YellowDog
# imposing a separate limit
MAX_NODES_IN_WORKER_POOL = 100000

# API URL and Application Key/Secret
YD_API_URL_VAR = "YD_API_URL"
YD_DEFAULT_API_URL = "https://api.yellowdog.ai"
YD_API_KEY_ID_VAR = "YD_API_KEY_ID"
YD_API_KEY_SECRET_VAR = "YD_API_KEY_SECRET"

# Provider configuration property names
#   Required
PROP_CLUSTER_NAMESPACE = "cluster_namespace"
PROP_HEAD_START_RAY_SCRIPT = "head_start_ray_script"
PROP_WORKER_START_RAY_SCRIPT = "worker_start_ray_script"
PROP_AUTH = "auth"
#   Optional
PROP_CLUSTER_TAG = "cluster_tag"
PROP_CLUSTER_LIFETIME = "cluster_lifetime"
PROP_BUILD_TIMEOUT = "head_node_build_timeout"
PROP_REQUIRED_FILES = "required_files"

# Node configuration property names
#   Required
PROP_CRT = "compute_requirement_template"
#   Optional
PROP_IMAGES_ID = "images_id"
PROP_USERDATA = "userdata"
PROP_CAPTURE_TASKOUTPUT = "capture_taskoutput"
PROP_METRICS_ENABLED = "metrics_enabled"

# Tag and value names
TAG_PUBLIC_IP = "publicip"
TAG_PRIVATE_IP = "privateip"
TAG_TERMINATED = "terminated"
VAL_TRUE = "true"

# Other
PROP_SSH_USER = "ssh_user"
PROP_SSH_PRIVATE_KEY = "ssh_private_key"
SCRIPT_FILE_PREFIX = "file:"


LOG = logging.getLogger(__name__)


class RayDogNodeProvider(NodeProvider):
    """
    The RayDog implementation of a Ray autoscaling provider.
    """

    def __init__(self, provider_config: dict[str, Any], cluster_name: str) -> None:
        """
        Called by Ray to provide nodes for the cluster.
        """

        # ToDo: Remove
        LOG.setLevel(logging.DEBUG)
        cli_logger.configure(verbosity=2)

        LOG.debug(f"RayDogNodeProvider {cluster_name} {provider_config}")

        # Force the cluster name to be lower case, to match the naming requirements for YellowDog
        # ToDo: Needs tighter name enforcement
        cluster_name = cluster_name.lower()

        super().__init__(provider_config, cluster_name)

        # If running client-side the bootstrap function puts the auth info into provider_config
        self._auth_config = self.provider_config.get(PROP_AUTH)

        self._tag_store = TagStore(cluster_name)
        self._cmd_runner = None
        self._scripts = {}
        self._files_to_upload = set(provider_config.get(PROP_REQUIRED_FILES, []))

        self.head_node_public_ip = None
        self.head_node_private_ip = None

        # Work out whether this is the head node (i.e., autoscaling config
        # provided & running as the YD agent)
        config_file = self._get_autoscaling_config_option()
        if config_file:
            self._basepath = os.path.dirname(config_file)
            self._on_head_node = self._is_running_as_yd_agent()
        else:
            self._basepath = "."
            self._on_head_node = False

        # Decide how to boot up, depending on the situation
        if self._on_head_node:
            self._tag_store.connect(None, TAG_SERVER_PORT, self._auth_config)
            self._auto_raydog = AutoRayDog(
                provider_config, cluster_name, self._tag_store
            )

            # Make sure that YellowDog knows about this cluster
            if not self._auto_raydog.find_raydog_cluster():
                raise Exception(f"Failed to find info in YellowDog for {cluster_name}")

        else:  # Running on a client node
            self._auto_raydog = AutoRayDog(
                provider_config, cluster_name, self._tag_store
            )

            # Try to find an existing cluster
            if self._auto_raydog.find_raydog_cluster():
                LOG.debug(f"Found an existing head node")
                # Get the tags from an existing head node
                self._tag_store.connect(
                    self._auto_raydog.head_node_public_ip,
                    TAG_SERVER_PORT,
                    self._auth_config,
                )
            else:
                # Ray will create a new head node ... later
                pass

    @staticmethod
    def _is_running_as_yd_agent() -> bool:
        """
        Detect when autoscaler is running under the YellowDog agent.
        """
        # ToDo: there's no hard requirement for this to be the username
        return (os.environ.get("USER") == "yd-agent") or (
            os.environ.get("LOGNAME") == "yd-agent"
        )

    @staticmethod
    def _get_autoscaling_config_option() -> str | None:
        """
        Get the path for the autoscaling config file, if set.
        """
        # ToDo: this is unsafe for the case with no '='
        for arg in sys.argv:
            if arg.startswith("--autoscaling-config="):
                return arg.split("=")[1]
        return None

    def non_terminated_nodes(self, tag_filters: dict[str, str]) -> list[str]:
        """
        Return a list of node IDs filtered by the specified tags dict.

        This list must not include terminated nodes. For performance reasons,
        providers are allowed to cache the result of a call to
        non_terminated_nodes() to serve single-node queries (e.g. is_running(node_id)).
        This means that non_terminated_nodes() must be called again to refresh results.

        Examples:
            >>> from ray.autoscaler.node_provider import NodeProvider
            >>> from ray.autoscaler.tags import TAG_RAY_NODE_KIND
            >>> provider = NodeProvider(...) # doctest: +SKIP
            >>> provider.non_terminated_nodes( # doctest: +SKIP
            ...     {TAG_RAY_NODE_KIND: "worker"})
            ["node-1", "node-2"]
        """
        LOG.debug(f"non_terminated_nodes {tag_filters}")

        # Make sure the tags are up to date
        self._tag_store.refresh()

        # Look for nodes that meet the criteria
        shortlist = self._tag_store.find_matches(None, TAG_TERMINATED, "")
        for k, v in tag_filters.items():
            shortlist = self._tag_store.find_matches(shortlist, k, v)

        result = shortlist
        LOG.debug(f"matching nodes: {result}")
        return result

    def is_running(self, node_id: str) -> bool:
        """
        Is the specified node is running?
        """
        LOG.debug(f"is_running {node_id}")

        # True if the node exists but terminated flag isn't set
        status = self._tag_store.get_tag(node_id, TAG_TERMINATED)
        return status == ""

    def is_terminated(self, node_id: str) -> bool:
        """
        Is the specified node terminated?
        """
        LOG.debug(f"is_terminated {node_id}")

        # True if the node doesn't exist or the terminated flag is set
        status = self._tag_store.get_tag(node_id, TAG_TERMINATED)
        return (status is None) or (status != "")

    def node_tags(self, node_id: str) -> dict[str, str]:
        """
        Returns the tags of the given node ID.
        """
        LOG.debug(f"node_tags {node_id}")
        return self._tag_store.get_all_tags(node_id)

    def set_node_tags(self, node_id: str, tags: dict) -> None:
        """
        Sets the tag values (string dict) for the specified node.
        """
        LOG.debug(f"set_node_tags {node_id} {tags}")
        self._tag_store.update_tags(node_id, tags)

    def external_ip(self, node_id: str) -> str:
        """
        Returns the external IP of the given node.
        """
        LOG.debug(f"external_ip {node_id}")
        ip = self._tag_store.get_tag(node_id, TAG_PUBLIC_IP)
        if ip:
            return ip
        public_ip, _ = self._get_ip_addresses(node_id)
        return public_ip

    def internal_ip(self, node_id: str) -> str:
        """
        Returns the internal IP (Ray IP) of the given node.
        """
        LOG.debug(f"internal_ip {node_id}")
        ip = self._tag_store.get_tag(node_id, TAG_PRIVATE_IP)
        if ip:
            return ip
        _, private_ip = self._get_ip_addresses(node_id)
        return private_ip

    def create_node(
        self, node_config: dict[str, Any], tags: dict[str, str], count: int
    ) -> dict[str, Any] | None:
        """
        Creates a number of nodes within the namespace.
        """
        LOG.debug(f"create_node {node_config} {tags} {count}")

        node_type = tags[TAG_RAY_NODE_KIND]
        flavour = tags[TAG_RAY_USER_NODE_TYPE].lower()

        # Check that YellowDog knows how to create instances of this type
        # ToDo: handle the on-prem case
        if not self._auto_raydog.has_worker_pool(flavour):
            node_init_script = self._load_script(node_config.get(PROP_USERDATA))

            # Valkey must be installed on the head node
            if node_type == NODE_KIND_HEAD:
                node_init_script += r"""
VALKEY_VERSION=8.1.1
CPU=`arch | sed s/aarch64/arm64/`
curl -O  https://download.valkey.io/releases/valkey-$VALKEY_VERSION-jammy-$CPU.tar.gz
tar -xf valkey-$VALKEY_VERSION-jammy-$CPU.tar.gz  -C $YD_AGENT_HOME 
chown -R $YD_AGENT_USER:$YD_AGENT_USER $YD_AGENT_HOME/valkey*
"""
            # Create the YellowDog worker pool
            self._auto_raydog.create_worker_pool(
                flavour=flavour,
                node_type=node_type,
                node_config=node_config,
                count=count,
                userdata=node_init_script,
                metrics_enabled=node_config.get("metrics_enabled", False),
            )

        # Start the required tasks
        if node_type == NODE_KIND_HEAD:
            # Create a head node
            head_id = self._auto_raydog.create_head_node(
                flavour=flavour,
                ray_start_script=self._get_script_from_provider_config(
                    PROP_HEAD_START_RAY_SCRIPT
                ),
            )

            # Initialise tags & remember the IP addresses
            self._tag_store.update_tags(head_id, tags)
            self.head_node_public_ip, self.head_node_private_ip = (
                self._get_ip_addresses(head_id)
            )

            # Sync tag values with the head node
            self._tag_store.connect(
                self.head_node_public_ip, TAG_SERVER_PORT, self._auth_config
            )

            # Upload any extra scripts that the head node might need to
            # understand the autoscaler config file
            if self._files_to_upload:
                cmd_runner: CommandRunnerInterface = (
                    self._get_head_node_command_runner()
                )
                for filename in self._files_to_upload:
                    LOG.debug(f"Uploading {filename}")
                    cmd_runner.run_rsync_up(filename, f"~/{filename}")
        else:
            # Create worker nodes
            new_nodes = self._auto_raydog.create_worker_node_tasks(
                flavour=flavour,
                ray_start_script=self._get_script_from_provider_config(
                    PROP_WORKER_START_RAY_SCRIPT
                ),
                count=count,
            )

            # Initialise tags
            for node_id in new_nodes:
                self._tag_store.update_tags(node_id, tags)

    def create_node_with_resources_and_labels(
        self,
        node_config: dict[str, Any],
        tags: dict[str, str],
        count: int,
        resources: dict[str, float],
        labels: dict[str, str],
    ) -> dict[str, Any] | None:
        """
        Create nodes with a given resource and label config.
        This is the method actually called by the autoscaler. Prefer to
        implement this when possible directly, otherwise it delegates to the
        create_node() implementation.

        Optionally may throw a ray.autoscaler.node_launch_exception.NodeLaunchException.
        """
        LOG.info(f"create_node_with_resources_and_labels {node_config} {tags} {count}")
        return self.create_node(node_config, tags, count)

    def _get_ip_addresses(self, node_id: str) -> tuple[str, str]:
        """
        Get the public & private IP addresses from YellowDog.
        """
        public_ip, private_ip = self._auto_raydog.get_ip_addresses(node_id)

        # Add to the tag store
        self._tag_store.update_tags(
            node_id, {TAG_PRIVATE_IP: private_ip, TAG_PUBLIC_IP: public_ip}
        )

        # Add to the data used for reverse lookups
        self._internal_ip_cache[private_ip] = node_id
        self._external_ip_cache[public_ip] = node_id

        return (public_ip, private_ip)

    def terminate_node(self, node_id: str) -> None:
        """
        Terminates the specified node.
        """
        LOG.debug(f"terminate_node {node_id}")
        self._auto_raydog.yd_client.work_client.cancel_task_by_id(node_id, True)
        self._tag_store.update_tags(node_id, {TAG_TERMINATED: VAL_TRUE})
        # ToDo: can we delete the tags for terminated nodes, without creating sync issues?

    def terminate_nodes(self, node_ids: list[str]) -> None:
        """
        Terminates a set of nodes.
        """
        LOG.debug(f"terminate_nodes {node_ids}")
        if self._auto_raydog.head_node_task_id in node_ids:
            # if the head node is being terminated, just shut down the cluster
            self._auto_raydog.shut_down()
            for node_id in node_ids:
                self._tag_store.update_tags(node_id, {TAG_TERMINATED: VAL_TRUE})
        else:
            for node_id in node_ids:
                self.terminate_node(node_id)
        return None

    def prepare_for_head_node(self, cluster_config: dict[str, Any]) -> dict[str, Any]:
        """
        Returns a new cluster config with custom configs for head node.
        """
        LOG.debug(f"prepare_for_head_node {cluster_config}")
        return cluster_config

    @staticmethod
    def bootstrap_config(cluster_config: dict[str, Any]) -> dict[str, Any]:
        """
        Bootstraps the cluster config by adding env defaults if needed.
        """
        LOG.debug(f"bootstrap_config {cluster_config}")

        # copy the global auth info to the provider, so the constructor sees it
        cluster_config["provider"]["auth"] = cluster_config["auth"].copy()
        return cluster_config

    def _get_head_node_command_runner(self) -> CommandRunnerInterface:
        """
        Create a CommandRunner object for the head node.
        """
        assert self._auth_config

        if not self._cmd_runner:
            self._cmd_runner = self.get_command_runner(
                "Head Node",
                self._auto_raydog.head_node_task_id,
                self._auth_config,
                self.cluster_name,
                subprocess,
                False,
            )
        return self._cmd_runner

    def _get_script_from_provider_config(self, property_name: str) -> str:
        """
        Get a script from the provider config.
        """

        script = self._scripts.get(property_name)
        if script is not None:
            return script

        script = self._load_script(self.provider_config.get(property_name))
        self._scripts[property_name] = script
        return script

    def _load_script(self, script_or_script_path: str | None) -> str:
        """
        Load a script either directly or from a file.
        If None, return an empty string.
        """

        if script_or_script_path is None:
            return ""

        if not script_or_script_path.startswith(SCRIPT_FILE_PREFIX):
            return script_or_script_path

        script_path = script_or_script_path[len(SCRIPT_FILE_PREFIX) :].lstrip().rstrip()
        full_script_path = os.path.join(self._basepath, script_path)
        if not os.path.exists(full_script_path):
            raise Exception(f"Script file '{full_script_path}' does not exist")

        self._files_to_upload.add(script_path)

        with open(full_script_path) as f:
            return f.read()


class TagStore:
    """
    Manage the tags used to control everything. The tag store is a dedicated
    Redis/Valkey server running on the head node.
    """

    def __init__(self, cluster_name: str):
        self._cluster_name = cluster_name
        self._redis: redis = None
        self._tags: dict[str, dict] = {}

    def find_matches(
        self, longlist: list[str] | None, tag_name: str, tag_value: str
    ) -> list[str]:
        if longlist is None:
            longlist = self._tags.keys()
        shortlist = list(
            filter(
                lambda x: tag_value == self._tags.get(x, {}).get(tag_name, ""), longlist
            )
        )
        return shortlist

    def _update_tags(self, node_id: str, new_tags: dict[str, str]) -> None:
        assert node_id.startswith("ydid:task:")
        if node_id in self._tags:
            self._tags[node_id].update(new_tags)
        else:
            self._tags[node_id] = new_tags.copy()

    def update_tags(self, node_id: str, new_tags: dict[str, str]) -> None:
        self._update_tags(node_id, new_tags)
        self._writeback(node_id, new_tags)

    def get_all_tags(self, node_id: str) -> dict[str, str]:
        return self._tags.get(node_id, {})

    def get_tag(self, node_id: str, tag_name: str) -> str | None:
        if node_id in self._tags:
            return self._tags[node_id].get(tag_name, "")
        else:
            return None

    def connect(
        self, remote_server: str | None, port: int, auth_config: dict[str, str] = None
    ) -> None:
        """
        Connect to the Redis tag server on the head node.
        """

        # setup an SSH tunnel, if required
        if remote_server is not None:
            LOG.debug(f"Setting up SSH tunnel to tag server on {remote_server}")
            tunnel = SSHTunnelForwarder(
                remote_server,
                ssh_username=auth_config[PROP_SSH_USER],
                ssh_pkey=auth_config[PROP_SSH_PRIVATE_KEY],
                remote_bind_address=(LOCALHOST, port),
            )
            tunnel.start()
            LOG.debug(f"SSH tunnel local port {tunnel.local_bind_port}")
            port = tunnel.local_bind_port

        # Connect to Redis
        self._redis = redis.Redis(host="localhost", port=port, decode_responses=True)

        # Do an initial sync of tags
        if self._tags:
            self._writeback_all()
        self.refresh()

    def refresh(self) -> None:
        """
        Read all tags from the head node.
        """
        if self._redis:
            prefix = f"{self._cluster_name}:"

            cur, redis_keys = self._redis.scan(cursor=0, match=prefix + "*")
            while True:
                for key in redis_keys:
                    node_id = key.removeprefix(prefix)
                    tags = self._redis.hgetall(key)

                    # logger.debug(f"Tags for {node_id} {tags}")
                    self._update_tags(node_id, tags)

                if not cur:
                    break
                cur, redis_keys = self._redis.scan(cursor=cur, match=prefix + "*")

    def _writeback(self, node_id: str, tags: dict[str, str]) -> None:
        """
        Upload tag data for one node to the tag server.
        """
        if self._redis:
            self._redis.hset(f"{self._cluster_name}:{node_id}", mapping=tags)

    def _writeback_all(self) -> None:
        """
        Upload a set of tag data to the head node.
        """
        if self._redis:
            for node_id, node_tags in self._tags.items():
                self._writeback(node_id, node_tags)


class AutoRayDog:
    """
    Connect to YellowDog and use it to set up Ray clusters.
    """

    def __init__(
        self, provider_config: dict[str, Any], cluster_name: str, tag_store: TagStore
    ):
        self._is_shut_down = False

        self._namespace = provider_config[PROP_CLUSTER_NAMESPACE]
        self._cluster_name = cluster_name
        self._cluster_tag = provider_config.get(PROP_CLUSTER_TAG, "")  # Optional

        self._tag_store: TagStore = tag_store

        # Store the worker pool IDs for each node flavour
        self._worker_pools = {}

        # Store the work requirement ID for this cluster
        self._work_requirement_id: str | None = None

        # Generate a postfix for the cluster name, to avoid name clashes
        self._uniqueid = "".join(
            random.choices("0123456789abcdefghijklmnopqrstuvwxyz", k=8)
        )

        # Establish timeouts
        self._cluster_lifetime: timedelta | None = self._parse_duration(
            provider_config.get(PROP_CLUSTER_LIFETIME)
        )
        self._build_timeout: timedelta | None = self._parse_duration(
            provider_config.get(PROP_BUILD_TIMEOUT)
        )

        # Get the PlatformClient object
        self.yd_client: PlatformClient = self._get_yd_client()

        # The YD Task ID for the head node task
        self.head_node_task_id: str | None = None

        # Head node IP addresses
        self.head_node_public_ip: str | None = None
        self.head_node_private_ip: str | None = None

        # Add counter to worker pool task group names to allow
        # for task groups that have completed; one counter covers
        # all worker pool types
        self._worker_task_group_counter = 1

    def has_worker_pool(self, flavour: str) -> bool:
        """
        Is there an existing worker pool for this type of node?
        """
        return flavour in self._worker_pools

    def create_worker_pool(
        self,
        flavour: str,
        node_type: str,
        node_config: dict[str, Any],
        count: int,
        userdata: str,
        metrics_enabled: bool = True,
    ) -> None:
        """
        Create a new worker pool for the given type of node.
        """

        LOG.debug(f"create_worker_pool {flavour}")

        compute_requirement_template_id = node_config[PROP_CRT]
        images_id = node_config.get(PROP_IMAGES_ID, None)

        worker_pool_name = f"{self._cluster_name}-{self._uniqueid}-{flavour}"

        compute_requirement_template_usage = ComputeRequirementTemplateUsage(
            templateId=compute_requirement_template_id,
            requirementName=worker_pool_name,
            requirementNamespace=self._namespace,
            requirementTag=self._cluster_tag,
            targetInstanceCount=count,
            imagesId=images_id,
            userData=userdata,
            instanceTags=None,
        )

        provisioned_worker_pool_properties = ProvisionedWorkerPoolProperties(
            createNodeWorkers=NodeWorkerTarget.per_node(1),
            minNodes=0,
            maxNodes=1 if node_type == NODE_KIND_HEAD else MAX_NODES_IN_WORKER_POOL,
            workerTag=f"{flavour}_{self._uniqueid}",
            metricsEnabled=metrics_enabled,
            idleNodeShutdown=AutoShutdown(
                enabled=True,
                timeout=IDLE_NODE_YD_SHUTDOWN,
            ),
            idlePoolShutdown=AutoShutdown(
                enabled=True,
                timeout=self._cluster_lifetime,
            ),
        )

        worker_pool: WorkerPool = (
            self.yd_client.worker_pool_client.provision_worker_pool(
                compute_requirement_template_usage, provisioned_worker_pool_properties
            )
        )

        self._worker_pools[flavour] = worker_pool.id

    def _get_yd_client(self) -> PlatformClient:
        """
        Create the PlatformClient object using creds from environment variables
        and/or a .env file.
        """

        # Load extra environment variables from a .env file if it exists;
        # do not override existing variables (environment takes precedence)
        load_dotenv(verbose=False, override=False)

        # YellowDog API URL and Application credentials
        self._api_url = os.getenv(YD_API_URL_VAR, YD_DEFAULT_API_URL)
        self._api_key_id = os.getenv(YD_API_KEY_ID_VAR)
        self._api_key_secret = os.getenv(YD_API_KEY_SECRET_VAR)

        if not all([self._api_key_id, self._api_key_secret]):
            raise Exception(
                f"YellowDog application key ID '{YD_API_KEY_ID_VAR}' and "
                f"secret '{YD_API_KEY_SECRET_VAR}' env. variables must be set"
            )

        return PlatformClient.create(
            ServicesSchema(defaultUrl=self._api_url),
            ApiKey(
                self._api_key_id,
                self._api_key_secret,
            ),
        )

    def find_raydog_cluster(self) -> bool:
        """
        Try to find an existing RayDog cluster in YellowDog.
        Return True if the cluster was found, False otherwise.
        """

        # Is there a live work requirement with the right name?
        candidates = self.yd_client.work_client.get_work_requirements(
            WorkRequirementSearch(
                statuses=[WorkRequirementStatus.RUNNING], namespace=self._namespace
            )
        )

        work_req_id: str | None = None
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
        work_requirement = self._get_work_requirement()

        self._is_shut_down: bool = (
            work_requirement.status != WorkRequirementStatus.RUNNING
        )

        self._uniqueid: str = work_requirement.name[-8:]
        self._cluster_name: str = work_requirement.name[:-9]
        self._cluster_tag: str = work_requirement.tag

        # Task group for the head node
        head_task_group: TaskGroup = work_requirement.taskGroups[0]
        self._cluster_lifetime = head_task_group.runSpecification.taskTimeout

        head_task: Task = self._get_tasks_in_task_group(head_task_group.id).list_all()[
            0
        ]
        self.head_node_task_id = head_task.id

        # Find which worker pools already exist
        worker_pools: SearchClient[WorkerPoolSummary] = (
            self.yd_client.worker_pool_client.get_worker_pools(
                WorkerPoolSearch(
                    namespace=self._namespace,
                    statuses=[
                        WorkerPoolStatus.EMPTY,
                        WorkerPoolStatus.IDLE,
                        WorkerPoolStatus.PENDING,
                        WorkerPoolStatus.RUNNING,
                        WorkerPoolStatus.CONFIGURING,
                    ],
                )
            )
        )

        worker_pool_prefix = f"{self._cluster_name}-{self._uniqueid}-"
        for worker_pool in worker_pools.iterate():
            # ToDo: Should check the worker pool is in a useable state
            if worker_pool.name.startswith(worker_pool_prefix):
                flavour = worker_pool.name.removeprefix(worker_pool_prefix)
                self._worker_pools[flavour] = worker_pool.id

        # Get the node details for the head node
        self.head_node_public_ip, self.head_node_private_ip = self.get_ip_addresses(
            head_task.id
        )

        return True

    def _get_work_requirement(self) -> WorkRequirement:
        """
        Get the latest state of the YellowDog work requirement for this cluster.
        """
        return self.yd_client.work_client.get_work_requirement_by_id(
            self._work_requirement_id
        )

    def _get_tasks_in_task_group(self, task_group_id: str) -> SearchClient[Task]:
        """
        Helper method to do a search to find all the Tasks in a TaskGroup.
        """
        return self.yd_client.work_client.get_tasks(
            TaskSearch(
                workRequirementId=self._work_requirement_id,
                taskGroupId=task_group_id,
                statuses=[TaskStatus.EXECUTING],
            )
        )

    def get_ip_addresses(self, node_id: str) -> tuple[str, str]:
        """
        Extract the public and private IP addresses for the node.
        """
        while True:
            task: Task = self.yd_client.work_client.get_task_by_id(node_id)
            if task.status in [TaskStatus.PENDING, TaskStatus.READY]:
                LOG.debug(f"Waiting for {node_id} to start running")
                sleep(5)
            else:
                break

        yd_node_id: str = self._get_node_id_for_task(task)
        yd_node: Node = self.yd_client.worker_pool_client.get_node_by_id(yd_node_id)

        return yd_node.details.publicIpAddress, yd_node.details.privateIpAddress

    def shut_down(self) -> None:
        """
        Shut down the Ray cluster.
        """
        if not self._is_shut_down:
            self._is_shut_down = True

            # Cancel the work requirement & abort all tasks
            if self._work_requirement_id is not None:
                try:
                    self.yd_client.work_client.cancel_work_requirement_by_id(
                        self._work_requirement_id, abort=True
                    )
                except HTTPError as e:
                    if "InvalidWorkRequirementStatusException" in str(e):
                        pass  # Suppress exception if it's just a state transition error
                self._work_requirement_id = None

            # Shut down all worker pools
            for worker_pool_id in self._worker_pools.values():
                self.yd_client.worker_pool_client.shutdown_worker_pool_by_id(
                    worker_pool_id
                )
            self._worker_pools = {}

    def _get_node_id_for_task(self, task: Task) -> str:
        """
        Get the YellowDog ID for the node running a particular task.
        """
        return self.yd_client.worker_pool_client.get_node_by_worker_id(task.workerId).id

    @staticmethod
    def _parse_duration(duration_str: str | None) -> timedelta | None:
        """
        Regular expression parser to match days, hours, minutes, and seconds
        (e.g., "1d 2h 30m 15s")'; standalone numbers are treated as seconds.
        """
        if duration_str is None:
            return None

        # Check if the string is a standalone number; treat as seconds
        if re.match(r"^\d+$", duration_str.strip()):
            return timedelta(seconds=int(duration_str))

        # Regular expression to match days, hours, minutes, and seconds (e.g., "1d 2h 30m 15s")
        pattern = r"(?:(\d+)d\s*)?(?:(\d+)h\s*)?(?:(\d+)m\s*)?(?:(\d+)s\s*)?"
        match = re.match(pattern, duration_str.strip().lower())

        if not match or not re.search(r"[dhms]", duration_str, re.IGNORECASE):
            raise Exception(
                f"Invalid duration '{duration_str}' "
                f"durations should be of form (e.g.) '1d 2h 30m 15s'"
            )

        # Extract days, hours, minutes, seconds (convert to int, default to 0 if not present)
        days = int(match.group(1) or 0)
        hours = int(match.group(2) or 0)
        minutes = int(match.group(3) or 0)
        seconds = int(match.group(4) or 0)

        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

    def create_head_node(self, flavour: str, ray_start_script: str) -> str:
        """
        Create the head node for the cluster.
        """

        start_time = datetime.now()

        # Create the work requirement in YellowDog, if it isn't already there
        if self._work_requirement_id:
            work_requirement = self._get_work_requirement()
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
                        finishIfAllTasksFinished=True,
                        runSpecification=RunSpecification(
                            taskTypes=[TASK_TYPE],
                            workerTags=[f"{flavour}_{self._uniqueid}"],
                            namespaces=[self._namespace],
                            exclusiveWorkers=True,
                            taskTimeout=self._cluster_lifetime,
                        ),
                    )
                ],
            )

            work_requirement = self.yd_client.work_client.add_work_requirement(
                work_requirement
            )
            self._work_requirement_id = work_requirement.id

        # Create a task to run the head node
        head_node_task = Task(
            taskType=TASK_TYPE,
            taskData=ray_start_script,
            arguments=["taskdata.txt"],
            environment={
                "YD_API_KEY_ID": self._api_key_id,
                "YD_API_KEY_SECRET": self._api_key_secret,
                "YD_API_URL": self._api_url,
            },
            name="head_node_task",
        )

        self.head_node_task_id = (
            self.yd_client.work_client.add_tasks_to_task_group_by_id(
                work_requirement.taskGroups[0].id, [head_node_task]
            )[0].id
        )

        # Wait for the head node to start
        if self._build_timeout:
            endtime = start_time + self._build_timeout
            timed_out = lambda: (datetime.now() >= endtime)
        else:
            timed_out = lambda: False

        while True:
            head_task = self.yd_client.work_client.get_task_by_id(
                self.head_node_task_id
            )
            if head_task.status == TaskStatus.EXECUTING:
                break

            if timed_out():
                self.shut_down()
                raise TimeoutError(
                    "Timeout waiting for Ray head node task to enter EXECUTING state"
                )

            sleep(HEAD_NODE_TASK_POLLING_INTERVAL.total_seconds())

        return head_task.id

    def create_worker_node_tasks(
        self, flavour: str, ray_start_script: str, count: int
    ) -> list[str]:
        """
        Create the worker node tasks for a given worker pool.
        """

        LOG.debug(f"create_worker_node tasks {flavour} {count}")

        # Get the latest state of the work requirement from YellowDog
        work_requirement: WorkRequirement = (
            self.yd_client.work_client.get_work_requirement_by_id(
                self._work_requirement_id
            )
        )

        # Look for a task group for this node flavour in a
        # suitable state to have worker node tasks added
        task_group: TaskGroup | None = None
        for tg in work_requirement.taskGroups:
            if tg.tag == flavour and tg.status in [
                TaskGroupStatus.RUNNING,
                TaskStatus.PENDING,
            ]:
                task_group = tg
                break

        # If there isn't one, create it
        if not task_group:
            index = len(work_requirement.taskGroups)

            work_requirement.taskGroups.append(
                TaskGroup(
                    name=f"worker-nodes-{flavour}-{self._worker_task_group_counter}",
                    tag=flavour,
                    finishIfAnyTaskFailed=False,
                    finishIfAllTasksFinished=True,
                    runSpecification=RunSpecification(
                        taskTypes=[TASK_TYPE],
                        workerTags=[f"{flavour}_{self._uniqueid}"],
                        namespaces=[self._namespace],
                        exclusiveWorkers=True,
                        taskTimeout=self._cluster_lifetime,
                    ),
                )
            )
            self._worker_task_group_counter += 1

            work_requirement = self.yd_client.work_client.update_work_requirement(
                work_requirement
            )
            task_group = work_requirement.taskGroups[index]

        # Add tasks to create worker nodes
        worker_node_task = Task(
            taskType=TASK_TYPE,
            taskData=ray_start_script,
            arguments=["taskdata.txt"],
            environment={"RAY_HEAD_IP": self.head_node_private_ip},
        )

        new_tasks = self.yd_client.work_client.add_tasks_to_task_group_by_id(
            task_group.id,
            [worker_node_task for _ in range(count)],
        )

        # Return a list of node ids
        return [task.id for task in new_tasks]
