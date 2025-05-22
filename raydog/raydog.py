"""
Build a Ray cluster using YellowDog.
"""

from copy import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import auto
from re import I
from time import sleep

from requests.exceptions import HTTPError
from yellowdog_client.model import (
    ApiKey,
    AutoShutdown,
    ComputeRequirementTemplateUsage,
    Node,
    NodeWorkerTarget,
    ProvisionedWorkerPool,
    ProvisionedWorkerPoolProperties,
    RunSpecification,
    ServicesSchema,
    Task,
    TaskOutput,
    TaskData,
    TaskDataInput,
    TaskDataOutput,
    TaskGroup,
    TaskOutput,
    TaskStatus,
    WorkRequirement,
    
)
from yellowdog_client.platform_client import PlatformClient
from utils.ray_ssh_tunnels import RayTunnels, SSHTunnelSpec

YD_DEFAULT_API_URL = "https://api.yellowdog.ai"

HEAD_NODE_TASK_GROUP_NAME = "head-node"
WORKER_NODES_TASK_GROUP_NAME = "worker-nodes"
OBSERVABILITY_NODE_TASK_GROUP_NAME = "observability-node"
K3S_SERVER_TASK_GROUP_NAME = "k3s-servers"

TASK_TYPE = "bash"

HEAD_NODE_TASK_POLLING_INTERVAL = timedelta(seconds=10)
IDLE_NODE_AND_POOL_SHUTDOWN_TIMEOUT = timedelta(minutes=10)
NODE_BOOT_TIMEOUT=timedelta(minutes=60)


@dataclass
class WorkerNodeWorkerPool:
    compute_requirement_template_usage: ComputeRequirementTemplateUsage
    provisioned_worker_pool_properties: ProvisionedWorkerPoolProperties
    task_group: TaskGroup
    task_prototype: Task
    worker_pool_id: str | None = None

@dataclass
class RayDogWorkSpecification:
    compute_requirement_template_id: str
    task_script: str
    images_id: str | None = None
    userdata: str | None = None
    instance_tags: dict[str, str] | None = None
    metrics_enabled: bool | None = None
    task_inputs: dict[str,str] | None = None
    capture_taskoutput: bool = False
    environment_variables: dict[str, str] = field(default_factory=dict)
    
    def get_yellowdog_objects(
        self, cluster_name: str, 
        cluster_namespace: str, 
        cluster_tag: str | None, 
        cluster_lifetime: timedelta | None, 
        node_naming: str, 
        task_name: str, 
        task_group_name: str,
        auto_shutdown: AutoShutdown | None
    ) -> tuple[ComputeRequirementTemplateUsage, ProvisionedWorkerPoolProperties, Task, TaskGroup]:
        
        task_inputs = None
        if self.task_inputs is not None:
            task_inputs = [TaskDataInput(source, dest) for source, dest in self.task_inputs.items()]
            
        return (
            ComputeRequirementTemplateUsage(
                templateId=self.compute_requirement_template_id,
                requirementName=node_naming,
                requirementNamespace=cluster_namespace,
                requirementTag=cluster_tag,
                targetInstanceCount=1,
                imagesId=self.images_id,
                userData=self.userdata,
                instanceTags=self.instance_tags,
            ),
            ProvisionedWorkerPoolProperties(
                createNodeWorkers=NodeWorkerTarget.per_node(1),
                minNodes=0,
                maxNodes=1,
                workerTag=node_naming,
                metricsEnabled=self.metrics_enabled,
                idleNodeShutdown=auto_shutdown,
                idlePoolShutdown=auto_shutdown,
            ),
            Task(name=task_name,
                taskType=TASK_TYPE,
                taskData=self.task_script,
                data=TaskData(task_inputs),
                arguments=["taskdata.txt"],
                environment=self.environment_variables,
                outputs=None if self.capture_taskoutput is False else [TaskOutput.from_task_process()],
            ),
            TaskGroup(
                name=task_group_name,
                finishIfAnyTaskFailed=True,
                runSpecification=RunSpecification(
                    taskTypes=[TASK_TYPE],
                    workerTags=[node_naming],
                    namespaces=[cluster_namespace],
                    exclusiveWorkers=True,
                    taskTimeout=cluster_lifetime,
                ),
            )
        )

class RayDogCluster:
    """
    A class representing a Ray cluster managed by YellowDog.
    """

    def __init__(
        self,
        yd_application_key_id: str,
        yd_application_key_secret: str,
        cluster_name: str,
        cluster_namespace: str,
        head_node_work_specification: RayDogWorkSpecification,
        yd_platform_api_url: str = YD_DEFAULT_API_URL,
        cluster_tag: str | None = None,
        observability_work_specification: RayDogWorkSpecification | None = None,
        k3s_work_specification: RayDogWorkSpecification | None = None,
        cluster_lifetime: timedelta | None = None,
    ):
        """
        Initialise the properties of the RayDog cluster and the Ray head node.
        Optionally set the properties of an observability node.

        :param yd_application_key_id: the key ID of the YellowDog application for connecting
            to the YellowDog platform.
        :param yd_application_key_secret: the key secret of the YellowDog application.
        :param cluster_name: a name for the cluster; the name must be unique to the
            YellowDog account and is used as the basis for the work requirement
            and worker pool names, and the worker tags.
        :param cluster_namespace: the YellowDog namespace to use for the cluster.
        :param head_node_compute_requirement_template_id: the YellowDog
            compute requirement template ID for the head node.
        :param head_node_ray_start_script: the Bash script for starting the ray head
            node processes.
        :param yd_platform_api_url: the URL of the YellowDog platform API.
        :param cluster_tag: an optional tag to use for the YellowDog work requirement
            and worker pool(s).
        :param head_node_images_id: the images ID to use for the head node
            (if required).
        :param head_node_userdata: optional userdata for use when the head node instance
            is provisioned.
        :param head_node_instance_tags: optional instance tags to use for the head node
            instance.
        :param head_node_metrics_enabled: whether to enable metrics collection for the
            head node.
        :param head_node_capture_taskoutput: whether to capture the console output of the
            head node task.
        :param enable_observability: whether to enable observability node support
        :param observability_node_compute_requirement_template_id: the compute requirement
            template to use for the observability node.
        :param observability_node_instance_tags: optional instance tags to use for the
            observability node instance.
        :param observability_node_images_id: the images ID to use for the observability node
            (if required).
        :param observability_node_userdata: optional userdata for use when the observability
            node instance is provisioned.
        :param observability_node_metrics_enabled: whether to enable metrics collection for
            the observability node.
        :param observability_node_start_script: the Bash script for starting the observability
            node processes.
        :param observability_node_capture_taskoutput: whether to capture the console output
            of the observability node task.
        :param cluster_lifetime: an optional timeout that will shut down the Ray
            cluster if it expires.
        """

        self._cluster_name = cluster_name
        self._cluster_namespace = cluster_namespace
        self._cluster_tag = cluster_tag
        self._cluster_lifetime = cluster_lifetime

        self._task_number = 0  # Running total of tasks

        head_node_naming = f"{cluster_name}-00-head"

        self._auto_shutdown = AutoShutdown(
            enabled=True,
            timeout=IDLE_NODE_AND_POOL_SHUTDOWN_TIMEOUT,
        )

        head_node_objects = head_node_work_specification.get_yellowdog_objects(cluster_name, cluster_namespace, cluster_tag, cluster_lifetime, head_node_naming, self._next_task_name, HEAD_NODE_TASK_GROUP_NAME, self._auto_shutdown)
        self._head_node_compute_requirement_template_usage = head_node_objects[0]
        self._head_node_provisioned_worker_pool_properties = head_node_objects[1]
        self._head_node_task = head_node_objects[2]
        self._head_node_task_group = head_node_objects[3]
        
        self._work_requirement = work_requirement = WorkRequirement(
            name=cluster_name,
            namespace=cluster_namespace,
            tag=cluster_tag,
            taskGroups=[self._head_node_task_group],
        )
        self._task_group_running_total = 0  # Note: never decremented
        self._is_built = False
        self._is_shut_down = False

        # Properties publicly available for reading
        self.yd_client = PlatformClient.create(
            ServicesSchema(defaultUrl=yd_platform_api_url),
            ApiKey(id=yd_application_key_id, secret=yd_application_key_secret),
        )
        self.work_requirement_id: str | None = None
        self.head_node_worker_pool_id: str | None = None
        self.head_node_node_id: str | None = None
        self.head_node_private_ip: str | None = None
        self.head_node_public_ip: str | None = None
        self.head_node_task_id: str | None = None
        self.worker_node_worker_pools: dict[str, WorkerNodeWorkerPool] = {}
        
        self.enable_observability = False
        if observability_work_specification is not None:
            self.enable_observability = True
            # Set observability node properties
            observability_node_naming = f"{cluster_name}-observability-00"
            observability_objects = observability_work_specification.get_yellowdog_objects(cluster_name, cluster_namespace, cluster_tag, cluster_lifetime, observability_node_naming, self._next_task_name, OBSERVABILITY_NODE_TASK_GROUP_NAME, self._auto_shutdown)
            self._observability_node_compute_requirement_template_usage = observability_objects[0]
            self._observability_node_provisioned_worker_pool_properties = observability_objects[1]           
            self._observability_node_task = observability_objects[2]
            self._work_requirement.taskGroups.append(observability_objects[3])
    
            self.observability_node_id: str | None = None
            self.observability_node_private_ip: str | None = None
            self.observability_node_worker_pool_id: str | None = None
            self.observability_node_private_ip: str | None = None
            self.observability_node_task_id: str | None = None
        
        self.enable_k3s = False
        if k3s_work_specification is not None:
            self.enable_k3s = True
            
            k3s_naming = f"{cluster_name}-k3s-00"
            k3s_objects = k3s_work_specification.get_yellowdog_objects(cluster_name, cluster_namespace, cluster_tag, cluster_lifetime, k3s_naming, self._next_task_name, K3S_SERVER_TASK_GROUP_NAME, self._auto_shutdown)
            
            self._k3s_compute_requirement_template_usage = k3s_objects[0]
            self._k3s_provisioned_worker_pool_properties = k3s_objects[1]           
            self._k3s_task = k3s_objects[2]
            self._work_requirement.taskGroups.append(k3s_objects[3])
    
            self.k3s_id: str | None = None
            self.k3s_private_ip: str | None = None
            self.k3s_worker_pool_id: str | None = None
            self.k3s_private_ip: str | None = None
            self.k3s_task_id: str | None = None
        
        self._ssh_tunnels: RayTunnels | None = None

    def add_worker_pool(
        self,
        worker_node_compute_requirement_template_id: str,
        worker_node_task_script: str,
        worker_pool_node_count: int,
        worker_pool_internal_name: str | None = None,
        worker_node_images_id: str | None = None,
        worker_node_userdata: str | None = None,
        worker_node_instance_tags: dict[str, str] | None = None,
        worker_node_metrics_enabled: bool | None = None,
        worker_node_capture_taskoutput: bool = False,
        workers_per_node: int = 1,
        worker_node_environment_variables: dict[str, str] = {},
        worker_node_task_inputs: dict[str, str] | None = None,
        worker_node_task_outputs: dict[str, str] | None = None,
    ) -> str | None:
        """
        Add a worker pool and task group that will provide Ray worker nodes.

        :param worker_node_compute_requirement_template_id: the YellowDog compute
            requirement template ID to use for the worker nodes in this worker
            pool.
        :param worker_node_task_script: the Bash script for starting the ray worker
            nodes in this worker pool.
        :param worker_pool_node_count: the number of ray worker nodes to create in
            this worker pool. Must be > 0.
        :param worker_pool_internal_name: an optional internal name that can be used
            to look up the worker_node_worker_pool_object. Must be unique to the cluster.
        :param worker_node_images_id: the images ID to use with the compute
            requirement template, if required.
        :param worker_node_userdata: optional userdata for use when the worker node
            instances are provisioned.
        :param worker_node_instance_tags: optional instance tags to apply to the
            worker node instances.
        :param worker_node_metrics_enabled: whether to enable metrics collection for the
            worker nodes.
        :param worker_node_capture_taskoutput: whether to capture the console output of the
            worker node tasks.
        :return: returns the worker pool ID if a worker pool was created, or None if the
            pool will be created later using the build() method.
        """

        if self._is_shut_down:
            raise Exception(
                "'add_worker_pool()' method called on already shut-down cluster"
            )

        if worker_pool_node_count < 1:
            raise ValueError("worker_pool_node_count must be > 0")
        
        task_script = worker_node_task_script
        if task_script is None:
            task_script = DEFAULT_SCRIPTS['worker-node-task-script']

        self._task_group_running_total += 1
        worker_pool_index_str = str(self._task_group_running_total).zfill(2)
        task_group_name = f"{WORKER_NODES_TASK_GROUP_NAME}-{worker_pool_index_str}"
        worker_pool_name = f"{self._cluster_name}-{worker_pool_index_str}-wrkrs"
        
        worker_node_environment_variables.update({
            "CLUSTER_NAME": self._cluster_name,
        })

        task_inputs = None
        if worker_node_task_inputs is not None:
            task_inputs = [TaskDataInput(source, dest) for source, dest in worker_node_task_inputs.items()]
        task_outputs = None
        if worker_node_task_outputs is not None:
            task_outputs = [TaskDataOutput(source, dest, True) for source, dest in worker_node_task_outputs.items()]
            
        worker_node_worker_pool = WorkerNodeWorkerPool(
            compute_requirement_template_usage=ComputeRequirementTemplateUsage(
                templateId=worker_node_compute_requirement_template_id,
                requirementName=worker_pool_name,
                requirementNamespace=self._cluster_namespace,
                requirementTag=self._cluster_tag,
                targetInstanceCount=worker_pool_node_count,
                imagesId=worker_node_images_id,
                userData=worker_node_userdata,
                instanceTags=worker_node_instance_tags,
            ),
            provisioned_worker_pool_properties=ProvisionedWorkerPoolProperties(
                createNodeWorkers=NodeWorkerTarget.per_node(1),
                minNodes=0,
                maxNodes=worker_pool_node_count,
                workerTag=worker_pool_name,
                metricsEnabled=worker_node_metrics_enabled,
                idleNodeShutdown=self._auto_shutdown,
                idlePoolShutdown=self._auto_shutdown,
                nodeBootTimeout=NODE_BOOT_TIMEOUT,
            ),
            task_group=TaskGroup(
                name=task_group_name,
                finishIfAnyTaskFailed=False,
                runSpecification=RunSpecification(
                    taskTypes=[TASK_TYPE],
                    workerTags=[worker_pool_name],
                    namespaces=[self._cluster_namespace],
                    exclusiveWorkers=True,
                    taskTimeout=self._cluster_lifetime,
                ),
            ),
            task_prototype=Task(
                taskType=TASK_TYPE,
                taskData=task_script,
                arguments=["taskdata.txt"],
                environment=worker_node_environment_variables,
                data=TaskData(task_inputs, task_outputs),
                outputs=(
                    None
                    if worker_node_capture_taskoutput is False
                    else [TaskOutput.from_task_process()]
                ),
            ),
        )
        
        worker_node_worker_pool.task_prototype.environment.update({"WORKERS_PER_NODE": workers_per_node})

        internal_name = (
            task_group_name
            if worker_pool_internal_name is None
            else worker_pool_internal_name
        )
        if self.worker_node_worker_pools.get(internal_name) is not None:
            raise Exception(
                f"Internal name '{internal_name}' for worker node worker pool "
                "is already in use"
            )
        self.worker_node_worker_pools.update({internal_name: worker_node_worker_pool})

        if not self._is_built:
            return None  # Don't provision the worker pool; wait for build()

        # Provision the new worker pool
        worker_node_worker_pool.worker_pool_id = (
            self.yd_client.worker_pool_client.provision_worker_pool(
                worker_node_worker_pool.compute_requirement_template_usage,
                worker_node_worker_pool.provisioned_worker_pool_properties,
            ).id
        )

        # Add the new task group
        work_requirement = self.yd_client.work_client.get_work_requirement_by_id(
            self.work_requirement_id
        )
        work_requirement.taskGroups.append(worker_node_worker_pool.task_group)
        work_requirement = self.yd_client.work_client.update_work_requirement(
            work_requirement
        )

        # Add the worker node tasks to the task group
        self._add_tasks_to_task_group(
            task_group_id=work_requirement.taskGroups[
                len(work_requirement.taskGroups) - 1
            ].id,
            worker_node_worker_pool=worker_node_worker_pool,
        )

        return worker_node_worker_pool.worker_pool_id

    def build(
        self, head_node_build_timeout: timedelta | None = None
    ) -> (str, str | None):
        """
        Build the cluster. This method will block until the Ray head node
        is ready, and optionally also the observability node.

        Note that Ray worker nodes will still be in the process
        of configuring and joining the cluster after this method returns.

        :param head_node_build_timeout: an optional timeout for building the head node;
            if the timeout expires before the head node task is executing, a TimeoutError
            exception will be raised.
        :return: a tuple containing the private IP address of the head node, and the
            public IP address of the head node (or None).
        """

        if self._is_built:
            raise Exception("'build()' method already called")

        if self._is_shut_down:
            raise Exception("'build()' method called on already shut-down cluster")

        start_time = datetime.now(timezone.utc)

        # Provision all currently defined worker pools
        self.head_node_worker_pool_id = (
            self.yd_client.worker_pool_client.provision_worker_pool(
                self._head_node_compute_requirement_template_usage,
                self._head_node_provisioned_worker_pool_properties,
            ).id
        )
        for worker_node_worker_pool in self.worker_node_worker_pools.values():
            worker_node_worker_pool.worker_pool_id = (
                self.yd_client.worker_pool_client.provision_worker_pool(
                    worker_node_worker_pool.compute_requirement_template_usage,
                    worker_node_worker_pool.provisioned_worker_pool_properties,
                ).id
            )

        # Add currently defined worker node task groups to the work requirement,
        # and submit it
        self._work_requirement.taskGroups += [
            worker_node_worker_pool.task_group
            for worker_node_worker_pool in self.worker_node_worker_pools.values()
        ]
        self._work_requirement = self.yd_client.work_client.add_work_requirement(
            self._work_requirement
        )
        self.work_requirement_id = self._work_requirement.id

        if self.enable_observability:
            self.observability_node_worker_pool_id = (
                self.yd_client.worker_pool_client.provision_worker_pool(
                    self._observability_node_compute_requirement_template_usage,
                    self._observability_node_provisioned_worker_pool_properties,
                ).id
            )
            self.observability_node_task_id = (
                self.yd_client.work_client.add_tasks_to_task_group_by_id(
                    self._work_requirement.taskGroups[1].id,
                    [self._observability_node_task],
                )[0].id
            )
            while True:
                observability_task = self.yd_client.work_client.get_task_by_id(
                    self.observability_node_task_id
                )
                if observability_task.status == TaskStatus.EXECUTING:
                    break
                if (
                    head_node_build_timeout is not None
                    and datetime.now(timezone.utc) - start_time
                    >= head_node_build_timeout
                ):
                    self.shut_down()
                    raise TimeoutError(
                        "Timeout waiting for observability node task to enter EXECUTING state"
                    )

            self.observability_node_id = observability_task.workerId.replace(
                "wrkr", "node"
            )[:-2]
            observability_node: Node = self.yd_client.worker_pool_client.get_node_by_id(
                self.observability_node_id
            )
            self.observability_node_private_ip = (
                observability_node.details.privateIpAddress
            )
            self._head_node_task.environment.update(
                {"OBSERVABILITY_HOST": self.observability_node_private_ip}
            )

        if self.enable_k3s:
            self.k3s_worker_pool_id = (
                self.yd_client.worker_pool_client.provision_worker_pool(
                    self._k3s_compute_requirement_template_usage,
                    self._k3s_provisioned_worker_pool_properties,
                ).id
            )
            tgid = 2 if self.enable_observability else 1
            self.k3s_server_task_id = (
                self.yd_client.work_client.add_tasks_to_task_group_by_id(
                    self._work_requirement.taskGroups[tgid].id,
                    [self._k3s_task],
                )[0].id
            )
            while True:
                k3s_server_task = self.yd_client.work_client.get_task_by_id(
                    self.k3s_server_task_id
                )
                if k3s_server_task.status == TaskStatus.EXECUTING:
                    break
                if (
                    head_node_build_timeout is not None
                    and datetime.now(timezone.utc) - start_time
                    >= head_node_build_timeout
                ):
                    self.shut_down()
                    raise TimeoutError(
                        "Timeout waiting for observability node task to enter EXECUTING state"
                    )
    
            self.k3s_server_id = k3s_server_task.workerId.replace(
                "wrkr", "node"
            )[:-2]
            k3s_server: Node = self.yd_client.worker_pool_client.get_node_by_id(
                self.k3s_server_id
            )
            self.k3s_server_public_ip = (k3s_server.details.publicIpAddress)
            self.k3s_server_private_ip = (k3s_server.details.privateIpAddress)
            self._head_node_task.environment.update({"K3S_URL": f"https://{self.k3s_server_private_ip}:6443"})
        # Add the head node task to the first task group
        self.head_node_task_id = (
            self.yd_client.work_client.add_tasks_to_task_group_by_id(
                self._work_requirement.taskGroups[0].id,
                [self._head_node_task],
            )[0].id
        )

        while True:  # Check for execution of the head node task
            task = self.yd_client.work_client.get_task_by_id(self.head_node_task_id)
            if task.status == TaskStatus.EXECUTING:
                break
            if (
                head_node_build_timeout is not None
                and datetime.now(timezone.utc) - start_time >= head_node_build_timeout
            ):
                self.shut_down()
                raise TimeoutError(
                    "Timeout waiting for Ray head node task to enter EXECUTING state"
                )
            sleep(HEAD_NODE_TASK_POLLING_INTERVAL.seconds)

        # Set the head node ID and get the node details
        self.head_node_node_id = task.workerId.replace("wrkr", "node")[:-2]
        node: Node = self.yd_client.worker_pool_client.get_node_by_id(
            self.head_node_node_id
        )
        self.head_node_private_ip = node.details.privateIpAddress
        self.head_node_public_ip = node.details.publicIpAddress

        # Add worker node tasks to their task groups, one task per worker node
        for task_group_index, worker_node_worker_pool in enumerate(
            self.worker_node_worker_pools.values()
        ):
            if self.enable_k3s:
                task_group_index +=1
            if self.enable_observability:
                task_group_index +=1
            
            self._add_tasks_to_task_group(
                task_group_id=self._work_requirement.taskGroups[task_group_index+1].id,
                worker_node_worker_pool=worker_node_worker_pool,
            )

        self._is_built = True
        return self.head_node_private_ip, self.head_node_public_ip

    def remove_worker_pool(self, worker_pool_id: str):
        """
        Terminate the compute requirement associated with a worker pool.

        :param worker_pool_id: the ID of the worker pool to remove.
        """

        if self._is_shut_down:
            raise Exception(
                "'remove_worker_pool()' method called on already shut-down cluster"
            )

        for (
            worker_pool_internal_name,
            worker_node_worker_pool,
        ) in self.worker_node_worker_pools.items():
            if worker_pool_id == worker_node_worker_pool.worker_pool_id:
                name_to_remove = worker_pool_internal_name
                break
        else:
            raise Exception(
                f"Worker pool ID '{worker_pool_id}' not "
                "in current list of worker node worker pools"
            )

        worker_pool: ProvisionedWorkerPool = (
            self.yd_client.worker_pool_client.get_worker_pool_by_id(worker_pool_id)
        )
        self.yd_client.compute_client.terminate_compute_requirement_by_id(
            worker_pool.computeRequirementId
        )
        self.worker_node_worker_pools.pop(name_to_remove)

    def remove_worker_pool_by_internal_name(self, internal_name: str):
        """
        Remove a worker pool by its internal name. Raises exception if
        worker pool not found.

        :param internal_name: the internal name of the worker pool to remove.
        """
        if self._is_shut_down:
            raise Exception(
                "'remove_worker_pool_by_internal_name()' "
                "method called on already shut-down cluster"
            )

        worker_node_worker_pool = self.worker_node_worker_pools.get(internal_name)
        if worker_node_worker_pool is None:
            raise Exception(
                f"Worker pool with internal name '{internal_name}' not found"
            )

        if worker_node_worker_pool.worker_pool_id is not None:
            self.remove_worker_pool(worker_node_worker_pool.worker_pool_id)
        else:
            self.worker_node_worker_pools.pop(internal_name)

    @property
    def worker_pool_ids(self) -> list[str]:
        """
        Generate the current list of worker pool IDs.
        """
        return [
            x.worker_pool_id
            for x in self.worker_node_worker_pools.values()
            if x.worker_pool_id is not None
        ]

    @property
    def worker_pool_internal_names(self) -> list[str]:
        """
        Generate the current list of worker pool internal names.
        """
        return list(self.worker_node_worker_pools.keys())

    def shut_down(self):
        """
        Shut down the Ray cluster by cancelling the work requirement, including
        aborting all its tasks, and shutting down all remaining worker pools.
        """

        if self._is_shut_down:
            return

        if self.work_requirement_id is not None:
            try:
                self.yd_client.work_client.cancel_work_requirement_by_id(
                    self.work_requirement_id, abort=True
                )
            except HTTPError as e:
                if "InvalidWorkRequirementStatusException" in str(e):
                    pass  # Suppress exception if it's just a state transition error
            self.work_requirement_id = None

        if self.head_node_worker_pool_id is not None:
            self.yd_client.worker_pool_client.shutdown_worker_pool_by_id(
                self.head_node_worker_pool_id
            )
            self.head_node_worker_pool_id = None

        if self.enable_k3s and self.k3s_worker_pool_id is not None:
            self.yd_client.worker_pool_client.shutdown_worker_pool_by_id(self.k3s_worker_pool_id)
            self.k3s_worker_pool_id = None
            
        if self.enable_observability and self.observability_node_worker_pool_id is not None:
            self.yd_client.worker_pool_client.shutdown_worker_pool_by_id(self.observability_node_worker_pool_id)
            self.observability_node_worker_pool_id = None

        for worker_node_worker_pool in self.worker_node_worker_pools.values():
            if worker_node_worker_pool.worker_pool_id is not None:
                self.yd_client.worker_pool_client.shutdown_worker_pool_by_id(
                    worker_node_worker_pool.worker_pool_id
                )
        self.worker_node_worker_pools = {}

        self._is_shut_down = True

    def _add_tasks_to_task_group(
        self, task_group_id: str, worker_node_worker_pool: WorkerNodeWorkerPool
    ):
        """
        Internal utility to add worker pool tasks to the applicable task group.

        :param task_group_id: the ID of the task group.
        :param worker_node_worker_pool: the properties of the worker nodes worker pool.
        """

        worker_node_worker_pool.task_prototype.environment.update({"RAY_HEAD_NODE_PRIVATE_IP": self.head_node_private_ip})
        
        if self.enable_k3s:
            worker_node_worker_pool.task_prototype.environment.update({"K3S_URL": f"https://{self.k3s_server_private_ip}:6443"})
            
        if self.enable_observability:
            worker_node_worker_pool.task_prototype.environment.update({"OBSERVABILITY_HOST": self.observability_node_private_ip})

        target_instances = worker_node_worker_pool.compute_requirement_template_usage.targetInstanceCount
        tasks: list[Task] = []
        for _ in range(target_instances):
            task = copy(worker_node_worker_pool.task_prototype)
            task.name = self._next_task_name
            tasks.append(task)

        self.yd_client.work_client.add_tasks_to_task_group_by_id(task_group_id, tasks)

    @property
    def _next_task_name(self) -> str:
        """
        Generate a unique task name.
        """
        self._task_number += 1
        return f"task-{str(self._task_number).zfill(5)}"
        
    def start_tunnels(self):
        ports = []
        tunnel_address=""
        
        if self.enable_k3s:
            # Nodeport numbers in the 30k range allocated as per scripts/k3s/manifests/ray-head.yaml
            ports.append(("localhost", 10001, "localhost", 30001))
            ports.append(("localhost", 8265, "localhost", 30002))
            ports.append(("localhost", 6379, "localhost", 30003))
            # Nodeport numbers in the 30k range allocated as per scripts/k3s/manifests/kube-prometheus-stack.yaml
            ports.append(("localhost", 3000, "localhost", 30030))
            ports.append(("localhost", 9090, "localhost", 30090))
            tunnel_address = self.k3s_server_public_ip
        else:
            ports.append(("localhost", 10001, "localhost", 10001))
            ports.append(("localhost", 8265, "localhost", 8265))
            ports.append(("localhost", 6379, "localhost", 6379))
            if self.enable_observability:
                ports.append(("localhost", 3000, str(self.observability_node_private_ip), 3000))
                ports.append(("localhost", 9090, str(self.observability_node_private_ip), 9090))
            tunnel_address = str(self.head_node_public_ip)
                
        self._ssh_tunnels = RayTunnels(
            ray_head_ip_address=tunnel_address,
            ssh_user="yd-agent",
            private_key_file="private-key",
            ray_tunnel_specs=[SSHTunnelSpec(*p) for p in ports]
        )
        self._ssh_tunnels.start_tunnels()
    
    def stop_tunnels(self):
        if self._ssh_tunnels is not None:
            self._ssh_tunnels.stop_tunnels()
        
