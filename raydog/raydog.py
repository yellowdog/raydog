"""
Build a Ray cluster using YellowDog.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from time import sleep

from requests.exceptions import HTTPError
from yellowdog_client.model import (
    AutoShutdown,
    ComputeRequirementTemplateUsage,
    Node,
    NodeWorkerTarget,
    ProvisionedWorkerPool,
    ProvisionedWorkerPoolProperties,
    RunSpecification,
    Task,
    TaskGroup,
    TaskStatus,
    WorkRequirement,
)
from yellowdog_client.platform_client import PlatformClient

HEAD_NODE_RAY_START_SCRIPT_DEFAULT = r"""#!/usr/bin/bash
trap "ray stop; echo Ray stopped" EXIT
set -euo pipefail
VENV=/opt/yellowdog/agent/venv
source $VENV/bin/activate
ray start --disable-usage-stats --head --port=6379 --block
"""

WORKER_NODE_RAY_START_SCRIPT_DEFAULT = r"""#!/usr/bin/bash
trap "ray stop; echo Ray stopped" EXIT
set -euo pipefail
VENV=/opt/yellowdog/agent/venv
source $VENV/bin/activate
ray start --disable-usage-stats --address=$RAY_HEAD_NODE_PRIVATE_IP:6379 --block
"""

HEAD_NODE_TASK_GROUP_NAME = "head-node"
WORKER_NODES_TASK_GROUP_NAME = "worker-nodes"

TASK_TYPE = "bash"

HEAD_NODE_TASK_POLLING_INTERVAL_SECONDS = 10.0
IDLE_NODE_AND_POOL_SHUTDOWN_MINUTES = 3.0


@dataclass
class WorkerNodeWorkerPool:
    compute_requirement_template_usage: ComputeRequirementTemplateUsage
    provisioned_worker_pool_properties: ProvisionedWorkerPoolProperties
    task_group: TaskGroup
    task_prototype: Task


class RayDogCluster:
    """
    A class representing a Ray cluster managed by YellowDog.
    """

    def __init__(
        self,
        client: PlatformClient,
        cluster_name: str,
        cluster_namespace: str,
        head_node_compute_requirement_template_id: str,
        cluster_tag: str | None = None,
        head_node_images_id: str | None = None,
        head_node_userdata: str | None = None,
        head_node_instance_tags: dict[str, str] | None = None,
        head_node_metrics_enabled: bool | None = None,
        head_node_ray_start_script: str = HEAD_NODE_RAY_START_SCRIPT_DEFAULT,
        cluster_lifetime: timedelta | None = None,
    ):
        """
        Initialise the properties of the RayDog cluster and the Ray head node.

        :param client: a YellowDog PlatformClient object for connecting to the
            YellowDog platform.
        :param cluster_name: a name for the cluster; the name must be unique to the
            YellowDog account and is used as the basis for the work requirement
            and worker pool names, and the worker tags.
        :param cluster_namespace: the YellowDog namespace to use for the cluster.
        :param head_node_compute_requirement_template_id: the YellowDog
            compute requirement template ID for the head node.
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
        :param head_node_ray_start_script: the Bash script for starting the ray head
            node.
        :param cluster_lifetime: an optional timeout that will shut down the Ray
            cluster if it expires.
        """

        self._client = client
        self._cluster_name = cluster_name
        self._cluster_namespace = cluster_namespace
        self._cluster_tag = cluster_tag
        self._cluster_lifetime = cluster_lifetime

        head_node_naming = f"{cluster_name}-00"

        self._auto_shut_down = AutoShutdown(
            enabled=True,
            timeout=timedelta(minutes=IDLE_NODE_AND_POOL_SHUTDOWN_MINUTES),
        )

        self._head_node_compute_requirement_template_usage = (
            ComputeRequirementTemplateUsage(
                templateId=head_node_compute_requirement_template_id,
                requirementName=head_node_naming,
                requirementNamespace=cluster_namespace,
                requirementTag=cluster_tag,
                targetInstanceCount=1,
                imagesId=head_node_images_id,
                userData=head_node_userdata,
                instanceTags=head_node_instance_tags,
            )
        )

        self._head_node_provisioned_worker_pool_properties = (
            ProvisionedWorkerPoolProperties(
                createNodeWorkers=NodeWorkerTarget.per_node(1),
                minNodes=0,
                maxNodes=1,
                workerTag=head_node_naming,
                metricsEnabled=head_node_metrics_enabled,
                idleNodeShutdown=self._auto_shut_down,
                idlePoolShutdown=self._auto_shut_down,
            )
        )

        self._head_node_task = Task(
            taskType=TASK_TYPE,
            taskData=head_node_ray_start_script,
            arguments=["taskdata.txt"],
        )

        self._work_requirement = WorkRequirement(
            name=cluster_name,
            namespace=cluster_namespace,
            tag=cluster_tag,
            taskGroups=[
                TaskGroup(
                    name=HEAD_NODE_TASK_GROUP_NAME,
                    finishIfAnyTaskFailed=True,
                    runSpecification=RunSpecification(
                        taskTypes=[TASK_TYPE],
                        workerTags=[head_node_naming],
                        namespaces=[cluster_namespace],
                        exclusiveWorkers=True,
                        taskTimeout=cluster_lifetime,
                    ),
                ),
            ],
        )

        self._worker_node_worker_pools: list[WorkerNodeWorkerPool] = []

        # Public properties
        self.work_requirement_id: str | None = None
        self.head_node_worker_pool_id: str | None = None
        self.head_node_node_id: str | None = None
        self.head_node_private_ip: str | None = None
        self.head_node_public_ip: str | None = None
        self.head_node_task_id: str | None = None
        self.worker_node_worker_pool_ids: list[str] = []

    def add_worker_pool(
        self,
        worker_node_compute_requirement_template_id: str,
        worker_pool_node_count: int,
        worker_node_images_id: str | None = None,
        worker_node_userdata: str | None = None,
        worker_node_instance_tags: dict[str, str] | None = None,
        worker_node_metrics_enabled: bool | None = None,
        worker_node_task_script: str = WORKER_NODE_RAY_START_SCRIPT_DEFAULT,
    ) -> str | None:
        """
        Add a worker pool and task group that will provide Ray worker nodes.

        :param worker_node_compute_requirement_template_id: the YellowDog compute
            requirement template ID to use for the worker nodes in this worker
            pool.
        :param worker_pool_node_count: the number of ray worker nodes to create in
            this worker pool. Must be > 0.
        :param worker_node_images_id: the images ID to use with the compute
            requirement template, if required.
        :param worker_node_userdata: optional userdata for use when the worker node
            instances are provisioned.
        :param worker_node_instance_tags: optional instance tags to apply to the
            worker node instances.
        :param worker_node_metrics_enabled: whether to enable metrics collection for the
            worker nodes.
        :param worker_node_task_script: the Bash script for starting the ray worker
            nodes in this worker pool.
        :return: returns the worker pool ID if a worker pool was created, or None if the
            pool will be created later using the build() method.
        """

        if worker_pool_node_count < 1:
            raise ValueError("worker_pool_node_count must be > 0")

        worker_pool_index_str = str(len(self._worker_node_worker_pools) + 1).zfill(2)

        worker_node_worker_pool = WorkerNodeWorkerPool(
            compute_requirement_template_usage=ComputeRequirementTemplateUsage(
                templateId=worker_node_compute_requirement_template_id,
                requirementName=f"{self._cluster_name}-{worker_pool_index_str}",
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
                workerTag=f"{self._cluster_name}-{worker_pool_index_str}",
                metricsEnabled=worker_node_metrics_enabled,
                idleNodeShutdown=self._auto_shut_down,
                idlePoolShutdown=self._auto_shut_down,
            ),
            task_group=TaskGroup(
                name=f"{WORKER_NODES_TASK_GROUP_NAME}-{worker_pool_index_str}",
                finishIfAnyTaskFailed=False,
                runSpecification=RunSpecification(
                    taskTypes=[TASK_TYPE],
                    workerTags=[f"{self._cluster_name}-{worker_pool_index_str}"],
                    namespaces=[self._cluster_namespace],
                    exclusiveWorkers=True,
                    taskTimeout=self._cluster_lifetime,
                ),
            ),
            task_prototype=Task(
                taskType=TASK_TYPE,
                taskData=worker_node_task_script,
                arguments=["taskdata.txt"],
                environment={},
            ),
        )

        self._worker_node_worker_pools.append(worker_node_worker_pool)

        if self.head_node_private_ip is None:
            return None  # No head node set up yet; wait for build()

        # Provision the new worker pool
        worker_pool_id = self._client.worker_pool_client.provision_worker_pool(
            worker_node_worker_pool.compute_requirement_template_usage,
            worker_node_worker_pool.provisioned_worker_pool_properties,
        ).id
        self.worker_node_worker_pool_ids.append(worker_pool_id)

        # Add the new task group
        work_requirement = self._client.work_client.get_work_requirement_by_id(
            self.work_requirement_id
        )
        work_requirement.taskGroups.append(worker_node_worker_pool.task_group)
        work_requirement = self._client.work_client.update_work_requirement(
            work_requirement
        )

        # Add the worker node tasks to the task group
        self._add_tasks_to_task_group(
            task_group_id=work_requirement.taskGroups[
                len(work_requirement.taskGroups) - 1
            ].id,
            worker_node_worker_pool=worker_node_worker_pool,
        )

        return worker_pool_id

    def build(
        self, head_node_build_timeout: timedelta | None = None
    ) -> (str, str | None):
        """
        Build the cluster. This method will block until the Ray head node
        is ready, but note that Ray worker nodes will still be configuring
        and joining the cluster.

        :param head_node_build_timeout: an optional timeout for building the head node;
            if the timeout expires before the head node task is executing, a TimeoutError
            exception will be raised.
        :return: a tuple containing the private IP address of the head node, and the
            public IP address of the head node (or None).
        """

        start_time = datetime.now(timezone.utc)

        # Provision all currently defined worker pools
        self.head_node_worker_pool_id = (
            self._client.worker_pool_client.provision_worker_pool(
                self._head_node_compute_requirement_template_usage,
                self._head_node_provisioned_worker_pool_properties,
            ).id
        )
        for worker_node_worker_pool in self._worker_node_worker_pools:
            self.worker_node_worker_pool_ids.append(
                self._client.worker_pool_client.provision_worker_pool(
                    worker_node_worker_pool.compute_requirement_template_usage,
                    worker_node_worker_pool.provisioned_worker_pool_properties,
                ).id
            )

        # Add currently defined worker node task groups to the work requirement,
        # and submit it
        self._work_requirement.taskGroups += [
            worker_node_worker_pool.task_group
            for worker_node_worker_pool in self._worker_node_worker_pools
        ]
        self._work_requirement = self._client.work_client.add_work_requirement(
            self._work_requirement
        )
        self.work_requirement_id = self._work_requirement.id

        # Add the head node task to the first task group
        self.head_node_task_id = self._client.work_client.add_tasks_to_task_group_by_id(
            self._work_requirement.taskGroups[0].id,
            [self._head_node_task],
        )[0].id

        while True:  # Check for execution of the head node task
            task = self._client.work_client.get_task_by_id(self.head_node_task_id)
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
            sleep(HEAD_NODE_TASK_POLLING_INTERVAL_SECONDS)

        # Set the head node ID and get the node details
        self.head_node_node_id = task.workerId.replace("wrkr", "node")[:-2]
        node: Node = self._client.worker_pool_client.get_node_by_id(
            self.head_node_node_id
        )
        self.head_node_private_ip = node.details.privateIpAddress
        self.head_node_public_ip = node.details.publicIpAddress

        # Add worker node tasks to their task groups, one task per worker node
        for task_group_index, worker_node_worker_pool in enumerate(
            self._worker_node_worker_pools
        ):
            self._add_tasks_to_task_group(
                task_group_id=self._work_requirement.taskGroups[
                    task_group_index + 1
                ].id,
                worker_node_worker_pool=worker_node_worker_pool,
            )

        return self.head_node_private_ip, self.head_node_public_ip

    def remove_worker_pool(self, worker_pool_id):
        """
        Terminate the compute requirement associated with a worker pool.

        :param worker_pool_id: the ID of the worker pool to remove.
        """

        worker_pool: ProvisionedWorkerPool = (
            self._client.worker_pool_client.get_worker_pool_by_id(worker_pool_id)
        )
        self._client.compute_client.terminate_compute_requirement_by_id(
            worker_pool.computeRequirementId
        )
        self.worker_node_worker_pool_ids.remove(worker_pool_id)

    def shut_down(self):
        """
        Shut down the Ray cluster by cancelling the work requirement, including
        aborting all its tasks, and shutting down all the worker pools.
        """

        if self.work_requirement_id is not None:
            try:
                self._client.work_client.cancel_work_requirement_by_id(
                    self.work_requirement_id, abort=True
                )
            except HTTPError as e:
                if "InvalidWorkRequirementStatusException" in str(e):
                    pass  # Suppress exception if it's just a state transition error
            self.work_requirement_id = None

        if self.head_node_worker_pool_id is not None:
            self._client.worker_pool_client.shutdown_worker_pool_by_id(
                self.head_node_worker_pool_id
            )
            self.head_node_worker_pool_id = None

        for worker_pool_id in self.worker_node_worker_pool_ids:
            self._client.worker_pool_client.shutdown_worker_pool_by_id(worker_pool_id)
        self.worker_node_worker_pool_ids = []

    def _add_tasks_to_task_group(
        self, task_group_id: str, worker_node_worker_pool: WorkerNodeWorkerPool
    ):
        """
        Internal utility to add worker pool tasks to the applicable task group

        :param task_group_id: the ID of the task group.
        :param worker_node_worker_pool: the properties of the worker nodes worker pool.
        """

        worker_node_worker_pool.task_prototype.environment.update(
            {"RAY_HEAD_NODE_PRIVATE_IP": self.head_node_private_ip}
        )
        self._client.work_client.add_tasks_to_task_group_by_id(
            task_group_id,
            [
                worker_node_worker_pool.task_prototype
                for _ in range(
                    worker_node_worker_pool.compute_requirement_template_usage.targetInstanceCount
                )
            ],
        )
