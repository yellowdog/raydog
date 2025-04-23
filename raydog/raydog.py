"""
Build a Ray cluster using YellowDog.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from time import sleep

from yellowdog_client.model import (
    AutoShutdown,
    ComputeRequirementTemplateUsage,
    Node,
    NodeWorkerTarget,
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
        worker_node_task_script: str = WORKER_NODE_RAY_START_SCRIPT_DEFAULT,
        cluster_lifetime: timedelta | None = None,
    ):

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

        self._worker_node_task = Task(
            taskType=TASK_TYPE,
            taskData=worker_node_task_script,
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
        self.head_node_worker_pool_id: str | None = None
        self.worker_node_worker_pool_ids: list[str] = []
        self.work_requirement_id: str | None = None
        self.head_node_task_id: str | None = None
        self.worker_node_task_ids: list[str] | None = None
        self.head_node_node_id: str | None = None

    def add_worker_pool(
        self,
        worker_node_compute_requirement_template_id: str,
        worker_pool_node_count: int,
        worker_node_images_id: str | None = None,
        worker_node_userdata: str | None = None,
        worker_node_instance_tags: dict[str, str] | None = None,
        worker_node_metrics_enabled: bool | None = None,
    ):
        """
        Add the data to create a set of worker nodes.
        """
        worker_pool_index = str(len(self._worker_node_worker_pools) + 1).zfill(2)

        self._worker_node_worker_pools.append(
            WorkerNodeWorkerPool(
                compute_requirement_template_usage=ComputeRequirementTemplateUsage(
                    templateId=worker_node_compute_requirement_template_id,
                    requirementName=f"{self._cluster_name}-{worker_pool_index}",
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
                    workerTag=f"{self._cluster_name}-{worker_pool_index}",
                    metricsEnabled=worker_node_metrics_enabled,
                    idleNodeShutdown=self._auto_shut_down,
                    idlePoolShutdown=self._auto_shut_down,
                ),
                task_group=TaskGroup(
                    name=f"{WORKER_NODES_TASK_GROUP_NAME}-{worker_pool_index}",
                    finishIfAnyTaskFailed=False,
                    runSpecification=RunSpecification(
                        taskTypes=[TASK_TYPE],
                        workerTags=[f"{self._cluster_name}-{worker_pool_index}"],
                        namespaces=[self._cluster_namespace],
                        exclusiveWorkers=True,
                        taskTimeout=self._cluster_lifetime,
                    ),
                ),
            )
        )

    def build(
        self, head_node_build_timeout: timedelta | None = None
    ) -> (str, str | None):
        """
        Build the cluster. Returns the private IP, and the public IP of the head node
        or None.

        Raises TimeoutError if the build_timeout is exceeded before the head node task
        starts to execute.
        """

        start_time = datetime.now()

        # Provision the worker pools
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

        # Add the additional task groups to the work requirement and submit
        self._work_requirement.taskGroups = self._work_requirement.taskGroups + [
            worker_node_worker_pool.task_group
            for worker_node_worker_pool in self._worker_node_worker_pools
        ]
        self._work_requirement = self._client.work_client.add_work_requirement(
            self._work_requirement
        )
        self.work_requirement_id = self._work_requirement.id

        # Add the head node task
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
                and datetime.now() - start_time >= head_node_build_timeout
            ):
                self.shut_down()
                raise TimeoutError(
                    "Timeout waiting for Ray head node task to enter EXECUTING state"
                )
            sleep(HEAD_NODE_TASK_POLLING_INTERVAL_SECONDS)

        # Set the head node ID get the node details
        self.head_node_node_id = task.workerId.replace("wrkr", "node")[:-2]
        node: Node = self._client.worker_pool_client.get_node_by_id(
            self.head_node_node_id
        )

        # Update the worker node task with the IP address of the head node
        self._worker_node_task.environment = {
            "RAY_HEAD_NODE_PRIVATE_IP": node.details.privateIpAddress
        }

        # Add worker node tasks to their task groups, one task per worker node
        for task_group_index, worker_node_worker_pool in enumerate(
            self._worker_node_worker_pools
        ):
            self._client.work_client.add_tasks_to_task_group_by_id(
                self._work_requirement.taskGroups[task_group_index + 1].id,
                [
                    self._worker_node_task
                    for _ in range(
                        worker_node_worker_pool.compute_requirement_template_usage.targetInstanceCount
                    )
                ],
            )

        return node.details.privateIpAddress, node.details.publicIpAddress

    def shut_down(self):
        """
        Shut down the Ray cluster by cancelling the work requirement and
        shutting down the worker pool(s).
        """
        if self.work_requirement_id is not None:
            self._client.work_client.cancel_work_requirement_by_id(
                self.work_requirement_id, abort=True
            )
        if self.head_node_worker_pool_id is not None:
            self._client.worker_pool_client.shutdown_worker_pool_by_id(
                self.head_node_worker_pool_id
            )
        for worker_pool_id in self.worker_node_worker_pool_ids:
            self._client.worker_pool_client.shutdown_worker_pool_by_id(worker_pool_id)


@dataclass
class WorkerNodeWorkerPool:
    compute_requirement_template_usage: ComputeRequirementTemplateUsage
    provisioned_worker_pool_properties: ProvisionedWorkerPoolProperties
    task_group: TaskGroup
