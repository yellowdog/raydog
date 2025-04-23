"""
Build a Ray cluster using YellowDog.
"""

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
        compute_requirement_template_id: str,
        total_node_count: int,
        cluster_tag: str | None = None,
        images_id: str | None = None,
        head_node_ray_start_script: str = HEAD_NODE_RAY_START_SCRIPT_DEFAULT,
        worker_node_task_script: str = WORKER_NODE_RAY_START_SCRIPT_DEFAULT,
        userdata: str | None = None,
        instance_tags: dict[str, str] | None = None,
        metrics_enabled: bool | None = None,
        cluster_timeout: timedelta | None = None,
        # Set the following to use a separate worker pool for the worker nodes
        worker_node_compute_requirement_template_id: str | None = None,
        worker_node_images_id: str | None = None,
        worker_node_userdata: str | None = None,
    ):

        self._client = client

        auto_shut_down = AutoShutdown(
            enabled=True,
            timeout=timedelta(minutes=IDLE_NODE_AND_POOL_SHUTDOWN_MINUTES),
        )

        self._compute_requirement_template_usage = ComputeRequirementTemplateUsage(
            templateId=compute_requirement_template_id,
            requirementName=cluster_name,
            requirementNamespace=cluster_namespace,
            requirementTag=cluster_tag,
            targetInstanceCount=(
                total_node_count
                if worker_node_compute_requirement_template_id is None
                else 1
            ),
            imagesId=images_id,
            userData=userdata,
            instanceTags=instance_tags,
        )

        self._provisioned_worker_pool_properties = ProvisionedWorkerPoolProperties(
            createNodeWorkers=NodeWorkerTarget.per_node(1),
            minNodes=0,
            maxNodes=(
                total_node_count
                if worker_node_compute_requirement_template_id is None
                else 1
            ),
            workerTag=cluster_name,
            metricsEnabled=metrics_enabled,
            idleNodeShutdown=auto_shut_down,
            idlePoolShutdown=auto_shut_down,
        )

        # Optionally use a separate worker pool for the worker nodes
        self._worker_node_compute_requirement_template_usage = (
            None
            if worker_node_compute_requirement_template_id is None
            else ComputeRequirementTemplateUsage(
                templateId=worker_node_compute_requirement_template_id,
                requirementName=f"{cluster_name}-wrkr",
                requirementNamespace=cluster_namespace,
                requirementTag=cluster_tag,
                targetInstanceCount=total_node_count - 1,
                imagesId=(
                    images_id
                    if worker_node_images_id is None
                    else worker_node_images_id
                ),
                userData=(
                    userdata if worker_node_userdata is None else worker_node_userdata
                ),
                instanceTags=instance_tags,
            )
        )
        self._worker_node_provisioned_worker_pool_properties = (
            None
            if self._worker_node_compute_requirement_template_usage is None
            else ProvisionedWorkerPoolProperties(
                createNodeWorkers=NodeWorkerTarget.per_node(1),
                minNodes=0,
                maxNodes=total_node_count - 1,
                workerTag=f"{cluster_name}-wrkr",
                metricsEnabled=metrics_enabled,
                idleNodeShutdown=auto_shut_down,
                idlePoolShutdown=auto_shut_down,
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

        run_specification = RunSpecification(
            taskTypes=[TASK_TYPE],
            workerTags=[cluster_name],
            namespaces=[cluster_namespace],
            exclusiveWorkers=True,
            taskTimeout=cluster_timeout,
        )

        # Optionally target the separate worker node worker pool
        worker_tasks_run_specification = (
            None
            if self._worker_node_compute_requirement_template_usage is None
            else RunSpecification(
                taskTypes=[TASK_TYPE],
                workerTags=[f"{cluster_name}-wrkr"],
                namespaces=[cluster_namespace],
                exclusiveWorkers=True,
                taskTimeout=cluster_timeout,
            )
        )

        self._work_requirement = WorkRequirement(
            name=cluster_name,
            namespace=cluster_namespace,
            tag=cluster_tag,
            taskGroups=[
                TaskGroup(
                    name=HEAD_NODE_TASK_GROUP_NAME,
                    finishIfAnyTaskFailed=True,
                    runSpecification=run_specification,
                ),
                TaskGroup(
                    name=WORKER_NODES_TASK_GROUP_NAME,
                    finishIfAnyTaskFailed=True,
                    runSpecification=(
                        run_specification
                        if worker_tasks_run_specification is None
                        else worker_tasks_run_specification
                    ),
                ),
            ],
        )

        self._total_node_count = total_node_count

        # Public properties
        self.worker_pool_id: str | None = None
        self.worker_nodes_worker_pool_id: str | None = None
        self.work_requirement_id: str | None = None
        self.head_node_task_id: str | None = None
        self.worker_node_task_ids: list[str] | None = None
        self.head_node_node_id: str | None = None

    def build(self, build_timeout: timedelta | None = None) -> (str, str | None):
        """
        Build the cluster. Returns the private IP and the public IP of the head node or None.

        Raises TimeoutError if the build_timeout is exceeded before the head node task
        starts to execute.
        """

        start_time = datetime.now()

        self.worker_pool_id = self._client.worker_pool_client.provision_worker_pool(
            self._compute_requirement_template_usage,
            self._provisioned_worker_pool_properties,
        ).id

        if (
            self._worker_node_compute_requirement_template_usage is not None
            and self._total_node_count > 1
        ):
            self.worker_nodes_worker_pool_id = (
                self._client.worker_pool_client.provision_worker_pool(
                    self._worker_node_compute_requirement_template_usage,
                    self._worker_node_provisioned_worker_pool_properties,
                ).id
            )

        self._work_requirement = self._client.work_client.add_work_requirement(
            self._work_requirement
        )
        self.work_requirement_id = self._work_requirement.id

        self.head_node_task_id = self._client.work_client.add_tasks_to_task_group_by_id(
            self._work_requirement.taskGroups[0].id,
            [self._head_node_task],
        )[0].id

        while True:  # Check for execution of the head node task
            task = self._client.work_client.get_task_by_id(self.head_node_task_id)
            if task.status == TaskStatus.EXECUTING:
                break
            if (
                build_timeout is not None
                and datetime.now() - start_time >= build_timeout
            ):
                self.shut_down()
                raise TimeoutError(
                    "Timeout waiting for Ray head node task to enter EXECUTING state"
                )
            sleep(HEAD_NODE_TASK_POLLING_INTERVAL_SECONDS)

        self.head_node_node_id = task.workerId.replace("wrkr", "node")[:-2]
        node: Node = self._client.worker_pool_client.get_node_by_id(
            self.head_node_node_id
        )

        # Add one task per worker node
        if self._total_node_count > 1:

            # Update the worker node task with the IP address of the head node
            self._worker_node_task.environment = {
                "RAY_HEAD_NODE_PRIVATE_IP": node.details.privateIpAddress
            }

            self.worker_node_task_ids = [
                task.id
                for task in self._client.work_client.add_tasks_to_task_group_by_id(
                    self._work_requirement.taskGroups[1].id,
                    [self._worker_node_task for _ in range(self._total_node_count - 1)],
                )
            ]

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
        if self.worker_pool_id is not None:
            self._client.worker_pool_client.shutdown_worker_pool_by_id(
                self.worker_pool_id
            )
        if self.worker_nodes_worker_pool_id is not None:
            self._client.worker_pool_client.shutdown_worker_pool_by_id(
                self.worker_nodes_worker_pool_id
            )
