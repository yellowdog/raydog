"""
The real platform: ``Platform`` implemented over the YellowDog SDK.

This is the only module in RayDog that imports ``yellowdog_client``.
"""

from __future__ import annotations

import functools
import logging
import re
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from enum import Enum
from types import MappingProxyType
from typing import Any, TypeVar

import requests
from yellowdog_client import PlatformClient
from yellowdog_client import model as yd
from yellowdog_client.model.exceptions import (
    BaseCustomException,
    InvalidRequestException,
)
from yellowdog_client.model.exceptions import (
    ServiceClientException as SdkClientException,
)

from yellowdog_ray.core.platform import (
    NodeInfo,
    PoolInfo,
    PoolSpec,
    PoolStatus,
    PoolSummary,
    TaskGroupInfo,
    TaskGroupSpec,
    TaskGroupStatus,
    TaskInfo,
    TaskSpec,
    TaskStatus,
    WorkRequirementInfo,
    WorkRequirementStatus,
    WorkRequirementSummary,
)
from yellowdog_ray.errors import (
    InvalidRequestError,
    NameInUseError,
    NotFoundError,
    PlatformError,
)

LOG = logging.getLogger(__name__)

DEFAULT_API_URL = "https://api.yellowdog.ai"

TASK_TYPE = "bash"
# The agent passes the task's script to bash as this file
TASK_ARGUMENTS = ["taskdata.txt"]

_NAME_IN_USE = re.compile(r"already exists|must be unique|duplicate names")
# "Invalid transition": the target has already ended, or is already ending
_ALREADY_ENDED = re.compile(r"^Invalid\w*Status\w*Exception$")
_REGISTERED = ("RUNNING", "LATE")
_TASK_ENDED = tuple(s.value for s in TaskStatus if s.is_terminal)
# HTTP status for the SDK's own exception types, which carry none
_CUSTOM_STATUS = {
    "InvalidRequestException": 400,
    "NotAuthorisedException": 403,
    "InternalServerException": 500,
    "ServerErrorException": 500,
}

C = TypeVar("C", bound=type)

# The calls logged at DEBUG: every call to the platform, but not ``connect``, which
# is given credentials
_LOGGED_CALLS = (
    "create_work_requirement",
    "add_task_group",
    "get_work_requirement",
    "find_work_requirement",
    "list_work_requirements",
    "cancel_work_requirement",
    "add_tasks",
    "get_task",
    "list_tasks",
    "cancel_task",
    "provision_pool",
    "get_pool",
    "find_pool",
    "list_pools",
    "set_pool_target",
    "remove_pool",
    "live_instance_count",
    "list_nodes",
    "public_ips",
)


def _shown(value: object) -> str | None:
    """
    How an argument appears in the log: by name or ID, never by contents. A task's
    script and environment, and a pool's userdata, may hold anything.
    """
    if value is None:
        return None
    if isinstance(value, str | int):  # names, IDs, sizes and flags
        text = str(value)
        return text if len(text) <= 80 else text[:77] + "..."
    if isinstance(value, PoolSpec | TaskGroupSpec):
        return value.name
    if isinstance(value, Sequence):
        if value and all(isinstance(v, TaskGroupSpec) for v in value):
            return "[" + ", ".join(v.name for v in value) + "]"
        if all(isinstance(v, TaskSpec) for v in value):
            return f"{len(value)} task(s)"
        return f"{len(value)} item(s)"
    return type(value).__name__


def _logs_calls(cls: C) -> C:
    """Log each platform call at DEBUG: what it was, how long it took, how it ended."""

    def logged(name: str, method: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(method)
        def call(self: object, *args: object, **kwargs: object) -> Any:
            if not LOG.isEnabledFor(logging.DEBUG):
                return method(self, *args, **kwargs)
            shown = [x for x in map(_shown, args) if x is not None]
            shown += [f"{k}={x}" for k, v in kwargs.items() if (x := _shown(v))]
            what = f"{name}({', '.join(shown)})"
            started = time.monotonic()
            try:
                result = method(self, *args, **kwargs)
            except BaseException as e:
                elapsed = time.monotonic() - started
                LOG.debug("%s failed after %.2fs: %s", what, elapsed, type(e).__name__)
                raise
            LOG.debug("%s took %.2fs", what, time.monotonic() - started)
            return result

        return call

    for name in _LOGGED_CALLS:
        setattr(cls, name, logged(name, getattr(cls, name)))
    return cls


@_logs_calls
class YellowDogPlatform:
    def __init__(self, client: PlatformClient) -> None:
        self._client = client
        self._group_lock = threading.Lock()

    @classmethod
    def connect(
        cls, key_id: str, key_secret: str, url: str = DEFAULT_API_URL
    ) -> YellowDogPlatform:
        return cls(
            PlatformClient.create(
                yd.ServicesSchema(defaultUrl=url), yd.ApiKey(key_id, key_secret)
            )
        )

    # --- work requirements -------------------------------------------------

    def create_work_requirement(
        self,
        namespace: str,
        name: str,
        tag: str | None,
        task_groups: Sequence[TaskGroupSpec],
    ) -> WorkRequirementInfo:
        work_requirement = yd.WorkRequirement(
            namespace=namespace,
            name=name,
            tag=tag,
            taskGroups=[_sdk_task_group(g, namespace) for g in task_groups],
        )
        with _errors():
            return _wr_info(
                self._client.work_client.add_work_requirement(work_requirement)
            )

    def add_task_group(
        self, work_requirement_id: str, task_group: TaskGroupSpec
    ) -> TaskGroupInfo:
        # Adding a group means reading the work requirement and writing it back:
        # two at once in this process would lose one, so they take turns. (Across
        # processes RayDog never adds groups to one cluster concurrently: only
        # the head node's monitor adds worker groups.)
        with self._group_lock, _errors():
            work = self._client.work_client
            work_requirement = work.get_work_requirement_by_id(work_requirement_id)
            groups = list(work_requirement.taskGroups or [])
            groups.append(_sdk_task_group(task_group, work_requirement.namespace))
            work_requirement.taskGroups = groups
            updated = work.update_work_requirement(work_requirement)
        for grp in updated.taskGroups or []:
            if grp.name == task_group.name:
                return _group_info(grp)
        raise PlatformError(
            f"task group '{task_group.name}' missing after update of "
            f"{work_requirement_id}"
        )

    def get_work_requirement(self, work_requirement_id: str) -> WorkRequirementInfo:
        with _errors():
            return _wr_info(
                self._client.work_client.get_work_requirement_by_id(work_requirement_id)
            )

    def find_work_requirement(
        self, namespace: str, name: str
    ) -> WorkRequirementInfo | None:
        try:
            with _errors():
                return _wr_info(
                    self._client.work_client.get_work_requirement_by_name(
                        namespace, name
                    )
                )
        except NotFoundError:
            return None

    def list_work_requirements(
        self,
        namespace: str,
        name_contains: str | None = None,
        live_only: bool = False,
    ) -> list[WorkRequirementSummary]:
        statuses = (
            [
                yd.WorkRequirementStatus[s.value]
                for s in WorkRequirementStatus
                if s.is_live
            ]
            if live_only
            else None
        )
        search = yd.WorkRequirementSearch(
            namespace=namespace, name=name_contains, statuses=statuses
        )
        with _errors():
            found = self._client.work_client.get_work_requirements(search).list_all()
        return [
            WorkRequirementSummary(
                id=_required(w.id),
                name=_required(w.name),
                namespace=_required(w.namespace),
                tag=w.tag,
                status=_status(WorkRequirementStatus, w.status),
                created=_required(w.createdTime),
            )
            for w in found
        ]

    def cancel_work_requirement(self, work_requirement_id: str) -> None:
        with _errors(tolerate_ended=True):
            self._client.work_client.cancel_work_requirement_by_id(
                work_requirement_id, abort=True
            )

    # --- tasks -------------------------------------------------------------

    def add_tasks(
        self, task_group_id: str, tasks: Sequence[TaskSpec]
    ) -> list[TaskInfo]:
        sdk_tasks = [
            yd.Task(
                name=t.name,
                taskType=TASK_TYPE,
                taskData=t.script,
                arguments=list(TASK_ARGUMENTS),
                environment=dict(t.environment),
                tag=t.tag,
                timeout=t.timeout,
            )
            for t in tasks
        ]
        with _errors():
            added = self._client.work_client.add_tasks_to_task_group_by_id(
                task_group_id, sdk_tasks
            )
        return [_task_info(t) for t in added]

    def get_task(self, task_id: str) -> TaskInfo:
        with _errors():
            return _task_info(self._client.work_client.get_task_by_id(task_id))

    def list_tasks(
        self,
        work_requirement_id: str,
        task_group_id: str | None = None,
        live_only: bool = False,
    ) -> list[TaskInfo]:
        statuses = (
            [yd.TaskStatus[s.value] for s in TaskStatus if s.is_live]
            if live_only
            else None
        )
        search = yd.TaskSearch(
            workRequirementId=work_requirement_id,
            taskGroupId=task_group_id,
            statuses=statuses,
        )
        with _errors():
            found = self._client.work_client.get_tasks(search).list_all()
        return [_task_info(t) for t in found]

    def cancel_task(self, task_id: str) -> None:
        with _errors(tolerate_ended=True):
            self._client.work_client.cancel_task_by_id(task_id, True)

    # --- worker pools and compute -----------------------------------------

    def provision_pool(self, spec: PoolSpec) -> PoolInfo:
        usage = yd.ComputeRequirementTemplateUsage(
            templateId=spec.template,
            requirementName=spec.name,
            requirementNamespace=spec.namespace,
            requirementTag=spec.tag,
            targetInstanceCount=spec.target_instances,
            imagesId=spec.images_id,
            userData=spec.userdata,
            instanceTags=dict(spec.instance_tags) or None,
        )
        properties = yd.ProvisionedWorkerPoolProperties(
            createNodeWorkers=yd.NodeWorkerTarget.per_node(1),
            minNodes=0,
            maxNodes=spec.max_nodes,
            workerTag=spec.worker_tag,
            metricsEnabled=spec.metrics_enabled,
            idleNodeShutdown=yd.AutoShutdown(
                enabled=True, timeout=spec.idle_node_shutdown
            ),
            idlePoolShutdown=(
                yd.AutoShutdown(enabled=True, timeout=spec.idle_pool_shutdown)
                if spec.idle_pool_shutdown is not None
                else yd.AutoShutdown(enabled=False)
            ),
        )
        with _errors():
            pool = self._client.worker_pool_client.provision_worker_pool(
                usage, properties
            )
        return PoolInfo(
            id=_required(pool.id),
            name=spec.name,
            namespace=spec.namespace,
            status=_status(PoolStatus, pool.status),
            tag=spec.tag,
            instance_tags=MappingProxyType(dict(spec.instance_tags)),
            target_instances=spec.target_instances,
            created=_required(pool.createdTime),
        )

    def get_pool(self, pool_id: str) -> PoolInfo:
        with _errors():
            return self._pool_info(
                self._client.worker_pool_client.get_worker_pool_by_id(pool_id)
            )

    def find_pool(self, namespace: str, name: str) -> PoolInfo | None:
        try:
            with _errors():
                return self._pool_info(
                    self._client.worker_pool_client.get_worker_pool_by_name(
                        namespace, name
                    )
                )
        except NotFoundError:
            return None

    def find_image_family(self, namespace: str, name: str) -> str | None:
        try:
            with _errors():
                family = self._client.images_client.get_image_family_by_name(
                    namespace, name
                )
        except NotFoundError:
            return None
        return str(family.id) if family.id else None

    def list_pools(
        self,
        namespace: str,
        name_contains: str | None = None,
        live_only: bool = False,
    ) -> list[PoolSummary]:
        statuses = (
            [yd.WorkerPoolStatus[s.value] for s in PoolStatus if s.may_hold_instances]
            if live_only
            else None
        )
        search = yd.WorkerPoolSearch(
            namespace=namespace, name=name_contains, statuses=statuses
        )
        with _errors():
            found = self._client.worker_pool_client.get_worker_pools(search).list_all()
        return [
            PoolSummary(
                id=_required(w.id),
                name=_required(w.name),
                namespace=_required(w.namespace),
                status=_status(PoolStatus, w.status),
                created=_required(w.createdTime),
            )
            for w in found
        ]

    def set_pool_target(self, pool_id: str, instances: int) -> None:
        with _errors():
            self._client.worker_pool_client.resize_worker_pool_by_id(pool_id, instances)

    def remove_pool(self, pool_id: str) -> None:
        try:
            with _errors():
                pool = self._client.worker_pool_client.get_worker_pool_by_id(pool_id)
        except NotFoundError:
            return  # purged: nothing left to remove
        # Both steps are always attempted, each on its own. Compute goes first
        # because shutting a pool down does not stop a node that is running a task;
        # repeating either call on something already ended is harmless.
        failures: list[PlatformError] = []
        compute_id = getattr(pool, "computeRequirementId", None)
        if compute_id:
            try:
                with _errors(tolerate_ended=True):
                    self._client.compute_client.terminate_compute_requirement_by_id(
                        compute_id
                    )
            except NotFoundError:
                pass  # the compute requirement has already been purged
            except PlatformError as e:
                failures.append(e)
        try:
            with _errors(tolerate_ended=True):
                self._client.worker_pool_client.shutdown_worker_pool_by_id(pool_id)
        except NotFoundError:
            pass
        except PlatformError as e:
            failures.append(e)
        if failures:
            raise failures[0]

    def live_instance_count(self, pool_id: str) -> int:
        try:
            instances = self._instances(pool_id)
        except NotFoundError:
            return 0  # purged, and its instances with it
        return len([i for i in instances if _name(i.status) != "TERMINATED"])

    # --- nodes -------------------------------------------------------------

    def list_nodes(self, pool_id: str) -> list[NodeInfo]:
        with _errors():
            found = self._client.worker_pool_client.get_nodes(
                yd.NodeSearch(workerPoolId=pool_id)
            ).list_all()
        return [
            NodeInfo(
                id=_required(n.id),
                pool_id=pool_id,
                worker_ids=tuple(_required(w.id) for w in (n.workers or [])),
                private_ip=n.details.privateIpAddress if n.details else None,
                instance_id=n.details.instanceId if n.details else None,
            )
            for n in found
            if n.status is not None and n.status.name in _REGISTERED
        ]

    def public_ips(self, pool_id: str) -> dict[str, str]:
        return {
            i.id.instanceId: i.publicIpAddress
            for i in self._instances(pool_id)
            if i.publicIpAddress
            and getattr(i.id, "instanceId", None)
            and _name(i.status) != "TERMINATED"
        }

    def close(self) -> None:
        self._client.close()

    # --- helpers -----------------------------------------------------------

    def _instances(self, pool_id: str) -> list[Any]:
        with _errors():
            pool = self._client.worker_pool_client.get_worker_pool_by_id(pool_id)
            search = yd.InstanceSearch(
                computeRequirementId=_compute_requirement_id(pool)
            )
            return list(self._client.compute_client.get_instances(search).list_all())

    def _pool_info(self, pool: Any) -> PoolInfo:
        """
        Call inside ``_errors()``. A pool can outlive its compute requirement's
        record, so a missing one leaves the tags and target empty rather than making
        the pool look absent.
        """
        tag: str | None = None
        instance_tags: dict[str, str] = {}
        target = 0
        try:
            with _errors():
                compute = self._client.compute_client.get_compute_requirement_by_id(
                    _compute_requirement_id(pool)
                )
            tag = compute.tag
            instance_tags = _instance_tags(compute)
            target = compute.targetInstanceCount or 0
        except NotFoundError:
            pass
        return PoolInfo(
            id=_required(pool.id),
            name=_required(pool.name),
            namespace=_required(pool.namespace),
            status=_status(PoolStatus, pool.status),
            tag=tag,
            instance_tags=MappingProxyType(instance_tags),
            target_instances=target,
            created=_required(pool.createdTime),
        )


def _compute_requirement_id(pool: Any) -> str:
    compute_id = getattr(pool, "computeRequirementId", None)
    if not compute_id:
        raise PlatformError(
            f"worker pool {getattr(pool, 'id', '?')} is not a provisioned worker pool"
        )
    return str(compute_id)


def _instance_tags(compute: Any) -> dict[str, str]:
    """Instance tags are held per compute source; RayDog sets the same on all."""
    sources = getattr(getattr(compute, "provisionStrategy", None), "sources", None)
    for source in sources or []:
        tags = getattr(source, "instanceTags", None)
        if tags:
            return dict(tags)
    return {}


def _sdk_task_group(spec: TaskGroupSpec, namespace: str | None) -> yd.TaskGroup:
    return yd.TaskGroup(
        name=spec.name,
        tag=spec.tag,
        finishIfAnyTaskFailed=spec.finish_if_any_task_failed,
        finishIfAllTasksFinished=spec.finish_if_all_tasks_finished,
        runSpecification=yd.RunSpecification(
            taskTypes=[TASK_TYPE],
            workerTags=[spec.worker_tag],
            namespaces=[namespace] if namespace else None,
            taskTimeout=spec.task_timeout,
        ),
    )


def _group_info(grp: yd.TaskGroup) -> TaskGroupInfo:
    return TaskGroupInfo(
        id=_required(grp.id),
        name=grp.name,
        tag=grp.tag,
        status=_status(TaskGroupStatus, grp.status),
    )


def _wr_info(work_requirement: yd.WorkRequirement) -> WorkRequirementInfo:
    return WorkRequirementInfo(
        id=_required(work_requirement.id),
        name=work_requirement.name,
        namespace=work_requirement.namespace,
        tag=work_requirement.tag,
        status=_status(WorkRequirementStatus, work_requirement.status),
        task_groups=tuple(_group_info(g) for g in work_requirement.taskGroups or []),
        created=_required(work_requirement.createdTime),
    )


def _task_info(task: yd.Task) -> TaskInfo:
    return TaskInfo(
        id=_required(task.id),
        name=task.name or "",
        status=_status(TaskStatus, task.status),
        tag=task.tag,
        environment=MappingProxyType(dict(task.environment or {})),
        task_group_id=_required(task.taskGroupId),
        worker_id=task.workerId,
        error=_last_error(task),
        stopping=task.abortRequestedTime is not None
        and _name(task.status) not in _TASK_ENDED,
    )


def _last_error(task: yd.Task) -> str | None:
    errors = task.errors or []
    if not errors:
        return None
    last = errors[-1]
    return str(getattr(last, "error", None) or getattr(last, "errorType", None) or last)


E = TypeVar("E", bound=Enum)


def _status(enum: type[E], value: Any) -> E:
    name = _name(value)
    try:
        return enum(name)
    except ValueError:
        raise PlatformError(
            f"the platform returned a {enum.__name__} RayDog does not know: {name!r}"
        ) from None


def _name(status: Any) -> str | None:
    return getattr(status, "name", None)


def _required(value: Any) -> Any:
    if value is None:
        raise PlatformError("the platform returned an object with a missing field")
    return value


@contextmanager
def _errors(tolerate_ended: bool = False) -> Iterator[None]:
    """
    Translate SDK and HTTP errors into RayDog's.

    With ``tolerate_ended``, an "invalid status transition" error is swallowed: the
    thing being cancelled or removed has already ended, which is what was wanted.
    """
    try:
        yield
    except PlatformError:
        raise  # already translated by a nested block
    except InvalidRequestException as e:
        message = str(e)
        cls = NameInUseError if _NAME_IN_USE.search(message) else InvalidRequestError
        raise cls(message, error_type="InvalidRequestException", status_code=400) from e
    except (BaseCustomException, SdkClientException) as e:
        # The SDK turns some error bodies into its own exceptions, without a status
        sdk_type = type(e).__name__
        sdk_status = _CUSTOM_STATUS.get(sdk_type)
        raise PlatformError(
            str(e),
            error_type=sdk_type,
            status_code=sdk_status,
            retryable=sdk_status is not None and sdk_status >= 500,
        ) from e
    except requests.HTTPError as e:
        status, error_type, message = _http_details(e)
        if tolerate_ended and error_type and _ALREADY_ENDED.match(error_type):
            return
        if status == 404:
            raise NotFoundError(message, error_type=error_type, status_code=404) from e
        raise PlatformError(
            message,
            error_type=error_type,
            status_code=status,
            retryable=status == 429 or (status is not None and status >= 500),
        ) from e
    except requests.RequestException as e:
        # Connection failures, timeouts, truncated or undecodable responses
        raise PlatformError(str(e) or type(e).__name__, retryable=True) from e


def _http_details(error: requests.HTTPError) -> tuple[int | None, str | None, str]:
    response = error.response
    if response is None:
        return None, None, str(error)
    error_type: str | None = None
    message = str(error)
    try:
        body = response.json()
        if isinstance(body, dict):
            error_type = body.get("errorType")
            message = body.get("message") or message
    except ValueError:
        pass
    return response.status_code, error_type, message
