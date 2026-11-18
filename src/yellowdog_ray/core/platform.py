"""
Everything RayDog asks of the YellowDog platform, and nothing else.

``Platform`` is implemented over the YellowDog SDK by
``yellowdog_ray.core.yellowdog.YellowDogPlatform`` and in memory by
``yellowdog_ray.testing.FakePlatform``. The records here belong to RayDog, so that
changes to the SDK's models stay inside the adapter.

Conventions:

- ``get_*`` raise ``NotFoundError``; ``find_*`` match a name exactly and return
  ``None`` when there is no such object. Both return objects that have ended.
- ``list_*`` with ``name_contains`` is a substring match, as the platform's search is,
  and returns summaries; use ``get_*`` for the full record.
- ``cancel_*`` and ``remove_pool`` do not raise if the target has already ended,
  and ``remove_pool`` and ``live_instance_count`` accept a pool that no longer
  exists (nothing to remove; no instances).
- Cancelling always aborts tasks that are executing.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Protocol


class WorkRequirementStatus(str, Enum):
    RUNNING = "RUNNING"
    HELD = "HELD"
    FINISHING = "FINISHING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"

    @property
    def is_live(self) -> bool:
        """Able to run tasks, now or once released."""
        return self in (WorkRequirementStatus.RUNNING, WorkRequirementStatus.HELD)

    @property
    def is_terminal(self) -> bool:
        return self in (
            WorkRequirementStatus.COMPLETED,
            WorkRequirementStatus.FAILED,
            WorkRequirementStatus.CANCELLED,
        )


class TaskGroupStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    HELD = "HELD"
    FINISHING = "FINISHING"
    COMPLETED = "COMPLETED"
    FAILING = "FAILING"
    FAILED = "FAILED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"

    @property
    def accepts_tasks(self) -> bool:
        return self in (
            TaskGroupStatus.PENDING,
            TaskGroupStatus.RUNNING,
            TaskGroupStatus.HELD,
        )


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    READY = "READY"
    ALLOCATED = "ALLOCATED"
    DOWNLOADING = "DOWNLOADING"
    EXECUTING = "EXECUTING"
    UPLOADING = "UPLOADING"
    RESUBMITTED = "RESUBMITTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"
    CANCELLED = "CANCELLED"
    DISCARDED = "DISCARDED"

    @property
    def is_terminal(self) -> bool:
        return self in (
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.ABORTED,
            TaskStatus.CANCELLED,
            TaskStatus.DISCARDED,
        )

    @property
    def is_live(self) -> bool:
        return not self.is_terminal


class PoolStatus(str, Enum):
    PENDING = "PENDING"
    CONFIGURING = "CONFIGURING"
    EMPTY = "EMPTY"
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    SHUTDOWN = "SHUTDOWN"
    TERMINATED = "TERMINATED"

    @property
    def is_live(self) -> bool:
        """Able to supply nodes. A pool in SHUTDOWN may still hold instances."""
        return self not in (PoolStatus.SHUTDOWN, PoolStatus.TERMINATED)

    @property
    def may_hold_instances(self) -> bool:
        return self is not PoolStatus.TERMINATED


@dataclass(frozen=True)
class TaskGroupSpec:
    name: str
    worker_tag: str
    tag: str | None = None
    task_timeout: timedelta | None = None
    finish_if_any_task_failed: bool = False
    finish_if_all_tasks_finished: bool = False


@dataclass(frozen=True)
class TaskGroupInfo:
    id: str
    name: str
    tag: str | None
    status: TaskGroupStatus


@dataclass(frozen=True)
class WorkRequirementInfo:
    id: str
    name: str
    namespace: str
    tag: str | None
    status: WorkRequirementStatus
    task_groups: tuple[TaskGroupInfo, ...]
    created: datetime

    def task_group(self, name: str) -> TaskGroupInfo | None:
        return next((g for g in self.task_groups if g.name == name), None)


@dataclass(frozen=True)
class WorkRequirementSummary:
    """What a listing returns: no task groups."""

    id: str
    name: str
    namespace: str
    tag: str | None
    status: WorkRequirementStatus
    created: datetime


@dataclass(frozen=True)
class TaskSpec:
    """A node's task: a bash script run by the YellowDog agent."""

    name: str
    script: str
    environment: Mapping[str, str] = field(default_factory=dict)
    tag: str | None = None
    timeout: timedelta | None = None


@dataclass(frozen=True)
class TaskInfo:
    id: str
    name: str
    status: TaskStatus
    tag: str | None
    environment: Mapping[str, str]
    task_group_id: str
    worker_id: str | None
    error: str | None = None  # the most recent error, if the task has failed
    # Cancelled but still running: the abort is asynchronous. Such a task is on its
    # way out and must not be counted as a node.
    stopping: bool = False


@dataclass(frozen=True)
class PoolSpec:
    name: str
    namespace: str
    template: str
    worker_tag: str
    target_instances: int
    max_nodes: int
    images_id: str | None = None
    userdata: str | None = None
    instance_tags: Mapping[str, str] = field(default_factory=dict)
    tag: str | None = None
    metrics_enabled: bool = False
    idle_node_shutdown: timedelta = timedelta(minutes=3)
    idle_pool_shutdown: timedelta | None = timedelta(minutes=3)  # None: never


@dataclass(frozen=True)
class PoolInfo:
    id: str
    name: str
    namespace: str
    status: PoolStatus
    tag: str | None
    instance_tags: Mapping[str, str]
    target_instances: int
    created: datetime


@dataclass(frozen=True)
class PoolSummary:
    """What a listing returns: no tags or target."""

    id: str
    name: str
    namespace: str
    status: PoolStatus
    created: datetime


@dataclass(frozen=True)
class NodeInfo:
    """A registered node. The public IP is not here: see ``Platform.public_ips``."""

    id: str
    pool_id: str
    worker_ids: tuple[str, ...]
    private_ip: str | None
    instance_id: str | None


class Platform(Protocol):
    # --- work requirements -------------------------------------------------
    def create_work_requirement(
        self,
        namespace: str,
        name: str,
        tag: str | None,
        task_groups: Sequence[TaskGroupSpec],
    ) -> WorkRequirementInfo: ...

    def add_task_group(
        self, work_requirement_id: str, task_group: TaskGroupSpec
    ) -> TaskGroupInfo: ...

    def get_work_requirement(self, work_requirement_id: str) -> WorkRequirementInfo: ...

    def find_work_requirement(
        self, namespace: str, name: str
    ) -> WorkRequirementInfo | None: ...

    def list_work_requirements(
        self,
        namespace: str,
        name_contains: str | None = None,
        live_only: bool = False,
    ) -> list[WorkRequirementSummary]: ...

    def cancel_work_requirement(self, work_requirement_id: str) -> None: ...

    # --- tasks -------------------------------------------------------------
    def add_tasks(
        self, task_group_id: str, tasks: Sequence[TaskSpec]
    ) -> list[TaskInfo]: ...

    def get_task(self, task_id: str) -> TaskInfo: ...

    def list_tasks(
        self,
        work_requirement_id: str,
        task_group_id: str | None = None,
        live_only: bool = False,
    ) -> list[TaskInfo]: ...

    def cancel_task(self, task_id: str) -> None: ...

    # --- worker pools and compute -----------------------------------------
    def provision_pool(self, spec: PoolSpec) -> PoolInfo: ...

    def get_pool(self, pool_id: str) -> PoolInfo: ...

    def find_pool(self, namespace: str, name: str) -> PoolInfo | None: ...

    def find_image_family(self, namespace: str, name: str) -> str | None:
        """The ID of the image family of exactly this name, or None."""
        ...

    def list_pools(
        self,
        namespace: str,
        name_contains: str | None = None,
        live_only: bool = False,
    ) -> list[PoolSummary]:
        """With ``live_only``, every pool that may still hold instances."""
        ...

    def set_pool_target(self, pool_id: str, instances: int) -> None:
        """Ask for this many instances. Pools otherwise grow one step at a time."""
        ...

    def remove_pool(self, pool_id: str) -> None:
        """Terminate the pool's compute and shut the pool down."""
        ...

    def live_instance_count(self, pool_id: str) -> int:
        """Instances not yet terminated: the only evidence that a pool has gone."""
        ...

    # --- nodes -------------------------------------------------------------
    def list_nodes(self, pool_id: str) -> list[NodeInfo]: ...

    def public_ips(self, pool_id: str) -> dict[str, str]:
        """Instance ID to public IP, for instances that have one."""
        ...

    def close(self) -> None: ...
