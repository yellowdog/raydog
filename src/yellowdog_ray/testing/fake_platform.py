"""
An in-memory YellowDog, for testing RayDog and code built on it offline.

``FakePlatform`` implements ``yellowdog_ray.core.platform.Platform`` as a simulation
driven by a ``FakeClock``: nothing happens until the clock is advanced, and then
instances boot, nodes register, tasks start, aborts complete and idle nodes are
released, in one-second steps. Its rules were taken from probes of the real platform
and are checked against it by the contract tests in ``tests/contract``.

Three of its rules were seen later, in runs made by hand and not by a contract test:
a task that reaches its timeout becomes FAILED (the script is terminated, exit code
143); a task whose instance is lost becomes FAILED ("Task node allocation lost");
and a work requirement stays RUNNING after its head has failed or its lifetime has
passed, until it is cancelled.

Rules that were **not** observed on the real platform, and are assumptions:

- a task's timeout runs from the moment it starts executing;
- a pool with no instances and no tasks waiting shuts itself down after
  ``idle_pool_shutdown``;
- a pool's target is silently capped at its ``max_nodes``, at provisioning and on
  ``set_pool_target``;
- a work requirement stays RUNNING when *every* one of its task groups has ended
  (seen only with the head's group failed);
- a task group with ``finish_if_all_tasks_finished`` completes once it has at least
  one task and all its tasks have completed;
- a task group that fails does so at once (the real platform passes through FAILING
  for about half a minute);
- adding a task group to a work requirement that is not running is rejected.

Timings are attributes (``instance_start``, ``task_start`` and so on) and may be
changed per test.
"""

from __future__ import annotations

import functools
import re
import threading
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Any, NamedTuple, TypeVar, cast

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
from yellowdog_ray.errors import InvalidRequestError, NameInUseError, NotFoundError
from yellowdog_ray.testing.clock import FakeClock

_NAME = re.compile(r"^[a-z]([a-z0-9_-]{0,58}[a-z0-9])?$")
_MAX_TAG = 200
_STEP = timedelta(seconds=1)

F = TypeVar("F", bound=Callable[..., Any])


class Call(NamedTuple):
    method: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


def _api(method: F) -> F:
    """Record the call, raise an injected fault if one is queued, settle, then run."""

    @functools.wraps(method)
    def wrapper(self: FakePlatform, *args: Any, **kwargs: Any) -> Any:
        name = method.__name__
        with self._lock:
            self.calls.append(Call(name, args, kwargs))
            queued = self._faults.get(name)
            if queued:
                raise queued.pop(0)
            self._settle()
            return method(self, *args, **kwargs)

    return cast(F, wrapper)


@dataclass
class _Task:
    id: str
    spec: TaskSpec
    group: _Group
    status: TaskStatus = TaskStatus.READY
    instance: _Instance | None = None
    allocated_at: datetime | None = None
    executing_at: datetime | None = None
    abort_at: datetime | None = None
    error: str | None = None


@dataclass
class _Group:
    id: str
    spec: TaskGroupSpec
    work_requirement: _WorkRequirement
    status: TaskGroupStatus = TaskGroupStatus.RUNNING
    tasks: list[_Task] = field(default_factory=list)


@dataclass
class _WorkRequirement:
    id: str
    name: str
    namespace: str
    tag: str | None
    created: datetime
    status: WorkRequirementStatus = WorkRequirementStatus.RUNNING
    groups: list[_Group] = field(default_factory=list)
    cancelled_at: datetime | None = None


@dataclass
class _Instance:
    id: str
    pool: _Pool
    requested_at: datetime
    private_ip: str
    public_ip: str
    running_at: datetime | None = None
    node_id: str | None = None
    worker_id: str | None = None
    task: _Task | None = None
    idle_since: datetime | None = None
    ending_at: datetime | None = None
    terminated: bool = False

    @property
    def registered(self) -> bool:
        return self.node_id is not None

    @property
    def available(self) -> bool:
        """Alive and not on its way out."""
        return not self.terminated and self.ending_at is None


@dataclass
class _Pool:
    id: str
    spec: PoolSpec
    created: datetime
    target: int
    last_busy: datetime
    instances: list[_Instance] = field(default_factory=list)
    ever_registered: bool = False
    removed_at: datetime | None = None
    terminated: bool = False

    @property
    def removed(self) -> bool:
        return self.removed_at is not None


class FakePlatform:
    """See the module docstring."""

    def __init__(self, clock: FakeClock | None = None) -> None:
        self.clock = clock or FakeClock()
        self.calls: list[Call] = []
        # One lock around every platform call, so that tests may use several
        # threads, as Ray's autoscaler does
        self._lock = threading.RLock()

        # Timings, in seconds
        self.instance_start = 15.0  # requested -> running
        self.node_boot = 60.0  # running -> node registered
        self.task_start = 15.0  # allocated -> executing
        self.abort_delay = 10.0  # cancel of a started task -> ABORTED
        self.cancel_settle = 3.0  # all tasks ended -> work requirement CANCELLED
        self.instance_end = 20.0  # released -> terminated
        self.pool_removal = 30.0  # remove_pool -> instances terminated
        self.pool_pending = 20.0  # new pool reads PENDING for this long

        self._t = self.clock.now()
        self._seq = 0
        self._work_requirements: dict[str, _WorkRequirement] = {}
        self._groups: dict[str, _Group] = {}
        self._tasks: dict[str, _Task] = {}
        self._pools: dict[str, _Pool] = {}
        self._faults: dict[str, list[Exception]] = {}
        self._never_register: list[str] = []
        self._image_families: dict[tuple[str, str], str] = {}

    # ------------------------------------------------------------------
    # Controls and inspection (not part of Platform)
    # ------------------------------------------------------------------

    def add_image_family(self, namespace: str, name: str) -> str:
        """Make an image family exist, as it would in the portal; returns its ID."""
        with self._lock:
            family_id = f"ydid:imgfam:000000:FAKE00:{self._uuid()}"
            self._image_families[(namespace, name)] = family_id
            return family_id

    def fail_next(self, method: str, error: Exception, times: int = 1) -> None:
        """Make the next ``times`` calls of a Platform method raise ``error``."""
        if not callable(getattr(self, method, None)) or method.startswith("_"):
            raise ValueError(f"no such platform method: {method}")
        self._faults.setdefault(method, []).extend([error] * times)

    def node_boot_time(self, seconds: float) -> None:
        self.node_boot = seconds

    def never_register(self, pool_name_contains: str) -> None:
        """Instances of matching pools run but their nodes never register."""
        self._never_register.append(pool_name_contains)

    def fail_task(self, task_id: str) -> None:
        """As if the task's script exited with an error."""
        self._settle()
        self._end_task(self._task(task_id), TaskStatus.FAILED)

    def complete_task(self, task_id: str) -> None:
        """As if the task's script exited normally."""
        self._settle()
        self._end_task(self._task(task_id), TaskStatus.COMPLETED)

    def lose_node(self, task_id: str) -> None:
        """As if the instance running the task were reclaimed."""
        self._settle()
        task = self._task(task_id)
        instance = task.instance
        if instance is None or instance.task is not task:
            raise ValueError(f"task {task_id} is not running on a node")
        self._terminate(instance)
        instance.pool.target = max(0, instance.pool.target - 1)

    def purge_terminal(self) -> None:
        """Forget ended objects, as the platform does after 24 hours."""
        self._settle()
        for wr in [w for w in self._work_requirements.values() if w.status.is_terminal]:
            del self._work_requirements[wr.id]
            for grp in wr.groups:
                del self._groups[grp.id]
                for task in grp.tasks:
                    del self._tasks[task.id]
        for pool in [p for p in self._pools.values() if p.terminated]:
            del self._pools[pool.id]

    @property
    def live_instances(self) -> int:
        self._settle()
        return sum(self._live_count(p) for p in self._pools.values())

    # ------------------------------------------------------------------
    # Platform: work requirements
    # ------------------------------------------------------------------

    @_api
    def create_work_requirement(
        self,
        namespace: str,
        name: str,
        tag: str | None,
        task_groups: Sequence[TaskGroupSpec],
    ) -> WorkRequirementInfo:
        _check_name(name)
        _check_tag(tag)
        if any(
            w.namespace == namespace and w.name == name
            for w in self._work_requirements.values()
        ):
            raise NameInUseError(
                f"A work requirement already exists with the same name '{name}' "
                f"in namespace '{namespace}'",
                error_type="InvalidRequestException",
            )
        names = [g.name for g in task_groups]
        if len(set(names)) != len(names):
            raise NameInUseError(
                "workRequirement must not contain task groups with duplicate names",
                error_type="InvalidRequestException",
            )
        for spec in task_groups:
            _check_name(spec.name)
            _check_tag(spec.tag)
        unique = self._uuid()
        wr = _WorkRequirement(
            id=f"ydid:workreq:000000:{unique}",
            name=name,
            namespace=namespace,
            tag=tag,
            created=self._t,
        )
        self._work_requirements[wr.id] = wr
        for spec in task_groups:
            self._new_group(wr, spec)
        # Observed on the real platform: the response to a create carries the
        # creation time to the microsecond, but it is stored, and returned ever
        # after, to the millisecond. Code must not compare the two.
        info = self._wr_info(wr)
        return replace(info, created=info.created + timedelta(microseconds=577))

    @_api
    def add_task_group(
        self, work_requirement_id: str, task_group: TaskGroupSpec
    ) -> TaskGroupInfo:
        wr = self._work_requirement(work_requirement_id)
        if wr.status is not WorkRequirementStatus.RUNNING:
            raise InvalidRequestError(
                f"Work requirement is {wr.status.value}",
                error_type="InvalidRequestException",
            )
        _check_name(task_group.name)
        _check_tag(task_group.tag)
        if any(g.spec.name == task_group.name for g in wr.groups):
            raise NameInUseError(
                "workRequirement must not contain task groups with duplicate names",
                error_type="InvalidRequestException",
            )
        return _group_info(self._new_group(wr, task_group))

    @_api
    def get_work_requirement(self, work_requirement_id: str) -> WorkRequirementInfo:
        return self._wr_info(self._work_requirement(work_requirement_id))

    @_api
    def find_work_requirement(
        self, namespace: str, name: str
    ) -> WorkRequirementInfo | None:
        for wr in self._work_requirements.values():
            if wr.namespace == namespace and wr.name == name:
                return self._wr_info(wr)
        return None

    @_api
    def list_work_requirements(
        self,
        namespace: str,
        name_contains: str | None = None,
        live_only: bool = False,
    ) -> list[WorkRequirementSummary]:
        return [
            WorkRequirementSummary(
                id=wr.id,
                name=wr.name,
                namespace=wr.namespace,
                tag=wr.tag,
                status=wr.status,
                created=wr.created,
            )
            for wr in self._work_requirements.values()
            if wr.namespace == namespace
            and (name_contains is None or name_contains in wr.name)
            and (not live_only or wr.status.is_live)
        ]

    @_api
    def cancel_work_requirement(self, work_requirement_id: str) -> None:
        wr = self._work_requirement(work_requirement_id)
        if (
            wr.status
            in (
                WorkRequirementStatus.CANCELLING,
                WorkRequirementStatus.CANCELLED,
            )
            or wr.status.is_terminal
        ):
            return
        wr.status = WorkRequirementStatus.CANCELLING
        wr.cancelled_at = self._t
        for grp in wr.groups:
            if grp.status.accepts_tasks:
                grp.status = TaskGroupStatus.CANCELLING
            for task in grp.tasks:
                self._cancel(task)

    # ------------------------------------------------------------------
    # Platform: tasks
    # ------------------------------------------------------------------

    @_api
    def add_tasks(
        self, task_group_id: str, tasks: Sequence[TaskSpec]
    ) -> list[TaskInfo]:
        grp = self._groups.get(task_group_id)
        if grp is None:
            raise NotFoundError(
                f"Task group not found: {task_group_id}", status_code=404
            )
        if not grp.status.accepts_tasks:
            raise InvalidRequestError(
                "Tasks must not be added to finishing or terminal task groups "
                f"(status: {grp.status.value})",
                error_type="InvalidRequestException",
            )
        names = [t.name for t in tasks]
        existing = {t.spec.name for t in grp.tasks}
        if len(set(names)) != len(names) or existing.intersection(names):
            raise NameInUseError(
                "Task names must be unique within task group",
                error_type="InvalidRequestException",
            )
        for spec in tasks:
            _check_name(spec.name)
            _check_tag(spec.tag)
        created = []
        for spec in tasks:
            task = _Task(
                id=f"{grp.id.replace('taskgrp', 'task')}:{len(grp.tasks) + 1}",
                spec=spec,
                group=grp,
            )
            grp.tasks.append(task)
            self._tasks[task.id] = task
            created.append(task)
        self._scale_and_allocate()
        return [_task_info(t) for t in created]

    @_api
    def get_task(self, task_id: str) -> TaskInfo:
        return _task_info(self._task(task_id))

    @_api
    def list_tasks(
        self,
        work_requirement_id: str,
        task_group_id: str | None = None,
        live_only: bool = False,
    ) -> list[TaskInfo]:
        wr = self._work_requirement(work_requirement_id)
        return [
            _task_info(task)
            for grp in wr.groups
            if task_group_id is None or grp.id == task_group_id
            for task in grp.tasks
            if not live_only or task.status.is_live
        ]

    @_api
    def cancel_task(self, task_id: str) -> None:
        self._cancel(self._task(task_id))

    # ------------------------------------------------------------------
    # Platform: pools, compute and nodes
    # ------------------------------------------------------------------

    @_api
    def provision_pool(self, spec: PoolSpec) -> PoolInfo:
        _check_name(spec.name, what="requirementTemplateUsage.requirementName")
        _check_tag(spec.tag)
        if spec.max_nodes < 0 or spec.target_instances < 0:
            raise InvalidRequestError(
                "counts must not be negative", error_type="InvalidRequestException"
            )
        if any(
            p.spec.namespace == spec.namespace and p.spec.name == spec.name
            for p in self._pools.values()
        ):
            raise NameInUseError(
                "Worker pool already exists with the same name in this namespace",
                error_type="InvalidRequestException",
            )
        pool = _Pool(
            id=f"ydid:wrkrpool:000000:{self._uuid()}",
            spec=spec,
            created=self._t,
            target=min(spec.target_instances, spec.max_nodes),
            last_busy=self._t,
        )
        self._pools[pool.id] = pool
        self._launch_to_target(pool)
        self._scale_and_allocate()
        return self._pool_info(pool)

    @_api
    def get_pool(self, pool_id: str) -> PoolInfo:
        return self._pool_info(self._pool(pool_id))

    @_api
    def find_pool(self, namespace: str, name: str) -> PoolInfo | None:
        for pool in self._pools.values():
            if pool.spec.namespace == namespace and pool.spec.name == name:
                return self._pool_info(pool)
        return None

    @_api
    def find_image_family(self, namespace: str, name: str) -> str | None:
        return self._image_families.get((namespace, name))

    @_api
    def list_pools(
        self,
        namespace: str,
        name_contains: str | None = None,
        live_only: bool = False,
    ) -> list[PoolSummary]:
        return [
            PoolSummary(
                id=pool.id,
                name=pool.spec.name,
                namespace=pool.spec.namespace,
                status=self._pool_status(pool),
                created=pool.created,
            )
            for pool in self._pools.values()
            if pool.spec.namespace == namespace
            and (name_contains is None or name_contains in pool.spec.name)
            and (not live_only or self._pool_status(pool).may_hold_instances)
        ]

    @_api
    def set_pool_target(self, pool_id: str, instances: int) -> None:
        pool = self._pool(pool_id)
        if pool.removed:
            raise InvalidRequestError(
                "Worker pool is shut down", error_type="InvalidRequestException"
            )
        pool.target = max(0, min(instances, pool.spec.max_nodes))
        self._launch_to_target(pool)
        surplus = len([i for i in pool.instances if i.available]) - pool.target
        for instance in reversed(pool.instances):
            if surplus <= 0:
                break
            if instance.available and instance.task is None:
                instance.ending_at = self._t + timedelta(seconds=self.instance_end)
                surplus -= 1

    @_api
    def remove_pool(self, pool_id: str) -> None:
        pool = self._pools.get(pool_id)
        if pool is not None:
            self._remove(pool)

    @_api
    def live_instance_count(self, pool_id: str) -> int:
        pool = self._pools.get(pool_id)
        return self._live_count(pool) if pool is not None else 0

    @_api
    def list_nodes(self, pool_id: str) -> list[NodeInfo]:
        pool = self._pool(pool_id)
        return [
            NodeInfo(
                id=cast(str, i.node_id),
                pool_id=pool.id,
                worker_ids=(cast(str, i.worker_id),),
                private_ip=i.private_ip,
                instance_id=i.id,
            )
            for i in pool.instances
            if i.registered and not i.terminated
        ]

    @_api
    def public_ips(self, pool_id: str) -> dict[str, str]:
        pool = self._pool(pool_id)
        return {
            i.id: i.public_ip
            for i in pool.instances
            if i.running_at is not None and not i.terminated
        }

    def close(self) -> None:
        self.calls.append(Call("close", (), {}))

    # ------------------------------------------------------------------
    # Simulation
    # ------------------------------------------------------------------

    def _settle(self) -> None:
        now = self.clock.now()
        while self._t + _STEP <= now:
            self._t += _STEP
            self._step()

    def _step(self) -> None:
        t = self._t
        for pool in self._pools.values():
            for instance in pool.instances:
                if instance.terminated:
                    continue
                if instance.ending_at is not None and t >= instance.ending_at:
                    self._terminate(instance)
                    continue
                if instance.running_at is None:
                    if t >= instance.requested_at + timedelta(
                        seconds=self.instance_start
                    ):
                        instance.running_at = t
                elif (
                    not instance.registered
                    and instance.ending_at is None
                    and not any(s in pool.spec.name for s in self._never_register)
                    and t >= instance.running_at + timedelta(seconds=self.node_boot)
                ):
                    instance.node_id = f"ydid:node:000000:{self._uuid()}"
                    instance.worker_id = instance.node_id.replace("node", "wrkr") + ":1"
                    instance.idle_since = t
                    pool.ever_registered = True
            if pool.removed and not pool.terminated and self._live_count(pool) == 0:
                pool.terminated = True

        for task in self._tasks.values():
            if task.status.is_terminal:
                continue
            if task.abort_at is not None and t >= task.abort_at:
                self._end_task(task, TaskStatus.ABORTED)
            elif task.status is TaskStatus.ALLOCATED and task.allocated_at is not None:
                if t >= task.allocated_at + timedelta(seconds=self.task_start):
                    task.status = TaskStatus.EXECUTING
                    task.executing_at = t
            elif task.status is TaskStatus.EXECUTING and task.executing_at is not None:
                timeout = task.spec.timeout or task.group.spec.task_timeout
                if timeout is not None and t >= task.executing_at + timeout:
                    self._end_task(task, TaskStatus.FAILED)

        self._scale_and_allocate()

        for pool in self._pools.values():
            if pool.removed:
                continue
            waiting = self._waiting_tasks(pool)
            if waiting or self._live_count(pool) > 0:
                pool.last_busy = t
            for instance in pool.instances:
                if (
                    instance.available
                    and instance.registered
                    and instance.task is None
                    and not waiting
                    and instance.idle_since is not None
                    and t >= instance.idle_since + pool.spec.idle_node_shutdown
                ):
                    instance.ending_at = t + timedelta(seconds=self.instance_end)
                    pool.target = max(0, pool.target - 1)
            if (
                pool.spec.idle_pool_shutdown is not None
                and t >= pool.last_busy + pool.spec.idle_pool_shutdown
            ):
                self._remove(pool)

        for wr in self._work_requirements.values():
            for grp in wr.groups:
                all_ended = all(task.status.is_terminal for task in grp.tasks)
                if grp.status is TaskGroupStatus.CANCELLING and all_ended:
                    grp.status = TaskGroupStatus.CANCELLED
                elif (
                    grp.status is TaskGroupStatus.RUNNING
                    and grp.spec.finish_if_all_tasks_finished
                    and grp.tasks
                    and all(task.status is TaskStatus.COMPLETED for task in grp.tasks)
                ):
                    grp.status = TaskGroupStatus.COMPLETED
            if (
                wr.status is WorkRequirementStatus.CANCELLING
                and wr.cancelled_at is not None
                and all(
                    not g.status.accepts_tasks
                    and g.status is not TaskGroupStatus.CANCELLING
                    for g in wr.groups
                )
                and t >= wr.cancelled_at + timedelta(seconds=self.cancel_settle)
            ):
                wr.status = WorkRequirementStatus.CANCELLED

    def _waiting_tasks(self, pool: _Pool) -> list[_Task]:
        """READY tasks this pool could run, oldest first."""
        return [
            task
            for task in self._tasks.values()
            if task.status is TaskStatus.READY
            and task.group.spec.worker_tag == pool.spec.worker_tag
            and task.group.work_requirement.namespace == pool.spec.namespace
            and task.group.status.accepts_tasks
            and task.group.work_requirement.status is WorkRequirementStatus.RUNNING
        ]

    def _scale_and_allocate(self) -> None:
        for pool in self._pools.values():
            if pool.removed:
                continue
            waiting = self._waiting_tasks(pool)
            for instance in pool.instances:
                if not waiting:
                    break
                if instance.available and instance.registered and instance.task is None:
                    task = waiting.pop(0)
                    task.status = TaskStatus.ALLOCATED
                    task.allocated_at = self._t
                    task.instance = instance
                    instance.task = task
                    instance.idle_since = None
            # Demand scaling: tasks still waiting, beyond nodes already on their way
            available = [i for i in pool.instances if i.available]
            arriving = len([i for i in available if not i.registered])
            shortfall = len(waiting) - arriving
            if shortfall > 0:
                wanted = min(len(available) + shortfall, pool.spec.max_nodes)
                if not pool.ever_registered:
                    # A never-populated pool starts one instance to learn its
                    # node properties before it scales further
                    wanted = min(wanted, max(pool.target, 1))
                if wanted > pool.target:
                    pool.target = wanted
            self._launch_to_target(pool)

    def _launch_to_target(self, pool: _Pool) -> None:
        while (
            len([i for i in pool.instances if i.available]) < pool.target
            # instances on their way out still count against the pool's maximum
            and self._live_count(pool) < pool.spec.max_nodes
        ):
            self._seq += 1
            pool.instances.append(
                _Instance(
                    id=f"i-{self._seq:017x}",
                    pool=pool,
                    requested_at=self._t,
                    private_ip=f"10.0.{self._seq // 250}.{self._seq % 250 + 1}",
                    public_ip=f"203.0.{self._seq // 250}.{self._seq % 250 + 1}",
                )
            )

    def _cancel(self, task: _Task) -> None:
        if task.status.is_terminal or task.abort_at is not None:
            return
        if task.instance is None:
            task.status = TaskStatus.CANCELLED
        else:
            task.abort_at = self._t + timedelta(seconds=self.abort_delay)

    def _end_task(self, task: _Task, status: TaskStatus) -> None:
        if task.status.is_terminal:
            return
        task.status = status
        task.abort_at = None
        if status is TaskStatus.FAILED and task.error is None:
            task.error = "task failed"
        instance = task.instance
        if instance is not None and instance.task is task:
            instance.task = None
            instance.idle_since = self._t
        grp = task.group
        if (
            status is TaskStatus.FAILED
            and grp.spec.finish_if_any_task_failed
            and grp.status.accepts_tasks
        ):
            grp.status = TaskGroupStatus.FAILED
            for other in grp.tasks:
                self._cancel(other)

    def _terminate(self, instance: _Instance) -> None:
        instance.terminated = True
        instance.ending_at = None
        task = instance.task
        if task is not None:
            self._end_task(task, TaskStatus.FAILED)
        instance.task = None

    def _remove(self, pool: _Pool) -> None:
        if pool.removed:
            return
        pool.removed_at = self._t
        pool.target = 0
        for instance in pool.instances:
            if not instance.terminated:
                instance.ending_at = self._t + timedelta(seconds=self.pool_removal)

    def _live_count(self, pool: _Pool) -> int:
        return len([i for i in pool.instances if not i.terminated])

    # ------------------------------------------------------------------
    # Lookups and snapshots
    # ------------------------------------------------------------------

    def _uuid(self) -> str:
        self._seq += 1
        return str(uuid.UUID(int=self._seq))

    def _new_group(self, wr: _WorkRequirement, spec: TaskGroupSpec) -> _Group:
        unique = wr.id.rsplit(":", 1)[-1]
        grp = _Group(
            id=f"ydid:taskgrp:000000:{unique}:{len(wr.groups) + 1}",
            spec=spec,
            work_requirement=wr,
        )
        wr.groups.append(grp)
        self._groups[grp.id] = grp
        return grp

    def _work_requirement(self, work_requirement_id: str) -> _WorkRequirement:
        try:
            return self._work_requirements[work_requirement_id]
        except KeyError:
            raise NotFoundError(
                f"Work requirement not found: {work_requirement_id}",
                error_type="WorkRequirementNotFoundException",
                status_code=404,
            ) from None

    def _task(self, task_id: str) -> _Task:
        try:
            return self._tasks[task_id]
        except KeyError:
            raise NotFoundError(
                f"Task not found: {task_id}",
                error_type="TaskNotFoundException",
                status_code=404,
            ) from None

    def _pool(self, pool_id: str) -> _Pool:
        try:
            return self._pools[pool_id]
        except KeyError:
            raise NotFoundError(
                f"Worker pool not found: {pool_id}",
                error_type="WorkerPoolNotFoundException",
                status_code=404,
            ) from None

    def _wr_info(self, wr: _WorkRequirement) -> WorkRequirementInfo:
        return WorkRequirementInfo(
            id=wr.id,
            name=wr.name,
            namespace=wr.namespace,
            tag=wr.tag,
            status=wr.status,
            task_groups=tuple(_group_info(g) for g in wr.groups),
            created=wr.created,
        )

    def _pool_info(self, pool: _Pool) -> PoolInfo:
        return PoolInfo(
            id=pool.id,
            name=pool.spec.name,
            namespace=pool.spec.namespace,
            status=self._pool_status(pool),
            tag=pool.spec.tag,
            instance_tags=MappingProxyType(dict(pool.spec.instance_tags)),
            target_instances=pool.target,
            created=pool.created,
        )

    def _pool_status(self, pool: _Pool) -> PoolStatus:
        if pool.terminated:
            return PoolStatus.TERMINATED
        if pool.removed:
            return PoolStatus.SHUTDOWN
        nodes = [i for i in pool.instances if i.registered and not i.terminated]
        if not nodes:
            young = self._t < pool.created + timedelta(seconds=self.pool_pending)
            return PoolStatus.PENDING if young else PoolStatus.EMPTY
        if any(i.task is not None for i in nodes):
            return PoolStatus.RUNNING
        return PoolStatus.IDLE


def _check_name(name: str, what: str = "name") -> None:
    if not _NAME.match(name):
        raise InvalidRequestError(
            f"Request values are invalid. {what} must not be longer than 60 "
            "characters and only contain lowercase letters, digits, underscore or "
            "hyphen, start with a letter and end with a letter or digit",
            error_type="InvalidRequestException",
        )


def _check_tag(tag: str | None) -> None:
    if tag is not None and len(tag) > _MAX_TAG:
        raise InvalidRequestError(
            f"Request values are invalid. tag size must be between 0 and {_MAX_TAG}",
            error_type="InvalidRequestException",
        )


def _group_info(grp: _Group) -> TaskGroupInfo:
    return TaskGroupInfo(
        id=grp.id, name=grp.spec.name, tag=grp.spec.tag, status=grp.status
    )


def _task_info(task: _Task) -> TaskInfo:
    worker_id = task.instance.worker_id if task.instance is not None else None
    return TaskInfo(
        id=task.id,
        name=task.spec.name,
        status=task.status,
        tag=task.spec.tag,
        environment=MappingProxyType(dict(task.spec.environment)),
        task_group_id=task.group.id,
        worker_id=worker_id,
        error=task.error,
        stopping=task.abort_at is not None and not task.status.is_terminal,
    )
