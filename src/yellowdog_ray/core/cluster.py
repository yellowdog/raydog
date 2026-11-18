"""
The cluster core: what the Builder and the Autoscaler have in common.

A Ray cluster is one YellowDog work requirement. Each *role* in it (the head, an
optional observability node, each kind of worker) is a task group fed by its own
worker pool, and each node is a task. ``ClusterCore`` creates, finds, scales,
inspects and tears down such clusters through the ``Platform`` interface.

The core keeps no state of its own. Every operation takes a ``ClusterHandle`` and
asks the platform for whatever else it needs, so a cluster can be driven from any
process that can name it. The cluster's settings travel in the tag of its head task
group, which exists from the moment the cluster is created.
"""

from __future__ import annotations

import json
import logging
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Literal

from yellowdog_ray.core import naming
from yellowdog_ray.core.clock import Clock, SystemClock
from yellowdog_ray.core.platform import (
    NodeInfo,
    Platform,
    PoolInfo,
    PoolSpec,
    PoolStatus,
    TaskGroupInfo,
    TaskGroupSpec,
    TaskInfo,
    TaskSpec,
    TaskStatus,
    WorkRequirementInfo,
    WorkRequirementStatus,
)
from yellowdog_ray.core.wait import poll
from yellowdog_ray.errors import (
    BuildTimeoutError,
    ClusterExistsError,
    ClusterNotFoundError,
    ClusterStateError,
    ConfigError,
    NameInUseError,
    NodeStartError,
    NotFoundError,
    PlatformError,
    TeardownError,
)

LOG = logging.getLogger(__name__)

# An image family by name: one "/", with nothing before it that a cloud's own image
# path would have (GCP's have several "/", Azure's begin with one)
_IMAGE_FAMILY_NAME = re.compile(r"([^/\s:]+)/([^/\s]+)")

# Environment variables every node's task is given
ENV_CLUSTER = "RAYDOG_CLUSTER"
ENV_ROLE = "RAYDOG_ROLE"
ENV_NODE_TAGS = "RAYDOG_NODE_TAGS"

NODE_TASK_TAG_PREFIX = "raydog:node:"

# Keys of the settings carried in the head task group's marker, in seconds
_LIFETIME, _IDLE, _BUILD = "l", "i", "b"
# ... and in a worker task group's marker: the pool's maximum size
_MAX = "m"

_SUFFIX_ATTEMPTS = 5
_POLL_INTERVAL = timedelta(seconds=5)
# How long past its lifetime a cluster may still be winding down
_DEADLINE_GRACE = timedelta(minutes=10)


@dataclass(frozen=True)
class NodeSpec:
    """What a node is made from: where it runs and what it runs."""

    compute_requirement_template: str
    start_script: str
    images_id: str | None = None
    userdata: str | None = None
    instance_tags: Mapping[str, str] = field(default_factory=dict)
    metrics_enabled: bool = False


@dataclass(frozen=True)
class ClusterSettings:
    name: str
    namespace: str
    tag: str | None = None
    # How long the cluster may live, from its creation. None: no limit.
    lifetime: timedelta | None = timedelta(hours=24)
    # How long a node with no task is kept
    idle_shutdown: timedelta = timedelta(minutes=3)
    # How long a node is given to start
    build_timeout: timedelta = timedelta(minutes=10)


@dataclass(frozen=True)
class ClusterHandle:
    """Identifies one cluster. Cheap to rebuild: see ``ClusterCore.attach``."""

    settings: ClusterSettings
    work_requirement_id: str
    work_requirement_name: str
    created: datetime

    @property
    def deadline(self) -> datetime | None:
        """When every node's task times out; None if the cluster has no limit."""
        if self.settings.lifetime is None:
            return None
        return self.created + self.settings.lifetime


@dataclass(frozen=True)
class NodeAddress:
    task_id: str
    private_ip: str
    public_ip: str | None


@dataclass(frozen=True)
class NodeRecord:
    task_id: str
    role: str
    status: TaskStatus
    tags: Mapping[str, str]
    worker_id: str | None
    error: str | None
    stopping: bool = False  # cancelled, and about to end


@dataclass(frozen=True)
class PoolState:
    role: str
    pool_id: str | None  # None: the role has no pool (yet)
    status: PoolStatus | None
    live_instances: int
    nodes_by_status: Mapping[TaskStatus, int]  # live nodes only
    max_nodes: int | None = None  # as given to add_pool; None if not recorded


@dataclass(frozen=True)
class ClusterStatus:
    handle: ClusterHandle
    work_requirement_status: WorkRequirementStatus
    head: NodeRecord | None  # the live head, or failing that the most recent one
    head_alive: bool
    pools: Mapping[str, PoolState]  # by role
    past_deadline: bool


@dataclass(frozen=True)
class ClusterSummary:
    name: str
    work_requirement_id: str
    work_requirement_name: str
    status: WorkRequirementStatus
    created: datetime


@dataclass(frozen=True)
class Orphan:
    """Something RayDog created that no live, healthy cluster accounts for."""

    kind: Literal["pool", "work_requirement"]
    id: str
    name: str
    cluster: str
    reason: str


class ClusterCore:
    def __init__(self, platform: Platform, clock: Clock | None = None) -> None:
        self._platform = platform
        self._clock: Clock = clock or SystemClock()

    # ------------------------------------------------------------------
    # Creating and finding clusters
    # ------------------------------------------------------------------

    def create(self, settings: ClusterSettings) -> ClusterHandle:
        """
        Create an empty cluster: a work requirement with its head task group.

        Nothing that costs money exists until a pool is added.
        """
        naming.validate_cluster_name(settings.name)
        validate_settings(settings)
        if self._live_clusters(settings.name, settings.namespace):
            raise ClusterExistsError(
                f"a cluster named '{settings.name}' is already running in namespace "
                f"'{settings.namespace}'"
            )
        marker_settings = {
            _IDLE: int(settings.idle_shutdown.total_seconds()),
            _BUILD: int(settings.build_timeout.total_seconds()),
        }
        if settings.lifetime is not None:
            marker_settings[_LIFETIME] = int(settings.lifetime.total_seconds())
        head = TaskGroupSpec(
            name=naming.HEAD_ROLE,
            worker_tag="",  # filled in per attempt below
            tag=naming.group_marker(settings.name, naming.HEAD_ROLE, marker_settings),
            task_timeout=settings.lifetime,
            finish_if_any_task_failed=True,
        )
        created: WorkRequirementInfo | None = None
        for _ in range(_SUFFIX_ATTEMPTS):
            name = naming.work_requirement_name(settings.name, naming.new_suffix())
            worker_tag = naming.pool_name(name, naming.HEAD_ROLE)
            group = _with_worker_tag(head, worker_tag)
            try:
                created = self._platform.create_work_requirement(
                    settings.namespace, name, settings.tag, [group]
                )
                break
            except NameInUseError:
                continue  # an ended cluster used this suffix; try another
            except BaseException:
                # The request may have been carried out although we never heard:
                # do not leave a work requirement that nobody holds
                self._withdraw_named(settings.namespace, name)
                raise
        if created is None:
            raise PlatformError(
                f"could not find an unused name for cluster '{settings.name}'"
            )

        # The platform cannot enforce "one live cluster per name", because of the
        # suffix. If someone else created the same name at the same moment, the
        # one that sorts first keeps it and the other withdraws. If the check
        # itself fails we withdraw too, rather than leave a cluster nobody holds.
        try:
            rivals = self._live_clusters(settings.name, settings.namespace)
            duplicate = bool(rivals) and rivals[0].id != created.id
            # The handle is built from the stored record, not the response to the
            # create: the platform stores the creation time less precisely than it
            # first reports it, and attach() must rebuild exactly the same handle.
            stored = next((r for r in rivals if r.id == created.id), None)
            if stored is None and not duplicate:
                stored = self._platform.get_work_requirement(created.id)
        except BaseException:
            self._withdraw(created)
            raise
        if duplicate or stored is None:
            self._withdraw(created)
            raise ClusterExistsError(
                f"a cluster named '{settings.name}' was created at the same time in "
                f"namespace '{settings.namespace}'"
            )
        LOG.info(
            "created cluster %s in namespace %s (work requirement %s)",
            settings.name,
            settings.namespace,
            stored.name,
        )
        return _handle(stored, settings)

    def _withdraw_named(self, namespace: str, name: str) -> None:
        try:
            found = self._platform.find_work_requirement(namespace, name)
        except Exception:
            LOG.warning(
                "could not check whether work requirement %s was created; "
                "`raydog cleanup` will find it if so",
                name,
            )
            return
        if found is not None and found.status.is_live:
            self._withdraw(found)

    def _withdraw(self, work_requirement: WorkRequirementInfo) -> None:
        try:
            self._platform.cancel_work_requirement(work_requirement.id)
        except Exception:
            LOG.warning(
                "could not withdraw work requirement %s; `raydog cleanup` will find it",
                work_requirement.name,
            )

    def attach(self, name: str, namespace: str) -> ClusterHandle:
        """The handle of the live cluster with exactly this name."""
        live = self._live_clusters(name, namespace)
        if not live:
            raise ClusterNotFoundError(
                f"no running cluster named '{name}' in namespace '{namespace}'"
            )
        return _handle_from(live[0])

    def handle_for(self, work_requirement_id: str) -> ClusterHandle:
        """The handle for a known work requirement, whether or not it has ended."""
        try:
            work_requirement = self._platform.get_work_requirement(work_requirement_id)
        except NotFoundError:
            raise ClusterNotFoundError(
                f"work requirement {work_requirement_id} not found"
            ) from None
        if not _is_cluster(work_requirement):
            raise ClusterNotFoundError(
                f"work requirement '{work_requirement.name}' is not a RayDog cluster"
            )
        return _handle_from(work_requirement)

    def list_clusters(
        self, namespace: str, include_ended: bool = False
    ) -> list[ClusterSummary]:
        """RayDog clusters in the namespace, oldest first."""
        clusters = []
        for summary in self._platform.list_work_requirements(
            namespace, live_only=not include_ended
        ):
            if naming.parse_work_requirement_name(summary.name) is None:
                continue
            work_requirement = self._get_or_none(summary.id)
            if work_requirement is None or not _is_cluster(work_requirement):
                continue
            clusters.append(work_requirement)
        return [
            ClusterSummary(
                name=_cluster_name(w),
                work_requirement_id=w.id,
                work_requirement_name=w.name,
                status=w.status,
                created=w.created,
            )
            for w in sorted(clusters, key=_age_order)
        ]

    # ------------------------------------------------------------------
    # Pools
    # ------------------------------------------------------------------

    def add_pool(
        self,
        handle: ClusterHandle,
        role: str,
        node: NodeSpec,
        target: int,
        max_nodes: int,
    ) -> PoolInfo:
        """Give a role its worker pool, and its task group if it has none."""
        if target < 0 or max_nodes < 1 or max_nodes < target:
            raise ConfigError(
                f"pool sizes must satisfy 0 <= target <= max_nodes and max_nodes >= 1 "
                f"(got target={target}, max_nodes={max_nodes})"
            )
        naming.validate_role(role)
        settings = handle.settings
        name = naming.pool_name(handle.work_requirement_name, role)
        images_id = self._images_id(node.images_id)  # before anything is written
        work_requirement = self._running(handle)
        existing = self._platform.find_pool(settings.namespace, name)
        if existing is not None:
            raise ClusterStateError(_pool_exists_message(settings.name, role, existing))
        if work_requirement.task_group(role) is None:
            self._platform.add_task_group(
                handle.work_requirement_id,
                TaskGroupSpec(
                    name=role,
                    worker_tag=name,
                    # The maximum is recorded so that another process can
                    # resize the pool within it
                    tag=naming.group_marker(settings.name, role, {_MAX: max_nodes}),
                ),
            )
        ownership = naming.instance_tags(settings.name, handle.work_requirement_name)
        spec = PoolSpec(
            name=name,
            namespace=settings.namespace,
            template=node.compute_requirement_template,
            worker_tag=name,
            target_instances=target,
            max_nodes=max_nodes,
            images_id=images_id,
            userdata=node.userdata,
            instance_tags={**node.instance_tags, **ownership},
            tag=settings.tag,
            metrics_enabled=node.metrics_enabled,
            idle_node_shutdown=settings.idle_shutdown,
            # An empty pool costs nothing, so it is kept for as long as the
            # cluster can live, ready to grow again
            idle_pool_shutdown=settings.lifetime,
        )
        try:
            pool = self._platform.provision_pool(spec)
        except NameInUseError:
            # Someone else gave this role its pool between our look and our call
            raise ClusterStateError(
                f"cluster '{settings.name}' already has, or has had, a pool for "
                f"role '{role}'"
            ) from None
        except BaseException:
            # The pool may exist although we never heard. It did not before this
            # call (checked above), so if it is there now it is ours to remove.
            self._remove_pool_named(settings.namespace, name)
            raise
        # If the cluster was torn down while the pool was being provisioned, the
        # teardown did not see this pool: it is ours to remove. The same goes if
        # we cannot find out.
        try:
            still_running = self._get(handle).status is WorkRequirementStatus.RUNNING
        except ClusterNotFoundError:
            still_running = False
        except BaseException:
            self._remove_pool_named(settings.namespace, name)
            raise
        if not still_running:
            self._remove_pool_named(settings.namespace, name)
            raise ClusterStateError(
                f"cluster '{settings.name}' stopped running while its '{role}' pool "
                "was being added"
            )
        LOG.info(
            "added worker pool %s for %d node(s), at most %d", name, target, max_nodes
        )
        return pool

    def has_live_pool(self, handle: ClusterHandle, role: str) -> bool:
        """Does the role have a pool that can run nodes?"""
        pool = self._platform.find_pool(
            handle.settings.namespace,
            naming.pool_name(handle.work_requirement_name, role),
        )
        return pool is not None and pool.status.is_live

    def _remove_pool_named(self, namespace: str, name: str) -> None:
        """Best effort: used to undo a pool this very call created."""
        try:
            pool = self._platform.find_pool(namespace, name)
            if pool is not None:
                self._platform.remove_pool(pool.id)
        except Exception:
            LOG.warning("could not remove pool %s; `raydog cleanup` will find it", name)

    def remove_pool(self, handle: ClusterHandle, role: str) -> None:
        """Stop a role's nodes and remove its pool."""
        settings = handle.settings
        pool = self._platform.find_pool(
            settings.namespace, naming.pool_name(handle.work_requirement_name, role)
        )
        group = self._group(handle, role)
        if pool is None and group is None:
            raise ClusterStateError(
                f"cluster '{settings.name}' has no pool for role '{role}'"
            )
        failures: list[tuple[str, Exception]] = []
        if group is not None:
            for task in self._tasks(handle, group, live_only=True):
                _attempt(
                    failures,
                    f"cancel node {task.name}",
                    lambda task=task: self._platform.cancel_task(task.id),
                )
        if pool is not None:
            pool_id = pool.id
            _attempt(
                failures,
                f"remove pool {pool.name}",
                lambda: self._platform.remove_pool(pool_id),
            )
        if failures:
            raise failures[0][1]
        LOG.info("removed the %s pool of cluster %s", role, settings.name)

    # ------------------------------------------------------------------
    # Nodes
    # ------------------------------------------------------------------

    def add_nodes(
        self,
        handle: ClusterHandle,
        role: str,
        count: int,
        script: str,
        environment: Mapping[str, str] | None = None,
        node_tags: Mapping[str, str] | None = None,
    ) -> list[TaskInfo]:
        """Start ``count`` nodes in a role: one task each, running ``script``."""
        naming.validate_role(role)
        if count < 1:
            raise ConfigError(f"count must be at least 1 (got {count})")
        settings = handle.settings
        work_requirement = self._running(handle)
        group = work_requirement.task_group(role)
        if group is None or not group.status.accepts_tasks:
            raise ClusterStateError(
                f"cluster '{settings.name}' has no usable role '{role}': "
                "add its pool first"
            )
        timeout: timedelta | None = None
        if handle.deadline is not None:
            timeout = handle.deadline - self._clock.now()
            if timeout <= timedelta(0):
                raise ClusterStateError(
                    f"cluster '{settings.name}' has reached the end of its lifetime "
                    f"({settings.lifetime}) and cannot take new nodes"
                )
        one_head = ClusterStateError(
            f"cluster '{settings.name}' can have only one head node"
        )
        is_head = role == naming.HEAD_ROLE
        if is_head and (count > 1 or self._tasks(handle, group, live_only=True)):
            raise one_head
        pool = self._platform.find_pool(
            settings.namespace, naming.pool_name(handle.work_requirement_name, role)
        )
        if pool is None or not pool.status.is_live:
            # Tasks added now would wait for ever for a node that cannot come
            raise ClusterStateError(
                f"role '{role}' of cluster '{settings.name}' has no live pool "
                "to run nodes on"
            )

        base_environment = dict(environment or {})
        base_environment[ENV_CLUSTER] = settings.name
        base_environment[ENV_ROLE] = role
        if node_tags:
            base_environment[ENV_NODE_TAGS] = json.dumps(
                dict(node_tags), sort_keys=True
            )
        try:
            tasks = self._add_node_tasks(
                group, role, count, script, base_environment, timeout
            )
        except NameInUseError:
            if is_head:
                # The head's task has a fixed name, so the platform itself turns
                # away a second head however two callers interleave
                raise one_head from None
            raise

        LOG.info("started %d %s node(s) in cluster %s", len(tasks), role, settings.name)

        # A pool grows by itself to meet its tasks, but a new one does so one
        # instance at a time. Asking for the size outright is quicker; failing to
        # ask only makes it slower, so it is not an error. The target is only ever
        # raised here: lowering it is left to the pool's own idle shutdown, which
        # never takes a node that is busy.
        try:
            staying = [
                t for t in self._tasks(handle, group, live_only=True) if not t.stopping
            ]
            wanted = max(len(staying), len(tasks))
            if wanted > pool.target_instances:
                self._platform.set_pool_target(pool.id, wanted)
        except PlatformError as e:
            LOG.warning("could not set the size of the '%s' pool: %s", role, e)
        return tasks

    def _add_node_tasks(
        self,
        group: TaskGroupInfo,
        role: str,
        count: int,
        script: str,
        base_environment: Mapping[str, str],
        timeout: timedelta | None,
    ) -> list[TaskInfo]:
        return self._platform.add_tasks(
            group.id,
            [
                TaskSpec(
                    name=(
                        naming.HEAD_TASK_NAME
                        if role == naming.HEAD_ROLE
                        else naming.node_task_name(role)
                    ),
                    script=script,
                    environment=dict(base_environment),
                    tag=NODE_TASK_TAG_PREFIX + role,
                    timeout=timeout,
                )
                for _ in range(count)
            ],
        )

    def remove_nodes(self, handle: ClusterHandle, task_ids: Sequence[str]) -> None:
        """Stop nodes. Their instances go once they have been idle long enough."""
        if task_ids:
            LOG.info(
                "stopping %d node(s) of cluster %s", len(task_ids), handle.settings.name
            )
        failures: list[tuple[str, Exception]] = []
        for task_id in task_ids:
            _attempt(
                failures,
                f"cancel node {task_id}",
                lambda task_id=task_id: self._platform.cancel_task(task_id),
            )
        if failures:
            raise failures[0][1]

    def wait_for_node(
        self, handle: ClusterHandle, task_id: str, timeout: timedelta | None = None
    ) -> NodeAddress:
        """Wait until a node's task is running, and return where it is running."""
        settings = handle.settings
        work_requirement = self._get(handle)
        first = self._platform.get_task(task_id)
        role = next(
            (
                g.name
                for g in work_requirement.task_groups
                if g.id == first.task_group_id
            ),
            None,
        )
        if role is None:
            raise ClusterStateError(
                f"task {task_id} does not belong to cluster '{settings.name}'"
            )
        pool_name = naming.pool_name(handle.work_requirement_name, role)
        LOG.debug(
            "waiting for node %s of cluster %s to start", first.name, settings.name
        )

        def look() -> tuple[TaskInfo, PoolInfo | None, NodeInfo | None]:
            task = self._platform.get_task(task_id)
            if task.status.is_terminal:
                raise NodeStartError(
                    f"node {task.name} of cluster '{settings.name}' ended as "
                    f"{task.status.value} before it started"
                    + (f": {task.error}" if task.error else "")
                )
            if task.status is not TaskStatus.EXECUTING or task.worker_id is None:
                return task, None, None
            pool = self._platform.find_pool(settings.namespace, pool_name)
            if pool is None:
                return task, None, None
            node = next(
                (
                    n
                    for n in self._platform.list_nodes(pool.id)
                    if task.worker_id in n.worker_ids and n.private_ip
                ),
                None,
            )
            return task, pool, node

        limit = timeout if timeout is not None else settings.build_timeout
        try:
            _, pool, node = poll(
                self._clock,
                look,
                lambda found: found[2] is not None,
                limit,
                _POLL_INTERVAL,
            )
        except TimeoutError:
            # Not chained: what poll last saw is RayDog's own record, not news to
            # the user; how far the node got is
            raise BuildTimeoutError(
                f"node {first.name} of cluster '{settings.name}' did not start "
                f"within {_duration(limit)}"
                + self._why_not_started(settings.namespace, pool_name)
            ) from None
        assert pool is not None and node is not None and node.private_ip is not None
        public_ip = None
        if node.instance_id is not None:
            public_ip = self._platform.public_ips(pool.id).get(node.instance_id)
        LOG.info(
            "node %s of cluster %s is running at %s%s",
            first.name,
            settings.name,
            node.private_ip,
            f" (public address {public_ip})" if public_ip else "",
        )
        return NodeAddress(
            task_id=task_id, private_ip=node.private_ip, public_ip=public_ip
        )

    def _why_not_started(self, namespace: str, pool_name: str) -> str:
        """How far a node that did not start got, for its timeout's message."""
        try:
            pool = self._platform.find_pool(namespace, pool_name)
            if pool is None:
                return ": its worker pool was not created"
            if self._platform.live_instance_count(pool.id) == 0:
                return (
                    ": no instance was provisioned for it (the compute "
                    "requirement in the YellowDog portal says why)"
                )
            if not self._platform.list_nodes(pool.id):
                return (
                    ": its instance is running, but the YellowDog Agent on it never "
                    "registered (the instance's setup may have failed or still be "
                    "running: its console output says which)"
                )
            return (
                ": the YellowDog Agent on its instance registered but did not take "
                "the node's task (if the image has an Agent of its own, the setup "
                "that installs the current one may not have finished: the "
                "instance's console output says)"
            )
        except Exception:  # best effort: the timeout itself is what matters
            LOG.debug("could not find why a node did not start", exc_info=True)
            return ""

    def node_addresses(
        self,
        handle: ClusterHandle,
        role: str,
        worker_ids: Mapping[str, str],
        with_public: bool = False,
    ) -> dict[str, tuple[str | None, str | None]]:
        """
        ``(private_ip, public_ip)`` for running nodes of one role, in bulk.

        ``worker_ids`` maps each node's task ID to the worker it runs on (from
        ``NodeRecord.worker_id``). Costs two platform calls, or three with public
        addresses, however many nodes are asked about. Nodes the platform does not
        list yet are left out.
        """
        pool = self._platform.find_pool(
            handle.settings.namespace,
            naming.pool_name(handle.work_requirement_name, role),
        )
        if pool is None:
            return {}
        by_worker = {
            worker_id: node
            for node in self._platform.list_nodes(pool.id)
            for worker_id in node.worker_ids
        }
        public = self._platform.public_ips(pool.id) if with_public else {}
        addresses: dict[str, tuple[str | None, str | None]] = {}
        for task_id, worker_id in worker_ids.items():
            node = by_worker.get(worker_id)
            if node is not None and node.private_ip:
                addresses[task_id] = (
                    node.private_ip,
                    public.get(node.instance_id) if node.instance_id else None,
                )
        return addresses

    def nodes(
        self,
        handle: ClusterHandle,
        role: str | None = None,
        include_stopping: bool = False,
    ) -> list[NodeRecord]:
        """
        The cluster's live nodes, optionally of one role, oldest first.

        A node that has been told to stop but has not yet ended is left out unless
        ``include_stopping``.
        """
        work_requirement = self._get(handle)
        roles = {g.id: g.name for g in work_requirement.task_groups}
        return [
            _node_record(task, roles[task.task_group_id])
            for task in self._platform.list_tasks(
                handle.work_requirement_id, live_only=True
            )
            if task.task_group_id in roles
            and (role is None or roles[task.task_group_id] == role)
            and (include_stopping or not task.stopping)
        ]

    def status(self, handle: ClusterHandle) -> ClusterStatus:
        settings = handle.settings
        work_requirement = self._get(handle)
        # Live tasks only, plus the head's whole history (it may have died): a
        # long-lived cluster has many ended worker tasks, and none is needed here
        tasks = self._platform.list_tasks(handle.work_requirement_id, live_only=True)
        head_group = work_requirement.task_group(naming.HEAD_ROLE)
        head_tasks = (
            self._tasks(handle, head_group, live_only=False) if head_group else []
        )
        live_heads = [t for t in head_tasks if t.status.is_live]
        candidates = live_heads or head_tasks
        head_task = candidates[-1] if candidates else None
        head = _node_record(head_task, naming.HEAD_ROLE) if head_task else None

        pools: dict[str, PoolState] = {}
        for group in work_requirement.task_groups:
            pool = self._platform.find_pool(
                settings.namespace,
                naming.pool_name(handle.work_requirement_name, group.name),
            )
            live = Counter(
                t.status
                for t in tasks
                if t.task_group_id == group.id and t.status.is_live and not t.stopping
            )
            pools[group.name] = PoolState(
                role=group.name,
                pool_id=pool.id if pool else None,
                status=pool.status if pool else None,
                live_instances=(
                    self._platform.live_instance_count(pool.id) if pool else 0
                ),
                nodes_by_status=MappingProxyType(dict(live)),
                max_nodes=(naming.parse_group_marker(group.tag) or ("", "", {}))[2].get(
                    _MAX
                ),
            )

        deadline = handle.deadline
        return ClusterStatus(
            handle=handle,
            work_requirement_status=work_requirement.status,
            head=head,
            # Alive means running: a head that is only requested may never start
            head_alive=any(t.status is TaskStatus.EXECUTING for t in live_heads)
            and work_requirement.status is WorkRequirementStatus.RUNNING,
            pools=MappingProxyType(pools),
            past_deadline=deadline is not None and self._clock.now() >= deadline,
        )

    # ------------------------------------------------------------------
    # Teardown and orphans
    # ------------------------------------------------------------------

    def tear_down(
        self, handle: ClusterHandle, verify_timeout: timedelta | None = None
    ) -> None:
        """
        End the cluster: cancel its work and remove every one of its pools.

        Every step is attempted whatever happens to the others, and the cluster may
        already have ended, wholly or in part. With ``verify_timeout``, waits until
        no instance is left alive. Raises ``TeardownError`` listing anything that
        failed or would not go; safe to call again.
        """
        name = handle.settings.name
        LOG.info("shutting down cluster %s", name)
        failures: list[tuple[str, Exception]] = []
        self._tear_down(handle, verify_timeout, failures)
        if failures:
            LOG.warning(
                "shutting down cluster %s did not complete: %d step(s) failed",
                name,
                len(failures),
            )
            raise TeardownError(failures)
        LOG.info(
            "cluster %s is shut down%s",
            name,
            " and no instance remains" if verify_timeout is not None else "",
        )

    def find_orphans(self, namespace: str) -> list[Orphan]:
        """
        RayDog resources in the namespace that nothing healthy accounts for.

        - A worker pool whose cluster's work requirement has ended or gone.
        - A cluster that is still running but has no running head: one whose head did
          not start within its build timeout, or whose head has died.
        - A cluster still running well after its lifetime has passed.

        Only resources carrying RayDog's own markers are ever reported.
        """
        orphans: list[Orphan] = []
        for summary in self._platform.list_pools(namespace, live_only=True):
            found = self._pool_orphan_reason(namespace, summary.name)
            if found is not None:
                parsed = naming.parse_pool_name(summary.name)
                assert parsed is not None
                orphans.append(
                    Orphan(
                        kind="pool",
                        id=found[0],
                        name=summary.name,
                        cluster=parsed[0],
                        reason=found[1],
                    )
                )

        for wr_summary in self._platform.list_work_requirements(
            namespace, live_only=True
        ):
            if naming.parse_work_requirement_name(wr_summary.name) is None:
                continue
            work_requirement = self._get_or_none(wr_summary.id)
            if work_requirement is None:
                continue
            reason = self._cluster_orphan_reason(work_requirement)
            if reason is not None:
                orphans.append(
                    Orphan(
                        kind="work_requirement",
                        id=work_requirement.id,
                        name=work_requirement.name,
                        cluster=_cluster_name(work_requirement),
                        reason=reason,
                    )
                )
        return orphans

    def dead_clusters(
        self, name: str, namespace: str
    ) -> list[tuple[ClusterHandle, str]]:
        """
        Live clusters with exactly this name that are no longer working, each with
        the reason, judged as ``find_orphans`` judges them. A cluster still being
        built, or still running, is not among them.
        """
        dead = []
        for work_requirement in self._live_clusters(name, namespace):
            reason = self._cluster_orphan_reason(work_requirement)
            if reason is not None:
                dead.append((_handle_from(work_requirement), reason))
        return dead

    def _cluster_orphan_reason(
        self, work_requirement: WorkRequirementInfo
    ) -> str | None:
        """
        Why this live work requirement is an orphaned cluster, or None if it is
        healthy, not a RayDog cluster, or no longer live.
        """
        if not work_requirement.status.is_live or not _is_cluster(work_requirement):
            return None
        handle = _handle_from(work_requirement)
        now = self._clock.now()
        deadline = handle.deadline
        if deadline is not None and now > deadline + _DEADLINE_GRACE:
            lifetime = handle.settings.lifetime
            return f"it is still running although its lifetime ({lifetime}) has passed"
        head_group = work_requirement.task_group(naming.HEAD_ROLE)
        assert head_group is not None  # _is_cluster checked it
        head_tasks = self._tasks(handle, head_group, live_only=True)
        if any(t.status is TaskStatus.EXECUTING for t in head_tasks):
            return None
        if not head_tasks:
            # A head task that has ended will not start again: nothing to wait for
            ended = sorted(
                {
                    t.status.value
                    for t in self._tasks(handle, head_group, live_only=False)
                    if t.status.is_terminal
                }
            )
            if ended:
                return f"its head node ended ({', '.join(ended)})"
        if now - work_requirement.created <= handle.settings.build_timeout:
            return None  # still being built
        if head_tasks:
            return "its head node has not started within the build timeout"
        return "it is running but has no head node"

    def _pool_orphan_reason(
        self, namespace: str, pool_name: str
    ) -> tuple[str, str] | None:
        """``(pool_id, reason)`` if the named pool is an orphan, else None."""
        parsed = naming.parse_pool_name(pool_name)
        if parsed is None:
            return None
        cluster, suffix, _ = parsed
        wr_name = naming.work_requirement_name(cluster, suffix)
        owner = self._platform.find_work_requirement(namespace, wr_name)
        if owner is not None and owner.status.is_live:
            return None  # its cluster is running: judged as a whole
        pool = self._platform.find_pool(namespace, pool_name)
        if pool is None or not naming.pool_belongs(
            pool.instance_tags, cluster, wr_name
        ):
            return None
        if not pool.status.is_live and not self._platform.live_instance_count(pool.id):
            return None  # already on its way out, with nothing running
        if owner is None:
            return pool.id, "its cluster's work requirement no longer exists"
        return pool.id, f"its cluster's work requirement is {owner.status.value}"

    def remove_orphans(
        self, orphans: Sequence[Orphan], verify_timeout: timedelta | None = None
    ) -> None:
        """
        Remove what ``find_orphans`` reported, except anything that has stopped
        being an orphan since. Same guarantees as ``tear_down``.
        """
        failures: list[tuple[str, Exception]] = []
        pool_ids: dict[str, str] = {}
        for orphan in orphans:
            # Things change between finding and removing: each orphan is judged
            # again, and spared if it no longer is one
            try:
                if orphan.kind == "work_requirement":
                    work_requirement = self._get_or_none(orphan.id)
                    if (
                        work_requirement is None
                        or self._cluster_orphan_reason(work_requirement) is None
                    ):
                        continue
                    handle = _handle_from(work_requirement)
                else:
                    namespace = self._platform.get_pool(orphan.id).namespace
                    if self._pool_orphan_reason(namespace, orphan.name) is None:
                        continue
            except NotFoundError:
                continue  # gone since it was found
            except Exception as e:
                failures.append((f"re-check {orphan.name}", e))
                continue
            LOG.info("removing %s: %s", orphan.name, orphan.reason)
            if orphan.kind == "work_requirement":
                self._tear_down(handle, verify_timeout, failures)
            else:
                orphan_id = orphan.id
                pool_ids[orphan.name] = orphan_id
                _attempt(
                    failures,
                    f"remove pool {orphan.name}",
                    lambda orphan_id=orphan_id: self._platform.remove_pool(orphan_id),
                )
        if verify_timeout is not None:
            self._verify_gone(pool_ids, verify_timeout, failures)
        if failures:
            LOG.warning(
                "removing what was left behind did not complete: %d step(s) failed",
                len(failures),
            )
            raise TeardownError(failures)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _tear_down(
        self,
        handle: ClusterHandle,
        verify_timeout: timedelta | None,
        failures: list[tuple[str, Exception]],
    ) -> None:
        namespace = handle.settings.namespace
        wr_name = handle.work_requirement_name
        try:
            self._platform.cancel_work_requirement(handle.work_requirement_id)
        except NotFoundError:
            pass  # purged: its pools may still be here
        except Exception as e:
            failures.append((f"cancel work requirement {wr_name}", e))

        pool_ids: dict[str, str] = {}
        try:
            prefix = wr_name + naming.SEPARATOR
            for summary in self._platform.list_pools(
                namespace, name_contains=prefix, live_only=True
            ):
                # The listing matches substrings, so "my-demo_x_h" turns up when
                # looking for "demo_x_": keep only names that *start* with ours.
                # Everything that does is this cluster's, whatever follows, since
                # the work requirement's name is unique.
                if summary.name.startswith(prefix):
                    pool_ids[summary.name] = summary.id
        except Exception as e:
            failures.append((f"list the pools of {wr_name}", e))
        for name, pool_id in pool_ids.items():
            _attempt(
                failures,
                f"remove pool {name}",
                lambda pool_id=pool_id: self._platform.remove_pool(pool_id),
            )
        if verify_timeout is not None:
            self._verify_gone(pool_ids, verify_timeout, failures)

    def _verify_gone(
        self,
        pool_ids: Mapping[str, str],
        timeout: timedelta,
        failures: list[tuple[str, Exception]],
    ) -> None:
        """
        Wait until none of the pools has a live instance. Pool status is not
        evidence of that, so instances are counted.
        """

        def remaining() -> dict[str, int]:
            counts = {}
            for name, pool_id in pool_ids.items():
                count = self._platform.live_instance_count(pool_id)
                if count:
                    counts[name] = count
            return counts

        try:
            poll(self._clock, remaining, lambda left: not left, timeout, _POLL_INTERVAL)
        except TimeoutError:
            try:
                left = remaining()
            except Exception as e:
                failures.append(("count remaining instances", e))
                return
            for name, count in left.items():
                failures.append(
                    (
                        f"pool {name}",
                        ClusterStateError(
                            f"still has {count} live instance(s) after {timeout}"
                        ),
                    )
                )
        except Exception as e:
            failures.append(("count remaining instances", e))

    def _live_clusters(self, name: str, namespace: str) -> list[WorkRequirementInfo]:
        """Live clusters with exactly this name, the rightful owner first."""
        found = []
        for summary in self._platform.list_work_requirements(
            namespace, name_contains=name + naming.SEPARATOR, live_only=True
        ):
            parsed = naming.parse_work_requirement_name(summary.name)
            if parsed is None or parsed[0] != name:
                continue  # the listing matches substrings; this is another cluster
            work_requirement = self._get_or_none(summary.id)
            if work_requirement is not None and _is_cluster(work_requirement):
                found.append(work_requirement)
        return sorted(found, key=_age_order)

    def _get(self, handle: ClusterHandle) -> WorkRequirementInfo:
        try:
            return self._platform.get_work_requirement(handle.work_requirement_id)
        except NotFoundError:
            raise ClusterNotFoundError(
                f"cluster '{handle.settings.name}' no longer exists"
            ) from None

    def _get_or_none(self, work_requirement_id: str) -> WorkRequirementInfo | None:
        try:
            return self._platform.get_work_requirement(work_requirement_id)
        except NotFoundError:
            return None

    def _images_id(self, images_id: str | None) -> str | None:
        """
        A node's image as the platform takes it. The platform accepts an image
        family only by its ID, so ``namespace/name`` is looked up; anything else
        (a cloud image ID, a YellowDog ID, a cloud's own image path) is given as is.
        """
        match = _IMAGE_FAMILY_NAME.fullmatch(images_id or "")
        if match is None:
            return images_id
        namespace, name = match.groups()
        family_id = self._platform.find_image_family(namespace, name)
        if family_id is None:
            raise ConfigError(
                f"images_id '{images_id}': there is no image family '{name}' in "
                f"namespace '{namespace}' that this application key can see"
            )
        return family_id

    def _running(self, handle: ClusterHandle) -> WorkRequirementInfo:
        work_requirement = self._get(handle)
        if work_requirement.status is not WorkRequirementStatus.RUNNING:
            raise ClusterStateError(
                f"cluster '{handle.settings.name}' is not running "
                f"(its work requirement is {work_requirement.status.value})"
            )
        return work_requirement

    def _group(self, handle: ClusterHandle, role: str) -> TaskGroupInfo | None:
        work_requirement = self._get_or_none(handle.work_requirement_id)
        return work_requirement.task_group(role) if work_requirement else None

    def _tasks(
        self, handle: ClusterHandle, group: TaskGroupInfo, live_only: bool
    ) -> list[TaskInfo]:
        return self._platform.list_tasks(
            handle.work_requirement_id, task_group_id=group.id, live_only=live_only
        )


def validate_settings(settings: ClusterSettings) -> None:
    """
    The settings are stored in a tag as whole seconds, so that is all they may
    be; anything that could not be read back is refused before a cluster exists.
    """
    if not settings.namespace:
        raise ConfigError("namespace must not be empty")
    checks = [
        ("lifetime", settings.lifetime, 1),
        ("idle_shutdown", settings.idle_shutdown, 0),
        ("build_timeout", settings.build_timeout, 1),
    ]
    for name, value, minimum in checks:
        if value is None:
            continue
        seconds = value.total_seconds()
        if seconds < minimum or seconds != int(seconds):
            raise ConfigError(
                f"{name} must be a whole number of seconds, at least {minimum} "
                f"(got {value})"
            )


def _pool_exists_message(cluster: str, role: str, pool: PoolInfo) -> str:
    if pool.status.is_live:
        return f"cluster '{cluster}' already has a pool for role '{role}'"
    return (
        f"cluster '{cluster}' had a pool for role '{role}' which has been removed; "
        "the platform keeps its name for 24 hours, so use a different name"
    )


def _attempt(failures: list[tuple[str, Exception]], what: str, action: object) -> None:
    try:
        action()  # type: ignore[operator]
    except Exception as e:
        failures.append((what, e))


def _with_worker_tag(group: TaskGroupSpec, worker_tag: str) -> TaskGroupSpec:
    return TaskGroupSpec(
        name=group.name,
        worker_tag=worker_tag,
        tag=group.tag,
        task_timeout=group.task_timeout,
        finish_if_any_task_failed=group.finish_if_any_task_failed,
        finish_if_all_tasks_finished=group.finish_if_all_tasks_finished,
    )


def _age_order(work_requirement: WorkRequirementInfo) -> tuple[datetime, str]:
    return work_requirement.created, work_requirement.id


def _head_marker(
    work_requirement: WorkRequirementInfo,
) -> tuple[str, str, dict[str, int]] | None:
    head = work_requirement.task_group(naming.HEAD_ROLE)
    return naming.parse_group_marker(head.tag) if head is not None else None


def _is_cluster(work_requirement: WorkRequirementInfo) -> bool:
    """
    Is this work requirement a RayDog cluster? Its name must parse, and its head
    task group must carry the marker for the same cluster.
    """
    parsed = naming.parse_work_requirement_name(work_requirement.name)
    marker = _head_marker(work_requirement)
    return (
        parsed is not None
        and marker is not None
        and marker[0] == parsed[0]
        and marker[1] == naming.HEAD_ROLE
    )


def _cluster_name(work_requirement: WorkRequirementInfo) -> str:
    parsed = naming.parse_work_requirement_name(work_requirement.name)
    assert parsed is not None
    return parsed[0]


def _handle(
    work_requirement: WorkRequirementInfo, settings: ClusterSettings
) -> ClusterHandle:
    return ClusterHandle(
        settings=settings,
        work_requirement_id=work_requirement.id,
        work_requirement_name=work_requirement.name,
        created=work_requirement.created,
    )


def _handle_from(work_requirement: WorkRequirementInfo) -> ClusterHandle:
    """Rebuild a handle, settings included, from what the platform holds."""
    marker = _head_marker(work_requirement)
    assert marker is not None  # callers check _is_cluster first
    cluster, _, stored = marker
    defaults = ClusterSettings(name=cluster, namespace=work_requirement.namespace)
    settings = ClusterSettings(
        name=cluster,
        namespace=work_requirement.namespace,
        tag=work_requirement.tag,
        lifetime=(
            timedelta(seconds=stored[_LIFETIME]) if _LIFETIME in stored else None
        ),
        idle_shutdown=(
            timedelta(seconds=stored[_IDLE])
            if _IDLE in stored
            else defaults.idle_shutdown
        ),
        build_timeout=(
            timedelta(seconds=stored[_BUILD])
            if _BUILD in stored
            else defaults.build_timeout
        ),
    )
    return _handle(work_requirement, settings)


def _node_record(task: TaskInfo, role: str) -> NodeRecord:
    tags: dict[str, str] = {}
    encoded = task.environment.get(ENV_NODE_TAGS)
    if encoded:
        try:
            decoded = json.loads(encoded)
            if isinstance(decoded, dict):
                tags = {str(k): str(v) for k, v in decoded.items()}
        except ValueError:
            pass
    return NodeRecord(
        task_id=task.id,
        role=role,
        status=task.status,
        tags=MappingProxyType(tags),
        worker_id=task.worker_id,
        error=task.error,
        stopping=task.stopping,
    )


def _duration(span: timedelta) -> str:
    """A timeout as people say it: "10 minutes", "90 seconds", "1 hour"."""
    seconds = int(span.total_seconds())
    for unit, size in (("hour", 3600), ("minute", 60)):
        if seconds >= size and seconds % size == 0:
            count = seconds // size
            return f"{count} {unit}{'s' if count != 1 else ''}"
    return f"{seconds} second{'s' if seconds != 1 else ''}"
