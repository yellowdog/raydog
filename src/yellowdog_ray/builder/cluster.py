"""``RayDogCluster``: build a Ray cluster on YellowDog and control it by hand."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from types import MappingProxyType, TracebackType

from yellowdog_ray.builder import state as saved_state
from yellowdog_ray.builder.spec import ClusterSpec, NodeSpec, validate_node
from yellowdog_ray.core import naming
from yellowdog_ray.core.clock import Clock
from yellowdog_ray.core.cluster import (
    ClusterCore,
    ClusterHandle,
    ClusterStatus,
    NodeAddress,
)
from yellowdog_ray.core.platform import Platform, TaskStatus
from yellowdog_ray.credentials import Credentials, load_credentials
from yellowdog_ray.errors import (
    BuildTimeoutError,
    ClusterNotFoundError,
    ClusterStateError,
    ConfigError,
    NodeStartError,
    TeardownError,
)

LOG = logging.getLogger(__name__)

ENV_HEAD_IP = "RAY_HEAD_IP"
ENV_OBSERVABILITY_HOST = "OBSERVABILITY_HOST"

# How long shut_down(wait=True) waits for every instance to go
SHUTDOWN_VERIFY_TIMEOUT = timedelta(minutes=10)


@dataclass(frozen=True)
class HeadNode:
    task_id: str
    private_ip: str
    public_ip: str | None  # None when the head has no public address


@dataclass(frozen=True)
class WorkerPool:
    name: str
    role: str  # how the pool is known on the platform
    pool_id: str | None  # None until the cluster is built
    count: int  # nodes running or starting (before the build: nodes requested)
    max_count: int | None


@dataclass
class _Pool:
    """A worker pool this process has been told about."""

    name: str
    node: NodeSpec
    count: int
    max_count: int


class RayDogCluster:
    """
    A Ray cluster on YellowDog, built and controlled step by step.

    ::

        with RayDogCluster(spec) as cluster:
            head = cluster.build()
            cluster.add_worker_pool("workers", worker_node, count=4)
            ...
        # shut down here, whatever happened inside

    Constructing one does nothing; ``build()`` creates the cluster and returns once
    its head node is running. Leaving the ``with`` block shuts the cluster down
    unless ``detach()`` was called. A cluster can be picked up again from any
    process with ``RayDogCluster.attach(name, namespace)`` or ``from_state``.
    """

    def __init__(
        self,
        spec: ClusterSpec,
        *,
        credentials: Credentials | None = None,
        platform: Platform | None = None,
        clock: Clock | None = None,
    ) -> None:
        self._spec: ClusterSpec | None = spec
        self._credentials = credentials
        self._platform = platform
        self._owns_platform = platform is None
        self._clock = clock
        self._core: ClusterCore | None = None
        self._handle: ClusterHandle | None = None
        self._head: HeadNode | None = None
        self._observability: NodeAddress | None = None
        self._pools: dict[str, _Pool] = {}  # by role
        self._shut_down = False
        self._verified_gone = False
        self._build_failed = False
        self._detached = False

    # ------------------------------------------------------------------
    # Other ways to get one
    # ------------------------------------------------------------------

    @classmethod
    def attach(
        cls,
        name: str,
        namespace: str,
        *,
        credentials: Credentials | None = None,
        platform: Platform | None = None,
        clock: Clock | None = None,
    ) -> RayDogCluster:
        """The running cluster with this name, found on the platform."""
        cluster = cls._unbuilt(credentials, platform, clock)
        try:
            cluster._handle = cluster._ensure_core().attach(name, namespace)
        except BaseException:
            cluster._close()
            raise
        return cluster

    @classmethod
    def from_state(
        cls,
        state: str | os.PathLike[str] | Mapping[str, object],
        *,
        credentials: Credentials | None = None,
        platform: Platform | None = None,
        clock: Clock | None = None,
    ) -> RayDogCluster:
        """
        The cluster described by ``save_state()``: a mapping, a JSON string or
        the path of a JSON file. Works for a cluster that has since ended, so that
        anything it left behind can still be shut down.
        """
        loaded = saved_state.load(state)
        cluster = cls._unbuilt(credentials, platform, clock)
        # The state says which platform the cluster is on, but it never decides
        # where credentials are sent: a state file is not a trusted source for that
        resolved = cluster._resolve_credentials()
        stated_url = loaded.get(saved_state.API_URL)
        if resolved is not None and stated_url and stated_url != resolved.url:
            raise ConfigError(
                f"the saved state is for the platform at {stated_url}, but the "
                f"credentials in use are for {resolved.url}"
            )
        try:
            handle = cluster._ensure_core().handle_for(loaded["work_requirement_id"])
            found = {
                "cluster": handle.settings.name,
                "namespace": handle.settings.namespace,
                "work_requirement_name": handle.work_requirement_name,
            }
            for key, value in found.items():
                if loaded[key] != value:
                    raise ConfigError(
                        f"the saved state does not match the platform: it says "
                        f"{key} is '{loaded[key]}', but that work requirement's is "
                        f"'{value}'"
                    )
        except BaseException:
            cluster._close()
            raise
        cluster._handle = handle
        return cluster

    @classmethod
    def _unbuilt(
        cls,
        credentials: Credentials | None,
        platform: Platform | None,
        clock: Clock | None,
    ) -> RayDogCluster:
        cluster = cls.__new__(cls)
        RayDogCluster.__init__(
            cluster,
            None,  # type: ignore[arg-type]
            credentials=credentials,
            platform=platform,
            clock=clock,
        )
        return cluster

    # ------------------------------------------------------------------
    # What it is
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        if self._handle is not None:
            return self._handle.settings.name
        assert self._spec is not None
        return self._spec.name

    @property
    def namespace(self) -> str:
        if self._handle is not None:
            return self._handle.settings.namespace
        assert self._spec is not None
        return self._spec.namespace

    @property
    def handle(self) -> ClusterHandle | None:
        """Identifies the cluster on the platform; None until built or attached."""
        return self._handle

    @property
    def head(self) -> HeadNode | None:
        """The head node if it is running now, else None. Checked on each use."""
        if self._handle is None or self._shut_down:
            return None
        try:
            status = self._ensure_core().status(self._handle)
        except ClusterNotFoundError:
            return None
        if status.head is None or not status.head_alive:
            self._head = None
            return None
        if self._head is None or self._head.task_id != status.head.task_id:
            address = self._address_now(status.head.task_id)
            self._head = (
                HeadNode(address.task_id, address.private_ip, address.public_ip)
                if address is not None
                else None
            )
        return self._head

    @property
    def observability(self) -> NodeAddress | None:
        """The observability node if it is running now, else None."""
        if self._handle is None or self._shut_down:
            return None
        try:
            nodes = self._ensure_core().nodes(self._handle, naming.OBSERVABILITY_ROLE)
        except ClusterNotFoundError:
            return None
        running = [n.task_id for n in nodes if n.status is TaskStatus.EXECUTING]
        if not running:
            self._observability = None
        elif self._observability is None or self._observability.task_id not in running:
            self._observability = self._address_now(running[0])
        return self._observability

    @property
    def worker_pools(self) -> Mapping[str, WorkerPool]:
        """
        The worker pools, by name. A pool added by another process appears under
        its platform-safe name, which works wherever a pool name is asked for.
        """
        if self._shut_down:
            return MappingProxyType({})
        if self._handle is None:
            return MappingProxyType(
                {
                    pool.name: WorkerPool(
                        pool.name, role, None, pool.count, pool.max_count
                    )
                    for role, pool in self._pools.items()
                }
            )
        pools: dict[str, WorkerPool] = {}
        try:
            status = self._ensure_core().status(self._handle)
        except ClusterNotFoundError:
            return MappingProxyType({})
        for role, state in status.pools.items():
            if not role.startswith(naming.WORKER_ROLE_PREFIX):
                continue
            if state.status is None or not state.status.is_live:
                continue  # never given a pool, or its pool has been removed
            known = self._pools.get(role)
            name = known.name if known else role.removeprefix(naming.WORKER_ROLE_PREFIX)
            pools[name] = WorkerPool(
                name=name,
                role=role,
                pool_id=state.pool_id,
                count=sum(state.nodes_by_status.values()),
                max_count=state.max_nodes,
            )
        return MappingProxyType(pools)

    def status(self) -> ClusterStatus:
        return self._ensure_core().status(self._built())

    # ------------------------------------------------------------------
    # Building and shutting down
    # ------------------------------------------------------------------

    def build(self) -> HeadNode:
        """
        Create the cluster and wait for its head node to be running.

        If anything fails, everything this call created is removed before the
        error is raised, and ``build()`` may be tried again.
        """
        if self._shut_down:
            raise ClusterStateError(f"cluster '{self.name}' has been shut down")
        if self._build_failed:
            raise ClusterStateError(
                f"an earlier build of cluster '{self.name}' failed and could not be "
                "fully removed: call shut_down() first"
            )
        if self._handle is not None or self._spec is None:
            raise ClusterStateError(f"cluster '{self.name}' is already built")
        spec = self._spec
        spec.validate()
        LOG.info(
            "building cluster %s with %d worker pool(s)", self.name, len(self._pools)
        )
        core = self._ensure_core()
        # As `ray up` does: a cluster of this name that is no longer working only
        # holds the name, so it is removed. One still running, or still being
        # built, is left alone, and create() refuses the name.
        for old, reason in core.dead_clusters(spec.name, spec.namespace):
            LOG.warning(
                "cluster '%s' is no longer working (%s); removing it before "
                "building a new one",
                spec.name,
                reason,
            )
            core.tear_down(old)
        handle = core.create(spec.settings())
        self._handle = handle
        try:
            # Every pool is requested before anything is waited for, so that the
            # workers' instances boot while the head's does
            core.add_pool(handle, naming.HEAD_ROLE, spec.head, target=1, max_nodes=1)
            if spec.observability is not None:
                core.add_pool(
                    handle,
                    naming.OBSERVABILITY_ROLE,
                    spec.observability,
                    target=1,
                    max_nodes=1,
                )
            for role, pool in self._pools.items():
                core.add_pool(
                    handle, role, pool.node, target=pool.count, max_nodes=pool.max_count
                )

            environment: dict[str, str] = {}
            if spec.observability is not None:
                task = core.add_nodes(
                    handle,
                    naming.OBSERVABILITY_ROLE,
                    1,
                    spec.observability.start_script,
                )[0]
                self._observability = core.wait_for_node(handle, task.id)
                environment[ENV_OBSERVABILITY_HOST] = self._observability.private_ip

            head_task = core.add_nodes(
                handle, naming.HEAD_ROLE, 1, spec.head.start_script, environment
            )[0]
            address = core.wait_for_node(handle, head_task.id)
            self._head = HeadNode(
                address.task_id, address.private_ip, address.public_ip
            )

            # The workers' tasks need the head's address, so they are added only
            # now, to nodes that have usually registered already. A node that is
            # already registered sees a new task at its next connection to the
            # platform, not at once: expect the workers to start some tens of
            # seconds after this, however early their instances were ready.
            for role, pool in self._pools.items():
                if pool.count:
                    core.add_nodes(
                        handle,
                        role,
                        pool.count,
                        pool.node.start_script,
                        self._worker_environment(self._head),
                    )
        except BaseException:
            self._roll_back(handle)
            raise
        LOG.info("cluster %s is built", self.name)
        return self._head

    def shut_down(self, wait: bool = False) -> None:
        """
        End the cluster: stop every node and remove every pool.

        With ``wait``, returns only when no instance is left alive; that check is
        made even if the cluster was already shut down without it. Does nothing if
        the cluster was never built. If it raises ``TeardownError``, something may
        still be running: call it again.
        """
        if self._handle is None:
            self._shut_down = True
            self._close()
            return
        if self._shut_down and (self._verified_gone or not wait):
            return  # already done, and already checked if that was asked for
        try:
            self._ensure_core().tear_down(
                self._handle, SHUTDOWN_VERIFY_TIMEOUT if wait else None
            )
        finally:
            self._close()
        self._shut_down = True
        self._verified_gone = self._verified_gone or wait
        self._build_failed = False
        self._head = None
        self._observability = None

    def detach(self) -> None:
        """
        Let the cluster outlive this object: leaving the ``with`` block will not
        shut it down.
        """
        self._detached = True

    def __enter__(self) -> RayDogCluster:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._detached:
            self._close()
            return
        try:
            self.shut_down()
        except TeardownError as e:
            if exc_type is None:
                raise
            # Do not hide the error that got us here behind this one
            LOG.error(
                "cluster '%s' was not shut down cleanly and may still be running: %s",
                self.name,
                e,
            )

    # ------------------------------------------------------------------
    # Worker pools
    # ------------------------------------------------------------------

    def add_worker_pool(
        self, name: str, node: NodeSpec, count: int, max_count: int | None = None
    ) -> WorkerPool:
        """
        Add a pool of ``count`` worker nodes, able to grow to ``max_count``.

        Before ``build()``, the pool is created by the build, its instances booting
        alongside the head's. After, it is created now.
        """
        if self._shut_down:
            raise ClusterStateError(f"cluster '{self.name}' has been shut down")
        maximum = max(count, 1) if max_count is None else max_count
        if count < 0 or maximum < max(count, 1):
            raise ConfigError(
                f"worker pool '{name}': need 0 <= count <= max_count and "
                f"max_count >= 1 (got count={count}, max_count={max_count})"
            )
        validate_node(f"worker pool '{name}'", node)
        role = naming.worker_role(name)
        existing = self._pools.get(role)
        if existing is not None:
            raise ConfigError(
                f"cluster '{self.name}' already has a worker pool named "
                f"'{existing.name}'"
                + ("" if existing.name == name else f", which '{name}' clashes with")
            )
        pool = _Pool(name=name, node=node, count=count, max_count=maximum)
        if self._handle is None:
            self._pools[role] = pool
            return WorkerPool(name, role, None, count, maximum)

        handle = self._handle
        core = self._ensure_core()
        head = self._live_head()
        info = core.add_pool(handle, role, node, target=count, max_nodes=maximum)
        try:
            if count:
                core.add_nodes(
                    handle,
                    role,
                    count,
                    node.start_script,
                    self._worker_environment(head),
                )
        except BaseException:
            try:
                core.remove_pool(handle, role)
            except Exception as e:
                LOG.error("could not remove half-made worker pool '%s': %s", name, e)
            raise
        self._pools[role] = pool
        return WorkerPool(name, role, info.id, count, maximum)

    def resize_worker_pool(
        self, name: str, count: int, *, start_script: str | None = None
    ) -> WorkerPool:
        """
        Grow or shrink a worker pool to ``count`` nodes, within its maximum.

        Shrinking stops nodes that have not started yet first, then the most
        recently added. Growing a pool that another process added needs
        ``start_script``, since this one never saw it.
        """
        if count < 0:
            raise ConfigError(f"count must not be negative (got {count})")
        role = self._role_of(name)
        known = self._pools.get(role)
        if self._handle is None:
            if known is None:
                raise ClusterStateError(
                    f"cluster '{self.name}' has no worker pool named '{name}'"
                )
            if count > known.max_count:
                raise ConfigError(
                    f"worker pool '{name}' can have at most {known.max_count} nodes"
                )
            known.count = count
            return WorkerPool(name, role, None, count, known.max_count)
        handle = self._built()
        core = self._ensure_core()
        state = core.status(handle).pools.get(role)
        if state is None or state.status is None or not state.status.is_live:
            raise ClusterStateError(
                f"cluster '{self.name}' has no worker pool named '{name}'"
            )
        if state.max_nodes is not None and count > state.max_nodes:
            raise ConfigError(
                f"worker pool '{name}' can have at most {state.max_nodes} nodes"
            )
        nodes = core.nodes(handle, role)
        if count > len(nodes):
            script = start_script or (known.node.start_script if known else None)
            if script is None:
                raise ClusterStateError(
                    f"worker pool '{name}' was added by another process: pass "
                    "start_script= to grow it"
                )
            core.add_nodes(
                handle,
                role,
                count - len(nodes),
                script,
                self._worker_environment(self._live_head()),
            )
        elif count < len(nodes):
            # Keep running nodes in preference to ones that have not started; among
            # equals, the ones listed last (the most recently added) go first
            started = [n for n in nodes if n.status is TaskStatus.EXECUTING]
            starting = [n for n in nodes if n.status is not TaskStatus.EXECUTING]
            surplus = (started + starting)[count:]
            core.remove_nodes(handle, [n.task_id for n in surplus])
        if known is not None:
            known.count = count
        return WorkerPool(name, role, state.pool_id, count, state.max_nodes)

    def remove_worker_pool(self, name: str) -> None:
        """Stop a worker pool's nodes and remove it."""
        if self._handle is None and not self._shut_down:
            role = self._role_of(name)
            if role not in self._pools:
                raise ClusterStateError(
                    f"cluster '{self.name}' has no worker pool named '{name}'"
                )
            del self._pools[role]
            return
        handle = self._built()
        role = self._role_of(name)
        self._ensure_core().remove_pool(handle, role)
        self._pools.pop(role, None)

    # ------------------------------------------------------------------
    # Saved state
    # ------------------------------------------------------------------

    def save_state(self, path: str | os.PathLike[str] | None = None) -> str:
        """
        The cluster's identity as JSON, also written to ``path`` if given.

        Enough for ``from_state`` to find the cluster again. Holds no credentials.
        """
        text = saved_state.dump(
            self._built(), self._credentials.url if self._credentials else None
        )
        if path is not None:
            Path(path).write_text(text)
        return text

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _resolve_credentials(self) -> Credentials | None:
        """The credentials a connection would use; None if a platform was given."""
        if self._platform is not None and not self._owns_platform:
            return None
        if self._credentials is None:
            self._credentials = load_credentials()
        return self._credentials

    def _ensure_core(self) -> ClusterCore:
        if self._core is None:
            if self._platform is None:
                # Imported here so that the SDK is only needed when it is used
                from yellowdog_ray.core.yellowdog import YellowDogPlatform

                self._resolve_credentials()
                assert self._credentials is not None
                self._platform = YellowDogPlatform.connect(
                    self._credentials.key_id,
                    self._credentials.key_secret,
                    self._credentials.url,
                )
            self._core = ClusterCore(self._platform, self._clock)
        return self._core

    def _close(self) -> None:
        if self._owns_platform and self._platform is not None:
            try:
                self._platform.close()
            except Exception as e:
                LOG.warning("could not close the platform connection: %s", e)
            self._platform = None
            self._core = None

    def _built(self) -> ClusterHandle:
        if self._shut_down:
            raise ClusterStateError(f"cluster '{self.name}' has been shut down")
        if self._handle is None:
            raise ClusterStateError(f"cluster '{self.name}' has not been built")
        return self._handle

    def _roll_back(self, handle: ClusterHandle) -> None:
        """
        Undo a failed build. If that works the cluster is "not built" again; if
        it does not, the cluster is remembered so that shut_down() can finish the
        job, and is never simply forgotten.
        """
        self._head = None
        self._observability = None
        try:
            self._ensure_core().tear_down(handle)
        except BaseException as e:
            self._build_failed = True
            LOG.error(
                "cluster '%s' failed to build and could not be fully removed: call "
                "shut_down() to try again (`raydog cleanup` will also find it): %s",
                self.name,
                e,
            )
            if not isinstance(e, Exception):
                raise
            return
        self._handle = None

    def _role_of(self, name: str) -> str:
        for role, pool in self._pools.items():
            if pool.name == name:
                return role
        # A pool added by another process is shown under its platform-safe name:
        # that name must lead back to the same pool, not be made safe a second time
        shown_as = naming.WORKER_ROLE_PREFIX + name
        if self._handle is not None and shown_as != naming.worker_role(name):
            try:
                naming.validate_role(shown_as)
                if shown_as in self._ensure_core().status(self._handle).pools:
                    return shown_as
            except (ConfigError, ClusterNotFoundError):
                pass
        return naming.worker_role(name)

    def _address_now(self, task_id: str) -> NodeAddress | None:
        assert self._handle is not None
        try:
            return self._ensure_core().wait_for_node(
                self._handle, task_id, timeout=timedelta(0)
            )
        except (BuildTimeoutError, NodeStartError):
            return None

    def _live_head(self) -> HeadNode:
        """The head, checked afresh: workers must not be pointed at a dead one."""
        self._head = None
        head = self.head
        if head is None:
            raise ClusterStateError(
                f"the head node of cluster '{self.name}' is not running"
            )
        return head

    def _worker_environment(self, head: HeadNode) -> dict[str, str]:
        environment = {ENV_HEAD_IP: head.private_ip}
        observability = self.observability
        if observability is not None:
            environment[ENV_OBSERVABILITY_HOST] = observability.private_ip
        return environment
