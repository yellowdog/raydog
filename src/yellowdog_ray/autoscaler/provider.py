"""
``RayDogNodeProvider``: YellowDog as the cloud behind Ray's autoscaler.

Ray constructs this class itself, from the ``provider`` section of a cluster YAML,
in two kinds of process:

- on the **client**, for each ``ray up``, ``ray down``, ``ray exec`` and so on. Here
  the provider finds the cluster by name, creates its head node, and tears the
  cluster down;
- on the **head node**, once, inside Ray's monitor. Here it creates and removes
  worker nodes as the autoscaler asks.

Which of the two it is, it reads from its own config: the copy of the config that
Ray sends to the head node is stamped by ``prepare_for_head_node``.

A node, to Ray, is a YellowDog task. Nothing is torn down as a side effect: not by
the constructor, not by a failed lookup, not by garbage collection. A cluster ends
only when Ray asks for its head node to be terminated, or calls
``cleanup_cluster_resources``.
"""

from __future__ import annotations

import contextlib
import copy
import json
import logging
import os
import sys
import threading
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import click
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)

from yellowdog_ray import logs
from yellowdog_ray.autoscaler import config as cfg
from yellowdog_ray.autoscaler.nodes import NodeTable
from yellowdog_ray.core import naming
from yellowdog_ray.core.clock import Clock, SystemClock
from yellowdog_ray.core.cluster import ClusterCore, ClusterHandle, NodeSpec
from yellowdog_ray.core.platform import Platform
from yellowdog_ray.credentials import (
    KEY_ID_VAR,
    KEY_SECRET_VAR,
    URL_VAR,
    Credentials,
    load_credentials,
)
from yellowdog_ray.errors import (
    ClusterNotFoundError,
    ClusterStateError,
    ConfigError,
    RayDogError,
)

LOG = logging.getLogger(__name__)

TROUBLESHOOTING_HINT = (
    "See 'The head node does not start' in "
    "https://docs.yellowdog.ai/raydog/troubleshooting.html"
)
ENV_HEAD_IP = "RAY_HEAD_IP"
# What a node type declares, made true on the node: Ray's own launcher sets these
# for `ray start`, which then uses them in place of what it finds on the machine
ENV_RESOURCES = "RAY_OVERRIDE_RESOURCES"
ENV_LABELS = "RAY_OVERRIDE_LABELS"


def _declared(
    resources: Mapping[str, float], labels: Mapping[str, str]
) -> dict[str, str]:
    """A node type's declared resources and labels, for ``ray start``, if any."""
    return {
        **({ENV_RESOURCES: json.dumps(dict(resources))} if resources else {}),
        **({ENV_LABELS: json.dumps(dict(labels))} if labels else {}),
    }


ENV_CREDENTIALS_FILE = "RAYDOG_CREDENTIALS_FILE"

# How long `ray down` waits to see every instance gone
TEARDOWN_VERIFY_TIMEOUT = timedelta(minutes=10)
STATUS_TERMINATED = "terminated"


class RayDogNodeProvider(NodeProvider):
    def __init__(self, provider_config: dict[str, Any], cluster_name: str) -> None:
        """No network calls are made here, and nothing can be destroyed from here."""
        super().__init__(provider_config, cluster_name)
        self._config = cfg.parse_provider_config(provider_config, cluster_name)
        self._on_head = self._config.head is not None
        if not self._on_head:
            # On the head node, Ray's monitor already puts RayDog's lines in its log
            logs.show_from_environment()
        # Guards everything below it; held only briefly, never across a wait
        self._lock = threading.RLock()
        # Serialises the creation of worker pools: adding a task group reads and
        # rewrites the work requirement, so two at once could lose one
        self._pool_creation = threading.Lock()
        # One refresh at a time: an older one finishing after a newer one would
        # put the table back in the past
        self._refreshing = threading.Lock()
        self._core: ClusterCore | None = None
        self._clock: Clock = SystemClock()
        self._table = NodeTable(self._clock, self._config.worker_start_grace)
        self._handle: ClusterHandle | None = None
        self._last_refresh: datetime | None = None
        self._stale = True
        self._pools_ready: set[str] = set()
        self._head_ip: str | None = None

    # ------------------------------------------------------------------
    # Ray's interface: looking
    # ------------------------------------------------------------------

    def non_terminated_nodes(self, tag_filters: dict[str, str]) -> list[str]:
        """
        The cluster's live nodes with these tags. This is the one call that
        consults the platform; the per-node queries answer from what it found.
        """
        self._refresh()
        return self._table.ids(tag_filters)

    def is_running(self, node_id: str) -> bool:
        return self._table.is_running(node_id)

    def is_terminated(self, node_id: str) -> bool:
        return self._table.is_terminated(node_id)

    def node_tags(self, node_id: str) -> dict[str, str]:
        tags = self._table.tags(node_id)
        # Ray may ask about a node it listed a moment ago and that has since gone
        return tags or {TAG_RAY_NODE_STATUS: STATUS_TERMINATED}

    def set_node_tags(self, node_id: str, tags: dict[str, str]) -> None:
        self._table.set_tags(node_id, tags)

    # Ray's signatures say "str", but its own code polls these until they stop
    # returning None, which is how a node whose address is not known yet is told
    def external_ip(self, node_id: str) -> str | None:  # type: ignore[override]
        return self._table.external_ip(node_id)

    def internal_ip(self, node_id: str) -> str | None:  # type: ignore[override]
        return self._table.internal_ip(node_id)

    # ------------------------------------------------------------------
    # Ray's interface: creating
    # ------------------------------------------------------------------

    def create_node(
        self, node_config: dict[str, Any], tags: dict[str, str], count: int
    ) -> dict[str, Any] | None:
        return self.create_node_with_resources_and_labels(
            node_config, tags, count, {}, {}
        )

    def create_node_with_resources_and_labels(
        self,
        node_config: dict[str, Any],
        tags: dict[str, str],
        count: int,
        resources: dict[str, float],
        labels: dict[str, str],
    ) -> dict[str, Any] | None:
        """
        Ray's launchers call this, with what a worker's node type declares, and
        each worker is given the declaration. The head's comes from the config.
        """
        if tags.get(TAG_RAY_NODE_KIND) == NODE_KIND_HEAD:
            if self._on_head:
                raise ClusterStateError("a head node cannot create a head node")
            self._create_head(node_config, tags)
        else:
            if not self._on_head:
                raise ClusterStateError(
                    "worker nodes are created by the autoscaler on the head node, "
                    "not from the client"
                )
            self._create_workers(node_config, tags, count, resources, labels)
        return None

    def _create_head(self, node_config: dict[str, Any], tags: dict[str, str]) -> None:
        core = self._ensure_core()
        settings = self._config.settings()
        node_type = tags.get(TAG_RAY_USER_NODE_TYPE, "head")
        spec = self._node_spec(node_type, node_config, cfg.K_HEAD_SCRIPT)

        # Ray asks for a head only when it found none. If a cluster of this name
        # is still there, it is a dead one, and is cleared away first. One whose
        # head task is alive is never touched: that would be someone else's
        # cluster starting up.
        try:
            old = core.attach(settings.name, settings.namespace)
        except ClusterNotFoundError:
            old = None
        if old is not None:
            head = core.status(old).head
            if head is not None and head.status.is_live and not head.stopping:
                raise ClusterStateError(
                    f"cluster '{settings.name}' already has a head node "
                    f"({head.status.value}); not replacing it"
                )
            if (
                head is None
                and self._clock.now() - old.created <= old.settings.build_timeout
            ):
                # No head task yet, and young: someone else's `ray up`, mid-flight
                raise ClusterStateError(
                    f"cluster '{settings.name}' is being created by another "
                    "process; try again when it has finished"
                )
            LOG.warning(
                "cluster '%s' has no head node; removing it before creating a new one",
                settings.name,
            )
            core.tear_down(old)

        handle = core.create(settings)
        task_id: str | None = None
        try:
            core.add_pool(handle, naming.HEAD_ROLE, spec, target=1, max_nodes=1)
            task = core.add_nodes(
                handle,
                naming.HEAD_ROLE,
                1,
                spec.start_script,
                environment=_declared(
                    self._config.head_resources, self._config.head_labels
                ),
                node_tags=tags,
            )[0]
            task_id = task.id
            self._table.created_here(task.id, tags, naming.HEAD_ROLE)
            address = core.wait_for_node(handle, task.id)
        except BaseException as e:
            # This call made the cluster, so this call may unmake it. Said at
            # WARNING, which reaches the terminal of a `ray up` with nothing set up.
            if task_id is not None:
                self._table.forget([task_id])
            interrupted = isinstance(e, KeyboardInterrupt)
            LOG.warning(
                "%s before the head node of cluster '%s' started; removing the cluster",
                "ray up was interrupted" if interrupted else "ray up failed",
                settings.name,
            )
            finish = f"raydog down {settings.name} -n {settings.namespace}"
            try:
                core.tear_down(handle)
            except KeyboardInterrupt:
                LOG.warning(
                    "removal of cluster '%s' interrupted; `%s` finishes it",
                    settings.name,
                    finish,
                )
                raise
            except Exception as cleanup_error:
                LOG.error(
                    "cluster '%s' failed to start and could not be fully removed "
                    "(`%s` finishes it): %s",
                    settings.name,
                    finish,
                    cleanup_error,
                )
            else:
                LOG.warning(
                    "cluster '%s' removed; its instances are being released",
                    settings.name,
                )
            if isinstance(e, RayDogError) and click.get_current_context(silent=True):
                # Inside Ray's command line (`ray up`): an error it prints as one
                # line, as it does its own, rather than a Python traceback
                raise click.ClickException(f"{e}. {TROUBLESHOOTING_HINT}") from None
            raise
        self._table.set_address(task.id, address.private_ip, address.public_ip)
        with self._lock:
            self._handle = handle
            self._stale = True

    def _create_workers(
        self,
        node_config: dict[str, Any],
        tags: dict[str, str],
        count: int,
        resources: dict[str, float],
        labels: dict[str, str],
    ) -> None:
        core = self._ensure_core()
        node_type = tags.get(TAG_RAY_USER_NODE_TYPE)
        if not node_type:
            raise ConfigError(f"a worker node needs a '{TAG_RAY_USER_NODE_TYPE}' tag")
        role = naming.worker_role(node_type)
        handle = self._find_cluster()
        if handle is None:
            raise ClusterNotFoundError(
                f"cluster '{self._config.cluster_name}' no longer exists"
            )
        if role not in self._pools_ready:
            with self._pool_creation:
                if role not in self._pools_ready:
                    spec = self._node_spec(node_type, node_config, cfg.K_WORKER_SCRIPT)
                    maximum = self._config.max_workers.get(
                        node_type, cfg.DEFAULT_MAX_WORKERS
                    )
                    try:
                        core.add_pool(handle, role, spec, target=0, max_nodes=maximum)
                    except ClusterStateError:
                        # It may have one already, from before a restart of the
                        # monitor, say. Anything else (its pool was removed, the
                        # cluster has stopped) is a real error, every time.
                        if not core.has_live_pool(handle, role):
                            raise
                    self._pools_ready.add(role)
        tasks = core.add_nodes(
            handle,
            role,
            count,
            self._script(cfg.K_WORKER_SCRIPT),
            environment={
                ENV_HEAD_IP: self._head_private_ip(core, handle),
                **_declared(resources, labels),
            },
            node_tags=tags,
        )
        for task in tasks:
            self._table.created_here(task.id, tags, role)
        with self._lock:
            self._stale = True

    # ------------------------------------------------------------------
    # Ray's interface: ending
    # ------------------------------------------------------------------

    def terminate_node(self, node_id: str) -> dict[str, Any] | None:
        return self.terminate_nodes([node_id])

    def terminate_nodes(self, node_ids: list[str]) -> dict[str, Any] | None:
        """Stop nodes. Terminating the head node ends the whole cluster."""
        if not node_ids:
            return None
        core = self._ensure_core()
        # The cluster these nodes were listed from, not whichever cluster has the
        # name now: a stale node ID must never bring down a newer cluster
        with self._lock:
            handle = self._handle
        if handle is None:
            handle = self._find_cluster()
        if handle is None:
            self._table.forget(node_ids)
            return None
        if any(self._table.role(n) == naming.HEAD_ROLE for n in node_ids):
            self._tear_down(core, handle)
            return None
        try:
            core.remove_nodes(handle, node_ids)
        except BaseException:
            # Some may not have been stopped. Forget none: the ones that were told
            # to stop drop out of the next listing by themselves, and the rest
            # must stay visible so that Ray tries again.
            with self._lock:
                self._stale = True
            raise
        self._table.forget(node_ids)
        with self._lock:
            self._stale = True
        return None

    def cleanup_cluster_resources(self) -> None:
        """
        Called by ``ray down`` once the nodes have gone: make sure the cluster
        has gone with them. Never raises.
        """
        try:
            core = self._ensure_core()
            handle = self._find_cluster()
            if handle is not None:
                self._tear_down(core, handle)
        except Exception as e:
            LOG.error(
                "cluster '%s' may not be fully removed (`raydog cleanup` will find "
                "what is left): %s",
                self._config.cluster_name,
                e,
            )

    def _tear_down(self, core: ClusterCore, handle: ClusterHandle) -> None:
        try:
            # On the client there is someone waiting to be told it worked. On the
            # head node, this process is itself about to be stopped.
            core.tear_down(handle, None if self._on_head else TEARDOWN_VERIFY_TIMEOUT)
        finally:
            self._table.clear()
            with self._lock:
                self._handle = None
                self._head_ip = None
                self._pools_ready.clear()
                self._stale = True

    # ------------------------------------------------------------------
    # Ray's interface: configuration
    # ------------------------------------------------------------------

    @staticmethod
    def fillout_available_node_types_resources(
        cluster_config: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Ray calls this just before it validates a config. RayDog uses it to fill
        in the general Ray settings that Ray requires but gives an external
        provider no defaults for, so that a cluster YAML can leave them out.
        """
        return cfg.with_ray_defaults(cluster_config)

    @staticmethod
    def bootstrap_config(cluster_config: dict[str, Any]) -> dict[str, Any]:
        """
        Check the cluster config and make it self-contained. Runs on the client
        before anything is created (though not before every command: Ray caches
        the result).
        """
        return cfg.bootstrap(cluster_config, cfg.default_search_dirs(sys.argv))

    def prepare_for_head_node(self, cluster_config: dict[str, Any]) -> dict[str, Any]:
        """
        The config for the head node's copy of this provider: the same, stamped
        with which cluster it belongs to and where its credentials will be.
        """
        self._ensure_core()
        handle = self._find_cluster()
        if handle is None:
            raise ClusterNotFoundError(
                f"cluster '{self._config.cluster_name}' does not exist"
            )
        config = copy.deepcopy(cluster_config)
        provider = config["provider"]
        _, remote = cfg.credentials_paths(
            self._config.namespace, self._config.cluster_name
        )
        provider[cfg.K_HEAD] = {
            "work_requirement_id": handle.work_requirement_id,
            "credentials_file": remote,
        }
        provider.setdefault(cfg.K_DISABLE_UPDATERS, True)
        for key in (cfg.K_HEAD_SCRIPT, cfg.K_WORKER_SCRIPT):
            provider[key] = self._script(key)
        return config

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _connect(self, credentials: Credentials) -> tuple[Platform, Clock]:
        """The one place a connection is made (and what tests replace)."""
        from yellowdog_ray.core.yellowdog import YellowDogPlatform

        platform = YellowDogPlatform.connect(
            credentials.key_id, credentials.key_secret, credentials.url
        )
        return platform, SystemClock()

    def _ensure_core(self) -> ClusterCore:
        with self._lock:
            if self._core is None:
                if self._on_head:
                    assert self._config.head is not None
                    path = os.environ.get(ENV_CREDENTIALS_FILE) or os.path.expanduser(
                        self._config.head.credentials_file
                    )
                    credentials = load_credentials(credentials_file=path)
                else:
                    credentials = load_credentials()
                    # Ray is about to hash and copy this file to the head node
                    _write_credentials_file(
                        credentials,
                        cfg.credentials_paths(
                            self._config.namespace, self._config.cluster_name
                        )[0],
                    )
                platform, self._clock = self._connect(credentials)
                self._table.clock = self._clock
                self._core = ClusterCore(platform, self._clock)
                LOG.info(
                    "RayDog provider for cluster %s connected to YellowDog (%s)",
                    self._config.cluster_name,
                    "on the head node" if self._on_head else "from the client",
                )
            return self._core

    def _find_cluster(self) -> ClusterHandle | None:
        """
        The cluster, or None if there is none. The head node knows its own by
        ID; the client looks by name, afresh each time.
        """
        core = self._ensure_core()
        with self._lock:
            known = self._handle
        if self._on_head:
            if known is None:
                assert self._config.head is not None
                try:
                    known = core.handle_for(self._config.head.work_requirement_id)
                except ClusterNotFoundError:
                    return None
        else:
            try:
                known = core.attach(self._config.cluster_name, self._config.namespace)
            except ClusterNotFoundError:
                known = None
        with self._lock:
            self._handle = known
        return known

    def _refresh(self) -> None:
        core = self._ensure_core()
        with self._refreshing:
            self._refresh_now(core)

    def _refresh_now(self, core: ClusterCore) -> None:
        with self._lock:
            now = self._clock.now()
            fresh_enough = (
                not self._stale
                and self._last_refresh is not None
                and now - self._last_refresh < self._config.refresh_interval
            )
            if fresh_enough:
                return
            # Mark it done before starting: a change made while this refresh is
            # running sets _stale again and so triggers another
            self._stale = False
            self._last_refresh = now
        try:
            handle = self._find_cluster()
            if handle is None:
                self._table.refresh([], {})
                return
            try:
                records = core.nodes(handle)
            except ClusterNotFoundError:
                self._table.refresh([], {})
                return
            self._table.refresh(records, {})
            LOG.debug(
                "cluster %s has %d live node(s)", handle.settings.name, len(records)
            )
            workers = {r.task_id: r.worker_id for r in records if r.worker_id}
            for role, task_ids in self._table.needing_addresses().items():
                wanted = {t: workers[t] for t in task_ids if t in workers}
                if not wanted:
                    continue
                # Public addresses are only ever needed on the client (for SSH)
                found = core.node_addresses(handle, role, wanted, not self._on_head)
                for task_id, (private_ip, public_ip) in found.items():
                    self._table.set_address(task_id, private_ip, public_ip)
        except BaseException:
            with self._lock:
                self._stale = True
            raise

    def _head_private_ip(self, core: ClusterCore, handle: ClusterHandle) -> str:
        with self._lock:
            if self._head_ip is not None:
                return self._head_ip
        heads = core.nodes(handle, naming.HEAD_ROLE)
        workers = {r.task_id: r.worker_id for r in heads if r.worker_id}
        found = core.node_addresses(handle, naming.HEAD_ROLE, workers)
        address = next((private for private, _ in found.values() if private), None)
        if address is None:
            raise ClusterStateError(
                f"the head node of cluster '{self._config.cluster_name}' is not running"
            )
        with self._lock:
            self._head_ip = address
        return address

    def _node_spec(
        self, node_type: str, node_config: dict[str, Any], script_key: str
    ) -> NodeSpec:
        return replace(
            cfg.parse_node_config(node_type, node_config),
            start_script=self._script(script_key),
        )

    def _script(self, key: str) -> str:
        """
        A start script's text. Normally already inlined by bootstrap_config; if
        Ray skipped that, the file is read now.
        """
        value = getattr(self._config, key)
        if cfg.is_file_reference(value):
            value = cfg._inline(
                value, cfg.default_search_dirs(sys.argv), f"provider.{key}"
            )
        assert isinstance(value, str)
        return value


def _write_credentials_file(credentials: Credentials, path: Path) -> None:
    """
    Keep the cluster's credentials file up to date, readable only by its owner.

    Ray copies this file to the head node over SSH; that is the only way the
    credentials travel.
    """
    content = (
        f"{KEY_ID_VAR}={credentials.key_id}\n"
        f"{KEY_SECRET_VAR}={credentials.key_secret}\n"
        f"{URL_VAR}={credentials.url}\n"
    )
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    if path.is_symlink():
        # Never write a secret through a link to somewhere else
        path.unlink()
    try:
        unchanged = path.is_file() and path.read_text() == content
    except OSError:
        unchanged = False
    if not unchanged:
        # Written beside the target and moved into place, so that the file is
        # never there with the wrong contents or the wrong permissions
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
        descriptor = os.open(temporary, flags, 0o600)
        with os.fdopen(descriptor, "w") as file:
            file.write(content)
        os.replace(temporary, path)
    os.chmod(path, 0o600)
