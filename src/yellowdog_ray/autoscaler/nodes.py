"""
The provider's picture of the cluster's nodes, safe to use from many threads.

Ray asks a provider about individual nodes many times a second and from several
threads, but expects those answers from memory: ``non_terminated_nodes`` is the one
call that may go and look. This table is that memory. It is rebuilt from the
platform's list of the cluster's live tasks, so a node that has died simply stops
being in it.
"""

from __future__ import annotations

import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from yellowdog_ray.core import naming
from yellowdog_ray.core.clock import Clock
from yellowdog_ray.core.cluster import NodeRecord
from yellowdog_ray.core.platform import TaskStatus

# Ray's tag names and values, repeated here so that this module needs no Ray import
TAG_NODE_STATUS = "ray-node-status"
TAG_NODE_KIND = "ray-node-type"
STATUS_UNINITIALIZED = "uninitialized"
STATUS_UP_TO_DATE = "up-to-date"

# How long a node this process created is believed in before the platform lists it
UNLISTED_NODE_TRUST = timedelta(seconds=60)


@dataclass
class _Node:
    role: str
    status: TaskStatus
    creation_tags: dict[str, str]
    private_ip: str | None = None
    public_ip: str | None = None
    executing_since: datetime | None = None
    created_here: bool = False
    seen: bool = True  # present in a refresh (False: created here, not yet listed)
    created_at: datetime | None = None


@dataclass
class NodeTable:
    clock: Clock
    worker_start_grace: timedelta = timedelta(0)
    _nodes: dict[str, _Node] = field(default_factory=dict)
    _overlay: dict[str, dict[str, str]] = field(default_factory=dict)
    _forgotten: set[str] = field(default_factory=set)
    _lock: threading.RLock = field(default_factory=threading.RLock)

    # --- changes -----------------------------------------------------------

    def refresh(
        self,
        records: Sequence[NodeRecord],
        addresses: Mapping[str, tuple[str | None, str | None]],
    ) -> None:
        """
        Replace the table with the platform's current list of live nodes.

        ``addresses`` gives ``(private_ip, public_ip)`` for nodes whose addresses
        were looked up this time; addresses already known are kept.
        """
        now = self.clock.now()
        with self._lock:
            listed = {r.task_id for r in records}
            fresh: dict[str, _Node] = {}
            for record in records:
                if record.task_id in self._forgotten or record.stopping:
                    continue
                old = self._nodes.get(record.task_id)
                node = _Node(
                    role=record.role,
                    status=record.status,
                    creation_tags=dict(record.tags),
                    private_ip=old.private_ip if old else None,
                    public_ip=old.public_ip if old else None,
                    executing_since=old.executing_since if old else None,
                    created_here=old.created_here if old else False,
                )
                if record.status is TaskStatus.EXECUTING:
                    node.executing_since = node.executing_since or now
                else:
                    node.executing_since = None
                private_ip, public_ip = addresses.get(record.task_id, (None, None))
                node.private_ip = private_ip or node.private_ip
                node.public_ip = public_ip or node.public_ip
                fresh[record.task_id] = node
            # A node this process has just created may not be in the listing yet.
            # It is kept on trust, but not for ever: one that ended before any
            # listing showed it must not be counted as on its way for all time.
            for task_id, node in self._nodes.items():
                if (
                    not node.seen
                    and task_id not in listed
                    and node.created_at is not None
                    and now - node.created_at <= UNLISTED_NODE_TRUST
                ):
                    fresh[task_id] = node
            self._nodes = fresh
            # Forgotten nodes stay forgotten until the platform stops listing them
            self._forgotten &= listed
            for task_id in list(self._overlay):
                if task_id not in fresh:
                    del self._overlay[task_id]

    def created_here(self, task_id: str, tags: Mapping[str, str], role: str) -> None:
        """Record a node this process has just created, before any refresh."""
        with self._lock:
            self._forgotten.discard(task_id)
            self._nodes[task_id] = _Node(
                role=role,
                status=TaskStatus.READY,
                creation_tags=dict(tags),
                created_here=True,
                seen=False,
                created_at=self.clock.now(),
            )

    def set_address(
        self, task_id: str, private_ip: str | None, public_ip: str | None
    ) -> None:
        with self._lock:
            node = self._nodes.get(task_id)
            if node is not None:
                node.private_ip = private_ip or node.private_ip
                node.public_ip = public_ip or node.public_ip
                node.status = TaskStatus.EXECUTING
                node.executing_since = node.executing_since or self.clock.now()

    def forget(self, task_ids: Iterable[str]) -> None:
        """These nodes have been told to stop: they are gone as far as Ray goes."""
        with self._lock:
            for task_id in task_ids:
                self._forgotten.add(task_id)
                self._nodes.pop(task_id, None)
                self._overlay.pop(task_id, None)

    def clear(self) -> None:
        with self._lock:
            self._forgotten.update(self._nodes)
            self._nodes.clear()
            self._overlay.clear()

    def set_tags(self, task_id: str, tags: Mapping[str, str]) -> None:
        with self._lock:
            if task_id in self._nodes:
                self._overlay.setdefault(task_id, {}).update(tags)

    # --- questions ---------------------------------------------------------

    def ids(self, tag_filters: Mapping[str, str]) -> list[str]:
        with self._lock:
            return [
                task_id
                for task_id in self._nodes
                if all(self._tags(task_id).get(k) == v for k, v in tag_filters.items())
            ]

    def tags(self, task_id: str) -> dict[str, str]:
        with self._lock:
            return self._tags(task_id) if task_id in self._nodes else {}

    def is_running(self, task_id: str) -> bool:
        with self._lock:
            node = self._nodes.get(task_id)
            return node is not None and node.status is TaskStatus.EXECUTING

    def is_terminated(self, task_id: str) -> bool:
        with self._lock:
            return task_id not in self._nodes

    def internal_ip(self, task_id: str) -> str | None:
        with self._lock:
            node = self._nodes.get(task_id)
            return node.private_ip if node else None

    def external_ip(self, task_id: str) -> str | None:
        with self._lock:
            node = self._nodes.get(task_id)
            return node.public_ip if node else None

    def head_id(self) -> str | None:
        with self._lock:
            return next(
                (i for i, n in self._nodes.items() if n.role == naming.HEAD_ROLE), None
            )

    def role(self, task_id: str) -> str | None:
        with self._lock:
            node = self._nodes.get(task_id)
            return node.role if node else None

    def needing_addresses(self) -> dict[str, list[str]]:
        """Running nodes whose private IP is not known yet, by role."""
        with self._lock:
            wanted: dict[str, list[str]] = {}
            for task_id, node in self._nodes.items():
                if node.status is TaskStatus.EXECUTING and not node.private_ip:
                    wanted.setdefault(node.role, []).append(task_id)
            return wanted

    def __len__(self) -> int:
        with self._lock:
            return len(self._nodes)

    # --- internals ---------------------------------------------------------

    def _tags(self, task_id: str) -> dict[str, str]:
        node = self._nodes[task_id]
        overlay = self._overlay.get(task_id, {})
        tags = {**node.creation_tags, **overlay}
        if TAG_NODE_STATUS not in overlay:
            tags[TAG_NODE_STATUS] = self._status(node)
        return tags

    def _status(self, node: _Node) -> str:
        """
        What Ray should believe about a node whose status Ray itself has not
        set in this process.

        A head made by this process keeps the status it was made with: Ray's own
        updater is about to take it through its stages. Anything else is reported
        up to date once it has been running for the grace period and can be
        reached: for a worker nobody else will ever say so, and Ray drops a worker
        that claims to be up to date before its Ray process can have started.
        """
        created_as = node.creation_tags.get(TAG_NODE_STATUS, STATUS_UNINITIALIZED)
        if node.created_here and node.role == naming.HEAD_ROLE:
            return created_as
        # The grace period is for workers: it gives a worker's Ray process time to
        # start before Ray expects to hear from it. A head is up to date as soon
        # as it is running, or other commands could not find it.
        grace = (
            timedelta(0) if node.role == naming.HEAD_ROLE else self.worker_start_grace
        )
        if (
            node.executing_since is not None
            and node.private_ip
            and self.clock.now() - node.executing_since >= grace
        ):
            return STATUS_UP_TO_DATE
        return created_as if created_as != STATUS_UP_TO_DATE else STATUS_UNINITIALIZED
