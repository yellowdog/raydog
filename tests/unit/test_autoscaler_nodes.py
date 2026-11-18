from __future__ import annotations

import threading
from datetime import timedelta

from yellowdog_ray.autoscaler.nodes import (
    STATUS_UNINITIALIZED,
    STATUS_UP_TO_DATE,
    TAG_NODE_KIND,
    TAG_NODE_STATUS,
    NodeTable,
)
from yellowdog_ray.core.cluster import NodeRecord
from yellowdog_ray.core.platform import TaskStatus
from yellowdog_ray.testing import FakeClock

HEAD_TAGS = {TAG_NODE_KIND: "head", TAG_NODE_STATUS: STATUS_UNINITIALIZED, "x": "1"}
WORKER_TAGS = {TAG_NODE_KIND: "worker", TAG_NODE_STATUS: STATUS_UNINITIALIZED}


def record(
    task_id: str,
    role: str = "w-small",
    status: TaskStatus = TaskStatus.EXECUTING,
    tags: dict[str, str] | None = None,
    stopping: bool = False,
) -> NodeRecord:
    return NodeRecord(
        task_id=task_id,
        role=role,
        status=status,
        tags=WORKER_TAGS if tags is None else tags,
        worker_id=f"w-{task_id}",
        error=None,
        stopping=stopping,
    )


def table(grace: float = 0) -> NodeTable:
    return NodeTable(FakeClock(), timedelta(seconds=grace))


def test_refresh_replaces_the_table_and_filters_work() -> None:
    t = table()
    t.refresh(
        [
            record("h", "head", tags=HEAD_TAGS),
            record("a"),
            record("b", status=TaskStatus.READY),
        ],
        {"h": ("10.0.0.1", "1.2.3.4"), "a": ("10.0.0.2", None)},
    )
    assert t.ids({}) == ["h", "a", "b"] and len(t) == 3
    assert t.ids({TAG_NODE_KIND: "worker"}) == ["a", "b"]
    assert t.ids({TAG_NODE_KIND: "head", "x": "1"}) == ["h"]
    assert t.ids({TAG_NODE_KIND: "head", "x": "2"}) == []
    assert t.head_id() == "h" and t.role("a") == "w-small" and t.role("zz") is None
    assert (t.internal_ip("h"), t.external_ip("h")) == ("10.0.0.1", "1.2.3.4")
    assert (t.internal_ip("a"), t.external_ip("a")) == ("10.0.0.2", None)
    assert t.internal_ip("b") is None and t.internal_ip("zz") is None
    assert t.is_running("a") and not t.is_running("b") and not t.is_running("zz")
    assert not t.is_terminated("b") and t.is_terminated("zz")
    t.refresh([record("a")], {})
    assert t.ids({}) == ["a"] and t.is_terminated("h") and t.head_id() is None
    assert t.internal_ip("a") == "10.0.0.2", "a known address is kept"


def test_a_node_seen_from_elsewhere_is_up_to_date_once_running_and_reachable() -> None:
    t = table()
    t.refresh([record("h", "head", tags=HEAD_TAGS), record("a")], {})
    assert t.tags("h")[TAG_NODE_STATUS] == STATUS_UNINITIALIZED  # no address yet
    t.set_address("h", "10.0.0.1", "1.2.3.4")
    assert t.tags("h")[TAG_NODE_STATUS] == STATUS_UP_TO_DATE
    assert t.tags("h")["x"] == "1"
    t.refresh(
        [record("h", "head", tags=HEAD_TAGS), record("a", status=TaskStatus.READY)], {}
    )
    assert t.tags("a")[TAG_NODE_STATUS] == STATUS_UNINITIALIZED
    assert t.ids({TAG_NODE_STATUS: STATUS_UP_TO_DATE}) == ["h"]


def test_a_head_made_here_keeps_its_status_until_ray_changes_it() -> None:
    t = table()
    t.created_here("h", HEAD_TAGS, "head")
    assert t.ids(HEAD_TAGS) == ["h"], "listed before any refresh shows it"
    t.set_address("h", "10.0.0.1", "1.2.3.4")
    t.refresh([record("h", "head", tags=HEAD_TAGS)], {})
    assert t.tags("h")[TAG_NODE_STATUS] == STATUS_UNINITIALIZED
    assert t.ids(HEAD_TAGS) == ["h"]
    t.set_tags("h", {TAG_NODE_STATUS: "waiting-for-ssh"})
    assert t.tags("h")[TAG_NODE_STATUS] == "waiting-for-ssh"
    t.set_tags("h", {TAG_NODE_STATUS: STATUS_UP_TO_DATE, "ray-runtime-config": "rt"})
    t.refresh([record("h", "head", tags=HEAD_TAGS)], {})
    assert t.tags("h") == {**HEAD_TAGS, TAG_NODE_STATUS: STATUS_UP_TO_DATE,
                           "ray-runtime-config": "rt"}  # fmt: skip


def test_a_worker_made_here_becomes_up_to_date_by_itself() -> None:
    """With Ray's updaters off, nobody else will ever say a worker is ready."""
    t = table()
    t.created_here("a", WORKER_TAGS, "w-small")
    assert t.tags("a")[TAG_NODE_STATUS] == STATUS_UNINITIALIZED
    t.refresh([record("a", status=TaskStatus.READY)], {})
    assert t.tags("a")[TAG_NODE_STATUS] == STATUS_UNINITIALIZED
    t.refresh([record("a")], {"a": ("10.0.0.2", None)})
    assert t.tags("a")[TAG_NODE_STATUS] == STATUS_UP_TO_DATE


def test_worker_start_grace_delays_up_to_date() -> None:
    t = table(grace=45)
    clock = t.clock
    assert isinstance(clock, FakeClock)
    t.refresh([record("a", status=TaskStatus.READY)], {})
    clock.advance(100)  # time spent waiting to start does not count
    t.refresh([record("a")], {"a": ("10.0.0.2", None)})
    assert t.tags("a")[TAG_NODE_STATUS] == STATUS_UNINITIALIZED
    clock.advance(44)
    t.refresh([record("a")], {})
    assert t.tags("a")[TAG_NODE_STATUS] == STATUS_UNINITIALIZED
    clock.advance(1)
    assert t.tags("a")[TAG_NODE_STATUS] == STATUS_UP_TO_DATE


def test_a_node_created_here_survives_a_listing_that_predates_it() -> None:
    t = table()
    t.refresh([record("old")], {})
    t.created_here("new", WORKER_TAGS, "w-small")
    t.refresh([record("old")], {})  # the platform has not listed it yet
    assert t.ids({}) == ["old", "new"]
    t.refresh([record("old"), record("new", status=TaskStatus.READY)], {})
    t.refresh([record("old")], {})  # listed, then gone: it failed
    assert t.ids({}) == ["old"]


def test_forgotten_and_stopping_nodes_are_gone_at_once() -> None:
    t = table()
    t.refresh([record("a"), record("b"), record("c", stopping=True)], {})
    assert t.ids({}) == ["a", "b"]
    t.set_tags("a", {"k": "v"})
    t.forget(["a", "never-known"])
    assert t.ids({}) == ["b"] and t.is_terminated("a") and t.tags("a") == {}
    t.refresh([record("a"), record("b")], {})  # the platform still lists it
    assert t.ids({}) == ["b"]
    t.refresh([record("b")], {})
    t.refresh([record("a"), record("b")], {})  # a different node may reuse nothing,
    assert t.ids({}) == ["a", "b"]  # but once unlisted it is no longer suppressed
    assert "k" not in t.tags("a")


def test_tags_for_unknown_nodes_are_ignored_and_clear_empties() -> None:
    t = table()
    t.set_tags("ghost", {"k": "v"})
    t.refresh([record("ghost")], {})
    assert "k" not in t.tags("ghost")
    t.clear()
    assert t.ids({}) == [] and len(t) == 0
    t.refresh([record("ghost")], {})
    assert t.ids({}) == [], "cleared nodes stay gone while still listed"


def test_needing_addresses() -> None:
    t = table()
    t.refresh(
        [
            record("h", "head"),
            record("a"),
            record("b"),
            record("c", status=TaskStatus.READY),
        ],
        {"a": ("10.0.0.2", None)},
    )
    assert t.needing_addresses() == {"head": ["h"], "w-small": ["b"]}


def test_many_threads() -> None:
    t = table()
    errors: list[BaseException] = []
    stop = threading.Event()

    def worker(n: int) -> None:
        try:
            for i in range(300):
                task_id = f"t{(n * 7 + i) % 20}"
                t.refresh(
                    [record(f"t{j}") for j in range((n + i) % 20)],
                    {f"t{j}": (f"10.0.0.{j}", None) for j in range(5)},
                )
                t.created_here(f"new{n}-{i % 5}", WORKER_TAGS, "w-small")
                t.set_tags(task_id, {"k": str(i)})
                t.ids({TAG_NODE_KIND: "worker"})
                t.tags(task_id)
                t.internal_ip(task_id)
                t.is_running(task_id)
                t.needing_addresses()
                t.forget([f"t{i % 20}"])
                t.set_address(task_id, "10.0.0.9", None)
        except BaseException as e:
            errors.append(e)
        finally:
            stop.set()

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert not any(thread.is_alive() for thread in threads), "deadlock"
    assert errors == []
    for task_id in t.ids({}):
        assert TAG_NODE_STATUS in t.tags(task_id)


# --- added after the phase 5 review ------------------------------------------------


def test_grace_applies_to_workers_only() -> None:
    t = table(grace=120)
    t.refresh(
        [record("h", "head", tags=HEAD_TAGS), record("a")],
        {"h": ("10.0.0.1", "1.2.3.4"), "a": ("10.0.0.2", None)},
    )
    assert t.tags("h")[TAG_NODE_STATUS] == STATUS_UP_TO_DATE
    assert t.tags("a")[TAG_NODE_STATUS] == STATUS_UNINITIALIZED


def test_a_node_created_here_but_never_listed_is_dropped_after_a_while() -> None:
    t = table()
    clock = t.clock
    assert isinstance(clock, FakeClock)
    t.created_here("ghost", WORKER_TAGS, "w-small")
    clock.advance(30)
    t.refresh([], {})
    assert t.ids({}) == ["ghost"], "the platform may just be slow to list it"
    clock.advance(31)
    t.refresh([], {})
    assert t.ids({}) == []
