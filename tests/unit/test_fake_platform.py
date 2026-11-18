"""One test per simulation rule of FakePlatform (see its module docstring)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from yellowdog_ray.core.platform import (
    PoolSpec,
    PoolStatus,
    TaskGroupSpec,
    TaskGroupStatus,
    TaskSpec,
    TaskStatus,
    WorkRequirementStatus,
)
from yellowdog_ray.errors import (
    InvalidRequestError,
    NameInUseError,
    NotFoundError,
    PlatformError,
)
from yellowdog_ray.testing import FakeClock, FakePlatform

NS = "ns"
LONG = timedelta(hours=1)


@pytest.fixture
def p() -> FakePlatform:
    return FakePlatform()


def group(name: str = "head", worker_tag: str = "tag-a", **kw: object) -> TaskGroupSpec:
    return TaskGroupSpec(name=name, worker_tag=worker_tag, **kw)  # type: ignore[arg-type]


def pool(
    name: str = "pool-a",
    worker_tag: str = "tag-a",
    target: int = 1,
    max_nodes: int = 1,
    **kw: object,
) -> PoolSpec:
    defaults: dict[str, object] = {
        "idle_node_shutdown": LONG,
        "idle_pool_shutdown": LONG,
    }
    defaults.update(kw)
    return PoolSpec(
        name=name,
        namespace=NS,
        template="ns/template",
        worker_tag=worker_tag,
        target_instances=target,
        max_nodes=max_nodes,
        **defaults,  # type: ignore[arg-type]
    )


def task(name: str = "n1", **kw: object) -> TaskSpec:
    return TaskSpec(name=name, script="sleep 1", **kw)  # type: ignore[arg-type]


def running_node(p: FakePlatform, n: int = 1) -> tuple[str, str, list[str]]:
    """A work requirement with one group, a pool of n nodes, and n executing tasks."""
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    pl = p.provision_pool(pool(target=n, max_nodes=n))
    tasks = p.add_tasks(wr.task_groups[0].id, [task(f"n{i}") for i in range(n)])
    p.clock.advance(200)
    assert all(p.get_task(t.id).status is TaskStatus.EXECUTING for t in tasks)
    return wr.id, pl.id, [t.id for t in tasks]


# --- names -----------------------------------------------------------------


@pytest.mark.parametrize(
    "bad",
    ["Upper", "dot.name", "sp ace", "trailing-", "trailing_", "1digit", "a" * 61, ""],
)
def test_invalid_names_are_rejected(p: FakePlatform, bad: str) -> None:
    with pytest.raises(InvalidRequestError):
        p.create_work_requirement(NS, bad, None, [group()])
    with pytest.raises(InvalidRequestError):
        p.provision_pool(pool(name=bad))


@pytest.mark.parametrize("good", ["a", "under_score", "a" * 60, "demo_ab12cd34_w-x"])
def test_valid_names_are_accepted(p: FakePlatform, good: str) -> None:
    p.create_work_requirement(NS, good, None, [group()])
    p.provision_pool(pool(name=good, target=0))


def test_work_requirement_names_are_unique_even_after_cancel(p: FakePlatform) -> None:
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    with pytest.raises(NameInUseError):
        p.create_work_requirement(NS, "wr-a", None, [group()])
    p.cancel_work_requirement(wr.id)
    p.clock.advance(30)
    assert p.get_work_requirement(wr.id).status is WorkRequirementStatus.CANCELLED
    with pytest.raises(NameInUseError):
        p.create_work_requirement(NS, "wr-a", None, [group()])
    p.create_work_requirement("other-ns", "wr-a", None, [group()])


def test_pool_names_are_unique_even_after_removal(p: FakePlatform) -> None:
    pl = p.provision_pool(pool(target=0))
    p.remove_pool(pl.id)
    p.clock.advance(120)
    assert p.get_pool(pl.id).status is PoolStatus.TERMINATED
    with pytest.raises(NameInUseError):
        p.provision_pool(pool(target=0))


def test_purge_frees_names_of_ended_objects(p: FakePlatform) -> None:
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    pl = p.provision_pool(pool(target=0))
    live = p.create_work_requirement(NS, "wr-live", None, [group()])
    p.cancel_work_requirement(wr.id)
    p.remove_pool(pl.id)
    p.clock.advance(120)
    p.purge_terminal()
    p.create_work_requirement(NS, "wr-a", None, [group()])
    p.provision_pool(pool(target=0))
    assert p.find_work_requirement(NS, "wr-live") is not None
    with pytest.raises(NotFoundError):
        p.get_work_requirement(wr.id)
    assert p.get_work_requirement(live.id).status is WorkRequirementStatus.RUNNING


def test_task_and_group_names_are_unique_within_their_parent(p: FakePlatform) -> None:
    with pytest.raises(InvalidRequestError):
        p.create_work_requirement(NS, "wr-dup", None, [group("g"), group("g")])
    wr = p.create_work_requirement(NS, "wr-a", None, [group("g")])
    with pytest.raises(NameInUseError):
        p.add_task_group(wr.id, group("g"))
    gid = wr.task_groups[0].id
    p.add_tasks(gid, [task("n1")])
    with pytest.raises(NameInUseError):
        p.add_tasks(gid, [task("n1")])
    with pytest.raises(NameInUseError):
        p.add_tasks(gid, [task("n2"), task("n2")])
    other = p.add_task_group(wr.id, group("g2"))
    p.add_tasks(other.id, [task("n1")])


def test_tags_are_limited_to_200_characters(p: FakePlatform) -> None:
    p.create_work_requirement(NS, "wr-ok", "x" * 200, [group(tag="y" * 200)])
    with pytest.raises(InvalidRequestError):
        p.create_work_requirement(NS, "wr-long", "x" * 201, [group()])
    with pytest.raises(InvalidRequestError):
        p.create_work_requirement(NS, "wr-long2", None, [group(tag="y" * 201)])
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    with pytest.raises(InvalidRequestError):
        p.add_tasks(wr.task_groups[0].id, [task(tag="z" * 201)])


# --- lookup ----------------------------------------------------------------


def test_find_is_exact_and_list_is_substring(p: FakePlatform) -> None:
    a = p.create_work_requirement(NS, "demo", "t", [group()])
    p.create_work_requirement(NS, "demo-x", "t", [group()])
    found = p.find_work_requirement(NS, "demo")
    assert found is not None and found.id == a.id
    assert p.find_work_requirement(NS, "dem") is None
    names = {w.name for w in p.list_work_requirements(NS, name_contains="demo")}
    assert names == {"demo", "demo-x"}
    p.provision_pool(pool(name="demo", target=0))
    p.provision_pool(pool(name="demo-x", target=0))
    fp = p.find_pool(NS, "demo")
    assert fp is not None and fp.name == "demo"
    assert p.find_pool(NS, "emo") is None
    assert {x.name for x in p.list_pools(NS, name_contains="emo")} == {"demo", "demo-x"}


def test_get_unknown_raises_not_found(p: FakePlatform) -> None:
    for call in (p.get_work_requirement, p.get_task, p.get_pool, p.list_nodes):
        with pytest.raises(NotFoundError):
            call("ydid:nope")


def test_live_only_filters(p: FakePlatform) -> None:
    a = p.create_work_requirement(NS, "wr-a", None, [group()])
    p.create_work_requirement(NS, "wr-b", None, [group()])
    t = p.add_tasks(a.task_groups[0].id, [task("n1"), task("n2")])
    p.cancel_task(t[0].id)
    assert [x.name for x in p.list_tasks(a.id, live_only=True)] == ["n2"]
    assert len(p.list_tasks(a.id)) == 2
    assert len(p.list_tasks(a.id, task_group_id=a.task_groups[0].id)) == 2
    p.cancel_work_requirement(a.id)
    p.clock.advance(30)
    assert [w.name for w in p.list_work_requirements(NS, live_only=True)] == ["wr-b"]
    assert len(p.list_work_requirements(NS)) == 2


def test_tags_environment_and_instance_tags_round_trip(p: FakePlatform) -> None:
    wr = p.create_work_requirement(
        NS, "wr-a", "user-tag", [group(tag="raydog:1:demo:head")]
    )
    assert wr.tag == "user-tag" and wr.task_groups[0].tag == "raydog:1:demo:head"
    t = p.add_tasks(wr.task_groups[0].id, [task(tag="k", environment={"A": "1"})])[0]
    got = p.get_task(t.id)
    assert got.tag == "k" and dict(got.environment) == {"A": "1"}
    assert got.task_group_id == wr.task_groups[0].id
    pl = p.provision_pool(
        pool(target=0, tag="user-tag", instance_tags={"raydog-cluster": "demo"})
    )
    info = p.get_pool(pl.id)
    assert info.tag == "user-tag" and dict(info.instance_tags) == {
        "raydog-cluster": "demo"
    }


def test_ids_look_like_the_platforms(p: FakePlatform) -> None:
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    t = p.add_tasks(wr.task_groups[0].id, [task()])[0]
    pl = p.provision_pool(pool(target=0))
    assert wr.id.startswith("ydid:workreq:")
    assert t.id.startswith("ydid:task:") and wr.id.split(":")[-1] in t.id
    assert pl.id.startswith("ydid:wrkrpool:")


# --- task and work requirement lifecycle -----------------------------------


def test_empty_work_requirement_stays_running(p: FakePlatform) -> None:
    wr = p.create_work_requirement(
        NS, "wr-a", None, [group(finish_if_all_tasks_finished=True)]
    )
    p.clock.advance(600)
    got = p.get_work_requirement(wr.id)
    assert got.status is WorkRequirementStatus.RUNNING
    assert got.task_groups[0].status is TaskGroupStatus.RUNNING


def test_unmatched_task_stays_ready(p: FakePlatform) -> None:
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    t = p.add_tasks(wr.task_groups[0].id, [task()])[0]
    assert t.status is TaskStatus.READY
    p.clock.advance(600)
    assert p.get_task(t.id).status is TaskStatus.READY


def test_task_runs_through_ready_allocated_executing(p: FakePlatform) -> None:
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    pl = p.provision_pool(pool())
    t = p.add_tasks(wr.task_groups[0].id, [task()])[0]
    seen: list[TaskStatus] = []
    for _ in range(200):
        p.clock.advance(1)
        s = p.get_task(t.id).status
        if not seen or seen[-1] is not s:
            seen.append(s)
    assert seen == [TaskStatus.READY, TaskStatus.ALLOCATED, TaskStatus.EXECUTING]
    got = p.get_task(t.id)
    nodes = p.list_nodes(pl.id)
    assert got.worker_id is not None
    assert (
        next(n for n in nodes if got.worker_id in n.worker_ids).private_ip is not None
    )
    assert p.get_pool(pl.id).status is PoolStatus.RUNNING


def test_task_only_runs_on_a_pool_with_its_worker_tag_and_namespace(
    p: FakePlatform,
) -> None:
    wr = p.create_work_requirement(NS, "wr-a", None, [group(worker_tag="tag-a")])
    p.provision_pool(pool(name="pool-b", worker_tag="tag-b"))
    t = p.add_tasks(wr.task_groups[0].id, [task()])[0]
    p.clock.advance(300)
    assert p.get_task(t.id).status is TaskStatus.READY


def test_public_ip_is_on_the_instance_not_the_node(p: FakePlatform) -> None:
    _, pool_id, _ = running_node(p)
    node = p.list_nodes(pool_id)[0]
    assert node.instance_id is not None
    assert node.instance_id in p.public_ips(pool_id)


def test_cancel_unallocated_task_is_immediate_and_repeatable(p: FakePlatform) -> None:
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    t = p.add_tasks(wr.task_groups[0].id, [task()])[0]
    p.cancel_task(t.id)
    assert p.get_task(t.id).status is TaskStatus.CANCELLED
    p.cancel_task(t.id)
    assert p.get_task(t.id).status is TaskStatus.CANCELLED


def test_cancel_executing_task_aborts_after_a_delay(p: FakePlatform) -> None:
    _, _, tasks = running_node(p)
    p.cancel_task(tasks[0])
    assert p.get_task(tasks[0]).status is TaskStatus.EXECUTING
    p.clock.advance(30)
    assert p.get_task(tasks[0]).status is TaskStatus.ABORTED
    p.cancel_task(tasks[0])


def test_cancel_work_requirement_is_two_step_and_repeatable(p: FakePlatform) -> None:
    wr_id, _, tasks = running_node(p)
    gid = p.get_work_requirement(wr_id).task_groups[0].id
    extra = p.add_tasks(gid, [task("unstarted")])[0]
    p.cancel_work_requirement(wr_id)
    assert p.get_work_requirement(wr_id).status is WorkRequirementStatus.CANCELLING
    assert p.get_task(extra.id).status is TaskStatus.CANCELLED
    assert p.get_task(tasks[0]).status is TaskStatus.EXECUTING
    p.clock.advance(60)
    got = p.get_work_requirement(wr_id)
    assert got.status is WorkRequirementStatus.CANCELLED
    assert got.task_groups[0].status is TaskGroupStatus.CANCELLED
    assert p.get_task(tasks[0]).status is TaskStatus.ABORTED
    p.cancel_work_requirement(wr_id)


def test_adding_to_an_ended_work_requirement_is_rejected(p: FakePlatform) -> None:
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    p.cancel_work_requirement(wr.id)
    p.clock.advance(30)
    with pytest.raises(InvalidRequestError):
        p.add_tasks(wr.task_groups[0].id, [task()])
    with pytest.raises(InvalidRequestError):
        p.add_task_group(wr.id, group("late"))


def test_task_group_can_be_added_to_a_running_work_requirement(p: FakePlatform) -> None:
    wr = p.create_work_requirement(NS, "wr-a", None, [group("head")])
    g = p.add_task_group(wr.id, group("workers", tag="t"))
    assert g.status is TaskGroupStatus.RUNNING and g.tag == "t"
    assert [x.name for x in p.get_work_requirement(wr.id).task_groups] == [
        "head",
        "workers",
    ]


def test_head_failure_leaves_other_groups_running(p: FakePlatform) -> None:
    wr = p.create_work_requirement(
        NS,
        "wr-a",
        None,
        [
            group("head", worker_tag="head", finish_if_any_task_failed=True),
            group("workers", worker_tag="w"),
        ],
    )
    p.provision_pool(pool(name="pool-h", worker_tag="head"))
    p.provision_pool(pool(name="pool-w", worker_tag="w"))
    head = p.add_tasks(wr.task_groups[0].id, [task("head")])[0]
    worker = p.add_tasks(wr.task_groups[1].id, [task("w1")])[0]
    p.clock.advance(200)
    p.fail_task(head.id)
    p.clock.advance(60)
    got = p.get_work_requirement(wr.id)
    assert p.get_task(head.id).status is TaskStatus.FAILED
    assert got.task_groups[0].status is TaskGroupStatus.FAILED
    assert got.task_groups[1].status is TaskGroupStatus.RUNNING
    assert p.get_task(worker.id).status is TaskStatus.EXECUTING
    assert got.status is WorkRequirementStatus.RUNNING


def test_failing_group_aborts_its_other_tasks(p: FakePlatform) -> None:
    wr = p.create_work_requirement(
        NS, "wr-a", None, [group(finish_if_any_task_failed=True)]
    )
    p.provision_pool(pool(target=2, max_nodes=2))
    a, b = p.add_tasks(wr.task_groups[0].id, [task("a"), task("b")])
    p.clock.advance(200)
    p.fail_task(a.id)
    p.clock.advance(60)
    assert p.get_task(b.id).status is TaskStatus.ABORTED
    with pytest.raises(InvalidRequestError):
        p.add_tasks(wr.task_groups[0].id, [task("c")])


def test_task_fails_at_its_timeout(p: FakePlatform) -> None:
    wr = p.create_work_requirement(
        NS, "wr-a", None, [group(task_timeout=timedelta(minutes=10))]
    )
    p.provision_pool(pool())
    grp, own = p.add_tasks(
        wr.task_groups[0].id,
        [task("uses-group-timeout"), task("own", timeout=timedelta(minutes=5))],
    )
    p.clock.advance(200)
    p.provision_pool(pool(name="pool-2"))
    p.clock.advance(200)
    assert p.get_task(grp.id).status is TaskStatus.EXECUTING
    p.clock.advance(300)
    assert p.get_task(own.id).status is TaskStatus.FAILED
    p.clock.advance(300)
    assert p.get_task(grp.id).status is TaskStatus.FAILED


def test_complete_task(p: FakePlatform) -> None:
    _, _, tasks = running_node(p)
    p.complete_task(tasks[0])
    assert p.get_task(tasks[0]).status is TaskStatus.COMPLETED


# --- pools -----------------------------------------------------------------


def test_pool_with_zero_target_is_pending_then_empty(p: FakePlatform) -> None:
    pl = p.provision_pool(pool(target=0))
    assert pl.status is PoolStatus.PENDING
    p.clock.advance(30)
    assert p.get_pool(pl.id).status is PoolStatus.EMPTY
    assert p.live_instance_count(pl.id) == 0


def test_pool_starts_its_target_instances(p: FakePlatform) -> None:
    pl = p.provision_pool(pool(target=3, max_nodes=5))
    assert p.live_instance_count(pl.id) == 3
    p.clock.advance(200)
    assert len(p.list_nodes(pl.id)) == 3
    assert p.get_pool(pl.id).status is PoolStatus.IDLE


def test_unpopulated_pool_grows_one_instance_first(p: FakePlatform) -> None:
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    pl = p.provision_pool(pool(target=0, max_nodes=2))
    tasks = p.add_tasks(wr.task_groups[0].id, [task(f"n{i}") for i in range(3)])
    p.clock.advance(10)
    assert p.live_instance_count(pl.id) == 1
    p.clock.advance(50)
    assert p.live_instance_count(pl.id) == 1
    p.clock.advance(240)
    assert p.live_instance_count(pl.id) == 2
    statuses = sorted(p.get_task(t.id).status.value for t in tasks)
    assert statuses == ["EXECUTING", "EXECUTING", "READY"]


def test_populated_pool_grows_to_demand_in_one_step(p: FakePlatform) -> None:
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    pl = p.provision_pool(pool(target=1, max_nodes=10))
    gid = wr.task_groups[0].id
    p.add_tasks(gid, [task("first")])
    p.clock.advance(200)
    p.add_tasks(gid, [task(f"n{i}") for i in range(5)])
    p.clock.advance(5)
    assert p.live_instance_count(pl.id) == 6


def test_set_pool_target_overrides_the_cautious_start(p: FakePlatform) -> None:
    pl = p.provision_pool(pool(target=0, max_nodes=4))
    p.set_pool_target(pl.id, 3)
    assert p.live_instance_count(pl.id) == 3
    assert p.get_pool(pl.id).target_instances == 3
    p.set_pool_target(pl.id, 99)
    assert p.live_instance_count(pl.id) == 4


def test_set_pool_target_down_never_stops_a_busy_node(p: FakePlatform) -> None:
    _, pool_id, tasks = running_node(p, n=2)
    p.cancel_task(tasks[0])
    p.clock.advance(30)
    p.set_pool_target(pool_id, 0)
    p.clock.advance(60)
    assert p.live_instance_count(pool_id) == 1
    assert p.get_task(tasks[1]).status is TaskStatus.EXECUTING


def test_idle_node_is_released(p: FakePlatform) -> None:
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    pl = p.provision_pool(pool(idle_node_shutdown=timedelta(seconds=0)))
    t = p.add_tasks(wr.task_groups[0].id, [task()])[0]
    p.clock.advance(200)
    assert p.live_instance_count(pl.id) == 1
    p.cancel_task(t.id)
    p.clock.advance(120)
    assert p.live_instance_count(pl.id) == 0
    assert p.get_pool(pl.id).status is PoolStatus.EMPTY


def test_released_pool_refills_when_a_task_arrives(p: FakePlatform) -> None:
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    pl = p.provision_pool(pool(idle_node_shutdown=timedelta(seconds=0)))
    gid = wr.task_groups[0].id
    t = p.add_tasks(gid, [task("a")])[0]
    p.clock.advance(200)
    p.cancel_task(t.id)
    p.clock.advance(120)
    again = p.add_tasks(gid, [task("b")])[0]
    p.clock.advance(200)
    assert p.get_task(again.id).status is TaskStatus.EXECUTING
    assert p.live_instance_count(pl.id) == 1


def test_idle_pool_shuts_itself_down(p: FakePlatform) -> None:
    pl = p.provision_pool(pool(target=0, idle_pool_shutdown=timedelta(minutes=3)))
    p.clock.advance(170)
    assert p.get_pool(pl.id).status.is_live
    p.clock.advance(120)
    assert not p.get_pool(pl.id).status.is_live


def test_remove_pool_ends_instances_and_is_repeatable(p: FakePlatform) -> None:
    _, pool_id, tasks = running_node(p)
    p.remove_pool(pool_id)
    assert p.get_pool(pool_id).status is PoolStatus.SHUTDOWN
    assert p.live_instance_count(pool_id) == 1
    p.remove_pool(pool_id)
    p.clock.advance(120)
    assert p.live_instance_count(pool_id) == 0
    assert p.get_pool(pool_id).status is PoolStatus.TERMINATED
    assert p.get_task(tasks[0]).status is TaskStatus.FAILED
    p.remove_pool(pool_id)


def test_removed_pool_runs_no_new_tasks(p: FakePlatform) -> None:
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    pl = p.provision_pool(pool())
    p.clock.advance(200)
    p.remove_pool(pl.id)
    t = p.add_tasks(wr.task_groups[0].id, [task()])[0]
    p.clock.advance(300)
    assert p.get_task(t.id).status is TaskStatus.READY
    assert p.live_instance_count(pl.id) == 0


# --- faults and inspection --------------------------------------------------


def test_never_register_leaves_instances_running_and_tasks_ready(
    p: FakePlatform,
) -> None:
    p.never_register("pool-a")
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    pl = p.provision_pool(pool())
    t = p.add_tasks(wr.task_groups[0].id, [task()])[0]
    p.clock.advance(600)
    assert p.get_task(t.id).status is TaskStatus.READY
    assert p.live_instance_count(pl.id) == 1
    assert p.list_nodes(pl.id) == []
    assert p.get_pool(pl.id).status is PoolStatus.EMPTY


def test_node_boot_time_is_adjustable(p: FakePlatform) -> None:
    p.node_boot_time(600)
    wr = p.create_work_requirement(NS, "wr-a", None, [group()])
    p.provision_pool(pool())
    t = p.add_tasks(wr.task_groups[0].id, [task()])[0]
    p.clock.advance(300)
    assert p.get_task(t.id).status is TaskStatus.READY
    p.clock.advance(400)
    assert p.get_task(t.id).status is TaskStatus.EXECUTING


def test_lose_node_fails_its_task_and_ends_the_instance(p: FakePlatform) -> None:
    _, pool_id, tasks = running_node(p)
    p.lose_node(tasks[0])
    assert p.get_task(tasks[0]).status is TaskStatus.FAILED
    assert p.live_instance_count(pool_id) == 0


def test_fail_next_raises_then_recovers(p: FakePlatform) -> None:
    p.fail_next("provision_pool", PlatformError("boom", retryable=True), times=2)
    for _ in range(2):
        with pytest.raises(PlatformError, match="boom"):
            p.provision_pool(pool(target=0))
    p.provision_pool(pool(target=0))


def test_fail_next_rejects_unknown_methods(p: FakePlatform) -> None:
    with pytest.raises(ValueError):
        p.fail_next("no_such_method", PlatformError("x"))


def test_calls_are_recorded(p: FakePlatform) -> None:
    p.create_work_requirement(NS, "wr-a", None, [group()])
    p.list_work_requirements(NS)
    assert [c.method for c in p.calls] == [
        "create_work_requirement",
        "list_work_requirements",
    ]
    assert p.calls[0].args[1] == "wr-a"


def test_settle_is_idempotent_without_time_passing(p: FakePlatform) -> None:
    wr_id, pool_id, tasks = running_node(p)
    before = (p.get_work_requirement(wr_id), p.get_pool(pool_id), p.get_task(tasks[0]))
    after = (p.get_work_requirement(wr_id), p.get_pool(pool_id), p.get_task(tasks[0]))
    assert before == after


def test_live_instances_counts_across_pools(p: FakePlatform) -> None:
    p.provision_pool(pool(name="pool-a", target=2, max_nodes=2))
    p.provision_pool(pool(name="pool-b", target=1, max_nodes=1))
    assert p.live_instances == 3


def test_shares_a_clock_when_given_one() -> None:
    clock = FakeClock()
    p = FakePlatform(clock)
    start = clock.now()
    clock.sleep(5)
    assert (clock.now() - start).total_seconds() == 5
    assert p.clock is clock


# --- added after the phase 2 review ----------------------------------------------


def test_idle_pool_shutdown_waits_while_the_pool_has_instances(p: FakePlatform) -> None:
    p.node_boot_time(300)
    pl = p.provision_pool(
        pool(target=2, max_nodes=2, idle_pool_shutdown=timedelta(minutes=3))
    )
    p.clock.advance(250)
    assert p.get_pool(pl.id).status.is_live
    assert p.live_instance_count(pl.id) == 2


def test_idle_pool_shuts_down_after_its_last_node_is_released(p: FakePlatform) -> None:
    pl = p.provision_pool(
        pool(
            idle_node_shutdown=timedelta(seconds=60),
            idle_pool_shutdown=timedelta(minutes=3),
        )
    )
    p.clock.advance(170)  # node registered at ~75s, released at ~135s, gone by ~155s
    assert p.live_instance_count(pl.id) == 0
    assert p.get_pool(pl.id).status.is_live
    p.clock.advance(200)
    assert not p.get_pool(pl.id).status.is_live


def test_live_instances_never_exceed_max_nodes(p: FakePlatform) -> None:
    pl = p.provision_pool(pool(target=3, max_nodes=3))
    p.clock.advance(200)
    p.set_pool_target(pl.id, 0)  # all three are now on their way out, but still alive
    p.set_pool_target(pl.id, 3)
    assert p.live_instance_count(pl.id) <= 3
    p.clock.advance(300)
    assert p.live_instance_count(pl.id) == 3


def test_lose_node_needs_a_task_that_is_on_a_node(p: FakePlatform) -> None:
    _, _, tasks = running_node(p)
    p.complete_task(tasks[0])
    with pytest.raises(ValueError):
        p.lose_node(tasks[0])


def test_removing_or_counting_an_unknown_pool_is_not_an_error(p: FakePlatform) -> None:
    p.remove_pool("ydid:wrkrpool:gone")
    assert p.live_instance_count("ydid:wrkrpool:gone") == 0


def test_list_pools_live_only_includes_shut_down_pools_with_instances(
    p: FakePlatform,
) -> None:
    _, pool_id, _ = running_node(p)
    gone = p.provision_pool(pool(name="pool-gone", target=0))
    p.remove_pool(gone.id)
    p.remove_pool(pool_id)
    assert {x.name for x in p.list_pools(NS, live_only=True)} == {"pool-a", "pool-gone"}
    p.clock.advance(120)
    assert p.list_pools(NS, live_only=True) == []
    assert len(p.list_pools(NS)) == 2


def test_failed_task_carries_an_error(p: FakePlatform) -> None:
    _, _, tasks = running_node(p)
    assert p.get_task(tasks[0]).error is None
    p.fail_task(tasks[0])
    assert p.get_task(tasks[0]).error


def test_pool_without_idle_shutdown_never_shuts_itself_down(p: FakePlatform) -> None:
    pl = p.provision_pool(pool(target=0, idle_pool_shutdown=None))
    p.clock.advance(7200)
    assert p.get_pool(pl.id).status.is_live


def test_create_response_time_is_more_precise_than_the_stored_one(
    p: FakePlatform,
) -> None:
    created = p.create_work_requirement(NS, "wr-a", None, [group()])
    stored = p.get_work_requirement(created.id)
    assert created.created != stored.created
    assert abs(created.created - stored.created) < timedelta(milliseconds=1)
    assert p.get_work_requirement(created.id).created == stored.created
    found = p.find_work_requirement(NS, "wr-a")
    assert found is not None and found.created == stored.created


def test_image_families_are_found_by_exact_namespace_and_name(p: FakePlatform) -> None:
    family_id = p.add_image_family("images", "ubuntu-amd64")
    assert family_id.startswith("ydid:imgfam:")
    assert p.find_image_family("images", "ubuntu-amd64") == family_id
    assert p.find_image_family("images", "ubuntu") is None
    assert p.find_image_family("other", "ubuntu-amd64") is None
    assert p.add_image_family("other", "ubuntu-amd64") != family_id
