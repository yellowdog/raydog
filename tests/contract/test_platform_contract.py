"""
What RayDog relies on the platform to do, checked against the fake and the real one.

A failure against the real platform means the fake is wrong: fix the fake (and its
unit test), not the expectation here.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from tests.conftest import Harness
from yellowdog_ray.core.platform import (
    PoolStatus,
    TaskGroupSpec,
    TaskGroupStatus,
    TaskSpec,
    TaskStatus,
    WorkRequirementStatus,
)
from yellowdog_ray.errors import InvalidRequestError, NameInUseError, NotFoundError


def group(
    h: Harness, name: str = "head", worker_tag: str | None = None
) -> TaskGroupSpec:
    # A worker tag no pool has, unless the test says otherwise: tasks stay READY
    return TaskGroupSpec(
        name=name, worker_tag=worker_tag or h.name("nowhere"), tag="g-tag"
    )


def new_work_requirement(h: Harness, label: str = "wr", **kw: str) -> str:
    wr = h.platform.create_work_requirement(
        h.namespace, kw.get("name") or h.name(label), "raydog-test", [group(h)]
    )
    h.work_requirements.append(wr.id)
    return wr.id


# --- names ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "suffix", ["-Upper", ".dot", "-trailing-", "-trailing_", "-" + "a" * 60]
)
def test_invalid_work_requirement_names_are_rejected(
    harness: Harness, suffix: str
) -> None:
    with pytest.raises(InvalidRequestError):
        wr = harness.platform.create_work_requirement(
            harness.namespace, harness.name("bad") + suffix, None, [group(harness)]
        )
        harness.work_requirements.append(wr.id)


def test_invalid_pool_name_is_rejected(harness: Harness) -> None:
    with pytest.raises(InvalidRequestError):
        pool = harness.platform.provision_pool(
            harness.pool_spec(harness.name("bad") + "-trailing-")
        )
        harness.pools.append(pool.id)


def test_underscores_are_allowed_in_names(harness: Harness) -> None:
    name = harness.name("under") + "_x_y"
    wr_id = new_work_requirement(harness, name=name)
    assert harness.platform.get_work_requirement(wr_id).name == name


def test_work_requirement_name_stays_reserved_after_cancel(harness: Harness) -> None:
    p = harness.platform
    wr_id = new_work_requirement(harness)
    name = p.get_work_requirement(wr_id).name
    with pytest.raises(NameInUseError):
        p.create_work_requirement(harness.namespace, name, None, [group(harness)])
    p.cancel_work_requirement(wr_id)
    harness.wait_until(
        lambda: p.get_work_requirement(wr_id).status,
        lambda s: s is WorkRequirementStatus.CANCELLED,
        timeout_s=60,
    )
    with pytest.raises(NameInUseError):
        p.create_work_requirement(harness.namespace, name, None, [group(harness)])
    p.cancel_work_requirement(wr_id)  # repeatable


def test_task_and_group_names_are_unique_within_their_parent(harness: Harness) -> None:
    p = harness.platform
    wr_id = new_work_requirement(harness)
    wr = p.get_work_requirement(wr_id)
    with pytest.raises(NameInUseError):
        p.add_task_group(wr_id, group(harness, "head"))
    gid = wr.task_groups[0].id
    p.add_tasks(gid, [TaskSpec(name="n1", script="true")])
    with pytest.raises(NameInUseError):
        p.add_tasks(gid, [TaskSpec(name="n1", script="true")])


# --- lookup ----------------------------------------------------------------------


def test_find_is_exact_and_list_is_substring(harness: Harness) -> None:
    p = harness.platform
    base = harness.name("find")
    a = new_work_requirement(harness, name=base)
    new_work_requirement(harness, name=base + "-x")
    found = p.find_work_requirement(harness.namespace, base)
    assert found is not None and found.id == a
    assert found.task_groups[0].tag == "g-tag"
    assert p.find_work_requirement(harness.namespace, base + "-nope") is None
    listed = p.list_work_requirements(harness.namespace, name_contains=base)
    assert {w.name for w in listed} == {base, base + "-x"}
    live = p.list_work_requirements(
        harness.namespace, name_contains=base, live_only=True
    )
    assert {w.name for w in live} == {base, base + "-x"}


def test_get_unknown_raises_not_found(harness: Harness) -> None:
    p = harness.platform
    with pytest.raises(NotFoundError):
        p.get_work_requirement(
            "ydid:workreq:000000:00000000-0000-0000-0000-00000000dead"
        )


# --- task groups and tasks -------------------------------------------------------


def test_an_image_family_is_found_by_its_exact_name(harness: Harness) -> None:
    """
    RayDog turns ``images_id: namespace/name`` into the family's ID, which is the only
    form the platform accepts for a pool's image.
    """
    p = harness.platform
    assert p.find_image_family(harness.namespace, "raydog-no-such-family") is None
    assert p.find_image_family("raydog-no-such-namespace", "x") is None
    if harness.image_family is None:
        pytest.skip("needs RAYDOG_TEST_IMAGE_FAMILY (namespace/name of a family)")
    namespace, name = harness.image_family.split("/")
    family_id = p.find_image_family(namespace, name)
    assert family_id is not None and family_id.startswith("ydid:imgfam:")
    assert p.find_image_family(namespace, name[:-1]) is None, "exact, not a prefix"


def test_task_group_can_be_added_to_a_running_work_requirement(
    harness: Harness,
) -> None:
    p = harness.platform
    wr_id = new_work_requirement(harness)
    added = p.add_task_group(wr_id, group(harness, "workers"))
    assert added.status is TaskGroupStatus.RUNNING and added.tag == "g-tag"
    wr = p.get_work_requirement(wr_id)
    assert [g.name for g in wr.task_groups] == ["head", "workers"]
    assert wr.status is WorkRequirementStatus.RUNNING
    assert wr.tag == "raydog-test"


def test_unmatched_task_is_ready_and_carries_its_data(harness: Harness) -> None:
    p = harness.platform
    wr_id = new_work_requirement(harness)
    gid = p.get_work_requirement(wr_id).task_groups[0].id
    spec = TaskSpec(name="n1", script="true", environment={"A": "1"}, tag="kind=x")
    added = p.add_tasks(gid, [spec, TaskSpec(name="n2", script="true")])
    assert [t.name for t in added] == ["n1", "n2"]
    got = p.get_task(added[0].id)
    assert got.status is TaskStatus.READY and got.worker_id is None
    assert got.tag == "kind=x" and dict(got.environment) == {"A": "1"}
    assert got.task_group_id == gid
    listed = {t.name: t for t in p.list_tasks(wr_id)}
    assert set(listed) == {"n1", "n2"}
    assert dict(listed["n1"].environment) == {"A": "1"} and listed["n1"].tag == "kind=x"
    assert {t.name for t in p.list_tasks(wr_id, task_group_id=gid, live_only=True)} == {
        "n1",
        "n2",
    }


def test_cancel_unallocated_task_is_immediate_and_repeatable(harness: Harness) -> None:
    p = harness.platform
    wr_id = new_work_requirement(harness)
    gid = p.get_work_requirement(wr_id).task_groups[0].id
    a, b = p.add_tasks(
        gid, [TaskSpec(name="n1", script="true"), TaskSpec(name="n2", script="true")]
    )
    p.cancel_task(a.id)
    harness.wait_until(
        lambda: p.get_task(a.id).status,
        lambda s: s is TaskStatus.CANCELLED,
        timeout_s=15,
    )
    p.cancel_task(a.id)
    assert [t.name for t in p.list_tasks(wr_id, live_only=True)] == ["n2"]
    wr = p.get_work_requirement(wr_id)
    assert wr.status is WorkRequirementStatus.RUNNING
    assert wr.task_groups[0].status is TaskGroupStatus.RUNNING
    assert p.get_task(b.id).status is TaskStatus.READY


def test_cancelled_work_requirement_rejects_new_tasks(harness: Harness) -> None:
    p = harness.platform
    wr_id = new_work_requirement(harness)
    gid = p.get_work_requirement(wr_id).task_groups[0].id
    task = p.add_tasks(gid, [TaskSpec(name="n1", script="true")])[0]
    p.cancel_work_requirement(wr_id)
    harness.wait_until(
        lambda: p.get_work_requirement(wr_id).status,
        lambda s: s is WorkRequirementStatus.CANCELLED,
        timeout_s=60,
    )
    assert p.get_task(task.id).status is TaskStatus.CANCELLED
    assert (
        p.get_work_requirement(wr_id).task_groups[0].status is TaskGroupStatus.CANCELLED
    )
    with pytest.raises(InvalidRequestError):
        p.add_tasks(gid, [TaskSpec(name="late", script="true")])
    assert (
        p.list_work_requirements(
            harness.namespace,
            name_contains=p.get_work_requirement(wr_id).name,
            live_only=True,
        )
        == []
    )


# --- pools -----------------------------------------------------------------------


def test_empty_pool_lifecycle(harness: Harness) -> None:
    p = harness.platform
    name = harness.name("pool")
    created = p.provision_pool(
        harness.pool_spec(name, target=0, instance_tags={"raydog-cluster": "demo"})
    )
    harness.pools.append(created.id)
    assert created.name == name and created.status.is_live

    with pytest.raises(NameInUseError):
        p.provision_pool(harness.pool_spec(name))

    found = p.find_pool(harness.namespace, name)
    assert found is not None and found.id == created.id
    assert found.tag == "raydog-test" and found.target_instances == 0
    assert dict(found.instance_tags).get("raydog-cluster") == "demo"
    assert p.find_pool(harness.namespace, name + "-nope") is None
    assert name in {
        x.name for x in p.list_pools(harness.namespace, name_contains=name[:-2])
    }
    assert p.live_instance_count(created.id) == 0
    assert p.list_nodes(created.id) == []
    assert p.public_ips(created.id) == {}

    harness.wait_until(
        lambda: p.get_pool(created.id).status,
        lambda s: s is PoolStatus.EMPTY,
        timeout_s=90,
    )
    p.remove_pool(created.id)
    assert not p.get_pool(created.id).status.is_live
    p.remove_pool(created.id)  # repeatable
    harness.wait_until(
        lambda: p.get_pool(created.id).status,
        lambda s: s is PoolStatus.TERMINATED,
        timeout_s=180,
        step_s=5,
    )
    with pytest.raises(NameInUseError):
        p.provision_pool(harness.pool_spec(name))
    p.remove_pool(created.id)  # still repeatable once terminated


# --- with instances ----------------------------------------------------------------


def test_node_runs_tasks_and_removing_its_pool_ends_a_busy_instance(
    instance_harness: Harness,
) -> None:
    h = instance_harness
    p = h.platform
    name = h.name("node")
    backstop = timedelta(minutes=15)  # no task outlives this, whatever happens here
    nodes = TaskGroupSpec(name="nodes", worker_tag=name, task_timeout=backstop)
    wr = p.create_work_requirement(h.namespace, name, "raydog-test", [nodes])
    h.work_requirements.append(wr.id)
    pool = p.provision_pool(h.pool_spec(name, target=0, max_nodes=1))
    h.pools.append(pool.id)
    gid = wr.task_groups[0].id
    first = p.add_tasks(gid, [TaskSpec(name="n1", script="sleep 3000")])[0]

    # The pool grows from zero to meet the task
    h.wait_until(lambda: p.live_instance_count(pool.id), lambda n: n == 1, 120, 5)
    got = h.wait_until(
        lambda: p.get_task(first.id), lambda t: t.status is TaskStatus.EXECUTING, 600, 5
    )
    assert got.worker_id is not None
    node = next(n for n in p.list_nodes(pool.id) if got.worker_id in n.worker_ids)
    assert node.private_ip and node.instance_id
    assert p.get_pool(pool.id).status is PoolStatus.RUNNING
    assert set(p.public_ips(pool.id)) <= {node.instance_id}

    # Abort is asynchronous, and frees the node for the next task
    p.cancel_task(first.id)
    h.wait_until(
        lambda: p.get_task(first.id).status, lambda s: s is TaskStatus.ABORTED, 120, 2
    )
    second = p.add_tasks(gid, [TaskSpec(name="n2", script="sleep 3000")])[0]
    h.wait_until(
        lambda: p.get_task(second.id).status,
        lambda s: s is TaskStatus.EXECUTING,
        300,
        2,
    )
    assert p.live_instance_count(pool.id) == 1

    # Removing a pool ends its instance even while the node is running a task.
    # (Shutting the pool down alone would not: remove_pool must terminate compute.)
    p.remove_pool(pool.id)
    h.wait_until(lambda: p.live_instance_count(pool.id), lambda n: n == 0, 300, 5)
    ended = h.wait_until(
        lambda: p.get_task(second.id).status, lambda s: s.is_terminal, 300, 5
    )
    print(f"task on a removed pool ended as {ended.value}")
