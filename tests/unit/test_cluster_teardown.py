"""ClusterCore teardown and orphan handling against the in-memory platform."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest

from yellowdog_ray.core import naming
from yellowdog_ray.core.cluster import (
    ClusterCore,
    ClusterHandle,
    ClusterSettings,
    NodeSpec,
)
from yellowdog_ray.core.platform import (
    PoolSpec,
    TaskGroupSpec,
    TaskSpec,
    WorkRequirementStatus,
)
from yellowdog_ray.errors import PlatformError, TeardownError
from yellowdog_ray.testing import FakePlatform

NS = "ns"
SETTINGS = ClusterSettings(name="demo", namespace=NS)
NODE = NodeSpec(compute_requirement_template="ns/tmpl", start_script="ray start")
WORKERS = naming.worker_role("small")
VERIFY = timedelta(minutes=5)


@pytest.fixture
def p() -> FakePlatform:
    return FakePlatform()


@pytest.fixture
def core(p: FakePlatform) -> ClusterCore:
    return ClusterCore(p, p.clock)


def running(
    core: ClusterCore, name: str = "demo", workers: int = 2, **settings: object
) -> ClusterHandle:
    handle = core.create(replace(SETTINGS, name=name, **settings))  # type: ignore[arg-type]
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    head = core.add_nodes(handle, naming.HEAD_ROLE, 1, "ray start --head")[0]
    core.wait_for_node(handle, head.id)
    if workers:
        core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=workers)
        for task in core.add_nodes(handle, WORKERS, workers, "ray start"):
            core.wait_for_node(handle, task.id)
    return handle


def test_tear_down_ends_everything_and_verifies(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = running(core)
    assert p.live_instances == 3
    core.tear_down(handle, verify_timeout=VERIFY)
    assert p.live_instances == 0
    wr = p.get_work_requirement(handle.work_requirement_id)
    assert wr.status in (
        WorkRequirementStatus.CANCELLING,
        WorkRequirementStatus.CANCELLED,
    )
    assert all(not x.status.is_live for x in p.list_pools(NS))
    core.tear_down(handle, verify_timeout=VERIFY)  # repeatable


def test_tear_down_without_verification_returns_at_once(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = running(core)
    before = p.clock.now()
    core.tear_down(handle)
    assert p.clock.now() == before
    assert p.live_instances == 3  # still on their way out
    p.clock.advance(120)
    assert p.live_instances == 0


def test_tear_down_after_the_work_requirement_has_ended(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = running(core)
    p.cancel_work_requirement(handle.work_requirement_id)
    p.clock.advance(60)
    assert p.live_instances == 3  # cancelling the work does not remove the pools
    core.tear_down(handle, verify_timeout=VERIFY)
    assert p.live_instances == 0


def test_tear_down_after_the_work_requirement_has_been_purged(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = running(core, workers=0)
    lingering = core.add_pool(handle, WORKERS, NODE, target=1, max_nodes=1)
    p.cancel_work_requirement(handle.work_requirement_id)
    p.clock.advance(60)
    p.purge_terminal()
    assert p.find_work_requirement(NS, handle.work_requirement_name) is None
    assert p.live_instance_count(lingering.id) == 1
    core.tear_down(handle, verify_timeout=VERIFY)
    assert p.live_instances == 0


def test_tear_down_of_a_dead_head_cluster(core: ClusterCore, p: FakePlatform) -> None:
    handle = running(core)
    head = core.status(handle).head
    assert head is not None
    p.fail_task(head.task_id)
    p.clock.advance(60)
    core.tear_down(ClusterCore(p, p.clock).attach("demo", NS), verify_timeout=VERIFY)
    assert p.live_instances == 0


def test_one_failing_pool_does_not_stop_the_rest(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = running(core)
    p.fail_next("remove_pool", PlatformError("boom"))
    with pytest.raises(TeardownError) as caught:
        core.tear_down(handle, verify_timeout=timedelta(seconds=90))
    failures = caught.value.failures
    assert any("boom" in str(error) for _, error in failures)
    assert any("still has 1 live instance" in str(error) for _, error in failures)
    assert p.live_instances == 1  # only the pool whose removal failed
    core.tear_down(handle, verify_timeout=VERIFY)
    assert p.live_instances == 0


def test_a_failing_cancel_still_removes_the_pools(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = running(core)
    p.fail_next("cancel_work_requirement", PlatformError("cannot cancel"))
    with pytest.raises(TeardownError, match="cannot cancel"):
        core.tear_down(handle, verify_timeout=VERIFY)
    assert p.live_instances == 0


def test_verification_reports_instances_that_will_not_go(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = running(core, workers=0)
    p.pool_removal = 3600  # instances linger for an hour
    with pytest.raises(TeardownError, match="still has 1 live instance"):
        core.tear_down(handle, verify_timeout=timedelta(minutes=2))


@pytest.mark.parametrize("other", ["demo2", "demo-ab12cd34", "dem", "my-demo"])
def test_tear_down_leaves_similarly_named_clusters_alone(
    core: ClusterCore, p: FakePlatform, other: str
) -> None:
    theirs = running(core, name=other, workers=1)
    mine = running(core, workers=1)
    core.tear_down(mine, verify_timeout=VERIFY)
    assert p.live_instances == 2
    status = core.status(theirs)
    assert status.head_alive and status.pools["w-small"].live_instances == 1
    assert core.find_orphans(NS) == []


def test_tear_down_is_not_fooled_by_a_shared_suffix(
    core: ClusterCore, p: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The platform's listing matches substrings, and "my-demo_x_head" contains
    "demo_x_": only exact parsing keeps the two clusters apart.
    """
    monkeypatch.setattr(naming, "new_suffix", lambda: "samesame")
    theirs = running(core, name="my-demo", workers=1)
    mine = running(core, workers=1)
    assert theirs.work_requirement_name.endswith(mine.work_requirement_name)
    core.tear_down(mine, verify_timeout=VERIFY)
    assert p.live_instances == 2
    assert core.status(theirs).head_alive
    core.tear_down(theirs, verify_timeout=VERIFY)
    assert p.live_instances == 0


# --- orphans -------------------------------------------------------------------------


def test_healthy_clusters_have_no_orphans(core: ClusterCore, p: FakePlatform) -> None:
    running(core)
    running(core, name="other", workers=0)
    p.clock.advance(7200)
    assert core.find_orphans(NS) == []


def test_pools_outliving_their_work_requirement_are_orphans(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = running(core)
    p.cancel_work_requirement(handle.work_requirement_id)
    p.clock.advance(60)
    orphans = core.find_orphans(NS)
    assert {o.kind for o in orphans} == {"pool"}
    assert {o.name for o in orphans} == {
        f"{handle.work_requirement_name}_head",
        f"{handle.work_requirement_name}_w-small",
    }
    assert all(o.cluster == "demo" for o in orphans)
    p.purge_terminal()  # the work requirement's record goes; the pools are still there
    assert len(core.find_orphans(NS)) == 2
    core.remove_orphans(orphans, verify_timeout=VERIFY)
    assert p.live_instances == 0
    assert core.find_orphans(NS) == []


def test_a_cluster_whose_head_never_started_is_an_orphan_after_the_build_timeout(
    core: ClusterCore, p: FakePlatform
) -> None:
    p.never_register("demo")
    handle = core.create(replace(SETTINGS, build_timeout=timedelta(minutes=5)))
    pool = core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    p.clock.advance(120)
    assert core.find_orphans(NS) == []  # still within its build timeout
    p.clock.advance(240)
    (orphan,) = core.find_orphans(NS)
    assert (orphan.kind, orphan.id, orphan.cluster) == (
        "work_requirement",
        handle.work_requirement_id,
        "demo",
    )
    assert "head" in orphan.reason
    core.remove_orphans([orphan], verify_timeout=VERIFY)
    assert p.live_instance_count(pool.id) == 0
    assert core.find_orphans(NS) == []


def test_a_cluster_whose_head_died_is_an_orphan(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = running(core, workers=1)
    head = core.status(handle).head
    assert head is not None
    p.clock.advance(3600)
    p.fail_task(head.task_id)
    p.clock.advance(60)
    (orphan,) = core.find_orphans(NS)
    assert orphan.kind == "work_requirement"
    core.remove_orphans([orphan], verify_timeout=VERIFY)
    assert p.live_instances == 0


def test_a_cluster_whose_head_ended_is_an_orphan_at_once(
    core: ClusterCore, p: FakePlatform
) -> None:
    """
    A head task that has ended will not start again, so there is nothing to wait
    for: the cluster is reported straight away, not after its build timeout.
    """
    handle = running(core, build_timeout=timedelta(minutes=30))
    head = core.status(handle).head
    assert head is not None
    p.fail_task(head.task_id)
    p.clock.advance(10)
    assert p.clock.now() - handle.created < timedelta(minutes=30)
    (orphan,) = core.find_orphans(NS)
    assert orphan.kind == "work_requirement" and orphan.cluster == "demo"
    assert "head node ended" in orphan.reason and "FAILED" in orphan.reason


def test_a_cluster_starting_its_head_is_not_an_orphan(
    core: ClusterCore, p: FakePlatform
) -> None:
    p.node_boot_time(3000)
    handle = core.create(replace(SETTINGS, build_timeout=timedelta(minutes=30)))
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    core.add_nodes(handle, naming.HEAD_ROLE, 1, "x")
    p.clock.advance(1200)  # slow, but still within its build timeout
    assert core.find_orphans(NS) == []


def test_foreign_resources_are_never_orphans(
    core: ClusterCore, p: FakePlatform
) -> None:
    def spec(name: str, **tags: str) -> PoolSpec:
        return PoolSpec(
            name=name, namespace=NS, template="t", worker_tag=name, target_instances=1,
            max_nodes=1, instance_tags=tags, idle_node_shutdown=timedelta(hours=9),
            idle_pool_shutdown=None,
        )  # fmt: skip

    # A pool whose name looks right but which carries no, or the wrong, ownership tags
    p.provision_pool(spec("demo_aaaaaaaa_head"))
    p.provision_pool(
        spec("demo_bbbbbbbb_head", **naming.instance_tags("other", "demo_bbbbbbbb"))
    )
    p.provision_pool(
        spec("demo_cccccccc_head", **naming.instance_tags("demo", "demo_zzzzzzzz"))
    )
    # v2-style names
    p.provision_pool(spec("raytest-00-head"))
    p.provision_pool(spec("demo-ab12cd34-small"))
    # A live work requirement that is not RayDog's, with no tasks
    wr = p.create_work_requirement(
        NS, "demo_dddddddd", None, [TaskGroupSpec(name="head", worker_tag="x")]
    )
    p.add_tasks(wr.task_groups[0].id, [TaskSpec(name="t", script="")])
    p.clock.advance(7200)
    assert core.find_orphans(NS) == []
    assert p.live_instances == 5


def test_orphans_in_another_namespace_are_not_reported(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = running(core, workers=0, namespace="elsewhere")
    p.cancel_work_requirement(handle.work_requirement_id)
    p.clock.advance(60)
    assert core.find_orphans(NS) == []
    assert len(core.find_orphans("elsewhere")) == 1


def test_remove_orphans_reports_what_it_could_not_remove(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = running(core, workers=1)
    p.cancel_work_requirement(handle.work_requirement_id)
    p.clock.advance(60)
    orphans = core.find_orphans(NS)
    p.fail_next("remove_pool", PlatformError("boom"))
    with pytest.raises(TeardownError):
        core.remove_orphans(orphans, verify_timeout=timedelta(seconds=90))
    assert p.live_instances == 1
    core.remove_orphans(core.find_orphans(NS), verify_timeout=VERIFY)
    assert p.live_instances == 0


# --- added after the phase 3 review ------------------------------------------------


def test_tear_down_removes_a_pool_it_cannot_parse(
    core: ClusterCore, p: FakePlatform
) -> None:
    """Anything named for this cluster's work requirement goes, whatever its tail."""
    handle = running(core, workers=0)
    odd = p.provision_pool(
        PoolSpec(
            name=handle.work_requirement_name + "_w_x",
            namespace=NS,
            template="t",
            worker_tag="x",
            target_instances=2,
            max_nodes=2,
            idle_node_shutdown=timedelta(hours=9),
            idle_pool_shutdown=None,
        )
    )
    core.tear_down(handle, verify_timeout=VERIFY)
    assert p.live_instance_count(odd.id) == 0 and p.live_instances == 0


def test_a_cluster_whose_head_never_starts_is_an_orphan(
    core: ClusterCore, p: FakePlatform
) -> None:
    """The stale-agent case: the head task exists but its node never registers."""
    p.never_register("demo")
    handle = core.create(replace(SETTINGS, build_timeout=timedelta(minutes=5)))
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    core.add_nodes(handle, naming.HEAD_ROLE, 1, "x")
    p.clock.advance(240)
    assert core.find_orphans(NS) == []
    p.clock.advance(120)
    (orphan,) = core.find_orphans(NS)
    assert orphan.kind == "work_requirement" and "head" in orphan.reason
    core.remove_orphans([orphan], verify_timeout=VERIFY)
    assert p.live_instances == 0


def test_a_cluster_past_its_lifetime_is_an_orphan(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = running(core, workers=1, lifetime=timedelta(hours=1))
    p.clock.advance(3500)
    assert core.find_orphans(NS) == []
    # Make the tasks outlive the deadline, as a node that never started would
    for task in p.list_tasks(handle.work_requirement_id, live_only=True):
        p._tasks[task.id].spec = replace(p._tasks[task.id].spec, timeout=None)
        p._tasks[task.id].group.spec = replace(
            p._tasks[task.id].group.spec, task_timeout=None
        )
    p.clock.advance(100 + 600)
    (orphan,) = core.find_orphans(NS)
    assert orphan.kind == "work_requirement" and "lifetime" in orphan.reason


def test_remove_orphans_spares_a_cluster_that_has_recovered(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(replace(SETTINGS, build_timeout=timedelta(minutes=2)))
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    p.clock.advance(300)
    orphans = core.find_orphans(NS)
    assert [o.kind for o in orphans] == ["work_requirement"]
    head = core.add_nodes(handle, naming.HEAD_ROLE, 1, "x")[0]  # late, but it arrives
    core.wait_for_node(handle, head.id)
    core.remove_orphans(orphans, verify_timeout=VERIFY)
    assert core.status(handle).head_alive and p.live_instances == 1


def test_remove_orphans_spares_a_pool_whose_cluster_is_live(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = running(core, workers=0)
    pool = p.find_pool(NS, handle.work_requirement_name + "_head")
    assert pool is not None
    from yellowdog_ray.core.cluster import Orphan

    stale = Orphan(
        kind="pool", id=pool.id, name=pool.name, cluster="demo", reason="stale"
    )
    core.remove_orphans([stale], verify_timeout=VERIFY)
    assert p.live_instances == 1 and core.status(handle).head_alive
