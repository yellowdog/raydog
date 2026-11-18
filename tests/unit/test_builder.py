"""RayDogCluster against the in-memory platform."""

from __future__ import annotations

import json
import logging
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from yellowdog_ray import ClusterSpec, NodeSpec, RayDogCluster
from yellowdog_ray.core import naming
from yellowdog_ray.core.platform import TaskStatus, WorkRequirementStatus
from yellowdog_ray.credentials import Credentials
from yellowdog_ray.errors import (
    BuildTimeoutError,
    ClusterExistsError,
    ClusterNotFoundError,
    ClusterStateError,
    ConfigError,
    NodeStartError,
    PlatformError,
    TeardownError,
)
from yellowdog_ray.testing import FakePlatform

NS = "ns"
HEAD = NodeSpec(compute_requirement_template="ns/head", start_script="ray start --head")
WORKER = NodeSpec(compute_requirement_template="ns/worker", start_script="ray start")
OBS = NodeSpec(compute_requirement_template="ns/obs", start_script="run-observability")
SPEC = ClusterSpec(name="demo", namespace=NS, head=HEAD)
BOOM = PlatformError("boom")


@pytest.fixture
def p() -> FakePlatform:
    return FakePlatform()


def cluster(p: FakePlatform, spec: ClusterSpec = SPEC) -> RayDogCluster:
    return RayDogCluster(spec, platform=p, clock=p.clock)


def env_of(p: FakePlatform, task_id: str) -> dict[str, str]:
    return dict(p.get_task(task_id).environment)


def tasks_by_role(p: FakePlatform, c: RayDogCluster) -> dict[str, list[str]]:
    assert c.handle is not None
    wr = p.get_work_requirement(c.handle.work_requirement_id)
    roles = {g.id: g.name for g in wr.task_groups}
    out: dict[str, list[str]] = {}
    for t in p.list_tasks(wr.id, live_only=True):
        out.setdefault(roles[t.task_group_id], []).append(t.id)
    return out


# --- spec -------------------------------------------------------------------------


def test_spec_settings_carry_every_field() -> None:
    spec = ClusterSpec(
        name="demo",
        namespace=NS,
        head=HEAD,
        tag="t",
        lifetime=timedelta(hours=2),
        build_timeout=timedelta(minutes=4),
        idle_shutdown=timedelta(seconds=30),
    )
    s = spec.settings()
    assert (s.name, s.namespace, s.tag) == ("demo", NS, "t")
    assert (s.lifetime, s.build_timeout, s.idle_shutdown) == (
        timedelta(hours=2),
        timedelta(minutes=4),
        timedelta(seconds=30),
    )


@pytest.mark.parametrize(
    "bad",
    [
        {"name": "Not_Valid"},
        {"namespace": ""},
        {"lifetime": timedelta(0)},
        {"build_timeout": timedelta(seconds=0.5)},
        {"head": replace(HEAD, start_script="  ")},
        {"head": replace(HEAD, compute_requirement_template="")},
        {"observability": replace(OBS, start_script="")},
    ],
)
def test_invalid_specs_are_rejected_before_anything_is_created(
    p: FakePlatform, bad: dict[str, object]
) -> None:
    spec = replace(SPEC, **bad)  # type: ignore[arg-type]
    with pytest.raises(ConfigError):
        spec.validate()
    with pytest.raises(ConfigError):
        cluster(p, spec).build()
    assert p.calls == []


# --- build ------------------------------------------------------------------------


def test_constructing_does_nothing(p: FakePlatform) -> None:
    c = cluster(p)
    assert p.calls == []
    assert (c.name, c.namespace) == ("demo", NS)
    assert c.handle is None and c.head is None and c.observability is None
    RayDogCluster(SPEC)  # and needs no credentials


def test_build_starts_a_head_and_returns_where_it_is(p: FakePlatform) -> None:
    c = cluster(p)
    head = c.build()
    assert head.private_ip.startswith("10.") and head.public_ip is not None
    assert p.get_task(head.task_id).status is TaskStatus.EXECUTING
    assert c.head == head and c.handle is not None
    assert p.live_instances == 1
    assert c.status().head_alive
    env = env_of(p, head.task_id)
    assert "OBSERVABILITY_HOST" not in env and env["RAYDOG_ROLE"] == "head"


def test_build_twice_is_refused(p: FakePlatform) -> None:
    c = cluster(p)
    c.build()
    with pytest.raises(ClusterStateError):
        c.build()
    assert p.live_instances == 1


def test_a_second_cluster_with_the_same_name_is_refused(p: FakePlatform) -> None:
    cluster(p).build()
    with pytest.raises(ClusterExistsError):
        cluster(p).build()
    assert p.live_instances == 1


def test_a_dead_cluster_with_the_same_name_is_replaced(
    p: FakePlatform, caplog: pytest.LogCaptureFixture
) -> None:
    """
    As ray up does: a cluster of this name whose head has ended is removed, with
    a warning, and the new one is built in its place.
    """
    old = cluster(p)
    old.build()
    assert old.handle is not None and old.head is not None
    old_id = old.handle.work_requirement_id
    p.fail_task(old.head.task_id)
    p.clock.advance(10)
    new = cluster(p)
    new.build()
    assert new.handle is not None and new.handle.work_requirement_id != old_id
    assert not p.get_work_requirement(old_id).status.is_live
    said = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("demo" in m and "removing it" in m for m in said), said
    p.clock.advance(300)
    assert p.live_instances == 1  # the new head only


def test_a_cluster_still_being_built_is_not_replaced(p: FakePlatform) -> None:
    """With no head task yet, inside its build timeout, it may be someone's build."""
    p.node_boot_time(3000)
    starting = RayDogCluster(SPEC, platform=p, clock=p.clock)
    core = starting._ensure_core()
    handle = core.create(SPEC.settings())
    core.add_pool(handle, naming.HEAD_ROLE, HEAD, target=1, max_nodes=1)
    with pytest.raises(ClusterExistsError):
        cluster(p).build()
    assert p.get_work_requirement(handle.work_requirement_id).status.is_live


def test_observability_node_starts_first_and_its_address_is_handed_on(
    p: FakePlatform,
) -> None:
    c = cluster(p, replace(SPEC, observability=OBS))
    c.add_worker_pool("small", WORKER, count=1)
    head = c.build()
    assert c.observability is not None
    obs_ip = c.observability.private_ip
    assert obs_ip != head.private_ip
    assert env_of(p, head.task_id)["OBSERVABILITY_HOST"] == obs_ip
    roles = tasks_by_role(p, c)
    assert set(roles) == {"head", "observability", "w-small"}
    worker_env = env_of(p, roles["w-small"][0])
    assert worker_env["OBSERVABILITY_HOST"] == obs_ip
    assert worker_env["RAY_HEAD_IP"] == head.private_ip
    assert "RAY_HEAD_IP" not in env_of(p, roles["observability"][0])


def count_calls_of_a_clean_build(spec: ClusterSpec) -> int:
    p = FakePlatform()
    c = cluster(p, spec)
    c.add_worker_pool("small", WORKER, count=2)
    c.build()
    return len(p.calls)


FULL = replace(SPEC, observability=OBS)
PLATFORM_METHODS = [
    "create_work_requirement", "add_task_group", "get_work_requirement",
    "find_work_requirement", "list_work_requirements", "add_tasks", "get_task",
    "list_tasks", "provision_pool", "get_pool", "find_pool", "list_pools",
    "set_pool_target", "list_nodes", "public_ips",
]  # fmt: skip


def fail_nth_call(p: FakePlatform, nth: int, mode: str) -> dict[str, int]:
    """
    Make the n-th platform call fail, once. Modes: "before" (the request never
    arrives), "after" (it is carried out but the response is lost), "interrupt"
    (Ctrl-C arrives during it).
    """
    seen = {"n": 0, "failed": 0}

    def wrap(name: str) -> None:
        real = getattr(p, name)

        def call(*args: object, **kwargs: object) -> object:
            seen["n"] += 1
            if seen["n"] != nth + 1 or seen["failed"]:
                return real(*args, **kwargs)
            seen["failed"] = 1
            if mode == "after":
                real(*args, **kwargs)
            raise KeyboardInterrupt if mode == "interrupt" else BOOM

        setattr(p, name, call)

    for method in PLATFORM_METHODS:
        wrap(method)
    return seen


@pytest.mark.parametrize("mode", ["before", "after", "interrupt"])
@pytest.mark.parametrize("nth", range(count_calls_of_a_clean_build(FULL)))
def test_any_failure_during_build_leaves_nothing_running(nth: int, mode: str) -> None:
    """Fail the n-th platform call of a build, for every n, in three ways."""
    p = FakePlatform()
    c = cluster(p, FULL)
    c.add_worker_pool("small", WORKER, count=2)
    seen = fail_nth_call(p, nth, mode)
    try:
        c.build()
    except (PlatformError, KeyboardInterrupt):
        pass
    else:
        # A few calls may fail without failing the build (sizing a pool)
        c.shut_down()
    # Not long enough for idle nodes to be released by themselves: whatever is
    # gone was removed by the rollback
    p.clock.advance(100)
    assert p.live_instances == 0
    assert [x.name for x in p.list_pools(NS) if x.status.is_live] == []
    assert p.list_work_requirements(NS, live_only=True) == []
    assert seen["failed"] or nth >= seen["n"]
    if seen["failed"] and c.handle is None:
        p.clock.advance(60)
        cluster(p, FULL).build()  # the name is free again


def test_a_rollback_that_fails_keeps_the_cluster_within_reach(p: FakePlatform) -> None:
    """An outage that breaks the build also breaks its rollback."""
    c = cluster(p, replace(SPEC, idle_shutdown=timedelta(hours=2)))
    c.add_worker_pool("small", WORKER, count=2)
    real_add_tasks = p.add_tasks

    def outage_after_head_task(group_id: str, tasks: list[object]) -> list[object]:
        result = real_add_tasks(group_id, tasks)  # type: ignore[arg-type]
        p.fail_next("get_task", BOOM, times=50)
        p.fail_next("cancel_work_requirement", BOOM)
        p.fail_next("list_pools", BOOM)
        return result  # type: ignore[return-value]

    p.add_tasks = outage_after_head_task  # type: ignore[method-assign,assignment]
    with pytest.raises(PlatformError):
        c.build()
    assert p.live_instances == 3  # the rollback could not remove anything
    assert c.handle is not None  # ... so the cluster is not forgotten
    with pytest.raises(ClusterStateError, match="shut_down"):
        c.build()
    p._faults.clear()  # the outage ends
    c.shut_down(wait=True)
    assert p.live_instances == 0


def test_a_head_that_fails_to_start_rolls_the_build_back(p: FakePlatform) -> None:
    c = cluster(p)
    c.add_worker_pool("small", WORKER, count=2)
    real = p.add_tasks

    def add_then_fail(group_id: str, tasks: list[object]) -> list[object]:
        added = real(group_id, tasks)  # type: ignore[arg-type]
        p.clock.advance(120)
        p.fail_task(added[0].id)
        return added  # type: ignore[return-value]

    p.add_tasks = add_then_fail  # type: ignore[method-assign,assignment]
    with pytest.raises(NodeStartError):
        c.build()
    p.clock.advance(300)
    assert p.live_instances == 0 and c.handle is None and c.head is None


def test_a_head_that_never_registers_times_out_and_rolls_back(p: FakePlatform) -> None:
    p.never_register("demo")
    c = cluster(p, replace(SPEC, build_timeout=timedelta(minutes=3)))
    start = p.clock.now()
    with pytest.raises(BuildTimeoutError):
        c.build()
    assert p.clock.now() - start == timedelta(minutes=3)
    p.clock.advance(300)
    assert p.live_instances == 0
    assert p.list_work_requirements(NS, live_only=True) == []


def test_build_can_be_retried_after_a_rollback(p: FakePlatform) -> None:
    c = cluster(p)
    p.fail_next("provision_pool", BOOM)
    with pytest.raises(PlatformError):
        c.build()
    p.clock.advance(60)
    head = c.build()
    assert p.get_task(head.task_id).status is TaskStatus.EXECUTING


# --- shut down and context manager ------------------------------------------------


def test_shut_down_ends_the_cluster_and_is_idempotent(p: FakePlatform) -> None:
    c = cluster(p)
    c.add_worker_pool("small", WORKER, count=2)
    c.build()
    p.clock.advance(300)
    assert p.live_instances == 3
    c.shut_down(wait=True)
    assert p.live_instances == 0
    calls = len(p.calls)
    c.shut_down()
    c.shut_down(wait=True)  # already verified: nothing more to do
    assert len(p.calls) == calls
    with pytest.raises(ClusterStateError):
        c.build()
    with pytest.raises(ClusterStateError):
        c.add_worker_pool("more", WORKER, count=1)


def test_shut_down_without_wait_returns_at_once(p: FakePlatform) -> None:
    c = cluster(p)
    c.build()
    before = p.clock.now()
    c.shut_down()
    assert p.clock.now() == before
    p.clock.advance(120)
    assert p.live_instances == 0


def test_shut_down_of_a_cluster_never_built_does_nothing(p: FakePlatform) -> None:
    c = cluster(p)
    c.shut_down()
    assert p.calls == []


def test_a_failed_shut_down_can_be_repeated(p: FakePlatform) -> None:
    c = cluster(p)
    c.build()
    p.fail_next("remove_pool", BOOM)
    with pytest.raises(TeardownError):
        c.shut_down()
    c.shut_down(wait=True)
    assert p.live_instances == 0


def test_leaving_the_with_block_shuts_the_cluster_down(p: FakePlatform) -> None:
    with cluster(p) as c:
        c.build()
        assert p.live_instances == 1
    p.clock.advance(120)
    assert p.live_instances == 0


def test_an_exception_in_the_block_still_shuts_down_and_propagates(
    p: FakePlatform,
) -> None:
    with pytest.raises(RuntimeError, match="user code"), cluster(p) as c:
        c.build()
        raise RuntimeError("user code")
    p.clock.advance(120)
    assert p.live_instances == 0


def test_a_teardown_failure_does_not_mask_the_users_exception(
    p: FakePlatform, caplog: pytest.LogCaptureFixture
) -> None:
    with (
        caplog.at_level(logging.ERROR),
        pytest.raises(RuntimeError, match="user code"),
        cluster(p) as c,
    ):
        c.build()
        p.fail_next("remove_pool", BOOM)
        raise RuntimeError("user code")
    assert "boom" in caplog.text


def test_a_teardown_failure_alone_is_raised_from_the_block(p: FakePlatform) -> None:
    with pytest.raises(TeardownError), cluster(p) as c:
        c.build()
        p.fail_next("remove_pool", BOOM)


def test_detach_leaves_the_cluster_running(p: FakePlatform) -> None:
    with cluster(p) as c:
        c.build()
        c.detach()
    p.clock.advance(600)
    assert p.live_instances == 1
    assert (
        RayDogCluster.attach("demo", NS, platform=p, clock=p.clock).status().head_alive
    )


def test_status_needs_a_built_cluster(p: FakePlatform) -> None:
    with pytest.raises(ClusterStateError):
        cluster(p).status()


def test_a_platform_passed_in_is_not_closed_but_one_made_here_is(
    p: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    with cluster(p) as c:
        c.build()
    assert "close" not in [call.method for call in p.calls]

    made: list[FakePlatform] = []

    def connect(key_id: str, key_secret: str, url: str) -> FakePlatform:
        assert (key_id, key_secret, url) == ("id", "secret", "https://example")
        made.append(FakePlatform(p.clock))
        return made[-1]

    from yellowdog_ray.core import yellowdog

    monkeypatch.setattr(yellowdog.YellowDogPlatform, "connect", connect)
    creds = Credentials("id", "secret", "https://example")
    with RayDogCluster(SPEC, credentials=creds, clock=p.clock) as c:
        assert made == []  # not connected until needed
        c.build()
    assert made and made[0].calls[-1].method == "close"


# --- worker pools -----------------------------------------------------------------


def test_pools_added_before_build_boot_alongside_the_head(p: FakePlatform) -> None:
    c = cluster(p)
    pool = c.add_worker_pool("small", WORKER, count=2, max_count=4)
    assert (pool.name, pool.role, pool.pool_id, pool.count, pool.max_count) == (
        "small", "w-small", None, 2, 4,
    )  # fmt: skip
    assert p.calls == [] and set(c.worker_pools) == {"small"}
    seen_at_head_task: list[int] = []
    real = p.add_tasks

    def spy(group_id: str, tasks: list[object]) -> list[object]:
        seen_at_head_task.append(p.live_instances)
        return real(group_id, tasks)  # type: ignore[arg-type,return-value]

    p.add_tasks = spy  # type: ignore[method-assign,assignment]
    head = c.build()
    assert seen_at_head_task[0] == 3  # head + 2 workers already requested
    p.clock.advance(300)
    workers = tasks_by_role(p, c)["w-small"]
    assert len(workers) == 2
    assert all(env_of(p, t)["RAY_HEAD_IP"] == head.private_ip for t in workers)
    built = c.worker_pools["small"]
    assert built.pool_id is not None and (built.count, built.max_count) == (2, 4)


def test_add_worker_pool_to_a_running_cluster(p: FakePlatform) -> None:
    c = cluster(p)
    head = c.build()
    pool = c.add_worker_pool("big", WORKER, count=3)
    assert pool.pool_id is not None and (pool.count, pool.max_count) == (3, 3)
    p.clock.advance(300)
    workers = tasks_by_role(p, c)["w-big"]
    assert len(workers) == 3 and p.live_instances == 4
    assert env_of(p, workers[0])["RAY_HEAD_IP"] == head.private_ip


def test_an_empty_pool_can_be_added_and_grown_later(p: FakePlatform) -> None:
    c = cluster(p)
    c.build()
    pool = c.add_worker_pool("later", WORKER, count=0, max_count=2)
    assert (pool.count, pool.max_count) == (0, 2) and p.live_instances == 1
    grown = c.resize_worker_pool("later", 2)
    assert grown.count == 2
    p.clock.advance(300)
    assert p.live_instances == 3


@pytest.mark.parametrize(("count", "max_count"), [(-1, None), (3, 2), (0, 0), (2, -1)])
def test_impossible_pool_sizes_are_rejected(
    p: FakePlatform, count: int, max_count: int | None
) -> None:
    c = cluster(p)
    with pytest.raises(ConfigError):
        c.add_worker_pool("small", WORKER, count=count, max_count=max_count)
    c.build()
    with pytest.raises(ConfigError):
        c.add_worker_pool("small", WORKER, count=count, max_count=max_count)
    assert p.live_instances == 1 and set(c.worker_pools) == set()


def test_pool_names_must_be_distinct_even_after_sanitising(p: FakePlatform) -> None:
    c = cluster(p)
    c.add_worker_pool("small", WORKER, count=1)
    with pytest.raises(ConfigError, match="small"):
        c.add_worker_pool("small", WORKER, count=1)
    c.build()
    with pytest.raises(ConfigError, match="small"):
        c.add_worker_pool("small", WORKER, count=1)
    assert p.live_instances == 2
    with pytest.raises(ConfigError):
        c.add_worker_pool("", WORKER, count=1)


def test_a_failed_add_removes_the_half_made_pool(p: FakePlatform) -> None:
    c = cluster(p)
    c.build()
    p.fail_next("add_tasks", BOOM)
    with pytest.raises(PlatformError):
        c.add_worker_pool("small", WORKER, count=2)
    p.clock.advance(300)
    assert p.live_instances == 1 and "small" not in c.worker_pools


def test_resize_grows_and_shrinks_within_the_maximum(p: FakePlatform) -> None:
    c = cluster(p)
    c.build()
    c.add_worker_pool("small", WORKER, count=1, max_count=3)
    p.clock.advance(300)
    first = tasks_by_role(p, c)["w-small"]
    assert c.resize_worker_pool("small", 3).count == 3
    p.clock.advance(300)
    assert p.live_instances == 4
    with pytest.raises(ConfigError):
        c.resize_worker_pool("small", 4)
    with pytest.raises(ConfigError):
        c.resize_worker_pool("small", -1)
    c.resize_worker_pool("small", 1)
    p.clock.advance(60)
    # The newest nodes go first
    assert tasks_by_role(p, c)["w-small"] == first
    c.resize_worker_pool("small", 0)
    p.clock.advance(600)
    assert c.worker_pools["small"].count == 0 and p.live_instances == 1
    with pytest.raises(ClusterStateError):
        c.resize_worker_pool("unknown", 1)


def test_resize_before_build_changes_what_build_will_create(p: FakePlatform) -> None:
    c = cluster(p)
    c.add_worker_pool("small", WORKER, count=1, max_count=3)
    assert c.resize_worker_pool("small", 3).count == 3
    c.build()
    assert p.live_instances == 4


def test_remove_worker_pool(p: FakePlatform) -> None:
    c = cluster(p)
    c.add_worker_pool("gone-early", WORKER, count=5)
    c.remove_worker_pool("gone-early")
    c.add_worker_pool("small", WORKER, count=2)
    c.build()
    assert p.live_instances == 3
    c.remove_worker_pool("small")
    p.clock.advance(300)
    assert p.live_instances == 1 and "small" not in c.worker_pools
    with pytest.raises(ClusterStateError):
        c.remove_worker_pool("never-there")


def test_pools_come_and_go_on_a_running_cluster(p: FakePlatform) -> None:
    """Added, grown, shrunk and removed after the build, without touching the rest."""
    c = cluster(p)
    c.add_worker_pool("steady", WORKER, count=1)
    head = c.build()
    p.clock.advance(300)
    assert p.live_instances == 2

    c.add_worker_pool("extra", WORKER, count=1, max_count=3)
    c.resize_worker_pool("extra", 3)
    p.clock.advance(300)
    roles = tasks_by_role(p, c)
    assert len(roles["w-extra"]) == 3 and p.live_instances == 5
    assert {name: pool.count for name, pool in c.worker_pools.items()} == {
        "steady": 1,
        "extra": 3,
    }

    c.resize_worker_pool("extra", 1)
    c.remove_worker_pool("extra")
    p.clock.advance(300)
    assert set(c.worker_pools) == {"steady"} and p.live_instances == 2
    status = c.status()
    assert status.head_alive and c.head == head
    assert tasks_by_role(p, c)["w-steady"] == roles["w-steady"]
    with pytest.raises(ClusterStateError):
        c.resize_worker_pool("extra", 1)


def test_pools_cannot_be_added_when_the_head_is_dead(p: FakePlatform) -> None:
    c = cluster(p)
    head = c.build()
    p.fail_task(head.task_id)
    p.clock.advance(60)
    with pytest.raises(ClusterStateError, match="head"):
        c.add_worker_pool("small", WORKER, count=1)
    assert p.live_instances == 1
    c.shut_down(wait=True)
    assert p.live_instances == 0


# --- attach and saved state -------------------------------------------------------


def test_attach_by_name_gives_a_working_cluster(p: FakePlatform) -> None:
    original = cluster(p)
    original.add_worker_pool("small", WORKER, count=2, max_count=3)
    head = original.build()
    p.clock.advance(300)
    other = RayDogCluster.attach("demo", NS, platform=p, clock=p.clock)
    assert (other.name, other.namespace) == ("demo", NS)
    assert other.handle == original.handle
    assert other.head == head
    pools = other.worker_pools
    assert set(pools) == {"small"}
    assert (pools["small"].count, pools["small"].max_count) == (2, 3)
    other.resize_worker_pool("small", 1)
    with pytest.raises(ClusterStateError, match="start_script"):
        other.resize_worker_pool("small", 3)  # this process never saw its script
    other.resize_worker_pool("small", 3, start_script="ray start")
    other.add_worker_pool("extra", WORKER, count=1)
    p.clock.advance(300)
    assert p.live_instances == 5
    other.shut_down(wait=True)
    assert p.live_instances == 0


def test_attach_to_nothing_is_not_found(p: FakePlatform) -> None:
    with pytest.raises(ClusterNotFoundError):
        RayDogCluster.attach("demo", NS, platform=p, clock=p.clock)


def test_an_attached_cluster_with_observability_knows_its_address(
    p: FakePlatform,
) -> None:
    original = cluster(p, replace(SPEC, observability=OBS))
    original.build()
    other = RayDogCluster.attach("demo", NS, platform=p, clock=p.clock)
    assert other.observability == original.observability
    other.add_worker_pool("small", WORKER, count=1)
    worker = tasks_by_role(p, other)["w-small"][0]
    assert original.observability is not None
    assert env_of(p, worker)["OBSERVABILITY_HOST"] == original.observability.private_ip


def test_state_round_trips_through_string_file_and_mapping(
    p: FakePlatform, tmp_path: Path
) -> None:
    c = cluster(p)
    with pytest.raises(ClusterStateError):
        c.save_state()
    c.build()
    path = tmp_path / "state.json"
    text = c.save_state(path)
    assert path.read_text() == text
    state = json.loads(text)
    assert c.handle is not None
    assert state == {
        "raydog_state_version": 1,
        "cluster": "demo",
        "namespace": NS,
        "work_requirement_id": c.handle.work_requirement_id,
        "work_requirement_name": c.handle.work_requirement_name,
    }
    for source in (text, path, str(path), state):
        again = RayDogCluster.from_state(source, platform=p, clock=p.clock)
        assert again.handle == c.handle and again.head == c.head


def test_state_holds_the_api_url_but_never_a_secret(
    p: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    from yellowdog_ray.core import yellowdog

    monkeypatch.setattr(yellowdog.YellowDogPlatform, "connect", lambda *a: p)
    creds = Credentials("the-id", "the-secret", "https://example")
    c = RayDogCluster(SPEC, credentials=creds, clock=p.clock)
    c.build()
    text = c.save_state()
    assert json.loads(text)["api_url"] == "https://example"
    assert "the-secret" not in text and "the-id" not in text


def test_from_state_can_clean_up_after_a_cluster_has_ended(p: FakePlatform) -> None:
    c = cluster(p)
    c.add_worker_pool("small", WORKER, count=1)
    c.build()
    state = c.save_state()
    assert c.handle is not None
    p.cancel_work_requirement(c.handle.work_requirement_id)  # ended some other way
    p.clock.advance(60)
    assert p.live_instances == 2  # its pools are still up
    late = RayDogCluster.from_state(state, platform=p, clock=p.clock)
    assert late.status().work_requirement_status is WorkRequirementStatus.CANCELLED
    assert late.head is None
    late.shut_down(wait=True)
    assert p.live_instances == 0
    p.clock.advance(60)
    p.purge_terminal()
    with pytest.raises(ClusterNotFoundError):
        RayDogCluster.from_state(state, platform=p, clock=p.clock)


@pytest.mark.parametrize(
    "state",
    [
        "{not json",
        "[]",
        {"raydog_state_version": 2, "cluster": "demo", "namespace": NS,
         "work_requirement_id": "x", "work_requirement_name": "demo_aaaaaaaa"},
        {"raydog_state_version": 1, "cluster": "demo", "namespace": NS},
        {"cluster": "demo", "namespace": NS, "work_requirement_id": "x",
         "work_requirement_name": "demo_aaaaaaaa"},
        {"raydog_state_version": 1, "cluster": "demo", "namespace": NS,
         "work_requirement_id": 7, "work_requirement_name": "demo_aaaaaaaa"},
    ],
)  # fmt: skip
def test_bad_state_is_a_config_error(p: FakePlatform, state: object) -> None:
    with pytest.raises(ConfigError):
        RayDogCluster.from_state(state, platform=p, clock=p.clock)  # type: ignore[arg-type]
    assert p.calls == []


def test_worker_role_is_what_the_pool_is_called_on_the_platform(
    p: FakePlatform,
) -> None:
    c = cluster(p)
    c.build()
    pool = c.add_worker_pool("ray.worker.default", WORKER, count=1)
    assert pool.role == naming.worker_role("ray.worker.default")
    assert set(c.worker_pools) == {"ray.worker.default"}
    other = RayDogCluster.attach("demo", NS, platform=p, clock=p.clock)
    # Another process sees it under its platform-safe name
    assert set(other.worker_pools) == {pool.role.removeprefix("w-")}


# --- added after the phase 4 review ------------------------------------------------


def test_waiting_after_a_plain_shut_down_still_verifies(p: FakePlatform) -> None:
    c = cluster(p)
    c.add_worker_pool("small", WORKER, count=2)
    c.build()
    c.shut_down()
    assert p.live_instances == 3  # on their way out
    c.shut_down(wait=True)
    assert p.live_instances == 0


def test_waiting_after_the_with_block_still_verifies(p: FakePlatform) -> None:
    with cluster(p) as c:
        c.build()
    assert p.live_instances == 1
    c.shut_down(wait=True)
    assert p.live_instances == 0


@pytest.mark.parametrize("mode", ["before", "after", "interrupt"])
@pytest.mark.parametrize("nth", range(12))
def test_any_failure_adding_a_pool_leaves_no_extra_instances(
    p: FakePlatform, nth: int, mode: str
) -> None:
    c = cluster(p)
    c.build()
    seen = fail_nth_call(p, nth, mode)
    try:
        c.add_worker_pool("small", WORKER, count=3)
    except (PlatformError, KeyboardInterrupt):
        p.clock.advance(100)
        assert p.live_instances == 1, "only the head should be left"
        assert "small" not in c.worker_pools
    else:
        p.clock.advance(300)
        assert p.live_instances == 4
    assert seen["failed"] or nth >= seen["n"]
    c.shut_down(wait=True)
    assert p.live_instances == 0


def test_head_reflects_the_present_not_the_moment_of_the_build(p: FakePlatform) -> None:
    c = cluster(p)
    head = c.build()
    assert c.head == head
    p.fail_task(head.task_id)
    p.clock.advance(60)
    assert c.head is None
    assert c.handle is not None
    p.cancel_work_requirement(c.handle.work_requirement_id)
    p.clock.advance(60)
    p.purge_terminal()
    assert c.head is None and c.observability is None  # gone entirely: still no error


def test_a_pool_added_elsewhere_can_be_used_by_the_name_shown(p: FakePlatform) -> None:
    original = cluster(p)
    original.build()
    original.add_worker_pool("gpu_workers", WORKER, count=2, max_count=3)
    p.clock.advance(300)
    other = RayDogCluster.attach("demo", NS, platform=p, clock=p.clock)
    (shown,) = other.worker_pools
    assert shown != "gpu_workers"  # its platform-safe name
    assert other.resize_worker_pool(shown, 1).count == 1
    other.remove_worker_pool(shown)
    p.clock.advance(120)
    assert p.live_instances == 1


def test_a_shut_down_cluster_refuses_work_and_does_not_reconnect(
    p: FakePlatform,
) -> None:
    c = cluster(p)
    c.add_worker_pool("small", WORKER, count=1)
    c.build()
    c.shut_down(wait=True)
    calls = len(p.calls)
    assert dict(c.worker_pools) == {} and c.head is None and c.observability is None
    for action in (
        c.status,
        lambda: c.remove_worker_pool("small"),
        lambda: c.resize_worker_pool("small", 0),
        c.save_state,
    ):
        with pytest.raises(ClusterStateError):
            action()
    assert len(p.calls) == calls


def test_resizing_a_pool_that_has_been_removed_is_refused(p: FakePlatform) -> None:
    c = cluster(p)
    c.build()
    c.add_worker_pool("small", WORKER, count=2)
    other = RayDogCluster.attach("demo", NS, platform=p, clock=p.clock)
    other.remove_worker_pool("small")
    for count in (0, 1, 2):
        with pytest.raises(ClusterStateError, match="small"):
            c.resize_worker_pool("small", count)


def test_shrinking_stops_nodes_that_have_not_started_first(p: FakePlatform) -> None:
    c = cluster(p)
    c.build()
    c.add_worker_pool("small", WORKER, count=1, max_count=4)
    p.clock.advance(300)
    (running,) = tasks_by_role(p, c)["w-small"]
    c.resize_worker_pool("small", 4)  # three more, not yet started
    c.resize_worker_pool("small", 1)
    assert tasks_by_role(p, c)["w-small"] == [running]
    assert p.get_task(running).status is TaskStatus.EXECUTING


def test_a_failed_attach_closes_the_connection_it_opened(
    p: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    from yellowdog_ray.core import yellowdog

    monkeypatch.setattr(yellowdog.YellowDogPlatform, "connect", lambda *a: p)
    creds = Credentials("i", "s")
    with pytest.raises(ClusterNotFoundError):
        RayDogCluster.attach("demo", NS, credentials=creds, clock=p.clock)
    assert p.calls[-1].method == "close"


def test_state_cannot_redirect_credentials(
    p: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    from yellowdog_ray.core import yellowdog

    connected: list[tuple[object, ...]] = []

    def connect(*args: object) -> FakePlatform:
        connected.append(args)
        return p

    monkeypatch.setattr(yellowdog.YellowDogPlatform, "connect", connect)
    monkeypatch.setenv("YD_API_KEY_ID", "env-id")
    monkeypatch.setenv("YD_API_KEY_SECRET", "env-secret")
    monkeypatch.delenv("YD_API_URL", raising=False)
    c = cluster(p)
    c.build()
    state = json.loads(c.save_state())
    state["api_url"] = "http://attacker.example:8080/x"
    with pytest.raises(ConfigError, match=r"attacker\.example"):
        RayDogCluster.from_state(state, clock=p.clock)
    assert connected == []  # the key went nowhere


def test_state_that_does_not_match_the_platform_is_refused(p: FakePlatform) -> None:
    c = cluster(p)
    c.build()
    other = cluster(p, replace(SPEC, name="other"))
    other.build()
    assert other.handle is not None
    state = json.loads(c.save_state())
    edited = {**state, "work_requirement_id": other.handle.work_requirement_id}
    with pytest.raises(ConfigError, match="does not match"):
        RayDogCluster.from_state(edited, platform=p, clock=p.clock)
    with pytest.raises(ConfigError, match="does not match"):
        RayDogCluster.from_state(
            {**state, "namespace": "elsewhere"}, platform=p, clock=p.clock
        )
    assert p.live_instances == 2


@pytest.mark.parametrize(
    "state", [b"{}", 7, None, {"raydog_state_version": True, "cluster": "demo",
              "namespace": NS, "work_requirement_id": "x",
              "work_requirement_name": "demo_aaaaaaaa"}]
)  # fmt: skip
def test_more_bad_state_is_a_config_error(p: FakePlatform, state: object) -> None:
    with pytest.raises(ConfigError):
        RayDogCluster.from_state(state, platform=p, clock=p.clock)  # type: ignore[arg-type]


def test_state_from_awkward_files(p: FakePlatform, tmp_path: Path) -> None:
    c = cluster(p)
    c.build()
    odd = tmp_path / "{x}.json"
    c.save_state(odd)
    assert (
        RayDogCluster.from_state(str(odd), platform=p, clock=p.clock).handle == c.handle
    )
    binary = tmp_path / "binary"
    binary.write_bytes(b"\xff\xfe\x00")
    with pytest.raises(ConfigError):
        RayDogCluster.from_state(binary, platform=p, clock=p.clock)
