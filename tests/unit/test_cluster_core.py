"""ClusterCore against the in-memory platform: everything except teardown."""

from __future__ import annotations

import json
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
    NodeInfo,
    PoolSpec,
    PoolStatus,
    TaskGroupSpec,
    TaskSpec,
    TaskStatus,
    WorkRequirementInfo,
    WorkRequirementStatus,
)
from yellowdog_ray.errors import (
    BuildTimeoutError,
    ClusterExistsError,
    ClusterNotFoundError,
    ClusterStateError,
    ConfigError,
    NodeStartError,
    PlatformError,
)
from yellowdog_ray.testing import FakePlatform

NS = "ns"
SETTINGS = ClusterSettings(name="demo", namespace=NS)
NODE = NodeSpec(compute_requirement_template="ns/tmpl", start_script="ray start")
WORKERS = naming.worker_role("small")
BOOM = PlatformError("boom", retryable=True)


@pytest.fixture
def p() -> FakePlatform:
    return FakePlatform()


@pytest.fixture
def core(p: FakePlatform) -> ClusterCore:
    return ClusterCore(p, p.clock)


def start_head(core: ClusterCore, handle: ClusterHandle) -> str:
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    head = core.add_nodes(handle, naming.HEAD_ROLE, 1, "ray start --head")[0]
    core.wait_for_node(handle, head.id)
    return head.id


def last_call(p: FakePlatform, method: str) -> tuple[object, ...]:
    return next(c.args for c in reversed(p.calls) if c.method == method)


# --- create ------------------------------------------------------------------------


def test_create_makes_a_marked_work_requirement(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(replace(SETTINGS, tag="user-tag"))
    parsed = naming.parse_work_requirement_name(handle.work_requirement_name)
    assert parsed is not None and parsed[0] == "demo"
    wr = p.get_work_requirement(handle.work_requirement_id)
    assert wr.name == handle.work_requirement_name and wr.tag == "user-tag"
    assert wr.status is WorkRequirementStatus.RUNNING
    assert [g.name for g in wr.task_groups] == ["head"]
    assert naming.parse_group_marker(wr.task_groups[0].tag) == (
        "demo",
        "head",
        {"l": 86400, "i": 180, "b": 600},
    )
    assert handle.created == wr.created
    assert handle.deadline == wr.created + timedelta(hours=24)
    assert p.live_instances == 0


def test_head_group_fails_as_a_whole_and_carries_the_lifetime(
    core: ClusterCore, p: FakePlatform
) -> None:
    core.create(SETTINGS)
    groups = last_call(p, "create_work_requirement")[3]
    assert isinstance(groups, list)
    (head,) = groups
    assert isinstance(head, TaskGroupSpec)
    assert head.finish_if_any_task_failed is True
    assert head.task_timeout == timedelta(hours=24)


def test_create_rejects_an_invalid_name_before_touching_the_platform(
    core: ClusterCore, p: FakePlatform
) -> None:
    with pytest.raises(ConfigError):
        core.create(replace(SETTINGS, name="Not_Valid"))
    assert p.calls == []


def test_create_refuses_a_name_that_is_live(core: ClusterCore, p: FakePlatform) -> None:
    first = core.create(SETTINGS)
    with pytest.raises(ClusterExistsError):
        core.create(SETTINGS)
    assert len(p.list_work_requirements(NS, live_only=True)) == 1
    p.cancel_work_requirement(first.work_requirement_id)
    p.clock.advance(30)
    again = core.create(SETTINGS)  # the same name, the same day
    assert again.work_requirement_name != first.work_requirement_name


def test_create_retries_on_a_suffix_collision(
    core: ClusterCore, p: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    suffixes = iter(["aaaaaaaa", "aaaaaaaa", "aaaaaaaa", "bbbbbbbb"])
    monkeypatch.setattr(naming, "new_suffix", lambda: next(suffixes))
    first = core.create(SETTINGS)
    p.cancel_work_requirement(first.work_requirement_id)
    p.clock.advance(30)
    second = core.create(SETTINGS)
    assert first.work_requirement_name == "demo_aaaaaaaa"
    assert second.work_requirement_name == "demo_bbbbbbbb"


def test_simultaneous_creates_leave_exactly_one_cluster(
    p: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    a, b = ClusterCore(p, p.clock), ClusterCore(p, p.clock)
    first = a.create(SETTINGS)
    # b looked before a's work requirement existed: its first check finds nothing
    real = b._live_clusters
    looks = []

    def stale_first_look(name: str, namespace: str) -> list[WorkRequirementInfo]:
        looks.append(name)
        return [] if len(looks) == 1 else real(name, namespace)

    monkeypatch.setattr(b, "_live_clusters", stale_first_look)
    with pytest.raises(ClusterExistsError):
        b.create(SETTINGS)
    p.clock.advance(30)
    live = p.list_work_requirements(NS, live_only=True)
    assert [w.id for w in live] == [first.work_requirement_id]


def test_create_failure_leaves_nothing(core: ClusterCore, p: FakePlatform) -> None:
    p.fail_next("create_work_requirement", BOOM)
    with pytest.raises(PlatformError):
        core.create(SETTINGS)
    assert p.list_work_requirements(NS) == [] and p.live_instances == 0
    core.create(SETTINGS)


# --- attach, handle_for, list --------------------------------------------------------


def test_attach_returns_the_same_handle_from_another_process(
    core: ClusterCore, p: FakePlatform
) -> None:
    settings = ClusterSettings(
        name="demo",
        namespace=NS,
        tag="t",
        lifetime=timedelta(hours=2),
        idle_shutdown=timedelta(seconds=30),
        build_timeout=timedelta(minutes=4),
    )
    created = core.create(settings)
    attached = ClusterCore(p, p.clock).attach("demo", NS)
    assert attached == created
    assert attached.settings == settings


def test_settings_with_no_lifetime_round_trip(
    core: ClusterCore, p: FakePlatform
) -> None:
    created = core.create(replace(SETTINGS, lifetime=None))
    assert created.deadline is None
    assert core.attach("demo", NS).settings.lifetime is None
    head = last_call(p, "create_work_requirement")[3][0]  # type: ignore[index]
    assert head.task_timeout is None


def test_attach_unknown_or_ended_cluster_is_not_found(
    core: ClusterCore, p: FakePlatform
) -> None:
    with pytest.raises(ClusterNotFoundError):
        core.attach("demo", NS)
    handle = core.create(SETTINGS)
    p.cancel_work_requirement(handle.work_requirement_id)
    p.clock.advance(30)
    with pytest.raises(ClusterNotFoundError):
        core.attach("demo", NS)
    with pytest.raises(ClusterNotFoundError):
        core.attach("demo", "other-namespace")


@pytest.mark.parametrize(
    "other", ["demo2", "demo-ab12cd34", "dem", "demo-x", "my-demo", "xdemo"]
)
def test_similar_names_are_different_clusters(
    core: ClusterCore, p: FakePlatform, other: str
) -> None:
    theirs = core.create(replace(SETTINGS, name=other))
    with pytest.raises(ClusterNotFoundError):
        core.attach("demo", NS)
    mine = core.create(SETTINGS)
    assert core.attach("demo", NS) == mine
    assert core.attach(other, NS) == theirs


def test_a_lookalike_without_the_marker_is_not_a_cluster(
    core: ClusterCore, p: FakePlatform
) -> None:
    p.create_work_requirement(
        NS, "demo_zzzzzzzz", None, [TaskGroupSpec(name="head", worker_tag="x")]
    )
    p.create_work_requirement(
        NS,
        "demo_yyyyyyyy",
        None,
        [
            TaskGroupSpec(
                name="head", worker_tag="x", tag=naming.group_marker("other", "head")
            )
        ],
    )
    with pytest.raises(ClusterNotFoundError):
        core.attach("demo", NS)
    assert core.list_clusters(NS) == []
    core.create(SETTINGS)  # and they do not block the name


def test_handle_for_works_on_an_ended_cluster(
    core: ClusterCore, p: FakePlatform
) -> None:
    created = core.create(SETTINGS)
    p.cancel_work_requirement(created.work_requirement_id)
    p.clock.advance(30)
    assert core.handle_for(created.work_requirement_id) == created
    foreign = p.create_work_requirement(
        NS, "plain", None, [TaskGroupSpec(name="g", worker_tag="x")]
    )
    with pytest.raises(ClusterNotFoundError):
        core.handle_for(foreign.id)
    with pytest.raises(ClusterNotFoundError):
        core.handle_for("ydid:workreq:nope")


def test_list_shows_raydog_clusters_only(core: ClusterCore, p: FakePlatform) -> None:
    a = core.create(SETTINGS)
    b = core.create(replace(SETTINGS, name="other"))
    p.create_work_requirement(
        NS, "plain", None, [TaskGroupSpec(name="g", worker_tag="x")]
    )
    core.create(replace(SETTINGS, namespace="elsewhere"))
    p.cancel_work_requirement(b.work_requirement_id)
    p.clock.advance(30)
    live = core.list_clusters(NS)
    assert [(c.name, c.work_requirement_id) for c in live] == [
        ("demo", a.work_requirement_id)
    ]
    everything = core.list_clusters(NS, include_ended=True)
    assert {c.name: c.status for c in everything} == {
        "demo": WorkRequirementStatus.RUNNING,
        "other": WorkRequirementStatus.CANCELLED,
    }


# --- pools ---------------------------------------------------------------------------


def test_add_pool_provisions_a_named_tagged_pool(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(
        replace(SETTINGS, tag="user-tag", idle_shutdown=timedelta(seconds=45))
    )
    node = NodeSpec(
        compute_requirement_template="ns/tmpl",
        start_script="x",
        images_id="ami-1",
        userdata="#!/bin/bash",
        instance_tags={"team": "research", "raydog-cluster": "forged"},
        metrics_enabled=True,
    )
    info = core.add_pool(handle, WORKERS, node, target=2, max_nodes=5)
    (spec,) = last_call(p, "provision_pool")
    assert isinstance(spec, PoolSpec)
    assert spec.name == f"{handle.work_requirement_name}_w-small" == info.name
    assert spec.worker_tag == spec.name and spec.namespace == NS
    assert (spec.template, spec.images_id, spec.userdata) == (
        "ns/tmpl",
        "ami-1",
        "#!/bin/bash",
    )
    assert (spec.target_instances, spec.max_nodes, spec.metrics_enabled) == (2, 5, True)
    assert spec.tag == "user-tag"
    assert dict(spec.instance_tags) == {
        "team": "research",
        "raydog-cluster": "demo",
        "raydog-work-requirement": handle.work_requirement_name,
    }
    assert spec.idle_node_shutdown == timedelta(seconds=45)
    assert spec.idle_pool_shutdown == timedelta(hours=24)
    wr = p.get_work_requirement(handle.work_requirement_id)
    group = wr.task_group("w-small")
    assert group is not None
    assert naming.parse_group_marker(group.tag) == ("demo", "w-small", {"m": 5})
    assert p.live_instance_count(info.id) == 2


def test_pool_of_an_unlimited_cluster_never_shuts_itself_down(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(replace(SETTINGS, lifetime=None))
    core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=1)
    (spec,) = last_call(p, "provision_pool")
    assert isinstance(spec, PoolSpec) and spec.idle_pool_shutdown is None


def test_head_pool_uses_the_existing_head_group(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    wr = p.get_work_requirement(handle.work_requirement_id)
    assert [g.name for g in wr.task_groups] == ["head"]


def test_a_role_has_one_pool(core: ClusterCore, p: FakePlatform) -> None:
    handle = core.create(SETTINGS)
    core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=1)
    with pytest.raises(ClusterStateError):
        core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=1)


@pytest.mark.parametrize(("target", "max_nodes"), [(3, 2), (0, 0), (-1, 2)])
def test_add_pool_rejects_impossible_sizes(
    core: ClusterCore, p: FakePlatform, target: int, max_nodes: int
) -> None:
    handle = core.create(SETTINGS)
    with pytest.raises(ConfigError):
        core.add_pool(handle, WORKERS, NODE, target=target, max_nodes=max_nodes)
    assert p.list_pools(NS) == []


def test_an_image_family_name_is_given_to_the_pool_as_its_id(
    core: ClusterCore, p: FakePlatform
) -> None:
    """The platform takes a family only by ID; ``namespace/name`` is looked up."""
    family_id = p.add_image_family("images", "ubuntu-amd64")
    handle = core.create(SETTINGS)
    node = replace(NODE, images_id="images/ubuntu-amd64")
    core.add_pool(handle, WORKERS, node, target=1, max_nodes=1)
    (spec,) = last_call(p, "provision_pool")
    assert isinstance(spec, PoolSpec)
    assert spec.images_id == family_id


@pytest.mark.parametrize(
    "images_id",
    [
        "ami-0123456789abcdef0",
        "ydid:imgfam:000000:BFB983:3d306f2d-83a6-4a79-b607-c73979e0dff2",
        "projects/my-project/global/images/ubuntu",  # GCP's own path
        "/subscriptions/x/resourceGroups/y/providers/Microsoft.Compute/images/z",
    ],
)
def test_any_other_images_id_is_passed_on_unchanged(
    core: ClusterCore, p: FakePlatform, images_id: str
) -> None:
    handle = core.create(SETTINGS)
    core.add_pool(
        handle, WORKERS, replace(NODE, images_id=images_id), target=1, max_nodes=1
    )
    (spec,) = last_call(p, "provision_pool")
    assert isinstance(spec, PoolSpec)
    assert spec.images_id == images_id
    assert not any(c.method == "find_image_family" for c in p.calls)


def test_an_unknown_image_family_is_refused_before_anything_is_added(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    calls = len(p.calls)
    node = replace(NODE, images_id="images/no-such-family")
    with pytest.raises(ConfigError, match=r"images_id 'images/no-such-family'"):
        core.add_pool(handle, WORKERS, node, target=1, max_nodes=1)
    written = [c.method for c in p.calls[calls:]]
    assert "add_task_group" not in written and "provision_pool" not in written
    assert p.list_pools(NS) == []


@pytest.mark.parametrize(
    "method", ["find_image_family", "add_task_group", "provision_pool"]
)
def test_add_pool_failure_leaves_nothing_running_and_can_be_retried(
    core: ClusterCore, p: FakePlatform, method: str
) -> None:
    p.add_image_family("images", "ubuntu-amd64")
    node = replace(NODE, images_id="images/ubuntu-amd64")
    handle = core.create(SETTINGS)
    p.fail_next(method, BOOM)
    with pytest.raises(PlatformError):
        core.add_pool(handle, WORKERS, node, target=2, max_nodes=2)
    assert p.live_instances == 0
    core.add_pool(handle, WORKERS, node, target=2, max_nodes=2)
    wr = p.get_work_requirement(handle.work_requirement_id)
    assert [g.name for g in wr.task_groups] == ["head", "w-small"]


def test_add_pool_to_an_ended_cluster_is_refused(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    p.cancel_work_requirement(handle.work_requirement_id)
    with pytest.raises(ClusterStateError):
        core.add_pool(handle, WORKERS, NODE, target=1, max_nodes=1)
    assert p.live_instances == 0


# --- nodes ---------------------------------------------------------------------------


def test_add_nodes_creates_marked_tasks_and_sizes_the_pool(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    pool = core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=10)
    p.clock.advance(3600)
    tasks = core.add_nodes(
        handle,
        WORKERS,
        3,
        "ray start --address=$RAY_HEAD_IP",
        environment={"RAY_HEAD_IP": "10.0.0.1"},
        node_tags={"ray-node-kind": "worker", "ray-user-node-type": "small"},
    )
    assert len({t.name for t in tasks}) == 3
    assert all(
        t.name.startswith("w-small-") and t.tag == "raydog:node:w-small" for t in tasks
    )
    env = dict(tasks[0].environment)
    assert env["RAY_HEAD_IP"] == "10.0.0.1"
    assert (env["RAYDOG_CLUSTER"], env["RAYDOG_ROLE"]) == ("demo", "w-small")
    assert json.loads(env["RAYDOG_NODE_TAGS"]) == {
        "ray-node-kind": "worker",
        "ray-user-node-type": "small",
    }
    specs = last_call(p, "add_tasks")[1]
    assert isinstance(specs, list) and isinstance(specs[0], TaskSpec)
    assert specs[0].script == "ray start --address=$RAY_HEAD_IP"
    assert specs[0].timeout == timedelta(hours=23)  # what is left of the lifetime
    # The whole request is asked for at once, not one instance at a time
    assert p.live_instance_count(pool.id) == 3


def test_nodes_of_an_unlimited_cluster_have_no_timeout(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(replace(SETTINGS, lifetime=None))
    core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=1)
    core.add_nodes(handle, WORKERS, 1, "x")
    assert last_call(p, "add_tasks")[1][0].timeout is None  # type: ignore[index]


def test_adding_more_nodes_sizes_the_pool_to_all_live_nodes(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    pool = core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=10)
    first = core.add_nodes(handle, WORKERS, 2, "x")
    p.clock.advance(300)
    core.remove_nodes(handle, [first[0].id])
    p.clock.advance(30)
    core.add_nodes(handle, WORKERS, 3, "x")
    assert last_call(p, "set_pool_target") == (pool.id, 4)


def test_add_nodes_survives_a_failure_to_size_the_pool(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=10)
    p.fail_next("set_pool_target", BOOM)
    tasks = core.add_nodes(handle, WORKERS, 2, "x")
    assert len(tasks) == 2
    p.clock.advance(600)  # the pool still grows by itself, one step at a time
    assert [n.status for n in core.nodes(handle)] == [TaskStatus.EXECUTING] * 2


def test_add_nodes_failure_starts_nothing(core: ClusterCore, p: FakePlatform) -> None:
    handle = core.create(SETTINGS)
    core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=10)
    p.fail_next("add_tasks", BOOM)
    with pytest.raises(PlatformError):
        core.add_nodes(handle, WORKERS, 3, "x")
    p.clock.advance(300)
    assert p.live_instances == 0 and core.nodes(handle) == []


def test_there_is_only_one_head(core: ClusterCore, p: FakePlatform) -> None:
    handle = core.create(SETTINGS)
    start_head(core, handle)
    with pytest.raises(ClusterStateError):
        core.add_nodes(handle, naming.HEAD_ROLE, 1, "ray start --head")
    with pytest.raises(ClusterStateError):
        core.add_nodes(replace(handle), naming.HEAD_ROLE, 2, "x")


def test_add_nodes_needs_a_known_role_and_a_positive_count(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    with pytest.raises(ClusterStateError):
        core.add_nodes(handle, WORKERS, 1, "x")
    core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=1)
    with pytest.raises(ConfigError):
        core.add_nodes(handle, WORKERS, 0, "x")


def test_a_cluster_past_its_deadline_takes_no_new_nodes(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(replace(SETTINGS, lifetime=timedelta(hours=1)))
    core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=1)
    p.clock.advance(3600)
    with pytest.raises(ClusterStateError, match="lifetime"):
        core.add_nodes(handle, WORKERS, 1, "x")
    assert core.nodes(handle) == []


def test_remove_nodes_cancels_them_all_even_if_one_fails(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=3)
    tasks = core.add_nodes(handle, WORKERS, 3, "x")
    p.clock.advance(400)
    p.fail_next("cancel_task", BOOM)
    with pytest.raises(PlatformError):
        core.remove_nodes(handle, [t.id for t in tasks])
    p.clock.advance(30)
    statuses = sorted(p.get_task(t.id).status.value for t in tasks)
    assert statuses == ["ABORTED", "ABORTED", "EXECUTING"]
    core.remove_nodes(handle, [t.id for t in tasks])
    p.clock.advance(30)
    assert core.nodes(handle) == []


def test_remove_pool_cancels_its_nodes_and_ends_its_instances(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    pool = core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=2)
    tasks = core.add_nodes(handle, WORKERS, 2, "x")
    p.clock.advance(400)
    core.remove_pool(handle, WORKERS)
    p.clock.advance(120)
    assert p.live_instance_count(pool.id) == 0
    assert p.get_pool(pool.id).status is PoolStatus.TERMINATED
    assert all(p.get_task(t.id).status.is_terminal for t in tasks)
    with pytest.raises(ClusterStateError):
        core.remove_pool(handle, naming.worker_role("never-added"))


# --- wait_for_node ------------------------------------------------------------


def test_wait_for_node_returns_its_addresses(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    head = core.add_nodes(handle, naming.HEAD_ROLE, 1, "ray start --head")[0]
    address = core.wait_for_node(handle, head.id)
    assert address.task_id == head.id
    assert address.private_ip.startswith("10.") and address.public_ip is not None
    assert p.get_task(head.id).status is TaskStatus.EXECUTING


def test_wait_for_node_without_a_public_ip(
    core: ClusterCore, p: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle = core.create(SETTINGS)
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    head = core.add_nodes(handle, naming.HEAD_ROLE, 1, "x")[0]
    monkeypatch.setattr(p, "public_ips", lambda pool_id: {})
    assert core.wait_for_node(handle, head.id).public_ip is None


def test_wait_for_node_reports_a_node_that_failed_to_start(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    head = core.add_nodes(handle, naming.HEAD_ROLE, 1, "x")[0]
    p.clock.advance(100)
    p.fail_task(head.id)
    with pytest.raises(NodeStartError, match=r"FAILED.*task failed"):
        core.wait_for_node(handle, head.id)


def test_wait_for_node_times_out_at_the_build_timeout(
    core: ClusterCore, p: FakePlatform
) -> None:
    p.never_register("demo")
    handle = core.create(replace(SETTINGS, build_timeout=timedelta(minutes=4)))
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    head = core.add_nodes(handle, naming.HEAD_ROLE, 1, "x")[0]
    start = p.clock.now()
    with pytest.raises(BuildTimeoutError):
        core.wait_for_node(handle, head.id)
    assert p.clock.now() - start == timedelta(minutes=4)
    with pytest.raises(BuildTimeoutError):
        core.wait_for_node(handle, head.id, timeout=timedelta(seconds=30))
    assert p.clock.now() - start == timedelta(minutes=4, seconds=30)


def test_a_node_that_never_registers_is_explained(
    core: ClusterCore, p: FakePlatform
) -> None:
    """
    The timeout says how far the node got, and how long it was given, in words a
    user can act on; RayDog's internal records are not part of it.
    """
    p.never_register("demo")
    handle = core.create(replace(SETTINGS, build_timeout=timedelta(minutes=4)))
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    head = core.add_nodes(handle, naming.HEAD_ROLE, 1, "x")[0]
    with pytest.raises(BuildTimeoutError) as caught:
        core.wait_for_node(handle, head.id)
    message = str(caught.value)
    assert "did not start within 4 minutes" in message
    assert "instance is running" in message and "never registered" in message
    assert "console output" in message
    assert caught.value.__cause__ is None and caught.value.__suppress_context__
    assert "TaskInfo" not in message


def test_a_node_with_no_instance_is_explained(
    core: ClusterCore, p: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    p.never_register("demo")
    handle = core.create(replace(SETTINGS, build_timeout=timedelta(seconds=90)))
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    head = core.add_nodes(handle, naming.HEAD_ROLE, 1, "x")[0]
    monkeypatch.setattr(p, "live_instance_count", lambda pool_id: 0)
    with pytest.raises(BuildTimeoutError, match="within 90 seconds") as caught:
        core.wait_for_node(handle, head.id)
    assert "no instance" in str(caught.value) and "portal" in str(caught.value)


def test_a_node_whose_agent_does_not_take_its_task_is_explained(
    core: ClusterCore, p: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Seen on an image with an Agent of its own: it registers at boot, but the task
    waits for the Agent the setup script installs.
    """
    p.never_register("demo")
    handle = core.create(replace(SETTINGS, build_timeout=timedelta(minutes=1)))
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    head = core.add_nodes(handle, naming.HEAD_ROLE, 1, "x")[0]
    registered = NodeInfo(
        id="n", pool_id="p", worker_ids=(), private_ip="10.0.0.1", instance_id="i"
    )
    monkeypatch.setattr(p, "list_nodes", lambda pool_id: [registered])
    with pytest.raises(BuildTimeoutError) as caught:
        core.wait_for_node(handle, head.id)
    message = str(caught.value)
    assert "within 1 minute:" in message and "did not take the node's task" in message
    assert "console output" in message


def test_the_explanation_does_not_hide_the_timeout_if_the_platform_fails(
    core: ClusterCore, p: FakePlatform
) -> None:
    """Looking for the reason is best effort; the timeout is still reported."""
    p.never_register("demo")
    handle = core.create(replace(SETTINGS, build_timeout=timedelta(minutes=2)))
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    head = core.add_nodes(handle, naming.HEAD_ROLE, 1, "x")[0]
    real = p.find_pool
    calls = {"n": 0}

    def failing_late(*args: object) -> object:
        calls["n"] += 1
        if p.clock.now() - handle.created >= timedelta(minutes=2):
            raise PlatformError("down")
        return real(*args)  # type: ignore[arg-type]

    p.find_pool = failing_late  # type: ignore[method-assign,assignment]
    with pytest.raises(BuildTimeoutError, match="did not start within 2 minutes"):
        core.wait_for_node(handle, head.id)


# --- nodes and status ------------------------------------------------------------


def test_nodes_lists_live_nodes_with_roles_and_tags(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    head_id = start_head(core, handle)
    core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=3)
    workers = core.add_nodes(
        handle, WORKERS, 2, "x", node_tags={"ray-node-kind": "worker"}
    )
    core.remove_nodes(handle, [workers[1].id])
    p.clock.advance(60)
    nodes = {n.task_id: n for n in core.nodes(handle)}
    assert set(nodes) == {head_id, workers[0].id}
    assert nodes[head_id].role == "head" and dict(nodes[head_id].tags) == {}
    assert nodes[workers[0].id].role == "w-small"
    assert dict(nodes[workers[0].id].tags) == {"ray-node-kind": "worker"}
    assert [n.task_id for n in core.nodes(handle, role="head")] == [head_id]


def test_status_of_a_healthy_cluster(core: ClusterCore, p: FakePlatform) -> None:
    handle = core.create(SETTINGS)
    head_id = start_head(core, handle)
    core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=3)
    core.add_nodes(handle, WORKERS, 2, "x")
    p.clock.advance(300)
    status = core.status(handle)
    assert status.work_requirement_status is WorkRequirementStatus.RUNNING
    assert status.head_alive and not status.past_deadline
    assert status.head is not None and status.head.task_id == head_id
    assert set(status.pools) == {"head", "w-small"}
    workers = status.pools["w-small"]
    assert workers.live_instances == 2 and workers.status is PoolStatus.RUNNING
    assert dict(workers.nodes_by_status) == {TaskStatus.EXECUTING: 2}
    assert status.pools["head"].live_instances == 1


def test_status_sees_a_dead_head_while_workers_run_on(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    head_id = start_head(core, handle)
    core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=1)
    core.add_nodes(handle, WORKERS, 1, "x")
    p.clock.advance(300)
    p.fail_task(head_id)
    p.clock.advance(60)
    status = core.status(handle)
    assert not status.head_alive
    assert status.head is not None and status.head.status is TaskStatus.FAILED
    assert status.head.error
    # The platform does not stop the workers: that is RayDog's job
    assert status.work_requirement_status is WorkRequirementStatus.RUNNING
    assert dict(status.pools["w-small"].nodes_by_status) == {TaskStatus.EXECUTING: 1}


def test_status_before_the_head_starts_and_after_the_deadline(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(replace(SETTINGS, lifetime=timedelta(minutes=30)))
    status = core.status(handle)
    assert status.head is None and not status.head_alive
    assert set(status.pools) == {"head"} and status.pools["head"].pool_id is None
    assert not status.past_deadline
    p.clock.advance(1801)
    assert core.status(handle).past_deadline


def test_status_of_a_cluster_with_a_group_but_no_pool(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    p.fail_next("provision_pool", BOOM)
    with pytest.raises(PlatformError):
        core.add_pool(handle, WORKERS, NODE, target=1, max_nodes=1)
    state = core.status(handle).pools["w-small"]
    assert state.pool_id is None and state.status is None and state.live_instances == 0


# --- added after the phase 3 review ------------------------------------------------


@pytest.mark.parametrize("role", ["w_x", "small", "", "w-Up"])
def test_roles_are_validated(core: ClusterCore, p: FakePlatform, role: str) -> None:
    handle = core.create(SETTINGS)
    with pytest.raises(ConfigError):
        core.add_pool(handle, role, NODE, target=1, max_nodes=1)
    with pytest.raises(ConfigError):
        core.add_nodes(handle, role, 1, "x")
    assert p.live_instances == 0 and p.list_pools(NS) == []


@pytest.mark.parametrize(
    "bad",
    [
        {"lifetime": timedelta(seconds=-5)},
        {"lifetime": timedelta(0)},
        {"lifetime": timedelta(seconds=90.5)},
        {"idle_shutdown": timedelta(seconds=-1)},
        {"idle_shutdown": timedelta(milliseconds=1500)},
        {"build_timeout": timedelta(0)},
        {"namespace": ""},
    ],
)
def test_settings_are_validated_before_anything_is_created(
    core: ClusterCore, p: FakePlatform, bad: dict[str, object]
) -> None:
    with pytest.raises(ConfigError):
        core.create(replace(SETTINGS, **bad))  # type: ignore[arg-type]
    assert p.calls == []


@pytest.mark.parametrize("method", ["list_work_requirements", "get_work_requirement"])
def test_create_withdraws_if_it_cannot_confirm_it_is_alone(
    core: ClusterCore, p: FakePlatform, method: str
) -> None:
    real = getattr(p, method)
    seen = {"created": False}

    def flaky(*args: object, **kwargs: object) -> object:
        if seen["created"]:
            raise BOOM
        return real(*args, **kwargs)

    real_create = p.create_work_requirement

    def create(*args: object, **kwargs: object) -> object:
        result = real_create(*args, **kwargs)  # type: ignore[arg-type]
        seen["created"] = True
        return result

    p.create_work_requirement = create  # type: ignore[method-assign,assignment]
    setattr(p, method, flaky)
    with pytest.raises(PlatformError):
        core.create(SETTINGS)
    setattr(p, method, real)
    p.clock.advance(30)
    assert p.list_work_requirements(NS, live_only=True) == []
    core.create(SETTINGS)  # and the name is free to try again


def test_the_platform_itself_refuses_a_second_head(
    core: ClusterCore, p: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two callers both pass the 'no head yet' check; only one head may result."""
    handle = core.create(SETTINGS)
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    monkeypatch.setattr(core, "_tasks", lambda *a, **k: [])  # both saw no head
    core.add_nodes(handle, naming.HEAD_ROLE, 1, "x")
    with pytest.raises(ClusterStateError, match="only one head"):
        core.add_nodes(handle, naming.HEAD_ROLE, 1, "x")
    assert len(p.list_tasks(handle.work_requirement_id, live_only=True)) == 1


def test_add_nodes_refuses_a_role_whose_pool_has_gone(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=2)
    core.remove_pool(handle, WORKERS)
    with pytest.raises(ClusterStateError, match="pool"):
        core.add_nodes(handle, WORKERS, 1, "x")
    assert core.nodes(handle) == []
    with pytest.raises(ClusterStateError, match="different name"):
        core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=2)


def test_a_racing_add_pool_is_a_state_error(
    core: ClusterCore, p: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle = core.create(SETTINGS)
    core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=1)
    monkeypatch.setattr(
        p, "find_pool", lambda namespace, name: None
    )  # looked too early
    with pytest.raises(ClusterStateError):
        core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=1)


def test_a_pool_added_while_the_cluster_is_being_torn_down_is_removed(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    real = p.provision_pool

    def provision_then_lose_the_race(spec: PoolSpec) -> object:
        info = real(spec)
        core.tear_down(handle)  # teardown ran between our check and our provision
        return info

    p.provision_pool = provision_then_lose_the_race  # type: ignore[method-assign,assignment]
    with pytest.raises(ClusterStateError):
        core.add_pool(handle, WORKERS, NODE, target=2, max_nodes=2)
    p.clock.advance(120)
    assert p.live_instances == 0


def test_head_alive_means_the_head_is_running_not_merely_requested(
    core: ClusterCore, p: FakePlatform
) -> None:
    p.never_register("demo")
    handle = core.create(SETTINGS)
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    core.add_nodes(handle, naming.HEAD_ROLE, 1, "x")
    p.clock.advance(3600)
    status = core.status(handle)
    assert status.head is not None and status.head.status is TaskStatus.READY
    assert not status.head_alive


def test_status_does_not_read_every_task_there_has_ever_been(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    start_head(core, handle)
    core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=2)
    for _ in range(5):
        tasks = core.add_nodes(handle, WORKERS, 2, "x")
        core.remove_nodes(handle, [t.id for t in tasks])
        p.clock.advance(30)
    seen: list[int] = []
    real = p.list_tasks

    def counting(*args: object, **kwargs: object) -> list[object]:
        result = real(*args, **kwargs)  # type: ignore[arg-type]
        seen.append(len(result))
        return result  # type: ignore[return-value]

    p.list_tasks = counting  # type: ignore[method-assign,assignment]
    core.status(handle)
    assert sum(seen) <= 3  # the head (twice at most), never the ten ended workers


def test_a_pools_maximum_is_recorded_for_other_processes(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    core.add_pool(handle, naming.HEAD_ROLE, NODE, target=1, max_nodes=1)
    core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=7)
    pools = ClusterCore(p, p.clock).status(handle).pools
    assert pools["w-small"].max_nodes == 7
    assert pools["head"].max_nodes is None  # the head group's marker holds the settings


def test_a_node_told_to_stop_is_no_longer_counted(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    pool = core.add_pool(handle, WORKERS, NODE, target=0, max_nodes=5)
    tasks = core.add_nodes(handle, WORKERS, 2, "x")
    p.clock.advance(300)
    core.remove_nodes(handle, [tasks[1].id])
    # The abort has been asked for but has not happened yet
    assert p.get_task(tasks[1].id).status is TaskStatus.EXECUTING
    assert [n.task_id for n in core.nodes(handle)] == [tasks[0].id]
    stopping = core.nodes(handle, include_stopping=True)
    assert [n.stopping for n in stopping] == [False, True]
    assert dict(core.status(handle).pools[WORKERS].nodes_by_status) == {
        TaskStatus.EXECUTING: 1
    }
    core.add_nodes(handle, WORKERS, 2, "x")
    assert last_call(p, "set_pool_target") == (pool.id, 3)


# --- added after the phase 4 review ------------------------------------------------


@pytest.mark.parametrize("error", [BOOM, KeyboardInterrupt()])
def test_create_withdraws_when_the_create_response_is_lost(
    core: ClusterCore, p: FakePlatform, error: BaseException
) -> None:
    real = p.create_work_requirement

    def create_then_lose_the_response(*args: object, **kwargs: object) -> object:
        real(*args, **kwargs)  # type: ignore[arg-type]
        raise error

    p.create_work_requirement = create_then_lose_the_response  # type: ignore[method-assign,assignment]
    with pytest.raises(type(error)):
        core.create(SETTINGS)
    p.create_work_requirement = real  # type: ignore[method-assign]
    p.clock.advance(30)
    assert p.list_work_requirements(NS, live_only=True) == []
    core.create(SETTINGS)


@pytest.mark.parametrize("error", [BOOM, KeyboardInterrupt()])
def test_add_pool_removes_the_pool_if_it_cannot_confirm_the_cluster(
    core: ClusterCore, p: FakePlatform, error: BaseException
) -> None:
    handle = core.create(SETTINGS)
    real = p.provision_pool

    def provision_then_outage(spec: PoolSpec) -> object:
        info = real(spec)
        p.fail_next("get_work_requirement", error)  # type: ignore[arg-type]
        return info

    p.provision_pool = provision_then_outage  # type: ignore[method-assign,assignment]
    with pytest.raises(type(error)):
        core.add_pool(handle, WORKERS, NODE, target=3, max_nodes=3)
    p.clock.advance(100)
    assert p.live_instances == 0


def test_adding_nodes_never_lowers_a_pools_target(
    core: ClusterCore, p: FakePlatform
) -> None:
    handle = core.create(SETTINGS)
    pool = core.add_pool(handle, WORKERS, NODE, target=4, max_nodes=6)
    core.add_nodes(handle, WORKERS, 1, "x")
    targets = [c.args for c in p.calls if c.method == "set_pool_target"]
    assert targets == [] or targets[-1] == (pool.id, 4)
    assert p.get_pool(pool.id).target_instances == 4
