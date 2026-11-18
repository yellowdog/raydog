"""
RayDogNodeProvider, driven the way Ray drives it, against the in-memory platform.

The helpers below replay the call sequences that Ray's own code makes: ``ray up`` on
the client, the monitor's update loop and node launchers on the head node, and
``ray down``.
"""

from __future__ import annotations

import json
import logging
import os
import stat
import subprocess
import threading
from pathlib import Path
from typing import Any

import pytest
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    STATUS_UNINITIALIZED,
    STATUS_UP_TO_DATE,
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)

from tests.unit.test_autoscaler_config import cluster_config
from yellowdog_ray.autoscaler import RayDogNodeProvider
from yellowdog_ray.autoscaler import config as cfg
from yellowdog_ray.autoscaler.provider import with_cloud_instance_id
from yellowdog_ray.core import naming
from yellowdog_ray.core.clock import Clock
from yellowdog_ray.core.cluster import ClusterCore
from yellowdog_ray.core.platform import Platform, PoolSpec, TaskStatus
from yellowdog_ray.credentials import Credentials
from yellowdog_ray.errors import (
    BuildTimeoutError,
    ClusterStateError,
    ConfigError,
    CredentialsError,
    NodeStartError,
    PlatformError,
)
from yellowdog_ray.testing import FakePlatform

NS = "ns"
BOOM = PlatformError("boom")
HEAD_TAGS = {
    TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
    TAG_RAY_USER_NODE_TYPE: "head",
    TAG_RAY_LAUNCH_CONFIG: "launch-hash-1",
    TAG_RAY_NODE_NAME: "ray-demo-head",
    TAG_RAY_NODE_STATUS: STATUS_UNINITIALIZED,
}
HEAD_KIND = {TAG_RAY_NODE_KIND: NODE_KIND_HEAD}
WORKER_KIND = {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}


@pytest.fixture
def p() -> FakePlatform:
    return FakePlatform()


@pytest.fixture(autouse=True)
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A private home directory and credentials in the environment."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("YD_API_KEY_ID", "the-id")
    monkeypatch.setenv("YD_API_KEY_SECRET", "the-secret")
    monkeypatch.delenv("YD_API_URL", raising=False)
    monkeypatch.delenv("RAYDOG_CREDENTIALS_FILE", raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def config(**provider: object) -> dict[str, Any]:
    """A cluster config as Ray hands it over after bootstrap."""
    return cfg.bootstrap(
        cluster_config(refresh_interval_seconds=0, **provider), [Path.cwd()]
    )


def make(p: FakePlatform, provider_section: dict[str, Any]) -> RayDogNodeProvider:
    class Provider(RayDogNodeProvider):
        connected_with: Credentials | None = None

        def _connect(self, credentials: Credentials) -> tuple[Platform, Clock]:
            Provider.connected_with = credentials
            return p, p.clock

    return Provider(provider_section, "demo")


def ray_up(
    provider: RayDogNodeProvider,
    conf: dict[str, Any],
    launch_hash: str = "launch-hash-1",
) -> tuple[str, dict[str, Any]]:
    """
    What ``ray up`` does to a provider. Returns the head's ID and the config
    prepared for the head node.
    """
    tags = {**HEAD_TAGS, TAG_RAY_LAUNCH_CONFIG: launch_hash}
    nodes = provider.non_terminated_nodes(HEAD_KIND)
    if nodes and provider.node_tags(nodes[0]).get(TAG_RAY_LAUNCH_CONFIG) != launch_hash:
        provider.terminate_node(nodes[0])
        nodes = []
    if not nodes:
        head_type = conf["available_node_types"][conf["head_node_type"]]
        provider.create_node(head_type["node_config"], dict(tags), 1)
        nodes = provider.non_terminated_nodes(tags)  # Ray gives this 50 seconds
        assert len(nodes) == 1, "ray up would time out fetching the new head"
    head = nodes[0]
    head_config = provider.prepare_for_head_node(conf)
    # The node updater thread
    provider.set_node_tags(head, {TAG_RAY_NODE_STATUS: "waiting-for-ssh"})
    assert not provider.is_terminated(head)
    assert provider.external_ip(head), "the updater polls until this is set"
    provider.node_tags(head)
    provider.set_node_tags(head, {TAG_RAY_NODE_STATUS: "syncing-files"})
    provider.set_node_tags(head, {TAG_RAY_NODE_STATUS: "setting-up"})
    provider.set_node_tags(
        head,
        {
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            "ray-runtime-config": "runtime-hash",
            "ray-file-mounts-contents": "mounts-hash",
        },
    )
    provider.non_terminated_nodes(tags)
    return head, head_config


def running_head(provider: RayDogNodeProvider) -> str | None:
    """How every other Ray command finds the head (``_get_running_head_node``)."""
    for node in provider.non_terminated_nodes(HEAD_KIND):
        if provider.node_tags(node).get(TAG_RAY_NODE_STATUS) == STATUS_UP_TO_DATE:
            return node
    return None


def launch(
    head: RayDogNodeProvider,
    conf: dict[str, Any],
    node_type: str,
    count: int,
    *,
    resources: dict[str, float] | None = None,
    labels: dict[str, str] | None = None,
    tags: dict[str, str] | None = None,
) -> None:
    """What one of the monitor's node launcher threads does."""
    all_tags = {
        TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
        TAG_RAY_USER_NODE_TYPE: node_type,
        TAG_RAY_LAUNCH_CONFIG: f"worker-hash-{node_type}",
        TAG_RAY_NODE_NAME: "ray-demo-worker",
        TAG_RAY_NODE_STATUS: STATUS_UNINITIALIZED,
        **(tags or {}),
    }
    node_config = conf["available_node_types"][node_type]["node_config"]
    head.create_node_with_resources_and_labels(
        node_config,
        all_tags,
        count,
        {"CPU": 1} if resources is None else resources,
        labels or {},
    )


def monitor_iteration(head: RayDogNodeProvider) -> list[str]:
    """One pass of the v1 monitor's update loop, as far as the provider sees it."""
    nodes = head.non_terminated_nodes({})
    for node in nodes:
        for _ in range(8):
            head.node_tags(node)
            head.internal_ip(node)
        head.is_running(node)
    return nodes


def ray_down(provider: RayDogNodeProvider) -> None:
    """What ``ray down`` does: workers, then the head, then wait, then clean up."""
    for _ in range(100):
        workers = provider.non_terminated_nodes(WORKER_KIND)
        if not workers:
            break
        provider.terminate_nodes(workers)
    heads = provider.non_terminated_nodes(HEAD_KIND)
    if heads:
        provider.terminate_nodes(heads)
    for _ in range(100):
        if not provider.non_terminated_nodes({}):
            break
    else:
        pytest.fail("ray down would wait for ever")
    provider.cleanup_cluster_resources()


def cluster(
    p: FakePlatform, **provider: object
) -> tuple[dict[str, Any], RayDogNodeProvider, RayDogNodeProvider, str]:
    """A cluster brought up as Ray would: (config, client, head provider, head id)."""
    conf = config(**provider)
    client = make(p, conf["provider"])
    head_id, head_config = ray_up(client, conf)
    return conf, client, make(p, head_config["provider"]), head_id


# --- construction -----------------------------------------------------------------


def test_constructing_makes_no_calls_and_cannot_destroy(
    p: FakePlatform, home: Path
) -> None:
    conf = config()
    ClusterCore(p, p.clock).create(
        cfg.parse_provider_config(conf["provider"], "demo").settings()
    )
    calls = len(p.calls)
    provider = make(p, {**conf["provider"], "_config_cache_path": "/tmp/x"})
    assert len(p.calls) == calls
    assert not (home / ".raydog").exists()
    assert not hasattr(provider, "__del__")
    assert provider.cluster_name == "demo"


def test_a_bad_provider_section_is_reported_by_the_constructor(p: FakePlatform) -> None:
    with pytest.raises(ConfigError, match="namespace"):
        make(p, {"type": "external", "module": cfg.MODULE})
    assert p.calls == []


def test_it_works_from_the_raw_yaml_section(p: FakePlatform) -> None:
    """Ray does not always run bootstrap_config first (``ray get-head-ip``)."""
    _, client, _, head_id = cluster(p)
    raw = cluster_config(refresh_interval_seconds=0)["provider"]
    other = make(p, raw)
    assert running_head(other) == head_id
    assert other.external_ip(head_id) == client.external_ip(head_id)


# --- ray up -----------------------------------------------------------------------


def test_ray_up_creates_a_cluster_with_a_head(p: FakePlatform) -> None:
    conf = config(tag="user-tag")
    client = make(p, conf["provider"])
    assert client.non_terminated_nodes({}) == []
    head_id, head_config = ray_up(client, conf)
    assert p.get_task(head_id).status is TaskStatus.EXECUTING
    assert p.live_instances == 1
    tags = client.node_tags(head_id)
    assert tags[TAG_RAY_NODE_STATUS] == STATUS_UP_TO_DATE
    assert tags[TAG_RAY_LAUNCH_CONFIG] == "launch-hash-1"
    assert tags["ray-runtime-config"] == "runtime-hash"
    assert client.is_running(head_id) and not client.is_terminated(head_id)
    assert client.internal_ip(head_id) and client.external_ip(head_id)
    # The head runs the head script, with nothing secret in its task
    task = p.get_task(head_id)
    spec = next(c.args[1][0] for c in p.calls if c.method == "add_tasks")
    assert spec.script == with_cloud_instance_id("ray start --head")  # autoscaler v2
    assert "the-secret" not in str(dict(task.environment)) + str(task.tag)
    work_requirement = p.list_work_requirements(NS, live_only=True)[0]
    assert work_requirement.tag == "user-tag"
    # The head's copy of the config says which cluster it is and where its
    # credentials will be; the client's config is not altered
    stamped = head_config["provider"]["raydog_head"]
    assert stamped == {
        "work_requirement_id": work_requirement.id,
        "credentials_file": "~/.raydog/credentials/ns.demo",
    }
    assert "raydog_head" not in conf["provider"]
    assert "the-secret" not in str(head_config)


def test_the_new_head_matches_rays_status_filter_at_once(p: FakePlatform) -> None:
    """After create_node, Ray looks for the head with the status it was given."""
    conf = config()
    client = make(p, conf["provider"])
    client.create_node(
        conf["available_node_types"]["head"]["node_config"], dict(HEAD_TAGS), 1
    )
    assert len(client.non_terminated_nodes(HEAD_TAGS)) == 1
    assert (
        client.non_terminated_nodes({**HEAD_TAGS, TAG_RAY_LAUNCH_CONFIG: "other"}) == []
    )


def test_other_commands_find_the_head_up_to_date(p: FakePlatform) -> None:
    conf, client, _, head_id = cluster(p)
    for _ in range(3):  # ray exec, ray attach, ray get-head-ip: a new process each
        other = make(p, conf["provider"])
        assert running_head(other) == head_id
        assert other.external_ip(head_id) == client.external_ip(head_id)
    assert p.live_instances == 1


def test_a_second_ray_up_reuses_the_head(p: FakePlatform) -> None:
    conf, _, _, head_id = cluster(p)
    creates = [c for c in p.calls if c.method == "create_work_requirement"]
    again = make(p, conf["provider"])
    second_id, _ = ray_up(again, conf)
    assert second_id == head_id
    assert [c for c in p.calls if c.method == "create_work_requirement"] == creates
    assert p.live_instances == 1


def test_ray_up_with_a_changed_head_config_replaces_the_cluster(
    p: FakePlatform,
) -> None:
    conf, _, _, head_id = cluster(p)
    again = make(p, conf["provider"])
    new_id, _ = ray_up(again, conf, launch_hash="launch-hash-2")
    assert new_id != head_id
    assert p.get_task(head_id).status.is_terminal or p.get_task(head_id).stopping
    p.clock.advance(300)
    assert p.live_instances == 1
    assert len(p.list_work_requirements(NS, live_only=True)) == 1


def test_ray_up_replaces_a_cluster_whose_head_has_died(p: FakePlatform) -> None:
    conf, _, head, head_id = cluster(p)
    launch(head, conf, "small", 2)
    p.clock.advance(300)
    assert p.live_instances == 3
    p.fail_task(head_id)
    p.clock.advance(60)
    again = make(p, conf["provider"])
    assert again.non_terminated_nodes(HEAD_KIND) == []
    new_id, _ = ray_up(again, conf)
    assert new_id != head_id and p.get_task(new_id).status is TaskStatus.EXECUTING
    p.clock.advance(300)
    assert p.live_instances == 1, "the dead cluster's workers went with it"
    assert len(p.list_work_requirements(NS, live_only=True)) == 1


def test_create_head_refuses_to_replace_a_cluster_with_a_live_head(
    p: FakePlatform,
) -> None:
    conf, _, _, head_id = cluster(p)
    other = make(p, conf["provider"])
    with pytest.raises(ClusterStateError, match="already has a head"):
        other.create_node(
            conf["available_node_types"]["head"]["node_config"], dict(HEAD_TAGS), 1
        )
    assert p.get_task(head_id).status is TaskStatus.EXECUTING and p.live_instances == 1


def test_a_head_that_fails_to_start_is_rolled_back(p: FakePlatform) -> None:
    conf = config()
    client = make(p, conf["provider"])
    real = p.add_tasks

    def add_then_fail(group_id: str, tasks: list[object]) -> list[object]:
        added = real(group_id, tasks)  # type: ignore[arg-type]
        p.clock.advance(120)
        p.fail_task(added[0].id)
        return added  # type: ignore[return-value]

    p.add_tasks = add_then_fail  # type: ignore[method-assign,assignment]
    with pytest.raises(NodeStartError):
        client.create_node(
            conf["available_node_types"]["head"]["node_config"], dict(HEAD_TAGS), 1
        )
    p.clock.advance(100)
    assert p.live_instances == 0 and client.non_terminated_nodes({}) == []
    assert p.list_work_requirements(NS, live_only=True) == []


def test_a_head_that_never_registers_times_out_and_is_rolled_back(
    p: FakePlatform,
) -> None:
    p.never_register("demo")
    conf = config(build_timeout_minutes=3)
    client = make(p, conf["provider"])
    with pytest.raises(BuildTimeoutError):
        client.create_node(
            conf["available_node_types"]["head"]["node_config"], dict(HEAD_TAGS), 1
        )
    p.clock.advance(100)
    assert p.live_instances == 0


def test_the_client_does_not_create_workers(p: FakePlatform) -> None:
    conf, client, _, _ = cluster(p)
    with pytest.raises(ClusterStateError, match="head node"):
        launch(client, conf, "small", 1)
    assert p.live_instances == 1


def test_prepare_for_head_node_needs_a_cluster(p: FakePlatform) -> None:
    conf = config()
    with pytest.raises(Exception, match="does not exist"):
        make(p, conf["provider"]).prepare_for_head_node(conf)


# --- a healthy cluster is never harmed --------------------------------------------


READ_ONLY_METHODS = [
    "list_work_requirements", "get_work_requirement", "list_tasks", "find_pool",
    "list_nodes", "public_ips", "find_work_requirement", "get_task",
]  # fmt: skip


@pytest.mark.parametrize("method", READ_ONLY_METHODS)
@pytest.mark.parametrize("times", [1, 3])
def test_a_failing_platform_never_costs_a_healthy_cluster(
    p: FakePlatform, method: str, times: int
) -> None:
    """``ray exec`` and friends against a cluster, while the platform misbehaves."""
    conf, _, head, head_id = cluster(p)
    launch(head, conf, "small", 2)
    p.clock.advance(300)
    cancels = [c for c in p.calls if c.method.startswith(("cancel", "remove"))]
    for _ in range(4):
        other = make(p, conf["provider"])
        p.fail_next(method, BOOM, times=times)
        try:
            running_head(other)
            other.external_ip(head_id)
            other.node_tags(head_id)
        except PlatformError:
            pass
        p._faults.clear()
    assert [c for c in p.calls if c.method.startswith(("cancel", "remove"))] == cancels
    assert p.live_instances == 3
    assert running_head(make(p, conf["provider"])) == head_id


def test_a_similarly_named_cluster_is_invisible(p: FakePlatform) -> None:
    _, _, _, head_id = cluster(p)
    other_conf = cfg.bootstrap(
        {**cluster_config(refresh_interval_seconds=0), "cluster_name": "dem"},
        [Path.cwd()],
    )

    class Other(RayDogNodeProvider):
        def _connect(self, credentials: Credentials) -> tuple[Platform, Clock]:
            return p, p.clock

    other = Other(other_conf["provider"], "dem")
    assert other.non_terminated_nodes({}) == []
    other.cleanup_cluster_resources()
    other.terminate_nodes([head_id])  # not its node: nothing happens
    assert p.get_task(head_id).status is TaskStatus.EXECUTING and p.live_instances == 1


# --- credentials ------------------------------------------------------------------


def test_the_credentials_file_is_private_and_kept_out_of_the_config(
    p: FakePlatform, home: Path
) -> None:
    conf = config()
    client = make(p, conf["provider"])
    path = home / ".raydog" / "credentials" / "ns.demo"
    assert not path.exists()
    client.non_terminated_nodes({})  # the first thing every Ray command does
    assert path.read_text() == (
        "YD_API_KEY_ID=the-id\nYD_API_KEY_SECRET=the-secret\n"
        "YD_API_URL=https://api.yellowdog.ai\n"
    )
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert conf["file_mounts"] == {"~/.raydog/credentials/ns.demo": str(path)}
    assert "the-secret" not in str(conf)


def test_the_credentials_file_follows_a_change_of_key(
    p: FakePlatform, home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conf = config()
    make(p, conf["provider"]).non_terminated_nodes({})
    path = home / ".raydog" / "credentials" / "ns.demo"
    os.chmod(path, 0o644)
    monkeypatch.setenv("YD_API_KEY_SECRET", "rotated")
    make(p, conf["provider"]).non_terminated_nodes({})
    assert "rotated" in path.read_text()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_missing_credentials_are_reported_not_ignored(
    p: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("YD_API_KEY_SECRET")
    client = make(p, config()["provider"])
    with pytest.raises(CredentialsError, match="YD_API_KEY_SECRET"):
        client.non_terminated_nodes({})


def test_the_head_reads_its_credentials_from_the_synced_file(
    p: FakePlatform, home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _conf, _, head, _ = cluster(p)
    # On the head node there are no credentials in the environment
    monkeypatch.delenv("YD_API_KEY_ID")
    monkeypatch.delenv("YD_API_KEY_SECRET")
    head.non_terminated_nodes({})
    assert type(head).connected_with == Credentials("the-id", "the-secret")  # type: ignore[attr-defined]
    path = home / ".raydog" / "credentials" / "ns.demo"
    before = path.stat().st_mtime_ns
    head.non_terminated_nodes({})
    assert path.stat().st_mtime_ns == before, "the head never writes the file"

    elsewhere = home / "mounted" / "creds"
    elsewhere.parent.mkdir()
    elsewhere.write_text("YD_API_KEY_ID=other-id\nYD_API_KEY_SECRET=other-secret\n")
    monkeypatch.setenv("RAYDOG_CREDENTIALS_FILE", str(elsewhere))
    in_container = make(p, head.provider_config)
    in_container.non_terminated_nodes({})
    assert type(in_container).connected_with == Credentials("other-id", "other-secret")  # type: ignore[attr-defined]


# --- the head node: scaling -------------------------------------------------------


def test_scale_up_creates_a_pool_per_node_type(p: FakePlatform) -> None:
    conf, client, head, head_id = cluster(p)
    launch(head, conf, "small", 2, tags={"ray-launch-request": "req-1"})
    launch(head, conf, "big.gpu", 1)
    nodes = monitor_iteration(head)
    assert len(nodes) == 4 and head_id in nodes
    workers = head.non_terminated_nodes(WORKER_KIND)
    assert len(workers) == 3
    small = head.non_terminated_nodes({TAG_RAY_USER_NODE_TYPE: "small"})
    assert len(small) == 2
    assert head.node_tags(small[0])["ray-launch-request"] == "req-1"
    assert head.node_tags(small[0])[TAG_RAY_NODE_STATUS] == STATUS_UNINITIALIZED
    assert head.internal_ip(small[0]) is None and not head.is_running(small[0])

    specs = {c.args[0].name: c.args[0] for c in p.calls if c.method == "provision_pool"}
    by_role = {naming.parse_pool_name(name)[2]: spec for name, spec in specs.items()}  # type: ignore[index]
    small_spec: PoolSpec = by_role[naming.worker_role("small")]
    gpu_spec: PoolSpec = by_role[naming.worker_role("big.gpu")]
    assert (small_spec.template, small_spec.images_id) == ("ns/worker", "ami-1")
    assert (small_spec.max_nodes, gpu_spec.max_nodes) == (3, 10)
    assert gpu_spec.template == "ns/gpu"

    p.clock.advance(300)
    monitor_iteration(head)
    assert all(head.is_running(n) and head.internal_ip(n) for n in workers)
    assert all(
        head.node_tags(n)[TAG_RAY_NODE_STATUS] == STATUS_UP_TO_DATE for n in workers
    )
    assert p.live_instances == 4
    # Each worker was told where the head is, and runs the worker script
    env = dict(p.get_task(workers[0]).environment)
    assert env["RAY_HEAD_IP"] == client.internal_ip(head_id)
    worker_specs = [c.args[1][0] for c in p.calls if c.method == "add_tasks"][1:]
    worker_script = with_cloud_instance_id("ray start --address=$RAY_HEAD_IP:6379")
    assert all(s.script == worker_script for s in worker_specs)

    launch(head, conf, "small", 1)  # more of the same type: no new pool
    assert len([c for c in p.calls if c.method == "provision_pool"]) == 3
    assert len(head.non_terminated_nodes(WORKER_KIND)) == 4


def test_workers_wait_for_the_grace_period_before_claiming_to_be_ready(
    p: FakePlatform,
) -> None:
    conf, _, head, _ = cluster(p, worker_start_grace_seconds=120)
    launch(head, conf, "small", 1)
    (worker,) = head.non_terminated_nodes(WORKER_KIND)
    for _ in range(40):
        p.clock.advance(5)
        head.non_terminated_nodes({})
        if head.is_running(worker):
            break
    assert head.is_running(worker)
    assert head.node_tags(worker)[TAG_RAY_NODE_STATUS] == STATUS_UNINITIALIZED
    p.clock.advance(119)
    head.non_terminated_nodes({})
    assert head.node_tags(worker)[TAG_RAY_NODE_STATUS] == STATUS_UNINITIALIZED
    p.clock.advance(10)
    assert head.node_tags(worker)[TAG_RAY_NODE_STATUS] == STATUS_UP_TO_DATE


def test_scale_down_removes_nodes_at_once(p: FakePlatform) -> None:
    conf, _, head, head_id = cluster(p)
    launch(head, conf, "small", 3)
    p.clock.advance(300)
    workers = head.non_terminated_nodes(WORKER_KIND)
    head.terminate_nodes(workers[:2])
    assert head.non_terminated_nodes(WORKER_KIND) == workers[2:]
    assert head.is_terminated(workers[0]) and not head.is_running(workers[0])
    assert head.node_tags(workers[0]) == {TAG_RAY_NODE_STATUS: "terminated"}
    head.terminate_node(workers[2])
    assert head.non_terminated_nodes({}) == [head_id]
    p.clock.advance(300)
    assert p.live_instances == 1
    head.terminate_nodes([])  # nothing to do is not an error


@pytest.mark.parametrize("how", ["task fails", "instance lost"])
def test_a_dead_worker_disappears_at_the_next_refresh(
    p: FakePlatform, how: str
) -> None:
    conf, _, head, _ = cluster(p)
    launch(head, conf, "small", 2)
    p.clock.advance(300)
    workers = monitor_iteration(head)[1:]
    assert len(workers) == 2
    if how == "task fails":
        p.fail_task(workers[0])
    else:
        p.lose_node(workers[0])
    remaining = head.non_terminated_nodes(WORKER_KIND)
    assert remaining == [workers[1]]
    assert head.is_terminated(workers[0])


def test_a_restarted_monitor_carries_on(p: FakePlatform) -> None:
    conf, _, head, _head_id = cluster(p)
    launch(head, conf, "small", 1)
    p.clock.advance(300)
    restarted = make(p, head.provider_config)
    assert len(monitor_iteration(restarted)) == 2
    launch(restarted, conf, "small", 1)  # its pool already exists
    assert len(restarted.non_terminated_nodes(WORKER_KIND)) == 2
    assert len([c for c in p.calls if c.method == "provision_pool"]) == 2


def test_the_head_node_never_creates_a_cluster_or_a_head(p: FakePlatform) -> None:
    conf, _, head, _ = cluster(p)
    with pytest.raises(ClusterStateError):
        head.create_node(
            conf["available_node_types"]["head"]["node_config"], dict(HEAD_TAGS), 1
        )
    assert len([c for c in p.calls if c.method == "create_work_requirement"]) == 1


def test_the_monitor_loop_costs_a_bounded_number_of_platform_calls(
    p: FakePlatform,
) -> None:
    conf, _, head, _ = cluster(p)
    launch(head, conf, "small", 3)
    p.clock.advance(300)
    monitor_iteration(head)  # addresses are looked up once
    for _ in range(3):
        before = len(p.calls)
        monitor_iteration(head)
        assert len(p.calls) - before <= 2, [c.method for c in p.calls[before:]]


def test_refreshes_are_rate_limited(p: FakePlatform) -> None:
    conf = cfg.bootstrap(cluster_config(), [Path.cwd()])  # the default: 2 seconds
    client = make(p, conf["provider"])
    client.non_terminated_nodes({})
    before = len(p.calls)
    for _ in range(10):
        client.non_terminated_nodes({})
    assert len(p.calls) == before
    p.clock.advance(2)
    client.non_terminated_nodes({})
    assert len(p.calls) > before


def test_launcher_threads_and_the_monitor_loop_together(p: FakePlatform) -> None:
    """Two launchers for one type, two for another, and the loop refreshing."""
    conf, _, head, _head_id = cluster(p)
    errors: list[BaseException] = []
    done = threading.Event()

    def launcher(node_type: str) -> None:
        try:
            for _ in range(5):
                launch(head, conf, node_type, 2)
        except BaseException as e:
            errors.append(e)

    def loop() -> None:
        try:
            while not done.is_set():
                monitor_iteration(head)
        except BaseException as e:
            errors.append(e)

    launchers = [
        threading.Thread(target=launcher, args=(t,))
        for t in ("small", "small", "big.gpu", "big.gpu")
    ]
    monitor = threading.Thread(target=loop)
    monitor.start()
    for thread in launchers:
        thread.start()
    for thread in launchers:
        thread.join(timeout=60)
    done.set()
    monitor.join(timeout=60)
    assert not any(t.is_alive() for t in [*launchers, monitor]), "deadlock"
    assert errors == []
    assert len([c for c in p.calls if c.method == "provision_pool"]) == 3
    workers = head.non_terminated_nodes(WORKER_KIND)
    assert len(workers) == 40 and len(set(workers)) == 40
    assert len(head.non_terminated_nodes({TAG_RAY_USER_NODE_TYPE: "small"})) == 20
    groups = p.get_work_requirement(
        p.list_work_requirements(NS, live_only=True)[0].id
    ).task_groups
    assert len(groups) == 3, "no task group was lost or duplicated"


# --- ray down ---------------------------------------------------------------------


def test_ray_down_ends_everything(p: FakePlatform) -> None:
    conf, _, head, _ = cluster(p)
    launch(head, conf, "small", 2)
    launch(head, conf, "big.gpu", 1)
    p.clock.advance(300)
    assert p.live_instances == 4
    client = make(p, conf["provider"])  # ray down is a new process
    ray_down(client)
    assert p.live_instances == 0, "ray down waited until every instance had gone"
    assert p.list_work_requirements(NS, live_only=True) == []
    assert client.non_terminated_nodes({}) == []
    ray_down(make(p, conf["provider"]))  # and again: nothing to do, no error


def test_cleanup_never_raises(
    p: FakePlatform, caplog: pytest.LogCaptureFixture
) -> None:
    conf, client, _, _ = cluster(p)
    p.fail_next("remove_pool", BOOM)
    client.cleanup_cluster_resources()
    assert "boom" in caplog.text
    client.cleanup_cluster_resources()
    p.clock.advance(120)
    assert p.live_instances == 0
    p.fail_next("list_work_requirements", BOOM, times=5)
    make(p, conf["provider"]).cleanup_cluster_resources()


def test_ray_up_after_ray_down_starts_afresh(p: FakePlatform) -> None:
    conf, client, _, head_id = cluster(p)
    ray_down(client)
    new_id, _ = ray_up(make(p, conf["provider"]), conf)
    assert new_id != head_id and p.live_instances == 1


def test_the_head_node_asked_to_terminate_itself_ends_the_cluster(
    p: FakePlatform,
) -> None:
    conf, _, head, head_id = cluster(p)
    launch(head, conf, "small", 2)
    p.clock.advance(300)
    head.non_terminated_nodes({})
    before = p.clock.now()
    head.terminate_node(head_id)
    assert p.clock.now() == before, "the head does not wait around to watch"
    assert head.non_terminated_nodes({}) == []
    p.clock.advance(120)
    assert p.live_instances == 0


# --- bootstrap_config -------------------------------------------------------------


def test_bootstrap_config_resolves_files_beside_the_yaml(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    yaml_dir = home / "configs"
    (yaml_dir / "scripts").mkdir(parents=True)
    (yaml_dir / "cluster.yaml").write_text("placeholder")
    (yaml_dir / "scripts" / "head.sh").write_text("ray start --head --block\n")
    elsewhere = home / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    monkeypatch.setattr(
        "sys.argv", ["/usr/bin/ray", "up", "-y", str(yaml_dir / "cluster.yaml")]
    )
    raw = cluster_config(head_start_script="file:scripts/head.sh")
    result = RayDogNodeProvider.bootstrap_config(raw)
    assert result["provider"]["head_start_script"] == "ray start --head --block\n"
    assert raw["provider"]["head_start_script"] == "file:scripts/head.sh"


# --- added after the phase 5 review ------------------------------------------------


def test_a_failed_cancel_does_not_hide_a_live_worker(p: FakePlatform) -> None:
    """If a worker could not be stopped, Ray must go on seeing it, to try again."""
    conf, _, head, _ = cluster(p)
    launch(head, conf, "small", 3)
    p.clock.advance(300)
    workers = head.non_terminated_nodes(WORKER_KIND)
    p.fail_next("cancel_task", BOOM)
    with pytest.raises(PlatformError):
        head.terminate_nodes(workers)
    p.clock.advance(60)
    still_running = [w for w in workers if p.get_task(w).status is TaskStatus.EXECUTING]
    assert len(still_running) == 1
    for _ in range(3):
        assert head.non_terminated_nodes(WORKER_KIND) == still_running
    head.terminate_nodes(still_running)  # Ray's next attempt works
    assert head.non_terminated_nodes(WORKER_KIND) == []
    p.clock.advance(300)
    assert p.live_instances == 1


def test_ray_down_terminates_head_and_workers_in_one_call(p: FakePlatform) -> None:
    """What Ray's teardown_cluster really sends: everything at once."""
    conf, _, head, head_id = cluster(p)
    launch(head, conf, "small", 2)
    p.clock.advance(300)
    client = make(p, conf["provider"])
    everything = client.non_terminated_nodes({})
    assert len(everything) == 3 and head_id in everything
    client.terminate_nodes(everything)
    assert client.non_terminated_nodes({}) == []
    client.cleanup_cluster_resources()
    assert p.live_instances == 0


def test_the_grace_period_does_not_apply_to_the_head(p: FakePlatform) -> None:
    """Other commands need the head to be up to date the moment they look."""
    conf, _, _, head_id = cluster(p, worker_start_grace_seconds=120)
    p.clock.advance(600)
    for _ in range(2):
        assert running_head(make(p, conf["provider"])) == head_id


def test_ray_up_does_not_destroy_a_cluster_that_is_still_being_created(
    p: FakePlatform,
) -> None:
    conf = config()
    settings = cfg.parse_provider_config(conf["provider"], "demo").settings()
    being_created = ClusterCore(p, p.clock).create(settings)  # no head task yet
    other = make(p, conf["provider"])
    assert other.non_terminated_nodes(HEAD_KIND) == []
    with pytest.raises(ClusterStateError, match="being created"):
        other.create_node(
            conf["available_node_types"]["head"]["node_config"], dict(HEAD_TAGS), 1
        )
    status = p.get_work_requirement(being_created.work_requirement_id).status
    assert status.is_live
    # ... but one that has had its chance and still has no head is cleared away
    p.clock.advance(11 * 60)
    head_id, _ = ray_up(make(p, conf["provider"]), conf)
    assert p.get_task(head_id).status is TaskStatus.EXECUTING
    assert not p.get_work_requirement(being_created.work_requirement_id).status.is_live


def test_a_worker_that_dies_before_it_is_ever_listed_does_not_linger(
    p: FakePlatform,
) -> None:
    conf, _, head, _ = cluster(p)
    real = p.add_tasks

    def add_and_fail_at_once(group_id: str, tasks: list[object]) -> list[object]:
        added = real(group_id, tasks)  # type: ignore[arg-type]
        for task in added:
            p.cancel_task(task.id)  # gone before any listing shows it
        return added  # type: ignore[return-value]

    p.add_tasks = add_and_fail_at_once  # type: ignore[method-assign,assignment]
    launch(head, conf, "small", 2)
    assert len(head.non_terminated_nodes(WORKER_KIND)) == 2  # not yet known to be gone
    p.clock.advance(90)
    assert head.non_terminated_nodes(WORKER_KIND) == []


def test_a_role_whose_pool_cannot_be_used_is_reported_each_time(
    p: FakePlatform,
) -> None:
    conf, _, head, _ = cluster(p)
    launch(head, conf, "small", 1)
    handle = ClusterCore(p, p.clock).attach("demo", NS)
    ClusterCore(p, p.clock).remove_pool(handle, naming.worker_role("small"))
    restarted = make(p, head.provider_config)
    for _ in range(2):
        with pytest.raises(ClusterStateError, match="pool"):
            launch(restarted, conf, "small", 1)
    assert restarted.non_terminated_nodes(WORKER_KIND) == []


def test_terminating_uses_the_cluster_the_nodes_were_listed_from(
    p: FakePlatform,
) -> None:
    """A stale node ID must never bring down a newer cluster of the same name."""
    conf, client, _, old_head = cluster(p)
    assert client.non_terminated_nodes({}) == [old_head]
    # Elsewhere: the cluster is replaced by a new one with the same name
    elsewhere = make(p, conf["provider"])
    ray_down(elsewhere)
    new_head, _ = ray_up(make(p, conf["provider"]), conf)
    # The first client, still holding the old listing, terminates the old head
    client.terminate_nodes([old_head])
    assert p.get_task(new_head).status is TaskStatus.EXECUTING
    assert running_head(make(p, conf["provider"])) == new_head


def test_the_client_knows_workers_public_addresses(p: FakePlatform) -> None:
    conf, client, head, _ = cluster(p)
    launch(head, conf, "small", 2)
    p.clock.advance(300)
    workers = client.non_terminated_nodes(WORKER_KIND)
    assert len(workers) == 2 and all(client.external_ip(w) for w in workers)
    head.non_terminated_nodes({})
    assert all(head.external_ip(w) is None for w in workers), "the head has no need"


def test_the_credentials_file_is_never_written_through_a_symlink(
    p: FakePlatform, home: Path
) -> None:
    directory = home / ".raydog" / "credentials"
    directory.mkdir(parents=True)
    target = home / "elsewhere"
    target.write_text("untouched")
    (directory / "ns.demo").symlink_to(target)
    client = make(p, config()["provider"])
    client.non_terminated_nodes({})
    assert target.read_text() == "untouched"
    written = directory / "ns.demo"
    assert not written.is_symlink() and "the-secret" in written.read_text()
    assert stat.S_IMODE(written.stat().st_mode) == 0o600
    os.chmod(written, 0o644)
    make(p, config()["provider"]).non_terminated_nodes({})
    assert stat.S_IMODE(written.stat().st_mode) == 0o600, "fixed even if unchanged"


# --- logging on the user's machine ------------------------------------------------


@pytest.fixture
def package_logger(monkeypatch: pytest.MonkeyPatch) -> logging.Logger:
    """RayDog's logger, put back as it was after the test."""
    logger = logging.getLogger("yellowdog_ray")
    monkeypatch.setattr(logger, "handlers", [])
    monkeypatch.setattr(logger, "level", logging.NOTSET)
    return logger


def test_raydog_log_level_shows_raydogs_lines_during_ray_up(
    p: FakePlatform,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    package_logger: logging.Logger,
) -> None:
    """
    Ray's command line sets up only its own logger, so RayDog's lines would go
    nowhere on the user's machine; RAYDOG_LOG_LEVEL shows them on stderr, once
    however many times Ray makes a provider.
    """
    monkeypatch.setenv("RAYDOG_LOG_LEVEL", "debug")
    make(p, config()["provider"])
    make(p, config()["provider"])
    logging.getLogger("yellowdog_ray.core.cluster").debug("a platform call")
    err = capsys.readouterr().err
    assert err.count("a platform call") == 1
    assert "DEBUG yellowdog_ray.core.cluster: a platform call" in err


def test_without_raydog_log_level_nothing_changes(
    p: FakePlatform,
    monkeypatch: pytest.MonkeyPatch,
    package_logger: logging.Logger,
) -> None:
    monkeypatch.delenv("RAYDOG_LOG_LEVEL", raising=False)
    make(p, config()["provider"])
    assert package_logger.handlers == [] and package_logger.level == logging.NOTSET


def test_raydog_log_level_is_not_acted_on_on_the_head_node(
    p: FakePlatform,
    monkeypatch: pytest.MonkeyPatch,
    package_logger: logging.Logger,
) -> None:
    """There, Ray's monitor already sends RayDog's lines to monitor.log."""
    monkeypatch.setenv("RAYDOG_LOG_LEVEL", "INFO")
    section = config()["provider"]
    section["raydog_head"] = {
        "work_requirement_id": "ydid:workreq:1",
        "credentials_file": "~/.raydog/credentials/ns.demo",
    }
    make(p, section)
    assert package_logger.handlers == []


def test_a_mistyped_raydog_log_level_is_reported_and_ignored(
    p: FakePlatform,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    package_logger: logging.Logger,
) -> None:
    monkeypatch.setenv("RAYDOG_LOG_LEVEL", "loud")
    make(p, config()["provider"])
    assert package_logger.handlers == []
    assert [r.levelname for r in caplog.records] == ["WARNING"]
    assert "RAYDOG_LOG_LEVEL=loud" in caplog.text


# --- Ctrl-C during ray up ---------------------------------------------------------


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]


def test_an_interrupted_ray_up_says_it_removed_the_cluster(
    p: FakePlatform, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Ctrl-C while ray up waits for the head node: the cluster is removed, and the
    user is told so at WARNING, which reaches the terminal with nothing set up.
    """
    conf = config()
    client = make(p, conf["provider"])
    p.fail_next("get_task", KeyboardInterrupt())  # type: ignore[arg-type]
    with pytest.raises(KeyboardInterrupt):
        client.create_node(
            conf["available_node_types"]["head"]["node_config"], dict(HEAD_TAGS), 1
        )
    p.clock.advance(100)
    assert p.live_instances == 0
    said = _warnings(caplog)
    assert any("interrupted" in m and "'demo'" in m for m in said), said
    assert any("removed" in m for m in said), said


def test_a_second_interrupt_during_removal_says_how_to_finish(
    p: FakePlatform, caplog: pytest.LogCaptureFixture
) -> None:
    conf = config()
    client = make(p, conf["provider"])
    p.fail_next("get_task", KeyboardInterrupt())  # type: ignore[arg-type]
    p.fail_next("cancel_work_requirement", KeyboardInterrupt())  # type: ignore[arg-type]
    with pytest.raises(KeyboardInterrupt):
        client.create_node(
            conf["available_node_types"]["head"]["node_config"], dict(HEAD_TAGS), 1
        )
    said = _warnings(caplog)
    assert any("raydog down demo -n ns" in m for m in said), said


def test_a_head_that_fails_to_start_says_the_cluster_was_removed(
    p: FakePlatform, caplog: pytest.LogCaptureFixture
) -> None:
    p.never_register("demo")
    conf = config(build_timeout_minutes=3)
    client = make(p, conf["provider"])
    with pytest.raises(BuildTimeoutError):
        client.create_node(
            conf["available_node_types"]["head"]["node_config"], dict(HEAD_TAGS), 1
        )
    assert any("removed" in m and "'demo'" in m for m in _warnings(caplog))


def test_ray_up_reports_a_head_that_did_not_start_without_a_traceback(
    p: FakePlatform,
) -> None:
    """
    Inside Ray's command line, the error is one Ray prints as a single line (a
    click exception), pointing to the troubleshooting page; the cluster is removed.
    """
    import click

    p.never_register("demo")
    conf = config(build_timeout_minutes=3)
    client = make(p, conf["provider"])
    with (
        click.Context(click.Command("up")),
        pytest.raises(click.ClickException) as caught,
    ):
        client.create_node(
            conf["available_node_types"]["head"]["node_config"], dict(HEAD_TAGS), 1
        )
    message = caught.value.format_message()
    assert "did not start within 3 minutes" in message and "never registered" in message
    assert "troubleshooting" in message
    assert caught.value.__cause__ is None
    p.clock.advance(100)
    assert p.live_instances == 0


# --- Ray's autoscaler v2 ----------------------------------------------------------

V2_LINE = 'export RAY_CLOUD_INSTANCE_ID="$YD_TASK_ID"'


def test_by_default_every_node_gets_what_autoscaler_v2_needs(p: FakePlatform) -> None:
    """
    Ray's autoscaler v2 matches its nodes to RayDog's by RAY_CLOUD_INSTANCE_ID (the
    node's task ID, known only once the task exists, so taken from YD_TASK_ID) and
    needs each node's type in RAY_NODE_TYPE_NAME; RAY_enable_autoscaler_v2 turns it on.
    """
    conf, _, head, head_id = cluster(p)
    launch(head, conf, "small", 1)
    nodes = head.non_terminated_nodes({})
    worker_id = next(n for n in nodes if n != head_id)
    for task_id, node_type in ((head_id, "head"), (worker_id, "small")):
        task = p.get_task(task_id)
        env = dict(task.environment)
        assert env["RAY_enable_autoscaler_v2"] == "1"
        assert env["RAY_NODE_TYPE_NAME"] == node_type
    scripts = [
        task.script for c in p.calls if c.method == "add_tasks" for task in c.args[1]
    ]
    assert len(scripts) == 2 and all(V2_LINE in s for s in scripts)


def test_with_the_original_autoscaler_nothing_is_added(p: FakePlatform) -> None:
    conf, _, head, _ = cluster(p, autoscaler_v2=False)
    launch(head, conf, "small", 1)
    for c in p.calls:
        if c.method == "add_tasks":
            for task in c.args[1]:
                assert V2_LINE not in task.script
                assert "RAY_enable_autoscaler_v2" not in task.environment
                assert "RAY_NODE_TYPE_NAME" not in task.environment


def test_the_head_has_the_resources_and_labels_its_type_declares(
    p: FakePlatform,
) -> None:
    """
    The head type's declaration is given to the head as well, so that it wins over
    whatever the head start script tells ``ray start``: Ray's own launcher leaves
    the head to its start commands, and the two can disagree silently.
    """
    raw = cluster_config(refresh_interval_seconds=0)
    raw["available_node_types"]["head"]["resources"] = {"CPU": 4}
    raw["available_node_types"]["head"]["labels"] = {"role": "head"}
    conf = cfg.bootstrap(raw, [Path.cwd()])
    head_id, _ = ray_up(make(p, conf["provider"]), conf)
    env = dict(p.get_task(head_id).environment)
    assert json.loads(env["RAY_OVERRIDE_RESOURCES"]) == {"CPU": 4}
    assert json.loads(env["RAY_OVERRIDE_LABELS"]) == {"role": "head"}

    # A head type that declares nothing: Ray finds out for itself, as it would
    undeclared = cluster_config(refresh_interval_seconds=0)
    del undeclared["available_node_types"]["head"]["resources"]
    conf = cfg.bootstrap(undeclared, [Path.cwd()])
    elsewhere = FakePlatform()
    head_id, _ = ray_up(make(elsewhere, conf["provider"]), conf)
    env = dict(elsewhere.get_task(head_id).environment)
    assert "RAY_OVERRIDE_RESOURCES" not in env
    assert "RAY_OVERRIDE_LABELS" not in env


@pytest.mark.parametrize("autoscaler_v2", [True, False])
def test_a_worker_has_the_resources_and_labels_its_type_declares(
    p: FakePlatform, autoscaler_v2: bool
) -> None:
    """
    Ray plans with the resources each node type declares, so its own launcher makes
    ``ray start`` use them (RAY_OVERRIDE_RESOURCES, RAY_OVERRIDE_LABELS) whatever the
    machine has; so does RayDog. Otherwise a node that has fewer CPUs than its type
    declares leaves Ray's plan short, and autoscaler v2 reports the work it cannot
    place as requests no node type can fulfill.
    """
    conf, _, head, head_id = cluster(p, autoscaler_v2=autoscaler_v2)
    launch(
        head,
        conf,
        "small",
        1,
        resources={"CPU": 2, "memory": 1e9},
        labels={"zone": "a"},
    )
    launch(head, conf, "small", 1, resources={}, labels={})
    workers = [n for n in head.non_terminated_nodes({}) if n != head_id]
    declared, undeclared = (dict(p.get_task(w).environment) for w in workers)
    assert json.loads(declared["RAY_OVERRIDE_RESOURCES"]) == {
        "CPU": 2,
        "memory": 1e9,
    }
    assert json.loads(declared["RAY_OVERRIDE_LABELS"]) == {"zone": "a"}
    assert "RAY_OVERRIDE_RESOURCES" not in undeclared
    assert "RAY_OVERRIDE_LABELS" not in undeclared


def test_the_instance_id_line_comes_after_the_shebang_and_works() -> None:
    script = with_cloud_instance_id('#!/usr/bin/bash\necho "$RAY_CLOUD_INSTANCE_ID"\n')
    assert script.startswith("#!/usr/bin/bash\n")
    ran = subprocess.run(
        ["bash", "-c", script],
        env={"YD_TASK_ID": "ydid:task:x:1:1", "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )
    assert ran.stdout.strip() == "ydid:task:x:1:1"
    assert with_cloud_instance_id("echo hi\n").startswith(V2_LINE)
