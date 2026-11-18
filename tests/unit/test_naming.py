from __future__ import annotations

import re

import pytest

from yellowdog_ray.core import naming
from yellowdog_ray.core.platform import PoolSpec, TaskGroupSpec, TaskSpec
from yellowdog_ray.errors import ConfigError
from yellowdog_ray.testing import FakePlatform


@pytest.mark.parametrize("name", ["a", "demo", "demo-2", "my-cluster-01", "a" * 24])
def test_valid_cluster_names(name: str) -> None:
    naming.validate_cluster_name(name)


@pytest.mark.parametrize(
    "name",
    ["", "Demo", "demo_x", "demo.x", "1demo", "-demo", "demo-", "a" * 25, "de mo"],
)
def test_invalid_cluster_names(name: str) -> None:
    with pytest.raises(ConfigError):
        naming.validate_cluster_name(name)


def test_new_suffix_is_eight_safe_characters() -> None:
    suffixes = {naming.new_suffix() for _ in range(200)}
    assert len(suffixes) == 200
    assert all(re.fullmatch(r"[a-z0-9]{8}", s) for s in suffixes)


def test_simple_worker_names_are_kept() -> None:
    assert naming.worker_role("small") == "w-small"
    assert naming.worker_role("gpu-a10") == "w-gpu-a10"


@pytest.mark.parametrize(
    "name",
    ["ray.worker.default", "Ray_Worker", "x" * 40, "---", "a b", "trailing-", "é"],
)
def test_awkward_worker_names_are_made_safe(name: str) -> None:
    role = naming.worker_role(name)
    assert re.fullmatch(r"w-[a-z0-9]([a-z0-9-]*[a-z0-9])?", role), role
    assert len(role) <= 26
    assert re.search(r"-[0-9a-f]{8}$", role), "a changed name carries a hash"
    assert naming.worker_role(name) == role  # stable


def test_different_names_never_share_a_role() -> None:
    names = ["a.b", "a_b", "a-b", "A-B", "a--b", "x" * 40, "x" * 41]
    roles = {naming.worker_role(n) for n in names}
    assert len(roles) == len(names)
    assert naming.worker_role("head") != naming.HEAD_ROLE
    assert naming.worker_role("observability") != naming.OBSERVABILITY_ROLE


def test_longest_names_are_accepted_by_the_platform() -> None:
    cluster = "c" * 24
    role = naming.worker_role("n" * 24)
    assert len(role) == 26
    wr_name = naming.work_requirement_name(cluster, naming.new_suffix())
    pool = naming.pool_name(wr_name, role)
    assert len(wr_name) == 33 and len(pool) == 60
    p = FakePlatform()
    wr = p.create_work_requirement(
        "ns", wr_name, None, [TaskGroupSpec(name=role, worker_tag=pool)]
    )
    p.provision_pool(
        PoolSpec(name=pool, namespace="ns", template="t", worker_tag=pool,
                 target_instances=0, max_nodes=1)
    )  # fmt: skip
    p.add_tasks(
        wr.task_groups[0].id, [TaskSpec(name=naming.node_task_name(role), script="")]
    )


def test_pool_name_too_long_is_a_config_error() -> None:
    with pytest.raises(ConfigError):
        naming.pool_name("c" * 24 + "_abcdefgh", "w-" + "n" * 30)


def test_work_requirement_name_round_trip() -> None:
    name = naming.work_requirement_name("demo-2", "ab12cd34")
    assert name == "demo-2_ab12cd34"
    assert naming.parse_work_requirement_name(name) == ("demo-2", "ab12cd34")


def test_pool_name_round_trip() -> None:
    name = naming.pool_name("demo_ab12cd34", "w-small")
    assert name == "demo_ab12cd34_w-small"
    assert naming.parse_pool_name(name) == ("demo", "ab12cd34", "w-small")
    assert naming.parse_pool_name("demo_ab12cd34_head") == ("demo", "ab12cd34", "head")


def test_a_cluster_name_that_looks_like_a_suffix_is_not_split() -> None:
    assert naming.parse_pool_name("demo-ab12cd34_zzzzzzzz_head") == (
        "demo-ab12cd34",
        "zzzzzzzz",
        "head",
    )
    assert naming.parse_work_requirement_name("demo-ab12cd34_zzzzzzzz") == (
        "demo-ab12cd34",
        "zzzzzzzz",
    )


@pytest.mark.parametrize(
    "name",
    [
        "demo-ab12cd34",  # v2 autoscaler style: no separator
        "raytest-00-head",  # v2 builder style
        "demo_short",  # suffix not 8 characters
        "demo_ab12cd34_h",  # a pool name is not a work requirement name
        "demo_ab12cd34_",
        "Demo_ab12cd34",
        "",
    ],
)
def test_foreign_names_do_not_parse_as_work_requirements(name: str) -> None:
    assert naming.parse_work_requirement_name(name) is None


@pytest.mark.parametrize(
    "name",
    ["demo_ab12cd34", "demo-ab12cd34-h", "demo_ab12cd34_h_x", "demo_ab12cd34_H", ""],
)
def test_foreign_names_do_not_parse_as_pools(name: str) -> None:
    assert naming.parse_pool_name(name) is None


def test_group_marker_round_trip() -> None:
    plain = naming.group_marker("demo", "w-small")
    assert plain == "raydog:1:demo:w-small"
    assert naming.parse_group_marker(plain) == ("demo", "w-small", {})
    full = naming.group_marker("demo", "head", {"l": 86400, "i": 180, "b": 600})
    assert full == "raydog:1:demo:head:b=600,i=180,l=86400"
    assert naming.parse_group_marker(full) == (
        "demo",
        "head",
        {"l": 86400, "i": 180, "b": 600},
    )
    assert (
        len(
            naming.group_marker(
                "c" * 24, "w-" + "n" * 24, {"l": 10**9, "i": 10**6, "b": 10**6}
            )
        )
        <= 200
    )


@pytest.mark.parametrize(
    "tag",
    [
        None,
        "",
        "my-ray-tag",
        "raydog:2:demo:head",
        "raydog:1:demo",
        "raydog:1:demo:head:l=x",
        "raydog:1::head",
    ],
)
def test_foreign_tags_are_not_markers(tag: str | None) -> None:
    assert naming.parse_group_marker(tag) is None


def test_instance_tags_and_ownership() -> None:
    tags = naming.instance_tags("demo", "demo_ab12cd34")
    assert tags == {
        "raydog-cluster": "demo",
        "raydog-work-requirement": "demo_ab12cd34",
    }
    assert naming.pool_belongs({**tags, "user": "x"}, "demo", "demo_ab12cd34")
    assert not naming.pool_belongs(tags, "demo", "demo_zzzzzzzz")
    assert not naming.pool_belongs(tags, "demo2", "demo_ab12cd34")
    assert not naming.pool_belongs({}, "demo", "demo_ab12cd34")
    assert not naming.pool_belongs({"raydog-cluster": "demo"}, "demo", "demo_ab12cd34")


def test_node_task_names_are_unique_and_valid() -> None:
    role = naming.worker_role("n" * 24)
    names = {naming.node_task_name(role) for _ in range(1000)}
    assert len(names) == 1000
    assert all(
        len(n) <= 60 and re.fullmatch(r"[a-z][a-z0-9-]*[a-z0-9]", n) for n in names
    )
    assert naming.node_task_name("head").startswith("head-")


# --- added after the phase 3 review ------------------------------------------------


@pytest.mark.parametrize(
    "role",
    ["head", "observability", "w-small", "w-a", "w-gpu-a10-1a2b3c4d"],
)
def test_valid_roles(role: str) -> None:
    naming.validate_role(role)


@pytest.mark.parametrize(
    "role",
    [
        "",
        "w_x",
        "w-",
        "small",
        "h",
        "o",
        "worker-small",
        "w-Up",
        "w-a_b",
        "w-" + "n" * 25,
        "h-x",
        "w--a-",
    ],
)
def test_invalid_roles(role: str) -> None:
    with pytest.raises(ConfigError):
        naming.validate_role(role)


def test_every_worker_role_is_a_valid_role() -> None:
    for name in ["small", "ray.worker.default", "x" * 40, "---", "a", "0", "a-b"]:
        naming.validate_role(naming.worker_role(name))


def test_an_empty_worker_name_is_rejected() -> None:
    with pytest.raises(ConfigError):
        naming.worker_role("")


def test_a_name_cannot_imitate_another_names_hashed_role() -> None:
    hashed = naming.worker_role("Foo")  # altered, so it carries a hash
    imitation = hashed.removeprefix("w-")
    assert naming.worker_role(imitation) != hashed
    assert naming.worker_role("---") != naming.worker_role(
        naming.worker_role("---").removeprefix("w-")
    )


def test_many_awkward_names_do_not_collide() -> None:
    names = [
        f"gpu{sep}pool{sep2}{i}"
        for sep in ".*_ /"
        for sep2 in "._*"
        for i in range(400)
    ]
    roles = {naming.worker_role(n) for n in names}
    assert len(roles) == len(names)


@pytest.mark.parametrize(
    "tag",
    [
        "raydog:1:demo:head:l=²",
        "raydog:1:demo:head:l=1,l=2",
        "raydog:1:demo:head:=5",
        "raydog:1:demo:head:l",
    ],
)
def test_malformed_marker_settings_do_not_parse_or_raise(tag: str) -> None:
    assert naming.parse_group_marker(tag) is None
