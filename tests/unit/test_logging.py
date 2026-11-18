"""
What RayDog logs. INFO tells the story of a cluster's life; DEBUG adds every
platform call and how long it took; nothing secret is logged at any level.
"""

from __future__ import annotations

import contextlib
import logging
from datetime import timedelta

import pytest

from yellowdog_ray import ClusterSpec, NodeSpec, RayDogCluster
from yellowdog_ray.cli import main
from yellowdog_ray.testing import FakePlatform

NS = "ns"
SECRET = "s3cr3t-value"
HEAD = NodeSpec(
    compute_requirement_template="ns/head",
    start_script=f"echo {SECRET}; ray start --head",
    userdata=f"#!/bin/bash\nTOKEN={SECRET}\n",
)
WORKER = NodeSpec(
    compute_requirement_template="ns/worker", start_script=f"echo {SECRET}; ray start"
)


@pytest.fixture
def p() -> FakePlatform:
    return FakePlatform()


def messages(caplog: pytest.LogCaptureFixture, level: int = logging.INFO) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno >= level and r.name.startswith("yellowdog_ray")
    ]


def test_a_cluster_s_life_is_told_at_info(
    p: FakePlatform, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="yellowdog_ray")
    cluster = RayDogCluster(
        ClusterSpec(name="demo", namespace=NS, head=HEAD), platform=p, clock=p.clock
    )
    cluster.add_worker_pool("small", WORKER, count=2)
    head = cluster.build()
    cluster.resize_worker_pool("small", 1)
    cluster.remove_worker_pool("small")
    cluster.shut_down(wait=True)

    told = "\n".join(messages(caplog))
    for expected in (
        "building cluster demo with 1 worker pool",
        "created cluster demo in namespace ns",
        "added worker pool demo_",
        "_head for 1 node",
        "_w-small for 2 node",
        "started 1 head node(s) in cluster demo",
        f"node head of cluster demo is running at {head.private_ip}",
        "started 2 w-small node(s) in cluster demo",
        "cluster demo is built",
        "stopping 1 node(s) of cluster demo",
        "removed the w-small pool of cluster demo",
        "shutting down cluster demo",
        "cluster demo is shut down and no instance remains",
    ):
        assert expected in told, f"{expected!r} not in:\n{told}"


def test_looking_at_a_cluster_logs_nothing_at_info(
    p: FakePlatform, caplog: pytest.LogCaptureFixture
) -> None:
    cluster = RayDogCluster(
        ClusterSpec(name="demo", namespace=NS, head=HEAD), platform=p, clock=p.clock
    )
    cluster.build()
    caplog.set_level(logging.INFO, logger="yellowdog_ray")
    cluster.status()
    RayDogCluster.attach("demo", NS, platform=p, clock=p.clock).status()
    assert messages(caplog) == []


def test_an_incomplete_shutdown_is_a_warning(
    p: FakePlatform, caplog: pytest.LogCaptureFixture
) -> None:
    cluster = RayDogCluster(
        ClusterSpec(name="demo", namespace=NS, head=HEAD), platform=p, clock=p.clock
    )
    cluster.build()
    caplog.set_level(logging.INFO, logger="yellowdog_ray")
    p.fail_next("remove_pool", RuntimeError("boom"))
    with pytest.raises(Exception):  # noqa: B017 - which one is the teardown's business
        cluster.shut_down()
    warnings = messages(caplog, logging.WARNING)
    assert any("did not complete" in m for m in warnings), warnings


def test_nothing_secret_is_logged_even_at_debug(
    p: FakePlatform, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="yellowdog_ray")
    cluster = RayDogCluster(
        ClusterSpec(name="demo", namespace=NS, head=HEAD), platform=p, clock=p.clock
    )
    cluster.add_worker_pool("small", WORKER, count=1)
    cluster.build()
    cluster.shut_down(wait=True)
    assert caplog.records
    assert not [m for m in messages(caplog, logging.DEBUG) if SECRET in m]


def test_raydog_is_quiet_unless_asked(
    p: FakePlatform, capsys: pytest.CaptureFixture[str]
) -> None:
    RayDogCluster(
        ClusterSpec(name="demo", namespace=NS, head=HEAD), platform=p, clock=p.clock
    ).build()
    assert main(["down", "demo", "-n", NS, "--yes"], platform=p, clock=p.clock) == 0
    assert "shutting down" not in capsys.readouterr().err


@pytest.mark.parametrize("flag", ["-v", "--verbose"])
def test_raydog_verbose_logs_the_story(
    p: FakePlatform, capsys: pytest.CaptureFixture[str], flag: str
) -> None:
    RayDogCluster(
        ClusterSpec(name="demo", namespace=NS, head=HEAD), platform=p, clock=p.clock
    ).build()
    assert (
        main([flag, "down", "demo", "-n", NS, "--yes"], platform=p, clock=p.clock) == 0
    )
    err = capsys.readouterr().err
    assert "INFO" in err and "shutting down cluster demo" in err


def test_raydog_verbose_works_after_the_command_too(
    p: FakePlatform, capsys: pytest.CaptureFixture[str]
) -> None:
    RayDogCluster(
        ClusterSpec(name="demo", namespace=NS, head=HEAD), platform=p, clock=p.clock
    ).build()
    assert (
        main(["down", "demo", "-n", NS, "--yes", "-v"], platform=p, clock=p.clock) == 0
    )
    assert "shutting down cluster demo" in capsys.readouterr().err


def test_raydog_leaves_logging_as_it_found_it(p: FakePlatform) -> None:
    package = logging.getLogger("yellowdog_ray")
    before = (package.level, list(package.handlers))
    main(["-vv", "list", "-n", NS], platform=p, clock=p.clock)
    assert (package.level, list(package.handlers)) == before


def test_platform_calls_are_logged_at_debug_with_their_duration(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from unittest.mock import MagicMock

    from yellowdog_ray.core.yellowdog import YellowDogPlatform

    client = MagicMock()
    client.work_client.cancel_task_by_id.return_value = None
    platform = YellowDogPlatform(client)
    caplog.set_level(logging.DEBUG, logger="yellowdog_ray")
    platform.cancel_task("ydid:task:1")
    (record,) = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert record.getMessage().startswith("cancel_task(ydid:task:1) took ")
    assert record.getMessage().endswith("s")

    caplog.clear()
    client.work_client.cancel_task_by_id.side_effect = RuntimeError("down")
    with pytest.raises(RuntimeError):
        platform.cancel_task("ydid:task:2")
    (record,) = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert "cancel_task(ydid:task:2) failed after" in record.getMessage()
    assert "RuntimeError" in record.getMessage()


def test_platform_call_logging_names_things_but_never_shows_their_contents(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from unittest.mock import MagicMock

    from yellowdog_ray.core import platform as plat
    from yellowdog_ray.core.yellowdog import YellowDogPlatform

    client = MagicMock()
    platform = YellowDogPlatform(client)
    caplog.set_level(logging.DEBUG, logger="yellowdog_ray")
    task = plat.TaskSpec(
        name="w-small-ab12",
        script=f"echo {SECRET}",
        environment={"TOKEN": SECRET},
        timeout=timedelta(hours=1),
    )
    with contextlib.suppress(Exception):  # the mock's answer need not make sense
        platform.add_tasks("ydid:taskgrp:1", [task, task])
    pool = plat.PoolSpec(
        name="demo_ab_w-small",
        namespace=NS,
        template="ns/worker",
        worker_tag="demo_ab_w-small",
        target_instances=1,
        max_nodes=2,
        userdata=f"TOKEN={SECRET}",
    )
    with contextlib.suppress(Exception):
        platform.provision_pool(pool)
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert "add_tasks(ydid:taskgrp:1, 2 task(s))" in logged
    assert "provision_pool(demo_ab_w-small)" in logged
    assert SECRET not in logged
