import dataclasses
from datetime import datetime, timedelta, timezone

import pytest

from yellowdog_ray.core import platform as p
from yellowdog_ray.core.clock import SystemClock


def test_task_status_partition() -> None:
    assert p.TaskStatus.EXECUTING.is_live
    assert p.TaskStatus.READY.is_live
    assert p.TaskStatus.ABORTED.is_terminal
    for status in p.TaskStatus:
        assert status.is_live != status.is_terminal


def test_work_requirement_cancelling_is_neither_live_nor_terminal() -> None:
    s = p.WorkRequirementStatus
    assert not s.CANCELLING.is_live and not s.CANCELLING.is_terminal
    assert s.RUNNING.is_live and not s.RUNNING.is_terminal
    assert s.CANCELLED.is_terminal and s.FAILED.is_terminal and s.COMPLETED.is_terminal


def test_pool_shutdown_is_not_live() -> None:
    assert p.PoolStatus.RUNNING.is_live
    assert p.PoolStatus.EMPTY.is_live
    assert not p.PoolStatus.SHUTDOWN.is_live
    assert not p.PoolStatus.TERMINATED.is_live


def test_statuses_take_the_platform_names() -> None:
    assert p.TaskStatus("EXECUTING") is p.TaskStatus.EXECUTING
    assert p.PoolStatus("CONFIGURING") is p.PoolStatus.CONFIGURING
    assert p.TaskGroupStatus("FAILING") is p.TaskGroupStatus.FAILING


def test_records_are_frozen() -> None:
    spec = p.TaskGroupSpec(name="head", worker_tag="demo_ab_h")
    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.name = "other"  # type: ignore[misc]


def test_spec_defaults() -> None:
    group = p.TaskGroupSpec(name="head", worker_tag="t")
    assert group.tag is None and group.task_timeout is None
    assert group.finish_if_any_task_failed is False
    task = p.TaskSpec(name="n1", script="echo hi")
    assert dict(task.environment) == {} and task.tag is None and task.timeout is None
    pool = p.PoolSpec(
        name="demo_ab_h",
        namespace="ns",
        template="t/t",
        worker_tag="demo_ab_h",
        target_instances=1,
        max_nodes=1,
    )
    assert pool.idle_node_shutdown == timedelta(minutes=3)
    assert pool.idle_pool_shutdown == timedelta(minutes=3)
    assert dict(pool.instance_tags) == {} and pool.metrics_enabled is False


def test_mapping_defaults_are_not_shared() -> None:
    a = p.TaskSpec(name="a", script="")
    b = p.TaskSpec(name="b", script="")
    assert a.environment is not b.environment


def test_info_records_hold_what_callers_need() -> None:
    group = p.TaskGroupInfo(
        id="g", name="head", tag=None, status=p.TaskGroupStatus.RUNNING
    )
    wr = p.WorkRequirementInfo(
        id="w",
        name="demo_ab",
        namespace="ns",
        tag=None,
        status=p.WorkRequirementStatus.RUNNING,
        task_groups=(group,),
        created=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    assert wr.task_groups[0].name == "head"
    assert hash(group)
    node = p.NodeInfo(
        id="n",
        pool_id="pool",
        worker_ids=("w1",),
        private_ip="10.0.0.1",
        instance_id="i-1",
    )
    assert node.worker_ids == ("w1",)


def test_system_clock_is_utc() -> None:
    assert SystemClock().now().tzinfo is not None
