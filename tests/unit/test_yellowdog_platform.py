"""The SDK adapter, against autospec'd SDK clients: no network."""

from __future__ import annotations

import typing
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, create_autospec

import pytest
import requests
from yellowdog_client import PlatformClient
from yellowdog_client import model as yd
from yellowdog_client.model.exceptions import InvalidRequestException

from yellowdog_ray.core import platform as p
from yellowdog_ray.core.yellowdog import YellowDogPlatform
from yellowdog_ray.errors import (
    InvalidRequestError,
    NameInUseError,
    NotFoundError,
    PlatformError,
)

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def client() -> MagicMock:
    hints = typing.get_type_hints(PlatformClient.__init__)
    mock = MagicMock()
    for name in ("work_client", "worker_pool_client", "compute_client"):
        setattr(mock, name, create_autospec(hints[name], instance=True))
    return mock


@pytest.fixture
def platform(client: MagicMock) -> YellowDogPlatform:
    return YellowDogPlatform(client)


def http_error(status: int, error_type: str = "SomeException") -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status
    response._content = (
        b'{"detail":[],"errorType":"' + error_type.encode() + b'","message":"m"}'
    )
    return requests.HTTPError(f"{status} Client Error", response=response)


def search(items: list[Any]) -> SimpleNamespace:
    return SimpleNamespace(list_all=lambda: items, iterate=lambda: iter(items))


def sdk_work_requirement(status: str = "RUNNING") -> yd.WorkRequirement:
    wr = yd.WorkRequirement(
        namespace="ns",
        name="demo_ab",
        tag="user",
        taskGroups=[
            yd.TaskGroup(
                name="head",
                tag="raydog:1:demo:head",
                runSpecification=yd.RunSpecification(taskTypes=["bash"]),
            )
        ],
    )
    wr.id = "ydid:workreq:1"
    wr.status = yd.WorkRequirementStatus[status]
    wr.createdTime = NOW
    assert wr.taskGroups is not None
    wr.taskGroups[0].id = "ydid:taskgrp:1:1"
    wr.taskGroups[0].status = yd.TaskGroupStatus.RUNNING
    return wr


def sdk_task(status: str = "EXECUTING") -> yd.Task:
    task = yd.Task(taskType="bash", name="n1", tag="k", environment={"A": "1"})
    task.id = "ydid:task:1:1:1"
    task.status = yd.TaskStatus[status]
    task.taskGroupId = "ydid:taskgrp:1:1"
    task.workerId = "ydid:wrkr:9:1"
    return task


def sdk_pool(status: str = "RUNNING") -> yd.ProvisionedWorkerPool:
    pool = yd.ProvisionedWorkerPool()
    pool.id = "ydid:wrkrpool:1"
    pool.name = "demo_ab_head"
    pool.namespace = "ns"
    pool.status = yd.WorkerPoolStatus[status]
    pool.createdTime = NOW
    pool.computeRequirementId = "ydid:compreq:1"
    return pool


def sdk_compute_requirement(status: str = "RUNNING") -> SimpleNamespace:
    source = SimpleNamespace(instanceTags={"raydog-cluster": "demo"})
    return SimpleNamespace(
        id="ydid:compreq:1",
        status=yd.ComputeRequirementStatus[status],
        tag="user",
        targetInstanceCount=2,
        provisionStrategy=SimpleNamespace(sources=[SimpleNamespace(), source]),
    )


# --- work requirements -------------------------------------------------------


def test_create_work_requirement_builds_the_sdk_model(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.work_client.add_work_requirement.return_value = sdk_work_requirement()
    info = platform.create_work_requirement(
        "ns",
        "demo_ab",
        "user",
        [
            p.TaskGroupSpec(
                name="head",
                worker_tag="demo_ab_head",
                tag="raydog:1:demo:head",
                task_timeout=timedelta(hours=24),
                finish_if_any_task_failed=True,
            )
        ],
    )
    sent = client.work_client.add_work_requirement.call_args.args[0]
    assert (sent.namespace, sent.name, sent.tag) == ("ns", "demo_ab", "user")
    grp = sent.taskGroups[0]
    assert (grp.name, grp.tag, grp.finishIfAnyTaskFailed) == (
        "head",
        "raydog:1:demo:head",
        True,
    )
    assert grp.finishIfAllTasksFinished is False
    spec = grp.runSpecification
    assert spec.taskTypes == ["bash"] and spec.workerTags == ["demo_ab_head"]
    assert spec.namespaces == ["ns"] and spec.taskTimeout == timedelta(hours=24)
    assert (
        info.id == "ydid:workreq:1" and info.status is p.WorkRequirementStatus.RUNNING
    )
    assert info.task_groups[0].tag == "raydog:1:demo:head" and info.created == NOW


def test_add_task_group_updates_and_returns_the_new_group(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.work_client.get_work_requirement_by_id.return_value = sdk_work_requirement()

    def update(wr: yd.WorkRequirement) -> yd.WorkRequirement:
        assert wr.taskGroups is not None
        wr.taskGroups[-1].id = "ydid:taskgrp:1:2"
        wr.taskGroups[-1].status = yd.TaskGroupStatus.RUNNING
        return wr

    client.work_client.update_work_requirement.side_effect = update
    info = platform.add_task_group(
        "ydid:workreq:1",
        p.TaskGroupSpec(name="w-a", worker_tag="demo_ab_w-a"),
    )
    sent = client.work_client.update_work_requirement.call_args.args[0]
    assert [g.name for g in sent.taskGroups] == ["head", "w-a"]
    assert sent.taskGroups[1].runSpecification.namespaces == ["ns"]
    assert info.id == "ydid:taskgrp:1:2" and info.name == "w-a"


def test_find_work_requirement_returns_none_on_404(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.work_client.get_work_requirement_by_name.side_effect = http_error(
        404, "WorkRequirementNotFoundException"
    )
    assert platform.find_work_requirement("ns", "nope") is None
    client.work_client.get_work_requirement_by_name.assert_called_once_with(
        "ns", "nope"
    )


def test_get_work_requirement_raises_not_found(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.work_client.get_work_requirement_by_id.side_effect = http_error(
        404, "WorkRequirementNotFoundException"
    )
    with pytest.raises(NotFoundError) as caught:
        platform.get_work_requirement("x")
    assert caught.value.status_code == 404
    assert caught.value.error_type == "WorkRequirementNotFoundException"


def test_list_work_requirements_searches_and_summarises(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    summary = SimpleNamespace(
        id="i",
        name="demo_ab",
        namespace="ns",
        tag=None,
        status=yd.WorkRequirementStatus.RUNNING,
        createdTime=NOW,
    )
    client.work_client.get_work_requirements.return_value = search([summary])
    result = platform.list_work_requirements("ns", name_contains="demo", live_only=True)
    sent = client.work_client.get_work_requirements.call_args.args[0]
    assert (sent.namespace, sent.name) == ("ns", "demo")
    assert set(sent.statuses) == {
        yd.WorkRequirementStatus.RUNNING,
        yd.WorkRequirementStatus.HELD,
    }
    assert result == [
        p.WorkRequirementSummary(
            "i", "demo_ab", "ns", None, p.WorkRequirementStatus.RUNNING, NOW
        )
    ]
    platform.list_work_requirements("ns")
    assert client.work_client.get_work_requirements.call_args.args[0].statuses is None


def test_cancel_work_requirement_aborts(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    platform.cancel_work_requirement("w")
    client.work_client.cancel_work_requirement_by_id.assert_called_once_with(
        "w", abort=True
    )


def test_cancel_swallows_only_the_already_ended_error(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    cancel = client.work_client.cancel_work_requirement_by_id
    cancel.side_effect = http_error(403, "InvalidWorkRequirementStatusException")
    platform.cancel_work_requirement("w")
    cancel.side_effect = http_error(403, "NotAuthorisedException")
    with pytest.raises(PlatformError) as caught:
        platform.cancel_work_requirement("w")
    assert caught.value.status_code == 403
    assert caught.value.error_type == "NotAuthorisedException"
    assert not isinstance(caught.value, NotFoundError)


# --- tasks -------------------------------------------------------------------


def test_add_tasks_builds_bash_tasks(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.work_client.add_tasks_to_task_group_by_id.return_value = [sdk_task("READY")]
    result = platform.add_tasks(
        "g",
        [
            p.TaskSpec(
                name="n1",
                script="ray start",
                environment={"A": "1"},
                tag="k",
                timeout=timedelta(minutes=5),
            )
        ],
    )
    group_id, tasks = client.work_client.add_tasks_to_task_group_by_id.call_args.args
    task = tasks[0]
    assert group_id == "g"
    assert (task.name, task.taskType, task.taskData) == ("n1", "bash", "ray start")
    assert task.arguments == ["taskdata.txt"] and task.environment == {"A": "1"}
    assert task.tag == "k" and task.timeout == timedelta(minutes=5)
    assert result[0].status is p.TaskStatus.READY
    assert result[0].task_group_id == "ydid:taskgrp:1:1"
    assert dict(result[0].environment) == {"A": "1"}


def test_list_tasks_live_only_filters_by_status(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.work_client.get_tasks.return_value = search([sdk_task()])
    result = platform.list_tasks("w", task_group_id="g", live_only=True)
    sent = client.work_client.get_tasks.call_args.args[0]
    assert (sent.workRequirementId, sent.taskGroupId) == ("w", "g")
    assert yd.TaskStatus.EXECUTING in sent.statuses
    assert yd.TaskStatus.FAILED not in sent.statuses
    assert result[0].worker_id == "ydid:wrkr:9:1"
    platform.list_tasks("w")
    assert client.work_client.get_tasks.call_args.args[0].statuses is None


def test_cancel_task_aborts_and_tolerates_ended(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    platform.cancel_task("t")
    client.work_client.cancel_task_by_id.assert_called_once_with("t", True)
    client.work_client.cancel_task_by_id.side_effect = http_error(
        403, "InvalidTaskStatusException"
    )
    platform.cancel_task("t")


# --- pools ---------------------------------------------------------------------


def test_provision_pool_builds_both_sdk_models(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.worker_pool_client.provision_worker_pool.return_value = sdk_pool("PENDING")
    info = platform.provision_pool(
        p.PoolSpec(
            name="demo_ab_head",
            namespace="ns",
            template="ns/tmpl",
            worker_tag="demo_ab_head",
            target_instances=1,
            max_nodes=3,
            images_id="ami-1",
            userdata="#!/bin/bash",
            instance_tags={"raydog-cluster": "demo"},
            tag="user",
            metrics_enabled=True,
            idle_node_shutdown=timedelta(seconds=0),
            idle_pool_shutdown=timedelta(hours=1),
        )
    )
    usage, props = client.worker_pool_client.provision_worker_pool.call_args.args
    assert (usage.templateId, usage.requirementName, usage.requirementNamespace) == (
        "ns/tmpl",
        "demo_ab_head",
        "ns",
    )
    assert (usage.requirementTag, usage.targetInstanceCount) == ("user", 1)
    assert (usage.imagesId, usage.userData) == ("ami-1", "#!/bin/bash")
    assert usage.instanceTags == {"raydog-cluster": "demo"}
    assert (props.minNodes, props.maxNodes, props.workerTag) == (0, 3, "demo_ab_head")
    assert props.metricsEnabled is True
    assert props.createNodeWorkers.targetCount == 1
    assert (
        props.idleNodeShutdown.enabled
        and props.idleNodeShutdown.timeout == timedelta(0)
    )
    assert props.idlePoolShutdown.timeout == timedelta(hours=1)
    assert info.status is p.PoolStatus.PENDING and info.target_instances == 1
    assert dict(info.instance_tags) == {"raydog-cluster": "demo"} and info.tag == "user"


def test_provision_pool_omits_empty_instance_tags(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.worker_pool_client.provision_worker_pool.return_value = sdk_pool("PENDING")
    platform.provision_pool(
        p.PoolSpec(
            name="a",
            namespace="ns",
            template="t",
            worker_tag="a",
            target_instances=0,
            max_nodes=1,
        )
    )
    usage = client.worker_pool_client.provision_worker_pool.call_args.args[0]
    assert usage.instanceTags is None


def test_get_pool_reads_tags_and_target_from_the_compute_requirement(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.worker_pool_client.get_worker_pool_by_id.return_value = sdk_pool()
    client.compute_client.get_compute_requirement_by_id.return_value = (
        sdk_compute_requirement()
    )
    info = platform.get_pool("ydid:wrkrpool:1")
    assert info.tag == "user" and info.target_instances == 2
    assert dict(info.instance_tags) == {"raydog-cluster": "demo"}
    assert info.status is p.PoolStatus.RUNNING and info.created == NOW


def test_find_pool_returns_none_on_404(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.worker_pool_client.get_worker_pool_by_name.side_effect = http_error(
        404, "WorkerPoolNotFoundException"
    )
    assert platform.find_pool("ns", "nope") is None


def test_set_pool_target_resizes(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    platform.set_pool_target("pool", 7)
    client.worker_pool_client.resize_worker_pool_by_id.assert_called_once_with(
        "pool", 7
    )


def test_remove_pool_terminates_compute_then_shuts_down(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.worker_pool_client.get_worker_pool_by_id.return_value = sdk_pool()
    order = MagicMock()
    order.attach_mock(
        client.compute_client.terminate_compute_requirement_by_id, "terminate"
    )
    order.attach_mock(client.worker_pool_client.shutdown_worker_pool_by_id, "shutdown")
    platform.remove_pool("ydid:wrkrpool:1")
    assert [c[0] for c in order.mock_calls] == ["terminate", "shutdown"]
    client.compute_client.terminate_compute_requirement_by_id.assert_called_once_with(
        "ydid:compreq:1"
    )


def test_remove_pool_that_no_longer_exists_is_not_an_error(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.worker_pool_client.get_worker_pool_by_id.side_effect = http_error(
        404, "WorkerPoolNotFoundException"
    )
    platform.remove_pool("gone")


def instance(instance_id: str, status: str, public_ip: str | None) -> SimpleNamespace:
    return SimpleNamespace(
        id=SimpleNamespace(instanceId=instance_id),
        status=yd.InstanceStatus[status],
        publicIpAddress=public_ip,
    )


def test_live_instance_count_and_public_ips(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.worker_pool_client.get_worker_pool_by_id.return_value = sdk_pool()
    client.compute_client.get_instances.return_value = search(
        [
            instance("i-1", "RUNNING", "1.2.3.4"),
            instance("i-2", "TERMINATING", None),
            instance("i-3", "TERMINATED", "5.6.7.8"),
            instance("i-4", "PENDING", None),
        ]
    )
    assert platform.live_instance_count("ydid:wrkrpool:1") == 3
    assert platform.public_ips("ydid:wrkrpool:1") == {"i-1": "1.2.3.4"}
    sent = client.compute_client.get_instances.call_args.args[0]
    assert sent.computeRequirementId == "ydid:compreq:1"


def test_list_nodes_returns_registered_nodes(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    def node(node_id: str, status: str) -> SimpleNamespace:
        return SimpleNamespace(
            id=node_id,
            workerPoolId="pool",
            status=yd.NodeStatus[status],
            workers=[SimpleNamespace(id=f"{node_id}:1")],
            details=SimpleNamespace(privateIpAddress="10.0.0.1", instanceId="i-1"),
        )

    client.worker_pool_client.get_nodes.return_value = search(
        [node("n1", "RUNNING"), node("n2", "TERMINATED"), node("n3", "DEREGISTERED")]
    )
    nodes = platform.list_nodes("pool")
    assert client.worker_pool_client.get_nodes.call_args.args[0].workerPoolId == "pool"
    assert nodes == [
        p.NodeInfo(
            id="n1",
            pool_id="pool",
            worker_ids=("n1:1",),
            private_ip="10.0.0.1",
            instance_id="i-1",
        )
    ]


# --- error mapping -------------------------------------------------------------


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("A work requirement already exists with the same name 'x'", NameInUseError),
        (
            "Worker pool already exists with the same name in this namespace",
            NameInUseError,
        ),
        ("Task names must be unique within task group", NameInUseError),
        ("must not contain task groups with duplicate names", NameInUseError),
        ("Request values are invalid. name must not be longer", InvalidRequestError),
    ],
)
def test_invalid_request_mapping(
    platform: YellowDogPlatform, client: MagicMock, message: str, expected: type
) -> None:
    client.work_client.get_task_by_id.side_effect = InvalidRequestException(message)
    with pytest.raises(InvalidRequestError) as caught:
        platform.get_task("t")
    assert type(caught.value) is expected
    assert caught.value.error_type == "InvalidRequestException"


@pytest.mark.parametrize(
    ("status", "retryable"), [(429, True), (500, True), (503, True), (400, False)]
)
def test_http_errors_become_platform_errors(
    platform: YellowDogPlatform, client: MagicMock, status: int, retryable: bool
) -> None:
    client.work_client.get_task_by_id.side_effect = http_error(status, "Whatever")
    with pytest.raises(PlatformError) as caught:
        platform.get_task("t")
    assert (caught.value.status_code, caught.value.retryable) == (status, retryable)
    assert caught.value.error_type == "Whatever"


def test_connection_errors_are_retryable_platform_errors(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.work_client.get_task_by_id.side_effect = requests.ConnectionError("no route")
    with pytest.raises(PlatformError) as caught:
        platform.get_task("t")
    assert caught.value.retryable and caught.value.status_code is None


def test_unexpected_exceptions_are_not_swallowed(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.work_client.get_task_by_id.side_effect = KeyError("bug")
    with pytest.raises(KeyError):
        platform.get_task("t")


def test_close_closes_the_client(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    platform.close()
    client.close.assert_called_once_with()


# --- added after the phase 2 review ----------------------------------------------


@pytest.mark.parametrize(
    ("ours", "theirs"),
    [
        (p.TaskStatus, yd.TaskStatus),
        (p.TaskGroupStatus, yd.TaskGroupStatus),
        (p.WorkRequirementStatus, yd.WorkRequirementStatus),
        (p.PoolStatus, yd.WorkerPoolStatus),
    ],
)
def test_status_enums_cover_the_sdk(ours: Any, theirs: Any) -> None:
    """A status the SDK can return but RayDog cannot represent breaks every listing."""
    assert {m.name for m in theirs} <= {m.name for m in ours}


def test_resubmitted_task_is_live() -> None:
    assert p.TaskStatus("RESUBMITTED").is_live


def test_sdk_server_exception_is_retryable(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    from yellowdog_client.model.exceptions import InternalServerException

    client.work_client.get_task_by_id.side_effect = InternalServerException("oops")
    with pytest.raises(PlatformError) as caught:
        platform.get_task("t")
    assert caught.value.retryable and caught.value.status_code == 500
    assert caught.value.error_type == "InternalServerException"


def test_sdk_not_authorised_exception_keeps_its_status(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    from yellowdog_client.model.exceptions import NotAuthorisedException

    client.work_client.get_task_by_id.side_effect = NotAuthorisedException("no")
    with pytest.raises(PlatformError) as caught:
        platform.get_task("t")
    assert not caught.value.retryable and caught.value.status_code == 403


@pytest.mark.parametrize(
    "error",
    [
        requests.exceptions.ChunkedEncodingError("cut off"),
        requests.exceptions.ContentDecodingError("garbled"),
        requests.exceptions.RetryError("gave up"),
        requests.exceptions.InvalidJSONError("not json"),
    ],
)
def test_every_requests_error_is_translated(
    platform: YellowDogPlatform, client: MagicMock, error: Exception
) -> None:
    client.work_client.get_task_by_id.side_effect = error
    with pytest.raises(PlatformError) as caught:
        platform.get_task("t")
    assert caught.value.retryable


def test_task_error_is_reported(platform: YellowDogPlatform, client: MagicMock) -> None:
    task = sdk_task("FAILED")
    errors: Any = [
        SimpleNamespace(error="exit 1", errorType="TASK_FAILED", processExitCode=1),
        SimpleNamespace(error="exit 2", errorType="TASK_FAILED", processExitCode=2),
    ]
    task.errors = errors
    client.work_client.get_task_by_id.return_value = task
    assert platform.get_task("t").error == "exit 2"
    task.errors = None
    assert platform.get_task("t").error is None


def test_remove_pool_shuts_down_even_if_terminate_fails(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.worker_pool_client.get_worker_pool_by_id.return_value = sdk_pool()
    terminate = client.compute_client.terminate_compute_requirement_by_id
    terminate.side_effect = http_error(503, "Unavailable")
    with pytest.raises(PlatformError) as caught:
        platform.remove_pool("ydid:wrkrpool:1")
    assert caught.value.status_code == 503
    client.worker_pool_client.shutdown_worker_pool_by_id.assert_called_once_with(
        "ydid:wrkrpool:1"
    )


def test_remove_pool_tolerates_each_step_already_done(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.worker_pool_client.get_worker_pool_by_id.return_value = sdk_pool("SHUTDOWN")
    client.compute_client.terminate_compute_requirement_by_id.side_effect = http_error(
        403, "InvalidComputeRequirementStatusException"
    )
    client.worker_pool_client.shutdown_worker_pool_by_id.side_effect = http_error(
        403, "InvalidWorkerPoolStatusException"
    )
    platform.remove_pool("ydid:wrkrpool:1")
    client.compute_client.terminate_compute_requirement_by_id.assert_called_once()
    client.worker_pool_client.shutdown_worker_pool_by_id.assert_called_once()


def test_remove_pool_shuts_down_when_its_compute_has_gone(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.worker_pool_client.get_worker_pool_by_id.return_value = sdk_pool()
    client.compute_client.terminate_compute_requirement_by_id.side_effect = http_error(
        404, "ComputeRequirementNotFoundException"
    )
    platform.remove_pool("ydid:wrkrpool:1")
    client.worker_pool_client.shutdown_worker_pool_by_id.assert_called_once()


def test_find_pool_survives_a_missing_compute_requirement(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.worker_pool_client.get_worker_pool_by_name.return_value = sdk_pool(
        "TERMINATED"
    )
    client.compute_client.get_compute_requirement_by_id.side_effect = http_error(
        404, "ComputeRequirementNotFoundException"
    )
    info = platform.find_pool("ns", "demo_ab_head")
    assert info is not None and info.status is p.PoolStatus.TERMINATED
    assert info.tag is None and dict(info.instance_tags) == {}
    assert info.target_instances == 0


def test_live_instance_count_of_a_purged_pool_is_zero(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.worker_pool_client.get_worker_pool_by_id.side_effect = http_error(
        404, "WorkerPoolNotFoundException"
    )
    assert platform.live_instance_count("gone") == 0


def test_instances_with_missing_fields_do_not_crash(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.worker_pool_client.get_worker_pool_by_id.return_value = sdk_pool()
    client.compute_client.get_instances.return_value = search(
        [
            SimpleNamespace(id=None, status=None, publicIpAddress="1.1.1.1"),
            instance("i-1", "RUNNING", "1.2.3.4"),
        ]
    )
    assert platform.live_instance_count("ydid:wrkrpool:1") == 2
    assert platform.public_ips("ydid:wrkrpool:1") == {"i-1": "1.2.3.4"}


def test_list_pools_live_only(platform: YellowDogPlatform, client: MagicMock) -> None:
    client.worker_pool_client.get_worker_pools.return_value = search([])
    platform.list_pools("ns", name_contains="demo", live_only=True)
    sent = client.worker_pool_client.get_worker_pools.call_args.args[0]
    assert (sent.namespace, sent.name) == ("ns", "demo")
    assert yd.WorkerPoolStatus.RUNNING in sent.statuses
    assert yd.WorkerPoolStatus.TERMINATED not in sent.statuses
    # SHUTDOWN is included: such a pool can still hold a running instance
    assert yd.WorkerPoolStatus.SHUTDOWN in sent.statuses
    platform.list_pools("ns")
    assert client.worker_pool_client.get_worker_pools.call_args.args[0].statuses is None


def test_provision_pool_can_disable_idle_pool_shutdown(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    client.worker_pool_client.provision_worker_pool.return_value = sdk_pool("PENDING")
    platform.provision_pool(
        p.PoolSpec(
            name="a",
            namespace="ns",
            template="t",
            worker_tag="a",
            target_instances=0,
            max_nodes=1,
            idle_pool_shutdown=None,
        )
    )
    props = client.worker_pool_client.provision_worker_pool.call_args.args[1]
    assert props.idlePoolShutdown.enabled is False
    assert props.idleNodeShutdown.enabled is True


def test_a_task_being_aborted_is_marked_stopping(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    task = sdk_task("EXECUTING")
    client.work_client.get_task_by_id.return_value = task
    assert platform.get_task("t").stopping is False
    task.abortRequestedTime = NOW
    assert platform.get_task("t").stopping is True
    task.status = yd.TaskStatus.ABORTED
    assert platform.get_task("t").stopping is False


def test_adding_task_groups_from_two_threads_loses_neither(
    platform: YellowDogPlatform, client: MagicMock
) -> None:
    """
    The SDK offers only read-then-write: without care, the second writer would
    overwrite the first's group.
    """
    import threading
    import time

    stored = sdk_work_requirement()

    def get(_: str) -> yd.WorkRequirement:
        copy = sdk_work_requirement()
        copy.taskGroups = list(stored.taskGroups or [])
        time.sleep(0.05)  # long enough for the other thread to read too
        return copy

    def update(wr: yd.WorkRequirement) -> yd.WorkRequirement:
        for grp in wr.taskGroups or []:
            grp.id = grp.id or f"ydid:taskgrp:1:{grp.name}"
            grp.status = yd.TaskGroupStatus.RUNNING
        stored.taskGroups = list(wr.taskGroups or [])
        return wr

    client.work_client.get_work_requirement_by_id.side_effect = get
    client.work_client.update_work_requirement.side_effect = update
    threads = [
        threading.Thread(
            target=platform.add_task_group,
            args=("w", p.TaskGroupSpec(name=name, worker_tag=name)),
        )
        for name in ("w-a", "w-b", "w-c")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert sorted(g.name for g in stored.taskGroups or []) == [
        "head",
        "w-a",
        "w-b",
        "w-c",
    ]
