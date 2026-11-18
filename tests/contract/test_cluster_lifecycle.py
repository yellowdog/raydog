"""A whole cluster's life through ClusterCore, on the fake and on the real platform."""

from __future__ import annotations

from datetime import timedelta

import pytest

from tests.conftest import Harness
from yellowdog_ray.core import naming
from yellowdog_ray.core.cluster import ClusterCore, ClusterSettings, NodeSpec
from yellowdog_ray.core.platform import TaskStatus, WorkRequirementStatus
from yellowdog_ray.errors import ClusterExistsError, ClusterNotFoundError

WORKERS = naming.worker_role("small")
VERIFY = timedelta(minutes=6)


def settings(h: Harness) -> ClusterSettings:
    return ClusterSettings(
        name=h.name("lc"),  # short: a cluster name may be at most 24 characters
        namespace=h.namespace,
        tag="raydog-test",
        lifetime=timedelta(minutes=20),  # the backstop for everything started here
        idle_shutdown=timedelta(minutes=1),
        build_timeout=timedelta(minutes=10),
    )


def node(h: Harness) -> NodeSpec:
    return NodeSpec(
        compute_requirement_template=h.template,
        start_script="sleep 3000",
        images_id=h.images_id,
        userdata=h.userdata,
    )


def test_cluster_without_nodes(harness: Harness) -> None:
    h = harness
    core = ClusterCore(h.platform, h.clock)
    wanted = settings(h)
    handle = core.create(wanted)
    h.work_requirements.append(handle.work_requirement_id)
    assert handle.settings == wanted

    with pytest.raises(ClusterExistsError):
        core.create(wanted)

    attached = ClusterCore(h.platform, h.clock).attach(wanted.name, h.namespace)
    assert attached == handle
    assert wanted.name in {c.name for c in core.list_clusters(h.namespace)}

    pool = core.add_pool(handle, WORKERS, node(h), target=0, max_nodes=2)
    h.pools.append(pool.id)
    status = core.status(handle)
    assert status.work_requirement_status is WorkRequirementStatus.RUNNING
    assert status.head is None and not status.head_alive and not status.past_deadline
    assert set(status.pools) == {"head", WORKERS}
    assert status.pools[WORKERS].pool_id == pool.id
    assert status.pools[WORKERS].live_instances == 0
    assert status.pools["head"].pool_id is None

    core.tear_down(handle, verify_timeout=VERIFY)
    core.tear_down(handle, verify_timeout=VERIFY)  # repeatable
    h.wait_until(
        lambda: h.platform.get_work_requirement(handle.work_requirement_id).status,
        lambda s: s is WorkRequirementStatus.CANCELLED,
        timeout_s=90,
    )
    with pytest.raises(ClusterNotFoundError):
        core.attach(wanted.name, h.namespace)
    assert wanted.name not in {o.cluster for o in core.find_orphans(h.namespace)}
    # ... and the name can be used again straight away
    again = core.create(wanted)
    h.work_requirements.append(again.work_requirement_id)
    assert again.work_requirement_name != handle.work_requirement_name
    core.tear_down(again, verify_timeout=VERIFY)


def test_cluster_with_a_head_and_workers(instance_harness: Harness) -> None:
    h = instance_harness
    core = ClusterCore(h.platform, h.clock)
    handle = core.create(settings(h))
    h.work_requirements.append(handle.work_requirement_id)

    head_pool = core.add_pool(handle, naming.HEAD_ROLE, node(h), target=1, max_nodes=1)
    worker_pool = core.add_pool(handle, WORKERS, node(h), target=0, max_nodes=2)
    h.pools += [head_pool.id, worker_pool.id]

    head = core.add_nodes(handle, naming.HEAD_ROLE, 1, "sleep 3000")[0]
    head_address = core.wait_for_node(handle, head.id)
    assert head_address.private_ip

    workers = core.add_nodes(
        handle,
        WORKERS,
        2,
        "sleep 3000",
        environment={"RAY_HEAD_IP": head_address.private_ip},
        node_tags={"ray-node-kind": "worker"},
    )
    addresses = [core.wait_for_node(handle, w.id) for w in workers]
    assert len({a.private_ip for a in addresses} | {head_address.private_ip}) == 3

    status = core.status(handle)
    assert status.head_alive and status.head is not None
    assert status.head.task_id == head.id
    assert dict(status.pools[WORKERS].nodes_by_status) == {TaskStatus.EXECUTING: 2}
    assert status.pools[WORKERS].live_instances == 2
    assert status.pools["head"].live_instances == 1
    by_id = {n.task_id: n for n in core.nodes(handle)}
    assert set(by_id) == {head.id, workers[0].id, workers[1].id}
    assert dict(by_id[workers[0].id].tags) == {"ray-node-kind": "worker"}

    # From "another process": only the name is needed
    elsewhere = ClusterCore(h.platform, h.clock)
    assert elsewhere.attach(handle.settings.name, h.namespace) == handle

    core.remove_nodes(handle, [workers[1].id])
    h.wait_until(
        lambda: [n.task_id for n in core.nodes(handle, role=WORKERS)],
        lambda ids: ids == [workers[0].id],
        timeout_s=120,
        step_s=2,
    )
    assert core.find_orphans(h.namespace) == [] or handle.settings.name not in {
        o.cluster for o in core.find_orphans(h.namespace)
    }

    elsewhere.tear_down(handle, verify_timeout=VERIFY)
    for pool_id in (head_pool.id, worker_pool.id):
        assert h.platform.live_instance_count(pool_id) == 0
    assert handle.settings.name not in {
        o.cluster for o in core.find_orphans(h.namespace)
    }
