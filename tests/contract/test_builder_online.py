"""The Builder's whole life, on the fake and (on request) on the real platform."""

from __future__ import annotations

from datetime import timedelta

from tests.conftest import Harness
from yellowdog_ray import ClusterSpec, NodeSpec, RayDogCluster


def node(h: Harness) -> NodeSpec:
    return NodeSpec(
        compute_requirement_template=h.template,
        start_script="sleep 3000",
        images_id=h.images_id,
        userdata=h.userdata,
    )


def test_build_scale_add_and_remove_pools_reattach_and_shut_down(
    instance_harness: Harness,
) -> None:
    h = instance_harness
    spec = ClusterSpec(
        name=h.name("b"),
        namespace=h.namespace,
        head=node(h),
        tag="raydog-test",
        lifetime=timedelta(minutes=20),  # the backstop for everything started here
        idle_shutdown=timedelta(minutes=1),
    )
    cluster = RayDogCluster(spec, platform=h.platform, clock=h.clock)
    cluster.add_worker_pool("small", node(h), count=1, max_count=2)
    try:
        head = cluster.build()
        assert cluster.handle is not None
        h.work_requirements.append(cluster.handle.work_requirement_id)
        assert head.private_ip
        state = cluster.save_state()

        # As another process would: nothing but the saved state
        other = RayDogCluster.from_state(state, platform=h.platform, clock=h.clock)
        assert other.handle == cluster.handle and other.head == head
        assert other.worker_pools["small"].max_count == 2

        other.resize_worker_pool("small", 2, start_script="sleep 3000")
        h.wait_until(
            lambda: (
                other.status().pools[other.worker_pools["small"].role].live_instances
            ),
            lambda n: n == 2,
            timeout_s=600,
            step_s=5,
        )
        h.wait_until(
            lambda: sum(
                other.status()
                .pools[other.worker_pools["small"].role]
                .nodes_by_status.values()
            ),
            lambda n: n == 2,
            timeout_s=600,
            step_s=5,
        )
        other.resize_worker_pool("small", 1)
        h.wait_until(
            lambda: other.worker_pools["small"].count, lambda n: n == 1, 120, 2
        )
        assert other.status().head_alive

        # A pool added to the running cluster, and then removed from it
        late = other.add_worker_pool("late", node(h), count=1)
        assert late.pool_id is not None and set(other.worker_pools) == {"small", "late"}
        h.wait_until(
            lambda: sum(other.status().pools[late.role].nodes_by_status.values()),
            lambda n: n == 1,
            timeout_s=600,
            step_s=5,
        )
        assert other.status().pools[late.role].live_instances == 1
        other.remove_worker_pool("late")
        assert set(other.worker_pools) == {"small"}

        def late_instances() -> int:
            state = other.status().pools.get(late.role)
            return state.live_instances if state is not None else 0

        h.wait_until(late_instances, lambda n: n == 0, timeout_s=600, step_s=5)
        status = other.status()
        assert status.head_alive, "removing a pool must not disturb the head"
        small = status.pools[other.worker_pools["small"].role]
        # (its instance count may still include the one it gave up when it shrank)
        assert sum(small.nodes_by_status.values()) == 1, "nor the other pools"

        other.shut_down(wait=True)
    finally:
        cluster.shut_down()  # a no-op by now on a good run; the safety net otherwise
    for pool in h.platform.list_pools(h.namespace, name_contains=spec.name):
        assert h.platform.live_instance_count(pool.id) == 0
