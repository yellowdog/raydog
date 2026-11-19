#!/usr/bin/env python3
"""
Build a Ray cluster with the RayDog Builder, run a small job on it, shut it down.

    python usage-example.py
    RAYDOG_OBSERVABILITY=1 python usage-example.py       # graphs in the dashboard
    RAYDOG_OBSERVABILITY=node python usage-example.py    # and logs, on a node

Settings come from the environment: see settings.py.
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import timedelta
from typing import Any

import settings as example_settings

from yellowdog_ray import (
    ClusterSpec,
    HeadNode,
    PortForward,
    RayDogCluster,
    RayTunnels,
)

WORKER_NODES = 2
WORKER_JOIN_TIMEOUT_SECONDS = 600
GRAFANA_PORT = 3000


def tunnel_forwards(
    observability: str, observability_node: Any
) -> tuple[PortForward, ...]:
    """
    The Ray client and the dashboard on the head node; and, with observability,
    Grafana (on the head, or on the observability node) at localhost:3000, where the
    head's start script told Ray's dashboard to find it for the graphs it embeds.
    """
    forwards = (RayTunnels.CLIENT, RayTunnels.DASHBOARD)
    if not observability:
        return forwards
    host = observability_node.private_ip if observability_node else "localhost"
    return (*forwards, PortForward(GRAFANA_PORT, GRAFANA_PORT, remote_host=host))


def run_ray_job(
    head: HeadNode,
    ssh_user: str,
    ssh_private_key: str,
    workers: int,
    observability: str,
    observability_node: Any,
) -> None:
    """Connect to the cluster through an SSH tunnel and run a trivial Ray job."""
    import ray

    if head.public_ip is None:
        raise SystemExit("This example needs a head node with a public IP address")
    with RayTunnels(
        head.public_ip,
        user=ssh_user,
        private_key=ssh_private_key,
        forwards=tunnel_forwards(observability, observability_node),
    ):
        ray.init(address="ray://localhost:10001")
        # build() returns when the head node is running; the workers join shortly
        # after. The head offers no CPUs itself, so wait for them.
        deadline = time.monotonic() + WORKER_JOIN_TIMEOUT_SECONDS
        while sum(1 for node in ray.nodes() if node["Alive"]) < workers + 1:
            if time.monotonic() > deadline:
                raise SystemExit("The worker nodes did not join the cluster in time")
            time.sleep(5)

        @ray.remote
        def square(x: int) -> int:
            return x * x

        print("Result:", sum(ray.get([square.remote(i) for i in range(100)])))
        ray.shutdown()

        if observability and sys.stdin.isatty():
            # Keep the cluster, and the tunnel, until the user has had a look
            where = "observability" if observability == "node" else "head"
            print(
                "Ray's dashboard, with graphs from Grafana: http://localhost:8265\n"
                f"Grafana itself: http://localhost:{GRAFANA_PORT} (its admin "
                f"password is in ~/grafana-admin-password on the {where} node)"
            )
            input("Press Enter to shut the cluster down: ")


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = example_settings.load(default_cluster_name="raydog-usage")
    spec = ClusterSpec(
        name=settings.cluster_name,
        namespace=settings.namespace,
        head=settings.head,
        # A backstop: the cluster is shut down below, but if this process dies
        # first, nothing outlives this
        lifetime=timedelta(hours=1),
        # With RAYDOG_OBSERVABILITY=node: started before the head, which is told
        # its address
        observability=(
            settings.observability_node if settings.observability == "node" else None
        ),
    )

    # Leaving the 'with' block shuts the cluster down, whatever happens inside it
    with RayDogCluster(spec) as cluster:
        # Worker instances are requested when the cluster is built, so they boot
        # while the head node does
        cluster.add_worker_pool("workers", settings.worker, count=WORKER_NODES)
        head = cluster.build()
        print(f"Head node: private IP {head.private_ip}, public IP {head.public_ip}")
        run_ray_job(
            head,
            settings.ssh_user,
            settings.ssh_private_key,
            WORKER_NODES,
            settings.observability,
            cluster.observability,
        )
    print("Cluster shut down")


if __name__ == "__main__":
    main()
