#!/usr/bin/env python3
"""
Manage a running RayDog Builder cluster: resize it, and hand it between processes.

    python manage-example.py

Shows: adding worker pools to a running cluster, resizing and removing them,
checking status, saving the cluster's state, and picking the cluster up again from
that state (as a different process would) to shut it down.

Settings come from the environment: see settings.py.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import timedelta
from pathlib import Path

import settings as example_settings

from yellowdog_ray import ClusterSpec, RayDogCluster


def show(cluster: RayDogCluster, title: str) -> None:
    status = cluster.status()
    print(f"--- {title}: head {'running' if status.head_alive else 'NOT running'}")
    for name, pool in cluster.worker_pools.items():
        print(f"    pool '{name}': {pool.count} node(s), maximum {pool.max_count}")


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = example_settings.load(default_cluster_name="raydog-manage")
    spec = ClusterSpec(
        name=settings.cluster_name,
        namespace=settings.namespace,
        head=settings.head,
        lifetime=timedelta(hours=1),
    )
    # A private directory: the state file says which cluster to act on
    state_file = Path(tempfile.mkdtemp(prefix="raydog-")) / "cluster-state.json"

    cluster = RayDogCluster(spec)
    try:
        head = cluster.build()
        print(f"Head node: private IP {head.private_ip}, public IP {head.public_ip}")

        # Pools can be added to a running cluster, and grown within their maximum
        cluster.add_worker_pool("small", settings.worker, count=1, max_count=4)
        cluster.add_worker_pool("burst", settings.worker, count=0, max_count=2)
        show(cluster, "two pools added")

        cluster.resize_worker_pool("small", 3)
        cluster.resize_worker_pool("burst", 2)
        show(cluster, "scaled up")

        cluster.resize_worker_pool("small", 1)  # nodes not yet started go first
        cluster.remove_worker_pool("burst")
        show(cluster, "scaled down")

        # The state file identifies the cluster; it holds no credentials
        cluster.save_state(state_file)
        print(f"State saved to {state_file}")

        # ... later, or in another process: pick the cluster up again from the file
        later = RayDogCluster.from_state(state_file)
        show(later, "re-attached from saved state")
        later.shut_down(wait=True)  # returns once no instance is left running
        print("Cluster shut down")
    finally:
        # Whatever happened above (an error, Ctrl-C), do not leave it running.
        # After a clean run this finds nothing left to do.
        cluster.shut_down()
        state_file.unlink(missing_ok=True)
        state_file.parent.rmdir()


if __name__ == "__main__":
    main()
