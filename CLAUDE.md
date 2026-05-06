# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

**RayDog** (`yellowdog-ray` on PyPI) is a Python integration library that bridges [Ray](https://ray.io/) distributed computing clusters with the [YellowDog](https://yellowdog.ai/) cloud provisioning platform. It enables users to build and autoscale Ray clusters by provisioning YellowDog worker pools and tasks.

## Common Commands

```bash
make build          # Build the package with setuptools
make install        # Build and install in editable mode
make stubs          # Generate .pyi type stub files via stubgen
make clean          # Remove build artifacts

make format         # Run pyupgrade + isort + black
make black          # Black formatting only
make isort          # Import sorting only

make docs           # Build Sphinx documentation locally
```

> There is no automated test suite. Manual testing uses the scripts in `examples/`.

## Architecture

RayDog models a Ray cluster as a **YellowDog work requirement**. The cluster lifecycle is tied to the work requirement lifecycle. Nodes are YellowDog tasks inside task groups; the head node and each worker pool are separate task groups.

### Two usage modes

**Builder** (`src/yellowdog_ray/raydog/builder.py`)
Manual, imperative API. The user calls `RayDogCluster.add_worker_pool()` / `remove_worker_pool()` to control cluster size explicitly. Internally constructs YellowDog `WorkRequirement`, `TaskGroup`, and `Task` objects, then submits them via the YellowDog SDK. Supports optional observability nodes (Prometheus + Grafana).

**Autoscaler** (`src/yellowdog_ray/raydog/autoscaler.py`)
Implements Ray's `NodeProvider` interface so Ray's autoscaler can drive YellowDog as a cloud backend. Managed via `ray up` / `ray down` using a YAML cluster config file (see `examples/autoscaler/raydog.yaml`). YellowDog credentials are read from environment variables: `YD_API_KEY_ID`, `YD_API_KEY_SECRET`, and optionally `YD_API_URL` (defaults to `https://api.yellowdog.ai`); a `.env` file is also supported.

Internally the Autoscaler has three classes: `RayDogNodeProvider` (the Ray interface), `AutoRayDog` (YellowDog provisioning), and `TagStore`. The `TagStore` is a Redis/Valkey instance running on the head node that tracks node state; from the client side it is accessed via an SSH tunnel.

Both modes share the same underlying provisioning model — the Builder is useful for long-running clusters or when fine-grained control is needed; the Autoscaler is for job-driven workloads.

### Supporting utilities

- `src/yellowdog_ray/utils/utils.py` — Retrieves IP addresses from YellowDog node objects.
- `src/yellowdog_ray/utils/ray_ssh_tunnels.py` — `RayTunnels` class for Builder use: creates SSH tunnels to the head node for the Ray dashboard (port 8265) and Ray client (port 10001).

### Saving and restoring Builder cluster state

`RayDogCluster` has `save_state_to_json()` / `save_state_to_json_file()` methods that capture the work requirement ID and all worker pool IDs. A `RayDogClusterProxy` object can load this state and call `shut_down()` to tear down a cluster from a separate process or at a later time.

### Key dependencies

| Package | Role |
|---|---|
| `yellowdog-sdk >= 13.6.1` | All YellowDog API calls (provisioning, work requirements, tasks) |
| `ray[client]` | Ray cluster/distributed computing framework |
| `paramiko 3.5.1` | SSH connections to provisioned nodes |
| `sshtunnel` | SSH tunnel abstraction used by `RayTunnels` |
| `redis` | Cluster-internal communication |

## Package Layout

```
src/yellowdog_ray/
  __init__.py              # Version (1.0.1) and public exports
  raydog/
    builder.py             # RayDogCluster (manual cluster management)
    autoscaler.py          # NodeProvider + CommandRunnerInterface impl
  utils/
    utils.py               # IP address helpers
    ray_ssh_tunnels.py     # RayTunnels SSH tunnel management
examples/
  builder/                 # Runnable builder usage examples
  autoscaler/              # Runnable autoscaler examples + YAML configs
docs/                      # Sphinx source (builder.rst, autoscaler.rst)
```

## Release Process

See `RELEASING.md` for the full process. Short version:
1. Bump version in `src/yellowdog_ray/__init__.py` on the `next-version` branch
2. Merge `next-version` → `main`
3. Tag the commit (`git tag -a v<version>`) and push with tags
4. `make pypi-check-build` → `make pypi-prod-upload`
5. Build and push the docs Docker image via `make docs-publish-image`, then notify the ops team
