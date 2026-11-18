# Changelog

Notable changes to RayDog (`yellowdog-ray`). Versions follow [semantic versioning](https://semver.org).

## 3.0.0 (unreleased)

A rewrite. Nothing is compatible with version 2: see the "Moving from version 2" page of the documentation for a mapping of every name.

### Added

- A shared core for the Builder and the Autoscaler, with a simulated YellowDog platform (`yellowdog_ray.testing.FakePlatform`) that is shipped for users' own tests.
- The `raydog` command: `list`, `status`, `down`, `cleanup` and `tunnel`. `raydog tunnel` moves a default port that is already in use on this machine to the next free one, and says so.
- Builder: `ClusterSpec` and `NodeSpec`; context-manager lifecycle; `resize_worker_pool`; `RayDogCluster.attach` and `from_state`, which give a full cluster object from a name or from saved state; `status()`.
- Autoscaler: validation of the cluster YAML with errors that name the setting at fault; `use_internal_ips` support; `worker_start_grace_seconds`; support for Ray's `cleanup_cluster_resources` hook; every node given the resources and labels its node type declares, as Ray's own cluster launcher does for workers, the head included.
- A lifetime for every cluster, 24 hours by default, enforced by the platform as a deadline for all of its nodes.
- Ownership markers on everything RayDog creates, and detection and removal of what failed runs leave behind.
- An offline test suite, contract tests that run against both the simulated and the real platform, real-platform tests of both modes, GitHub Actions workflows for checks, and a local release script.
- Support for Python 3.13 and 3.14.
- Logging under the `yellowdog_ray` logger: a cluster's lifecycle at INFO, and every platform call with its duration at DEBUG. `raydog -v` and `-vv` show them, and so does `ray up` with `RAYDOG_LOG_LEVEL` set to `INFO` or `DEBUG`.
- Autoscaler: clusters run Ray's autoscaler v2 by default, with RayDog giving every node what it needs; `autoscaler_v2: false` runs the original autoscaler.
- The Builder replaces a cluster of the same name that no longer works (its head has ended, it never had one within its build timeout, or its lifetime has passed), as `ray up` does. `raydog list` and `raydog status` say how to remove clusters that no longer work.
- `images_id` accepts a YellowDog image family by name (`namespace/name`), which RayDog looks up, as well as by ID; the documentation recommends families.
- A node that does not start within the build timeout is reported with how far it got: no instance, an Agent that never registered, or an Agent that did not take its task.

### Changed

- `raydog cleanup` reports a cluster whose head node has ended at once, rather than after its build timeout.
- When the head node does not start, `ray up` ends with a one-line error rather than a Python traceback, and says it has removed the cluster; so does an interrupted `ray up`.
- The Builder's `RayDogCluster` takes a `ClusterSpec`; worker pools are addressed by name.
- The Autoscaler's provider is `yellowdog_ray.autoscaler.RayDogNodeProvider`, and its settings are renamed (`namespace`, `tag`, `lifetime_hours`, `build_timeout_minutes`, `head_start_script`, `worker_start_script`).
- The Autoscaler's head node receives YellowDog credentials over SSH, through Ray's own file sync, instead of in a task's environment.
- A failed build removes what it created; shutting down attempts every step, can verify that no instance is left, and reports what could not be removed.
- `RayTunnels` uses the OpenSSH client and is a context manager. It refuses to start if a local port is already in use, and does not check host keys unless asked to (`ssh_options`).
- Ray 2.54 or later is required.
- Version 16.1.1 or later of the YellowDog SDK (`yellowdog-sdk`) is required.
- The Builder example's observability node is set up when it boots, before its Agent starts: Prometheus (with its storage capped), Loki, and Grafana with Ray's dashboards and an admin password of its own. Grafana Alloy's configuration is one file, shared with the Packer image.
- The Packer image keeps `ray stop` from ending a node's task, as the example setup scripts do, so `ray down` works on clusters built from it.

### Removed

- The Valkey tag store on the Autoscaler's head node, and the SSH tunnel to it.
- `RayDogClusterProxy`; the `files_to_upload` and `head_node_tag_server_port` settings; the `auth` block inside `provider`; the `RAY_HEAD_NODE_PRIVATE_IP` variable.
- The `redis`, `sshtunnel` and `paramiko` dependencies.
- SSH private keys from the examples.
- Passwordless `sudo` for the Agent's user, from the example setup scripts and the Packer image: Ray jobs run as that user.

### Fixed

- Cluster discovery by name prefix, which could attach one cluster's commands to another cluster.
- Several ways in which a client command could shut down a running cluster as a side effect.
- Worker nodes that had died remaining visible to Ray.
- Scale-up failing after a restart of the head node's monitor.
- A wait loop in the Builder that polled without pausing and could not detect failure.

## 2.0.1

- Aligned with changes in the YellowDog SDK.
