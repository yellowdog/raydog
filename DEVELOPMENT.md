# Developing RayDog

For anyone changing RayDog itself. Using it is covered by the documentation in `docs/`; releasing it by [`RELEASING.md`](RELEASING.md); the tests in detail by [`tests/README.md`](tests/README.md).

## Setting up

```bash
uv venv && source .venv/bin/activate
make install        # uv pip install -e ".[dev,docs]"
```

Always `uv pip`, never `pip`. `make update` upgrades every dependency except Ray, which is held at the version the examples and machine image pin (see `RELEASING.md` for the places that pin it: Ray must be the same version on a user's machine and on the nodes).

## The daily loop

```bash
make format         # apply ruff fixes and formatting
make check          # lint, mypy --strict, offline tests and docs: what a release must pass
pytest tests/unit/test_cluster_core.py -k teardown     # one file, one pattern
```

| Command | What it runs | Needs |
|---|---|---|
| `make test` | offline tests, a few seconds, no network | nothing |
| `make test-online` | contract tests against a live account, no instances | YellowDog credentials |
| `make test-instances` | everything that starts instances, then the sweep | credentials, templates, an image, an SSH key |
| `make test-sweep` | removes anything `raydog-test-…` left behind, fails if an instance is alive | credentials |
| `make docs` | Sphinx, warnings are errors, full rebuild | nothing |

Online settings (`YD_API_KEY_ID`, `RAYDOG_TEST_WORKER_TEMPLATE` and the rest) come from the environment or a repo-root `.env`; `tests/README.md` lists them. Without them the online tests are *skipped*, not failed, so read pytest's summary line: `-rs` says why a test skipped.

A second checkout made with `git worktree` shares the virtual environment's editable install, which points at the first checkout's `src`. Run tests there with `PYTHONPATH=src`, or they test the other checkout's code.

## Conventions

- `ruff` (line length 88) is clean everywhere; `mypy --strict` is clean on `src`, `tests` and `bin`. The examples are not type-checked; `tests/examples/` runs them.
- A multi-line docstring has its opening and closing quotes on lines of their own; a one-line docstring stays on one line (ruff `D213`, `D209`).
- Documentation prose, Markdown and reStructuredText, has no hard line breaks; code blocks are exempt.
- Errors RayDog raises on purpose derive from `RayDogError` (`errors.py`). Messages say what happened and what to do, in a sentence a user can act on.
- Shell scripts for nodes install the YellowDog Agent **last**: the moment it starts, the node is offered work.
- `CHANGELOG.md` keeps a `## <version> (unreleased)` heading; add a line for anything a user would notice.

## How it fits together

A Ray cluster is one YellowDog **work requirement**. Each kind of node (a *role*: `head`, `observability`, `w-<name>` for workers) is a **task group** fed by its own **worker pool**. A node is a **task**: the script that starts Ray on it. Cancelling the task ends the node; an instance with no task is released by the platform.

```
src/yellowdog_ray/
  errors.py            RayDogError hierarchy
  credentials.py       load_credentials(): arguments > environment > .env > credentials file
  logs.py              RayDog's log lines on a terminal: `raydog -v`, RAYDOG_LOG_LEVEL
  core/
    platform.py        the Platform protocol and RayDog's own records: the seam for testing
    yellowdog.py       YellowDogPlatform: the ONLY module that imports the YellowDog SDK
    naming.py          names, roles, ownership markers, and parsing them back exactly
    cluster.py         ClusterCore: create, attach, pools, nodes, status, tear_down, orphans
    wait.py, clock.py  polling with a timeout; an injectable clock
  builder/             RayDogCluster, ClusterSpec, saved state: thin and stateful, over the core
  autoscaler/
    provider.py        RayDogNodeProvider, in its client role and its head-node role
    config.py          provider and node_config validation, file: inlining, bootstrap
    nodes.py           thread-safe node table, rebuilt from the platform on each refresh
  tunnels.py           RayTunnels: one `ssh -N -L ...` subprocess
  cli.py               the `raydog` command
  testing/             FakePlatform and FakeClock, shipped so that users can test offline too
```

The Builder and the Autoscaler are two front ends over `ClusterCore`. Everything that talks to YellowDog goes through the `Platform` protocol, which has two implementations: the real one in `core/yellowdog.py` and the simulation in `testing/fake_platform.py`.

## What must stay true

Each of these was a bug, or a near miss, in version 2.

- **`ClusterCore` is stateless.** Every operation takes a `ClusterHandle` and asks the platform for the rest, so any process that can name a cluster can drive it: `ray up` on a laptop, the autoscaler on the head, `raydog down` a week later. Cluster settings travel in the head task group's tag (`raydog:1:<cluster>:head:b=…,i=…,l=…`).
- **Identity is exact.** The platform's name and tag searches match substrings, so searching for `demo` also finds `demo-2`. Names are parsed (`<cluster>_<8-char suffix>[_<role>]`, the underscore reserved as the separator) and compared, never prefix-matched. Pools also carry `raydog-cluster` and `raydog-work-requirement` instance tags. Only objects carrying RayDog's markers are ever touched.
- **Nothing is torn down as a side effect.** Teardown happens only when asked (`tear_down`, `shut_down`, `terminate_node` of the head, `cleanup_cluster_resources`, `raydog down`, `raydog cleanup --delete`), or to roll back what the same call created. No `__del__`, and constructors do no I/O. Ray keeps a provider for the life of its process and never closes it, so a finaliser runs at interpreter shutdown, and a constructor that fails after finding a live cluster would take it down from a read-only command such as `ray exec`.
- **Teardown tries every step, then reports.** It cancels the work requirement, removes every pool whose name starts with the work requirement's name, and can verify by counting live instances. A pool reading `SHUTDOWN` can still hold a running instance, so pool status is never treated as proof.
- **The Autoscaler's node state comes from YellowDog.** A Ray node ID is a task ID. `non_terminated_nodes` refreshes the table from the platform; every other query answers from memory, because Ray asks many times a second from several threads. Tags Ray supplies at creation are stored in the task's environment; tags set later live in a per-process overlay. The status rules are in `nodes.py`.
- **Credentials never go into a task.** A task's environment can be read back through the API by anyone who can read the task. The Autoscaler's head node gets the credentials from a 0600 file that Ray's own file sync copies over SSH, which is also why Ray's node updaters must stay off (`disable_node_updaters`): with them on, Ray would copy the same files to every worker.

## What RayDog relies on YellowDog to do

Established by probing the real platform; the parts that need no instances are re-checked by `tests/contract/` against both the fake and the real platform. **If a contract test passes on the fake and fails on the real platform, fix the fake** (and its unit test), not the expectation.

Names and tags:

- Work requirement, pool and task names: at most 60 characters, lowercase letters, digits, `_` and `-`, starting with a letter and ending with a letter or digit.
- Names are unique within a namespace, **including against ended objects**, which are purged only after 24 hours. Hence the random suffix on every cluster's names.
- Tags are at most 200 characters and cannot be changed: an update silently ignores a new name or tag. Anything that changes cannot live in a tag.
- Tasks cannot be changed once added.
- A pool's `imagesId` takes an image family, group or image by its `ydid:` ID, or a cloud image ID, but not a family's `namespace/name`: given that, no instance is ever provisioned and nothing says why. RayDog looks the name up itself (`find_image_family`) when it creates a pool.

Lifecycles:

- A task waits in `READY` until a node takes it, then goes `ALLOCATED`, `EXECUTING`. A node record has the private address only; the public address is on the instance record.
- A task added after a node has registered waits for the node's next contact with the platform, about 40–50 seconds.
- Cancelling is asynchronous: an aborted task reads `EXECUTING` for some seconds, and a work requirement reads `CANCELLING` for up to a minute or so while tasks are executing. Cancelling an already cancelled work requirement is an error (403).
- When the head task fails, **only its own task group fails**. The work requirement stays `RUNNING` and the workers carry on, so RayDog must notice a dead head itself (`raydog cleanup`, and the Builder's and `ray up`'s replacement of a dead cluster), and every node has the cluster's lifetime as a deadline.
- A worker pool grows to meet its tasks, but a pool that has never had a node starts one instance first; setting the target explicitly overrides that.
- Shutting a pool down does not stop a node that is executing a task. Terminating its compute requirement does.
- A node whose Agent loses contact is declared lost, its task fails and its instance is terminated.
- A node whose image has an out-of-date Agent never registers: the instance runs, the pool stays `EMPTY` and the task stays `READY`. The build timeout's error message says how far a node got.
- A work requirement's creation time comes back to the microsecond when created and to the millisecond on every later read; never compare the two.

Instances:

- EC2 accepts at most 16 KB of userdata once base64-encoded, about 12 KB of script; an instance given more never boots.
- Ubuntu's cloud images have under 7 GB of usable disk. Python, Ray and `uv`'s cache fill it quickly; the setup scripts clean the cache.

## What the provider relies on Ray to do

Ray's external provider interface is the v1 `NodeProvider`. It is not deprecated, and Ray's autoscaler v2 drives it through an adapter: there is no other way for a third party to plug in. The supported floor is Ray 2.54.0: 2.51 to 2.53 have a bug that stops the original autoscaler when `request_resources()` is called. `tests/unit/test_autoscaler_provider.py` replays the call sequences below.

- **One provider per process, never closed.** Ray caches provider instances for the life of the process, keyed by the provider section and cluster name. Hence no `__del__`.
- **`bootstrap_config` runs only in the client, and only on a cache miss.** Ray caches the bootstrapped config in its temp directory, keyed by the YAML's content; later commands skip it, and `ray get-head-ip` never bootstraps at all. So `bootstrap_config` is a pure function of the YAML, and the provider validates again when constructed. `file:` paths are looked for beside the YAML, then in the working directory.
- **The head's copy of the config is marked by `prepare_for_head_node`**, which is how the provider knows it is on the head.
- **`ray up` waits about 50 seconds** after `create_node` returns for the head to appear in `non_terminated_nodes`; RayDog's `create_node` for the head therefore waits until the head task is running.
- **Many threads.** Node launcher threads, the monitor's loop and (with updaters on) one thread per node all call the provider at once. Everything is thread-safe, and `node_tags`, `internal_ip` and `is_running` answer from memory.
- **Workers report themselves up to date.** With updaters off, nobody else will. The original autoscaler drops a worker that it believes is up to date but has not heard from within 30 seconds, so a worker is reported up to date only after `worker_start_grace_seconds` of running.
- **`create_node_with_resources_and_labels`** passes each worker type's declared resources and labels. Ray's own launcher makes `ray start` use them (`RAY_OVERRIDE_RESOURCES`, `RAY_OVERRIDE_LABELS`), and so does RayDog. Without that, a node with fewer CPUs than declared leaves the autoscaler's plan short. Ray passes nothing for the head, whose `create_node` comes from `ray up`; bootstrap records the head type's declaration in the provider section (`raydog_head_resources`, `raydog_head_labels`), and the head gets it the same way.
- **Autoscaler v2** (the default) needs `RAY_enable_autoscaler_v2=1` on the head and, on every node, `RAY_NODE_TYPE_NAME` and `RAY_CLOUD_INSTANCE_ID` set to the provider's node ID. That ID is the task's own ID, which exists only once the task does, so a line at the top of the start script sets it from `YD_TASK_ID`. Under v2 the `ray-launch-request` tag given at creation must come back from `node_tags`, and `is_running` is called for every node on every pass.
- **`cleanup_cluster_resources`** is called by `ray down` once every node has gone. It must not raise: Ray's message when it does talks about Azure.
- **`ray up` checks the head's start commands** for the substrings `ray start` and `autoscaling-config`, and warns if they are missing. RayDog starts Ray from the task, so bootstrap puts in a one-line `echo` that contains both.
- **Ray's CLI configures only the `ray` logger**, so RayDog's own lines reach a `ray up` terminal only at WARNING and above, or with `RAYDOG_LOG_LEVEL` set.

## Testing

- `tests/unit/` drives each layer against `FakePlatform`, a clock-driven simulation: nothing happens until `platform.clock.advance(seconds)`, and RayDog's own waits advance that clock rather than sleeping. Inject trouble with `fail_next`, `never_register`, `fail_task` and `lose_node`.
- Tests that inject a failure at *every* platform call of an operation (before the call, after it with the response lost, and as Ctrl-C) are the guard against leaks. Keep them passing, and extend them when adding a platform call.
- `tests/examples/` runs the example scripts and configurations against the fake. `tests/online/` runs real `ray up` and `ray down` against real instances, bare and in containers, with this checkout built into a wheel that Ray carries to the head.
- Check that a new test can fail: break the code it covers and watch it go red. A test that cannot fail is not a test.

## Debugging a real cluster

- **RayDog's own lines:** `raydog -v` (INFO) or `-vv` (DEBUG, every platform call and its duration); for `ray up` and the examples' `up.sh`, `RAYDOG_LOG_LEVEL=INFO` or `DEBUG`.
- **What the autoscaler thinks:** `ray exec <yaml> 'ray status -v'` lists pending nodes, recent failures and the demand it cannot place.
- **What the provider did on the head:** `ray exec <yaml> 'tail -n 200 /tmp/ray/session_latest/logs/monitor.log'`. Errors raised inside the autoscaler, RayDog's or Ray's, end up in `monitor.err`. Under v2, `monitor.log` also records each instance's state changes (`QUEUED`, `REQUESTED`, `ALLOCATED`, `RAY_RUNNING`) with its cloud instance ID, which is RayDog's task ID.
- **What YellowDog thinks:** `raydog status <name> --json` gives every node with its task status and addresses. The YellowDog portal shows the work requirement, task groups and pools under the cluster's name.
- **On a node:** the Agent's user is `yd-agent`, home `/opt/yellowdog/agent`; a task runs in a directory under `/var/opt/yellowdog/agent/data/workers/`; the examples' setup scripts log to `/var/log/raydog-node-setup.log`.
- **Something left behind:** `raydog list`, then `raydog down <name>`, then `raydog cleanup --delete --wait`; for test resources, `make test-sweep`.
