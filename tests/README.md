# Tests

Three tiers. Only the first runs by default and in pull-request CI.

| Tier | Command | Needs | Starts instances |
|---|---|---|---|
| Offline | `make test` | nothing | no |
| Online | `make test-online` (`pytest -m online`) | YellowDog credentials | no |
| Online, with instances | `make test-instances` (`pytest -m online_instances`, then the sweep) | credentials, a template and an image | yes, a few, for minutes |

## Layout

- `tests/unit/` tests one module at a time. `test_fake_platform.py` pins down every rule of the in-memory platform; `test_yellowdog_platform.py` checks the SDK adapter against stubbed SDK clients.
- `tests/contract/` states what RayDog relies on the platform to do, and runs each statement against both the fake and the real platform through the same `Harness` (see `conftest.py`). This is what keeps the fake honest: if a contract test passes against the fake and fails against the real platform, the fake is wrong, and the fix goes in the fake and its unit test.

- `tests/examples/` runs the example scripts and cluster configurations end to end against the fake.
- `tests/online/` runs Ray's own `ray up` and `ray down` against real instances running real Ray, in both the bare and the container variant, along with the `raydog` command and a real SSH tunnel. It is the slowest tier (about ten minutes per variant, up to three instances) and is part of `-m online_instances`. It needs an SSH key pair (`RAYDOG_TEST_SSH_PRIVATE_KEY`, with `<path>.pub` beside it) and a head node that accepts SSH from where the tests run; without the key it is skipped. The online tests run only from a developer's machine: CI runs the offline tests alone. In the container variant Docker keeps its images in memory on the test nodes, because a test image's disk may be too small for the Ray image; set `RAYDOG_TEST_DOCKER_IN_MEMORY=0` to use the disk.

Offline tests use no network and never sleep: time is a `FakeClock` that moves only when a test advances it.

## Running part of the instance tests

`make test-instances` runs every test that starts instances (the contract tests that do, and both variants of `tests/online`), then the sweep, whatever the outcome. Extra pytest options go in `ARGS`:

```
make test-instances ARGS="-k container"   # tests/online in containers (Docker), only
make test-instances ARGS="-k bare"        # tests/online with Ray installed on the nodes, only
make test-instances ARGS="tests/online"   # both variants of tests/online, no contract tests
RAYDOG_TEST_AUTOSCALER_V1=1 make test-instances ARGS="tests/online"   # with Ray's original autoscaler
```

The container variant runs Ray in the `rayproject/ray` image, set up by `examples/autoscaler/scripts/node-setup-docker.sh` and started by the `-docker` start scripts. Its workers need room for the image (see `RAYDOG_TEST_CONTAINER_WORKER_TEMPLATE` and `RAYDOG_TEST_DOCKER_IN_MEMORY`).

## Online settings

Read from the environment, or from a `.env` file at the repository root. Nothing is read from anywhere else, and nothing is written.

| Variable | Meaning | Default |
|---|---|---|
| `YD_API_KEY_ID`, `YD_API_KEY_SECRET` | YellowDog application key | none: online tests are skipped |
| `YD_API_URL` | platform API URL | `https://api.yellowdog.ai` |
| `RAYDOG_TEST_NAMESPACE` | namespace to create things in; must exist | `ray-test` |
| `RAYDOG_TEST_WORKER_TEMPLATE` | compute requirement template for test pools | none: online tests are skipped |
| `RAYDOG_TEST_IMAGES_ID` | image for test pools, if the template needs one: an image family as `namespace/name`, or an image ID | none |
| `RAYDOG_TEST_IMAGE_FAMILY` | `namespace/name` of an image family the key can see, for the contract test of the family lookup; without it that test checks only names that do not exist | none |
| `RAYDOG_TEST_HEAD_TEMPLATE` | compute requirement template for head nodes in `tests/online` | the worker template |
| `RAYDOG_TEST_CONTAINER_WORKER_TEMPLATE` | template for workers in the container variant of `tests/online`, whose instances need at least 8 GB of memory (or of free disk, with `RAYDOG_TEST_DOCKER_IN_MEMORY=0`) for the Ray image | the head template |
| `RAYDOG_TEST_DOCKER_IN_MEMORY` | `0` for Docker on the container variant's nodes to keep its images on disk, which needs a few gigabytes free, rather than in memory | in memory |
| `RAYDOG_TEST_SSH_PRIVATE_KEY` | for `tests/online`: a private key whose public half is beside it as `<path>.pub`; the head node must accept SSH from this machine | `~/.raydog-probe/id_ed25519` |
| `RAYDOG_TEST_AUTOSCALER_V1` | `1` to run `tests/online` with Ray's original autoscaler (`autoscaler_v2: false`); either way the tests check which autoscaler ran | autoscaler v2, RayDog's default |
| `RAYDOG_TEST_USERDATA_FILE` | a script run at instance boot; use the YellowDog Agent installer if the image's Agent may be out of date | none |

`tests/online` sets nodes up with the example setup scripts, so it needs an image those work on (plain Ubuntu); `RAYDOG_TEST_USERDATA_FILE` is not used there. It builds the RayDog under test into a wheel and lets Ray carry it to the head node, so nothing needs to be published or pushed first.

## Safety

Everything an online test creates is named `raydog-test-…`, is cancelled or removed when the test ends whether it passed or not, and the test fails if any instance is still alive after cleanup. Test pools also carry idle shutdowns as a backstop. A pool reading `SHUTDOWN` is not proof that its instances have gone, so cleanup counts instances. `make test-sweep` (`python -m tests.sweep`) removes anything named `raydog-test-…` that an interrupted run left behind and exits non-zero if an instance is still alive; `make test-instances` always runs it last.
