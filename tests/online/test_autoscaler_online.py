"""
The Autoscaler for real: Ray's own ``ray up``, a job that makes the cluster grow,
the ``raydog`` command, an SSH tunnel, idle scale-down and ``ray down``, against a
live YellowDog account with real instances running real Ray.

Takes about a quarter of an hour and up to three instances. Needs, beyond the
settings in ``tests/README.md``: an SSH key pair (``RAYDOG_TEST_SSH_PRIVATE_KEY``,
with its public half beside it as ``<path>.pub``), the ``ssh`` and ``rsync``
programs, and an image on which the example node setup script works (plain Ubuntu).
The head node must accept SSH from this machine.

The RayDog under test is built into a wheel here and carried to the head node by
Ray's own file sync, so nothing needs to be published first.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.conftest import REPO_ROOT, Harness, _real_harness, _setting
from yellowdog_ray import RayTunnels
from yellowdog_ray.tunnels import PortForward

pytestmark = pytest.mark.online_instances

SCRIPTS = REPO_ROOT / "examples" / "autoscaler" / "scripts"
# Two one-CPU tasks that must run on different nodes (a strictly spread placement
# group), on a head that offers no CPUs: the cluster has to grow by two workers.
# Without the placement group, a second worker that is slow to arrive lets the
# first one run both tasks in turn. Each also needs one of a resource that a
# worker has only if the worker type's declaration reached its `ray start`.
JOB = """
import ray
from ray.util.placement_group import placement_group, remove_placement_group
ray.init(address="auto")
group = placement_group([{"CPU": 1, "declared": 1}] * 2, strategy="STRICT_SPREAD")
ray.get(group.ready())
@ray.remote(num_cpus=1, resources={"declared": 1})
def where():
    return ray.util.get_node_ip_address()
tasks = [
    where.options(placement_group=group, placement_group_bundle_index=i).remote()
    for i in range(2)
]
print("RESULT", ",".join(sorted(set(ray.get(tasks)))))
remove_placement_group(group)
"""


# The Ray image is over 2 GB unpacked, and an image used for testing may have a
# disk too small for it (the one these tests were written against has 8 GB, mostly
# full). So, unless RAYDOG_TEST_DOCKER_IN_MEMORY=0, Docker keeps its images in
# memory on the test nodes. The example scripts themselves are run as they are.
DOCKER_IN_MEMORY = """
mkdir -p /mnt/raydog-docker /etc/docker
mount -t tmpfs -o size=8g tmpfs /mnt/raydog-docker
echo '{"data-root": "/mnt/raydog-docker"}' > /etc/docker/daemon.json
systemctl restart docker 2> /dev/null || true
"""


def run(
    command: list[str],
    env: dict[str, str],
    cwd: Path,
    timeout: float,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command, env=env, cwd=cwd, capture_output=True, text=True, timeout=timeout
    )
    if check and result.returncode != 0:
        tail = "\n".join((result.stdout + result.stderr).splitlines()[-40:])
        pytest.fail(f"{' '.join(command[:3])} failed ({result.returncode}):\n{tail}")
    return result


DIAGNOSE = """
echo "--- processes"; docker ps -a 2> /dev/null
pgrep -fa "ray|raylet|gcs" | head -20
echo "--- home"; ls -la ~ | head -30
echo "--- node setup"; tail -n 15 /var/log/raydog-node-setup.log 2> /dev/null
for f in $(find /var/opt/yellowdog -name taskoutput.txt 2> /dev/null); do
    echo "--- $f"; tail -n 60 "$f"
done
echo "--- disk and memory"; df -h / | tail -n 1; free -m | sed -n 2p
"""


def diagnose(address: str | None, key: Path) -> None:
    """
    Print what the head node has to say for itself: a failure on a real node is
    otherwise opaque. pytest shows this with the failure.
    """
    if not address:
        print("no head node address: nothing to diagnose")
        return
    try:
        result = subprocess.run(
            ["ssh", "-i", str(key), "-o", "StrictHostKeyChecking=no",
             "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR",
             "-o", "BatchMode=yes", "-o", "ConnectTimeout=20",
             "--", f"yd-agent@{address}", "bash -s"],
            input=DIAGNOSE, capture_output=True, text=True, timeout=120,
        )  # fmt: skip
        print(f"=== head node {address} ===\n{result.stdout}\n{result.stderr}")
    except (OSError, subprocess.SubprocessError) as e:
        print(f"could not reach the head node at {address}: {e}")


@pytest.fixture
def h() -> Any:
    harness = _real_harness()
    yield harness
    problems = harness.clean_up()
    harness.platform.close()
    if problems:
        pytest.fail("cleanup left resources behind: " + "; ".join(problems))


@pytest.mark.parametrize("variant", ["bare", "container"])
def test_ray_up_scale_and_down(h: Harness, tmp_path: Path, variant: str) -> None:
    """``bare``: Ray installed on the nodes. ``container``: Ray in Docker on each."""
    in_container = variant == "container"
    key = Path(
        _setting("RAYDOG_TEST_SSH_PRIVATE_KEY")
        or Path.home() / ".raydog-probe/id_ed25519"
    ).expanduser()
    public_key = Path(f"{key}.pub")
    if not key.is_file() or not public_key.is_file():
        pytest.skip("RAYDOG_TEST_SSH_PRIVATE_KEY (and its .pub) are needed")
    for program in ("ssh", "rsync"):
        if shutil.which(program) is None:
            pytest.skip(f"{program} is needed")

    bin_dir = Path(sys.executable).parent
    env = {
        **os.environ,
        "YD_API_KEY_ID": _setting("YD_API_KEY_ID") or "",
        "YD_API_KEY_SECRET": _setting("YD_API_KEY_SECRET") or "",
        "RAYDOG_NAMESPACE": h.namespace,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "RAY_USAGE_STATS_ENABLED": "0",
    }
    if _setting("YD_API_URL"):
        env["YD_API_URL"] = _setting("YD_API_URL") or ""
    ray, raydog = str(bin_dir / "ray"), str(bin_dir / "raydog")

    # The RayDog under test, as a wheel that Ray will copy to the head node
    wheels = tmp_path / "wheels"
    run(
        [sys.executable, "-m", "build", "-q", "-w", "-o", str(wheels)],
        env,
        REPO_ROOT,
        300,
    )
    # The example head script, changed only to install that wheel
    if in_container:
        head_script = (SCRIPTS / "head-start-docker.sh").read_text()
        changes = [
            (
                '    -e RAYDOG_PIP_SPEC="$RAYDOG_PIP_SPEC" \\\n',
                # At the same place under the container's home as on the host:
                # Ray's autoscaler looks at every file mount in the cluster config
                '    -v "$HOME/raydog-wheels:/root/raydog-wheels:ro" \\\n',
            ),
            (
                'pip install --quiet "$RAYDOG_PIP_SPEC"',
                "pip install --quiet /root/raydog-wheels/*.whl",
            ),
        ]
    else:
        head_script = (SCRIPTS / "head-start.sh").read_text()
        marker = "trap 'ray stop"
        install = (
            "uv pip install --python /opt/raydog/venv/bin/python --quiet "
            '"$HOME"/raydog-wheels/*.whl\n\n'
        )
        changes = [(marker, install + marker)]
    for old, new in changes:
        assert old in head_script, old
        head_script = head_script.replace(old, new, 1)
    (tmp_path / "head-start.sh").write_text(head_script)
    # The example worker script as it is: the node type's declared resources
    # (one CPU, whatever the instance has, and one "declared") are what its Ray uses
    worker_script = SCRIPTS / (
        "worker-start-docker.sh" if in_container else "worker-start.sh"
    )
    setup_script = SCRIPTS / (
        "node-setup-docker.sh" if in_container else "node-setup.sh"
    )

    name = h.name("as")
    settings_lines = (
        f'#!/usr/bin/bash\nSSH_PUBLIC_KEY="{public_key.read_text().strip()}"\n'
        'RAYDOG_PIP_SPEC="yellowdog-ray"\n'
    )
    if in_container and _setting("RAYDOG_TEST_DOCKER_IN_MEMORY", "1") != "0":
        settings_lines += DOCKER_IN_MEMORY

    def node(template: str) -> dict[str, Any]:
        config: dict[str, Any] = {
            "compute_requirement_template": template,
            "userdata": [settings_lines, f"file:{setup_script}"],
        }
        if h.images_id:
            config["images_id"] = h.images_id
        return config

    # A container's image needs more room (here, memory) than a small worker
    # instance may have, so the container variant can be given its own template
    worker_template = h.template
    if in_container:
        worker_template = (
            _setting("RAYDOG_TEST_CONTAINER_WORKER_TEMPLATE")
            or _setting("RAYDOG_TEST_HEAD_TEMPLATE")
            or h.template
        )
    cluster = {
        "cluster_name": name,
        "max_workers": 2,
        "idle_timeout_minutes": 1,
        "provider": {
            "type": "external",
            "module": "yellowdog_ray.autoscaler.RayDogNodeProvider",
            "namespace": h.namespace,
            "tag": "raydog-test",
            "lifetime_hours": 1,  # the backstop for everything started here
            "build_timeout_minutes": 12,
            "head_start_script": f"file:{tmp_path / 'head-start.sh'}",
            "worker_start_script": f"file:{worker_script}",
            "worker_start_grace_seconds": 30 if in_container else 0,
        },
        "auth": {"ssh_user": "yd-agent", "ssh_private_key": str(key)},
        "file_mounts": {"~/raydog-wheels": str(wheels)},
        "head_node_type": "head",
        "available_node_types": {
            "head": {
                "resources": {"CPU": 0},
                "node_config": node(
                    _setting("RAYDOG_TEST_HEAD_TEMPLATE") or h.template
                ),
            },
            "worker": {
                "resources": {"CPU": 1, "declared": 1},
                "min_workers": 0,
                "max_workers": 2,
                "node_config": node(worker_template),
            },
        },
        "setup_commands": [],
        "head_setup_commands": [],
        "worker_setup_commands": [],
    }
    config_file = tmp_path / "cluster.yaml"
    config_file.write_text(yaml.safe_dump(cluster, sort_keys=False))
    (tmp_path / "job.py").write_text(JOB)

    def status() -> dict[str, Any]:
        out = run([raydog, "status", name, "--json"], env, tmp_path, 120).stdout
        loaded = json.loads(out)
        assert isinstance(loaded, dict)
        return loaded

    def workers() -> int:
        pools = status()["pools"]
        return sum(
            sum(p["nodes"].values()) for role, p in pools.items() if role != "head"
        )

    head_address: str | None = None
    try:
        run(
            [ray, "up", "-y", "--no-config-cache", str(config_file)],
            env,
            tmp_path,
            1500,
        )
        state = status()
        head_address = state["head"]["public_ip"]
        assert state["state"] == "running" and state["head"]["status"] == "EXECUTING"
        assert workers() == 0
        listed = json.loads(run([raydog, "list", "--json"], env, tmp_path, 120).stdout)
        assert name in {c["name"] for c in listed}

        # A second `ray up` finds the cluster as it is
        run([ray, "up", "-y", str(config_file)], env, tmp_path, 600)
        assert status()["head"]["task_id"] == state["head"]["task_id"]

        # The head's dashboard, through a real SSH tunnel
        with socket.socket() as s:
            s.bind(("localhost", 0))
            port = s.getsockname()[1]
        with RayTunnels(
            state["head"]["public_ip"],
            "yd-agent",
            private_key=key,
            forwards=[PortForward(port, 8265)],
        ):
            # `ray up` returns while Ray is still starting on the head node, so
            # the dashboard may not be answering yet
            url = f"http://localhost:{port}/api/version"
            deadline = time.monotonic() + 180
            while True:
                try:
                    with urllib.request.urlopen(url, timeout=30) as response:
                        assert "ray_version" in response.read().decode()
                    break
                except OSError:
                    assert time.monotonic() < deadline, "the dashboard never answered"
                    time.sleep(5)

        # A job needing two CPUs, on a head that offers none: workers must appear
        run(
            [ray, "rsync-up", str(config_file), "job.py", "~/job.py"],
            env,
            tmp_path,
            300,
        )
        run_job = (
            "docker exec -i $(docker ps -q --filter name=raydog-head) "
            "python - < ~/job.py"
            if in_container
            else "source /opt/raydog/venv/bin/activate && python ~/job.py"
        )
        job = run([ray, "exec", str(config_file), run_job], env, tmp_path, 1200)
        result = next(
            line for line in job.stdout.splitlines() if line.startswith("RESULT ")
        )
        addresses = result.split()[1].split(",")
        assert len(addresses) == 2, "the two tasks ran on two different workers"
        assert state["head"]["private_ip"] not in addresses

        # The provider on the head node says what it did, in Ray's monitor log, which
        # is inside the head's container in the container variant
        log = "/tmp/ray/session_latest/logs/monitor.log"
        grep = f"grep -c 'node(s) in cluster' {log}"
        if in_container:
            grep = f"docker exec $(docker ps -q --filter name=raydog-head) {grep}"
        # (ray exec mixes its own log lines into the output, so the count is marked)
        count = f"echo RAYDOG_LINES=$({grep})"
        told = run([ray, "exec", str(config_file), count], env, tmp_path, 300)
        found = re.search(r"RAYDOG_LINES=(\d+)", told.stdout + told.stderr)
        assert found and int(found[1]) >= 1, told.stdout[-2000:]

        # Ray's own commands work on the head as they would with any provider
        ray_status = run(
            [ray, "exec", str(config_file), "ray status"], env, tmp_path, 300
        )
        assert "Resources" in ray_status.stdout, ray_status.stdout[-2000:]

        # With nothing left to do, Ray lets the workers go
        deadline = time.monotonic() + 420
        while workers() > 0:
            assert time.monotonic() < deadline, "idle workers were not removed"
            time.sleep(15)

        down = run([ray, "down", "-y", str(config_file)], env, tmp_path, 1200)
        # It begins with `ray stop` on the head, which must neither fail nor stop Ray
        # under the head's task: the nodes end when RayDog ends their tasks
        said = down.stdout + down.stderr
        assert "'ray stop' ignored" in said, said[-3000:]
        assert "not found" not in said and "Exception occurred" not in said, said[
            -3000:
        ]
        for pool in h.platform.list_pools(h.namespace, name_contains=name):
            assert h.platform.live_instance_count(pool.id) == 0, pool.name
        # The work requirement goes through CANCELLING, which can take a minute or so
        # after its last task has ended; `ray down` waits for the instances, not this
        deadline = time.monotonic() + 120
        while True:
            gone = run(
                [raydog, "status", name, "--json"], env, tmp_path, 120, check=False
            )
            assert gone.returncode in (0, 3)
            wr_state = (
                json.loads(gone.stdout)["state"] if gone.returncode == 0 else None
            )
            if wr_state in (None, "cancelled") or time.monotonic() > deadline:
                break
            time.sleep(10)
        assert wr_state in (None, "cancelled"), wr_state
    except BaseException:
        if head_address is None:
            with contextlib.suppress(Exception):
                head_address = status()["head"]["public_ip"]
        diagnose(head_address, key)
        raise
    finally:
        # Whatever happened above, do not leave it running
        down = run(
            [raydog, "down", name, "--yes", "--wait"], env, tmp_path, 900, check=False
        )
        assert down.returncode in (0, 3), down.stdout + down.stderr
