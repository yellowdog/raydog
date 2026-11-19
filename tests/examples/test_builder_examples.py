"""
The Builder examples run end to end against the in-memory platform.

They are real scripts, so they are loaded from their files; the only things replaced
are the connection to YellowDog, the clock, and (for the usage example) the part that
needs a real Ray cluster to talk to.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

from yellowdog_ray import PortForward, RayTunnels
from yellowdog_ray.core import cluster as core_cluster
from yellowdog_ray.core import yellowdog
from yellowdog_ray.core.cluster import NodeAddress
from yellowdog_ray.testing import FakePlatform

from .test_autoscaler_examples import USERDATA_LIMIT, login_shell

EXAMPLES = Path(__file__).resolve().parents[2] / "examples" / "builder"


def load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        name.replace("-", "_"), EXAMPLES / name
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def platform(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakePlatform:
    fake = FakePlatform()
    key = tmp_path / "id_test"
    key.write_text("not a real key")
    Path(f"{key}.pub").write_text("ssh-ed25519 AAAATEST example\n")
    for name, value in {
        "YD_API_KEY_ID": "id",
        "YD_API_KEY_SECRET": "secret",
        "RAYDOG_NAMESPACE": "ns",
        "RAYDOG_HEAD_TEMPLATE": "ns/head",
        "RAYDOG_WORKER_TEMPLATE": "ns/worker",
        "RAYDOG_SSH_PRIVATE_KEY": str(key),
    }.items():
        monkeypatch.setenv(name, value)
    for name in (
        "YD_API_URL",
        "RAYDOG_IMAGES_ID",
        "RAYDOG_CLUSTER_NAME",
        "RAYDOG_USE_CONTAINERS",
        "RAYDOG_OBSERVABILITY",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)  # no stray .env
    monkeypatch.syspath_prepend(str(EXAMPLES))  # the examples import settings.py
    monkeypatch.delitem(sys.modules, "settings", raising=False)
    monkeypatch.setattr(yellowdog.YellowDogPlatform, "connect", lambda *args: fake)
    monkeypatch.setattr(core_cluster, "SystemClock", lambda: fake.clock)
    return fake


def test_usage_example(
    platform: FakePlatform, capsys: pytest.CaptureFixture[str]
) -> None:
    example = load("usage-example.py")
    jobs = []

    def fake_job(
        head: object,
        ssh_user: str,
        ssh_private_key: str,
        workers: int,
        observability: str,
        observability_node: object,
    ) -> None:
        assert workers == 2 and observability == "" and observability_node is None
        jobs.append((ssh_user, platform.live_instances))

    setattr(example, "run_ray_job", fake_job)  # noqa: B010
    example.main()
    assert jobs == [("yd-agent", 3)]  # the head and two workers were up for the job
    platform.clock.advance(300)
    assert platform.live_instances == 0
    assert platform.list_work_requirements("ns", live_only=True) == []
    out = capsys.readouterr().out
    assert "Head node: private IP 10." in out and "Cluster shut down" in out


def test_usage_example_with_an_observability_node(
    platform: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    RAYDOG_OBSERVABILITY=node: the observability node starts before the head, and
    the job's tunnel reaches Grafana on it, where Ray's dashboard expects it.
    """
    monkeypatch.setenv("RAYDOG_OBSERVABILITY", "node")
    example = load("usage-example.py")
    seen: list[tuple[object, ...]] = []
    setattr(example, "run_ray_job", lambda *args: seen.append(args[-2:]))  # noqa: B010
    example.main()
    [(observability, node)] = seen
    assert observability == "node"
    assert node is not None and node.private_ip  # type: ignore[attr-defined]
    started = [
        task.name.split("-")[0]
        for c in platform.calls
        if c.method == "add_tasks"
        for task in c.args[1]
    ]
    assert started[:2] == ["observability", "head"]
    platform.clock.advance(120)
    assert platform.live_instances == 0


@pytest.mark.parametrize("value", ["1", "head", "yes"])
def test_usage_example_with_observability_on_the_head(
    platform: FakePlatform, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """
    RAYDOG_OBSERVABILITY=1, the default kind: Prometheus and Grafana on the head,
    with no node of their own.
    """
    monkeypatch.setenv("RAYDOG_OBSERVABILITY", value)
    example = load("usage-example.py")
    seen: list[tuple[object, ...]] = []
    setattr(example, "run_ray_job", lambda *args: seen.append(args[-2:]))  # noqa: B010
    example.main()
    assert seen == [("head", None)]
    specs = [c.args[0] for c in platform.calls if c.method == "provision_pool"]
    assert len(specs) == 2
    assert not any(spec.name.endswith("observability") for spec in specs)


def test_without_observability_there_is_no_observability_node(
    platform: FakePlatform,
) -> None:
    example = load("usage-example.py")
    seen: list[tuple[object, ...]] = []
    setattr(example, "run_ray_job", lambda *args: seen.append(args[-2:]))  # noqa: B010
    example.main()
    assert seen == [("", None)]
    names = [c.args[0].name for c in platform.calls if c.method == "provision_pool"]
    assert not any(name.endswith("observability") for name in names)


@pytest.mark.parametrize("value", ["1", "node"])
def test_observability_is_refused_with_containers(
    platform: FakePlatform, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """The container start scripts support neither kind."""
    monkeypatch.setenv("RAYDOG_OBSERVABILITY", value)
    monkeypatch.setenv("RAYDOG_USE_CONTAINERS", "1")
    example = load("usage-example.py")
    with pytest.raises(SystemExit, match="RAYDOG_USE_CONTAINERS"):
        example.main()
    assert platform.calls == []


def test_an_unknown_kind_of_observability_is_refused(
    platform: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RAYDOG_OBSERVABILITY", "loki")
    example = load("usage-example.py")
    with pytest.raises(SystemExit, match="RAYDOG_OBSERVABILITY must be"):
        example.main()
    assert platform.calls == []


def test_the_grafana_forward_goes_where_grafana_is() -> None:
    example = load("usage-example.py")
    address = NodeAddress(task_id="t", private_ip="10.0.0.9", public_ip=None)
    forwards = example.tunnel_forwards("node", address)
    assert PortForward(3000, 3000, remote_host="10.0.0.9") in forwards
    assert RayTunnels.CLIENT in forwards and RayTunnels.DASHBOARD in forwards
    assert PortForward(3000, 3000) in example.tunnel_forwards("head", None)
    assert example.tunnel_forwards("", None) == (
        RayTunnels.CLIENT,
        RayTunnels.DASHBOARD,
    )


def test_usage_example_shuts_down_when_the_job_fails(platform: FakePlatform) -> None:
    example = load("usage-example.py")

    def failing_job(*args: object) -> None:
        raise RuntimeError("job failed")

    setattr(example, "run_ray_job", failing_job)  # noqa: B010
    with pytest.raises(RuntimeError, match="job failed"):
        example.main()
    platform.clock.advance(300)
    assert platform.live_instances == 0


def test_manage_example(
    platform: FakePlatform, capsys: pytest.CaptureFixture[str]
) -> None:
    example = load("manage-example.py")
    example.main()
    assert platform.live_instances == 0  # it waited for the instances to go
    out = capsys.readouterr().out
    assert "pool 'small': 3 node(s), maximum 4" in out
    assert "pool 'burst': 2 node(s), maximum 2" in out
    assert "re-attached from saved state: head running" in out
    assert "Cluster shut down" in out


def test_the_node_setup_script_gets_the_users_public_key(
    platform: FakePlatform,
) -> None:
    example = load("usage-example.py")
    setattr(example, "run_ray_job", lambda *args: None)  # noqa: B010
    example.main()
    specs = [c.args[0] for c in platform.calls if c.method == "provision_pool"]
    assert specs and all("ssh-ed25519 AAAATEST example" in s.userdata for s in specs)
    assert all("REPLACE_WITH_YOUR_PUBLIC_KEY" not in s.userdata for s in specs)


@pytest.mark.parametrize(
    ("value", "head", "worker"),
    [("", "none", "none"), ("1", "head", "none"), ("node", "alloy", "alloy")],
)
def test_each_node_is_set_up_for_its_part_in_observability(
    platform: FakePlatform,
    monkeypatch: pytest.MonkeyPatch,
    value: str,
    head: str,
    worker: str,
) -> None:
    """
    Whatever a node needs is installed at boot, as root and before the Agent:
    Prometheus and Grafana on the head, or Alloy on the head and workers to send
    their data to the observability node, which gets Prometheus, Loki and Grafana.
    Each node's script carries only the functions it calls (see USERDATA_LIMIT).
    """
    monkeypatch.setenv("RAYDOG_OBSERVABILITY", value)
    settings = importlib.import_module("settings").load(default_cluster_name="c")
    roles = {
        settings.head.userdata: head,
        settings.worker.userdata: worker,
        settings.observability_node.userdata: "node",
    }
    for userdata, role in roles.items():
        assert f'OBSERVABILITY="{role}"' in userdata
        assert "REPLACE_WITH" not in userdata
        defined = {f for f in FUNCTIONS.values() if f"{f}() {{" in userdata}
        assert defined == NEEDS[role], role
        assert ("prometheus.remote_write" in userdata) == (role in ("alloy", "node"))


# What each OBSERVABILITY role calls, and the files that define it
FUNCTIONS = {
    "head.sh": "setup_head_observability",
    "alloy.sh": "install_alloy",
    "node.sh": "setup_observability",
}
NEEDS = {
    "none": set(),
    "head": {"setup_head_observability"},
    "alloy": {"install_alloy"},
    "node": {"install_alloy", "setup_observability"},
}


@pytest.mark.parametrize(("name", "function"), FUNCTIONS.items())
def test_each_observability_file_defines_what_the_setup_script_calls(
    name: str, function: str
) -> None:
    setup = (EXAMPLES / "scripts" / "node-setup-userdata.sh").read_text()
    text = (EXAMPLES / "scripts" / "observability" / name).read_text()
    assert f"{function}() {{" in text and f"{function}() {{" not in setup
    assert function in setup


def test_the_head_points_rays_dashboard_at_its_own_grafana() -> None:
    """Set up as in the Autoscaler example, and found by the same marker file."""
    setup = (EXAMPLES / "scripts" / "observability" / "head.sh").read_text()
    start = (EXAMPLES / "scripts" / "head-node-task-script.sh").read_text()
    assert "prom_metrics_service_discovery.json" in setup
    assert "PrivateTmp=false" in setup
    assert "touch /etc/raydog-observability" in setup
    assert "-f /etc/raydog-observability" in start
    assert 'RAY_PROMETHEUS_HOST="http://localhost:9090"' in start


@pytest.mark.parametrize("observability", ["", "1", "node"])
@pytest.mark.parametrize("containers", ["0", "1"])
def test_every_nodes_userdata_fits_in_an_instance(
    platform: FakePlatform,
    monkeypatch: pytest.MonkeyPatch,
    containers: str,
    observability: str,
) -> None:
    """See USERDATA_LIMIT."""
    monkeypatch.setenv("RAYDOG_USE_CONTAINERS", containers)
    if containers == "1" and observability:
        return  # refused
    monkeypatch.setenv("RAYDOG_OBSERVABILITY", observability)
    settings = importlib.import_module("settings").load(default_cluster_name="c")
    for spec in (settings.head, settings.worker, settings.observability_node):
        assert len(spec.userdata.encode()) <= USERDATA_LIMIT
        assert spec.userdata.startswith("#!/usr/bin/bash\n")
        # What is sent, shortened, is still a script
        parsed = subprocess.run(
            ["bash", "-n"], input=spec.userdata, capture_output=True, text=True
        )
        assert parsed.returncode == 0, parsed.stderr


def test_the_observability_node_keeps_its_setup_with_containers(
    platform: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    """It runs no Ray of its own, so containers make no difference to it."""
    monkeypatch.setenv("RAYDOG_USE_CONTAINERS", "1")
    settings = importlib.import_module("settings").load(default_cluster_name="c")
    assert 'OBSERVABILITY="node"' in settings.observability_node.userdata


def test_the_container_variant_uses_the_container_scripts(
    platform: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RAYDOG_USE_CONTAINERS", "1")
    example = load("usage-example.py")
    setattr(example, "run_ray_job", lambda *args: None)  # noqa: B010
    example.main()
    specs = [c.args[0] for c in platform.calls if c.method == "provision_pool"]
    assert len(specs) == 2
    for spec in specs:
        assert spec.userdata.startswith(
            '#!/usr/bin/bash\nSSH_PUBLIC_KEY="ssh-ed25519 AAAATEST example"\n'
        )
        assert "docker pull" in spec.userdata and "uv venv" not in spec.userdata
    scripts = [
        task.script
        for c in platform.calls
        if c.method == "add_tasks"
        for task in c.args[1]
    ]
    assert scripts and all("docker run" in script for script in scripts)
    assert any("--head" in script for script in scripts)
    assert any("RAY_HEAD_IP" in script for script in scripts)


def test_the_start_scripts_do_not_open_the_dashboard_to_the_network() -> None:
    for script in (EXAMPLES / "scripts").glob("head-node-task-script*.sh"):
        assert "--dashboard-host" not in script.read_text(), script.name


def test_missing_settings_are_named(
    platform: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("RAYDOG_NAMESPACE")
    example = load("usage-example.py")
    with pytest.raises(SystemExit, match="RAYDOG_NAMESPACE"):
        example.main()
    assert platform.calls == []


@pytest.mark.parametrize(
    "key",
    [
        'ssh-ed25519 AAAA" ; touch /tmp/pwned ; "',
        "ssh-ed25519 AAAA $(reboot)",
        "ssh-ed25519 AAAA `id`",
        "not a key at all\nsecond line",
        "",
    ],
)
def test_a_public_key_that_is_not_one_is_refused(
    platform: FakePlatform, tmp_path: Path, key: str
) -> None:
    Path(f"{tmp_path / 'id_test'}.pub").write_text(key)
    example = load("usage-example.py")
    with pytest.raises(SystemExit, match="does not look like"):
        example.main()
    assert platform.calls == []


def test_manage_example_shuts_down_if_reattaching_fails(
    platform: FakePlatform, monkeypatch: pytest.MonkeyPatch
) -> None:
    example = load("manage-example.py")
    real = example.RayDogCluster.from_state

    def broken(*args: object, **kwargs: object) -> object:
        raise KeyboardInterrupt

    monkeypatch.setattr(example.RayDogCluster, "from_state", broken)
    with pytest.raises(KeyboardInterrupt):
        example.main()
    monkeypatch.setattr(example.RayDogCluster, "from_state", real)
    platform.clock.advance(120)
    assert platform.live_instances == 0
    assert platform.list_work_requirements("ns", live_only=True) == []


def test_ray_commands_work_in_a_login_shell_on_a_node(tmp_path: Path) -> None:
    """
    Someone who connects to a node over SSH finds Ray, and cannot stop it from
    under the node's task by mistake.
    """
    setup = EXAMPLES / "scripts" / "node-setup-userdata.sh"
    assert login_shell(setup, tmp_path, "ray", "ray status").strip() == (
        "REAL ray status"
    )
    out = login_shell(setup, tmp_path, "ray", "ray stop")
    assert "REAL" not in out and "task" in out
