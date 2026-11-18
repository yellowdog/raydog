"""
The Autoscaler example configs are valid and bring a cluster up and down.

The templates are rendered as ``up.sh`` would render them, bootstrapped as ``ray up``
would bootstrap them, and then driven through the provider against the in-memory
platform using the same replay of Ray's calls as the unit tests.
"""

from __future__ import annotations

import base64
import importlib.util
import subprocess
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml
from ray.autoscaler._private.util import prepare_config, validate_config

from tests.unit.test_autoscaler_provider import (
    WORKER_KIND,
    launch,
    make,
    monitor_iteration,
    ray_down,
    ray_up,
)
from yellowdog_ray.autoscaler import RayDogNodeProvider
from yellowdog_ray.autoscaler.config import HEAD_START_NOTE
from yellowdog_ray.core.platform import TaskStatus
from yellowdog_ray.testing import FakePlatform

EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "autoscaler"
TEMPLATES = ["raydog.yaml", "raydog-docker.yaml"]
PUBLIC_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEXAMPLE user@example"


# A developer's own examples/autoscaler/.env must not change what these tests see
NO_ENV_FILE = Path("no-such-file.env")


def load_render() -> ModuleType:
    spec = importlib.util.spec_from_file_location("render", EXAMPLE / "render.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    key = tmp_path / "id_test"
    key.write_text("not a real key")
    Path(f"{key}.pub").write_text(PUBLIC_KEY + "\n")
    for name, value in {
        "HOME": str(tmp_path),
        "YD_API_KEY_ID": "id",
        "YD_API_KEY_SECRET": "the-secret",
        "RAYDOG_NAMESPACE": "ns",
        "RAYDOG_HEAD_TEMPLATE": "ns/head",
        "RAYDOG_WORKER_TEMPLATE": "ns/worker",
        "RAYDOG_SSH_PRIVATE_KEY": str(key),
    }.items():
        monkeypatch.setenv(name, value)
    for name in ("RAYDOG_CLUSTER_NAME", "RAYDOG_IMAGES_ID", "RAYDOG_PIP_SPEC",
                 "RAYDOG_SSH_PUBLIC_KEY", "RAYDOG_TAG", "RAYDOG_OBSERVABILITY",
                 "YD_API_URL"):  # fmt: skip
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(EXAMPLE)  # as up.sh does
    return tmp_path


def rendered(template: str) -> dict[str, Any]:
    render = load_render()
    text = render.render((EXAMPLE / template).read_text(), render.settings(NO_ENV_FILE))
    assert not render.SETTING.search(text)
    loaded = yaml.safe_load(text)
    assert isinstance(loaded, dict)
    return loaded


def as_ray_bootstraps(config: dict[str, Any]) -> dict[str, Any]:
    """The steps of Ray's ``_bootstrap_config``, in Ray's order."""
    config = prepare_config(config)
    config = RayDogNodeProvider.fillout_available_node_types_resources(config)
    validate_config(config)  # Ray's own schema and sanity checks
    return RayDogNodeProvider.bootstrap_config(config)


def test_a_small_yaml_is_enough() -> None:
    """
    Most of Ray's general settings need not be spelled out. ``max_workers`` and
    the three setup command lists must be, because Ray reads them before any
    provider code runs.
    """
    minimal = {
        "max_workers": 4,
        "setup_commands": [],
        "head_setup_commands": [],
        "worker_setup_commands": [],
        "cluster_name": "tiny",
        "provider": {
            "type": "external",
            "module": "yellowdog_ray.autoscaler.RayDogNodeProvider",
            "namespace": "ns",
            "head_start_script": "ray start --head --block",
            "worker_start_script": "ray start --address=$RAY_HEAD_IP:6379 --block",
        },
        "auth": {"ssh_user": "yd-agent", "ssh_private_key": "~/.ssh/id_ed25519"},
        "head_node_type": "head",
        "available_node_types": {
            "head": {
                "resources": {"CPU": 0},
                "node_config": {"compute_requirement_template": "ns/head"},
            },
            "worker": {
                "resources": {"CPU": 2},
                "max_workers": 4,
                "node_config": {"compute_requirement_template": "ns/worker"},
            },
        },
    }
    config = as_ray_bootstraps(minimal)
    assert config["cluster_synced_files"] == [] and config["rsync_exclude"] == []
    assert config["upscaling_speed"] == 1.0
    # RayDog's note in place of a start command, so that `ray up` does not warn
    assert config["head_start_ray_commands"] == HEAD_START_NOTE
    assert config["provider"]["raydog_max_workers"] == {"worker": 4}
    assert as_ray_bootstraps(config) == config


@pytest.mark.parametrize("template", TEMPLATES)
def test_the_example_is_a_valid_ray_cluster_config(template: str) -> None:
    bootstrapped = as_ray_bootstraps(rendered(template))
    provider = bootstrapped["provider"]
    assert provider["head_start_script"].startswith("#!/usr/bin/bash")
    assert "ray start --head" in provider["head_start_script"]
    assert "--autoscaling-config" in provider["head_start_script"]
    assert "RAY_HEAD_IP" in provider["worker_start_script"]
    for node_type in bootstrapped["available_node_types"].values():
        settings_lines, script = node_type["node_config"]["userdata"]
        assert settings_lines.startswith("#!/usr/bin/bash\n")
        assert f'SSH_PUBLIC_KEY="{PUBLIC_KEY}"' in settings_lines
        assert 'RAYDOG_PIP_SPEC="yellowdog-ray"' in settings_lines
        assert "yd-agent-installer.sh" in script
    assert "file:" not in str(provider)
    assert "the-secret" not in str(bootstrapped)


@pytest.mark.parametrize("template", TEMPLATES)
def test_the_example_cluster_comes_up_scales_and_goes_down(template: str) -> None:
    p = FakePlatform()
    raw = rendered(template)
    raw["provider"]["refresh_interval_seconds"] = 0
    config = as_ray_bootstraps(raw)

    assert config["cluster_name"] == "raydog-example"
    # make() builds providers for a cluster called "demo": use its class for ours
    client = type(make(p, config["provider"]))(
        config["provider"], config["cluster_name"]
    )
    head_id, head_config = ray_up(client, config)
    assert p.get_task(head_id).status is TaskStatus.EXECUTING

    head = type(client)(head_config["provider"], config["cluster_name"])
    launch(head, config, "worker", 2)
    p.clock.advance(300)
    assert len(monitor_iteration(head)) == 3 and p.live_instances == 3
    assert all(head.is_running(n) for n in head.non_terminated_nodes(WORKER_KIND))

    ray_down(type(client)(config["provider"], config["cluster_name"]))
    assert p.live_instances == 0


@pytest.mark.parametrize(
    "script",
    [
        *sorted(p.name for p in (EXAMPLE / "scripts").glob("*.sh")),
        "up.sh",
        "down.sh",
        "submit-job.sh",
    ],
)
def test_shell_scripts_parse(script: str) -> None:
    path = (
        EXAMPLE / script
        if (EXAMPLE / script).exists()
        else EXAMPLE / "scripts" / script
    )
    result = subprocess.run(["bash", "-n", str(path)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("template", TEMPLATES)
def test_the_cluster_is_tagged_with_raydog_tag(
    template: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RAYDOG_TAG tags the work requirement; without it, there is no tag."""
    untagged = as_ray_bootstraps(rendered(template))
    assert cfg_parse(untagged).tag is None
    monkeypatch.setenv("RAYDOG_TAG", "yes")  # text, even where YAML would see a bool
    tagged = as_ray_bootstraps(rendered(template))
    assert cfg_parse(tagged).tag == "yes"


def userdata_of(config: dict[str, Any], node_type: str) -> str:
    """A node type's userdata as RayDog sends it: the parts joined into a script."""
    from yellowdog_ray.autoscaler.config import parse_node_config

    spec = parse_node_config(
        node_type, config["available_node_types"][node_type]["node_config"]
    )
    assert spec.userdata is not None
    return spec.userdata


def test_observability_is_set_up_on_the_head_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    RAYDOG_OBSERVABILITY=1 turns on Prometheus and Grafana in the head node's setup;
    workers need nothing, because Ray tells Prometheus where their metrics are.
    """
    off = as_ray_bootstraps(rendered("raydog.yaml"))
    assert 'RAYDOG_OBSERVABILITY=""' in userdata_of(off, "head")
    monkeypatch.setenv("RAYDOG_OBSERVABILITY", "1")
    on = as_ray_bootstraps(rendered("raydog.yaml"))
    assert 'RAYDOG_OBSERVABILITY="1"' in userdata_of(on, "head")
    assert 'RAYDOG_OBSERVABILITY="1"' not in userdata_of(on, "worker")


def test_the_container_variant_has_no_observability() -> None:
    """Ray's files are inside the head's container, out of the services' sight."""
    assert "RAYDOG_OBSERVABILITY" not in (EXAMPLE / "raydog-docker.yaml").read_text()


@pytest.mark.parametrize("template", TEMPLATES)
def test_every_node_types_userdata_fits_in_an_instance(
    template: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """EC2 takes at most 16 KB of userdata, base64-encoded."""
    monkeypatch.setenv("RAYDOG_OBSERVABILITY", "1")
    config = as_ray_bootstraps(rendered(template))
    for node_type in config["available_node_types"]:
        script = userdata_of(config, node_type)
        assert len(base64.b64encode(script.encode())) <= 16 * 1024, node_type


def cfg_parse(config: dict[str, Any]) -> Any:
    from yellowdog_ray.autoscaler.config import parse_provider_config

    return parse_provider_config(config["provider"], config["cluster_name"])


def test_render_names_missing_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RAYDOG_NAMESPACE")
    monkeypatch.delenv("RAYDOG_WORKER_TEMPLATE")
    render = load_render()
    with pytest.raises(SystemExit, match="RAYDOG_NAMESPACE, RAYDOG_WORKER_TEMPLATE"):
        render.render(
            (EXAMPLE / "raydog.yaml").read_text(), render.settings(NO_ENV_FILE)
        )


@pytest.mark.parametrize(
    "key", ['ssh-ed25519 AAAA" ; reboot ; "', "ssh-ed25519 AAAA $(id)"]
)
def test_render_refuses_a_public_key_that_is_not_one(
    monkeypatch: pytest.MonkeyPatch, key: str
) -> None:
    monkeypatch.setenv("RAYDOG_SSH_PUBLIC_KEY", key)
    render = load_render()
    with pytest.raises(SystemExit, match="does not look like"):
        render.render(
            (EXAMPLE / "raydog.yaml").read_text(), render.settings(NO_ENV_FILE)
        )


def test_render_leaves_shell_variables_alone() -> None:
    render = load_render()
    text = "a: ${RAYDOG_NAMESPACE}\nb: $HOME ${HOME} $RAY_HEAD_IP ${OTHER_THING}\n"
    assert render.render(text, {"RAYDOG_NAMESPACE": "ns"}) == (
        "a: ns\nb: $HOME ${HOME} $RAY_HEAD_IP ${OTHER_THING}\n"
    )


def test_the_job_is_alone_in_its_directory() -> None:
    """``ray job submit --working-dir`` uploads the whole directory to the cluster."""
    assert sorted(p.name for p in (EXAMPLE / "job").iterdir()) == ["sleep_job.py"]


def test_the_container_head_script_gives_the_autoscaler_what_it_looks_for() -> None:
    """
    Found on real nodes: Ray's autoscaler, inside the head's container, reads the
    key (or starts no workers) and the credentials, at their usual places under its
    own home directory.
    """
    script = (EXAMPLE / "scripts" / "head-start-docker.sh").read_text()
    assert '-v "$KEY:/root/ray_bootstrap_key.pem:ro"' in script
    assert '-v "$HOME/.raydog:/root/.raydog:ro"' in script
    assert "-e HOME=/root" in script


def login_shell(setup_script: Path, tmp_path: Path, tool: str, command: str) -> str:
    """
    Run a command as Ray's cluster launcher does on a node: in a shell that has
    read the ``.bashrc`` the setup script writes, with a stand-in ``tool`` that
    reports how it was called.
    """
    text = setup_script.read_text()
    assert "<< 'BASHRC'" in text, "the setup script writes no .bashrc"
    bashrc = text.split("<< 'BASHRC'\n", 1)[1].split("\nBASHRC\n", 1)[0]
    fake = tmp_path / "bin" / tool
    fake.parent.mkdir(exist_ok=True)
    fake.write_text(f'#!/bin/sh\necho "REAL {tool} $*"\n')
    fake.chmod(0o755)
    # The environment's own directory goes on the PATH in place of the node's
    bashrc_path = tmp_path / "bashrc"
    bashrc_path.write_text(bashrc.replace("/opt/raydog/venv/bin", str(fake.parent)))
    result = subprocess.run(
        ["bash", "-c", f"source {bashrc_path}; ({command})"],
        capture_output=True,
        text=True,
        env={"PATH": f"{fake.parent if tool == 'docker' else ''}:/usr/bin:/bin"},
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_ray_commands_work_in_a_login_shell_on_a_node(tmp_path: Path) -> None:
    """``ray exec cluster.yaml 'ray status'`` must find Ray."""
    out = login_shell(
        EXAMPLE / "scripts" / "node-setup.sh", tmp_path, "ray", "ray status"
    )
    assert out.strip() == "REAL ray status"


def test_ray_stop_in_a_login_shell_leaves_the_node_running(tmp_path: Path) -> None:
    """
    ``ray down`` begins with ``ray stop`` on the head. A node is stopped by ending
    its task, so that command must not stop Ray under the task.
    """
    out = login_shell(
        EXAMPLE / "scripts" / "node-setup.sh", tmp_path, "ray", "ray stop"
    )
    assert "REAL" not in out and "task" in out


def test_ray_commands_reach_the_container_on_a_container_node(tmp_path: Path) -> None:
    out = login_shell(
        EXAMPLE / "scripts" / "node-setup-docker.sh", tmp_path, "docker", "ray status"
    )
    assert "REAL docker exec" in out and out.strip().endswith("ray status")
    out = login_shell(
        EXAMPLE / "scripts" / "node-setup-docker.sh", tmp_path, "docker", "ray stop"
    )
    assert "REAL docker exec" not in out and "task" in out
