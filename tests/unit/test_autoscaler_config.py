from __future__ import annotations

import copy
import subprocess
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from yellowdog_ray.autoscaler import config as cfg
from yellowdog_ray.errors import ConfigError


def cluster_config(**provider: object) -> dict[str, Any]:
    return {
        "cluster_name": "demo",
        "max_workers": 10,
        "provider": {
            "type": "external",
            "module": cfg.MODULE,
            "namespace": "ns",
            "head_start_script": "ray start --head",
            "worker_start_script": "ray start --address=$RAY_HEAD_IP:6379",
            **provider,
        },
        "auth": {"ssh_user": "yd-agent", "ssh_private_key": "~/.ssh/key"},
        "head_node_type": "head",
        "available_node_types": {
            "head": {
                "resources": {"CPU": 0},
                "node_config": {"compute_requirement_template": "ns/head"},
            },
            "small": {
                "resources": {"CPU": 1},
                "max_workers": 3,
                "node_config": {
                    "compute_requirement_template": "ns/worker",
                    "images_id": "ami-1",
                },
            },
            "big.gpu": {
                "resources": {"CPU": 8, "GPU": 1},
                "node_config": {"compute_requirement_template": "ns/gpu"},
            },
        },
        "file_mounts": {},
        **{key: [] for key in cfg.COMMAND_LISTS},
    }


def provider(**overrides: object) -> dict[str, object]:
    return cluster_config(**overrides)["provider"]  # type: ignore[no-any-return]


# --- the provider section ---------------------------------------------------------


def test_defaults() -> None:
    parsed = cfg.parse_provider_config(provider(), "demo")
    assert (parsed.cluster_name, parsed.namespace, parsed.tag) == ("demo", "ns", None)
    assert parsed.lifetime == timedelta(hours=24)
    assert parsed.build_timeout == timedelta(minutes=10)
    assert parsed.node_idle_shutdown == timedelta(seconds=60)
    assert parsed.worker_start_grace == timedelta(0)
    assert parsed.refresh_interval == timedelta(seconds=2)
    assert parsed.head is None and dict(parsed.max_workers) == {}
    settings = parsed.settings()
    assert (settings.name, settings.namespace) == ("demo", "ns")
    assert settings.idle_shutdown == timedelta(seconds=60)


def test_every_setting_can_be_given() -> None:
    parsed = cfg.parse_provider_config(
        provider(
            tag="t",
            lifetime_hours=0.5,
            build_timeout_minutes=4,
            node_idle_shutdown_seconds=0,
            worker_start_grace_seconds=45,
            refresh_interval_seconds=0,
            use_internal_ips=True,
        ),
        "demo",
    )
    assert parsed.tag == "t" and parsed.lifetime == timedelta(minutes=30)
    assert parsed.build_timeout == timedelta(minutes=4)
    assert parsed.node_idle_shutdown == timedelta(0)
    assert parsed.worker_start_grace == timedelta(seconds=45)
    assert parsed.refresh_interval == timedelta(0)


@pytest.mark.parametrize("value", ["none", "None", "NONE"])
def test_lifetime_can_be_unlimited(value: str) -> None:
    assert (
        cfg.parse_provider_config(provider(lifetime_hours=value), "demo").lifetime
        is None
    )


@pytest.mark.parametrize(
    ("overrides", "names"),
    [
        ({"namespace": None}, "namespace"),
        ({"namespace": ""}, "namespace"),
        ({"head_start_script": None}, "head_start_script"),
        ({"worker_start_script": 7}, "worker_start_script"),
        ({"tag": 7}, "tag"),
        ({"lifetime_hours": 0}, "lifetime_hours"),
        ({"lifetime_hours": -1}, "lifetime_hours"),
        ({"lifetime_hours": "forever"}, "lifetime_hours"),
        ({"lifetime_hours": 0.00001}, "lifetime_hours"),
        ({"build_timeout_minutes": 0}, "build_timeout_minutes"),
        ({"build_timeout_minutes": True}, "build_timeout_minutes"),
        ({"node_idle_shutdown_seconds": -1}, "node_idle_shutdown_seconds"),
        ({"worker_start_grace_seconds": "soon"}, "worker_start_grace_seconds"),
        ({"disable_node_updaters": "yes"}, "disable_node_updaters"),
        ({"disable_node_updaters": False}, "disable_node_updaters"),
        ({"cluster_namespace": "ns"}, "cluster_namespace"),  # the v2 spelling
        ({"files_to_upload": []}, "files_to_upload"),
        ({"raydog_head": {"work_requirement_id": 7}}, "raydog_head"),
        ({"raydog_max_workers": {"small": "many"}}, "raydog_max_workers"),
        ({"raydog_head_resources": {"CPU": "four"}}, "raydog_head_resources"),
        ({"raydog_head_labels": {"zone": 1}}, "raydog_head_labels"),
    ],
)
def test_mistakes_name_the_key(overrides: dict[str, object], names: str) -> None:
    section = provider(**overrides)
    section = {k: v for k, v in section.items() if v is not None}
    with pytest.raises(ConfigError, match=names):
        cfg.parse_provider_config(section, "demo")


@pytest.mark.parametrize("name", ["Not_Valid", "a" * 25, ""])
def test_the_cluster_name_is_checked(name: str) -> None:
    with pytest.raises(ConfigError):
        cfg.parse_provider_config(provider(), name)


def test_keys_ray_adds_are_accepted() -> None:
    cfg.parse_provider_config(
        provider(_config_cache_path="/tmp/x", _anything=1), "demo"
    )


def test_a_head_copy_is_recognised() -> None:
    stamped = {"work_requirement_id": "ydid:workreq:1", "credentials_file": "~/c"}
    parsed = cfg.parse_provider_config(
        provider(raydog_head=stamped, raydog_max_workers={"small": 3}), "demo"
    )
    assert parsed.head == cfg.HeadInfo("ydid:workreq:1", "~/c")
    assert dict(parsed.max_workers) == {"small": 3}


# --- node_config ------------------------------------------------------------------


def test_node_config() -> None:
    spec = cfg.parse_node_config(
        "small",
        {
            "compute_requirement_template": "ns/worker",
            "images_id": "ami-1",
            "userdata": ["#!/bin/bash", "echo two"],
            "instance_tags": {"team": "research"},
            "metrics_enabled": True,
        },
    )
    assert (
        spec.compute_requirement_template == "ns/worker" and spec.images_id == "ami-1"
    )
    assert spec.userdata == "#!/bin/bash\necho two" and spec.metrics_enabled is True
    assert dict(spec.instance_tags) == {"team": "research"} and spec.start_script == ""
    minimal = cfg.parse_node_config("x", {"compute_requirement_template": "t"})
    assert minimal.userdata is None and minimal.images_id is None


@pytest.mark.parametrize(
    ("node_config", "names"),
    [
        ({}, "compute_requirement_template"),
        ("not a mapping", "node_config"),
        ({"compute_requirement_template": "t", "InstanceType": "m5"}, "InstanceType"),
        ({"compute_requirement_template": "t", "userdata": 7}, "userdata"),
        ({"compute_requirement_template": "t", "userdata": ["a", 7]}, "userdata"),
        (
            {"compute_requirement_template": "t", "instance_tags": {"a": 1}},
            "instance_tags",
        ),
        (
            {"compute_requirement_template": "t", "metrics_enabled": "yes"},
            "metrics_enabled",
        ),
        ({"compute_requirement_template": "t", "images_id": 7}, "images_id"),
    ],
)
def test_node_config_mistakes_name_the_key(node_config: object, names: str) -> None:
    with pytest.raises(ConfigError, match=f"small.*{names}"):
        cfg.parse_node_config("small", node_config)


# --- bootstrap --------------------------------------------------------------------


def test_bootstrap_checks_and_completes_a_config(tmp_path: Path) -> None:
    original = cluster_config()
    before = copy.deepcopy(original)
    result = cfg.bootstrap(original, [tmp_path])
    assert original == before, "the input is not modified"
    assert result["provider"]["disable_node_updaters"] is True
    # big.gpu has no max_workers of its own: the cluster-wide figure applies
    assert result["provider"]["raydog_max_workers"] == {"small": 3, "big.gpu": 10}
    # What the head node type declares, for the provider to give the head node
    assert result["provider"]["raydog_head_resources"] == {"CPU": 0}
    assert result["provider"]["raydog_head_labels"] == {}
    parsed = cfg.parse_provider_config(result["provider"], "demo")
    assert parsed.head_resources == {"CPU": 0} and parsed.head_labels == {}
    local, remote = cfg.credentials_paths("ns", "demo")
    assert result["file_mounts"] == {remote: str(local)}
    assert remote == "~/.raydog/credentials/ns.demo"
    assert cfg.bootstrap(result, [tmp_path]) == result, (
        "running it again changes nothing"
    )


def test_bootstrap_inlines_files(tmp_path: Path) -> None:
    here, there = tmp_path / "here", tmp_path / "there"
    (here / "scripts").mkdir(parents=True)
    there.mkdir()
    (here / "scripts" / "head.sh").write_text("#!/bin/bash\nray start --head\n")
    (there / "worker.sh").write_text("ray start\n")
    (there / "setup.sh").write_text("apt-get install x\n")
    (there / "extra.sh").write_text("echo extra\n")
    config = cluster_config(
        head_start_script="file:scripts/head.sh",
        worker_start_script=f"file: {there / 'worker.sh'}",
    )
    config["available_node_types"]["small"]["node_config"]["userdata"] = [
        "file:setup.sh",
        "file:extra.sh",
        "echo inline",
    ]
    config["available_node_types"]["head"]["node_config"]["userdata"] = "file:setup.sh"
    result = cfg.bootstrap(config, [here, there])
    assert result["provider"]["head_start_script"] == "#!/bin/bash\nray start --head\n"
    assert result["provider"]["worker_start_script"] == "ray start\n"
    types = result["available_node_types"]
    assert types["small"]["node_config"]["userdata"] == [
        "apt-get install x\n",
        "echo extra\n",
        "echo inline",
    ]
    assert types["head"]["node_config"]["userdata"] == "apt-get install x\n"
    assert "file:" not in str(result["provider"]) + str(types)


def test_a_missing_file_names_the_path_and_where_it_looked(tmp_path: Path) -> None:
    config = cluster_config(head_start_script="file:nope.sh")
    with pytest.raises(ConfigError) as caught:
        cfg.bootstrap(config, [tmp_path / "a", tmp_path / "b"])
    message = str(caught.value)
    assert "head_start_script" in message and "nope.sh" in message
    assert str(tmp_path / "a") in message and str(tmp_path / "b") in message
    with pytest.raises(ConfigError, match="not followed by a path"):
        cfg.bootstrap(cluster_config(head_start_script="file:  "), [tmp_path])


def test_ray_up_does_not_warn_that_ray_will_not_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    ``ray up`` warns that Ray will not start, and that the head will launch no
    workers, unless a head start command mentions ``ray start`` and
    ``--autoscaling-config``. RayDog starts Ray from the head's task, so it puts in
    a command that says so, and does nothing else.
    """
    from ray.autoscaler._private import commands
    from ray.autoscaler._private.cli_logger import cli_logger

    warnings: list[str] = []
    monkeypatch.setattr(
        cli_logger, "warning", lambda msg, *a, **k: warnings.append(msg)
    )
    commands.warn_about_bad_start_command([])
    assert len(warnings) == 2, "Ray's own check, as it stands, warns about []"
    warnings.clear()

    result = cfg.bootstrap(cluster_config(), [tmp_path])
    commands.warn_about_bad_start_command(result["head_start_ray_commands"])
    assert warnings == []
    [command] = result["head_start_ray_commands"]
    ran = subprocess.run(["bash", "-c", command], capture_output=True, text=True)
    assert ran.returncode == 0 and ran.stdout.startswith("raydog: "), ran
    assert ran.stderr == ""


@pytest.mark.parametrize("key", cfg.COMMAND_LISTS)
def test_rays_command_lists_must_be_empty(key: str, tmp_path: Path) -> None:
    config = cluster_config()
    config[key] = ["pip install something"]
    with pytest.raises(ConfigError, match=key):
        cfg.bootstrap(config, [tmp_path])
    config[key] = None  # absent or null is fine
    cfg.bootstrap(config, [tmp_path])


@pytest.mark.parametrize(
    ("change", "names"),
    [
        (lambda c: c.update(cluster_name="Not_Valid"), "cluster name"),
        (lambda c: c.update(cluster_name=7), "cluster_name"),
        (lambda c: c.update(provider="x"), "provider"),
        (lambda c: c.update(available_node_types={}), "available_node_types"),
        (lambda c: c.update(head_node_type="nope"), "head_node_type"),
        (lambda c: c["provider"].update(namspace="typo"), "namspace"),
        (lambda c: c["available_node_types"]["small"].update(node_config={}), "small"),
        (lambda c: c.update(file_mounts="x"), "file_mounts"),
        (
            lambda c: c["available_node_types"].update(
                {"a.b": {"node_config": {"compute_requirement_template": "t"}},
                 "a_b": {"node_config": {"compute_requirement_template": "t"}}}
            ),
            None,
        ),
    ],
)  # fmt: skip
def test_bootstrap_rejects_mistakes(
    change: Any, names: str | None, tmp_path: Path
) -> None:
    config = cluster_config()
    change(config)
    if names is None:
        cfg.bootstrap(config, [tmp_path])  # distinct names give distinct roles
        return
    with pytest.raises(ConfigError, match=names):
        cfg.bootstrap(config, [tmp_path])


def test_existing_file_mounts_are_kept(tmp_path: Path) -> None:
    config = cluster_config()
    config["file_mounts"] = {"/remote/data": "/local/data"}
    result = cfg.bootstrap(config, [tmp_path])
    assert result["file_mounts"]["/remote/data"] == "/local/data"
    assert len(result["file_mounts"]) == 2
    config["file_mounts"] = None
    assert len(cfg.bootstrap(config, [tmp_path])["file_mounts"]) == 1


def test_search_dirs_include_the_directory_of_the_yaml_on_the_command_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    yaml_dir = tmp_path / "configs"
    yaml_dir.mkdir()
    (yaml_dir / "cluster.yaml").write_text("x")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    dirs = cfg.default_search_dirs(
        ["/usr/bin/ray", "up", "-y", str(yaml_dir / "cluster.yaml"), "--no-such-file"]
    )
    assert dirs == [yaml_dir.resolve(), elsewhere.resolve()]


# --- added after the phase 5 review ------------------------------------------------


def test_node_updaters_cannot_be_turned_on() -> None:
    """With them on, Ray would copy the credentials file to every worker."""
    with pytest.raises(ConfigError, match="disable_node_updaters"):
        cfg.parse_provider_config(provider(disable_node_updaters=False), "demo")
    cfg.parse_provider_config(provider(disable_node_updaters=True), "demo")


@pytest.mark.parametrize(
    "key",
    ["worker_liveness_check", "use_external_head_ip", "disable_launch_config_check",
     "foreground_node_launch", "use_internal_ips"],
)  # fmt: skip
def test_rays_own_provider_keys_pass_through(key: str) -> None:
    cfg.parse_provider_config(provider(**{key: True}), "demo")


@pytest.mark.parametrize("value", [float("inf"), float("nan"), -float("inf")])
@pytest.mark.parametrize("key", ["lifetime_hours", "build_timeout_minutes"])
def test_numbers_must_be_finite(key: str, value: float) -> None:
    with pytest.raises(ConfigError, match=key):
        cfg.parse_provider_config(provider(**{key: value}), "demo")


@pytest.mark.parametrize("namespace", ["a/b", "../x", "a\\b", ".", ".."])
def test_a_namespace_that_is_not_a_plain_name_is_refused(namespace: str) -> None:
    with pytest.raises(ConfigError, match="namespace"):
        cfg.parse_provider_config(provider(namespace=namespace), "demo")


def test_the_yaml_directory_is_searched_before_the_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    yaml_dir, elsewhere = tmp_path / "configs", tmp_path / "elsewhere"
    for directory, text in ((yaml_dir, "the right script"), (elsewhere, "a stranger")):
        (directory / "scripts").mkdir(parents=True)
        (directory / "scripts" / "head.sh").write_text(text)
    (yaml_dir / "cluster.yaml").write_text("x")
    monkeypatch.chdir(elsewhere)
    dirs = cfg.default_search_dirs(["ray", "up", str(yaml_dir / "cluster.yaml")])
    assert dirs[0] == yaml_dir.resolve() and elsewhere.resolve() in dirs
    result = cfg.bootstrap(
        cluster_config(head_start_script="file:scripts/head.sh"), dirs
    )
    assert result["provider"]["head_start_script"] == "the right script"


def test_autoscaler_v2_is_on_unless_turned_off() -> None:
    assert cfg.parse_provider_config(provider(), "demo").autoscaler_v2 is True
    off = cfg.parse_provider_config(provider(autoscaler_v2=False), "demo")
    assert off.autoscaler_v2 is False
    with pytest.raises(ConfigError, match="autoscaler_v2"):
        cfg.parse_provider_config(provider(autoscaler_v2="yes"), "demo")
