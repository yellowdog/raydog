"""
What every script that prepares or runs a node must get right, across the examples
and the Packer image: they are copied into users' own clusters, so they are held to
the same rules wherever they live.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from .test_autoscaler_examples import login_shell

REPO = Path(__file__).resolve().parents[2]
SETUP_SCRIPTS = [
    REPO / "examples" / "autoscaler" / "scripts" / "node-setup.sh",
    REPO / "examples" / "autoscaler" / "scripts" / "node-setup-docker.sh",
    REPO / "examples" / "builder" / "scripts" / "node-setup-userdata.sh",
]
PROVISION = REPO / "packer" / "scripts" / "provision.sh"
ALLOY_CONFIG = REPO / "examples" / "builder" / "scripts" / "config.alloy"
ALL_SCRIPTS = sorted(
    p
    for p in [*REPO.glob("examples/**/*.sh"), *REPO.glob("packer/**/*.sh")]
    if ".rendered" not in p.parts
)


@pytest.mark.parametrize("script", ALL_SCRIPTS, ids=lambda p: str(p.relative_to(REPO)))
def test_every_script_parses(script: Path) -> None:
    result = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("script", ALL_SCRIPTS, ids=lambda p: str(p.relative_to(REPO)))
def test_no_script_gives_the_agents_user_root(script: Path) -> None:
    """
    Ray jobs run as the Agent's user, so passwordless sudo would give every job
    root. Nothing the examples or the image do needs it.
    """
    text = script.read_text()
    assert "NOPASSWD" not in text and "sudoers" not in text and "sudo " not in text


@pytest.mark.parametrize("script", SETUP_SCRIPTS, ids=lambda p: p.name)
def test_a_setup_script_says_where_it_failed(script: Path) -> None:
    assert "' ERR" in script.read_text()


BOOT_SCRIPTS = [
    *SETUP_SCRIPTS,
    *sorted((REPO / "examples" / "builder" / "scripts" / "observability").glob("*.sh")),
]


@pytest.mark.parametrize("script", BOOT_SCRIPTS, ids=lambda p: p.name)
def test_packages_are_installed_at_boot_with_retries(script: Path) -> None:
    """
    A package repository can fail for a while, and a node whose setup fails at
    boot never starts: every install goes through ``apt_install``, which retries.
    """
    text = script.read_text()
    installs = text.count("apt-get install") + text.count("apt-get -y install")
    defines = "apt_install() {" in text
    assert installs == (1 if defines else 0), "install with apt_install"


@pytest.mark.parametrize("script", [*SETUP_SCRIPTS, PROVISION], ids=lambda p: p.name)
def test_ray_is_on_the_path_before_ssh_can_get_in(script: Path) -> None:
    """
    ``ray up`` connects as soon as the key is authorised, and runs Ray's commands
    in a shell that reads ``.bashrc``: that file must be in place first.
    """
    text = script.read_text()
    key = '>> "$YD_AGENT_HOME/.ssh/authorized_keys"'
    assert text.index("<< 'BASHRC'") < text.index(key)


def test_ray_stop_in_a_login_shell_leaves_an_image_node_running(
    tmp_path: Path,
) -> None:
    """
    An image built by the Packer template is used without the setup scripts, so it
    must protect the node's task from ``ray stop`` (with which ``ray down`` begins)
    just as they do.
    """
    assert login_shell(PROVISION, tmp_path, "ray", "ray status").strip() == (
        "REAL ray status"
    )
    out = login_shell(PROVISION, tmp_path, "ray", "ray stop")
    assert "REAL" not in out and "task" in out


def test_the_alloy_configuration_is_written_down_once() -> None:
    """The Builder's setup script and the image install the same file."""
    assert "prometheus.remote_write" in ALLOY_CONFIG.read_text()
    for path in (
        PROVISION,
        REPO / "examples" / "builder" / "scripts" / "node-setup-userdata.sh",
    ):
        assert "prometheus.remote_write" not in path.read_text(), path.name
    template = (REPO / "packer" / "raydog-ami.pkr.hcl").read_text()
    assert "examples/builder/scripts/config.alloy" in template


@pytest.mark.parametrize(
    ("missing", "says"),
    [
        ("ALLOY_CONFIG", "Alloy's configuration is not at"),
        ("PYTHON_VERSION", "PYTHON_VERSION must be set"),
    ],
)
def test_the_image_script_checks_its_inputs_before_doing_anything(
    tmp_path: Path, missing: str, says: str
) -> None:
    """
    Run any other way than by the Packer template, the image script would otherwise
    install for several minutes before finding Alloy's configuration missing.
    """
    env = {
        "PATH": "/usr/bin:/bin",
        "PYTHON_VERSION": "3.12.11",
        "RAY_VERSION": "2.60.0",
        "ALLOY_CONFIG": str(ALLOY_CONFIG),
    }
    if missing == "ALLOY_CONFIG":
        env[missing] = str(tmp_path / "absent")
    else:
        del env[missing]
    result = subprocess.run(
        ["bash", str(PROVISION)], env=env, capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 1
    assert says in result.stderr
    assert result.stdout == "", "nothing was started"


def test_the_alloy_configuration_reads_only_what_its_user_can() -> None:
    """
    On the head and workers Alloy runs as the Agent's user, which cannot read the
    systemd journal.
    """
    assert "loki.source.journal" not in ALLOY_CONFIG.read_text()


def test_the_container_setup_gives_the_agent_docker_before_it_starts() -> None:
    """
    Restarting the Agent after it has started would kill a task it had already
    taken; its service is given the docker group before it is installed instead.
    """
    text = SETUP_SCRIPTS[1].read_text()
    assert "systemctl restart" not in text
    assert text.index("SupplementaryGroups=docker") < text.index("yd-agent-installer")
