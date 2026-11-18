"""The raydog command, against the in-memory platform."""

from __future__ import annotations

import contextlib
import json
import socket
import stat
import sys
from datetime import timedelta
from pathlib import Path

import pytest

from tests.unit.test_tunnels import STUB
from yellowdog_ray import ClusterSpec, NodeSpec, RayDogCluster, __version__
from yellowdog_ray.cli import main
from yellowdog_ray.core.platform import PoolSpec, TaskGroupSpec
from yellowdog_ray.errors import PlatformError
from yellowdog_ray.testing import FakePlatform

NS = "ns"
NODE = NodeSpec(compute_requirement_template="ns/t", start_script="ray start")


@pytest.fixture
def p() -> FakePlatform:
    return FakePlatform()


@pytest.fixture(autouse=True)
def no_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RAYDOG_NAMESPACE", raising=False)


def run(p: FakePlatform, *argv: str) -> int:
    return main(list(argv), platform=p, clock=p.clock)


def build(
    p: FakePlatform, name: str = "demo", workers: int = 2, **spec: object
) -> RayDogCluster:
    cluster = RayDogCluster(
        ClusterSpec(name=name, namespace=NS, head=NODE, **spec),  # type: ignore[arg-type]
        platform=p,
        clock=p.clock,
    )
    if workers:
        cluster.add_worker_pool("small", NODE, count=workers, max_count=4)
    cluster.build()
    p.clock.advance(300)
    return cluster


# --- general ----------------------------------------------------------------------


def test_version_and_help(p: FakePlatform, capsys: pytest.CaptureFixture[str]) -> None:
    assert run(p, "--version") == 0
    assert capsys.readouterr().out.strip() == f"raydog {__version__}"
    assert run(p) == 2
    assert "COMMAND" in capsys.readouterr().out
    assert run(p, "no-such-command") == 2
    assert p.calls == []


def test_a_namespace_is_required(
    p: FakePlatform, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    assert run(p, "list") == 2
    assert "RAYDOG_NAMESPACE" in capsys.readouterr().err and p.calls == []
    monkeypatch.setenv("RAYDOG_NAMESPACE", NS)
    assert run(p, "list") == 0
    assert f"No clusters in namespace '{NS}'" in capsys.readouterr().out


def test_missing_credentials_are_a_usage_error(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("YD_API_KEY_ID", raising=False)
    monkeypatch.delenv("YD_API_KEY_SECRET", raising=False)
    assert main(["list", "-n", NS]) == 2
    assert "YD_API_KEY_ID" in capsys.readouterr().err


def test_a_platform_failure_is_reported_without_a_traceback(
    p: FakePlatform, capsys: pytest.CaptureFixture[str]
) -> None:
    p.fail_next("list_work_requirements", PlatformError("the platform is unwell"))
    assert run(p, "list", "-n", NS) == 1
    err = capsys.readouterr().err
    assert "the platform is unwell" in err and "Traceback" not in err


# --- list -------------------------------------------------------------------------


def test_list(p: FakePlatform, capsys: pytest.CaptureFixture[str]) -> None:
    build(p)
    dead = build(p, name="other", workers=0)
    assert dead.head is not None
    p.fail_task(dead.head.task_id)
    ended = build(p, name="gone", workers=0)
    ended.shut_down(wait=True)
    p.create_work_requirement(
        NS, "plain", None, [TaskGroupSpec(name="g", worker_tag="x")]
    )
    p.clock.advance(3600)

    assert run(p, "list", "-n", NS) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].split() == ["NAME", "STATE", "AGE", "NODES", "INSTANCES"]
    table = lines[1 : lines.index("")] if "" in lines else lines[1:]
    rows = {line.split()[0]: line.split() for line in table}
    assert set(rows) == {"demo", "other"}
    assert rows["demo"][1] == "running" and rows["demo"][-2:] == ["3", "3"]
    assert " ".join(rows["other"][1:3]) == "no head"

    assert run(p, "list", "-n", NS, "--all", "--json") == 0
    data = {c["name"]: c for c in json.loads(capsys.readouterr().out)}
    assert set(data) == {"demo", "other", "gone"}
    assert data["gone"]["state"] == "cancelled" and data["gone"]["instances"] == 0
    assert data["demo"]["nodes"] == 3 and data["demo"]["work_requirement"].startswith(
        "demo_"
    )


def test_list_shows_starting_and_expired(
    p: FakePlatform, capsys: pytest.CaptureFixture[str]
) -> None:
    p.never_register("slow")
    slow = RayDogCluster(
        ClusterSpec(name="slow", namespace=NS, head=NODE), platform=p, clock=p.clock
    )
    core_handle = slow._ensure_core().create(slow._spec.settings())  # type: ignore[union-attr]
    slow._ensure_core().add_pool(core_handle, "head", NODE, 1, 1)
    slow._ensure_core().add_nodes(core_handle, "head", 1, "x")
    run(p, "list", "-n", NS, "--json")
    assert json.loads(capsys.readouterr().out)[0]["state"] == "starting"
    p.clock.advance(3600)
    run(p, "list", "-n", NS, "--json")
    assert json.loads(capsys.readouterr().out)[0]["state"] == "no head"


def test_list_says_how_to_remove_clusters_that_no_longer_work(
    p: FakePlatform, capsys: pytest.CaptureFixture[str]
) -> None:
    """A dead cluster costs nothing but clutters: list says what removes it."""
    build(p, workers=0)
    run(p, "list", "-n", NS)
    assert "raydog down" not in capsys.readouterr().out  # nothing to remove
    dead = build(p, name="other", workers=0)
    assert dead.head is not None
    p.fail_task(dead.head.task_id)
    run(p, "list", "-n", NS)
    out = capsys.readouterr().out
    assert "raydog down <name>" in out and "no head" in out
    run(p, "list", "-n", NS, "--json")
    json.loads(capsys.readouterr().out)  # the hint is not mixed into JSON


# --- status -----------------------------------------------------------------------


def test_status(p: FakePlatform, capsys: pytest.CaptureFixture[str]) -> None:
    cluster = build(p, lifetime=timedelta(hours=2))
    assert cluster.head is not None
    assert run(p, "status", "demo", "-n", NS) == 0
    out = capsys.readouterr().out
    assert "Cluster demo in namespace ns: running" in out
    assert f"private {cluster.head.private_ip}" in out
    assert f"public {cluster.head.public_ip}" in out
    assert "left)" in out and "w-small" in out and "2 executing" in out

    assert run(p, "status", "demo", "-n", NS, "--json") == 0
    data = json.loads(capsys.readouterr().out)
    assert data["state"] == "running" and data["head"]["status"] == "EXECUTING"
    assert data["head"]["private_ip"] == cluster.head.private_ip
    assert data["pools"]["w-small"] == {
        "status": "RUNNING",
        "nodes": {"executing": 2},
        "instances": 2,
        "max_nodes": 4,
    }
    assert data["deadline"] is not None


def test_status_of_odd_and_ended_clusters(
    p: FakePlatform, capsys: pytest.CaptureFixture[str]
) -> None:
    cluster = build(p, workers=0, lifetime=None)
    assert cluster.head is not None
    p.fail_task(cluster.head.task_id)
    assert run(p, "status", "demo", "-n", NS) == 0
    out = capsys.readouterr().out
    assert "no head" in out and "FAILED" in out and "no limit" in out
    assert "raydog down demo -n ns" in out  # how to remove it
    cluster.shut_down(wait=True)
    assert run(p, "status", "demo", "-n", NS) == 0  # still findable once ended
    assert "cancelled" in capsys.readouterr().out
    assert run(p, "status", "nope", "-n", NS) == 3
    assert "no cluster named 'nope'" in capsys.readouterr().err


# --- down -------------------------------------------------------------------------


def test_down(p: FakePlatform, capsys: pytest.CaptureFixture[str]) -> None:
    build(p)
    build(p, name="demo2", workers=1)
    assert run(p, "down", "demo", "-n", NS, "--yes", "--wait") == 0
    out = capsys.readouterr().out
    assert "3 node(s), 3 instance(s)" in out and "no instances remain" in out
    assert p.live_instances == 2, "the similarly named cluster is untouched"
    assert run(p, "down", "demo", "-n", NS, "--yes") == 3
    assert run(p, "down", "demo2", "-n", NS, "--yes") == 0
    assert "being removed" in capsys.readouterr().out
    p.clock.advance(120)
    assert p.live_instances == 0


def test_down_dry_run_and_confirmation(
    p: FakePlatform, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    build(p)
    assert run(p, "down", "demo", "-n", NS, "--dry-run") == 0
    assert "Dry run: nothing removed" in capsys.readouterr().out
    monkeypatch.setattr("builtins.input", lambda prompt: "n")
    assert run(p, "down", "demo", "-n", NS) == 1
    assert "nothing removed" in capsys.readouterr().err
    monkeypatch.setattr(
        "builtins.input", lambda prompt: (_ for _ in ()).throw(EOFError)
    )
    assert run(p, "down", "demo", "-n", NS) == 1
    p.clock.advance(120)
    assert p.live_instances == 3
    monkeypatch.setattr("builtins.input", lambda prompt: "y")
    assert run(p, "down", "demo", "-n", NS, "--wait") == 0
    assert p.live_instances == 0


def test_down_removes_what_an_ended_cluster_left_behind(
    p: FakePlatform, capsys: pytest.CaptureFixture[str]
) -> None:
    cluster = build(p)
    assert cluster.handle is not None
    p.cancel_work_requirement(cluster.handle.work_requirement_id)  # ended elsewhere
    p.clock.advance(60)
    assert p.live_instances == 3
    assert run(p, "down", "demo", "-n", NS, "--yes", "--wait") == 0
    assert "Left over: pool" in capsys.readouterr().out
    assert p.live_instances == 0


def test_down_reports_an_incomplete_teardown_and_can_be_repeated(
    p: FakePlatform, capsys: pytest.CaptureFixture[str]
) -> None:
    build(p)
    p.fail_next("remove_pool", PlatformError("boom"))
    assert run(p, "down", "demo", "-n", NS, "--yes") == 4
    err = capsys.readouterr().err
    assert "boom" in err and "may still be running" in err and "Traceback" not in err
    assert run(p, "down", "demo", "-n", NS, "--yes", "--wait") == 0
    assert p.live_instances == 0


def test_interrupting_down_says_so(
    p: FakePlatform, capsys: pytest.CaptureFixture[str]
) -> None:
    build(p)
    p.fail_next("live_instance_count", KeyboardInterrupt())  # type: ignore[arg-type]
    assert run(p, "down", "demo", "-n", NS, "--yes", "--wait") == 130
    assert "may be incomplete" in capsys.readouterr().err
    assert run(p, "down", "demo", "-n", NS, "--yes", "--wait") in (0, 3)
    p.clock.advance(120)
    assert p.live_instances == 0


# --- cleanup ----------------------------------------------------------------------


def test_cleanup(p: FakePlatform, capsys: pytest.CaptureFixture[str]) -> None:
    build(p, name="healthy", workers=1)
    broken = build(p, name="broken", workers=1)
    assert broken.handle is not None
    p.cancel_work_requirement(broken.handle.work_requirement_id)
    foreign = PoolSpec(
        name="broken_aaaaaaaa_h", namespace=NS, template="t", worker_tag="x",
        target_instances=1, max_nodes=1, idle_node_shutdown=timedelta(hours=9),
        idle_pool_shutdown=None,
    )  # fmt: skip
    p.provision_pool(foreign)  # looks like one of ours, but carries no marker
    p.clock.advance(60)

    assert run(p, "cleanup", "-n", NS) == 0
    out = capsys.readouterr().out
    assert out.count("cluster 'broken'") == 2 and "healthy" not in out
    assert "Run again with --delete" in out
    assert p.live_instances == 5, "looking removes nothing"

    assert run(p, "cleanup", "-n", NS, "--json") == 0
    found = json.loads(capsys.readouterr().out)
    assert {o["kind"] for o in found} == {"pool"} and len(found) == 2

    assert run(p, "cleanup", "-n", NS, "--delete", "--wait") == 0
    assert "Removed 2 item(s)" in capsys.readouterr().out
    assert p.live_instances == 3, "the healthy cluster and the foreign pool remain"
    assert run(p, "cleanup", "-n", NS, "--delete") == 0
    assert "Nothing left over" in capsys.readouterr().out


def test_cleanup_that_cannot_finish_says_so(
    p: FakePlatform, capsys: pytest.CaptureFixture[str]
) -> None:
    broken = build(p, name="broken", workers=0)
    assert broken.handle is not None
    p.cancel_work_requirement(broken.handle.work_requirement_id)
    p.clock.advance(60)
    p.fail_next("remove_pool", PlatformError("boom"))
    assert run(p, "cleanup", "-n", NS, "--delete") == 4
    assert "may still be running" in capsys.readouterr().err
    assert run(p, "cleanup", "-n", NS, "--delete", "--wait") == 0
    assert p.live_instances == 0


# --- tunnel -----------------------------------------------------------------------


@pytest.fixture
def ssh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "fake-ssh"
    path.write_text(STUB.format(python=sys.executable))
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("RAYDOG_SSH", str(path))
    return path


def test_tunnel(p: FakePlatform, ssh: Path, capsys: pytest.CaptureFixture[str]) -> None:
    cluster = build(p, workers=0)
    assert cluster.head is not None
    code = run(
        p, "tunnel", "demo", "-n", NS, "-i", "/keys/id", "-u", "ubuntu",
        "-p", "18265:8265", "-p", "18264:10001", "-p", "18999", "--for", "0.3",
    )  # fmt: skip
    assert code == 0
    lines = capsys.readouterr().out.splitlines()
    # Ray's own ports are said to be what they are, with the address to use
    assert lines[0] == (
        "localhost:18265 -> demo head node, port 8265"
        "  (dashboard and job server: http://localhost:18265)"
    )
    assert lines[1] == (
        "localhost:18264 -> demo head node, port 10001"
        '  (Ray client: ray.init("ray://localhost:18264"))'
    )
    assert lines[2] == "localhost:18999 -> demo head node, port 18999"
    passed = Path(f"{ssh}.args").read_text().splitlines()
    assert passed[-1] == f"ubuntu@{cluster.head.public_ip}"
    assert "/keys/id" in passed and "localhost:18265:localhost:8265" in passed

    run(p, "tunnel", "demo", "-n", NS, "--internal", "-p", "18265:8265", "--for", "0.2")
    assert Path(f"{ssh}.args").read_text().splitlines()[-1] == (
        f"yd-agent@{cluster.head.private_ip}"
    )


def test_tunnel_moves_a_default_port_that_is_taken(
    p: FakePlatform, ssh: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """
    Without -p, a default local port already in use (another tunnel, a local Ray)
    is swapped for the next free one, and the lines printed say which.
    """
    build(p, workers=0)
    with contextlib.ExitStack() as held:
        taken = socket.socket()
        held.enter_context(taken)
        with contextlib.suppress(OSError):  # already in use here: as good
            taken.bind(("localhost", 8265))
            taken.listen()
        assert run(p, "tunnel", "demo", "-n", NS, "--for", "0.3") == 0
    out = capsys.readouterr().out
    line = next(
        x for x in out.splitlines() if x.startswith("localhost:") and "port 8265" in x
    )
    local = int(line.split()[0].split(":")[1])
    assert local != 8265
    assert f"http://localhost:{local})" in line
    assert f"local port 8265 is in use; using {local}" in out
    passed = Path(f"{ssh}.args").read_text().splitlines()
    assert f"localhost:{local}:localhost:8265" in passed


def test_tunnel_keeps_an_explicit_port_and_says_how_to_choose_another(
    p: FakePlatform, ssh: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    build(p, workers=0)
    with socket.socket() as taken:
        taken.bind(("localhost", 0))
        taken.listen()
        port = taken.getsockname()[1]
        assert run(p, "tunnel", "demo", "-n", NS, "-p", f"{port}:8265") == 1
    err = capsys.readouterr().err
    assert f"local port {port} is already in use" in err
    assert "-p <free port>:8265" in err


def test_tunnel_errors(
    p: FakePlatform, ssh: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(p, "tunnel", "nope", "-n", NS) == 3
    cluster = build(p, workers=0)
    assert run(p, "tunnel", "demo", "-n", NS, "-p", "eighty") == 2
    assert "eighty" in capsys.readouterr().err
    assert cluster.head is not None
    p.fail_task(cluster.head.task_id)
    assert run(p, "tunnel", "demo", "-n", NS) == 1
    assert "not running" in capsys.readouterr().err


def test_tunnel_bad_user_is_a_usage_error(
    p: FakePlatform, ssh: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    build(p, workers=0)
    assert run(p, "tunnel", "demo", "-n", NS, "-u", "") == 2
    assert "user" in capsys.readouterr().err
    assert not Path(f"{ssh}.args").exists()


def test_tunnel_refused_key_suggests_the_identity_option(
    p: FakePlatform, ssh: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The cluster's key is not in the CLI's hands: say how to give it."""
    build(p, workers=0)
    ssh.write_text(
        "#!/bin/sh\n"
        "echo 'yd-agent@host: Permission denied (publickey).' >&2\n"
        "exit 255\n"
    )
    assert run(p, "tunnel", "demo", "-n", NS, "-p", "18267:8265") == 1
    err = capsys.readouterr().err
    assert "Permission denied (publickey)" in err
    assert "-i" in err and "private key" in err

    # With a key given, the advice would be wrong
    assert run(p, "tunnel", "demo", "-n", NS, "-p", "18267:8265", "-i", "/k") == 1
    err = capsys.readouterr().err
    assert "Permission denied (publickey)" in err and "-i" not in err


def test_tunnel_passes_ssh_options(p: FakePlatform, ssh: Path) -> None:
    build(p, workers=0)
    code = run(
        p, "tunnel", "demo", "-n", NS, "-p", "18266:8265", "--for", "0.2",
        "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=/k",
    )  # fmt: skip
    assert code == 0
    passed = Path(f"{ssh}.args").read_text().splitlines()
    options = [passed[i + 1] for i, a in enumerate(passed) if a == "-o"]
    assert options[:2] == ["StrictHostKeyChecking=yes", "UserKnownHostsFile=/k"]


@pytest.mark.parametrize("signal_name", ["SIGTERM", "SIGHUP"])
def test_tunnel_closes_ssh_when_terminated(
    p: FakePlatform, ssh: Path, signal_name: str
) -> None:
    """A supervisor's SIGTERM must not leave the ssh process behind."""
    import os
    import signal
    import socket
    import threading
    import time

    signum = getattr(signal, signal_name)
    before = signal.getsignal(signum)
    build(p, workers=0)
    timer = threading.Timer(1.0, lambda: os.kill(os.getpid(), signum))
    timer.start()
    try:
        code = run(p, "tunnel", "demo", "-n", NS, "-p", "18267:8265", "--for", "20")
    finally:
        timer.cancel()
    assert code == 0
    assert signal.getsignal(signum) is before, "the handler is put back"
    time.sleep(0.2)
    with pytest.raises(OSError):
        socket.create_connection(("localhost", 18267), 0.3).close()


def test_status_of_an_ended_cluster_does_not_count_down(
    p: FakePlatform, capsys: pytest.CaptureFixture[str]
) -> None:
    build(p)
    assert run(p, "down", "demo", "-n", NS, "--yes") == 0
    p.clock.advance(60)
    capsys.readouterr()
    assert run(p, "status", "demo", "-n", NS) == 0
    out = capsys.readouterr().out
    assert "left)" not in out and "lifetime" in out


def test_down_goes_ahead_when_the_preview_cannot_be_shown(
    p: FakePlatform,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from yellowdog_ray.core.cluster import ClusterCore

    build(p)

    def broken(self: ClusterCore, handle: object) -> object:
        raise PlatformError("status unavailable")

    monkeypatch.setattr(ClusterCore, "status", broken)
    assert run(p, "down", "demo", "-n", NS, "--yes", "--wait") == 0
    captured = capsys.readouterr()
    assert "status unavailable" in captured.err
    assert p.live_instances == 0
