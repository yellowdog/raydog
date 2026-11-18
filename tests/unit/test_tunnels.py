"""RayTunnels, with a stand-in for the ssh program."""

from __future__ import annotations

import os
import socket
import stat
import sys
import time
from pathlib import Path

import pytest

from yellowdog_ray import PortForward, RayTunnels
from yellowdog_ray.errors import TunnelError

# A program that behaves like `ssh -N -L ...` as far as RayTunnels can tell: it
# listens on each forwarded local port until it is terminated. The mode is taken
# from the "host" it is asked to connect to.
STUB = """#!{python}
import socket, sys, time
args = sys.argv[1:]
open(sys.argv[0] + ".args", "w").write("\\n".join(args))
target = args[-1]
forwards = [args[i + 1] for i, a in enumerate(args) if a == "-L"]
if "refused" in target:
    sys.stderr.write("ssh: connect to host refused port 22: Connection refused\\n")
    sys.exit(255)
if "hang" in target:
    time.sleep(600)
listeners = []
for forward in forwards:
    host, port, _, _ = forward.split(":")
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, int(port)))
    s.listen()
    listeners.append(s)
if "dies" in target:
    time.sleep(0.5)
    sys.exit(1)
while True:
    time.sleep(1)
"""


@pytest.fixture
def ssh(tmp_path: Path) -> str:
    path = tmp_path / "fake-ssh"
    path.write_text(STUB.format(python=sys.executable))
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return str(path)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return int(s.getsockname()[1])


def listening(port: int) -> bool:
    try:
        with socket.create_connection(("localhost", port), 0.3):
            return True
    except OSError:
        return False


def tunnels(
    ssh: str, host: str = "10.1.2.3", **kw: object
) -> tuple[RayTunnels, int, int]:
    a, b = free_port(), free_port()
    t = RayTunnels(
        host,
        "yd-agent",
        forwards=(PortForward(a, 10001), PortForward(b, 8265)),
        ssh_command=ssh,
        **kw,  # type: ignore[arg-type]
    )
    return t, a, b


def test_the_command_line() -> None:
    t = RayTunnels(
        "1.2.3.4", "yd-agent", private_key="~/.ssh/key", ssh_options=["Port=2222"]
    )
    command = t.command()
    assert command[:2] == ["ssh", "-N"] and command[-1] == "yd-agent@1.2.3.4"
    assert command[-2] == "--"
    options = [command[i + 1] for i, a in enumerate(command) if a == "-o"]
    assert options[0] == "Port=2222", "the caller's options come first, so they win"
    for wanted in ("ExitOnForwardFailure=yes", "BatchMode=yes", "IdentitiesOnly=yes"):
        assert wanted in options
    assert command[command.index("-i") + 1] == os.path.expanduser("~/.ssh/key")
    forwards = [command[i + 1] for i, a in enumerate(command) if a == "-L"]
    assert forwards == [
        "localhost:10001:localhost:10001",
        "localhost:8265:localhost:8265",
    ]
    assert "-i" not in RayTunnels("h", "u").command()


def test_start_returns_when_every_port_is_open_and_stop_closes_them(ssh: str) -> None:
    t, a, b = tunnels(ssh)
    assert not t.is_running
    t.start()
    assert t.is_running and listening(a) and listening(b)
    passed = Path(ssh + ".args").read_text().splitlines()
    assert passed[-1] == "yd-agent@10.1.2.3"
    assert f"localhost:{a}:localhost:10001" in passed
    t.stop()
    assert not t.is_running
    time.sleep(0.2)
    assert not listening(a) and not listening(b)
    t.stop()  # repeatable
    t.start()  # and it can be opened again
    assert listening(a)
    t.stop()


def test_starting_twice_is_an_error(ssh: str) -> None:
    t, _, _ = tunnels(ssh)
    with t:
        with pytest.raises(TunnelError, match="already"):
            t.start()
        assert t.is_running


def test_a_failing_ssh_reports_its_own_message(ssh: str) -> None:
    t, a, _ = tunnels(ssh, host="refused-host")
    with pytest.raises(TunnelError, match="Connection refused"):
        t.start()
    assert not t.is_running and not listening(a)
    t.stop()


def test_an_ssh_that_never_connects_times_out_and_is_killed(ssh: str) -> None:
    t, a, _ = tunnels(ssh, host="hang-host", connect_timeout=1.0)
    started = time.monotonic()
    with pytest.raises(TunnelError, match="within 1 seconds"):
        t.start()
    assert time.monotonic() - started < 10
    assert not t.is_running and not listening(a)


def test_an_ssh_that_is_missing_is_reported(tmp_path: Path) -> None:
    t = RayTunnels(
        "h",
        "u",
        forwards=(PortForward(free_port(), 8265),),  # not the defaults: may be in use
        ssh_command=str(tmp_path / "no-such-ssh"),
    )
    with pytest.raises(TunnelError, match="could not run"):
        t.start()
    t.stop()


def test_the_context_manager_closes_on_the_way_out(ssh: str) -> None:
    t, a, _ = tunnels(ssh)
    with pytest.raises(RuntimeError), t:
        assert listening(a)
        raise RuntimeError("user code")
    time.sleep(0.2)
    assert not t.is_running and not listening(a)


def test_is_running_notices_a_tunnel_that_died(ssh: str) -> None:
    t, _, _ = tunnels(ssh, host="dies-later")
    t.start()
    time.sleep(1.5)
    assert not t.is_running
    t.stop()


@pytest.mark.parametrize("port", [0, -1, 70000, "80", True])
def test_port_numbers_are_checked(port: object) -> None:
    with pytest.raises(ValueError):
        PortForward(port, 8265)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        PortForward(8265, port)  # type: ignore[arg-type]


def test_arguments_are_checked() -> None:
    with pytest.raises(ValueError):
        RayTunnels("", "u")
    with pytest.raises(ValueError):
        RayTunnels("h", "u", forwards=())
    assert PortForward(8265, 8265) == RayTunnels.DASHBOARD
    assert PortForward(10001, 10001) == RayTunnels.CLIENT


def test_a_local_port_already_in_use_is_an_error_before_ssh_runs(ssh: str) -> None:
    """Otherwise the port's other owner would be taken for the tunnel."""
    t, a, _ = tunnels(ssh)
    with socket.socket() as other:
        other.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        other.bind(("localhost", a))
        other.listen()
        with pytest.raises(TunnelError, match=f"port {a} is already in use"):
            t.start()
        assert not t.is_running
        assert not Path(ssh + ".args").exists(), "ssh was never run"
    t.start()  # once it is free, the same object can be started
    t.stop()


def test_the_host_cannot_be_taken_for_an_option() -> None:
    command = RayTunnels("1.2.3.4", "yd-agent").command()
    assert command[-2:] == ["--", "yd-agent@1.2.3.4"]
    with pytest.raises(ValueError, match="host"):
        RayTunnels("-oProxyCommand=evil", "yd-agent")
    with pytest.raises(ValueError, match="user"):
        RayTunnels("1.2.3.4", "-evil")
