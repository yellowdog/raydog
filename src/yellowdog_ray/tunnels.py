"""
SSH port forwarding to a Ray head node, using the OpenSSH client.

::

    with RayTunnels(head.public_ip, user="yd-agent", private_key="~/.ssh/id_ed25519"):
        ray.init("ray://localhost:10001")      # the Ray client port
        ...                                    # the dashboard is at localhost:8265

One ``ssh`` process carries all the forwards, so they all work or none do. Your
SSH configuration (``~/.ssh/config``: jump hosts, agents and so on) applies, except
where the options below override it.

**Host keys are not checked by default**, as with Ray's own SSH: cloud instances
are short-lived and their addresses are reused, so a remembered host key says
little. Where that matters (the tunnel carries the Ray client and dashboard
traffic), record the head node's key yourself and pass
``ssh_options=["StrictHostKeyChecking=yes", "UserKnownHostsFile=<file>"]``.

The ``ssh`` process is stopped by ``stop()`` or by leaving the ``with`` block. A
process killed outright (``SIGKILL``, or ``SIGTERM`` with no handler) cannot do
that, and leaves its ``ssh`` running.
"""

from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import tempfile
import time
from collections.abc import Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import IO

from yellowdog_ray.errors import TunnelError

# Host keys are not checked, as with Ray's own SSH (see the module docstring).
# Override through ssh_options.
_DEFAULT_OPTIONS = (
    "ExitOnForwardFailure=yes",
    "BatchMode=yes",
    "ServerAliveInterval=15",
    "StrictHostKeyChecking=no",
    "UserKnownHostsFile=/dev/null",
    "LogLevel=ERROR",
)


@dataclass(frozen=True)
class PortForward:
    """
    Forward ``local_host:local_port`` here to ``remote_host:remote_port`` as seen
    from the head node.
    """

    local_port: int
    remote_port: int
    remote_host: str = "localhost"
    local_host: str = "localhost"

    def __post_init__(self) -> None:
        for name in ("local_port", "remote_port"):
            port = getattr(self, name)
            if (
                isinstance(port, bool)
                or not isinstance(port, int)
                or not 0 < port < 65536
            ):
                raise ValueError(f"{name} must be a port number (got {port!r})")

    def argument(self) -> str:
        return (
            f"{self.local_host}:{self.local_port}:{self.remote_host}:{self.remote_port}"
        )


class RayTunnels:
    DASHBOARD = PortForward(8265, 8265)  # the Ray dashboard and job server
    CLIENT = PortForward(10001, 10001)  # the Ray client server

    def __init__(
        self,
        host: str,
        user: str,
        private_key: str | os.PathLike[str] | None = None,
        forwards: Sequence[PortForward] = (CLIENT, DASHBOARD),
        ssh_options: Sequence[str] = (),
        ssh_command: str = "ssh",
        connect_timeout: float = 30.0,
    ) -> None:
        """
        ``ssh_options`` are extra ``-o`` settings (``"Name=value"``), which take
        precedence over the defaults.
        """
        if not host or not user:
            raise ValueError("host and user are required")
        for name, value in (("host", host), ("user", user)):
            # Neither may be taken by ssh for an option, or hide a second argument
            if value.startswith("-") or any(c.isspace() for c in value):
                raise ValueError(f"{name} is not valid (got {value!r})")
        if not forwards:
            raise ValueError("at least one port forward is required")
        self._host = host
        self._user = user
        self._private_key = os.fspath(private_key) if private_key is not None else None
        self._forwards = tuple(forwards)
        self._ssh_options = tuple(ssh_options)
        self._ssh_command = ssh_command
        self._connect_timeout = connect_timeout
        self._process: subprocess.Popen[bytes] | None = None
        self._stderr: IO[bytes] | None = None

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def command(self) -> list[str]:
        """The ssh command line that ``start()`` runs."""
        command = [self._ssh_command, "-N"]
        # ssh uses the first value it is given for an option, so the caller's go first
        for option in (*self._ssh_options, *_DEFAULT_OPTIONS):
            command += ["-o", option]
        if self._private_key is not None:
            command += ["-i", os.path.expanduser(self._private_key)]
            command += ["-o", "IdentitiesOnly=yes"]
        for forward in self._forwards:
            command += ["-L", forward.argument()]
        command += ["--", f"{self._user}@{self._host}"]
        return command

    def start(self) -> None:
        """
        Open the tunnels and return once every local port is accepting
        connections. Raises ``TunnelError``, leaving nothing running, if they cannot
        all be opened.
        """
        if self._process is not None:
            raise TunnelError("the tunnels have already been started")
        # Something already listening on a local port would be taken for the tunnel
        # below (and whatever was sent to it would go to the wrong place)
        for forward in self._forwards:
            if _accepting(forward):
                raise TunnelError(
                    f"local port {forward.local_port} is already in use; close "
                    "whatever is using it, or forward a different local port"
                )
        # Kept open for the life of the tunnel: closed in stop()
        self._stderr = tempfile.TemporaryFile()  # noqa: SIM115
        try:
            self._process = subprocess.Popen(
                self.command(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=self._stderr,
            )
        except OSError as e:
            self._discard()
            raise TunnelError(f"could not run '{self._ssh_command}': {e}") from None
        deadline = time.monotonic() + self._connect_timeout
        try:
            while True:
                if self._process.poll() is not None:
                    raise TunnelError(
                        f"ssh to {self._user}@{self._host} failed: "
                        + (
                            self._read_stderr()
                            or f"exit code {self._process.returncode}"
                        )
                    )
                if all(_accepting(f) for f in self._forwards):
                    return
                if time.monotonic() >= deadline:
                    raise TunnelError(
                        f"ssh to {self._user}@{self._host} did not open the tunnels "
                        f"within {self._connect_timeout:.0f} seconds"
                        + (f": {self._read_stderr()}" if self._read_stderr() else "")
                    )
                time.sleep(0.1)
        except BaseException:
            self.stop()
            raise

    def stop(self) -> None:
        """Close the tunnels. Safe to call at any time, any number of times."""
        process, self._process = self._process, None
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        self._discard()

    def __enter__(self) -> RayTunnels:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.stop()

    def _read_stderr(self) -> str:
        if self._stderr is None:
            return ""
        self._stderr.seek(0)
        return self._stderr.read().decode(errors="replace").strip()

    def _discard(self) -> None:
        if self._stderr is not None:
            with contextlib.suppress(OSError):
                self._stderr.close()
            self._stderr = None


def _accepting(forward: PortForward) -> bool:
    try:
        with socket.create_connection((forward.local_host, forward.local_port), 0.5):
            return True
    except OSError:
        return False
