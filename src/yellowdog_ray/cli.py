"""
The ``raydog`` command: see what clusters exist, and end them.

    raydog list                     clusters in a namespace
    raydog status CLUSTER           one cluster in detail
    raydog down CLUSTER             shut a cluster down
    raydog cleanup                  find (and with --delete, remove) leftovers
    raydog tunnel CLUSTER           forward the Ray ports of a cluster's head node

It works from any machine with YellowDog credentials: clusters are found on the
platform by name, whoever created them and however (Builder or ``ray up``).
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import signal
import socket
import sys
import time
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta
from typing import Any

from yellowdog_ray import __version__, logs
from yellowdog_ray.core import naming
from yellowdog_ray.core.clock import Clock, SystemClock
from yellowdog_ray.core.cluster import (
    ClusterCore,
    ClusterHandle,
    ClusterStatus,
    Orphan,
)
from yellowdog_ray.core.platform import Platform, TaskStatus
from yellowdog_ray.credentials import load_credentials
from yellowdog_ray.errors import (
    ClusterNotFoundError,
    ConfigError,
    CredentialsError,
    RayDogError,
    TeardownError,
    TunnelError,
)
from yellowdog_ray.tunnels import PortForward, RayTunnels

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_NOT_FOUND = 3
EXIT_INCOMPLETE = 4
EXIT_INTERRUPTED = 130

ENV_NAMESPACE = "RAYDOG_NAMESPACE"
ENV_SSH = "RAYDOG_SSH"
VERIFY_TIMEOUT = timedelta(minutes=10)


def main(
    argv: Sequence[str] | None = None,
    *,
    platform: Platform | None = None,
    clock: Clock | None = None,
) -> int:
    """Run the command line. Returns the process exit code."""
    parser = _parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as e:  # argparse has already printed its message
        return int(e.code or 0)
    if not getattr(args, "run", None):
        parser.print_help()
        return EXIT_USAGE
    with _verbose(getattr(args, "verbose", 0)):
        return _run(args, platform, clock)


def _run(
    args: argparse.Namespace, platform: Platform | None, clock: Clock | None
) -> int:
    owned: Platform | None = None
    try:
        if getattr(args, "namespace_needed", True):
            args.namespace = args.namespace or os.environ.get(ENV_NAMESPACE)
            if not args.namespace:
                raise ConfigError(f"give a namespace with -n, or set {ENV_NAMESPACE}")
        if platform is None:
            from yellowdog_ray.core.yellowdog import YellowDogPlatform

            credentials = load_credentials()
            platform = owned = YellowDogPlatform.connect(
                credentials.key_id, credentials.key_secret, credentials.url
            )
        clock = clock or SystemClock()
        return int(args.run(args, ClusterCore(platform, clock), clock))
    except (ConfigError, CredentialsError) as e:
        return _fail(EXIT_USAGE, str(e))
    except ClusterNotFoundError as e:
        return _fail(EXIT_NOT_FOUND, str(e))
    except TeardownError as e:
        print("Not everything could be removed:", file=sys.stderr)
        for what, error in e.failures:
            print(f"  {what}: {error}", file=sys.stderr)
        return _fail(
            EXIT_INCOMPLETE, "Something may still be running. Run the command again."
        )
    except RayDogError as e:
        return _fail(EXIT_FAILED, str(e))
    except KeyboardInterrupt:
        return _fail(
            EXIT_INTERRUPTED,
            "Interrupted. The operation may be incomplete: run the command again.",
        )
    finally:
        if owned is not None:
            owned.close()


@contextlib.contextmanager
def _verbose(level: int) -> Iterator[None]:
    """
    With -v, show what RayDog does (INFO); with -vv, every call it makes to the
    platform as well (DEBUG). Logging is put back as it was afterwards.
    """
    if not level:
        yield
        return
    package = logging.getLogger(logs.PACKAGE)
    handler = logs.stderr_handler()
    previous = package.level
    package.setLevel(logging.INFO if level == 1 else logging.DEBUG)
    package.addHandler(handler)
    try:
        yield
    finally:
        package.removeHandler(handler)
        package.setLevel(previous)


def _fail(code: int, message: str) -> int:
    print(f"raydog: {message}", file=sys.stderr)
    return code


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="raydog", description="See and end RayDog clusters on YellowDog."
    )
    parser.add_argument("--version", action="version", version=f"raydog {__version__}")
    verbose_help = "show what is done (-v), and every call to the platform (-vv)"
    parser.add_argument("-v", "--verbose", action="count", default=0, help=verbose_help)
    commands = parser.add_subparsers(metavar="COMMAND")

    def command(name: str, run: Any, help_text: str) -> argparse.ArgumentParser:
        sub = commands.add_parser(name, help=help_text, description=help_text)
        # Also accepted after the command; SUPPRESS keeps a -v given before it
        sub.add_argument(
            "-v",
            "--verbose",
            action="count",
            default=argparse.SUPPRESS,
            help=verbose_help,
        )
        sub.add_argument(
            "-n",
            "--namespace",
            help=f"the YellowDog namespace (default: ${ENV_NAMESPACE})",
        )
        sub.set_defaults(run=run)
        return sub

    sub = command("list", _list, "List the clusters in a namespace.")
    sub.add_argument(
        "--all", action="store_true", help="include clusters that have ended"
    )
    sub.add_argument("--json", action="store_true", help="print JSON")

    sub = command("status", _status, "Show one cluster in detail.")
    sub.add_argument("cluster")
    sub.add_argument("--json", action="store_true", help="print JSON")

    sub = command("down", _down, "Shut a cluster down.")
    sub.add_argument("cluster")
    sub.add_argument(
        "--wait", action="store_true", help="return only when no instance is left"
    )
    sub.add_argument("--yes", "-y", action="store_true", help="do not ask first")
    sub.add_argument(
        "--dry-run", action="store_true", help="show what would be removed, and stop"
    )

    sub = command(
        "cleanup",
        _cleanup,
        "Find what failed runs left behind: pools whose cluster has ended, and "
        "clusters running without a head node. Lists them; removes them only with "
        "--delete.",
    )
    sub.add_argument("--delete", action="store_true", help="remove what is found")
    sub.add_argument(
        "--wait", action="store_true", help="return only when no instance is left"
    )
    sub.add_argument("--json", action="store_true", help="print JSON")

    sub = command(
        "tunnel",
        _tunnel,
        "Forward the Ray client (10001) and dashboard (8265) ports of a cluster's "
        "head node to this machine over SSH, until interrupted.",
    )
    sub.add_argument("cluster")
    sub.add_argument("-i", "--identity", help="the SSH private key file")
    sub.add_argument("-u", "--user", default="yd-agent", help="the SSH user")
    sub.add_argument(
        "-p",
        "--port",
        action="append",
        metavar="LOCAL[:REMOTE]",
        help="a port to forward instead of the defaults; may be repeated",
    )
    sub.add_argument(
        "--internal",
        action="store_true",
        help="connect to the head node's private address",
    )
    sub.add_argument(
        "-o",
        "--ssh-option",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="an option for ssh, as its own -o; may be repeated. Host keys are "
        "not checked unless you pass StrictHostKeyChecking=yes and a "
        "UserKnownHostsFile",
    )
    sub.add_argument(
        "--for",
        dest="duration",
        type=float,
        metavar="SECONDS",
        help="close the tunnels after this long",
    )
    return parser


# ----------------------------------------------------------------------------
# list
# ----------------------------------------------------------------------------


def _list(args: argparse.Namespace, core: ClusterCore, clock: Clock) -> int:
    rows = []
    for summary in core.list_clusters(args.namespace, include_ended=args.all):
        try:
            status = core.status(core.handle_for(summary.work_requirement_id))
        except ClusterNotFoundError:
            continue  # ended and purged while we were looking
        rows.append(
            {
                "name": summary.name,
                "state": _state(status, clock),
                "created": summary.created.isoformat(),
                "age": _duration(clock.now() - summary.created),
                "nodes": _node_count(status),
                "instances": _instance_count(status),
                "work_requirement": summary.work_requirement_name,
            }
        )
    if args.json:
        print(json.dumps(rows, indent=2))
    elif not rows:
        print(f"No clusters in namespace '{args.namespace}'.")
    else:
        _table(
            ["NAME", "STATE", "AGE", "NODES", "INSTANCES"],
            [
                [r["name"], r["state"], r["age"], r["nodes"], r["instances"]]
                for r in rows
            ],
        )
        if any(r["state"] in NOT_WORKING for r in rows):
            print(
                "\nClusters marked 'no head' or 'expired' no longer work, but their "
                "work requirements stay until removed: `raydog down <name>`, or "
                "`raydog cleanup --delete` for all of them."
            )
    return EXIT_OK


# ----------------------------------------------------------------------------
# status
# ----------------------------------------------------------------------------


def _status(args: argparse.Namespace, core: ClusterCore, clock: Clock) -> int:
    handle = _find(core, args.cluster, args.namespace, include_ended=True)
    status = core.status(handle)
    head = status.head
    private_ip = public_ip = None
    if head is not None and head.worker_id and head.status is TaskStatus.EXECUTING:
        found = core.node_addresses(
            handle, naming.HEAD_ROLE, {head.task_id: head.worker_id}, with_public=True
        )
        private_ip, public_ip = found.get(head.task_id, (None, None))
    deadline = handle.deadline
    data = {
        "name": handle.settings.name,
        "namespace": handle.settings.namespace,
        "state": _state(status, clock),
        "work_requirement": handle.work_requirement_name,
        "work_requirement_id": handle.work_requirement_id,
        "work_requirement_status": status.work_requirement_status.value,
        "created": handle.created.isoformat(),
        "deadline": deadline.isoformat() if deadline else None,
        "head": {
            "status": head.status.value,
            "task_id": head.task_id,
            "private_ip": private_ip,
            "public_ip": public_ip,
            "error": head.error,
        }
        if head
        else None,
        "pools": {
            role: {
                "status": pool.status.value if pool.status else None,
                "nodes": {s.value.lower(): n for s, n in pool.nodes_by_status.items()},
                "instances": pool.live_instances,
                "max_nodes": pool.max_nodes,
            }
            for role, pool in status.pools.items()
        },
    }
    if args.json:
        print(json.dumps(data, indent=2))
        return EXIT_OK

    print(f"Cluster {data['name']} in namespace {data['namespace']}: {data['state']}")
    print(
        f"  work requirement  {handle.work_requirement_name}  "
        f"{status.work_requirement_status.value}"
    )
    print(f"  created           {_when(handle.created)}")
    if deadline is None:
        print("  lifetime          no limit")
    else:
        left = deadline - clock.now()
        if not status.work_requirement_status.is_live:
            remaining = "ended"
        elif left > timedelta(0):
            remaining = f"{_duration(left)} left"
        else:
            remaining = "passed"
        print(f"  lifetime          until {_when(deadline)} ({remaining})")
    if head is None:
        print("  head node         none")
    else:
        where = (
            f"  private {private_ip}  public {public_ip or '-'}" if private_ip else ""
        )
        why = f"  ({head.error})" if head.error else ""
        print(f"  head node         {head.status.value}{where}{why}")
    if status.pools:
        print("  pools")
        _table(
            ["ROLE", "POOL", "NODES", "INSTANCES", "MAX"],
            [
                [
                    role,
                    pool.status.value if pool.status else "none",
                    _nodes_text(pool.nodes_by_status),
                    pool.live_instances,
                    pool.max_nodes if pool.max_nodes is not None else "-",
                ]
                for role, pool in status.pools.items()
            ],
            indent="    ",
        )
    if data["state"] in NOT_WORKING:
        print(
            f"\nThis cluster no longer works. `raydog down {data['name']} -n "
            f"{data['namespace']}` removes what is left of it."
        )
    return EXIT_OK


# ----------------------------------------------------------------------------
# down
# ----------------------------------------------------------------------------


def _down(args: argparse.Namespace, core: ClusterCore, clock: Clock) -> int:
    try:
        handle: ClusterHandle | None = core.attach(args.cluster, args.namespace)
    except ClusterNotFoundError:
        handle = None
    # Whatever an earlier cluster of this name left behind goes too
    leftovers = [
        o for o in core.find_orphans(args.namespace) if o.cluster == args.cluster
    ]
    if handle is not None:
        leftovers = [o for o in leftovers if o.id != handle.work_requirement_id]
    if handle is None and not leftovers:
        raise ClusterNotFoundError(
            f"no cluster named '{args.cluster}' in namespace '{args.namespace}'"
        )

    if handle is not None:
        try:
            status = core.status(handle)
        except ClusterNotFoundError:
            raise
        except RayDogError as e:
            # The summary is a courtesy: failing to show it must not stop someone
            # shutting a cluster down
            print(f"Cluster {args.cluster}: state unknown ({e})", file=sys.stderr)
        else:
            print(
                f"Cluster {args.cluster}: {_state(status, clock)}, "
                f"{_node_count(status)} node(s), "
                f"{_instance_count(status)} instance(s)"
            )
    for orphan in leftovers:
        print(f"Left over: {_orphan_text(orphan)}")
    if args.dry_run:
        print("Dry run: nothing removed.")
        return EXIT_OK
    if not args.yes and not _confirm(f"Shut down cluster '{args.cluster}'?"):
        return _fail(EXIT_FAILED, "Not confirmed: nothing removed.")

    verify = VERIFY_TIMEOUT if args.wait else None
    failures: list[tuple[str, Exception]] = []
    for action in (
        (lambda: core.tear_down(handle, verify)) if handle is not None else None,
        (lambda: core.remove_orphans(leftovers, verify)) if leftovers else None,
    ):
        if action is None:
            continue
        try:
            action()
        except TeardownError as e:
            failures.extend(e.failures)
    if failures:
        raise TeardownError(failures)
    print(
        f"Cluster {args.cluster} shut down"
        + (
            "; no instances remain."
            if args.wait
            else "; its instances are being removed."
        )
    )
    return EXIT_OK


# ----------------------------------------------------------------------------
# cleanup
# ----------------------------------------------------------------------------


def _cleanup(args: argparse.Namespace, core: ClusterCore, clock: Clock) -> int:
    orphans = core.find_orphans(args.namespace)
    if args.json:
        print(
            json.dumps(
                [
                    {
                        "kind": o.kind,
                        "name": o.name,
                        "id": o.id,
                        "cluster": o.cluster,
                        "reason": o.reason,
                    }
                    for o in orphans
                ],
                indent=2,
            )
        )
    elif not orphans:
        print(f"Nothing left over in namespace '{args.namespace}'.")
    else:
        for orphan in orphans:
            print(_orphan_text(orphan))
    if not orphans:
        return EXIT_OK
    if not args.delete:
        if not args.json:
            print("Nothing removed. Run again with --delete to remove these.")
        return EXIT_OK
    core.remove_orphans(orphans, VERIFY_TIMEOUT if args.wait else None)
    if not args.json:
        print(
            f"Removed {len(orphans)} item(s); no instances remain."
            if args.wait
            else f"Removal of {len(orphans)} item(s) requested; their instances "
            "are being removed."
        )
    return EXIT_OK


# ----------------------------------------------------------------------------
# tunnel
# ----------------------------------------------------------------------------


# How far above a taken default port to look for a free one
_PORT_SEARCH = 100


def _use_of(forward: PortForward) -> str:
    """What a forwarded port is for, if it is one of Ray's, and how to use it."""
    if forward.remote_port == RayTunnels.CLIENT.remote_port:
        return f'  (Ray client: ray.init("ray://localhost:{forward.local_port}"))'
    if forward.remote_port == RayTunnels.DASHBOARD.remote_port:
        return f"  (dashboard and job server: http://localhost:{forward.local_port})"
    return ""


def _port_in_use(port: int) -> bool:
    """Whether a local port is taken: something answers on it, or it cannot be bound."""
    with (
        contextlib.suppress(OSError),
        socket.create_connection(("localhost", port), 0.5),
    ):
        return True
    with socket.socket() as probe:
        try:
            probe.bind(("localhost", port))
        except OSError:
            return True
    return False


def _free_ports(defaults: list[PortForward]) -> tuple[list[PortForward], list[str]]:
    """
    The default forwards, each moved to the next free local port if its own is
    taken (by another tunnel, or a Ray running locally), and what was moved.
    """
    chosen: list[PortForward] = []
    notes: list[str] = []
    used: set[int] = set()
    for forward in defaults:
        port = forward.local_port
        while port in used or _port_in_use(port):
            port += 1
            if port > forward.local_port + _PORT_SEARCH:
                raise TunnelError(
                    f"local ports {forward.local_port} to {port - 1} are all in use; "
                    f"choose one with -p <free port>:{forward.remote_port}"
                )
        if port != forward.local_port:
            notes.append(f"local port {forward.local_port} is in use; using {port}")
            forward = PortForward(port, forward.remote_port)
        used.add(port)
        chosen.append(forward)
    return chosen, notes


def _tunnel(args: argparse.Namespace, core: ClusterCore, clock: Clock) -> int:
    notes: list[str] = []
    if args.port:
        # Asked for by number: kept, and refused if taken
        forwards = [_forward(p) for p in args.port]
        for forward in forwards:
            if _port_in_use(forward.local_port):
                raise TunnelError(
                    f"local port {forward.local_port} is already in use; close "
                    "whatever is using it, or forward another: "
                    f"-p <free port>:{forward.remote_port}"
                )
    else:
        forwards, notes = _free_ports([RayTunnels.CLIENT, RayTunnels.DASHBOARD])
    handle = core.attach(args.cluster, args.namespace)
    head = core.status(handle).head
    if head is None or head.status is not TaskStatus.EXECUTING or not head.worker_id:
        raise RayDogError(f"the head node of cluster '{args.cluster}' is not running")
    found = core.node_addresses(
        handle, naming.HEAD_ROLE, {head.task_id: head.worker_id}, with_public=True
    )
    private_ip, public_ip = found.get(head.task_id, (None, None))
    host = private_ip if args.internal else public_ip
    if not host:
        raise RayDogError(
            f"the head node of cluster '{args.cluster}' has no "
            + ("known private address" if args.internal else "public address")
            + (
                ""
                if args.internal
                else " (use --internal to connect to its private one)"
            )
        )
    try:
        tunnels = RayTunnels(
            host,
            args.user,
            private_key=args.identity,
            forwards=forwards,
            ssh_options=args.ssh_option,
            ssh_command=os.environ.get(ENV_SSH) or "ssh",
        )
    except ValueError as e:
        raise ConfigError(str(e)) from None
    try:
        with _interruptible(), tunnels:
            for note in notes:
                print(note)
            for forward in forwards:
                print(
                    f"localhost:{forward.local_port} -> {args.cluster} head node, "
                    f"port {forward.remote_port}{_use_of(forward)}"
                )
            print("Press Ctrl-C to close.", flush=True)
            started = time.monotonic()
            try:
                while tunnels.is_running:
                    if (
                        args.duration is not None
                        and time.monotonic() - started >= args.duration
                    ):
                        return EXIT_OK
                    time.sleep(0.2)
            except KeyboardInterrupt:
                return EXIT_OK
    except TunnelError as e:
        if args.identity is None and "Permission denied" in str(e):
            raise TunnelError(
                f"{e}\nThe head node accepted none of the SSH keys offered: "
                "pass the cluster's private key file with -i"
            ) from None
        raise
    return _fail(EXIT_FAILED, "the SSH connection closed")


@contextlib.contextmanager
def _interruptible() -> Iterator[None]:
    """
    Treat SIGTERM and SIGHUP as Ctrl-C for the duration, so that being stopped
    by a supervisor or a closing terminal still closes the ssh process.
    """

    def interrupt(signum: int, frame: object) -> None:
        raise KeyboardInterrupt

    previous: dict[int, Any] = {}
    for name in ("SIGTERM", "SIGHUP"):
        signum = getattr(signal, name, None)  # no SIGHUP on Windows
        if signum is None:
            continue
        with contextlib.suppress(ValueError):  # not the main thread
            previous[signum] = signal.signal(signum, interrupt)
    try:
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def _forward(text: str) -> PortForward:
    local, _, remote = text.partition(":")
    try:
        return PortForward(int(local), int(remote or local))
    except ValueError:
        raise ConfigError(f"'{text}' is not a port, or LOCAL:REMOTE ports") from None


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------


def _find(
    core: ClusterCore, name: str, namespace: str, include_ended: bool
) -> ClusterHandle:
    try:
        return core.attach(name, namespace)
    except ClusterNotFoundError:
        if not include_ended:
            raise
    ended = [
        c for c in core.list_clusters(namespace, include_ended=True) if c.name == name
    ]
    if not ended:
        raise ClusterNotFoundError(
            f"no cluster named '{name}' in namespace '{namespace}'"
        )
    return core.handle_for(ended[-1].work_requirement_id)  # the most recent


# States of a live work requirement whose cluster no longer works
NOT_WORKING = ("no head", "expired")


def _state(status: ClusterStatus, clock: Clock) -> str:
    if not status.work_requirement_status.is_live:
        return status.work_requirement_status.value.lower()
    if status.past_deadline:
        return "expired"
    if status.head_alive:
        return "running"
    head = status.head
    age = clock.now() - status.handle.created
    if age <= status.handle.settings.build_timeout and (
        head is None or head.status.is_live
    ):
        return "starting"
    return "no head"


def _node_count(status: ClusterStatus) -> int:
    return sum(sum(p.nodes_by_status.values()) for p in status.pools.values())


def _instance_count(status: ClusterStatus) -> int:
    return sum(p.live_instances for p in status.pools.values())


def _nodes_text(nodes: Any) -> str:
    if not nodes:
        return "0"
    return ", ".join(f"{count} {state.value.lower()}" for state, count in nodes.items())


def _orphan_text(orphan: Orphan) -> str:
    kind = "pool" if orphan.kind == "pool" else "cluster"
    return f"{kind} {orphan.name} (cluster '{orphan.cluster}'): {orphan.reason}"


def _confirm(question: str) -> bool:
    try:
        return input(f"{question} [y/N] ").strip().lower() in ("y", "yes")
    except EOFError:
        return False


def _when(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%d %H:%M:%S UTC")


def _duration(delta: timedelta) -> str:
    seconds = max(0, int(delta.total_seconds()))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes = seconds // 60
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _table(headers: list[str], rows: list[list[Any]], indent: str = "") -> None:
    cells = [headers, *[[str(c) for c in row] for row in rows]]
    widths = [max(len(row[i]) for row in cells) for i in range(len(headers))]
    for row in cells:
        print(
            indent
            + "  ".join(c.ljust(w) for c, w in zip(row, widths, strict=True)).rstrip()
        )


if __name__ == "__main__":
    sys.exit(main())
