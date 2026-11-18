"""
The RayDog part of a Ray cluster config: what it may contain and what it means.

Only two parts of the cluster YAML are RayDog's: the ``provider`` section and each
node type's ``node_config``. Everything here is plain data handling with no platform
calls, so that a mistake is reported before anything is created.
"""

from __future__ import annotations

import copy
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

from yellowdog_ray.core import naming
from yellowdog_ray.core.cluster import ClusterSettings, NodeSpec, validate_settings
from yellowdog_ray.errors import ConfigError

FILE_PREFIX = "file:"
MODULE = "yellowdog_ray.autoscaler.RayDogNodeProvider"

# Keys of the provider section
K_NAMESPACE = "namespace"
K_TAG = "tag"
K_LIFETIME = "lifetime_hours"
K_BUILD_TIMEOUT = "build_timeout_minutes"
K_NODE_IDLE = "node_idle_shutdown_seconds"
K_WORKER_GRACE = "worker_start_grace_seconds"
K_REFRESH = "refresh_interval_seconds"
K_HEAD_SCRIPT = "head_start_script"
K_WORKER_SCRIPT = "worker_start_script"
K_DISABLE_UPDATERS = "disable_node_updaters"  # Ray's own key
# ... added by RayDog itself, never written by the user
K_MAX_WORKERS = "raydog_max_workers"
K_HEAD = "raydog_head"
K_HEAD_RESOURCES = "raydog_head_resources"  # what the head node type declares
K_HEAD_LABELS = "raydog_head_labels"

_USER_KEYS = {
    K_NAMESPACE, K_TAG, K_LIFETIME, K_BUILD_TIMEOUT, K_NODE_IDLE, K_WORKER_GRACE,
    K_REFRESH, K_HEAD_SCRIPT, K_WORKER_SCRIPT, K_DISABLE_UPDATERS,
}  # fmt: skip
# Ray's own provider keys, which pass through untouched
_OTHER_KEYS = {
    "type",
    "module",
    "use_internal_ips",
    "use_external_head_ip",
    "worker_liveness_check",
    "disable_launch_config_check",
    "foreground_node_launch",
    K_MAX_WORKERS,
    K_HEAD,
    K_HEAD_RESOURCES,
    K_HEAD_LABELS,
}

# Keys of a node type's node_config
N_TEMPLATE = "compute_requirement_template"
N_IMAGES = "images_id"
N_USERDATA = "userdata"
N_INSTANCE_TAGS = "instance_tags"
N_METRICS = "metrics_enabled"
_NODE_KEYS = {N_TEMPLATE, N_IMAGES, N_USERDATA, N_INSTANCE_TAGS, N_METRICS}

# Ray runs these over SSH; RayDog starts Ray itself, from the start scripts
COMMAND_LISTS = (
    "initialization_commands",
    "setup_commands",
    "head_setup_commands",
    "worker_setup_commands",
    "head_start_ray_commands",
    "worker_start_ray_commands",
)

# What RayDog puts in head_start_ray_commands. `ray up` warns that Ray will not
# start, and that the head will launch no workers, unless a head start command
# mentions `ray start` and `--autoscaling-config`; RayDog's head start script does
# both, as a YellowDog task. So this says so, over the SSH connection Ray already
# has, and does nothing else.
HEAD_START_NOTE = [
    "echo 'raydog: Ray is started on the head node by provider.head_start_script,"
    " as a YellowDog task (ray start --head"
    " --autoscaling-config=~/ray_bootstrap_config.yaml)'"
]

# Ray reads these general settings without checking that they are there, and for
# an external provider it supplies no defaults of its own. Filling them in lets a
# cluster YAML contain only what its author cares about.
RAY_DEFAULTS: dict[str, Any] = {
    "max_workers": 2,
    "upscaling_speed": 1.0,
    "idle_timeout_minutes": 5,
    "file_mounts": {},
    "cluster_synced_files": [],
    "file_mounts_sync_continuously": False,
    "rsync_exclude": [],
    "rsync_filter": [],
    **{key: [] for key in COMMAND_LISTS},
}

DEFAULT_MAX_WORKERS = 1000
_PLAIN_NAME = re.compile(r"[A-Za-z0-9._-]+")
NO_LIMIT = "none"


@dataclass(frozen=True)
class HeadInfo:
    """What the head node's copy of the config is stamped with."""

    work_requirement_id: str
    credentials_file: str


@dataclass(frozen=True)
class ProviderConfig:
    cluster_name: str
    namespace: str
    head_start_script: str
    worker_start_script: str
    tag: str | None = None
    lifetime: timedelta | None = timedelta(hours=24)
    build_timeout: timedelta = timedelta(minutes=10)
    node_idle_shutdown: timedelta = timedelta(seconds=60)
    worker_start_grace: timedelta = timedelta(0)
    refresh_interval: timedelta = timedelta(seconds=2)
    max_workers: Mapping[str, int] = field(default_factory=dict)
    head_resources: Mapping[str, float] = field(default_factory=dict)
    head_labels: Mapping[str, str] = field(default_factory=dict)
    head: HeadInfo | None = None

    def settings(self) -> ClusterSettings:
        return ClusterSettings(
            name=self.cluster_name,
            namespace=self.namespace,
            tag=self.tag,
            lifetime=self.lifetime,
            idle_shutdown=self.node_idle_shutdown,
            build_timeout=self.build_timeout,
        )


def parse_provider_config(
    provider: Mapping[str, object], cluster_name: str
) -> ProviderConfig:
    """
    Validate a provider section and turn it into typed settings.

    Accepts the section as the user wrote it as well as after ``bootstrap``: Ray
    does not run the bootstrap step before every command.
    """
    naming.validate_cluster_name(cluster_name)
    for key in provider:
        if key not in _USER_KEYS and key not in _OTHER_KEYS and not key.startswith("_"):
            raise ConfigError(
                f"provider: unknown key '{key}' (known keys: "
                + ", ".join(sorted(_USER_KEYS))
                + ")"
            )
    disable = provider.get(K_DISABLE_UPDATERS, True)
    if disable is not True:
        raise ConfigError(
            f"provider.{K_DISABLE_UPDATERS} must be true (or left out): with Ray's "
            "node updaters on, Ray would copy the cluster's files, the YellowDog "
            "credentials among them, to every worker node"
        )

    lifetime_value = provider.get(K_LIFETIME, 24)
    lifetime: timedelta | None
    if isinstance(lifetime_value, str) and lifetime_value.lower() == NO_LIMIT:
        lifetime = None
    else:
        lifetime = timedelta(hours=_number(provider, K_LIFETIME, 24, above=0))

    max_workers = provider.get(K_MAX_WORKERS, {})
    if not isinstance(max_workers, Mapping) or not all(
        isinstance(k, str) and isinstance(v, int) and not isinstance(v, bool)
        for k, v in max_workers.items()
    ):
        raise ConfigError(f"provider.{K_MAX_WORKERS} is malformed")

    head_resources = provider.get(K_HEAD_RESOURCES, {})
    if not isinstance(head_resources, Mapping) or not all(
        isinstance(k, str)
        and isinstance(v, (int, float))
        and not isinstance(v, bool)
        and v >= 0
        for k, v in head_resources.items()
    ):
        raise ConfigError(f"provider.{K_HEAD_RESOURCES} is malformed")
    head_labels = provider.get(K_HEAD_LABELS, {})
    if not isinstance(head_labels, Mapping) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in head_labels.items()
    ):
        raise ConfigError(f"provider.{K_HEAD_LABELS} is malformed")

    namespace = _text(provider, K_NAMESPACE)
    if not _PLAIN_NAME.fullmatch(namespace) or namespace in (".", ".."):
        # It becomes part of a file name (the credentials file)
        raise ConfigError(
            f"provider.{K_NAMESPACE} '{namespace}' is not a valid namespace name"
        )
    config = ProviderConfig(
        cluster_name=cluster_name,
        namespace=namespace,
        head_start_script=_text(provider, K_HEAD_SCRIPT),
        worker_start_script=_text(provider, K_WORKER_SCRIPT),
        tag=_optional_text(provider, K_TAG),
        lifetime=_whole_seconds(lifetime, K_LIFETIME),
        build_timeout=timedelta(
            minutes=_number(provider, K_BUILD_TIMEOUT, 10, above=0)
        ),
        node_idle_shutdown=timedelta(seconds=_number(provider, K_NODE_IDLE, 60)),
        worker_start_grace=timedelta(seconds=_number(provider, K_WORKER_GRACE, 0)),
        refresh_interval=timedelta(seconds=_number(provider, K_REFRESH, 2)),
        max_workers=dict(max_workers),
        head_resources=dict(head_resources),
        head_labels=dict(head_labels),
        head=_head_info(provider.get(K_HEAD)),
    )
    try:
        validate_settings(config.settings())
    except ConfigError as e:
        raise ConfigError(f"provider: {e}") from None
    return config


def parse_node_config(node_type: str, node_config: object) -> NodeSpec:
    """
    Validate a node type's ``node_config``. The start script is not part of it:
    the caller supplies the head's or the workers' from the provider section.
    """
    where = f"available_node_types.{node_type}.node_config"
    if not isinstance(node_config, Mapping):
        raise ConfigError(f"{where} must be a mapping")
    for key in node_config:
        if key not in _NODE_KEYS:
            raise ConfigError(
                f"{where}: unknown key '{key}' (known keys: "
                + ", ".join(sorted(_NODE_KEYS))
                + ")"
            )
    instance_tags = node_config.get(N_INSTANCE_TAGS) or {}
    if not isinstance(instance_tags, Mapping) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in instance_tags.items()
    ):
        raise ConfigError(f"{where}.{N_INSTANCE_TAGS} must map strings to strings")
    metrics = node_config.get(N_METRICS, False)
    if not isinstance(metrics, bool):
        raise ConfigError(f"{where}.{N_METRICS} must be true or false")
    return NodeSpec(
        compute_requirement_template=_text(node_config, N_TEMPLATE, where),
        start_script="",
        images_id=_optional_text(node_config, N_IMAGES, where),
        userdata=_userdata(node_config.get(N_USERDATA), where),
        instance_tags=dict(instance_tags),
        metrics_enabled=metrics,
    )


def with_ray_defaults(cluster_config: Mapping[str, Any]) -> dict[str, Any]:
    """The config with Ray's general settings filled in where missing or null."""
    config = dict(cluster_config)
    for key, value in RAY_DEFAULTS.items():
        if config.get(key) is None:
            config[key] = copy.deepcopy(value)
    return config


def bootstrap(
    cluster_config: Mapping[str, object], search_dirs: Sequence[Path]
) -> dict[str, Any]:
    """
    Check a whole cluster config and prepare it for use: a new dict.

    Every ``file:`` reference is replaced by the file's text, so that the config is
    self-contained when Ray copies it to the head node. Relative paths are looked
    for in ``search_dirs`` in order. Running this on its own output changes nothing.
    """
    config: dict[str, Any] = copy.deepcopy(dict(cluster_config))
    cluster_name = config.get("cluster_name")
    if not isinstance(cluster_name, str):
        raise ConfigError("cluster_name must be a string")
    provider = config.get("provider")
    if not isinstance(provider, dict):
        raise ConfigError("provider must be a mapping")

    for key in COMMAND_LISTS:
        mine = key == "head_start_ray_commands" and config.get(key) == HEAD_START_NOTE
        if config.get(key) and not mine:
            raise ConfigError(
                f"{key} must be empty: RayDog starts Ray from "
                f"provider.{K_HEAD_SCRIPT} and provider.{K_WORKER_SCRIPT}, and sets "
                "nodes up through each node type's userdata"
            )
    config["head_start_ray_commands"] = list(HEAD_START_NOTE)

    for key in (K_HEAD_SCRIPT, K_WORKER_SCRIPT):
        if key in provider:
            provider[key] = _inline(provider[key], search_dirs, f"provider.{key}")
    provider.setdefault(K_DISABLE_UPDATERS, True)

    node_types = config.get("available_node_types")
    if not isinstance(node_types, dict) or not node_types:
        raise ConfigError("available_node_types must define at least a head node type")
    head_type = config.get("head_node_type")
    if head_type not in node_types:
        raise ConfigError(
            f"head_node_type '{head_type}' is not one of available_node_types"
        )
    max_workers: dict[str, int] = {}
    roles: dict[str, str] = {}
    for node_type, definition in node_types.items():
        where = f"available_node_types.{node_type}"
        if not isinstance(definition, dict):
            raise ConfigError(f"{where} must be a mapping")
        node_config = definition.get("node_config")
        if isinstance(node_config, dict) and N_USERDATA in node_config:
            userdata = node_config[N_USERDATA]
            items = userdata if isinstance(userdata, list) else [userdata]
            inlined = [
                _inline(item, search_dirs, f"{where}.node_config.{N_USERDATA}")
                for item in items
            ]
            node_config[N_USERDATA] = (
                inlined if isinstance(userdata, list) else inlined[0]
            )
        parse_node_config(node_type, node_config)
        if node_type != head_type:
            role = naming.worker_role(node_type)
            if role in roles:
                raise ConfigError(
                    f"node types '{roles[role]}' and '{node_type}' are too alike: "
                    "rename one of them"
                )
            roles[role] = node_type
            limit = definition.get("max_workers", config.get("max_workers"))
            if isinstance(limit, int) and not isinstance(limit, bool) and limit > 0:
                max_workers[node_type] = limit
    provider[K_MAX_WORKERS] = max_workers
    # Ray gives a provider the declaration of each worker type it launches, but not
    # the head's: kept here, for the provider to give to the head node
    provider[K_HEAD_RESOURCES] = node_types[head_type].get("resources") or {}
    provider[K_HEAD_LABELS] = node_types[head_type].get("labels") or {}

    parsed = parse_provider_config(provider, cluster_name)

    # The head node needs credentials. They go by Ray's own file sync, over SSH,
    # and never through YellowDog: the config holds only the path.
    local, remote = credentials_paths(parsed.namespace, cluster_name)
    mounts = config.get("file_mounts")
    if mounts is None:
        mounts = config["file_mounts"] = {}
    if not isinstance(mounts, dict):
        raise ConfigError("file_mounts must be a mapping")
    mounts[remote] = str(local)
    return config


def credentials_paths(namespace: str, cluster_name: str) -> tuple[Path, str]:
    """
    Where the cluster's credentials file is kept on the client, and where Ray
    puts it on the head node (``~`` is the SSH user's home there).
    """
    name = f"{namespace}.{cluster_name}"
    return (
        Path.home() / ".raydog" / "credentials" / name,
        f"~/.raydog/credentials/{name}",
    )


def default_search_dirs(argv: Sequence[str]) -> list[Path]:
    """
    Where to look for relative ``file:`` paths: beside the cluster YAML, then in
    the working directory. Ray does not tell a provider where the YAML is, but it
    is named on the command line, so the directory of each command-line argument
    that is a file is used.
    """
    dirs: list[Path] = []
    for argument in argv[1:]:
        try:
            path = Path(argument)
            if path.is_file() and path.parent.resolve() not in dirs:
                dirs.append(path.parent.resolve())
        except (OSError, ValueError):
            continue
    if Path.cwd().resolve() not in dirs:
        dirs.append(Path.cwd().resolve())
    return dirs


def is_file_reference(value: object) -> bool:
    return isinstance(value, str) and value.startswith(FILE_PREFIX)


def _inline(value: object, search_dirs: Sequence[Path], where: str) -> object:
    if not is_file_reference(value):
        return value
    assert isinstance(value, str)
    name = value[len(FILE_PREFIX) :].strip()
    if not name:
        raise ConfigError(f"{where}: '{FILE_PREFIX}' is not followed by a path")
    path = Path(name).expanduser()
    candidates = [path] if path.is_absolute() else [d / path for d in search_dirs]
    for candidate in candidates:
        if candidate.is_file():
            try:
                return candidate.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError) as e:
                raise ConfigError(f"{where}: cannot read {candidate}: {e}") from None
    tried = ", ".join(str(c.parent) for c in candidates)
    raise ConfigError(f"{where}: file '{name}' not found (looked in: {tried})")


def _userdata(value: object, where: str) -> str | None:
    if value is None:
        return None
    items = value if isinstance(value, list) else [value]
    texts = [item for item in items if isinstance(item, str)]
    if len(texts) != len(items):
        raise ConfigError(f"{where}.{N_USERDATA} must be text, or a list of texts")
    return "\n".join(texts) or None


def _head_info(value: object) -> HeadInfo | None:
    if value is None:
        return None
    if (
        not isinstance(value, Mapping)
        or not isinstance(value.get("work_requirement_id"), str)
        or not isinstance(value.get("credentials_file"), str)
    ):
        raise ConfigError(f"provider.{K_HEAD} is malformed")
    return HeadInfo(value["work_requirement_id"], value["credentials_file"])


def _text(section: Mapping[str, object], key: str, where: str = "provider") -> str:
    value = section.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{where}.{key} is required and must be text")
    return value


def _optional_text(
    section: Mapping[str, object], key: str, where: str = "provider"
) -> str | None:
    value = section.get(key)
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ConfigError(f"{where}.{key} must be text")
    return value


def _number(
    section: Mapping[str, object], key: str, default: float, above: float | None = None
) -> float:
    value = section.get(key, default)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ConfigError(f"provider.{key} must be a number")
    if value < 0 or (above is not None and value <= above):
        raise ConfigError(
            f"provider.{key} must be "
            + ("greater than" if above is not None else "at least")
            + f" {above if above is not None else 0}"
        )
    return float(value)


def _whole_seconds(value: timedelta | None, key: str) -> timedelta | None:
    if value is not None and value.total_seconds() != int(value.total_seconds()):
        raise ConfigError(f"provider.{key} must come to a whole number of seconds")
    return value


__all__ = [
    "COMMAND_LISTS",
    "HeadInfo",
    "ProviderConfig",
    "bootstrap",
    "credentials_paths",
    "default_search_dirs",
    "parse_node_config",
    "parse_provider_config",
]
