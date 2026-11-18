"""
Names and ownership markers for everything RayDog creates.

A cluster named ``demo`` becomes:

- a work requirement ``demo_k3x9q2ab`` (the cluster name, a separator, and a random
  suffix so that the name can be reused the same day);
- a task group per role, named for the role and tagged ``raydog:1:demo:<role>``;
- a worker pool per role, ``demo_k3x9q2ab_<role>``, whose instances are tagged with
  the cluster and work requirement names.

The underscore is reserved as the separator, so cluster names may not contain one;
that is what lets a name be taken apart again without guessing. Roles are ``head``,
``observability`` and ``w-<name>`` (a worker pool or node type), so a user's pool
called "head" cannot collide with the real one.

The platform's own searches match substrings, on names and on tags, so nothing here
relies on them for identity: names are parsed and compared exactly.
"""

from __future__ import annotations

import hashlib
import re
import secrets
import string
from collections.abc import Mapping

from yellowdog_ray.errors import ConfigError

SEPARATOR = "_"
HEAD_ROLE = "head"
OBSERVABILITY_ROLE = "observability"
HEAD_TASK_NAME = "head"  # fixed, so the platform refuses a second head
WORKER_ROLE_PREFIX = "w-"

MAX_NAME = 60  # the platform's limit on work requirement, pool and task names
MAX_CLUSTER_NAME = 24
SUFFIX_LENGTH = 8
MAX_WORKER_NAME = 24  # so a role is at most 26, and a pool name at most 60

MARKER_PREFIX = "raydog"
MARKER_VERSION = "1"
CLUSTER_INSTANCE_TAG = "raydog-cluster"
WORK_REQUIREMENT_INSTANCE_TAG = "raydog-work-requirement"

_ALPHABET = string.ascii_lowercase + string.digits
_CLUSTER = r"[a-z](?:[a-z0-9-]*[a-z0-9])?"
_ROLE = r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?"
_CLUSTER_RE = re.compile(_CLUSTER)
_ROLE_RE = re.compile(_ROLE)
_WORK_REQUIREMENT_RE = re.compile(rf"({_CLUSTER})_([a-z0-9]{{{SUFFIX_LENGTH}}})")
_POOL_RE = re.compile(rf"({_CLUSTER})_([a-z0-9]{{{SUFFIX_LENGTH}}})_({_ROLE})")
_UNSAFE = re.compile(r"[^a-z0-9]+")
_HASH_LENGTH = 8
_LOOKS_HASHED = re.compile(rf"(?:^|-)[0-9a-f]{{{_HASH_LENGTH}}}$")
_SETTING = re.compile(r"([a-z]+)=([0-9]+)")


def validate_cluster_name(name: str) -> None:
    if not name:
        raise ConfigError("cluster name must not be empty")
    if len(name) > MAX_CLUSTER_NAME:
        raise ConfigError(
            f"cluster name '{name}' is {len(name)} characters; "
            f"the limit is {MAX_CLUSTER_NAME}"
        )
    if not _CLUSTER_RE.fullmatch(name):
        raise ConfigError(
            f"cluster name '{name}' is invalid: use lowercase letters, digits and "
            "hyphens, starting with a letter and ending with a letter or digit"
        )


def new_suffix() -> str:
    return _random(SUFFIX_LENGTH)


def worker_role(name: str) -> str:
    """
    The role for a worker pool or node type with this (user-chosen) name.

    Simple names are kept. Anything that has to be altered or shortened gets a short
    hash of the original appended, so two different names never share a role.
    """
    if not name:
        raise ConfigError("a worker pool or node type name must not be empty")
    safe = _UNSAFE.sub("-", name.lower()).strip("-")
    # A name that already ends like a hash is hashed too, or it could be chosen to
    # equal the role of some other, altered name
    if safe != name or len(safe) > MAX_WORKER_NAME or _LOOKS_HASHED.search(safe):
        digest = hashlib.sha1(name.encode()).hexdigest()[:_HASH_LENGTH]
        stem = safe[: MAX_WORKER_NAME - _HASH_LENGTH - 1].rstrip("-")
        safe = f"{stem}-{digest}" if stem else digest
    return WORKER_ROLE_PREFIX + safe


def validate_role(role: str) -> None:
    """
    A role is ``head``, ``observability`` or ``w-<name>``: anything else
    could not be found again from its pool's name, so it is refused before anything
    is created.
    """
    if role in (HEAD_ROLE, OBSERVABILITY_ROLE):
        return
    name = role.removeprefix(WORKER_ROLE_PREFIX)
    if (
        not role.startswith(WORKER_ROLE_PREFIX)
        or not _ROLE_RE.fullmatch(name)
        or len(name) > MAX_WORKER_NAME
    ):
        raise ConfigError(
            f"invalid role '{role}': use naming.HEAD_ROLE, naming.OBSERVABILITY_ROLE "
            "or naming.worker_role(name)"
        )


def work_requirement_name(cluster: str, suffix: str) -> str:
    return f"{cluster}{SEPARATOR}{suffix}"


def pool_name(work_requirement_name: str, role: str) -> str:
    name = f"{work_requirement_name}{SEPARATOR}{role}"
    if len(name) > MAX_NAME:
        raise ConfigError(
            f"worker pool name '{name}' is {len(name)} characters; "
            f"the limit is {MAX_NAME}"
        )
    return name


def parse_work_requirement_name(name: str) -> tuple[str, str] | None:
    """``(cluster, suffix)`` if this is the name of a RayDog work requirement."""
    match = _WORK_REQUIREMENT_RE.fullmatch(name)
    return (match[1], match[2]) if match else None


def parse_pool_name(name: str) -> tuple[str, str, str] | None:
    """``(cluster, suffix, role)`` if this is the name of a RayDog worker pool."""
    match = _POOL_RE.fullmatch(name)
    return (match[1], match[2], match[3]) if match else None


def group_marker(
    cluster: str, role: str, settings: Mapping[str, int] | None = None
) -> str:
    """The tag for a task group. The head's also carries the cluster's settings."""
    marker = ":".join((MARKER_PREFIX, MARKER_VERSION, cluster, role))
    if settings:
        marker += ":" + ",".join(f"{k}={int(v)}" for k, v in sorted(settings.items()))
    return marker


def parse_group_marker(tag: str | None) -> tuple[str, str, dict[str, int]] | None:
    """``(cluster, role, settings)`` if the tag is a RayDog marker."""
    parts = (tag or "").split(":")
    if len(parts) not in (4, 5) or parts[0] != MARKER_PREFIX:
        return None
    if parts[1] != MARKER_VERSION:
        return None
    cluster, role = parts[2], parts[3]
    if not _CLUSTER_RE.fullmatch(cluster) or not _ROLE_RE.fullmatch(role):
        return None
    settings: dict[str, int] = {}
    if len(parts) == 5:
        for pair in parts[4].split(","):
            match = _SETTING.fullmatch(pair)
            if match is None or match[1] in settings:
                return None
            settings[match[1]] = int(match[2])
    return cluster, role, settings


def instance_tags(cluster: str, work_requirement_name: str) -> dict[str, str]:
    """Tags for a pool's instances: its ownership marker."""
    return {
        CLUSTER_INSTANCE_TAG: cluster,
        WORK_REQUIREMENT_INSTANCE_TAG: work_requirement_name,
    }


def pool_belongs(
    tags: Mapping[str, str], cluster: str, work_requirement_name: str
) -> bool:
    return (
        tags.get(CLUSTER_INSTANCE_TAG) == cluster
        and tags.get(WORK_REQUIREMENT_INSTANCE_TAG) == work_requirement_name
    )


def node_task_name(role: str) -> str:
    """A task name for a node: unique without any counter to keep in step."""
    return f"{role}-{_random(8)}"


def _random(length: int) -> str:
    return "".join(secrets.choice(_ALPHABET) for _ in range(length))
