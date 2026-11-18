"""Saved cluster state: enough to find a cluster again, and nothing secret."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path

from yellowdog_ray.core.cluster import ClusterHandle
from yellowdog_ray.errors import ConfigError

STATE_VERSION = 1
_VERSION = "raydog_state_version"
_REQUIRED = ("cluster", "namespace", "work_requirement_id", "work_requirement_name")
API_URL = "api_url"


def dump(handle: ClusterHandle, api_url: str | None) -> str:
    state: dict[str, object] = {
        _VERSION: STATE_VERSION,
        "cluster": handle.settings.name,
        "namespace": handle.settings.namespace,
        "work_requirement_id": handle.work_requirement_id,
        "work_requirement_name": handle.work_requirement_name,
    }
    if api_url:
        state[API_URL] = api_url
    return json.dumps(state, indent=2)


def load(state: str | os.PathLike[str] | Mapping[str, object]) -> dict[str, str]:
    """Read state from a mapping, a JSON string, or the path of a JSON file."""
    data = _read(state)
    if not isinstance(data, dict):
        raise ConfigError("cluster state must be a JSON object")
    version = data.get(_VERSION)
    if type(version) is not int or version != STATE_VERSION:
        raise ConfigError(
            f"cluster state has version {version!r}; this RayDog reads version "
            f"{STATE_VERSION}"
        )
    result: dict[str, str] = {}
    for key in (*_REQUIRED, API_URL):
        value = data.get(key)
        if value is None and key == API_URL:
            continue
        if not isinstance(value, str) or not value:
            raise ConfigError(f"cluster state has no valid '{key}'")
        result[key] = value
    return result


def _read(state: object) -> object:
    if isinstance(state, Mapping):
        return dict(state)
    if isinstance(state, os.PathLike):
        return _parse(_read_file(Path(os.fspath(state))))
    if not isinstance(state, str):
        raise ConfigError(
            "cluster state must be a mapping, a JSON string or a file path, "
            f"not {type(state).__name__}"
        )
    # A string is JSON text if it parses as such; otherwise it names a file
    try:
        return json.loads(state)
    except ValueError:
        pass
    path = Path(state)
    try:
        is_file = path.is_file()
    except (OSError, ValueError):
        is_file = False
    if not is_file:
        raise ConfigError("cluster state is neither valid JSON nor the path of a file")
    return _parse(_read_file(path))


def _read_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as e:
        raise ConfigError(f"cannot read cluster state file {path}: {e}") from None


def _parse(text: str) -> object:
    try:
        return json.loads(text)
    except ValueError as e:
        raise ConfigError(f"the cluster state file is not valid JSON: {e}") from None
