"""Checks on the package as a whole."""

import importlib
import re
import subprocess
from pathlib import Path

import pytest

import yellowdog_ray

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_version_is_a_string() -> None:
    assert re.match(r"^\d+\.\d+", yellowdog_ray.__version__)


@pytest.mark.parametrize(
    "module",
    [
        "yellowdog_ray.autoscaler",
        "yellowdog_ray.builder",
        "yellowdog_ray.tunnels",
        "yellowdog_ray.cli",
    ],
)
def test_public_modules_import(module: str) -> None:
    importlib.import_module(module)


def _tracked_files() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files"], cwd=REPO_ROOT, capture_output=True, text=True, check=True
    )
    return [REPO_ROOT / line for line in out.stdout.splitlines() if line]


def test_no_private_keys_in_tree() -> None:
    offenders = []
    for path in _tracked_files():
        if path.name == "private-key":
            offenders.append(str(path))
            continue
        try:
            lines = path.read_text(errors="ignore").splitlines()
        except OSError:
            continue
        # A key file has the PEM header at the start of a line; prose that merely
        # mentions the header does not.
        if any(ln.startswith("-----BEGIN") and "PRIVATE KEY" in ln for ln in lines):
            offenders.append(str(path))
    assert offenders == []
