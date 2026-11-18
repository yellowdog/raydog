#!/usr/bin/env python3
"""
Fill in the ${RAYDOG_...} settings of a cluster YAML template.

    python render.py raydog.yaml .rendered/raydog.yaml

Ray's YAML has no way to read the environment, so this does it: every
``${RAYDOG_NAME}`` is replaced by the value of that environment variable (a .env
file in the current directory is read too). Nothing else in the file is touched.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

DEFAULTS = {
    "RAYDOG_CLUSTER_NAME": "raydog-example",
    "RAYDOG_PIP_SPEC": "yellowdog-ray",
    "RAYDOG_IMAGES_ID": "",
    "RAYDOG_TAG": "",
    "RAYDOG_OBSERVABILITY": "",
}
SETTING = re.compile(r"\$\{(RAYDOG_[A-Z_]+)\}")
PUBLIC_KEY = re.compile(r"[a-z0-9@.-]+ [A-Za-z0-9+/=]+( [A-Za-z0-9@._+-]+)?")


def settings(env_file: Path = Path(".env")) -> dict[str, str]:
    values = dict(DEFAULTS)
    if env_file.is_file():
        for line in env_file.read_text().splitlines():
            name, sep, value = line.strip().partition("=")
            if sep and name.startswith("RAYDOG_"):
                values[name] = value.strip().strip("'\"")
    values.update({k: v for k, v in os.environ.items() if k.startswith("RAYDOG_")})
    key = values.get("RAYDOG_SSH_PRIVATE_KEY")
    if key and "RAYDOG_SSH_PUBLIC_KEY" not in values:
        public = Path(f"{Path(key).expanduser()}.pub")
        if public.is_file():
            values["RAYDOG_SSH_PUBLIC_KEY"] = public.read_text().strip()
    return values


def render(template: str, values: dict[str, str]) -> str:
    missing = sorted({name for name in SETTING.findall(template) if name not in values})
    if missing:
        raise SystemExit("Set these (see README.md): " + ", ".join(missing))
    public_key = values.get("RAYDOG_SSH_PUBLIC_KEY", "")
    if public_key and not PUBLIC_KEY.fullmatch(public_key):
        # It is pasted into a script that runs as root on every node
        raise SystemExit(
            "RAYDOG_SSH_PUBLIC_KEY does not look like an OpenSSH public key"
        )
    return SETTING.sub(lambda match: values[match.group(1)], template)


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    source, target = Path(sys.argv[1]), Path(sys.argv[2])
    rendered = render(source.read_text(), settings())
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(rendered)


if __name__ == "__main__":
    main()
