"""
Settings shared by the Builder examples, read from the environment.

Nothing here is specific to one YellowDog account: set these variables (in your
shell or in a .env file) before running an example.

Required:
    YD_API_KEY_ID, YD_API_KEY_SECRET   a YellowDog application key
    RAYDOG_NAMESPACE                   the YellowDog namespace to create things in
    RAYDOG_HEAD_TEMPLATE               compute requirement template for the head node
    RAYDOG_WORKER_TEMPLATE             compute requirement template for worker nodes
    RAYDOG_SSH_PRIVATE_KEY             path of the private key for SSH to the head;
                                       its public half (<path>.pub) is installed on
                                       the nodes by the setup script

Optional:
    YD_API_URL                         for a dedicated YellowDog platform
    RAYDOG_IMAGES_ID                   machine image, if the templates need one
    RAYDOG_SSH_USER                    defaults to yd-agent
    RAYDOG_CLUSTER_NAME                defaults to the example's own name
    RAYDOG_USE_CONTAINERS              set to 1 to run Ray in containers on the
                                       nodes (the rayproject/ray image) instead of
                                       installing it on them; see README.md
    RAYDOG_OBSERVABILITY               set to 1 for an observability node, with
                                       Prometheus, Loki and Grafana, collecting
                                       every node's metrics and logs (not with
                                       containers); see README.md
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

from yellowdog_ray import NodeSpec

SCRIPTS = Path(__file__).parent / "scripts"
# The container variant sets nodes up with the same script as the Autoscaler example
CONTAINER_SETUP = (
    Path(__file__).parent.parent / "autoscaler" / "scripts" / "node-setup-docker.sh"
)
PUBLIC_KEY_PLACEHOLDER = "REPLACE_WITH_YOUR_PUBLIC_KEY"
ALLOY_CONFIG_PLACEHOLDER = "REPLACE_WITH_ALLOY_CONFIG"
# type, base64 body, optional comment: nothing a shell would act on
PUBLIC_KEY = re.compile(r"[a-z0-9@.-]+ [A-Za-z0-9+/=]+( [A-Za-z0-9@._+-]+)?")


def _switched_on(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


def without_comments(script: str) -> str:
    """
    A script without its comment lines (shell and Alloy) and blank lines, but with
    its first line. EC2 takes at most 16 KB of userdata once base64-encoded, and
    instances given more do not boot; the comments are for people reading the file.
    """
    first, *rest = script.splitlines()
    kept = [
        line
        for line in rest
        if line.strip() and not line.lstrip().startswith(("#", "//"))
    ]
    return "\n".join([first, *kept]) + "\n"


@dataclass(frozen=True)
class Settings:
    namespace: str
    head: NodeSpec
    worker: NodeSpec
    observability: NodeSpec
    observability_enabled: bool  # RAYDOG_OBSERVABILITY
    ssh_user: str
    ssh_private_key: str
    cluster_name: str


def load(default_cluster_name: str) -> Settings:
    load_dotenv(find_dotenv(usecwd=True))

    def need(name: str) -> str:
        value = os.environ.get(name)
        if not value:
            raise SystemExit(
                f"Set {name} (see {Path(__file__).name} for the full list)"
            )
        return value

    private_key = Path(need("RAYDOG_SSH_PRIVATE_KEY")).expanduser()
    public_key = Path(f"{private_key}.pub")
    if not public_key.is_file():
        raise SystemExit(f"Public key not found: {public_key}")
    # The node setup script installs Ray and authorises this key for SSH. The key
    # is pasted into a script that runs as root, so it must look like a key.
    key_text = public_key.read_text().strip()
    if not PUBLIC_KEY.fullmatch(key_text):
        raise SystemExit(f"{public_key} does not look like an OpenSSH public key")
    containers = _switched_on("RAYDOG_USE_CONTAINERS")
    observability = _switched_on("RAYDOG_OBSERVABILITY")
    if containers and observability:
        # The container start scripts run no Alloy and export no metrics
        raise SystemExit(
            "RAYDOG_OBSERVABILITY is not available with RAYDOG_USE_CONTAINERS: "
            "the container start scripts send nothing to an observability node"
        )
    # The setup script for a node that runs Ray as installed, or (for the
    # observability node) Prometheus, Loki and Grafana
    setup = without_comments(
        (SCRIPTS / "node-setup-userdata.sh")
        .read_text()
        .replace(PUBLIC_KEY_PLACEHOLDER, key_text)
        .replace(ALLOY_CONFIG_PLACEHOLDER, (SCRIPTS / "config.alloy").read_text())
    )
    observability_setup = setup.replace(
        'OBSERVABILITY_NODE="no"', 'OBSERVABILITY_NODE="yes"', 1
    )
    if containers:
        # That script takes its settings from the lines put before it
        userdata = without_comments(
            f'#!/usr/bin/bash\nSSH_PUBLIC_KEY="{key_text}"\n'
            + CONTAINER_SETUP.read_text()
        )
    else:
        userdata = setup
    suffix = "-docker.sh" if containers else ".sh"
    images_id = os.environ.get("RAYDOG_IMAGES_ID") or None

    def node(template_variable: str, script: str, setup: str = userdata) -> NodeSpec:
        return NodeSpec(
            compute_requirement_template=need(template_variable),
            start_script=(SCRIPTS / script).read_text(),
            images_id=images_id,
            userdata=setup,
        )

    return Settings(
        namespace=need("RAYDOG_NAMESPACE"),
        head=node("RAYDOG_HEAD_TEMPLATE", f"head-node-task-script{suffix}"),
        worker=node("RAYDOG_WORKER_TEMPLATE", f"worker-node-task-script{suffix}"),
        # Runs no Ray of its own, so the same with or without containers
        observability=node(
            "RAYDOG_HEAD_TEMPLATE",
            "observability-node-task-script.sh",
            observability_setup,
        ),
        ssh_user=os.environ.get("RAYDOG_SSH_USER") or "yd-agent",
        ssh_private_key=str(private_key),
        cluster_name=os.environ.get("RAYDOG_CLUSTER_NAME") or default_cluster_name,
        observability_enabled=observability,
    )
