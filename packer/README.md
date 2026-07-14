# Building RayDog AMIs

This directory contains a [Packer](https://developer.hashicorp.com/packer) template that bakes an AWS AMI for RayDog cluster nodes, so that node boot-time userdata shrinks to per-node YellowDog agent configuration only.

## What's in the image

Built on the most recent official Canonical **Ubuntu 26.04 LTS** (amd64) AMI:

- The **YellowDog agent** (installed with default configuration; its service is stopped and its state wiped before imaging, so instances launched from the AMI register cleanly)
- The `yd-agent` user with passwordless sudo, and optionally an authorised SSH public key
- **uv**, plus a Python virtual environment at `/opt/yellowdog/agent/venv` containing `ray[default,client]` and `yellowdog-ray`
- **Grafana Alloy** (installed but disabled; enabled at runtime by observability-aware task scripts via `OBSERVABILITY_HOST`)
- Firewall (`ufw`) disabled; cloud-init reset so YellowDog userdata runs as first boot

## Prerequisites

- Packer ≥ 1.10 (`brew tap hashicorp/tap && brew install hashicorp/tap/packer`)
- AWS credentials in the environment (any mechanism the AWS SDK supports). The IAM principal needs the [minimal IAM policy for the `amazon-ebs` builder](https://developer.hashicorp.com/packer/integrations/hashicorp/amazon#iam-task-or-instance-role) — note this includes actions such as `ec2:CreateKeyPair`, `ec2:RunInstances`, `ec2:CreateImage`, and `ec2:CreateTags`, which ordinary developer policies often lack

## Usage

From the repository root:

```shell
make ami
```

Overridable variables (defaults shown):

```shell
make ami PYTHON_VERSION=3.12.11 \
         RAY_VERSION=2.56.0 \
         RAYDOG_VERSION= \
         AWS_REGION=eu-west-2 \
         SSH_PUBLIC_KEY="ssh-rsa AAAA… user@host" \
         SUBNET_ID=subnet-0123456789abcdef0 \
         EXTRA_PIP_PACKAGES="numpy==2.3.1 pandas"
```

- `RAYDOG_VERSION` empty means the latest `yellowdog-ray` from PyPI.
- `EXTRA_PIP_PACKAGES` bakes additional Python packages into the venv, for dependencies shared by most Ray jobs (heavy packages install much faster at bake time than via per-job `runtime_env`). Space-separated pip requirement specifiers; empty installs nothing extra.
- `SUBNET_ID` empty means the region's default VPC. If the build fails with `VPCIdNotSpecified: No default VPC for this user`, the account/region has no default VPC: set `SUBNET_ID` to a **public** subnet (one whose route table has an internet gateway) so Packer can SSH to the build instance, or create a default VPC once with `aws ec2 create-default-vpc`.
- `make ami-validate` runs `packer init` + `packer validate` only (no AWS spend).

The build launches a temporary `t3.large`, provisions it, snapshots it into an AMI named `raydog-ray<ray>-py<python>-<timestamp>`, terminates the instance, and prints the AMI ID. The AMI is tagged with the Python, Ray, and RayDog versions and the source AMI.

## Using the AMI with YellowDog

Point a YellowDog compute source/requirement template at the AMI ID (or add it to a Machine Image Family). Node userdata then only needs whatever YellowDog injects for agent configuration — the package-installation steps in `examples/*/scripts/` are no longer needed for these instances.
