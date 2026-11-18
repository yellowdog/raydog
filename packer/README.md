# Building RayDog AMIs

This directory contains a [Packer](https://developer.hashicorp.com/packer) template that bakes an AWS AMI for RayDog cluster nodes, so that node boot-time userdata shrinks to per-node YellowDog agent configuration only.

## What's in the image

Built on the most recent official Canonical **Ubuntu 26.04 LTS** (amd64) AMI:

- The **YellowDog agent** (installed with default configuration; its service is stopped and its state wiped before imaging, so instances launched from the AMI register cleanly)
- Optionally, an authorised SSH public key for the `yd-agent` user, and the same `.bashrc` as the example setup scripts write: Ray on the `PATH`, and `ray stop` ignored, because a node is stopped by ending its task
- **uv**, plus a Python virtual environment at `/opt/yellowdog/agent/venv` containing `ray[default,client]` and `yellowdog-ray`
- **Grafana Alloy**, with the configuration in `examples/builder/scripts/config.alloy` (installed but disabled; the Builder example's start scripts run it when the cluster has an observability node, which installs Prometheus, Loki and Grafana for itself at boot)
- Firewall (`ufw`) disabled, since Ray uses many ports between nodes and the security group controls access; cloud-init reset so YellowDog userdata runs as first boot

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
         RAY_VERSION=2.60.0 \
         RAYDOG_VERSION= \
         AWS_REGION=eu-west-2 \
         SSH_PUBLIC_KEY="ssh-rsa AAAA… user@host" \
         SUBNET_ID=subnet-0123456789abcdef0 \
         EXTRA_PIP_PACKAGES="numpy==2.3.1 pandas"
```

- `RAYDOG_VERSION` empty means the latest `yellowdog-ray` version 3 from PyPI (the build fails if there is none yet: version 2 is never installed by default).
- `RAYDOG_PIP_SPEC` installs RayDog from any pip requirement in place of `RAYDOG_VERSION`, for example an unreleased branch: `RAYDOG_PIP_SPEC="yellowdog-ray @ git+https://github.com/yellowdog/raydog@next-version"`.
- `EXTRA_PIP_PACKAGES` bakes additional Python packages into the venv, for dependencies shared by most Ray jobs (heavy packages install much faster at bake time than via per-job `runtime_env`). Space-separated pip requirement specifiers; empty installs nothing extra.
- `SUBNET_ID` empty means the region's default VPC. If the build fails with `VPCIdNotSpecified: No default VPC for this user`, the account/region has no default VPC: set `SUBNET_ID` to a **public** subnet (one whose route table has an internet gateway) so Packer can SSH to the build instance, or create a default VPC once with `aws ec2 create-default-vpc`.
- `make ami-validate` runs `packer init` + `packer validate` only (no AWS spend).
- `scripts/provision.sh` is meant to be run by the template, on an instance that is about to be imaged. It ends by removing the SSH host keys and the login keys of `root` and `ubuntu`, so an instance it is run on by hand cannot be reached over SSH afterwards. It needs Alloy's configuration, which the template uploads to `/tmp/config.alloy`; run any other way, copy `examples/builder/scripts/config.alloy` across and set `ALLOY_CONFIG` to its path. It checks its inputs before installing anything.

The build launches a temporary `t3.large`, provisions it, snapshots it into an AMI named `raydog-ray<ray>-py<python>-<timestamp>`, terminates the instance, and prints the AMI ID. The AMI is tagged with the Python, Ray, and RayDog versions and the source AMI.

## Using the AMI with YellowDog

Add the AMI to a YellowDog image family and name the family as `images_id` (`namespace/name`), or point a template at the AMI ID directly. The AMI must be launchable from the AWS account the compute sources use: share it with that account if it was built in another. The image has the YellowDog Agent, Python, Ray and RayDog installed, with the Python environment where the example start scripts expect it (`/opt/raydog/venv`), so the node setup scripts in `examples/*/scripts/` are not needed with it. Userdata is then only needed for what differs between clusters: for the Autoscaler, and for the Builder examples' tunnels, that is authorising your SSH public key for the `yd-agent` user.
