# RayDog AMI Build Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A locally-runnable `make ami` target that uses Packer to bake an AWS AMI containing the YellowDog agent, uv, a Python venv with Ray + RayDog, and Grafana Alloy, on an Ubuntu 26.04 base.

**Architecture:** A `packer/` directory holds one `amazon-ebs` Packer template and one shell provisioning script (derived from the existing example userdata scripts, minus per-node concerns, plus AMI-hygiene cleanup). A Makefile target wraps `packer init` / `validate` / `build`. No CI; no YellowDog image-family registration — the build just outputs a tagged AMI ID.

**Tech Stack:** HashiCorp Packer (amazon-ebs builder), bash, GNU Make.

**Spec:** `docs/superpowers/specs/2026-07-14-raydog-ami-build-design.md`

## Global Constraints

- Base image: most recent official Canonical Ubuntu 26.04 LTS server AMI, amd64 (owner `099720109477`); wildcard the codename in the name filter.
- Default versions match the existing example scripts: `PYTHON_VERSION=3.12.11`, `RAY_VERSION=2.49.2`; `RAYDOG_VERSION` empty ⇒ latest from PyPI.
- No hard-coded SSH public keys — the key is an optional build variable.
- Provisioning script must use `set -euo pipefail` so any failure aborts the build (no half-baked AMIs).
- Alloy is installed but its service left disabled (matches current userdata behavior).
- The YellowDog agent is installed with the standard installer defaults (`YD_CONFIGURED_WP=FALSE`); its service is stopped and its data/logs wiped before imaging.
- AMI name: `raydog-ray{ray_version}-py{python_version}-{timestamp}`; tags carry Python/Ray/RayDog versions and source AMI.
- There is no automated test suite in this repo; the test cycle for each task is the stated validation command (`bash -n`, `shellcheck` if installed, `packer validate`, `aws ec2 describe-images`, and a final real build).
- Commit after each task, on the current branch (`next-version`). Do not commit the pre-existing unrelated `requirements.txt` modification.

---

### Task 1: Provisioning script

**Files:**
- Create: `packer/scripts/provision.sh`

**Interfaces:**
- Consumes: environment variables `PYTHON_VERSION`, `RAY_VERSION`, `RAYDOG_VERSION` (may be empty), `SSH_PUBLIC_KEY` (may be empty), provided by the Packer shell provisioner (Task 2). Runs as root.
- Produces: a fully provisioned filesystem ready for imaging: YD agent installed (service stopped, state wiped), venv at `/opt/yellowdog/agent/venv` with Ray + RayDog, Alloy installed & disabled with config at `/etc/alloy/config.alloy`, cloud-init cleaned.

- [ ] **Step 1: Write the script**

Create `packer/scripts/provision.sh` with exactly this content:

```bash
#!/usr/bin/env bash

# Packer provisioning script for RayDog AMIs.
#
# Expected environment variables (set by the Packer shell provisioner):
#   PYTHON_VERSION   e.g. "3.12.11" (required)
#   RAY_VERSION      e.g. "2.49.2"  (required)
#   RAYDOG_VERSION   e.g. "2.0.1"; empty means latest from PyPI
#   SSH_PUBLIC_KEY   public key text to authorise for yd-agent; empty to skip
#
# Runs as root.

set -euo pipefail

echo "Waiting for cloud-init to complete on the build instance"
cloud-init status --wait

export DEBIAN_FRONTEND=noninteractive

################################################################################

echo "Installing the YellowDog agent"
cd /root
curl -LsSf https://raw.githubusercontent.com/yellowdog/resources/refs/heads/main/agent-install/linux/yd-agent-installer.sh \
    | bash

YD_AGENT_USER="yd-agent"
YD_AGENT_HOME="/opt/yellowdog/agent"

################################################################################

echo "Adding $YD_AGENT_USER to passwordless sudoers"
usermod -aG sudo $YD_AGENT_USER
echo -e "$YD_AGENT_USER\tALL=(ALL)\tNOPASSWD: ALL" > \
    /etc/sudoers.d/020-$YD_AGENT_USER

################################################################################

if [[ -n "${SSH_PUBLIC_KEY:-}" ]]; then
  echo "Adding public SSH key for $YD_AGENT_USER"
  mkdir -p $YD_AGENT_HOME/.ssh
  chmod og-rwx $YD_AGENT_HOME/.ssh
  echo "$SSH_PUBLIC_KEY" >> $YD_AGENT_HOME/.ssh/authorized_keys
  chmod og-rw $YD_AGENT_HOME/.ssh/authorized_keys
  chown -R $YD_AGENT_USER:$YD_AGENT_USER $YD_AGENT_HOME/.ssh
else
  echo "SSH_PUBLIC_KEY not set; skipping SSH key installation"
fi

################################################################################

echo "Installing 'uv'"
export HOME=$YD_AGENT_HOME
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

echo "Installing Python v$PYTHON_VERSION and creating the Python virtual environment"
VENV=$YD_AGENT_HOME/venv
uv venv --python "$PYTHON_VERSION" $VENV
source $VENV/bin/activate

if [[ -n "${RAYDOG_VERSION:-}" ]]; then
  RAYDOG_SPEC="yellowdog-ray==$RAYDOG_VERSION"
else
  RAYDOG_SPEC="yellowdog-ray"
fi
echo "Installing Ray v$RAY_VERSION and RayDog ($RAYDOG_SPEC)"
uv pip install "ray[default,client]==$RAY_VERSION" "$RAYDOG_SPEC"

echo "Extending .bashrc to activate the Ray environment"
echo "source $VENV/bin/activate" >> $YD_AGENT_HOME/.bashrc

################################################################################

echo "Installing Grafana Alloy (service left disabled)"
apt-get -y install gpg
mkdir -p /etc/apt/keyrings/
wget -q -O - https://apt.grafana.com/gpg.key | gpg --dearmor \
    > /etc/apt/keyrings/grafana.gpg
echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" \
    > /etc/apt/sources.list.d/grafana.list
apt-get update
apt-get -y install alloy
systemctl disable alloy

cat > /etc/alloy/config.alloy <<EOF
local.file_match "ray" {
    path_targets = [{"__path__" = "/tmp/ray/session_latest/logs/*", job="ray"}]
}

loki.source.file "ray" {
    targets       = local.file_match.ray.targets
    forward_to    = [loki.write.loki.receiver]
    tail_from_end = true
}

local.file_match "worker_output" {
    path_targets = [{"__path__" = "/var/opt/yellowdog/agent/data/workers/*/taskoutput.txt", job="yellowdog_task_output"}]
}

loki.source.file "worker_output" {
    targets       = local.file_match.worker_output.targets
    forward_to    = [loki.write.loki.receiver]
    tail_from_end = true
}

loki.source.journal "journal" {
  max_age       = "24h0m0s"
  relabel_rules = discovery.relabel.logs_integrations_integrations_node_exporter_journal_scrape.rules
  forward_to    = [loki.write.loki.receiver]
}

loki.write "loki" {
	endpoint {
		url = string.format(
			"http://%s:3100/loki/api/v1/push",
			coalesce(sys.env("OBSERVABILITY_HOST"), "127.0.0.1"),
		)
	}
}

prometheus.exporter.self "alloy" {}

prometheus.scrape "ray" {
    targets    = [{__address__ = "127.0.0.1:10002", instance = constants.hostname, job = "ray" }]
    forward_to = [prometheus.remote_write.mimir.receiver]
}

prometheus.exporter.unix "integrations_node_exporter" {
  disable_collectors = ["ipvs", "btrfs", "infiniband", "xfs", "zfs"]
  enable_collectors = ["meminfo"]

  filesystem {
    fs_types_exclude     = "^(autofs|binfmt_misc|bpf|cgroup2?|configfs|debugfs|devpts|devtmpfs|tmpfs|fusectl|hugetlbfs|iso9660|mqueue|nsfs|overlay|proc|procfs|pstore|rpc_pipefs|securityfs|selinuxfs|squashfs|sysfs|tracefs)$"
    mount_points_exclude = "^/(dev|proc|run/credentials/.+|sys|var/lib/docker/.+)($|/)"
    mount_timeout        = "5s"
  }

  netclass {
    ignored_devices = "^(veth.*|cali.*|[a-f0-9]{15})$"
  }

  netdev {
    device_exclude = "^(veth.*|cali.*|[a-f0-9]{15})$"
  }
}

discovery.relabel "integrations_node_exporter" {
  targets = prometheus.exporter.unix.integrations_node_exporter.targets

  rule {
    target_label = "instance"
    replacement  = constants.hostname
  }

  rule {
    target_label = "job"
    replacement = "integrations/node_exporter"
  }
}

discovery.relabel "logs_integrations_integrations_node_exporter_journal_scrape" {
  targets = []

  rule {
    target_label = "instance"
    replacement  = constants.hostname
  }
  rule {
    source_labels = ["__journal__systemd_unit"]
    target_label  = "unit"
  }

  rule {
    source_labels = ["__journal__boot_id"]
    target_label  = "boot_id"
  }

  rule {
    source_labels = ["__journal__transport"]
    target_label  = "transport"
  }

  rule {
    source_labels = ["__journal_priority_keyword"]
    target_label  = "level"
  }
}

prometheus.scrape "integrations_node_exporter" {
  scrape_interval = "15s"
  targets    = discovery.relabel.integrations_node_exporter.output
  forward_to = [prometheus.remote_write.mimir.receiver]
}

prometheus.scrape "alloy" {
	targets    = prometheus.exporter.self.alloy.targets
	forward_to = [prometheus.remote_write.mimir.receiver]
}

prometheus.remote_write "mimir" {
	endpoint {
		url = string.format(
			"http://%s:9090/api/v1/write",
			coalesce(sys.env("OBSERVABILITY_HOST"), "127.0.0.1"),
		)
	}
}
EOF
chmod 755 /etc/alloy /etc/alloy/config.alloy

################################################################################

echo "Disabling firewall"
ufw disable

echo "Setting file/directory ownership to $YD_AGENT_USER"
chown -R $YD_AGENT_USER:$YD_AGENT_USER $YD_AGENT_HOME

################################################################################

echo "Cleaning up for imaging"

# The installer starts the agent; stop it and remove any state it created
# so instances launched from the AMI start fresh.
systemctl stop yd-agent.service || true
rm -rf /var/opt/yellowdog/agent/data/* || true
journalctl --rotate --vacuum-time=1s || true

apt-get clean
rm -rf /var/lib/apt/lists/*

# Reset cloud-init so YellowDog-injected userdata runs as first boot on
# instances launched from this AMI.
cloud-init clean --logs

# Regenerate machine identity on next boot.
truncate -s 0 /etc/machine-id
rm -f /var/lib/dbus/machine-id
ln -s /etc/machine-id /var/lib/dbus/machine-id

# Remove the Packer build user's authorized keys (cloud-init re-adds the
# instance keypair on next launch).
rm -f /root/.ssh/authorized_keys /home/ubuntu/.ssh/authorized_keys

echo "Provisioning complete"
```

- [ ] **Step 2: Syntax-check the script**

Run: `bash -n packer/scripts/provision.sh`
Expected: no output, exit code 0.

Run: `command -v shellcheck >/dev/null && shellcheck packer/scripts/provision.sh || echo "shellcheck not installed, skipping"`
Expected: no errors (SC2086-style info/style warnings about unquoted `$YD_AGENT_HOME`/`$VENV` are acceptable — they match the style of the existing example scripts and the values contain no spaces).

- [ ] **Step 3: Make executable and commit**

```bash
chmod +x packer/scripts/provision.sh
git add packer/scripts/provision.sh
git commit -m "Add Packer provisioning script for RayDog AMIs"
```

---

### Task 2: Packer template

**Files:**
- Create: `packer/raydog-ami.pkr.hcl`

**Interfaces:**
- Consumes: `packer/scripts/provision.sh` (Task 1) via a `shell` provisioner, passing `PYTHON_VERSION`, `RAY_VERSION`, `RAYDOG_VERSION`, `SSH_PUBLIC_KEY` as environment variables.
- Produces: input variables `python_version`, `ray_version`, `raydog_version`, `aws_region`, `instance_type`, `ssh_public_key`, `volume_size_gb` — the exact names the Makefile (Task 3) passes with `-var`.

- [ ] **Step 1: Check Packer is installed**

Run: `packer version || brew install packer`
Expected: `Packer v1.x.y` (any version ≥ 1.10).

Note: if Homebrew reports that HashiCorp Packer requires their tap, use `brew tap hashicorp/tap && brew install hashicorp/tap/packer`.

- [ ] **Step 2: Write the template**

Create `packer/raydog-ami.pkr.hcl` with exactly this content:

```hcl
# Packer template for building RayDog node AMIs.
#
# Bakes the YellowDog agent, uv, a Python venv containing Ray and RayDog,
# and Grafana Alloy (disabled) into an Ubuntu 26.04 LTS base image, so that
# boot-time userdata is reduced to per-node YellowDog agent configuration.
#
# Usage (via the Makefile at the repository root):
#   make ami [PYTHON_VERSION=…] [RAY_VERSION=…] [RAYDOG_VERSION=…]
#            [AWS_REGION=…] [SSH_PUBLIC_KEY="ssh-rsa …"]

packer {
  required_version = ">= 1.10.0"
  required_plugins {
    amazon = {
      source  = "github.com/hashicorp/amazon"
      version = ">= 1.3.0"
    }
  }
}

variable "python_version" {
  type        = string
  default     = "3.12.11"
  description = "Python version to install in the venv"
}

variable "ray_version" {
  type        = string
  default     = "2.49.2"
  description = "Ray version to install"
}

variable "raydog_version" {
  type        = string
  default     = ""
  description = "yellowdog-ray version to install; empty means latest from PyPI"
}

variable "aws_region" {
  type        = string
  default     = "eu-west-2"
  description = "AWS region in which to build and register the AMI"
}

variable "instance_type" {
  type        = string
  default     = "t3.large"
  description = "EC2 instance type for the temporary build instance"
}

variable "ssh_public_key" {
  type        = string
  default     = ""
  description = "Optional public key text to authorise for the yd-agent user"
}

variable "volume_size_gb" {
  type        = number
  default     = 32
  description = "Root EBS volume size for the AMI, in GB"
}

locals {
  timestamp = regex_replace(timestamp(), "[- TZ:]", "")
  ami_name  = "raydog-ray${var.ray_version}-py${var.python_version}-${local.timestamp}"
}

source "amazon-ebs" "raydog" {
  region        = var.aws_region
  instance_type = var.instance_type
  ssh_username  = "ubuntu"
  ami_name      = local.ami_name

  source_ami_filter {
    filters = {
      # Codename is wildcarded so the filter is robust to Canonical's
      # release-name string for 26.04.
      name                = "ubuntu/images/hvm-ssd-gp3/ubuntu-*-26.04-amd64-server-*"
      root-device-type    = "ebs"
      virtualization-type = "hvm"
    }
    owners      = ["099720109477"] # Canonical
    most_recent = true
  }

  launch_block_device_mappings {
    device_name           = "/dev/sda1"
    volume_size           = var.volume_size_gb
    volume_type           = "gp3"
    delete_on_termination = true
  }

  tags = {
    Name          = local.ami_name
    Project       = "raydog"
    PythonVersion = var.python_version
    RayVersion    = var.ray_version
    RayDogVersion = var.raydog_version == "" ? "latest" : var.raydog_version
    SourceAMI     = "{{ .SourceAMI }}"
    SourceAMIName = "{{ .SourceAMIName }}"
    BuiltBy       = "packer"
  }
}

build {
  sources = ["source.amazon-ebs.raydog"]

  provisioner "shell" {
    script          = "${path.root}/scripts/provision.sh"
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E bash '{{ .Path }}'"
    environment_vars = [
      "PYTHON_VERSION=${var.python_version}",
      "RAY_VERSION=${var.ray_version}",
      "RAYDOG_VERSION=${var.raydog_version}",
      "SSH_PUBLIC_KEY=${var.ssh_public_key}",
    ]
  }
}
```

- [ ] **Step 3: Validate the template**

```bash
cd packer && packer init . && packer fmt -check . && packer validate .
```

Expected: `packer init` downloads the amazon plugin on first run; `packer fmt -check` produces no output (exit 0); `packer validate` prints `The configuration is valid.`

If `packer fmt -check` fails, run `packer fmt .` and inspect the diff — formatting only, no content changes.

- [ ] **Step 4: Commit**

```bash
git add packer/raydog-ami.pkr.hcl
git commit -m "Add Packer template for RayDog AMIs"
```

---

### Task 3: Makefile target

**Files:**
- Modify: `Makefile` (variables near the top, after line 10 `VERSION := …`; targets before the `no_op` target at the end; update the `no_op` help comment)

**Interfaces:**
- Consumes: variable names `python_version`, `ray_version`, `raydog_version`, `aws_region`, `ssh_public_key` from `packer/raydog-ami.pkr.hcl` (Task 2).
- Produces: `make ami` and `make ami-validate` targets with overridable `PYTHON_VERSION`, `RAY_VERSION`, `RAYDOG_VERSION`, `AWS_REGION`, `SSH_PUBLIC_KEY` make variables.

- [ ] **Step 1: Add variables and targets**

After the `VERSION := …` line in `Makefile`, add:

```make
# AMI build settings (see packer/README.md)
PYTHON_VERSION ?= 3.12.11
RAY_VERSION ?= 2.49.2
RAYDOG_VERSION ?=
AWS_REGION ?= eu-west-2
SSH_PUBLIC_KEY ?=

PACKER_VARS = -var "python_version=$(PYTHON_VERSION)" \
              -var "ray_version=$(RAY_VERSION)" \
              -var "raydog_version=$(RAYDOG_VERSION)" \
              -var "aws_region=$(AWS_REGION)" \
              -var "ssh_public_key=$(SSH_PUBLIC_KEY)"
```

Before the `no_op` target, add:

```make
ami-validate:
	cd packer && packer init . && packer validate $(PACKER_VARS) .

ami: ami-validate
	cd packer && packer build $(PACKER_VARS) .
```

Update the `no_op` help comment to include `ami` and `ami-validate` in the listed targets:

```make
no_op:
	# Available targets are: build, clean, install, uninstall, format, update, docs,
	# docs-build-image, docs-publish-image, pypi-check-build, stubs,
	# pypi-test-upload, pypi-prod-upload, ami, ami-validate
```

- [ ] **Step 2: Test the validate target**

Run: `make ami-validate`
Expected: `The configuration is valid.` and exit code 0.

Run: `make ami-validate RAY_VERSION=2.50.0 RAYDOG_VERSION=2.0.1`
Expected: same success (variables pass through cleanly).

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "Add 'ami' and 'ami-validate' Makefile targets"
```

---

### Task 4: Documentation

**Files:**
- Create: `packer/README.md`
- Modify: `README.md` (add a short section pointing at `packer/`)

**Interfaces:**
- Consumes: variable and target names exactly as defined in Tasks 2 and 3.
- Produces: user-facing docs only; nothing depends on this downstream.

- [ ] **Step 1: Write packer/README.md**

Create `packer/README.md` with exactly this content:

````markdown
# Building RayDog AMIs

This directory contains a [Packer](https://developer.hashicorp.com/packer)
template that bakes an AWS AMI for RayDog cluster nodes, so that node
boot-time userdata shrinks to per-node YellowDog agent configuration only.

## What's in the image

Built on the most recent official Canonical **Ubuntu 26.04 LTS** (amd64) AMI:

- The **YellowDog agent** (installed with default configuration; its service
  is stopped and its state wiped before imaging, so instances launched from
  the AMI register cleanly)
- The `yd-agent` user with passwordless sudo, and optionally an authorised
  SSH public key
- **uv**, plus a Python virtual environment at `/opt/yellowdog/agent/venv`
  containing `ray[default,client]` and `yellowdog-ray`
- **Grafana Alloy** (installed but disabled; enabled at runtime by
  observability-aware task scripts via `OBSERVABILITY_HOST`)
- Firewall (`ufw`) disabled; cloud-init reset so YellowDog userdata runs as
  first boot

## Prerequisites

- Packer ≥ 1.10 (`brew tap hashicorp/tap && brew install hashicorp/tap/packer`)
- AWS credentials in the environment (any mechanism the AWS SDK supports)
  with permission to launch instances, create snapshots/AMIs, and tag
  resources in the target region

## Usage

From the repository root:

```shell
make ami
```

Overridable variables (defaults shown):

```shell
make ami PYTHON_VERSION=3.12.11 \
         RAY_VERSION=2.49.2 \
         RAYDOG_VERSION= \
         AWS_REGION=eu-west-2 \
         SSH_PUBLIC_KEY="ssh-rsa AAAA… user@host"
```

- `RAYDOG_VERSION` empty means the latest `yellowdog-ray` from PyPI.
- `make ami-validate` runs `packer init` + `packer validate` only (no AWS
  spend).

The build launches a temporary `t3.large`, provisions it, snapshots it into
an AMI named `raydog-ray<ray>-py<python>-<timestamp>`, terminates the
instance, and prints the AMI ID. The AMI is tagged with the Python, Ray, and
RayDog versions and the source AMI.

## Using the AMI with YellowDog

Point a YellowDog compute source/requirement template at the AMI ID (or add
it to a Machine Image Family). Node userdata then only needs whatever
YellowDog injects for agent configuration — the package-installation steps
in `examples/*/scripts/` are no longer needed for these instances.
````

- [ ] **Step 2: Add a note to the main README**

In `README.md`, after the introductory section (read the file first and place this where the surrounding structure fits best, e.g. after the installation section), add:

```markdown
## Building node machine images

The `packer/` directory contains a Packer template for baking AWS AMIs with
the YellowDog agent, Python, Ray, and RayDog pre-installed, which
substantially reduces node boot time compared with installing everything via
userdata. See [packer/README.md](packer/README.md).
```

- [ ] **Step 3: Commit**

```bash
git add packer/README.md README.md
git commit -m "Document the AMI build process"
```

---

### Task 5: End-to-end verification (requires AWS credentials; incurs small EC2 cost)

**Files:** none created; this task validates Tasks 1–3 against real AWS.

**Interfaces:**
- Consumes: `make ami` (Task 3), AWS credentials in the environment.
- Produces: a real AMI ID and confirmation the pipeline works; findings feed fixes to earlier tasks if anything fails.

- [ ] **Step 1: Confirm the base-AMI filter resolves**

```bash
aws ec2 describe-images --region "${AWS_REGION:-eu-west-2}" \
  --owners 099720109477 \
  --filters "Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-*-26.04-amd64-server-*" \
  --query 'sort_by(Images,&CreationDate)[-1].{Name:Name,ImageId:ImageId}'
```

Expected: one JSON object with a 26.04 image name and an `ami-…` ID. If this returns null, adjust the name filter (try dropping `-gp3` from the path: `ubuntu/images/hvm-ssd*/ubuntu-*-26.04-amd64-server-*`) in both this command and `packer/raydog-ami.pkr.hcl`, and re-run `make ami-validate`.

- [ ] **Step 2: Run the full build**

Run: `make ami`
Expected: Packer launches the build instance, streams the provisioning log (agent install, uv/Python/Ray/RayDog install, Alloy install, cleanup), creates the AMI, terminates the instance, and finishes with a line like:

```
--> amazon-ebs.raydog: AMIs were created:
eu-west-2: ami-0123456789abcdef0
```

This takes roughly 10–20 minutes. If the provisioning script fails, Packer aborts and terminates the instance without creating an AMI — fix the script (Task 1), commit, and re-run.

- [ ] **Step 3: Verify the AMI and cleanup**

```bash
aws ec2 describe-images --region "${AWS_REGION:-eu-west-2}" --owners self \
  --filters "Name=tag:Project,Values=raydog" \
  --query 'Images[].{Name:Name,Id:ImageId,Tags:Tags}'
```

Expected: the new AMI with its version tags.

```bash
aws ec2 describe-instances --region "${AWS_REGION:-eu-west-2}" \
  --filters "Name=tag:Name,Values=Packer Builder" \
            "Name=instance-state-name,Values=running,pending" \
  --query 'Reservations[].Instances[].InstanceId'
```

Expected: `[]` (no leftover build instances).

- [ ] **Step 4: Cluster smoke test (manual, optional but recommended)**

Point a YellowDog compute template at the new AMI ID, reduce the worker-pool userdata to agent-config-only, and run the autoscaler example (`examples/autoscaler/up.sh`). Confirm nodes register with YellowDog and the Ray cluster forms. Record the observed node-ready time vs. the full-userdata baseline in the PR/commit message.

- [ ] **Step 4b: Failure-path check (optional; incurs a partial build's EC2 cost)**

Run: `make ami RAY_VERSION=9.99.99`
Expected: the provisioning script fails at the `uv pip install` step (no such Ray version), Packer aborts, terminates the build instance, and reports the error; re-run the Step 3 queries to confirm no new AMI and no leftover instance.

- [ ] **Step 5: Commit any fixes made during verification**

```bash
git status
# commit any adjustments (e.g. AMI filter tweak) made in Steps 1-4
```
