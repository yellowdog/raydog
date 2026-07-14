#!/usr/bin/env bash

# Packer provisioning script for RayDog AMIs.
#
# Expected environment variables (set by the Packer shell provisioner):
#   PYTHON_VERSION   e.g. "3.12.11" (required)
#   RAY_VERSION      e.g. "2.56.0"  (required)
#   RAYDOG_VERSION   e.g. "2.0.1"; empty means latest from PyPI
#   SSH_PUBLIC_KEY   public key text to authorise for yd-agent; empty to skip
#   EXTRA_PIP_PACKAGES  space-separated pip requirement specifiers to add to
#                       the venv, e.g. "numpy==2.3.1 pandas"; empty to skip
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
# --seed installs pip into the venv, which Ray's `runtime_env={"pip": ...}`
# machinery requires (uv-created venvs otherwise have no pip module).
uv venv --seed --python "$PYTHON_VERSION" $VENV
source $VENV/bin/activate

if [[ -n "${RAYDOG_VERSION:-}" ]]; then
  RAYDOG_SPEC="yellowdog-ray==$RAYDOG_VERSION"
else
  RAYDOG_SPEC="yellowdog-ray"
fi
echo "Installing Ray v$RAY_VERSION and RayDog ($RAYDOG_SPEC)"
uv pip install "ray[default,client]==$RAY_VERSION" "$RAYDOG_SPEC"

if [[ -n "${EXTRA_PIP_PACKAGES:-}" ]]; then
  echo "Installing extra Python packages: $EXTRA_PIP_PACKAGES"
  # Intentionally unquoted: a space-separated list of requirement specifiers
  # shellcheck disable=SC2086
  uv pip install $EXTRA_PIP_PACKAGES
fi

echo "Extending .bashrc to activate the Ray environment"
echo "source $VENV/bin/activate" >> $YD_AGENT_HOME/.bashrc

################################################################################

echo "Installing Grafana Alloy (service left disabled)"
apt-get update
apt-get -y install gpg
mkdir -p /etc/apt/keyrings/
wget -q -O - https://apt.grafana.com/gpg.key | gpg --dearmor \
    > /etc/apt/keyrings/grafana.gpg
echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" \
    > /etc/apt/sources.list.d/grafana.list
apt-get update
apt-get -y install alloy
systemctl disable alloy

cat > /etc/alloy/config.alloy <<'EOF'
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
if systemctl is-active --quiet yd-agent.service; then
  echo "yd-agent service is still active after stop; aborting"
  exit 1
fi
if [[ -d /var/opt/yellowdog/agent/data ]]; then
  find /var/opt/yellowdog/agent/data -mindepth 1 -delete
fi
# The agent expects these directories to exist at startup (yda.paths.actions
# and yda.paths.workers) and does not recreate them itself.
install -d -o "$YD_AGENT_USER" -g "$YD_AGENT_USER" \
    /var/opt/yellowdog/agent/data/actions \
    /var/opt/yellowdog/agent/data/workers
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

# Remove host keys; cloud-init regenerates them on first boot of instances
# launched from this AMI.
rm -f /etc/ssh/ssh_host_*

# Remove the Packer build user's authorized keys (cloud-init re-adds the
# instance keypair on next launch).
rm -f /root/.ssh/authorized_keys /home/ubuntu/.ssh/authorized_keys

echo "Provisioning complete"
