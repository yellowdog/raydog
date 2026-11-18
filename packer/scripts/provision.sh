#!/usr/bin/env bash

# Packer provisioning script for RayDog AMIs.
#
# Expected environment variables (set by the Packer shell provisioner):
#   PYTHON_VERSION   e.g. "3.12.11" (required)
#   RAY_VERSION      e.g. "2.60.0"  (required)
#   RAYDOG_VERSION   e.g. "3.0.0"; empty means the latest version 3 from PyPI
#   RAYDOG_PIP_SPEC  a pip requirement for RayDog in place of RAYDOG_VERSION, e.g.
#                    "yellowdog-ray @ git+https://github.com/yellowdog/raydog@next-version";
#                    empty to use RAYDOG_VERSION
#   SSH_PUBLIC_KEY   public key text to authorise for yd-agent; empty to skip
#   EXTRA_PIP_PACKAGES  space-separated pip requirement specifiers to add to
#                       the venv, e.g. "numpy==2.3.1 pandas"; empty to skip
#   ALLOY_CONFIG     where Alloy's configuration is; default /tmp/config.alloy,
#                    where the template uploads examples/builder/scripts/config.alloy
#
# Runs as root, on an instance that is about to be imaged: it ends by removing the
# SSH host keys and the login keys of root and ubuntu, so an instance it is run on
# by hand cannot be reached over SSH afterwards.
#
# Downloads, unpinned: the latest installers for the YellowDog Agent and uv, and
# Grafana Alloy. Pin versions in these URLs and package names for reproducible
# images.

set -euo pipefail

# Checked first, rather than after several minutes of installing
ALLOY_CONFIG="${ALLOY_CONFIG:-/tmp/config.alloy}"
for name in PYTHON_VERSION RAY_VERSION; do
  if [[ -z "${!name:-}" ]]; then
    echo "provision.sh: $name must be set" >&2
    exit 1
  fi
done
if [[ ! -f "$ALLOY_CONFIG" ]]; then
  echo "provision.sh: Alloy's configuration is not at $ALLOY_CONFIG. The Packer" \
       "template uploads it; to run this script another way, copy" \
       "examples/builder/scripts/config.alloy to the instance and set ALLOY_CONFIG" \
       "to its path." >&2
  exit 1
fi

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

if [[ -n "${RAYDOG_PIP_SPEC:-}" ]]; then
  RAYDOG_SPEC="$RAYDOG_PIP_SPEC"
elif [[ -n "${RAYDOG_VERSION:-}" ]]; then
  RAYDOG_SPEC="yellowdog-ray==$RAYDOG_VERSION"
else
  # Version 2 has a different layout on the node and is not what this script
  # sets up, so it is never picked up by default
  RAYDOG_SPEC="yellowdog-ray>=3"
fi
echo "Installing Ray v$RAY_VERSION and RayDog ($RAYDOG_SPEC)"
uv pip install "ray[default,client]==$RAY_VERSION" "$RAYDOG_SPEC"

if [[ -n "${EXTRA_PIP_PACKAGES:-}" ]]; then
  echo "Installing extra Python packages: $EXTRA_PIP_PACKAGES"
  # Intentionally unquoted: a space-separated list of requirement specifiers
  # shellcheck disable=SC2086
  uv pip install $EXTRA_PIP_PACKAGES
fi

# The RayDog example start scripts look for the Python environment here
mkdir -p /opt/raydog
ln -sfn "$VENV" /opt/raydog/venv

# The same .bashrc as the example setup scripts write. Ray's cluster launcher runs
# its commands on a node over SSH, in a shell that reads this file: `ray exec
# cluster.yaml 'ray status'`, and the `ray stop` with which `ray down` begins.
echo "Writing .bashrc: Ray on the PATH, and 'ray stop' ignored"
cat > "$YD_AGENT_HOME/.bashrc" << 'BASHRC'
# Written by RayDog's node setup.
export PATH="/opt/raydog/venv/bin:$PATH"

# A RayDog node is stopped by ending its task, which stops Ray as it goes. Stopping
# Ray from here would end the task as a failure, so `ray stop` does nothing.
ray() {
    if [ "${1:-}" = "stop" ]; then
        echo "raydog: this node stops when its task ends; 'ray stop' ignored"
    else
        command ray "$@"
    fi
}
BASHRC

################################################################################

if [[ -n "${SSH_PUBLIC_KEY:-}" ]]; then
  echo "Adding public SSH key for $YD_AGENT_USER"
  install -d -m 700 "$YD_AGENT_HOME/.ssh"
  printf '%s\n' "$SSH_PUBLIC_KEY" >> "$YD_AGENT_HOME/.ssh/authorized_keys"
  chmod 600 "$YD_AGENT_HOME/.ssh/authorized_keys"
else
  echo "SSH_PUBLIC_KEY not set; skipping SSH key installation"
fi

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

install -d -m 755 /etc/alloy
install -m 644 "$ALLOY_CONFIG" /etc/alloy/config.alloy

################################################################################

# Ray uses many ports between the nodes of a cluster, so access is controlled by
# the instances' security group, not on the host. Ubuntu's cloud images ship with
# ufw off; this makes sure.
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
