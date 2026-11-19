#!/usr/bin/bash
# Sets up a fresh Ubuntu instance as a node for the RayDog Builder examples. Used as
# instance userdata: it runs once, as root, at first boot. settings.py fills in the
# public key, sets OBSERVABILITY for each kind of node, and adds the functions from
# observability/ that the node needs. Each node gets only those, because EC2 limits
# the size of userdata. For large or frequently started clusters, bake a machine
# image instead (see ../../../packer).
#
# The YellowDog Agent is installed after everything the node's start script needs,
# on purpose: the moment the Agent starts, the node is offered work. Only what needs
# the Agent's user to exist comes after it.
#
# Downloads, unpinned: the latest installers for uv and for the YellowDog Agent, and
# Grafana's packages. Pin versions in these URLs and package names if your nodes
# must be reproducible.

set -Eeuo pipefail

PYTHON_VERSION="3.12"
RAY_VERSION="2.60.0"          # must match the Ray on your own machine

# REQUIRED: the public SSH key to authorise for the yd-agent user, e.g. the
# contents of ~/.ssh/id_ed25519.pub. The examples connect to the head node with
# the matching private key.
SSH_PUBLIC_KEY="REPLACE_WITH_YOUR_PUBLIC_KEY"

# What this node does for observability (see RAYDOG_OBSERVABILITY in settings.py):
#   none   nothing
#   head   the head node, with Prometheus and Grafana for Ray's dashboard
#   alloy  a head or worker node that sends its metrics and logs to an
#          observability node, with Grafana Alloy
#   node   the observability node: Prometheus, Loki and Grafana are installed and
#          started here, so that they are ready before the node's task starts
OBSERVABILITY="none"

VENV=/opt/raydog/venv
LOG=/var/log/raydog-node-setup.log
YD_AGENT_USER="yd-agent"
YD_AGENT_HOME="/opt/yellowdog/agent"

# Say where it went wrong: this appears in the instance's console output
trap 'echo "raydog: node setup FAILED at line $LINENO; see $LOG on the instance" >&2' ERR

case "$SSH_PUBLIC_KEY" in
    ssh-*|ecdsa-*|sk-*) ;;
    *) echo "raydog: SSH_PUBLIC_KEY is not set; aborting node setup" >&2; exit 1 ;;
esac

echo "raydog: installing Python $PYTHON_VERSION and Ray $RAY_VERSION"
export HOME=/root DEBIAN_FRONTEND=noninteractive
export UV_INSTALL_DIR=/usr/local/bin UV_PYTHON_INSTALL_DIR=/opt/raydog/python
curl --fail -LsS --retry 5 https://astral.sh/uv/install.sh | sh >> "$LOG" 2>&1
# --seed puts pip in the environment, which Ray's runtime_env={"pip": ...} needs
uv venv --seed --python "$PYTHON_VERSION" "$VENV" >> "$LOG" 2>&1
uv pip install --python "$VENV/bin/python" "ray[default,client]==$RAY_VERSION" >> "$LOG" 2>&1
# The download cache is the size of Ray again, on what may be a small disk
uv cache clean >> "$LOG" 2>&1

################################################################################
# Observability. On the head and workers nothing depends on it, so a failure there
# is reported and the node carries on; on the observability node it is fatal.

add_grafana_repository() {
    [[ -f /etc/apt/sources.list.d/grafana.list ]] && return 0
    mkdir -p /etc/apt/keyrings &&
    curl --fail -LsS --retry 5 https://apt.grafana.com/gpg.key \
        | gpg --dearmor --yes -o /etc/apt/keyrings/grafana.gpg &&
    echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" \
        > /etc/apt/sources.list.d/grafana.list
}

# Installs packages, trying again for a few minutes: a package repository can fail
# for a while. `apt-get update` is allowed to fail, so that one broken repository on
# the image does not stop the rest.
apt_install() {
    for _ in 1 2 3 4 5; do
        apt-get update || true
        apt-get install -y "$@" && return 0
        sleep 20
    done
    return 1
}

# settings.py puts the functions this node needs, from observability/, in place of
# the next line
# REPLACE_WITH_OBSERVABILITY_FUNCTIONS

case "$OBSERVABILITY" in
    none) ;;
    head)
        echo "raydog: installing Prometheus and Grafana"
        setup_head_observability >> "$LOG" 2>&1 ||
            echo "raydog: Prometheus and Grafana could not be set up (see $LOG);" \
                 "Ray's dashboard will have no graphs" >&2
        ;;
    alloy)
        echo "raydog: installing Grafana Alloy"
        install_alloy >> "$LOG" 2>&1 ||
            echo "raydog: Grafana Alloy could not be installed (see $LOG); the" \
                 "observability node will have no data from this node" >&2
        ;;
    node) setup_observability ;;
    *) echo "raydog: OBSERVABILITY must be none, head, alloy or node" >&2; exit 1 ;;
esac

################################################################################

echo "raydog: installing the YellowDog Agent"
curl --fail -LsS --retry 5 \
    https://raw.githubusercontent.com/yellowdog/resources/refs/heads/main/agent-install/linux/yd-agent-installer.sh \
    | bash >> "$LOG" 2>&1

# For anyone who connects to a node over SSH: Ray is on the PATH, and `ray stop`
# cannot take Ray away from under the node's task. Written before the key is
# authorised, so that it is in place for the first connection.
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
chown "$YD_AGENT_USER:$YD_AGENT_USER" "$YD_AGENT_HOME/.bashrc"

if [[ -n "${GRAFANA_PASSWORD:-}" ]]; then
    # Grafana's admin password, for whoever can reach this node over SSH
    install -m 600 -o "$YD_AGENT_USER" -g "$YD_AGENT_USER" /dev/null \
        "$YD_AGENT_HOME/grafana-admin-password"
    printf '%s\n' "$GRAFANA_PASSWORD" > "$YD_AGENT_HOME/grafana-admin-password"
fi

echo "raydog: authorising the SSH key for $YD_AGENT_USER"
install -d -m 700 -o "$YD_AGENT_USER" -g "$YD_AGENT_USER" "$YD_AGENT_HOME/.ssh"
printf '%s\n' "$SSH_PUBLIC_KEY" >> "$YD_AGENT_HOME/.ssh/authorized_keys"
chmod 600 "$YD_AGENT_HOME/.ssh/authorized_keys"
chown "$YD_AGENT_USER:$YD_AGENT_USER" "$YD_AGENT_HOME/.ssh/authorized_keys"

chown -R "$YD_AGENT_USER:$YD_AGENT_USER" /opt/raydog
echo "raydog: node setup complete"
