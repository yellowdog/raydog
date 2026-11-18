# Sets up a fresh Ubuntu instance as a Ray node for RayDog. Used as instance
# userdata (it runs once, as root, at first boot), after a few lines of settings:
# see raydog.yaml. For large or frequently started clusters, bake a machine image
# instead (see ../../packer) and drop this script.
#
# The YellowDog Agent is installed last, on purpose: the moment it starts, the node
# is offered work, so everything a start script needs must already be in place.
#
# Downloads, unpinned: the latest installers for uv and for the YellowDog Agent, and
# (with RAYDOG_OBSERVABILITY) Prometheus and Grafana. Pin versions if your nodes
# must be reproducible.

set -euo pipefail

PYTHON_VERSION="${PYTHON_VERSION:-3.12}"
RAY_VERSION="${RAY_VERSION:-2.60.0}"          # must match the Ray on your own machine
RAYDOG_PIP_SPEC="${RAYDOG_PIP_SPEC:-yellowdog-ray}"
SSH_PUBLIC_KEY="${SSH_PUBLIC_KEY:-}"
RAYDOG_OBSERVABILITY="${RAYDOG_OBSERVABILITY:-}"   # "1" on a head node: see below

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

echo "raydog: installing Python $PYTHON_VERSION, Ray $RAY_VERSION and RayDog"
export HOME=/root
export UV_INSTALL_DIR=/usr/local/bin UV_PYTHON_INSTALL_DIR=/opt/raydog/python
curl --fail -LsS --retry 5 https://astral.sh/uv/install.sh | sh >> "$LOG" 2>&1
# --seed puts pip in the environment, which Ray's runtime_env={"pip": ...} needs
uv venv --seed --python "$PYTHON_VERSION" "$VENV" >> "$LOG" 2>&1
uv pip install --python "$VENV/bin/python" \
    "ray[default,client]==$RAY_VERSION" "$RAYDOG_PIP_SPEC" >> "$LOG" 2>&1
# The download cache is the size of Ray again, on what may be a small disk
uv cache clean >> "$LOG" 2>&1

# Ray copies files to the head node with rsync
if ! command -v rsync > /dev/null; then
    # `apt-get update` may fail on one broken third-party repository; carry on
    export DEBIAN_FRONTEND=noninteractive
    apt-get update >> "$LOG" 2>&1 || true
    apt-get install -y rsync >> "$LOG" 2>&1
fi

################################################################################
# RAYDOG_OBSERVABILITY=1, on the head node only: Prometheus and Grafana, for the
# graphs in Ray's dashboard. Ray keeps a list of every node's metrics endpoint for
# Prometheus, and writes its Grafana dashboards, under /tmp/ray; these services read
# them there. The cluster does not depend on them, so a failure is reported and the
# head carries on without them.

setup_observability() {
    export DEBIAN_FRONTEND=noninteractive
    mkdir -p /etc/apt/keyrings &&
    curl --fail -LsS --retry 5 https://apt.grafana.com/gpg.key \
        | gpg --dearmor --yes -o /etc/apt/keyrings/grafana.gpg &&
    echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" \
        > /etc/apt/sources.list.d/grafana.list || return 1
    apt-get update || true
    apt-get install -y --no-install-recommends prometheus grafana || return 1
    apt-get clean

    cat > /etc/prometheus/prometheus.yml << 'EOF'
global:
  scrape_interval: 10s
scrape_configs:
  - job_name: ray
    file_sd_configs:
      - files: [/tmp/ray/prom_metrics_service_discovery.json]
        refresh_interval: 10s
EOF
    # Storage capped, so that on a small disk old data goes rather than the service
    sed -i '/^ARGS=/d' /etc/default/prometheus
    echo 'ARGS="--storage.tsdb.retention.size=256MB"' >> /etc/default/prometheus

    cat > /etc/grafana/provisioning/datasources/raydog.yaml << 'EOF'
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    url: http://localhost:9090
    isDefault: true
EOF
    cat > /etc/grafana/provisioning/dashboards/raydog.yaml << 'EOF'
apiVersion: 1
providers:
  - name: Ray
    folder: Ray
    type: file
    updateIntervalSeconds: 30
    options:
      path: /tmp/ray/session_latest/metrics/grafana/dashboards
EOF
    # Open to view without signing in, because Ray's dashboard embeds its graphs;
    # an admin password of its own in place of the default "admin"
    GRAFANA_PASSWORD="$(head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9')"
    sed -i '/^GF_/d' /etc/default/grafana-server
    printf '%s\n' GF_SECURITY_ALLOW_EMBEDDING=true GF_AUTH_ANONYMOUS_ENABLED=true \
        GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer \
        "GF_SECURITY_ADMIN_PASSWORD=$GRAFANA_PASSWORD" >> /etc/default/grafana-server
    chown root:grafana /etc/default/grafana-server
    chmod 640 /etc/default/grafana-server

    # Both read files Ray writes under /tmp, so neither may have a private /tmp
    local service
    for service in prometheus grafana-server; do
        mkdir -p "/etc/systemd/system/$service.service.d"
        printf '[Service]\nPrivateTmp=false\n' \
            > "/etc/systemd/system/$service.service.d/raydog.conf"
    done
    systemctl daemon-reload
    systemctl enable prometheus grafana-server &&
    systemctl restart prometheus grafana-server || return 1
    # Tells the head's start script to point Ray's dashboard at them
    touch /etc/raydog-observability
}

if [[ "$RAYDOG_OBSERVABILITY" == "1" ]]; then
    echo "raydog: installing Prometheus and Grafana"
    setup_observability >> "$LOG" 2>&1 ||
        echo "raydog: Prometheus and Grafana could not be set up (see $LOG);" \
             "Ray's dashboard will have no graphs" >&2
fi

################################################################################

echo "raydog: installing the YellowDog Agent"
curl --fail -LsS --retry 5 \
    https://raw.githubusercontent.com/yellowdog/resources/refs/heads/main/agent-install/linux/yd-agent-installer.sh \
    | bash >> "$LOG" 2>&1

# Ray's cluster launcher runs its commands on a node over SSH, in a shell that reads
# this file: `ray exec cluster.yaml 'ray status'`, and the `ray stop` with which
# `ray down` begins. Written before the key is authorised, so that it is in place
# for the first connection.
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

echo "raydog: authorising the SSH key for $YD_AGENT_USER"
install -d -m 700 -o "$YD_AGENT_USER" -g "$YD_AGENT_USER" "$YD_AGENT_HOME/.ssh"
printf '%s\n' "$SSH_PUBLIC_KEY" >> "$YD_AGENT_HOME/.ssh/authorized_keys"
chmod 600 "$YD_AGENT_HOME/.ssh/authorized_keys"
chown "$YD_AGENT_USER:$YD_AGENT_USER" "$YD_AGENT_HOME/.ssh/authorized_keys"

if [[ -n "${GRAFANA_PASSWORD:-}" ]]; then
    # Grafana's admin password, for whoever can reach the head node over SSH
    install -m 600 -o "$YD_AGENT_USER" -g "$YD_AGENT_USER" /dev/null \
        "$YD_AGENT_HOME/grafana-admin-password"
    printf '%s\n' "$GRAFANA_PASSWORD" > "$YD_AGENT_HOME/grafana-admin-password"
fi

# The Agent's user owns the Python environment, so that packages can be added later
chown -R "$YD_AGENT_USER:$YD_AGENT_USER" /opt/raydog

echo "raydog: node setup complete"
