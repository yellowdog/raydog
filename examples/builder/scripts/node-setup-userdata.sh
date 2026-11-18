#!/usr/bin/bash
# Sets up a fresh Ubuntu instance as a node for the RayDog Builder examples. Used as
# instance userdata: it runs once, as root, at first boot. settings.py fills in the
# public key and Alloy's configuration, and turns on OBSERVABILITY_NODE for the
# observability node. For large or frequently started clusters, bake a machine image
# instead (see ../../../packer).
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

# "yes" on the observability node: Prometheus, Loki and Grafana are installed and
# started here, so that they are ready before the node's task starts.
OBSERVABILITY_NODE="no"

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
# Observability. Grafana Alloy ships each node's metrics and logs to the
# observability node; it is installed on every node, so that any cluster can have
# one. On the head and workers nothing else depends on it, so a failure there is
# reported and the node carries on; on the observability node it is fatal.

write_alloy_config() {
    install -d -m 755 /etc/alloy
    cat > /etc/alloy/config.alloy << 'ALLOY_CONFIG'
REPLACE_WITH_ALLOY_CONFIG
ALLOY_CONFIG
    chmod 644 /etc/alloy/config.alloy
}

add_grafana_repository() {
    [[ -f /etc/apt/sources.list.d/grafana.list ]] && return 0
    mkdir -p /etc/apt/keyrings &&
    curl --fail -LsS --retry 5 https://apt.grafana.com/gpg.key \
        | gpg --dearmor --yes -o /etc/apt/keyrings/grafana.gpg &&
    echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" \
        > /etc/apt/sources.list.d/grafana.list
}

install_alloy() {
    if ! command -v alloy > /dev/null; then
        add_grafana_repository || return 1
        # One broken repository on the image should not stop the rest
        apt-get update || true
        apt-get -y install alloy || return 1
    fi
    systemctl disable --now alloy || true    # the start scripts run it when wanted
    write_alloy_config
}

wait_for() {    # wait_for SERVICE URL: until the service answers, for up to 3 minutes
    for _ in $(seq 1 90); do
        curl --fail --silent --output /dev/null "$2" && return 0
        sleep 2
    done
    echo "raydog: $1 did not start; see 'journalctl -u $1' on the instance" >&2
    return 1
}

# Prometheus, Loki and Grafana, with Ray's dashboards, for the observability node
setup_observability() {
    echo "raydog: installing Prometheus, Loki and Grafana"
    add_grafana_repository >> "$LOG" 2>&1
    apt-get update >> "$LOG" 2>&1 || true
    apt-get -y install prometheus loki grafana alloy >> "$LOG" 2>&1
    apt-get clean
    write_alloy_config

    # Prometheus accepts the metrics that Alloy pushes. The option was renamed in
    # Prometheus 2.33, and the old name was dropped in 3.0.
    local receiver="--enable-feature=remote-write-receiver" help
    help="$(prometheus --help 2>&1 || true)"
    if [[ "$help" == *--web.enable-remote-write-receiver* ]]; then
        receiver="--web.enable-remote-write-receiver"
    fi
    # Its storage is capped, so that on a small disk it stops keeping old data
    # rather than failing
    touch /etc/default/prometheus
    sed -i '/^ARGS=/d' /etc/default/prometheus
    echo "ARGS=\"$receiver --storage.tsdb.retention.size=256MB\"" >> /etc/default/prometheus

    # Loki's packaged configuration logs at debug level, and in some versions turns
    # on an experimental query option; turn both off
    sed -i -e 's/log_level: debug/log_level: info/' \
        -e '/enable_multi_variant_queries: true/d' /etc/loki/config.yml

    # Grafana: Prometheus and Loki as data sources, Ray's dashboards in a "Ray"
    # folder, open to view without signing in (Ray's dashboard embeds its graphs),
    # and an admin password of its own in place of the default "admin"
    cat > /etc/grafana/provisioning/datasources/raydog.yaml << 'EOF'
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    url: http://localhost:9090
    isDefault: true
  - name: Loki
    type: loki
    url: http://localhost:3100
EOF
    cat > /etc/grafana/provisioning/dashboards/raydog.yaml << 'EOF'
apiVersion: 1
providers:
  - name: Ray
    folder: Ray
    type: file
    options:
      path: /var/lib/grafana/dashboards/ray
EOF
    GRAFANA_PASSWORD="$(head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9')"
    touch /etc/default/grafana-server
    sed -i '/^GF_/d' /etc/default/grafana-server
    cat >> /etc/default/grafana-server << EOF
GF_SECURITY_ALLOW_EMBEDDING=true
GF_AUTH_ANONYMOUS_ENABLED=true
GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
GF_SECURITY_ADMIN_PASSWORD=$GRAFANA_PASSWORD
EOF
    chown root:grafana /etc/default/grafana-server
    chmod 640 /etc/default/grafana-server
    # That setting takes effect only when Grafana first starts; on an image where it
    # has started before, set the password directly
    if [[ -f /var/lib/grafana/grafana.db ]]; then
        grafana cli --homepath /usr/share/grafana --config /etc/grafana/grafana.ini \
            admin reset-admin-password "$GRAFANA_PASSWORD" >> "$LOG" 2>&1
        chown -R grafana:grafana /var/lib/grafana
    fi

    # Ray writes its Grafana dashboards when a head node starts: start one briefly,
    # keep them, and leave nothing of it behind
    local dashboards=/tmp/ray/session_latest/metrics/grafana/dashboards
    "$VENV/bin/ray" start --head --disable-usage-stats >> "$LOG" 2>&1
    for _ in $(seq 1 60); do
        [[ -f "$dashboards/default_grafana_dashboard.json" ]] && break
        sleep 2
    done
    sleep 5    # for the rest of them
    install -d -o grafana -g grafana /var/lib/grafana/dashboards/ray
    install -o grafana -g grafana -m 644 "$dashboards"/*.json /var/lib/grafana/dashboards/ray/
    "$VENV/bin/ray" stop >> "$LOG" 2>&1
    rm -rf /tmp/ray

    systemctl daemon-reload
    systemctl enable prometheus loki grafana-server alloy >> "$LOG" 2>&1
    systemctl restart prometheus loki grafana-server alloy
    wait_for prometheus http://localhost:9090/-/ready
    wait_for loki http://localhost:3100/ready
    wait_for grafana-server http://localhost:3000/api/health
}

if [[ "$OBSERVABILITY_NODE" == "yes" ]]; then
    setup_observability
else
    echo "raydog: installing Grafana Alloy"
    install_alloy >> "$LOG" 2>&1 ||
        echo "raydog: Grafana Alloy could not be installed (see $LOG); the" \
             "observability node will have no data from this node" >&2
fi

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
