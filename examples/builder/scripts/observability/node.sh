#!/usr/bin/bash
# Functions for ../node-setup-userdata.sh: settings.py puts them, with alloy.sh, in
# the observability node's setup script (RAYDOG_OBSERVABILITY=node).

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
    apt_install prometheus loki grafana alloy >> "$LOG" 2>&1
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
