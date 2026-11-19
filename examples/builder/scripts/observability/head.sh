#!/usr/bin/bash
# Functions for ../node-setup-userdata.sh: settings.py puts them in the head node's
# setup script with RAYDOG_OBSERVABILITY=1.

# Prometheus and Grafana on the head node, for the graphs in Ray's dashboard, as in
# Ray's own guide for clusters on VMs. Ray keeps a list of every node's metrics
# endpoint for Prometheus, and writes its Grafana dashboards, under /tmp/ray; these
# services read them there, so the workers need nothing.
setup_head_observability() {
    add_grafana_repository || return 1
    apt_install --no-install-recommends prometheus grafana || return 1
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
    touch /etc/default/prometheus
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
    touch /etc/default/grafana-server
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
