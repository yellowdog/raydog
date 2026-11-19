#!/usr/bin/bash
# Runs on the observability node as a YellowDog task, as the Agent's user, for as
# long as the cluster lives. The node setup script has already installed and
# started Prometheus, Loki, Grafana and Alloy (see OBSERVABILITY there), so
# this checks that they are running and holds the node.

set -euo pipefail

for service in prometheus loki grafana-server alloy; do
    if ! systemctl is-active --quiet "$service"; then
        echo "raydog: $service is not running on the observability node" >&2
        exit 1
    fi
done

echo "raydog: observability node ready: Grafana on port 3000, Prometheus on 9090," \
     "Loki on 3100; Grafana's admin password is in ~/grafana-admin-password"
exec sleep infinity
