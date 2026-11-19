#!/usr/bin/bash
# Functions for ../node-setup-userdata.sh: settings.py puts them in the setup script
# of the head and workers when the cluster has an observability node, to which
# Grafana Alloy sends their metrics and logs, and of the observability node itself.

write_alloy_config() {
    install -d -m 755 /etc/alloy
    cat > /etc/alloy/config.alloy << 'ALLOY_CONFIG'
REPLACE_WITH_ALLOY_CONFIG
ALLOY_CONFIG
    chmod 644 /etc/alloy/config.alloy
}

install_alloy() {
    if ! command -v alloy > /dev/null; then
        add_grafana_repository || return 1
        apt_install alloy || return 1
    fi
    systemctl disable --now alloy || true    # the start scripts run it when wanted
    write_alloy_config
}
