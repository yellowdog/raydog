#!/usr/bin/bash
set -eo pipefail

MODE="${1:? Must supply mode (server|head|worker) as first argument}"
echo "Install mode: $MODE"

if [[ -z "$K3S_TOKEN" ]]; then
    echo "K3S_TOKEN env var must exist for cluster creation to be successful"
    exit 1
fi

common_setup() {
    
    # TEMP: Should be disabled in the image
    sudo systemctl stop prometheus-node-exporter
    
    MANIFEST_DIR="/var/lib/rancher/k3s/server/manifests"
    K3S_CONFIG_DIR="/etc/rancher/k3s"
    sudo mkdir -p "$MANIFEST_DIR" "$K3S_CONFIG_DIR"
    
    # Copy config files
    sudo cp config-$MODE.yaml "$K3S_CONFIG_DIR/config.yaml"
    sudo cp config-registries.yaml "$K3S_CONFIG_DIR/registries.yaml"
}

case "$MODE" in
    server)
        common_setup
        sudo cp manifests/* "$MANIFEST_DIR"
        # Don't install traefik ingress controller
        sudo touch "$MANIFEST_DIR/traefik.yaml.skip"
        # Install 
        curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server" sh -s - --egress-selector-mode=disabled 
        ;;
    head | worker)
        common_setup
        if [[ -z "$K3S_URL" ]]; then
            echo "K3S_URL env var must be present when mode is not server"
            exit 1
        fi
        # Install k3s
        curl -sfL https://get.k3s.io | sh -
        ;;
    *)
        echo "Supplied install mode not supported. Must be one of: server, head or worker."
        ;;
esac

echo "Sleeping forever..."
sleep infinity