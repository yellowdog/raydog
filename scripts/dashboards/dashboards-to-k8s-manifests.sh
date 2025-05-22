#!/usr/bin/env bash

# This script makes the following assumptions:
# - You have a virtualenv with Ray on the path
# - You are running the script from the root of the repo
# 
# Run this script when you have dashboards in JSON format from Ray in the 
# scripts/dashboards directory and want to convert them to k8s manifests.
# Only needed if the dashboards are updated. 
 
get_dashboard_name() {
    jq -r '.title' $1
}

create_dashboard() {
    cat > $2 << EOF   
apiVersion: v1
kind: ConfigMap
metadata:
    name: ray-$1
    labels:
        grafana_dashboard: "1"
data:
    $1.json: >
        $dashboard_json
EOF
}

for f in $(ls scripts/dashboards); do
    name="$(get_dashboard_name scripts/dashboards/$f)"
    k8sname="$(echo "${name,,}" | tr ' ' '-' )"
    filename="scripts/k3s/manifests/$k8sname.yaml"
    dashboard_json="$(jq -c '.' scripts/dashboards/$f)"
    echo "Creating dashboard $k8sname ($name)"
    create_dashboard $k8sname $filename
done