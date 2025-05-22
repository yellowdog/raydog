#!/usr/bin/bash

set -eo pipefail

sudo systemctl start loki
sudo systemctl start grafana-server
sudo systemctl start prometheus
sudo systemctl start alloy
# Hold indefinitely
while true; do
    sleep 60
done