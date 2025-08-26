#!/usr/bin/bash
# Run ray head node so we can get the dashboards it generates
VENV=/opt/yellowdog/agent/venv
source $VENV/bin/activate
ray start --head --port=6379

# Install and configure Grafana, Mimir, and Loki
sudo apt -y install grafana prometheus loki
sudo tee /etc/grafana/provisioning/datasources/prometheus.yaml <<EOF
datasources:
- name: Prometheus
  type: prometheus
  url: http://localhost:9090
EOF
sudo tee /etc/grafana/provisioning/datasources/loki.yaml <<EOF
datasources:
- name: loki
  type: loki
  url: http://localhost:3100
EOF
sudo tee /etc/grafana/provisioning/dashboards/ray.yaml <<EOF
apiVersion: 1
providers:
- name: Ray
  folder: Ray
  type: file
  options:
    path: /var/lib/grafana/dashboards/ray
EOF
sudo tee -a /etc/grafana/grafana.ini <<EOF
[security]
allow_embedding = true
[auth.anonymous]
enabled = true
org_name = Main Org.
org_role = Viewer
EOF
sudo sed -i 's/enable_multi_variant_queries: true//' /etc/loki/config.yml
sudo sed -i 's/log_level: debug/log_level: info/' /etc/loki/config.yml
echo 'ARGS="--enable-feature=remote-write-receiver"' | sudo tee -a /etc/default/prometheus
 
# Copy the dashboards 
sudo mkdir -p /var/lib/grafana/dashboards/ray && sudo cp /tmp/ray/session_latest/metrics/grafana/dashboards/*.json /var/lib/grafana/dashboards/ray/
sudo chown -R grafana:grafana /var/lib/grafana/dashboards

# (Re)start services
sudo systemctl daemon-reload
sudo /bin/systemctl enable --now grafana-server
sudo systemctl restart prometheus
sudo systemctl restart loki
sudo systemctl start alloy

# Kill ray
ray stop

# Hold indefinitely
while true; do
    sleep 60
done