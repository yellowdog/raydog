#!/usr/bin/bash

set -euo pipefail

echo "Installing the YellowDog agent"
cd /root || exit
curl -LsSf https://raw.githubusercontent.com/yellowdog/resources/refs/heads\
/main/agent-install/linux/yd-agent-installer.sh | bash &> /dev/null

################################################################################
YD_AGENT_USER="yd-agent"
YD_AGENT_HOME="/opt/yellowdog/agent"

echo "Adding $YD_AGENT_USER to passwordless sudoers"

ADMIN_GRP="sudo"
usermod -aG $ADMIN_GRP $YD_AGENT_USER
echo -e "$YD_AGENT_USER\tALL=(ALL)\tNOPASSWD: ALL" > \
        /etc/sudoers.d/020-$YD_AGENT_USER

################################################################################
echo "Adding public SSH key for $YD_AGENT_USER"

mkdir -p $YD_AGENT_HOME/.ssh
chmod og-rwx $YD_AGENT_HOME/.ssh

# Insert the required public key below
cat >> $YD_AGENT_HOME/.ssh/authorized_keys << EOM
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDBAwA8lQurxJh2m9zyB6A/QG7/0jRYQQgH0zJg\
Tr8+uGdYJs4hpbsU43jqfdiOY9gBN35j2LFfHHsYxJmFkFXh2DQn3+WZhzxYzPOiSIBtNnHmRY3j\
71wJbNUX1kF4VyifiaiuPviJd0YKD/y0UnhZKBs4EQQB9qPzpcSoixcLa6hgh5gqY8yA+BuI4dgK\
5SG2t5seujJ45bT67HvCeFYShFXPsvB9KwhptBF1Hd961+AoXO8IVXSEKBnrTTecbeFgc0V2vRqO\
TNdSiWrD71mij3NUd3dzp+9qepDZaNtNXMJ8jnF2nzk43JvrRzteWJlyya+63/bvdq/jj7jLH3tN\
pcyNw16YmctpjKr7uKc4k6gEa3b7YaELwX8g1xGQib95RXuzvef7qduDAbQbvadbvM97iohaeWMM\
7uh1rNM6qsVdyGd1FUVNFiPUqsQ5sQhRdnryu/lF10hDArGkhu+tmwQEFsp2ymFlaVexKWB/Q20q\
A0bE4yNXbZF4WUdBJzc= pwt@pwt-mbp-14.local
EOM

chmod og-rw $YD_AGENT_HOME/.ssh/authorized_keys
chown -R $YD_AGENT_USER:$YD_AGENT_USER $YD_AGENT_HOME/.ssh

################################################################################

echo "Installing 'uv'"
export HOME=$YD_AGENT_HOME
curl -LsSf https://astral.sh/uv/install.sh | sh &> /dev/null
source $HOME/.local/bin/env

PYTHON_VERSION="3.12.10"
echo "Installing Python v$PYTHON_VERSION and creating Python virtual environment"
VENV=$YD_AGENT_HOME/venv
uv venv --python $PYTHON_VERSION $VENV
VIRTUAL_ENV_DISABLE_PROMPT=true
source $VENV/bin/activate

echo "Installing Ray"
uv pip install ray[default,client]

echo "Setting file/directory ownership to $YD_AGENT_USER"
chown -R $YD_AGENT_USER:$YD_AGENT_USER $YD_AGENT_HOME/.local $VENV $YD_AGENT_HOME/.cache

echo "Installing Grafana Alloy"
sudo apt -y install gpg
sudo mkdir -p /etc/apt/keyrings/
wget -q -O - https://apt.grafana.com/gpg.key | gpg --dearmor | sudo tee /etc/apt/keyrings/grafana.gpg > /dev/null
echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" | sudo tee /etc/apt/sources.list.d/grafana.list
sudo apt-get update && sudo apt-get -y install alloy
sudo systemctl disable alloy
cat > /etc/alloy/config.alloy <<EOF
local.file_match "ray" {
    path_targets = [{"__path__" = "/tmp/ray/session_latest/logs/*", job="ray"}]
}

loki.source.file "ray" {
    targets       = local.file_match.ray.targets
    forward_to    = [loki.write.loki.receiver]
    tail_from_end = true
}

local.file_match "worker_output" {
    path_targets = [{"__path__" = "/var/opt/yellowdog/agent/workers/*/taskoutput.txt", job="yellowdog_worker_task_output"}]
}

loki.source.file "worker_output" {
    targets       = local.file_match.worker_output.targets
    forward_to    = [loki.write.loki.receiver]
    tail_from_end = true
}

loki.source.journal "journal" {
  max_age       = "24h0m0s"
  relabel_rules = discovery.relabel.logs_integrations_integrations_node_exporter_journal_scrape.rules
  forward_to    = [loki.write.loki.receiver]
}

loki.write "loki" {
	endpoint {
		url = string.format(
			"http://%s:3100/loki/api/v1/push",
			coalesce(sys.env("OBSERVABILITY_HOST"), "127.0.0.1"),
		)
	}
}

prometheus.exporter.self "alloy" {}

prometheus.scrape "ray" {
    targets    = [{__address__ = "127.0.0.1:10002", instance = constants.hostname, job = "ray" }]
    forward_to = [prometheus.remote_write.mimir.receiver]
}

prometheus.exporter.unix "integrations_node_exporter" {
  disable_collectors = ["ipvs", "btrfs", "infiniband", "xfs", "zfs"]
  enable_collectors = ["meminfo"]

  filesystem {
    fs_types_exclude     = "^(autofs|binfmt_misc|bpf|cgroup2?|configfs|debugfs|devpts|devtmpfs|tmpfs|fusectl|hugetlbfs|iso9660|mqueue|nsfs|overlay|proc|procfs|pstore|rpc_pipefs|securityfs|selinuxfs|squashfs|sysfs|tracefs)$"
    mount_points_exclude = "^/(dev|proc|run/credentials/.+|sys|var/lib/docker/.+)($|/)"
    mount_timeout        = "5s"
  }

  netclass {
    ignored_devices = "^(veth.*|cali.*|[a-f0-9]{15})$"
  }

  netdev {
    device_exclude = "^(veth.*|cali.*|[a-f0-9]{15})$"
  }
}

discovery.relabel "integrations_node_exporter" {
  targets = prometheus.exporter.unix.integrations_node_exporter.targets

  rule {
    target_label = "instance"
    replacement  = constants.hostname
  }

  rule {
    target_label = "job"
    replacement = "integrations/node_exporter"
  }
}

discovery.relabel "logs_integrations_integrations_node_exporter_journal_scrape" {
  targets = []
  
  rule {
    target_label = "instance"
    replacement  = constants.hostname
  }
  rule {
    source_labels = ["__journal__systemd_unit"]
    target_label  = "unit"
  }

  rule {
    source_labels = ["__journal__boot_id"]
    target_label  = "boot_id"
  }

  rule {
    source_labels = ["__journal__transport"]
    target_label  = "transport"
  }

  rule {
    source_labels = ["__journal_priority_keyword"]
    target_label  = "level"
  }
}

prometheus.scrape "integrations_node_exporter" {
  scrape_interval = "15s"
  targets    = discovery.relabel.integrations_node_exporter.output
  forward_to = [prometheus.remote_write.mimir.receiver]
}

prometheus.scrape "alloy" {
	targets    = prometheus.exporter.self.alloy.targets
	forward_to = [prometheus.remote_write.mimir.receiver]
}

prometheus.remote_write "mimir" {
	endpoint {
		url = string.format(
			"http://%s:9090/api/v1/write",
			coalesce(sys.env("OBSERVABILITY_HOST"), "127.0.0.1"),
		)
	}
}
EOF
sudo chmod 755 /etc/alloy
sudo chmod 755 /etc/alloy/config.alloy
################################################################################

echo "Disabling firewall"
ufw disable &> /dev/null

# Note: the Agent configuration script will restart the Agent