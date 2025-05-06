#!/usr/bin/env python3

MY_USERNAME = "pwt"  # Note; Match YD naming rules, lower case, etc.

# Dimensioning the cluster: total workers is the product of the vars below
# Each compute requirement is split across eu-west-2{a, b, c}
# Note: EBS limit of 500 per provisioning request, so max WORKER_NODES_PER_POOL
#       should be 1,500 (500 instances per AZ)

WORKER_NODES_PER_POOL = 2  # Must be <= 1500, assuming split across 3 AZs
NUM_WORKER_POOLS = 2
TOTAL_WORKER_NODES = WORKER_NODES_PER_POOL * NUM_WORKER_POOLS

# Sleep duration for each Ray task in the test job
TASK_SLEEP_TIME_SECONDS = 10

import logging
import time
from datetime import datetime, timedelta
from os import getenv

import dotenv
import ray
from yellowdog_client.model.range import T
from yellowdog_client.model.worker import Worker

from raydog.raydog import RayDogCluster

# This is needed temporarily because the preconfigured AMI doesn't
# have the Ray dashboard installed

RAY_HEAD_NODE_START_SCRIPT = r"""#!/usr/bin/bash
trap "ray stop; echo Ray stopped" EXIT
set -euo pipefail
VENV=/opt/yellowdog/agent/venv
source $VENV/bin/activate
/opt/yellowdog/agent/.local/bin/uv pip install -U ray[default]
export RAY_PROMETHEUS_HOST="http://$OBSERVABILITY_HOST:9090"
export RAY_GRAFANA_IFRAME_HOST="http://localhost:3000"
export RAY_GRAFANA_HOST="http://$OBSERVABILITY_HOST:3000"
alloy run /etc/alloy/config.alloy &
ray start --head --port=6379 --num-cpus=0 --memory=0 --dashboard-host=0.0.0.0 --metrics-export-port=10002 --min-worker-port=10003 --max-worker-port=19999 --block
"""
from utils.ray_ssh_tunnels import RayTunnels

def main():
    timestamp = str(datetime.timestamp(datetime.now())).replace(".", "-")
    raydog_cluster: RayDogCluster | None = None
    ssh_tunnels: RayTunnels | None = None
    try:
        # Read any extra environment variables from a file
        dotenv.load_dotenv(verbose=True, override=True)

        # Configure the Ray cluster
        raydog_cluster = RayDogCluster(
            yd_application_key_id=getenv("YD_API_KEY_ID"),
            yd_application_key_secret=getenv("YD_API_KEY_SECRET"),
            cluster_name=f"raytest-{timestamp}",  # Names the WP, WR and worker tag
            cluster_tag=f"{MY_USERNAME}-ray-testing",
            cluster_namespace=f"{MY_USERNAME}-ray",
            head_node_compute_requirement_template_id=(
                "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead-big"
                if TOTAL_WORKER_NODES > 1000
                else "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayhead"
            ),
            head_node_images_id="ami-01d201b7824bcda1c",  # 'ray-test-8gb' AMI eu-west-2
            head_node_metrics_enabled=True,
            head_node_ray_start_script=RAY_HEAD_NODE_START_SCRIPT,
            head_node_userdata=NODE_SETUP_SCRIPT,
            enable_observability=True,
            observability_node_compute_requirement_template_id="yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayworker",
            observability_node_images_id="ami-01d201b7824bcda1c",
            observability_node_userdata=NODE_SETUP_SCRIPT,
            observability_node_metrics_enabled=True
        )

        # Add the worker pools
        for _ in range(NUM_WORKER_POOLS):
            raydog_cluster.add_worker_pool(
                worker_node_compute_requirement_template_id=(
                    "yd-demo/yd-demo-aws-eu-west-2-split-ondemand-rayworker"
                ),
                worker_pool_node_count=WORKER_NODES_PER_POOL,
                worker_node_images_id="ami-01d201b7824bcda1c",  # 'ray-test-8gb' AMI eu-west-2
                worker_node_metrics_enabled=True,
                worker_node_userdata=NODE_SETUP_SCRIPT
            )

        # Build the Ray cluster
        print("Building Ray cluster")
        private_ip, public_ip = raydog_cluster.build(
            head_node_build_timeout=timedelta(seconds=600)
        )

        # Allow time for the API and Dashboard to start before creating
        # the SSH tunnels
        time.sleep(10)
        ssh_tunnels = RayTunnels(
            ray_head_ip_address=public_ip,
            ssh_user="yd-agent",
            private_key_file="private-key",
        )
        ssh_tunnels.start_tunnels()

        cluster_address = "ray://localhost:10001"
        print(
            f"Ray head node and SSH tunnels started; using client at: {cluster_address}"
        )
        print("Ray dashboard is available at: http://localhost:8265")

        input(
            "Wait for worker nodes to join the cluster ... then hit enter to run the sample job: "
        )

        # Run a simple application on the cluster
        print("Starting simple Ray test application")
        ray_test_job(cluster_address)
        print("Finished")

        input("Hit enter to shut down cluster: ")

    finally:
        # Make sure the Ray cluster gets shut down, and the SSH tunnels stopped
        if raydog_cluster is not None:
            print("Shutting down Ray cluster")
            raydog_cluster.shut_down()
        if ssh_tunnels is not None:
            ssh_tunnels.stop_tunnels()


# Define a remote task that uses 1 CPU
@ray.remote(num_cpus=1)
def ray_worker_task(task_id):
    print(f"Task {task_id} running on worker with 1 CPU")
    time.sleep(TASK_SLEEP_TIME_SECONDS)  # Simulate work (e.g., computation)
    return f"Task {task_id} completed"


def ray_test_job(cluster_address):
    print("Connecting Ray to", cluster_address)

    # Initialize Ray
    ray.init(address=cluster_address, logging_level=logging.ERROR)

    start_time = time.time()
    task_refs = [ray_worker_task.remote(i) for i in range(TOTAL_WORKER_NODES)]
    results = ray.get(task_refs)  # Wait for all tasks to complete

    # Print results and duration
    print(f"Results: {results[:5]} ...")  # Show first 5 results
    print(f"Total duration: {time.time() - start_time} seconds")

    ray.shutdown()  # Shut down Ray

NODE_SETUP_SCRIPT = r"""#!/usr/bin/bash

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
"""

# Entry point
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopped.")
