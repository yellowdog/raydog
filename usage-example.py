#!/usr/bin/env python3

import logging
import random
import time
from datetime import datetime, timedelta
from fractions import Fraction
from os import getenv
from time import sleep

import dotenv
import ray

from raydog.raydog import RayDogCluster


def main():
    timestamp = str(datetime.timestamp(datetime.now())).replace(".", "-")

    try:
        # Read any extra environment variables from a file
        dotenv.load_dotenv(verbose=True, override=True)

        # Configure the Ray cluster
        raydog_cluster = RayDogCluster(
            yd_application_key_id=getenv("YD_API_KEY_ID"),
            yd_application_key_secret=getenv("YD_API_KEY_SECRET"),
            cluster_name=f"raytest-{timestamp}",  # Names the WP, WR and worker tag
            cluster_namespace="pwt-ray",
            head_node_compute_requirement_template_id="yd-demo/yd-demo-aws-eu-west-2-split-ondemand",
            head_node_images_id="ami-0fef583e486727263",  # Ubuntu 22.04, AMD64, eu-west-2
            cluster_tag="my-ray-tag",
            head_node_userdata=NODE_SETUP_SCRIPT,
            cluster_lifetime=timedelta(seconds=600),
            head_node_metrics_enabled=True,
        )

        # Add the worker pools
        for _ in range(2):
            raydog_cluster.add_worker_pool(
                worker_node_compute_requirement_template_id="yd-demo/yd-demo-aws-eu-west-2-split-ondemand",
                worker_pool_node_count=2,
                worker_node_images_id="ami-0fef583e486727263",
                worker_node_userdata=NODE_SETUP_SCRIPT,
                worker_node_metrics_enabled=True,
            )

        # Build the Ray cluster
        print("Building Ray cluster")
        private_ip, public_ip = raydog_cluster.build(
            head_node_build_timeout=timedelta(seconds=300)
        )

        cluster_address = f"ray://{public_ip}:10001"
        print(f"Head node started: {cluster_address}")

        print("Pausing to allow all worker nodes to start")
        sleep(90)

        # Run a simple application on the cluster
        print("Starting simple Ray application")
        estimate_pi(cluster_address)
        print("Finished")

        input("Hit enter to shut down cluster ")

    finally:
        # Make sure the Ray cluster gets shut down
        raydog_cluster.shut_down()


# simple Ray example
@ray.remote
def pi4_sample(sample_count):
    """pi4_sample runs sample_count experiments, and returns the
    fraction of random coordinates that are inside a unit circle.
    """
    in_count = 0
    for i in range(sample_count):
        x = random.random()
        y = random.random()
        if x * x + y * y <= 1:
            in_count += 1
    return Fraction(in_count, sample_count)


def estimate_pi(cluster_address):
    print("Connecting Ray to", cluster_address)

    # Initialize Ray
    ray.init(address=cluster_address, logging_level=logging.ERROR)

    # Get several estimates of pi
    batches = 100
    print(f"Getting {batches} estimates of pi")
    start = time.time()

    results = []
    for _ in range(batches):
        results.append(pi4_sample.remote(1000 * 1000))

    output = ray.get(results)
    mypi = sum(output) * 4 / len(output)

    dur = time.time() - start
    print(f"Estimation took {dur} seconds")

    print(f"The average estimate is {mypi} =", float(mypi))

    # Shutdown Ray
    ray.shutdown()


# Example Bash script used to set up nodes in the Ray cluster.
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

################################################################################

echo "Disabling firewall"
ufw disable &> /dev/null

# Note: the Agent configuration script will restart the Agent
"""

# Entry point
if __name__ == "__main__":
    main()
