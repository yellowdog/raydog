#!/usr/bin/env python3

from datetime import datetime, timedelta
from os import getenv
from time import sleep

from yellowdog_client.model import ApiKey, ServicesSchema
from yellowdog_client.platform_client import PlatformClient

from raydog.raydog import RayDogCluster


def main():
    timestamp = str(datetime.timestamp(datetime.now())).replace(".", "-")

    # Configure the Ray cluster
    raydog_cluster = RayDogCluster(
        client=PlatformClient.create(
            ServicesSchema(defaultUrl="https://api.yellowdog.ai"),
            ApiKey(
                getenv("YD_VAR_APP_KEY_YELLOWDOG"),
                getenv("YD_VAR_APP_SECRET_YELLOWDOG"),
            ),
        ),
        cluster_name=f"raytest-{timestamp}",  # Names the WP, WR and worker tag
        cluster_namespace="pwt-ray",
        compute_requirement_template_id="yd-demo/yd-demo-aws-eu-west-2-split-ondemand",
        total_node_count=3,  # Total number of nodes including the head node
        images_id="ami-0fef583e486727263",  # Ubuntu 22.04, AMD64, eu-west-2
        cluster_tag="my-ray-tag",
        userdata=NODE_SETUP_SCRIPT,
        cluster_timeout=timedelta(seconds=600),
        worker_node_compute_requirement_template_id="yd-demo/yd-demo-aws-eu-west-2-split-spot",
        worker_node_images_id="ami-0fef583e486727263",
        worker_node_userdata=NODE_SETUP_SCRIPT,
    )

    # Build the Ray cluster & emit the public IP address
    print(raydog_cluster.build(build_timeout=timedelta(seconds=300)))

    # Wait, then shut down the Ray cluster
    sleep(120)
    raydog_cluster.shut_down()


NODE_SETUP_SCRIPT = r"""#!/usr/bin/bash

set -euo pipefail

# Download the Agent installer script
cd /root || exit
wget https://raw.githubusercontent.com/yellowdog/resources/refs/heads/main/agent-install/linux/yd-agent-installer.sh

# Run the Agent installer script
bash yd-agent-installer.sh

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
uv pip install ray[client]

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
