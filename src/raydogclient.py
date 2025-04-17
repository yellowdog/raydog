from datetime import timedelta

import dotenv
from yellowdog_client.model import (
    AutoShutdown,
    ComputeRequirementTemplateUsage,
    ProvisionedWorkerPoolProperties,
    RunSpecification,
    Task,
    TaskGroup,
    TaskOutput,
    WorkRequirement,
)

from raydog import RayDog


class RayDogClient(RayDog):
    def __init__(self):
        # read any extra environment variables from a file
        dotenv.load_dotenv(verbose=True, override=True)

        self.workreq = None

        # most setup is done in the base class
        super().__init__()

    def start_head_node(self):
        print("Telling YellowDog to create head node")

        # create a work requirement
        workreq = WorkRequirement(
            namespace=self.namespace,
            name=self._get_wr_name(),
            taskGroups=[
                TaskGroup(
                    name="head-tg-" + self.clusterid,
                    runSpecification=RunSpecification(
                        taskTypes=["ray-head"],
                        maxWorkers=1,
                        maximumTaskRetries=0,
                        workerTags=self.workertags,
                        exclusiveWorkers=True,
                    ),
                ),
                TaskGroup(
                    name="worker-tg-" + self.clusterid,
                    runSpecification=RunSpecification(
                        taskTypes=["ray-worker"],
                        maximumTaskRetries=2,
                        workerTags=self.workertags,
                        exclusiveWorkers=True,
                    ),
                ),
            ],
        )
        self.workreq = self.ydworkapi.add_work_requirement(workreq)

        # add a YellowDog task for the head node
        headtask = Task(
            name="ray-head",
            taskType="ray-head",
            environment={
                "YD_API_URL": self.api_url,
                "YD_API_KEY_ID": self.api_key_id,
                "YD_API_KEY_SECRET": self.api_key_secret,
                "YD_RAY_PORT": self.ray_port,
                "YD_CLUSTER_ID": self.clusterid,
            },
            outputs=[TaskOutput.from_task_process()],
        )
        newtasks = self.ydworkapi.add_tasks_to_task_group(
            self._get_head_task_group(), [headtask]
        )

        # wait for the head node to start
        print("Waiting for the head node")
        headtask = self._wait_for_task(newtasks[0].id)

        # read the head node's IP address
        publicip, privateip = self._get_ip_address(headtask.workerId)
        print("Head node IP address:", publicip)

        # return f"ray://{publicip}:{self.ray_port}"
        return f"ray://{publicip}:10001"

    def shutdown(self):
        # cancel the work requirement and abort all tasks
        print("Shutting down the Ray cluster")
        if self.workreq:
            self.ydworkapi.cancel_work_requirement_by_id(self.workreq.id, abort=True)

    def create_worker_pool(self):
        print("Initialising YellowDog worker pool")

        # TODO: make all of this configurable

        crtname = "yd-demo-aws-eu-west-2-split-ondemand"
        crt = self.ydclient.compute_client.get_compute_requirement_template_by_name(
            "yd-demo", crtname
        )

        imgname = "ubuntu-22-04-aws-amd64"
        imggrp = self.ydclient.images_client.get_latest_image_group_by_family_name(
            "yd-demo", imgname
        )

        self.worker_pool = self.ydclient.worker_pool_client.provision_worker_pool(
            ComputeRequirementTemplateUsage(
                templateId=crt.id,
                imagesId=imggrp.id,
                requirementNamespace=self.namespace,
                requirementName="wp-" + self.clusterid,
                userData=RayDogClient.setup_script,
                targetInstanceCount=1,
            ),
            ProvisionedWorkerPoolProperties(
                nodeBootTimeout=timedelta(seconds=300),
                idleNodeShutdown=AutoShutdown(timeout=timedelta(seconds=60)),
                idlePoolShutdown=AutoShutdown(timeout=timedelta(seconds=60)),
                minNodes=0,
                maxNodes=10,
                workerTag="raydog",
            ),
        )

    setup_script = r"""#!/usr/bin/bash

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
WORKER_SCRIPT="$YD_AGENT_HOME/start-ray-worker.sh"
HEAD_SCRIPT="$YD_AGENT_HOME/start-ray-head.sh"

echo "Creating Ray worker startup script $WORKER_SCRIPT"
cat > $WORKER_SCRIPT << 'EOT'
#!/usr/bin/bash
trap "ray stop; echo Ray stopped" EXIT
set -euo pipefail
VENV=/opt/yellowdog/agent/venv
VIRTUAL_ENV_DISABLE_PROMPT=true
source $VENV/bin/activate
ray start --disable-usage-stats --address=$YD_RAY_HEAD_NODE:6379 --block
EOT

chmod a+x $WORKER_SCRIPT
chown $YD_AGENT_USER:$YD_AGENT_USER $WORKER_SCRIPT

echo "Creating Ray head node startup script $HEAD_SCRIPT"
cp $WORKER_SCRIPT $HEAD_SCRIPT
sed -i "/^ray start/c\ray start --disable-usage-stats --head --port=6379 --block"  \
    $HEAD_SCRIPT

echo "Inserting the YellowDog task types for starting a Ray head node and worker nodes"
sed -i "/^yda.taskTypes:/a\  - name: \"ray-head\"\n    run: \"$HEAD_SCRIPT\"" \
    $YD_AGENT_HOME/application.yaml
sed -i "/^yda.taskTypes:/a\  - name: \"ray-worker\"\n    run: \"$WORKER_SCRIPT\"" \
    $YD_AGENT_HOME/application.yaml

echo "Disabling firewall"
ufw disable &> /dev/null

# Note: the Agent configuration script will restart the Agent
"""
