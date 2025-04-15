import dotenv
import sys
from datetime import timedelta

from yellowdog_client.model import ComputeRequirementTemplateUsage
from yellowdog_client.model import ProvisionedWorkerPoolProperties
from yellowdog_client.model import AutoShutdown

from yellowdog_client.model import WorkRequirement, RunSpecification, Task, TaskGroup

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
            namespace = self.namespace,
            name = self._get_wr_name(),

            taskGroups = [
                TaskGroup(
                    name = "head-tg-" + self.clusterid,
                    runSpecification=RunSpecification(
                        taskTypes=["ray-head"],
                        maxWorkers=1,
                        maximumTaskRetries=5,
                        workerTags=self.workertags
                    )
                ),
                TaskGroup(
                    name = "worker-tg-" + self.clusterid,
                    runSpecification=RunSpecification(
                        taskTypes=["ray-worker"],
                        maximumTaskRetries=5,
                        workerTags=self.workertags
                    )
                )
           ]
        )
        self.workreq = self.ydworkapi.add_work_requirement(workreq)

        # add a YellowDog task for the head node
        headtask = Task(
            name = "ray-head",
            taskType = "ray-head",
            environment = {
                "YD_API_URL" :        self.api_url, 
                "YD_API_KEY_ID" :     self.api_key_id,
                "YD_API_KEY_SECRET" : self.api_key_secret,
                "YD_RAY_PORT":        self.ray_port,
                "YD_CLUSTER_ID" :     self.clusterid
            }
        )
        newtasks = self.ydworkapi.add_tasks_to_task_group(self._get_head_task_group(), [headtask])

        # wait for the head node to start
        print("Waiting for the head node")
        headtask = self._wait_for_task(newtasks[0].id)

        # read the head node's IP address
        publicip, privateip = self._get_ip_address(headtask.workerId)
        print("Head node IP address:", publicip)

        #return f"ray://{publicip}:{self.ray_port}"
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
        crt = self.ydclient.compute_client.get_compute_requirement_template_by_name("yd-demo", crtname)

        imgname = "ubuntu-24-04-amd64"
        imggrp = self.ydclient.images_client.get_latest_image_group_by_family_name("yd-demo", imgname)

        self.worker_pool = self.ydclient.worker_pool_client.provision_worker_pool(
            ComputeRequirementTemplateUsage(
                templateId=crt.id,
                imagesId=imggrp.id,
                requirementNamespace=self.namespace,
                requirementName="wp-" + self.clusterid,
                userData=RayDogClient.setupscript,
                targetInstanceCount=1
            ),
            ProvisionedWorkerPoolProperties(
                nodeBootTimeout=timedelta(seconds=300),
                idleNodeShutdown=AutoShutdown(timeout=timedelta(seconds=300)),
                idlePoolShutdown=AutoShutdown(timeout=timedelta(seconds=300)),
                minNodes = 0,
                maxNodes = 10,
                workerTag="raydog"
            )
        )

    setupscript = """#!/usr/bin/bash

set -euo pipefail

# Add public SSH key for YD agent user
YD_AGENT_USER=yd-agent
YD_AGENT_HOME=/opt/yellowdog/agent
YD_AGENT_SSH=$YD_AGENT_HOME/.ssh

useradd -m -d $YD_AGENT_HOME -s /usr/bin/bash $YD_AGENT_USER 
mkdir -p $YD_AGENT_SSH
chmod og-rwx $YD_AGENT_SSH

# Insert the required public key below
cat >> $YD_AGENT_SSH/authorized_keys << EOM
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIMjp31n7ztJ2nB6Hm1i67ePowqhEDLnX21cGraTZ2TaO winston@Winstons-MacBook-Pro.local
EOM

chmod og-rw $YD_AGENT_SSH/authorized_keys
chown -R yd-agent:yd-agent $YD_AGENT_SSH

# Add the YD agent user to passwordless sudoers
usermod -aG sudo $YD_AGENT_USER
echo -e "yd-agent\tALL=(ALL)\tNOPASSWD: ALL" > \
        /etc/sudoers.d/020-yd-agent

# Install the packages we need
export DEBIAN_FRONTEND=noninteractive
apt update 
# apt upgrade 
apt install -y unzip bash-builtins python3 python3-pip python3-dev python3-venv wget

# Download the YD installer script
cd /root
wget https://raw.githubusercontent.com/yellowdog/resources/refs/heads/main/agent-install/linux/yd-agent-installer.sh

# Run the YD installer
bash yd-agent-installer.sh

# Create a Python virtual env 
VENV=/opt/yellowdog/agent/venv

python3 -m venv $VENV
VIRTUAL_ENV_DISABLE_PROMPT=true
source $VENV/bin/activate

# Install Ray
pip install ray ray[client]

# Create the script used to run Ray workers
cat > /opt/yellowdog/agent/start-ray-worker.sh << 'EOT'
#!/usr/bin/bash

set -euo pipefail

VENV=/opt/yellowdog/agent/venv

VIRTUAL_ENV_DISABLE_PROMPT=true
source $VENV/bin/activate

# shut down Ray when this script is interrupted
trap "ray stop; echo Ray stopped" EXIT
trap "echo Quitting; exit 0" TERM INT

# start ray
ray start --disable-usage-stats --address=$YD_RAY_HEAD_NODE:6379

# keep the task alive until it is killed
if [[ -e /usr/lib/bash/sleep ]] ; then
    # if the sleep command from the bash-builtins package is available, use it
    enable sleep
    while true; do sleep 10000; done
else
    # we have to spin more with the external sleep command, because SIGTERM won't 
    # get processed until after sleep exits
    while true; do sleep 10; done
fi
EOT
chmod a+x /opt/yellowdog/agent/start-ray-worker.sh
chown yd-agent:yd-agent /opt/yellowdog/agent/start-ray-worker.sh

# Change the script to be able to run the Ray head node
cp /opt/yellowdog/agent/start-ray-worker.sh /opt/yellowdog/agent/start-ray-head.sh
sed -i "/^ray start/c\ray start --disable-usage-stats --head --port=6379"  "/opt/yellowdog/agent/start-ray-head.sh"

# Setup the YellowDog tasks that run Ray
cat > setup-yd-task.py << 'EOT'
#!/usr/bin/env python3
import sys

config_file = "/opt/yellowdog/agent/application.yaml"

with open(config_file, "r") as f:
    inp = f.readlines()

outp = []
for line in inp:
    outp.append(line)
    if line.startswith('yda.taskTypes:'):
        outp.append('  - name: ' + sys.argv[1] + '\n')
        outp.append('    run: '  + sys.argv[2] + '\n')

with open(config_file, "w") as f:
    f.writelines(outp)
EOT
chmod a+x setup-yd-task.py

./setup-yd-task.py ray-head   "/opt/yellowdog/agent/start-ray-head.sh"
./setup-yd-task.py ray-worker "/opt/yellowdog/agent/start-ray-worker.sh"

# Restart the agent
systemctl --no-block restart yd-agent.service
"""

