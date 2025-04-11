#!/bin/bash

set -euo pipefail

# Get updates & install the packages we need
export DEBIAN_FRONTEND=noninteractive
apt update 
apt upgrade 
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
#!/usr/bin/env bash

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
