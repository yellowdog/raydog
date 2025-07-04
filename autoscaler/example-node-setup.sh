#!/usr/bin/bash

set -euo pipefail

echo "Installing the YellowDog agent"
cd /root || exit
curl -LsSf https://raw.githubusercontent.com/yellowdog/resources/refs/heads/main/agent-install/linux/yd-agent-installer.sh \
      | bash &> /dev/null

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
uv pip install "ray[client]" "ray[default]" sshtunnel yellowdog-sdk redis numpy

################################################################################

echo "Downloading RayDog files"

curl -L -o $YD_AGENT_HOME/raydog.tgz "https://drive.google.com/uc?export=download&id=1MM0OpucCLO8wySHmi_U-YyD9TD8Vt9yz"
tar -xf $YD_AGENT_HOME/raydog.tgz -C $YD_AGENT_HOME 

echo "Modifying Ray script"
RAYFILE=$VENV/bin/ray 
cp $RAYFILE $RAYFILE.bak
awk '1;/^import sys/{ print "import raydog"}' $RAYFILE.bak > $RAYFILE

echo "Create bashrc to setup the Ray environment"
echo "export PYTHONPATH=/opt/yellowdog/agent" > $YD_AGENT_HOME/.bashrc
echo "VIRTUAL_ENV_DISABLE_PROMPT=true" >> $YD_AGENT_HOME/.bashrc
echo "source $VENV/bin/activate" >> $YD_AGENT_HOME/.bashrc

################################################################################

echo "Disabling firewall"
ufw disable &> /dev/null

################################################################################

echo "Setting file/directory ownership to $YD_AGENT_USER"
chown -R $YD_AGENT_USER:$YD_AGENT_USER $YD_AGENT_HOME


# Note: the Agent configuration script will restart the Agent