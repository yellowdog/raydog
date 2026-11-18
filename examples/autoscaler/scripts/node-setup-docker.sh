# Sets up a fresh Ubuntu instance to run Ray in a container, for RayDog. Used as
# instance userdata, after a few lines of settings: see raydog-docker.yaml.
#
# The YellowDog Agent is installed last, on purpose: the moment it starts, the node
# is offered work, so the image must already have been pulled.
#
# Downloads, unpinned: the YellowDog Agent's latest installer, and the Ray image by
# its tag. Pin them if your nodes must be reproducible.

set -euo pipefail

RAY_IMAGE="${RAY_IMAGE:-rayproject/ray:2.60.0-py312}"   # Ray must match your own
RAYDOG_PIP_SPEC="${RAYDOG_PIP_SPEC:-yellowdog-ray}"
SSH_PUBLIC_KEY="${SSH_PUBLIC_KEY:-}"

LOG=/var/log/raydog-node-setup.log
YD_AGENT_USER="yd-agent"
YD_AGENT_HOME="/opt/yellowdog/agent"

# Say where it went wrong: this appears in the instance's console output
trap 'echo "raydog: node setup FAILED at line $LINENO; see $LOG on the instance" >&2' ERR

case "$SSH_PUBLIC_KEY" in
    ssh-*|ecdsa-*|sk-*) ;;
    *) echo "raydog: SSH_PUBLIC_KEY is not set; aborting node setup" >&2; exit 1 ;;
esac

# The start scripts run later, as tasks, and need to know these too
printf 'RAY_IMAGE=%q\nRAYDOG_PIP_SPEC=%q\n' "$RAY_IMAGE" "$RAYDOG_PIP_SPEC" > /etc/raydog-node.env
chmod 644 /etc/raydog-node.env

# Docker, and rsync (with which Ray copies files to the head node), unless the image
# has them already. `apt-get update` is allowed to fail: one unreachable or expired
# third-party repository on the image should not stop the node being set up.
export DEBIAN_FRONTEND=noninteractive
packages=()
command -v docker > /dev/null || packages+=(docker.io)
command -v rsync > /dev/null || packages+=(rsync)
if (( ${#packages[@]} )); then
    echo "raydog: installing ${packages[*]}"
    apt-get update >> "$LOG" 2>&1 || true
    apt-get install -y "${packages[@]}" >> "$LOG" 2>&1
fi
systemctl enable --now docker >> "$LOG" 2>&1

# The image needs room: rayproject/ray is over 2 GB unpacked, the GPU and ML
# images several times that. A full disk is the usual reason for this step failing.
echo "raydog: pulling $RAY_IMAGE"
docker pull "$RAY_IMAGE" >> "$LOG" 2>&1

# The start scripts run as the Agent's user and need to run containers. The Agent's
# service gets the docker group before it is installed, because the installer
# starts it, and restarting it later would kill any task it had already taken.
mkdir -p /etc/systemd/system/yd-agent.service.d
printf '[Service]\nSupplementaryGroups=docker\n' \
    > /etc/systemd/system/yd-agent.service.d/raydog-docker.conf

echo "raydog: installing the YellowDog Agent"
curl --fail -LsS --retry 5 \
    https://raw.githubusercontent.com/yellowdog/resources/refs/heads/main/agent-install/linux/yd-agent-installer.sh \
    | bash >> "$LOG" 2>&1
# The same, for logins over SSH: Ray's commands reach the container with `docker`
usermod -aG docker "$YD_AGENT_USER"

# Ray's cluster launcher runs its commands on a node over SSH, in a shell that reads
# this file: `ray exec cluster.yaml 'ray status'`, and the `ray stop` with which
# `ray down` begins. Ray is in the container, so `ray` is passed on to it. Written
# before the key is authorised, so that it is in place for the first connection.
cat > "$YD_AGENT_HOME/.bashrc" << 'BASHRC'
# Written by RayDog's node setup.

# A RayDog node is stopped by ending its task, which stops the container as it goes.
# Stopping Ray from here would end the task as a failure, so `ray stop` does nothing.
ray() {
    if [ "${1:-}" = "stop" ]; then
        echo "raydog: this node stops when its task ends; 'ray stop' ignored"
    else
        docker exec -i "$(docker ps -q --filter name=raydog- | head -n 1)" ray "$@"
    fi
}
BASHRC
chown "$YD_AGENT_USER:$YD_AGENT_USER" "$YD_AGENT_HOME/.bashrc"

echo "raydog: authorising the SSH key for $YD_AGENT_USER"
install -d -m 700 -o "$YD_AGENT_USER" -g "$YD_AGENT_USER" "$YD_AGENT_HOME/.ssh"
printf '%s\n' "$SSH_PUBLIC_KEY" >> "$YD_AGENT_HOME/.ssh/authorized_keys"
chmod 600 "$YD_AGENT_HOME/.ssh/authorized_keys"
chown "$YD_AGENT_USER:$YD_AGENT_USER" "$YD_AGENT_HOME/.ssh/authorized_keys"

echo "raydog: node setup complete"
