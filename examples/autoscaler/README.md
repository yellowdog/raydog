# The RayDog Autoscaler

YellowDog's autoscaler is a custom Ray node provider, configured in the normal way with a YAML file. An [example configuration file](raydog.yaml) is provided.

It's important to note that the commands to initialise nodes and start Ray processes are in the node provider config and node type configs, rather than the usual place at the top level of the YAML file.

The Python code for the autoscaler is in [../../src/yellowdog_ray/raydog/autoscaler.py](../../src/yellowdog_ray/raydog/autoscaler.py).

## Node Initialisation

Instances used for RayDog need to be configured with various things, either through a userdata script or by having a customised AMI.

The [example node setup script](scripts/example-node-setup.sh) takes a standard Ubuntu instance and configures it in the following steps:

1. Installs the YellowDog agent using the normal defaults; this established a `bash` task type that will run Bash scripts
2. Adds the public key to the `yd-agent` user for SSH access
3. Installs `uv`
4. Uses `uv` to install the specified versions of Python and Ray, RayDog, and various other packages
5. Disables the firewall to enable Ray internode communication

In addition, the head node needs to have Valkey or Redis installed and running on a (configurable) non-standard port, to store tag data. An additional script [scripts/install-valkey.sh](scripts/install-valkey.sh) is provided as an example of how to do this.

## Python Package Requirements

These Python packages will need to be installed on the client node: `ray[client]` and `yellowdog-ray`.

The head node will need the same, plus `ray[default]` 

Worker nodes just need `ray[default]`, plus whatever dependencies are needed for the Ray workload itself.
