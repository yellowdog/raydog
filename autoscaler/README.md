# The RayDog Autoscaler

YellowDog's autoscaler is a custom Ray node provider, configured in the normal way with a YAML file. An [example configuration file](raydog-autoscaler.yaml) is provided.

A couple of non-standard aspects to the config:

1. The commands to initialise nodes and start Ray are in the node provider config, rather than the usual place at the top level of the YAML file. These need to be structured as a bash script that YellowDog will launch as a Task.

2. The auth section from the top level has to be duplicated in the provider config. Unfortunately, the manual duplication is unavoidable - the YellowDog node provider needs to be able to connect to the head node (to access tags) at points that aren't expected in the normal autoscaler lifecycle.

The Python code for the autoscaler is in [../raydog/autoscaler.py](../src/yellowdog_ray/raydog/autoscaler.py).

## Node Initialisation

Instances used for RayDog need to be configured with various things, either through a cloudinit script or by having a customised AMI.

The [example script](example-node-setup.sh) takes a standard Ubuntu instance and configures it in the following steps:

1. Install the YellowDog agent
2. Add the public key for SSH access
3. Install uv
4. Use uv to install Python, Ray and various other packages
5. Download RayDog's autoscaler.py
6. Modify the ray script (see below)
7. Set PYTHONPATH

In addition, the head node needs to have Valkey or Redis installed and running on a non-standard port, to store tag data. The autoscaler adds the required steps to the node initialisation for the head node.

## Python Package Requirements

These Python packages will need to be installed on the client node: `ray[client] sshtunnel yellowdog-sdk redis`.

The head node will need the same, plus `ray[default]` 

Worker nodes just need `ray[default]`, plus whatever dependencies are needed for the Ray workload. 

## Getting `autoscaler.py` onto the Head Node

Once released, RayDog will be available on PyPI and the head node will be able to install it in the normal ways for published packages.

Short term, the example node setup script downloads the code from an account on Google Drive.

## Ray Script

In order for Ray up/down to be able to use the RayDog autoscaler package, the top level Python script that launches ray on the client node and the head node needs to be modified to import the RayDog package:

    import re  
    import sys  
    **import raydog**  
    from ray.scripts.scripts import main  


This snippet of shell script will make the required change:
 
    RAYFILE=$VENV/bin/ray 
    cp $RAYFILE $RAYFILE.bak
    awk '1;/^import sys/{ print "import raydog"}' $RAYFILE.bak > $RAYFILE

If RayDog is not installed in the standard site-packages directory, the PYTHONPATH environment variable will also need to be defined.

    export PYTHONPATH=/path/to/directory/containing/raydog
